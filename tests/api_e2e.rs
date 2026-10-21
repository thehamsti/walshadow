//! The HTTP API against a running daemon, local PostgreSQL and ClickHouse
//!
//! `api_tenants_apply_live_without_restart`: tenants change through the API
//! while others stream, and the daemon never restarts. Create, a live
//! table opt-in, a refused then acknowledged re-bind, detach and attach,
//! a two-step delete that keeps the destination's rows, cluster pause in
//! tenants mode, and the audit trail of all of it.
//!
//! `api_detach_slow_destination_does_not_stall_pump`: one tenant writes
//! through a TCP proxy that freezes mid initial load. Detaching it leaves
//! the other tenant's rows flowing, its teardown aborts and joins its
//! writers past the drain limit, a re-attach waits for that, and a restart
//! afterwards boots clean.
//!
//! Skipped silently when `initdb` or `clickhouse` is absent. Linux only

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use walshadow::shadow::{Shadow, ShadowConfig};

struct Tenant {
    id: &'static str,
    db: &'static str,
    /// ClickHouse port the tenant writes to
    ch_port: u16,
}

struct Harness {
    tmp: tempfile::TempDir,
    source: Shadow,
    ch: fx::ChServer,
    child: Option<Child>,
    bin: String,
    args: Vec<String>,
    spill_dir: PathBuf,
    stderr_path: PathBuf,
    ch_tcp: u16,
    api: SocketAddr,
    admin: String,
    read: String,
    http: reqwest::Client,
}

fn fragment(t: &Tenant) -> String {
    let id = t.id;
    format!(
        "[tenant.{id}]\ndbname = \"{db}\"\n\
         [tenant.{id}.ch]\nhost = \"127.0.0.1\"\nport = {port}\ndatabase = \"tenant_{id}\"\n\
         [tenant.{id}.stream]\nreplicate_all = false\n\
         [tenant.{id}.table.app.orders]\nreplicate = true\ninitial_load = \"copy\"\n\
         [tenant.{id}.namespace.app]\ntarget_database = \"tenant_{id}\"\nauto_create = true\n",
        db = t.db,
        port = t.ch_port,
    )
}

/// `api-key new`, through the binary
fn mint(bin: &str, id: &str, scope: &str) -> Result<(String, String)> {
    let out = Command::new(bin)
        .args(["api-key", "new", "--id", id, "--scope", scope])
        .output()?;
    anyhow::ensure!(
        out.status.success(),
        "api-key new: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let token = String::from_utf8(out.stdout)?.trim().to_string();
    let stanza = String::from_utf8(out.stderr)?
        .lines()
        .filter(|l| !l.starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    Ok((token, stanza))
}

impl Harness {
    async fn up(
        ports: &fx::Ports,
        dbs: &[&str],
        tenants: &[Tenant],
        stall_secs: u64,
        rows: &[(&str, u64)],
    ) -> Result<Self> {
        Self::up_with_registry(ports, dbs, tenants, stall_secs, rows, false).await
    }

    async fn up_with_registry(
        ports: &fx::Ports,
        dbs: &[&str],
        tenants: &[Tenant],
        stall_secs: u64,
        rows: &[(&str, u64)],
        sql: bool,
    ) -> Result<Self> {
        let tmp = tempfile::tempdir().unwrap();
        let mut scfg = ShadowConfig::new(
            tmp.path().join("source-data"),
            tmp.path().join("source-filtered"),
        );
        scfg.port = ports.source;
        scfg.socket_dir = tmp.path().join("source-sock");
        scfg.ctl_timeout = Duration::from_secs(60);
        fs::create_dir_all(&scfg.filter_out_dir).unwrap();
        fs::create_dir_all(&scfg.socket_dir).unwrap();
        let source = Shadow::new(scfg);
        source.initdb().context("initdb source")?;
        source.write_base_conf().context("source base conf")?;
        fx::append_source_conf(&source).context("append source conf")?;
        source.start().context("start source")?;
        for db in dbs {
            source.psql_one(&format!("CREATE DATABASE {db}"))?;
            let n = rows.iter().find(|(d, _)| d == db).map_or(50, |(_, n)| *n);
            connect(&source, db)
                .await?
                .batch_execute(&format!(
                    "CREATE SCHEMA app;\n\
                     CREATE TABLE app.orders (id bigint PRIMARY KEY, note text NOT NULL);\n\
                     CREATE TABLE app.items (id bigint PRIMARY KEY, note text NOT NULL);\n\
                     INSERT INTO app.orders SELECT g, '{db}-seed-' || g FROM generate_series(1, {n}) g;\n\
                     INSERT INTO app.items SELECT g, '{db}-item-' || g FROM generate_series(1, 20) g;\n"
                ))
                .await?;
        }
        source.psql_one("CHECKPOINT")?;
        let ch_tmp = tempfile::tempdir().unwrap();
        let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).context("spawn ch")?;

        let config = tmp.path().join("walshadow.toml");
        fs::write(
            &config,
            format!(
                "[tenants]\nstall_timeout_secs = {stall_secs}\nbridge_workers = 1\ncapacity = 8\n{}",
                if sql {
                    "registry = 'sql'\nregistry_schema = 'walshadow'\nregistry_poll_secs = 1\n"
                } else {
                    ""
                }
            ),
        )?;
        let frag_dir = config.with_extension("d");
        fs::create_dir_all(&frag_dir)?;
        for t in tenants {
            fs::write(
                frag_dir.join(format!("60-tenant-{}.toml", t.id)),
                fragment(t),
            )?;
        }
        let bin = env!("CARGO_BIN_EXE_walshadow-stream").to_string();
        let (admin, admin_stanza) = mint(&bin, "ops", "admin")?;
        let (read, read_stanza) = mint(&bin, "dash", "read")?;
        let keys = tmp.path().join("api-keys.toml");
        fs::write(&keys, format!("{admin_stanza}\n{read_stanza}\n"))?;
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&keys, fs::Permissions::from_mode(0o600))?;
        }
        let shadow_sock = tmp.path().join("shadow-sock");
        fs::create_dir_all(&shadow_sock)?;
        let filtered = tmp.path().join("filtered");
        fs::create_dir_all(&filtered)?;
        let spill_dir = tmp.path().join("spill");
        fs::create_dir_all(&spill_dir)?;
        let api: SocketAddr = format!("127.0.0.1:{}", fx::reserve_port()).parse().unwrap();
        let stderr_path = tmp.path().join("daemon.stderr.log");
        let args: Vec<String> = [
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &ports.source.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--slot",
            "walshadow_api",
            "--out-dir",
            filtered.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &ports.shadow.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &format!("127.0.0.1:{}", ports.metrics),
            "--walsender-bind",
            &format!("127.0.0.1:{}", ports.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            config.to_str().unwrap(),
            "--control-socket",
            tmp.path().join("control.sock").to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            tmp.path().join("shadow-data").to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "120",
            "--bridge-lib-dir",
            fx::pgext_dir().to_str().unwrap(),
            "--api-bind",
            &api.to_string(),
            "--api-keys-file",
            keys.to_str().unwrap(),
            // The default mutation budget, burst 5, is per key; this test is
            // one key doing everything
            "--api-rate-limit",
            "mutate=50/s:100",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let child = spawn_daemon(&bin, &args, &stderr_path)?;
        let h = Self {
            tmp,
            source,
            ch,
            child: Some(child),
            bin,
            args,
            spill_dir,
            stderr_path,
            ch_tcp: ports.ch_tcp,
            api,
            admin,
            read,
            http: reqwest::Client::new(),
        };
        fx::wait_for_listen(h.api, Duration::from_secs(120))
            .with_context(|| format!("API never came up\n{}", tail(&h.stderr())))?;
        Ok(h)
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    fn pid(&self) -> u32 {
        self.child.as_ref().unwrap().id()
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        etag: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        let url = format!("http://{}{path}", self.api);
        let mut req = self.http.request(method.parse().unwrap(), url);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        if let Some(e) = etag {
            req = req.header("if-match", format!("\"{e}\""));
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.expect("API reachable");
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn admin(
        &self,
        method: &str,
        path: &str,
        etag: Option<&str>,
        body: Option<Value>,
    ) -> (u16, Value) {
        self.call(method, path, Some(&self.admin.clone()), etag, body)
            .await
    }

    async fn tenant(&self, id: &str) -> Value {
        self.call(
            "GET",
            &format!("/v1/tenants/{id}"),
            Some(&self.read.clone()),
            None,
            None,
        )
        .await
        .1
    }

    async fn etag(&self, id: &str) -> String {
        self.tenant(id).await["etag"].as_str().unwrap().to_string()
    }

    /// Wait until the API reports `phase` for `id`
    async fn wait_phase(&self, id: &str, phase: &str, timeout: Duration) -> Result<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let t = self.tenant(id).await;
            if t["status"]["phase"] == phase {
                return Ok(t);
            }
            if Instant::now() >= deadline {
                bail!(
                    "tenant {id} never reached {phase}: {t}\n{}",
                    tail(&self.stderr())
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn wait_rows(
        &self,
        database: &str,
        table: &str,
        db: &str,
        rows: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let sql = format!(
            "SELECT count(), countIf(startsWith(note, '{db}-')) FROM {database}.{table} FINAL WHERE _is_deleted = 0"
        );
        loop {
            let got = self.ch.query(&sql).unwrap_or_default();
            if got.trim() == format!("{rows}\t{rows}") {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "{database}.{table}: want {rows} rows from {db}, have {:?}\n{}",
                    got.trim(),
                    tail(&self.stderr())
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            Command::new("kill")
                .args(["-TERM", &child.id().to_string()])
                .status()?;
            let deadline = Instant::now() + Duration::from_secs(90);
            while child.try_wait()?.is_none() {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    bail!("daemon did not stop\n{}", tail(&self.stderr()));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        self.child = Some(spawn_daemon(&self.bin, &self.args, &self.stderr_path)?);
        fx::wait_for_listen(self.api, Duration::from_secs(120))
            .with_context(|| format!("daemon did not come back\n{}", tail(&self.stderr())))
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.source.stop();
        let _ = &self.tmp;
    }
}

fn tail(log: &str) -> String {
    let lines: Vec<&str> = log.lines().collect();
    lines[lines.len().saturating_sub(60)..].join("\n")
}

async fn connect(source: &Shadow, db: &str) -> Result<tokio_postgres::Client> {
    let cfg = source.config();
    let conninfo =
        walshadow::pg::socket_conninfo(cfg.socket_dir.to_str().unwrap(), cfg.port, "postgres", db);
    let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn spawn_daemon(bin: &str, args: &[String], stderr_path: &Path) -> Result<Child> {
    let stderr_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(stderr_path)?;
    Command::new(bin)
        .args(args)
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .context("spawn daemon")
}

/// Rows keep arriving in `db` while the test changes tenants
fn keep_writing(
    source: &Shadow,
    db: &'static str,
    from: i64,
) -> (tokio::task::JoinHandle<i64>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let cfg = source.config().clone();
    let handle = tokio::spawn(async move {
        let conninfo = walshadow::pg::socket_conninfo(
            cfg.socket_dir.to_str().unwrap(),
            cfg.port,
            "postgres",
            db,
        );
        let (client, conn) = tokio_postgres::connect(&conninfo, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut next = from;
        while !flag.load(Ordering::Relaxed) {
            client
                .batch_execute(&format!(
                    "INSERT INTO app.orders VALUES ({next}, '{db}-live-{next}')"
                ))
                .await
                .unwrap();
            next += 1;
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        next - 1
    });
    (handle, stop)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_tenants_apply_live_without_restart() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let tenants = [
        Tenant {
            id: "a",
            db: "app_a",
            ch_port: ports.ch_tcp,
        },
        Tenant {
            id: "b",
            db: "app_b",
            ch_port: ports.ch_tcp,
        },
    ];
    let h = Harness::up(&ports, &["app_a", "app_b", "app_c"], &tenants, 120, &[])
        .await
        .expect("harness");
    let t = Duration::from_secs(120);
    let pid = h.pid();
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_phase("b", "active", t).await.unwrap();
    h.wait_rows("tenant_a", "app_orders", "app_a", 50, t)
        .await
        .unwrap();
    h.wait_rows("tenant_b", "app_orders", "app_b", 50, t)
        .await
        .unwrap();

    // Authentication and scope over real TCP
    let (s, _) = h.call("GET", "/v1/tenants", None, None, None).await;
    assert_eq!(s, 401);
    let (s, list) = h
        .call("GET", "/v1/tenants", Some(&h.read), None, None)
        .await;
    assert_eq!(s, 200, "{list}");
    let (s, _) = h
        .call("POST", "/v1/reload", Some(&h.read), None, None)
        .await;
    assert_eq!(s, 403);

    let (writer_a, stop_a) = keep_writing(&h.source, "app_a", 1000);
    let (writer_b, stop_b) = keep_writing(&h.source, "app_b", 1000);

    // Create c while a and b stream
    let c = json!({
        "id": "c",
        "dbname": "app_c",
        "ch": {"host": "127.0.0.1", "port": h.ch_tcp, "database": "tenant_c"},
        "stream": {"replicate_all": false},
        "table": {"app": {"orders": {"replicate": true, "initial_load": "copy"}}},
        "namespace": {"app": {"target_database": "tenant_c", "auto_create": true}},
    });
    let (s, out) = h.admin("POST", "/v1/tenants", None, Some(c)).await;
    assert_eq!(s, 202, "{out}");
    h.wait_phase("c", "active", t).await.unwrap();
    h.wait_rows("tenant_c", "app_orders", "app_c", 50, t)
        .await
        .unwrap();

    // A live opt-in for b: its incarnation does not change
    let b_before = h.tenant("b").await;
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/b",
            Some(&h.etag("b").await),
            Some(json!({"table": {"app": {"items": {"replicate": true, "initial_load": "copy"}}}})),
        )
        .await;
    assert_eq!(
        (s, out["classification"].clone()),
        (202, json!("live")),
        "{out}"
    );
    h.wait_rows("tenant_b", "app_items", "app_b", 20, t)
        .await
        .unwrap();
    assert_eq!(
        h.tenant("b").await["status"]["incarnation"],
        b_before["status"]["incarnation"]
    );

    // A re-bind of a to another database: refused, then acknowledged
    let moved = json!({"ch": {"database": "tenant_a2"}, "namespace": {"app": {"target_database": "tenant_a2"}}});
    let a_before = h.tenant("a").await;
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/a",
            Some(&h.etag("a").await),
            Some(moved.clone()),
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (409, json!("resync_not_acknowledged")),
        "{out}"
    );
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/a?acknowledge_resync=true",
            Some(&h.etag("a").await),
            Some(moved),
        )
        .await;
    assert_eq!(
        (s, out["classification"].clone()),
        (202, json!("rebind")),
        "{out}"
    );
    let deadline = Instant::now() + t;
    loop {
        let a = h.tenant("a").await;
        if a["status"]["phase"] == "active"
            && a["status"]["incarnation"] != a_before["status"]["incarnation"]
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "a never re-attached: {a}\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // A fresh initial load into the new database: every row, seed and live
    let count = |sql: &str| {
        h.ch.query(sql)
            .unwrap_or_default()
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
    };
    let deadline = Instant::now() + t;
    while count("SELECT count() FROM tenant_a2.app_orders FINAL WHERE _is_deleted = 0") < 50 {
        assert!(
            Instant::now() < deadline,
            "tenant_a2 never loaded\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Detach b; attach it again only with the acknowledgement
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/b/detach",
            Some(&h.etag("b").await),
            Some(json!({"reason": "test"})),
        )
        .await;
    assert_eq!(s, 202, "{out}");
    h.wait_phase("b", "detached", t).await.unwrap();
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/b/attach",
            Some(&h.etag("b").await),
            None,
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (409, json!("resync_not_acknowledged"))
    );
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/b/attach?acknowledge_resync=true",
            Some(&h.etag("b").await),
            None,
        )
        .await;
    assert_eq!(s, 202, "{out}");
    h.wait_phase("b", "active", t).await.unwrap();

    // Delete c in two steps: its rows stay in ClickHouse
    let (s, intent) = h
        .admin("POST", "/v1/tenants/c/delete-intent", None, None)
        .await;
    assert_eq!(s, 200, "{intent}");
    let token = intent["confirm_token"].as_str().unwrap();
    let url = format!("http://{}/v1/tenants/c", h.api);
    let resp = h
        .http
        .delete(url)
        .bearer_auth(&h.admin)
        .header("if-match", format!("\"{}\"", h.etag("c").await))
        .header("x-confirm-delete", token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 202);
    let deadline = Instant::now() + t;
    loop {
        let (s, _) = h
            .call("GET", "/v1/tenants/c", Some(&h.read), None, None)
            .await;
        if s == 404 {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(count("SELECT count() FROM tenant_c.app_orders FINAL WHERE _is_deleted = 0") >= 50);

    // Cluster pause works with tenants
    let (s, out) = h.admin("POST", "/v1/stream/pause", None, None).await;
    assert_eq!(s, 202, "{out}");
    let (_, status) = h.call("GET", "/v1/status", Some(&h.read), None, None).await;
    assert_eq!(status["paused"], true, "{status}");
    let (s, _) = h.admin("POST", "/v1/stream/resume", None, None).await;
    assert_eq!(s, 202);

    // a and b kept receiving through all of it
    stop_a.store(true, Ordering::Relaxed);
    stop_b.store(true, Ordering::Relaxed);
    let last_a = writer_a.await.unwrap();
    let last_b = writer_b.await.unwrap();
    let want_a = 50 + (last_a - 999) as u64;
    let deadline = Instant::now() + t;
    while count("SELECT count() FROM tenant_a2.app_orders FINAL WHERE _is_deleted = 0") < want_a {
        assert!(
            Instant::now() < deadline,
            "a's live rows never all landed\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    h.wait_rows(
        "tenant_b",
        "app_orders",
        "app_b",
        50 + (last_b - 999) as u64,
        t,
    )
    .await
    .unwrap();

    // Same process throughout
    assert_eq!(h.pid(), pid);
    assert!(
        !h.stderr().contains("restarting the session"),
        "{}",
        tail(&h.stderr())
    );
    // And all of it is in the audit log
    let (_, audit) = h.admin("GET", "/v1/audit?limit=1000", None, None).await;
    assert_eq!(audit["chain_ok"], true);
    let actions: Vec<String> = audit["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["outcome"] == "applied")
        .map(|r| r["action"].as_str().unwrap_or_default().to_string())
        .collect();
    for want in [
        "tenant.create",
        "tenant.patch",
        "tenant.detach",
        "tenant.attach",
        "tenant.delete_intent",
        "tenant.delete",
        "stream.pause",
        "stream.resume",
    ] {
        assert!(
            actions.iter().any(|a| a == want),
            "{want} missing from {actions:?}"
        );
    }
    let slots = h
        .source
        .psql_one("SELECT count(*) FROM pg_replication_slots")
        .unwrap();
    assert_eq!(slots, "1");
}

/// A TCP proxy in front of ClickHouse that stops forwarding while frozen
struct Proxy {
    port: u16,
    frozen: Arc<AtomicBool>,
}

impl Proxy {
    async fn spawn(upstream: u16) -> Self {
        let port = fx::reserve_port();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .unwrap();
        let frozen = Arc::new(AtomicBool::new(false));
        let gate = frozen.clone();
        tokio::spawn(async move {
            loop {
                let Ok((down, _)) = listener.accept().await else {
                    return;
                };
                let gate = gate.clone();
                tokio::spawn(async move {
                    let Ok(up) = tokio::net::TcpStream::connect(("127.0.0.1", upstream)).await
                    else {
                        return;
                    };
                    let (dr, dw) = down.into_split();
                    let (ur, uw) = up.into_split();
                    tokio::spawn(pump(dr, uw, gate.clone()));
                    tokio::spawn(pump(ur, dw, gate));
                });
            }
        });
        Self { port, frozen }
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    frozen: Arc<AtomicBool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut buf = vec![0u8; 64 << 10];
    loop {
        while frozen.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let n = match tokio::time::timeout(Duration::from_millis(100), from.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return,
            Ok(Ok(n)) => n,
            Err(_) => continue,
        };
        while frozen.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_detach_slow_destination_does_not_stall_pump() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let proxy = Proxy::spawn(ports.ch_tcp).await;
    let tenants = [
        Tenant {
            id: "a",
            db: "app_a",
            ch_port: ports.ch_tcp,
        },
        Tenant {
            id: "s",
            db: "app_s",
            ch_port: proxy.port,
        },
    ];
    // s's initial load is big enough to freeze it midway
    let mut h = Harness::up(
        &ports,
        &["app_a", "app_s"],
        &tenants,
        20,
        &[("app_s", 1_500_000)],
    )
    .await
    .expect("harness");
    let t = Duration::from_secs(180);
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_rows("tenant_a", "app_orders", "app_a", 50, t)
        .await
        .unwrap();
    // Freeze s's destination while its COPY is under way
    let deadline = Instant::now() + t;
    loop {
        let s = h.tenant("s").await;
        let copied = s["status"]["backfill_copy_rows"].as_u64().unwrap_or(0);
        if copied > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "s never started copying: {s}\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    proxy.frozen.store(true, Ordering::Relaxed);
    // The jobs view refreshes every 2 s, and keeps refreshing while the
    // destination is frozen: the publish never waits on a stuck load
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let s_view = h.tenant("s").await;
        if s_view["jobs"].as_array().is_some_and(|j| !j.is_empty()) {
            assert_eq!(s_view["jobs"][0]["kind"], "initial_load", "{s_view}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no job for s's initial load: {s_view}\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Live rows for s, stuck in its pipeline behind the frozen destination
    connect(&h.source, "app_s")
        .await
        .unwrap()
        .batch_execute("INSERT INTO app.orders SELECT g, 'app_s-live-' || g FROM generate_series(2000001, 2000010) g")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Detach s: routing drops at once, the drain runs off the pump
    let detached_at = Instant::now();
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/s/detach",
            Some(&h.etag("s").await),
            None,
        )
        .await;
    assert_eq!(s, 202, "{out}");
    h.wait_phase("s", "detaching", Duration::from_secs(10))
        .await
        .unwrap();
    // a keeps streaming within 10 s, well inside s's 20 s drain limit
    connect(&h.source, "app_a")
        .await
        .unwrap()
        .batch_execute("INSERT INTO app.orders VALUES (51, 'app_a-after-detach')")
        .await
        .unwrap();
    h.wait_rows(
        "tenant_a",
        "app_orders",
        "app_a",
        51,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(
        detached_at.elapsed() < Duration::from_secs(20),
        "the pump was not held by s's drain"
    );

    // A re-attach waits for the teardown to join every writer
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/s/attach?acknowledge_resync=true",
            Some(&h.etag("s").await),
            None,
        )
        .await;
    assert_eq!(s, 202, "{out}");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        h.tenant("s").await["status"]["phase"],
        "detaching",
        "still tearing down"
    );
    // Past the drain limit the teardown aborts and joins every writer; the
    // destination thaws only then, so the re-attach cannot have started
    let deadline = Instant::now() + Duration::from_secs(120);
    while !h.stderr().contains("tenant teardown finished") {
        assert!(
            !h.stderr().contains("left a writer running"),
            "a writer outlived the teardown\n{}",
            tail(&h.stderr())
        );
        assert!(
            Instant::now() < deadline,
            "teardown never finished\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        detached_at.elapsed() >= Duration::from_secs(15),
        "the teardown waited out its drain before aborting"
    );
    proxy.frozen.store(false, Ordering::Relaxed);
    h.wait_phase("s", "active", t).await.unwrap();

    // A restart after all of that boots clean and resumes both
    h.stop().unwrap();
    h.start().unwrap();
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_phase("s", "active", t).await.unwrap();
    assert!(h.spill_dir.join("tenants/s/tenant.toml").exists());
    let (_, crashes) = h
        .call("GET", "/v1/crashes", Some(&h.read), None, None)
        .await;
    assert!(
        crashes["crashes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["kind"] != "panic"),
        "{crashes}"
    );
}

/// R3-M3: a COPY pass blocked on its destination belongs to its tenant's
/// writers. The teardown aborts and joins it before giving the destination
/// up, so once the teardown reports joined, nothing of that tenant writes
/// again, even when the destination comes back
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn teardown_waits_for_blocked_backfill() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let proxy = Proxy::spawn(ports.ch_tcp).await;
    let tenants = [
        Tenant {
            id: "a",
            db: "app_a",
            ch_port: ports.ch_tcp,
        },
        Tenant {
            id: "s",
            db: "app_s",
            ch_port: proxy.port,
        },
    ];
    let h = Harness::up(
        &ports,
        &["app_a", "app_s"],
        &tenants,
        20,
        &[("app_s", 1_500_000)],
    )
    .await
    .expect("harness");
    let t = Duration::from_secs(180);
    h.wait_phase("a", "active", t).await.unwrap();
    let deadline = Instant::now() + t;
    while h.tenant("s").await["status"]["backfill_copy_rows"]
        .as_u64()
        .unwrap_or(0)
        == 0
    {
        assert!(
            Instant::now() < deadline,
            "s never started copying\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    proxy.frozen.store(true, Ordering::Relaxed);
    let rows = || {
        h.ch.query("SELECT count() FROM tenant_s.app_orders")
            .unwrap_or_default()
            .trim()
            .parse::<u64>()
            .unwrap_or(0)
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants/s/detach",
            Some(&h.etag("s").await),
            None,
        )
        .await;
    assert_eq!(s, 202, "{out}");
    let deadline = Instant::now() + Duration::from_secs(120);
    while !h.stderr().contains("tenant teardown finished") {
        assert!(
            !h.stderr().contains("left a writer running"),
            "a writer outlived the teardown\n{}",
            tail(&h.stderr())
        );
        assert!(
            Instant::now() < deadline,
            "teardown never finished\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    h.wait_phase("s", "detached", Duration::from_secs(10))
        .await
        .unwrap();
    // The destination comes back: the aborted pass must not resume
    proxy.frozen.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let settled = rows();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        rows(),
        settled,
        "a joined COPY pass wrote after its teardown"
    );
    assert!(settled < 1_500_000, "the load was cut short, not finished");
    // The rest of the daemon never noticed
    connect(&h.source, "app_a")
        .await
        .unwrap()
        .batch_execute("INSERT INTO app.orders VALUES (51, 'app_a-after-teardown')")
        .await
        .unwrap();
    h.wait_rows(
        "tenant_a",
        "app_orders",
        "app_a",
        51,
        Duration::from_secs(20),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_registry_api_updates_compare_authoritative_rows_and_delete_safely() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let h = Harness::up_with_registry(&ports, &["app_a"], &[], 30, &[], true)
        .await
        .expect("SQL harness");
    let pid = h.pid();
    let t = Duration::from_secs(120);
    let (s, out) = h
        .admin(
            "POST",
            "/v1/tenants",
            None,
            Some(json!({
                "id": "a", "dbname": "app_a",
                "ch": {"host": "127.0.0.1", "port": ports.ch_tcp, "database": "tenant_a"},
                "stream": {"replicate_all": false},
                "table": {"app": {"orders": {"replicate": true, "initial_load": "copy"}}},
                "namespace": {"app": {"target_database": "tenant_a", "auto_create": true}}
            })),
        )
        .await;
    assert_eq!(s, 202, "{out}");
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_rows("tenant_a", "app_orders", "app_a", 50, t)
        .await
        .unwrap();
    let before = h.etag("a").await;
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/a",
            Some(&before),
            Some(json!({"max_lag_bytes": 1_000_000_000_u64})),
        )
        .await;
    assert_eq!(s, 202, "{out}");
    let pg = connect(&h.source, "postgres").await.unwrap();
    let spec: String = pg
        .query_one("SELECT spec FROM walshadow.tenant WHERE id = 'a'", &[])
        .await
        .unwrap()
        .get(0);
    assert!(spec.contains("max_lag_bytes = 1000000000"), "{spec}");
    let stale = h.etag("a").await;
    assert_ne!(stale, before);
    let mut external: toml::Table = spec.parse().unwrap();
    external.insert("max_lag_bytes".into(), 2_000_000_000_i64.into());
    let external = toml::to_string(&external).unwrap();
    assert_eq!(
        pg.execute(
            "UPDATE walshadow.tenant SET spec = $1, updated_at = now() WHERE id = 'a'",
            &[&external]
        )
        .await
        .unwrap(),
        1
    );
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/a",
            Some(&stale),
            Some(json!({"max_lag_bytes": 3_000_000_000_u64})),
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (412, json!("precondition_failed"))
    );
    let stored: String = pg
        .query_one("SELECT spec FROM walshadow.tenant WHERE id = 'a'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, external, "stale API write must not overwrite SQL");
    let deadline = Instant::now() + t;
    loop {
        let (_, page) = h
            .admin(
                "GET",
                "/v1/audit?action=tenant_set.external_change",
                None,
                None,
            )
            .await;
        if page["records"].as_array().is_some_and(|records| {
            records
                .iter()
                .any(|r| r["actor"]["id"] == "sql_registry" && r["tenant"] == "a")
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "external SQL change not audited: {page}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let current = h.etag("a").await;
    let (s, intent) = h
        .admin("POST", "/v1/tenants/a/delete-intent", None, None)
        .await;
    assert_eq!(s, 200, "{intent}");
    let resp = h
        .http
        .delete(format!("http://{}/v1/tenants/a", h.api))
        .bearer_auth(&h.admin)
        .header("if-match", format!("\"{current}\""))
        .header(
            "x-confirm-delete",
            intent["confirm_token"].as_str().unwrap(),
        )
        .send()
        .await
        .unwrap();
    let s = resp.status().as_u16();
    let out: Value = resp.json().await.unwrap();
    assert_eq!(s, 202, "{out}");
    let count: i64 = pg
        .query_one("SELECT count(*) FROM walshadow.tenant WHERE id = 'a'", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
    let (s, _) = h.admin("GET", "/v1/tenants/a", None, None).await;
    assert_eq!(s, 404);
    h.wait_rows("tenant_a", "app_orders", "app_a", 50, t)
        .await
        .unwrap();
    assert_eq!(h.pid(), pid, "mutations never restart the daemon");
}

/// C4: a re-bind accepted while the daemon resumes the tenant, its
/// destination still opening, never lets the runtime opened for the old
/// binding route WAL: the rows written while the daemon was down land only
/// in the new database, after a fresh attach, never in the old one
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebind_during_resume_never_routes_the_old_runtime() {
    if !fx::tools::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let proxy = Proxy::spawn(ports.ch_tcp).await;
    let tenants = [
        Tenant {
            id: "a",
            db: "app_a",
            ch_port: ports.ch_tcp,
        },
        Tenant {
            id: "r",
            db: "app_r",
            ch_port: proxy.port,
        },
    ];
    let mut h = Harness::up(&ports, &["app_a", "app_r"], &tenants, 20, &[])
        .await
        .expect("harness");
    let t = Duration::from_secs(180);
    h.wait_phase("r", "active", t).await.unwrap();
    h.wait_rows("tenant_r", "app_orders", "app_r", 50, t)
        .await
        .unwrap();

    // Rows while the daemon is down, then a restart whose resume of r
    // blocks on its frozen destination
    h.stop().unwrap();
    connect(&h.source, "app_r")
        .await
        .unwrap()
        .batch_execute(
            "INSERT INTO app.orders SELECT g, 'app_r-while-down-' || g FROM generate_series(1001, 1010) g",
        )
        .await
        .unwrap();
    proxy.frozen.store(true, Ordering::Relaxed);
    let before_restart = h.stderr().len();
    h.start().unwrap();
    let deadline = Instant::now() + t;
    while !h.stderr().contains("tenant resumed") {
        assert!(
            Instant::now() < deadline,
            "a never resumed\n{}",
            tail(&h.stderr())
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Re-bind r to another database while its old runtime is still opening
    let etag = h.etag("r").await;
    let (s, out) = h
        .admin(
            "PATCH",
            "/v1/tenants/r?acknowledge_resync=true",
            Some(&etag),
            Some(json!({
                "ch": {"database": "tenant_r2"},
                "namespace": {"app": {"target_database": "tenant_r2"}},
            })),
        )
        .await;
    assert_eq!(s, 202, "{out}");
    assert_eq!(out["classification"], "rebind", "{out}");
    proxy.frozen.store(false, Ordering::Relaxed);

    // r attaches afresh on the new database, with every row
    h.wait_rows("tenant_r2", "app_orders", "app_r", 60, t)
        .await
        .unwrap();
    // The runtime opened for the old binding was discarded unrouted:
    // nothing was attached to the router that a re-bind then had to detach
    let since_restart = h.stderr()[before_restart..].to_string();
    assert!(
        since_restart
            .lines()
            .any(|l| l.contains("discarding a runtime opened for a binding")
                && l.contains("tenant=r")),
        "{}",
        tail(&since_restart)
    );
    assert!(
        !since_restart
            .lines()
            .any(|l| l.contains("tenant detached") && l.contains("tenant=r")),
        "the runtime opened for the old binding was routed, then detached\n{}",
        tail(&since_restart)
    );
    // and the old database never got the rows written while it was down
    let leaked = h
        .ch
        .query(
            "SELECT count() FROM tenant_r.app_orders FINAL WHERE startsWith(note, 'app_r-while-down-')",
        )
        .unwrap_or_default();
    assert_eq!(
        leaked.trim(),
        "0",
        "the runtime opened for the old binding routed WAL into it\n{}",
        tail(&h.stderr())
    );
}
