//! Many client databases on one cluster, one daemon, one physical slot.
//!
//! Each tenant replicates its own database into its own ClickHouse
//! database. Drives the `walshadow-stream` binary through: first attach of
//! two tenants with existing rows (priming, then COPY initial loads), live
//! changes in both with no cross-talk, a third tenant added by config
//! fragment + SIGHUP while the others stream, detaching one by config, and a
//! restart that resumes the active tenants from their durable state.
//!
//! Skipped silently when `initdb`, `pg_basebackup`, or `clickhouse` is
//! absent. Linux only

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use walshadow::shadow::{Shadow, ShadowConfig};

const TENANT_DBS: [&str; 3] = ["app_a", "app_b", "app_c"];

#[derive(Clone, Copy)]
enum Registry {
    Config,
    Sql,
}

struct Harness {
    tmp: tempfile::TempDir,
    source: Shadow,
    ch: fx::ChServer,
    child: Option<Child>,
    bin: String,
    args: Vec<String>,
    frag_dir: PathBuf,
    spill_dir: PathBuf,
    metrics_addr: SocketAddr,
    stderr_path: PathBuf,
    ch_tcp: u16,
    control_socket: PathBuf,
}

impl Harness {
    async fn up(ports: &fx::Ports) -> Result<Self> {
        Self::up_with(ports, Registry::Config).await
    }

    async fn up_with(ports: &fx::Ports, registry: Registry) -> Result<Self> {
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
        for db in TENANT_DBS {
            source.psql_one(&format!("CREATE DATABASE {db}"))?;
        }
        let h_source = &source;
        for db in TENANT_DBS {
            let c = connect(h_source, db).await?;
            c.batch_execute(&format!(
                "CREATE SCHEMA app;\n\
                 CREATE TABLE app.orders (id bigint PRIMARY KEY, note text NOT NULL);\n\
                 INSERT INTO app.orders SELECT g, '{db}-seed-' || g FROM generate_series(1, 50) g;\n"
            ))
            .await?;
        }
        source.psql_one("CHECKPOINT")?;

        let ch_tmp = tempfile::tempdir().unwrap();
        let ch = fx::ChServer::spawn(ch_tmp, ports.ch_tcp, ports.ch_http).context("spawn ch")?;

        let config_path = tmp.path().join("walshadow.toml");
        fs::write(
            &config_path,
            match registry {
                Registry::Config => {
                    "[tenants]\nstall_timeout_secs = 120\nbridge_workers = 1\ncapacity = 8\n"
                }
                Registry::Sql => {
                    "[tenants]\nstall_timeout_secs = 120\nbridge_workers = 1\ncapacity = 8\n\
                     registry = \"sql\"\nregistry_schema = \"walshadow\"\nregistry_poll_secs = 1\n"
                }
            },
        )?;
        let frag_dir = config_path.with_extension("d");
        fs::create_dir_all(&frag_dir)?;
        let initial: &[(&str, &str)] = match registry {
            Registry::Config => &[("a", "app_a"), ("b", "app_b")],
            Registry::Sql => &[],
        };
        for &(id, db) in initial {
            fs::write(
                frag_dir.join(format!("60-tenant-{id}.toml")),
                tenant_fragment(id, db, ports.ch_tcp, "active"),
            )?;
        }

        let shadow_data = tmp.path().join("shadow-data");
        let shadow_sock = tmp.path().join("shadow-sock");
        fs::create_dir_all(&shadow_sock).unwrap();
        let shadow_filter_dir = tmp.path().join("filtered");
        fs::create_dir_all(&shadow_filter_dir).unwrap();
        let spill_dir = tmp.path().join("spill");
        fs::create_dir_all(&spill_dir).unwrap();
        let control_socket = tmp.path().join("control.sock");
        let bin = env!("CARGO_BIN_EXE_walshadow-stream").to_string();
        let pgext_dir = fx::pgext_dir();
        let stderr_path = tmp.path().join("daemon.stderr.log");
        let metrics_addr: SocketAddr = format!("127.0.0.1:{}", ports.metrics).parse().unwrap();
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
            "walshadow_tenants",
            "--out-dir",
            shadow_filter_dir.to_str().unwrap(),
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
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", ports.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            config_path.to_str().unwrap(),
            "--control-socket",
            control_socket.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            shadow_data.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "120",
            "--bridge-lib-dir",
            pgext_dir.to_str().unwrap(),
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
            frag_dir,
            spill_dir,
            metrics_addr,
            stderr_path,
            ch_tcp: ports.ch_tcp,
            control_socket,
        };
        fx::wait_for_listen(h.metrics_addr, Duration::from_secs(120))
            .with_context(|| format!("daemon metrics endpoint never came up\n{}", h.stderr()))?;
        Ok(h)
    }

    fn ctl(&self, words: &[&str], stdin: &str) -> Result<String> {
        use std::io::Write;
        let mut child = Command::new(&self.bin)
            .arg("ctl")
            .arg("--socket")
            .arg(&self.control_socket)
            .args(words)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child.stdin.take().unwrap().write_all(stdin.as_bytes())?;
        let out = child.wait_with_output()?;
        anyhow::ensure!(
            out.status.success(),
            "ctl {words:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    fn sighup(&self) -> Result<()> {
        let pid = self.child.as_ref().context("daemon gone")?.id();
        let ok = Command::new("kill")
            .args(["-HUP", &pid.to_string()])
            .status()?
            .success();
        anyhow::ensure!(ok, "kill -HUP {pid} failed");
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()?;
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                if child.try_wait()?.is_some() {
                    break;
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    bail!("daemon did not stop\n{}", self.stderr());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        self.child = Some(spawn_daemon(&self.bin, &self.args, &self.stderr_path)?);
        fx::wait_for_listen(self.metrics_addr, Duration::from_secs(120))
            .with_context(|| format!("daemon did not come back\n{}", self.stderr()))
    }

    /// Wait until `tenant_<id>.orders` holds exactly `rows` live rows, all
    /// from `db`
    async fn wait_rows(&self, id: &str, db: &str, rows: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let sql = format!(
            "SELECT count(), countIf(startsWith(note, '{db}-')) \
             FROM tenant_{id}.orders FINAL WHERE _is_deleted = 0"
        );
        loop {
            let got = self.ch.query(&sql).unwrap_or_default();
            if got.trim() == format!("{rows}\t{rows}") {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "tenant {id}: want {rows} rows from {db}, have {:?}\n{}",
                    got.trim(),
                    tail(&self.stderr())
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    fn state(&self, id: &str) -> Result<toml::Table> {
        let path = self.spill_dir.join("tenants").join(id).join("tenant.toml");
        let text = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        Ok(toml::from_str(&text)?)
    }

    async fn wait_phase(&self, id: &str, phase: &str, timeout: Duration) -> Result<toml::Table> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(state) = self.state(id)
                && state.get("phase").and_then(|v| v.as_str()) == Some(phase)
            {
                return Ok(state);
            }
            if Instant::now() >= deadline {
                bail!(
                    "tenant {id} never reached {phase}: {:?}\n{}",
                    self.state(id).ok(),
                    tail(&self.stderr())
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
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
    lines[lines.len().saturating_sub(40)..].join("\n")
}

/// Tenant `b` names its table explicitly, the others replicate everything,
/// so both scope paths attach through priming
fn tenant_fragment(id: &str, db: &str, ch_port: u16, state: &str) -> String {
    let scope = if id == "b" {
        format!(
            "[tenant.{id}.stream]\nreplicate_all = false\n\
             [tenant.{id}.table.app.orders]\nreplicate = true\ninitial_load = \"copy\"\n"
        )
    } else {
        format!("[tenant.{id}.stream]\nreplicate_all = true\n")
    };
    format!(
        "[tenant.{id}]\n\
         dbname = \"{db}\"\n\
         state = \"{state}\"\n\
         [tenant.{id}.ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"tenant_{id}\"\n\
         {scope}\
         [tenant.{id}.namespace.app]\n\
         target_database = \"tenant_{id}\"\n\
         auto_create = true\n"
    )
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenants_share_one_slot_attach_live_detach_and_resume() {
    if !fx::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let mut h = Harness::up(&ports).await.expect("harness");
    let t = Duration::from_secs(120);

    // First attach: existing rows arrive through each tenant's initial load
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_phase("b", "active", t).await.unwrap();
    h.wait_rows("a", "app_a", 50, t).await.unwrap();
    h.wait_rows("b", "app_b", 50, t).await.unwrap();
    let b_state = h.state("b").unwrap();
    let start = b_state["start_tables"].as_array().unwrap();
    assert_eq!(start.len(), 1, "{b_state:?}");
    assert_eq!(start[0]["initial_load"].as_str(), Some("copy"));
    assert!(
        !h.stderr()
            .lines()
            .any(|l| l.contains("tenant=b") && l.contains("ensure CH dest")),
        "explicit tables stay out of scope while priming"
    );

    // Live changes stay in their own tenant
    let a = connect(&h.source, "app_a").await.unwrap();
    let b = connect(&h.source, "app_b").await.unwrap();
    a.batch_execute(
        "INSERT INTO app.orders SELECT g, 'app_a-live-' || g FROM generate_series(51, 80) g;\n\
         DELETE FROM app.orders WHERE id <= 10;\n",
    )
    .await
    .unwrap();
    b.batch_execute(
        "INSERT INTO app.orders SELECT g, 'app_b-live-' || g FROM generate_series(51, 60) g;",
    )
    .await
    .unwrap();
    h.wait_rows("a", "app_a", 70, t).await.unwrap();
    h.wait_rows("b", "app_b", 60, t).await.unwrap();

    // One slot for all of them
    let slots = h
        .source
        .psql_one("SELECT count(*) FROM pg_replication_slots")
        .unwrap();
    assert_eq!(slots, "1", "one physical slot serves every tenant");

    // A third tenant attaches through ctl while the others keep streaming
    let c = connect(&h.source, "app_c").await.unwrap();
    let spec = tenant_fragment("c", "app_c", h.ch_tcp, "active")
        .lines()
        .filter(|l| {
            !l.starts_with("[tenant.c]") && !l.starts_with("dbname") && !l.starts_with("state")
        })
        .map(|l| l.replace("[tenant.c.", "["))
        .collect::<Vec<_>>()
        .join("\n");
    h.ctl(
        &["tenant", "add", "c", "--dbname", "app_c", "--spec", "-"],
        &spec,
    )
    .unwrap();
    assert!(h.frag_dir.join("60-tenant-c.toml").exists());
    a.batch_execute("INSERT INTO app.orders VALUES (81, 'app_a-during-attach')")
        .await
        .unwrap();
    c.batch_execute("INSERT INTO app.orders VALUES (51, 'app_c-during-attach')")
        .await
        .unwrap();
    h.wait_phase("c", "active", t).await.unwrap();
    h.wait_rows("c", "app_c", 51, t).await.unwrap();
    h.wait_rows("a", "app_a", 71, t).await.unwrap();

    let listed = h.ctl(&["tenant", "list"], "").unwrap();
    for id in ["a", "b", "c"] {
        assert!(listed.lines().any(|l| l.starts_with(id)), "{listed}");
    }
    let metrics = fx::http_get(h.metrics_addr, "/metrics").unwrap();
    assert!(
        metrics.contains("walshadow_tenant_info{tenant=\"a\",dbname=\"app_a\",phase=\"active\"} 1"),
        "{metrics}"
    );
    // A spec that fails validation is refused and changes nothing
    assert!(
        h.ctl(&["tenant", "add", "bad", "--dbname", "app_a"], "")
            .is_err(),
        "two tenants may not follow one database"
    );

    // Detach b through ctl: it stops receiving and stops holding WAL
    h.ctl(&["tenant", "detach", "b"], "").unwrap();
    let detached = h.wait_phase("b", "detached", t).await.unwrap();
    assert_eq!(
        detached.get("reason").and_then(|v| v.as_str()),
        Some("detached by operator")
    );
    b.batch_execute("INSERT INTO app.orders VALUES (61, 'app_b-after-detach')")
        .await
        .unwrap();
    c.batch_execute("INSERT INTO app.orders VALUES (52, 'app_c-after-detach')")
        .await
        .unwrap();
    h.wait_rows("c", "app_c", 52, t).await.unwrap();
    h.wait_rows("b", "app_b", 60, Duration::from_secs(1))
        .await
        .unwrap();
    // A reload with nothing changed reconciles to the same tenant set
    h.sighup().unwrap();
    c.batch_execute("INSERT INTO app.orders VALUES (53, 'app_c-after-reload')")
        .await
        .unwrap();
    h.wait_rows("c", "app_c", 53, t).await.unwrap();

    // Restart: active tenants resume from their state, nothing re-primes
    h.stop().unwrap();
    a.batch_execute("INSERT INTO app.orders VALUES (82, 'app_a-while-down')")
        .await
        .unwrap();
    c.batch_execute("UPDATE app.orders SET note = 'app_c-updated' WHERE id = 1")
        .await
        .unwrap();
    h.start().unwrap();
    h.wait_rows("a", "app_a", 72, t).await.unwrap();
    h.wait_rows("c", "app_c", 53, t).await.unwrap();
    let note =
        h.ch.query("SELECT note FROM tenant_c.orders FINAL WHERE _is_deleted = 0 AND id = 1")
            .unwrap();
    assert_eq!(note.trim(), "app_c-updated");
    assert_eq!(
        h.state("a").unwrap().get("phase").and_then(|v| v.as_str()),
        Some("active")
    );
    assert!(
        !h.stderr().contains("tenant attached; priming")
            || h.stderr().matches("tenant attached; priming").count() == 3,
        "restart must resume active tenants rather than re-attach them"
    );
    h.stop().unwrap();
}

/// Tenant spec without the `[tenant.<id>]` prefix, for `ctl tenant add`
fn spec_of(id: &str, db: &str, ch_port: u16) -> String {
    tenant_fragment(id, db, ch_port, "active")
        .lines()
        .filter(|l| {
            !l.starts_with(&format!("[tenant.{id}]"))
                && !l.starts_with("dbname")
                && !l.starts_with("state")
        })
        .map(|l| l.replace(&format!("[tenant.{id}."), "["))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_registry_manages_tenants_and_a_broken_tenant_stays_isolated() {
    if !fx::requirements_available() {
        return;
    }
    let ports = fx::Ports::alloc();
    let mut h = Harness::up_with(&ports, Registry::Sql)
        .await
        .expect("harness");
    let t = Duration::from_secs(120);

    // Tenants arrive as registry rows through ctl
    h.ctl(
        &["tenant", "add", "a", "--dbname", "app_a", "--spec", "-"],
        &spec_of("a", "app_a", h.ch_tcp),
    )
    .unwrap();
    let rows = h
        .source
        .psql_one(
            "SELECT string_agg(id || ':' || dbname || ':' || state, ',') FROM walshadow.tenant",
        )
        .unwrap();
    assert_eq!(rows, "a:app_a:active");
    h.wait_phase("a", "active", t).await.unwrap();
    h.wait_rows("a", "app_a", 50, t).await.unwrap();

    // A tenant whose destination is unreachable fails alone
    let broken =
        spec_of("c", "app_c", h.ch_tcp).replace(&format!("port = {}", h.ch_tcp), "port = 1");
    h.ctl(
        &["tenant", "add", "c", "--dbname", "app_c", "--spec", "-"],
        &broken,
    )
    .unwrap();
    let state = h.wait_phase("c", "detached", t).await.unwrap();
    assert!(
        state["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("attach failed"),
        "{state:?}"
    );
    let a = connect(&h.source, "app_a").await.unwrap();
    a.batch_execute("INSERT INTO app.orders VALUES (51, 'app_a-beside-broken')")
        .await
        .unwrap();
    h.wait_rows("a", "app_a", 51, t).await.unwrap();

    // Rows edited straight in SQL take effect within a poll
    h.source
        .psql_one("UPDATE walshadow.tenant SET state = 'detached' WHERE id = 'a'")
        .unwrap();
    h.wait_phase("a", "detached", t).await.unwrap();
    a.batch_execute("INSERT INTO app.orders VALUES (52, 'app_a-after-detach')")
        .await
        .unwrap();
    h.wait_rows("a", "app_a", 51, Duration::from_secs(2))
        .await
        .unwrap();
    // Re-attaching resyncs every table, catching the row written meanwhile
    h.source
        .psql_one("UPDATE walshadow.tenant SET state = 'active' WHERE id = 'a'")
        .unwrap();
    h.wait_rows("a", "app_a", 52, t).await.unwrap();
    assert!(
        h.frag_dir.join("70-registry.toml").exists(),
        "registry mirrored into config"
    );
    h.stop().unwrap();
}
