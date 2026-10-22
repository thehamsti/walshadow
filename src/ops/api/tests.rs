//! The API in process: the real router, service, reload pipeline and audit
//! log over a temp config, no network and no database

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::keys::Scope;
use super::rate_limit::Rate;
use super::routes::{Class, ROUTES};
use super::*;
use crate::control::Reloader;
use crate::metrics::{MetricsRegistry, MetricsSnapshot};
use crate::tenants::{Phase, TenantState, tenant_dir};

#[path = "docs_tests.rs"]
mod docs;

const BASE: &str = "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n";

fn tenant_a() -> String {
    "[tenant.a]\ndbname = 'a_db'\n[tenant.a.ch]\nhost = 'ch'\ndatabase = 'a'\npassword = 'hunter2'\n"
        .into()
}

struct Api {
    dir: tempfile::TempDir,
    router: axum::Router,
    admin: String,
    read: String,
    expired: String,
    disabled: String,
    admin2: String,
    state: ApiState,
    /// Checks every response [`Api::call`] gets against the document this
    /// API serves
    conformance: docs::Conformance,
}

fn generous() -> Rates {
    let lots = Rate::new(10_000.0, 10_000.0);
    Rates {
        preauth: lots,
        auth_failure: lots,
        read: lots,
        expensive: lots,
        mutate: lots,
    }
}

impl Api {
    async fn new(public_livez: bool, rates: Rates) -> Self {
        Self::exposed(
            super::openapi::Exposure {
                public_livez,
                public_docs: false,
            },
            rates,
        )
        .await
    }

    async fn exposed(exposure: super::openapi::Exposure, rates: Rates) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("ch-config.toml");
        std::fs::write(&config, BASE).unwrap();
        std::fs::create_dir_all(dir.path().join("ch-config.d")).unwrap();
        std::fs::write(
            crate::ops::tenant_ctl::tenant_fragment_path(&config, "a"),
            tenant_a(),
        )
        .unwrap();
        let state_dir = dir.path().join("api");
        let reloader = Arc::new(Reloader::new(Some(config.clone()), toml::Table::new()));
        reloader.set_audit(crate::ops::audit::AuditLog::open(&state_dir, 1 << 20, 3).unwrap());
        let metrics = MetricsRegistry::new();
        metrics
            .set(MetricsSnapshot {
                session_state: "streaming",
                tenants: vec![crate::metrics::TenantMetrics {
                    id: "a".into(),
                    dbname: "a_db".into(),
                    phase: "active",
                    lag_bytes: 7,
                    jobs: vec![crate::metrics::JobView {
                        kind: "initial_load",
                        namespace: "public".into(),
                        name: "orders".into(),
                        mode: "copy",
                        state: "running",
                        s_lsn: "0/10".into(),
                        copy_next_block: Some(3),
                    }],
                    ..Default::default()
                }],
                ..MetricsSnapshot::default()
            })
            .await;
        let ctx = crate::control::SharedCtx {
            fragment: crate::control::fragment_path(&config, "50-api.toml").unwrap(),
            ch_config: config,
            cli_base: toml::Table::new(),
            metrics,
            reloader,
            mutation_lock: crate::control::mutation_lock(),
            spill_dir: Some(dir.path().join("spill")),
        };
        let (keys_file, tokens) = keys::tests::key_file(
            dir.path(),
            &[
                ("ops", &[Scope::Admin], None, false),
                ("dash", &[Scope::Read], None, false),
                ("old", &[Scope::Admin], Some("2001-01-01T00:00:00Z"), false),
                ("off", &[Scope::Admin], None, true),
                ("ops2", &[Scope::Admin], None, false),
            ],
        );
        let keys = KeyStore::open(&keys_file).unwrap();
        let crashes = crate::ops::crash::CrashStore::open(&state_dir).unwrap();
        crashes
            .record_exit("connect failed: password=hunter2")
            .unwrap();
        let cfg = ApiConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            keys_file,
            tls: None,
            allow_plaintext: false,
            state_dir,
            public_livez: exposure.public_livez,
            public_docs: exposure.public_docs,
            trusted_proxies: Vec::new(),
            rates,
            deleted_retention: Duration::from_secs(3600),
            ready_grace: Duration::from_secs(600),
            max_in_flight: 64,
            max_connections: 256,
        };
        let state = ApiState::new(cfg, ctx, keys, Some(crashes)).await.unwrap();
        Self {
            conformance: docs::Conformance::new(&state.docs.json, exposure),
            router: router(state.clone()),
            admin: tokens[0].clone(),
            read: tokens[1].clone(),
            expired: tokens[2].clone(),
            disabled: tokens[3].clone(),
            admin2: tokens[4].clone(),
            dir,
            state,
        }
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let mut req = Request::builder().method(method).uri(path);
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let mut req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string()))
                .unwrap(),
            None => req.body(Body::empty()).unwrap(),
        };
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 5], 4000))));
        let resp = self.router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 22)
            .await
            .unwrap();
        self.conformance
            .check(method, path, status, &headers, &bytes);
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
        (status, headers, body)
    }

    async fn get(&self, path: &str, token: &str) -> (StatusCode, axum::http::HeaderMap, Value) {
        self.call("GET", path, Some(token), &[], None).await
    }

    async fn etag(&self, id: &str) -> String {
        let (s, h, _) = self.get(&format!("/v1/tenants/{id}"), &self.admin).await;
        assert_eq!(s, StatusCode::OK);
        h["etag"].to_str().unwrap().trim_matches('"').to_string()
    }

    fn audit(&self) -> Vec<crate::ops::audit::Record> {
        self.state
            .ctx
            .reloader
            .audit()
            .unwrap()
            .read(&crate::ops::audit::Filter {
                limit: 1000,
                ..Default::default()
            })
            .unwrap()
            .records
    }

    fn frag(&self, id: &str) -> String {
        std::fs::read_to_string(crate::ops::tenant_ctl::tenant_fragment_path(
            &self.state.ctx.ch_config,
            id,
        ))
        .unwrap_or_default()
    }

    async fn attach_on_disk(&self, id: &str) {
        let mut state = TenantState::new(id, &format!("{id}_db"), 7);
        state.phase = Phase::Active;
        state.incarnation = Some("inc-1".into());
        state
            .store(&tenant_dir(&self.dir.path().join("spill"), id))
            .await
            .unwrap();
    }
}

fn concrete(path: &str) -> String {
    path.replace("{id}", "a").replace("{request_id}", "r")
}

/// Acceptance 1: every route, every way to fail authentication, 401; a
/// read key on every admin route, 403
#[tokio::test]
async fn every_route_needs_a_key_and_its_scope() {
    let api = Api::new(false, generous()).await;
    let bad_secret = format!("wsk_ops_{}", "0".repeat(64));
    let unknown = format!("wsk_ghost_{}", "0".repeat(64));
    for r in ROUTES {
        let path = concrete(r.path);
        for token in [
            None,
            Some("not-a-key"),
            Some(bad_secret.as_str()),
            Some(unknown.as_str()),
            Some(api.expired.as_str()),
            Some(api.disabled.as_str()),
        ] {
            let (s, h, body) = api.call(r.method, &path, token, &[], None).await;
            assert_eq!(
                s,
                StatusCode::UNAUTHORIZED,
                "{} {} {token:?}",
                r.method,
                path
            );
            assert_eq!(h["www-authenticate"], "Bearer realm=\"walshadow\"");
            assert_eq!(body["error"]["code"], "unauthenticated");
            assert!(h.contains_key("x-request-id"));
            assert_eq!(h["cache-control"], "no-store");
        }
        // A token in the query string is refused even when valid
        let (s, ..) = api
            .call(
                r.method,
                &format!("{path}?access_token={}", api.admin),
                None,
                &[],
                None,
            )
            .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        let (s, _, body) = api.call(r.method, &path, Some(&api.read), &[], None).await;
        match r.scope {
            Scope::Admin => {
                assert_eq!(s, StatusCode::FORBIDDEN, "{} {}", r.method, path);
                assert_eq!(body["error"]["code"], "forbidden");
            }
            Scope::Read => assert!(
                s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
                "{} {} {s}",
                r.method,
                path
            ),
        }
        let (s, ..) = api.call(r.method, &path, Some(&api.admin), &[], None).await;
        assert!(
            s != StatusCode::UNAUTHORIZED && s != StatusCode::FORBIDDEN,
            "{} {} {s}",
            r.method,
            path
        );
    }
    // Paths outside the table: authentication first, so nothing enumerates
    let (s, ..) = api.call("GET", "/v1/nope", None, &[], None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let (s, ..) = api.get("/v1/nope", &api.admin).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    // A read key's 403 on a mutation is audited as denied
    assert!(
        api.audit()
            .iter()
            .any(|r| r.draft.outcome == "denied" && r.draft.actor.id.as_deref() == Some("dash"))
    );
}

/// Acceptance 2: `/livez` is public only when asked for, and says nothing
#[tokio::test]
async fn livez_is_public_only_with_the_flag() {
    let closed = Api::new(false, generous()).await;
    let (s, ..) = closed.call("GET", "/livez", None, &[], None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let open = Api::new(true, generous()).await;
    let (s, _, body) = open.call("GET", "/livez", None, &[], None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, Value::String("ok".into()));
    for r in ROUTES.iter().filter(|r| r.class != Class::Public) {
        let (s, ..) = open
            .call(r.method, &concrete(r.path), None, &[], None)
            .await;
        assert_eq!(s, StatusCode::UNAUTHORIZED, "{}", r.path);
    }
}

#[tokio::test]
async fn reads_report_without_leaking_secrets() {
    let api = Api::new(false, generous()).await;
    let (s, _, who) = api.get("/v1/whoami", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(who["principal"]["key_id"], "dash");
    let (s, _, list) = api.get("/v1/tenants", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(list["layout"], "tenants");
    assert_eq!(list["tenants"][0]["id"], "a");
    assert_eq!(list["tenants"][0]["status"]["lag_bytes"], 7);
    assert_eq!(list["tenants"][0]["destination"]["kind"], "clickhouse");
    let (s, h, one) = api.get("/v1/tenants/a", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(h["etag"].to_str().unwrap().starts_with("\"r-"));
    assert_eq!(one["spec"]["ch"]["password"], "***");
    assert_eq!(one["jobs"][0]["name"], "orders");
    let (s, ..) = api.get("/v1/tenants/zzz", &api.read).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _, jobs) = api.get("/v1/jobs", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(jobs["jobs"][0]["state"], "running");
    let (s, _, jobs) = api.get("/v1/tenants/a/jobs", &api.read).await;
    assert_eq!(
        (s, jobs["jobs"][0]["copy_next_block"].clone()),
        (StatusCode::OK, json!(3))
    );
    let (s, _, health) = api.get("/v1/health", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(health["checks"].as_array().unwrap().len() > 5);
    let (s, ..) = api.get("/v1/health/ready", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = api.get("/v1/health/live", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    let (s, h, text) = api.get("/v1/metrics", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        h["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/openmetrics-text")
    );
    let text = text.as_str().unwrap();
    assert!(
        text.contains("walshadow_tenant_lag_bytes{tenant=\"a\"} 7"),
        "{text}"
    );
    assert!(text.contains("walshadow_api_requests_total"));
    let (s, _, status) = api.get("/v1/status", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(status["epoch"].is_string());
    let (s, _, crashes) = api.get("/v1/crashes", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    let crash = &crashes["crashes"][0];
    assert_eq!(crash["kind"], "exit");
    assert!(!crash.to_string().contains("hunter2"));
    let (s, ..) = api
        .get(
            &format!("/v1/crashes/{}", crash["id"].as_str().unwrap()),
            &api.read,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    tracing::subscriber::with_default(
        {
            use tracing_subscriber::prelude::*;
            tracing_subscriber::registry().with(crate::ops::error_ring::ErrorRingLayer)
        },
        || tracing::warn!(target: "walshadow::api_test", tenant = "a", "something odd"),
    );
    let (s, _, errors) = api.get("/v1/tenants/a/errors", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        errors["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["message"] == "something odd")
    );
    let (s, _, errors) = api.get("/v1/errors?level=warn&limit=5", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert!(errors["events"].as_array().unwrap().len() <= 5);
    // Admin reads
    let (s, _, config) = api.get("/v1/config", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    assert!(!config.to_string().contains("hunter2"), "{config}");
    let (s, _, page) = api.get("/v1/audit", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(page["chain_ok"], true);
    // Without a source database the catalog listing is unavailable, not a crash
    let (s, _, body) = api.get("/v1/tenants/a/tables", &api.read).await;
    assert_eq!(
        (s, body["error"]["code"].clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json!("source_unavailable"))
    );
}

/// Acceptance 3: who, what, when, before and after, for every mutation
#[tokio::test]
async fn every_mutation_is_audited() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let b = json!({"id": "b", "dbname": "b_db", "ch": {"host": "ch", "database": "b", "password": "s3cret"}});
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants",
            Some(&api.admin),
            &[],
            Some(b.clone()),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{out}");
    let created = out["request_id"].as_str().unwrap().to_string();
    // Again: rejected before it ran, one record
    let (s, _, again) = api
        .call("POST", "/v1/tenants", Some(&api.admin), &[], Some(b))
        .await;
    assert_eq!(
        (s, again["error"]["code"].clone()),
        (StatusCode::CONFLICT, json!("exists"))
    );
    let etag = api.etag("b").await;
    let (s, ..) = api
        .call(
            "PATCH",
            "/v1/tenants/b",
            Some(&api.admin),
            &[("if-match", &format!("\"{etag}\""))],
            Some(json!({"max_lag_bytes": 99, "ch": {"password": "rotated"}})),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    assert!(api.frag("b").contains("max_lag_bytes = 99"));
    let (s, ..) = api
        .call(
            "PUT",
            "/v1/tenants/b",
            Some(&api.admin),
            &[("if-match", "\"r-stale\"")],
            Some(json!({"dbname": "b_db", "ch": {"host": "ch", "database": "b"}})),
        )
        .await;
    assert_eq!(s, StatusCode::PRECONDITION_FAILED);
    let etag = api.etag("b").await;
    let (s, ..) = api
        .call(
            "POST",
            "/v1/tenants/b/detach",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"reason": "maintenance"})),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    let etag = api.etag("b").await;
    let (s, _, body) = api
        .call(
            "POST",
            "/v1/tenants/b/attach",
            Some(&api.admin),
            &[("if-match", &etag)],
            None,
        )
        .await;
    assert_eq!(
        (s, body["error"]["code"].clone()),
        (StatusCode::CONFLICT, json!("resync_not_acknowledged"))
    );
    let (s, ..) = api
        .call(
            "POST",
            "/v1/tenants/b/attach?acknowledge_resync=true",
            Some(&api.admin),
            &[("if-match", &etag)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    for (path, action) in [
        ("/v1/stream/pause", "stream.pause"),
        ("/v1/stream/resume", "stream.resume"),
        ("/v1/reload", "config.reload"),
    ] {
        let (s, _, body) = api.call("POST", path, Some(&api.admin), &[], None).await;
        assert_eq!(s, StatusCode::ACCEPTED, "{path}: {body}");
        let records = api.audit();
        assert_eq!(
            records
                .iter()
                .filter(|r| r.draft.request_id == body["request_id"].as_str().unwrap())
                .count(),
            2,
            "HTTP request id correlates attempt and result"
        );

        assert!(
            api.audit()
                .iter()
                .any(|r| r.draft.action == action && r.draft.outcome == "applied")
        );
    }
    let records = api.audit();
    let of = |rid: &str| -> Vec<&crate::ops::audit::Record> {
        records
            .iter()
            .filter(|r| r.draft.request_id == rid)
            .collect()
    };
    let create = of(&created);
    assert_eq!(create.len(), 2, "attempt and result");
    assert_eq!(
        (
            create[0].draft.phase.as_str(),
            create[1].draft.outcome.as_str()
        ),
        ("attempt", "applied")
    );
    let r = &create[1];
    assert_eq!(r.draft.actor.kind, "api_key");
    assert_eq!(r.draft.actor.id.as_deref(), Some("ops"));
    assert_eq!(r.draft.actor.ip.as_deref(), Some("10.0.0.5"));
    assert_eq!(r.draft.action, "tenant.create");
    assert_eq!(r.draft.tenant.as_deref(), Some("b"));
    assert!(r.ts.ends_with('Z'));
    assert!(r.draft.before.is_null());
    assert_eq!(r.draft.after["ch"]["password"], "***(changed)");
    // Rejections leave one result and no attempt
    let rejected: Vec<_> = records
        .iter()
        .filter(|r| r.draft.outcome == "rejected")
        .collect();
    assert!(rejected.len() >= 3, "exists, stale etag, no ack");
    for r in &rejected {
        assert_eq!(of(&r.draft.request_id).len(), 1);
    }
    let patch = records
        .iter()
        .find(|r| r.draft.action == "tenant.patch" && r.draft.outcome == "applied")
        .unwrap();
    assert_eq!(patch.draft.before["max_lag_bytes"], Value::Null);
    assert_eq!(patch.draft.after["max_lag_bytes"], 99);
    assert_eq!(patch.draft.after["ch"]["password"], "***(changed)");
    // No secret ever reaches the file
    let text = std::fs::read_to_string(api.dir.path().join("api/audit/audit.jsonl")).unwrap();
    assert!(!text.contains("s3cret") && !text.contains("rotated") && !text.contains("hunter2"));
    let (_, _, page) = api.get("/v1/audit?tenant=b", &api.admin).await;
    assert_eq!(page["chain_ok"], true);
    assert!(
        page["records"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["tenant"] == "b")
    );
    // The operation is readable after the fact
    let (s, _, op) = api
        .get(&format!("/v1/operations/{created}"), &api.admin)
        .await;
    assert_eq!((s, op["state"].clone()), (StatusCode::OK, json!("applied")));
}

#[tokio::test]
async fn an_unwritable_audit_log_blocks_the_mutation() {
    use std::os::unix::fs::PermissionsExt;
    let api = Api::new(false, generous()).await;
    let audit = api.dir.path().join("api/audit");
    std::fs::remove_file(audit.join("audit.jsonl")).unwrap();
    std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o500)).unwrap();
    let etag = api.etag("a").await;
    let (s, _, body) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"max_lag_bytes": 5})),
        )
        .await;
    std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        (s, body["error"]["code"].clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json!("audit_unavailable"))
    );
    assert!(!api.frag("a").contains("max_lag_bytes"), "nothing changed");
}

#[tokio::test]
async fn dry_runs_classify_and_change_nothing() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let etag = api.etag("a").await;
    let before = api.frag("a");
    let (s, _, out) = api
        .call(
            "PATCH",
            "/v1/tenants/a?dry_run=true&acknowledge_resync=true",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"ch": {"database": "elsewhere"}})),
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["classification"], "rebind");
    assert_eq!(out["dry_run"], true);
    assert_eq!(api.frag("a"), before);
    assert!(api.audit().iter().all(|r| r.draft.action != "tenant.patch"));
    // A refused change says why
    let (s, _, out) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"ch": {"decoder_batch_size": 3}})),
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::CONFLICT, json!("change_rejected"))
    );
    // If-Match is required
    let (s, ..) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[],
            Some(json!({})),
        )
        .await;
    assert_eq!(s, StatusCode::PRECONDITION_REQUIRED);
}

/// Acceptance 4
#[tokio::test]
async fn rate_limits_refuse_bursts_per_key_and_per_ip() {
    let rates = Rates {
        read: Rate::new(1.0, 2.0),
        mutate: Rate::new(1.0, 1.0),
        auth_failure: Rate::new(1.0 / 60.0, 3.0),
        ..generous()
    };
    let api = Api::new(false, rates).await;
    for _ in 0..2 {
        let (s, ..) = api.get("/v1/whoami", &api.read).await;
        assert_eq!(s, StatusCode::OK);
    }
    let (s, h, body) = api.get("/v1/whoami", &api.read).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["error"]["code"], "rate_limited");
    let retry: u64 = h["retry-after"].to_str().unwrap().parse().unwrap();
    assert!((1..=2).contains(&retry));
    // Another key is unaffected
    let (s, ..) = api.get("/v1/whoami", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    // The bucket refills
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let (s, ..) = api.get("/v1/whoami", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    // Mutations are stricter
    let (s, ..) = api
        .call("POST", "/v1/reload", Some(&api.admin2), &[], None)
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    let (s, ..) = api
        .call("POST", "/v1/reload", Some(&api.admin2), &[], None)
        .await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    // Failed authentication from one address: throttled before auth, even
    // for a valid key
    for _ in 0..3 {
        let (s, ..) = api.call("GET", "/v1/whoami", Some("nope"), &[], None).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
    }
    let (s, ..) = api.get("/v1/whoami", &api.admin).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
}

/// Acceptance 5: intent, then a confirmed delete; restore
#[tokio::test]
async fn deletes_need_a_confirmed_intent_and_can_be_restored() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let spill = tenant_dir(&api.dir.path().join("spill"), "a");
    let etag = api.etag("a").await;
    // No intent: no delete
    let (s, ..) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::PRECONDITION_REQUIRED);
    let (s, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let token = intent["confirm_token"].as_str().unwrap().to_string();
    assert_eq!(intent["impact"]["detaches"], true);
    // A wrong token, and the right one from another key
    let (s, _, body) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", "deadbeef")],
            None,
        )
        .await;
    assert_eq!(
        (s, body["error"]["code"].clone()),
        (StatusCode::CONFLICT, json!("confirmation_invalid"))
    );
    let (s, ..) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin2),
            &[("if-match", &etag), ("x-confirm-delete", &token)],
            None,
        )
        .await;
    assert_eq!(
        s,
        StatusCode::CONFLICT,
        "bound to the key that asked, and now spent"
    );
    // A token, then the tenant changes: void
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let token = intent["confirm_token"].as_str().unwrap().to_string();
    let (s, ..) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"max_lag_bytes": 9})),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    let etag = api.etag("a").await;
    let (s, ..) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", &token)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    // The real thing
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let token = intent["confirm_token"].as_str().unwrap().to_string();
    let (s, _, out) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", &token)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{out}");
    assert!(api.frag("a").is_empty(), "declaration removed");
    assert!(spill.exists(), "replication state kept");
    let (s, ..) = api.get("/v1/tenants/a", &api.read).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (_, _, deleted) = api.get("/v1/tenants-deleted", &api.admin).await;
    assert_eq!(deleted["deleted"][0]["tenant"], "a");
    assert_eq!(deleted["deleted"][0]["deleted_by"], "ops");
    let (s, _, one) = api.get("/v1/tenants-deleted/a", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(one["spec"]["ch"]["password"], "***");
    let tombstones: Vec<_> = std::fs::read_dir(api.dir.path().join("api/deleted"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(tombstones.len(), 1);
    // Restore as active needs the acknowledgement; detached does not
    let (s, ..) = api
        .call(
            "POST",
            "/v1/tenants-deleted/a/restore",
            Some(&api.admin),
            &[],
            Some(json!({"desired": "active"})),
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants-deleted/a/restore",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{out}");
    assert!(api.frag("a").contains("state = \"detached\""));
    assert!(
        api.frag("a").contains("hunter2"),
        "secrets restored with it"
    );
    // Restoring onto a live id
    let (s, ..) = api
        .call(
            "POST",
            "/v1/tenants-deleted/a/restore",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::NOT_FOUND, "the tombstone was consumed");
    // Every step is in the audit log
    let actions: Vec<String> = api.audit().iter().map(|r| r.draft.action.clone()).collect();
    for a in ["tenant.delete_intent", "tenant.delete", "tenant.restore"] {
        assert!(actions.iter().any(|x| x == a), "{a} in {actions:?}");
    }
}

#[tokio::test]
async fn an_identical_recreation_voids_an_older_token() {
    let api = Api::new(false, generous()).await;
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let stale = intent["confirm_token"].as_str().unwrap().to_string();
    let etag = api.etag("a").await;
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let token = intent["confirm_token"].as_str().unwrap().to_string();
    let (s, ..) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", &token)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    // Declared again, identical: same ETag, but the stale token is void
    let (s, ..) = api
        .call(
            "POST",
            "/v1/tenants",
            Some(&api.admin),
            &[],
            Some(json!({"id": "a", "dbname": "a_db", "ch": {"host": "ch", "database": "a", "password": "hunter2"}})),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED);
    let etag2 = api.etag("a").await;
    let (s, ..) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag2), ("x-confirm-delete", &stale)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::CONFLICT);
}

#[test]
fn config_checks_refuse_plaintext_off_loopback() {
    let cfg = |bind: &str, tls: bool, allow: bool| ApiConfig {
        bind: bind.parse().unwrap(),
        keys_file: "/k".into(),
        tls: tls.then(|| ("/c".into(), "/k".into())),
        allow_plaintext: allow,
        state_dir: "/s".into(),
        public_livez: false,
        public_docs: false,
        trusted_proxies: Vec::new(),
        rates: Rates::default(),
        deleted_retention: Duration::from_secs(1),
        ready_grace: Duration::from_secs(1),
        max_in_flight: 1,
        max_connections: 1,
    };
    assert!(cfg("127.0.0.1:9000", false, false).check().is_ok());
    assert!(cfg("[::1]:9000", false, false).check().is_ok());
    let err = cfg("0.0.0.0:9000", false, false)
        .check()
        .unwrap_err()
        .to_string();
    assert!(err.contains("--api-tls-cert"), "{err}");
    assert!(cfg("0.0.0.0:9000", true, false).check().is_ok());
    assert!(cfg("0.0.0.0:9000", false, true).check().is_ok());
}

#[test]
fn trusted_proxies_and_rate_specs_parse() {
    let c = Cidr::parse("10.0.0.0/8").unwrap();
    assert!(c.contains("10.1.2.3".parse().unwrap()));
    assert!(!c.contains("11.0.0.1".parse().unwrap()));
    assert!(c.contains("::ffff:10.0.0.1".parse().unwrap()));
    assert!(!c.contains("::1".parse().unwrap()));
    assert!(Cidr::parse("::1").unwrap().contains("::1".parse().unwrap()));
    assert!(
        Cidr::parse("0.0.0.0/0")
            .unwrap()
            .contains("8.8.8.8".parse().unwrap())
    );
    assert!(Cidr::parse("10.0.0.0/33").is_err());
    assert!(Cidr::parse("nope").is_err());
    let mut r = Rates::default();
    r.set("read=5/s:10").unwrap();
    assert_eq!(r.read, Rate::new(5.0, 10.0));
    for spec in [
        "mutate=1/m:2",
        "expensive=1/s:1",
        "preauth=9/s:9",
        "auth_failure=1/m:1",
    ] {
        r.set(spec).unwrap();
    }
    assert!(r.set("bogus=1/s:1").is_err());
    assert!(r.set("read").is_err());
}

/// The listener over real TCP: plaintext on loopback and TLS
#[tokio::test]
async fn serves_over_tcp_and_tls() {
    let api = Api::new(false, generous()).await;
    let (addr, server) = serve(api.state.clone()).await.unwrap();
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/v1/whoami"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let resp = client
        .get(format!("http://{addr}/v1/whoami"))
        .bearer_auth(&api.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    server.abort();
    // TLS with a throwaway certificate
    let tls_dir = api.dir.path().join("tls");
    std::fs::create_dir_all(&tls_dir).unwrap();
    let Some((ca, cert, key)) = self_signed(&tls_dir) else {
        eprintln!("openssl unavailable; TLS listener not exercised");
        return;
    };
    let mut cfg = (*api.state.cfg).clone();
    cfg.tls = Some((cert, key));
    let state = ApiState {
        cfg: Arc::new(cfg),
        ..api.state.clone()
    };
    let (addr, server) = serve(state).await.unwrap();
    let ca = reqwest::Certificate::from_pem(&std::fs::read(&ca).unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .build()
        .unwrap();
    let resp = client
        .get(format!("https://localhost:{}/v1/whoami", addr.port()))
        .bearer_auth(&api.read)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = client
        .post(format!("https://localhost:{}/v1/reload", addr.port()))
        .bearer_auth(&api.read)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    server.abort();
}

/// A throwaway CA, and a `localhost` leaf it signed: `(ca, leaf, key)`
fn self_signed(dir: &Path) -> Option<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)> {
    let run = |args: &[&str]| -> Option<()> {
        std::process::Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?
            .success()
            .then_some(())
    };
    std::fs::write(
        dir.join("leaf.ext"),
        "basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\n\
         keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n",
    )
    .ok()?;
    run(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=test-ca",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
    ])?;
    run(&[
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
        "leaf.key",
        "-out",
        "leaf.csr",
    ])?;
    run(&[
        "x509",
        "-req",
        "-in",
        "leaf.csr",
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-days",
        "1",
        "-extfile",
        "leaf.ext",
        "-out",
        "leaf.pem",
    ])?;
    Some((
        dir.join("ca.pem"),
        dir.join("leaf.pem"),
        dir.join("leaf.key"),
    ))
}

#[tokio::test]
async fn invalid_mutation_bodies_are_rejected_once_and_audited() {
    let api = Api::new(false, generous()).await;
    let etag = api.etag("a").await;
    let before = api.frag("a");
    for (method, path, body, code) in [
        ("PUT", "/v1/tenants/a", json!([]), "invalid_body"),
        (
            "PUT",
            "/v1/tenants/a",
            json!({"id": "b", "dbname": "a_db"}),
            "invalid_id",
        ),
        (
            "PUT",
            "/v1/tenants/a",
            json!({"dbname": "a_db", "ch": null}),
            "invalid_body",
        ),
        (
            "PUT",
            "/v1/tenants/a",
            json!({"dbname": 42}),
            "invalid_tenant",
        ),
        ("PATCH", "/v1/tenants/a", json!(false), "invalid_body"),
        (
            "PATCH",
            "/v1/tenants/a",
            json!({"ch": {"items": [null]}}),
            "invalid_body",
        ),
        (
            "PATCH",
            "/v1/tenants/a",
            json!({"dbname": null}),
            "invalid_tenant",
        ),
        ("POST", "/v1/tenants", json!({}), "invalid_body"),
        ("POST", "/v1/tenants", json!({"id": "../bad"}), "invalid_id"),
    ] {
        let (s, h, out) = api
            .call(
                method,
                path,
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(body),
            )
            .await;
        assert_eq!(
            (s, out["error"]["code"].clone()),
            (StatusCode::BAD_REQUEST, json!(code)),
            "{method} {path}: {out}"
        );
        let records: Vec<_> = api
            .audit()
            .into_iter()
            .filter(|r| r.draft.request_id == h["x-request-id"].to_str().unwrap())
            .collect();
        assert_eq!(records.len(), 1, "{method} {path}");
        assert_eq!(
            (
                records[0].draft.phase.as_str(),
                records[0].draft.outcome.as_str()
            ),
            ("result", "rejected")
        );
        assert_eq!(records[0].draft.actor.id.as_deref(), Some("ops"));
        assert_eq!(api.frag("a"), before);
    }
    let (s, h, _) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[],
            Some(json!({})),
        )
        .await;
    assert_eq!(s, StatusCode::PRECONDITION_REQUIRED);
    assert_eq!(
        api.audit()
            .iter()
            .filter(|r| r.draft.request_id == h["x-request-id"].to_str().unwrap())
            .count(),
        1
    );
}

#[tokio::test]
async fn pending_rebind_is_refused_by_api_and_socket_after_detach() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let etag = api.etag("a").await;
    let (s, _, out) = api
        .call(
            "PATCH",
            "/v1/tenants/a?acknowledge_resync=true",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"ch": {"database": "b"}})),
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{out}");
    let dir = tenant_dir(&api.dir.path().join("spill"), "a");
    let fence = crate::tenants::RebindFence::load(&dir)
        .await
        .unwrap()
        .unwrap();
    let before = api.frag("a");
    for phase in [Phase::Active, Phase::Detached] {
        let mut state = TenantState::load(&dir).await.unwrap().unwrap();
        state.phase = phase;
        state.store(&dir).await.unwrap();
        let etag = api.etag("a").await;
        let (s, _, out) = api
            .call(
                "PATCH",
                "/v1/tenants/a?acknowledge_resync=true",
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(json!({"ch": {"database": "c"}})),
            )
            .await;
        assert_eq!(
            (s, out["error"]["code"].clone()),
            (StatusCode::CONFLICT, json!("rebind_pending"))
        );
        let (s, _, out) = api
            .call(
                "PATCH",
                "/v1/tenants/a",
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(json!({"ch": {"database": "c"}})),
            )
            .await;
        assert_eq!(
            (s, out["error"]["code"].clone()),
            (StatusCode::CONFLICT, json!("rebind_pending"))
        );
        let body: toml::Table = format!(
            "acknowledge_resync = true\n{}",
            tenant_a().replace("database = 'a'", "database = 'c'")
        )
        .parse()
        .unwrap();
        let err = crate::ops::tenant_ctl::put(
            &api.state.ctx,
            &body,
            &crate::ops::audit::Actor::unix(Some(42)),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("rebind_pending"), "{err}");
        assert_eq!(
            crate::tenants::RebindFence::load(&dir).await.unwrap(),
            Some(fence.clone())
        );
        assert_eq!(api.frag("a"), before);
    }
}

#[tokio::test(start_paused = true)]
async fn expired_delete_and_invalid_restore_leave_the_tenant_unchanged() {
    let api = Api::new(false, generous()).await;
    let etag = api.etag("a").await;
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let token = intent["confirm_token"].as_str().unwrap();
    tokio::time::advance(super::deleted::TOKEN_TTL + Duration::from_secs(1)).await;
    let (s, _, out) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", token)],
            None,
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::GONE, json!("confirmation_expired"))
    );
    assert!(!api.frag("a").is_empty());
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants-deleted/unknown/restore",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::NOT_FOUND, json!("not_found"))
    );
    let root = crate::ch_emitter::load_effective(&api.state.ctx.ch_config, toml::Table::new())
        .await
        .unwrap();
    let decl = crate::tenants::TenantsConfig::from_table(&root)
        .unwrap()
        .unwrap()
        .get("a")
        .unwrap()
        .clone();
    api.state
        .deletes
        .write_tombstone(&decl, "ops", "deleted", None)
        .unwrap();
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants-deleted/a/restore",
            Some(&api.admin),
            &[],
            Some(json!({"desired": "paused"})),
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::BAD_REQUEST, json!("invalid_body"))
    );
    assert_eq!(api.state.deletes.list().len(), 1);
    for (field, value, want) in [
        ("stuck_teardowns", vec!["a".to_string()], "stuck"),
        ("tearing_down", vec!["a".to_string()], "draining"),
    ] {
        api.state
            .ctx
            .metrics
            .update(|snap| {
                snap.stuck_teardowns.clear();
                snap.tearing_down.clear();
                if field == "stuck_teardowns" {
                    snap.stuck_teardowns = value;
                } else {
                    snap.tearing_down = value;
                }
            })
            .await;
        let (s, _, out) = api.get("/v1/tenants-deleted/a", &api.admin).await;
        assert_eq!((s, out["teardown"].clone()), (StatusCode::OK, json!(want)));
        // Health says the same: draining is fine, a stuck writer degrades
        let (_, _, health) = api.get("/v1/health", &api.read).await;
        let tenants = health["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == "tenants")
            .unwrap()
            .clone();
        let (status, detail) = if want == "stuck" {
            ("degraded", "teardown left a writer running for a")
        } else {
            ("ok", "tearing down a")
        };
        assert_eq!(tenants["status"], status, "{tenants}");
        assert!(
            tenants["detail"].as_str().unwrap().contains(detail),
            "{tenants}"
        );
    }
}

#[tokio::test]
async fn single_layout_and_missing_audit_have_explicit_responses() {
    let mut api = Api::new(false, generous()).await;
    std::fs::write(&api.state.ctx.ch_config, "[source]\ndbname = 'postgres'\n").unwrap();
    std::fs::remove_file(crate::ops::tenant_ctl::tenant_fragment_path(
        &api.state.ctx.ch_config,
        "a",
    ))
    .unwrap();
    api.state.ctx.reloader = Arc::new(Reloader::new(
        Some(api.state.ctx.ch_config.clone()),
        toml::Table::new(),
    ));
    api.router = router(api.state.clone());
    let (s, _, out) = api.get("/v1/tenants", &api.admin).await;
    assert_eq!(
        (s, out),
        (StatusCode::OK, json!({"layout": "single", "tenants": []}))
    );
    let (s, _, out) = api.get("/v1/audit", &api.admin).await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json!("audit_unavailable"))
    );
    let (s, _, out) = api.get("/v1/health/ready", &api.admin).await;
    assert!(
        matches!(s, StatusCode::OK | StatusCode::SERVICE_UNAVAILABLE),
        "{out}"
    );
}

#[tokio::test]
async fn trusted_proxy_preauth_and_busy_limits_are_enforced() {
    let mut api = Api::new(false, generous()).await;
    Arc::make_mut(&mut api.state.cfg).trusted_proxies = vec![Cidr::parse("10.0.0.0/8").unwrap()];
    let req = Request::builder()
        .header("x-forwarded-for", "192.0.2.1, 192.0.2.2")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        client_ip(&api.state, "10.0.0.5:4".parse().unwrap(), &req),
        "192.0.2.2".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(
        client_ip(&api.state, "198.51.100.1:4".parse().unwrap(), &req),
        "198.51.100.1".parse::<std::net::IpAddr>().unwrap()
    );
    let bad = Request::builder()
        .header("x-forwarded-for", "bogus")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        client_ip(&api.state, "10.0.0.5:4".parse().unwrap(), &bad),
        "10.0.0.5".parse::<std::net::IpAddr>().unwrap()
    );
    assert!(
        !Cidr::parse("10.0.0.0/8")
            .unwrap()
            .contains("2001:db8::1".parse().unwrap())
    );
    api.state.in_flight = Arc::new(tokio::sync::Semaphore::new(1));
    let held = api.state.in_flight.clone().acquire_owned().await.unwrap();
    api.router = router(api.state.clone());
    let (s, _, out) = api.get("/v1/status", &api.admin).await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json!("busy"))
    );
    drop(held);
    let (s, ..) = api.get("/v1/status", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = api.get("/v1/status?token=bad", &api.admin).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    let mut rates = generous();
    rates.preauth = Rate::new(0.001, 1.0);
    let api = Api::new(false, rates).await;
    let (s, ..) = api.get("/v1/status", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    let (s, h, out) = api.get("/v1/status", &api.admin).await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::TOO_MANY_REQUESTS, json!("rate_limited"))
    );
    assert!(h.contains_key("retry-after"));
}

#[tokio::test]
async fn config_and_socket_failures_are_audited_without_unwanted_writes() {
    use crate::config::{CliOverrides, ConfigResolver, ResolverBoot};
    let api = Api::new(false, generous()).await;
    let actor = crate::ops::audit::Actor::unix(Some(42));
    let unknown: toml::Table = "id = 'unknown'".parse().unwrap();
    let err = crate::ops::tenant_ctl::remove(&api.state.ctx, &unknown, &actor)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not_found"));
    let bad: toml::Table = "id = 'a'\nstate = 'bad'".parse().unwrap();
    let err = crate::ops::tenant_ctl::set_state(&api.state.ctx, &bad, &actor)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("state must be active or detached"));
    assert!(api.audit().iter().any(|r| r.draft.action == "tenant.state"
        && r.draft.outcome == "rejected"
        && r.draft.actor.uid == Some(42)));
    for invalid in [
        toml::Table::new(),
        "[tenant.a]\ndbname = 42".parse().unwrap(),
    ] {
        let before = api.audit().len();
        assert!(
            crate::ops::tenant_ctl::put(&api.state.ctx, &invalid, &actor)
                .await
                .is_err()
        );
        assert_eq!(api.audit().len(), before + 1);
    }
    let before = api.audit().len();
    assert!(
        crate::ops::tenant_ctl::remove(&api.state.ctx, &toml::Table::new(), &actor)
            .await
            .is_err()
    );
    assert_eq!(api.audit().len(), before + 1);

    let active: toml::Table = "id = 'a'\nstate = 'active'".parse().unwrap();
    assert_eq!(
        crate::ops::tenant_ctl::set_state(&api.state.ctx, &active, &actor)
            .await
            .unwrap(),
        "OK\n"
    );
    let before = api.frag("a");
    let err =
        crate::control::apply_fragment(&api.state.ctx, &actor, "config.apply", None, |root| {
            root.insert("database".into(), toml::Value::Table(toml::Table::new()));
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, "invalid_config");
    assert!(!api.state.ctx.fragment.exists());
    let merged = crate::ch_emitter::load_effective(&api.state.ctx.ch_config, toml::Table::new())
        .await
        .unwrap();
    let decl = crate::tenants::TenantsConfig::from_table(&merged)
        .unwrap()
        .unwrap()
        .get("a")
        .unwrap()
        .clone();
    let base =
        crate::ch_emitter::EmitterConfig::from_table(&decl.effective_table(&merged)).unwrap();
    let (resolver, _) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        Some(api.state.ctx.ch_config.clone()),
        toml::Table::new(),
        crate::mapping::mapping_handle(Default::default()),
        ResolverBoot::default(),
    );
    resolver.bind_tenant("a");
    resolver.refuse_reloads(Some(Arc::new(|| {})));
    api.state
        .ctx
        .reloader
        .set_tenant_resolver("a", Some(resolver))
        .await;
    let (s, _, out) = api
        .call("POST", "/v1/reload", Some(&api.admin), &[], None)
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::UNPROCESSABLE_ENTITY, json!("apply_refused"))
    );
    let err = api.state.ctx.reloader.reload().await.unwrap_err();
    assert!(err.to_string().contains("reload tenant a"));
    assert_eq!(api.frag("a"), before);
    assert!(
        api.audit()
            .iter()
            .any(|r| r.draft.action == "config.reload" && r.draft.outcome == "failed")
    );
    use std::os::unix::fs::PermissionsExt;
    let audit = api.dir.path().join("api/audit");
    std::fs::remove_file(audit.join("audit.jsonl")).unwrap();
    std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o500)).unwrap();
    let (s, _, out) = api
        .call("POST", "/v1/stream/pause", Some(&api.admin), &[], None)
        .await;
    std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::SERVICE_UNAVAILABLE, json!("audit_unavailable"))
    );
    assert!(
        !api.state.ctx.fragment.exists(),
        "audit refusal precedes persist"
    );
}

#[tokio::test]
async fn snowflake_views_show_retained_outbox_and_teardown_jobs() {
    let api = Api::new(false, generous()).await;
    let body = format!(
        r#"[tenant.a]
dbname = 'a_db'
[tenant.a.destination]
kind = 'snowflake'
[tenant.a.snowflake]
account_url = 'https://acme-test.snowflakecomputing.com'
user = 'REPLICATOR'
role = 'REPLICATION'
warehouse = 'INGEST'
database = 'DB'
ack_after = 'outbox'
[tenant.a.snowflake.auth]
method = 'oauth'
token_file = '/run/secrets/token'
[tenant.a.snowflake.state]
directory = '{}'
max_bytes = 10737418240
[tenant.a.snowflake.stage]
bucket = 'replication'
prefix = 'a/'
region = 'us-east-1'
name = 'DB.INTERNAL.LOAD_STAGE'
"#,
        api.dir.path().join("sf").display()
    );
    std::fs::write(
        crate::ops::tenant_ctl::tenant_fragment_path(&api.state.ctx.ch_config, "a"),
        body,
    )
    .unwrap();
    api.state
        .ctx
        .metrics
        .update(|snap| {
            snap.stuck_teardowns = vec!["b".into()];
            snap.session_state = "starting";
            snap.uptime_seconds = 3600;
        })
        .await;
    let (s, _, out) = api.get("/v1/tenants/a", &api.admin).await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["destination"]["kind"], "snowflake");
    assert_eq!(out["destination"]["database"], "DB");
    let (s, _, out) = api.get("/v1/jobs", &api.admin).await;
    assert_eq!(s, StatusCode::OK);
    assert!(
        out["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["kind"] == "teardown" && j["state"] == "stuck")
    );
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK, "{out}");
    assert_eq!(out["impact"]["warnings"].as_array().unwrap().len(), 1);
    let (s, _, _) = api.get("/v1/health/ready", &api.admin).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(api.state.service.ctx().ch_config, api.state.ctx.ch_config);
}

#[tokio::test]
async fn raw_request_rejections_use_the_error_envelope_and_authenticate_first() {
    let api = Api::new(false, generous()).await;
    let oversized = "x".repeat((1 << 20) + 1);
    for (method, path, raw, content_type, authenticated, expected) in [
        (
            "POST",
            "/v1/tenants",
            "{",
            Some("application/json"),
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "POST",
            "/v1/tenants/a/detach",
            "{",
            Some("application/json"),
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "PUT",
            "/v1/tenants/a",
            "{}",
            None,
            true,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            "PATCH",
            "/v1/tenants/a?dry_run=bogus",
            "{}",
            Some("application/json"),
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "POST",
            "/v1/tenants",
            oversized.as_str(),
            Some("application/json"),
            true,
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            "POST",
            "/v1/tenants",
            oversized.as_str(),
            Some("application/json"),
            false,
            StatusCode::UNAUTHORIZED,
        ),
    ] {
        let mut req = Request::builder().method(method).uri(path);
        if authenticated {
            req = req.header("authorization", format!("Bearer {}", api.admin));
        }
        if let Some(content_type) = content_type {
            req = req.header("content-type", content_type);
        }
        let res = api
            .router
            .clone()
            .oneshot(req.body(Body::from(raw.to_string())).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), expected, "{method} {path}");
        let headers = res.headers().clone();
        let request_id = res.headers()["x-request-id"].to_str().unwrap().to_string();
        let body = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        api.conformance
            .check(method, path, expected, &headers, &body);
        let out: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(out["error"]["request_id"], request_id);
        assert_eq!(
            out["error"]["code"],
            if authenticated {
                "invalid_request"
            } else {
                "unauthenticated"
            }
        );
        let records: Vec<_> = api
            .audit()
            .into_iter()
            .filter(|r| r.draft.request_id == request_id)
            .collect();
        assert_eq!(records.len(), usize::from(authenticated));
        if authenticated {
            assert_eq!(records[0].draft.outcome, "rejected");
        }
    }
    assert_eq!(api.frag("a"), tenant_a());
}

#[tokio::test]
async fn rejected_dry_runs_never_append_audit_records() {
    let api = Api::new(false, generous()).await;
    let etag = api.etag("a").await;
    for (method, path, body, expected) in [
        (
            "POST",
            "/v1/tenants?dry_run=true",
            json!({}),
            StatusCode::BAD_REQUEST,
        ),
        (
            "PUT",
            "/v1/tenants/a?dry_run=true",
            json!([]),
            StatusCode::BAD_REQUEST,
        ),
        (
            "PATCH",
            "/v1/tenants/a?dry_run=true",
            json!(false),
            StatusCode::BAD_REQUEST,
        ),
        (
            "PATCH",
            "/v1/tenants/a?dry_run=true",
            json!({"ch": {"made_up": 1}}),
            StatusCode::CONFLICT,
        ),
        (
            "POST",
            "/v1/tenants-deleted/unknown/restore?dry_run=true",
            json!({}),
            StatusCode::NOT_FOUND,
        ),
        (
            "POST",
            "/v1/tenants/unknown/attach?dry_run=true",
            json!({}),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let (s, _, out) = api
            .call(
                method,
                path,
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(body),
            )
            .await;
        assert_eq!(s, expected, "{out}");
        assert!(api.audit().is_empty());
        assert_eq!(api.frag("a"), tenant_a());
    }
}

#[tokio::test]
async fn rollback_conflict_and_storage_failure_audit_their_actual_outcomes() {
    use crate::config::{CliOverrides, ConfigResolver, ResolverBoot};
    use std::os::unix::fs::PermissionsExt;
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let root = crate::ch_emitter::load_effective(&api.state.ctx.ch_config, toml::Table::new())
        .await
        .unwrap();
    let decl = crate::tenants::TenantsConfig::from_table(&root)
        .unwrap()
        .unwrap()
        .get("a")
        .unwrap()
        .clone();
    let base = crate::ch_emitter::EmitterConfig::from_table(&decl.effective_table(&root)).unwrap();
    let (resolver, _) = ConfigResolver::new(
        &base,
        CliOverrides::default(),
        Some(api.state.ctx.ch_config.clone()),
        toml::Table::new(),
        crate::mapping::mapping_handle(Default::default()),
        ResolverBoot::default(),
    );
    resolver.bind_tenant("a");
    api.state
        .ctx
        .reloader
        .set_tenant_resolver("a", Some(resolver.clone()))
        .await;
    let path = crate::ops::tenant_ctl::tenant_fragment_path(&api.state.ctx.ch_config, "a");
    let original = api.frag("a");
    resolver.refuse_reloads(Some(Arc::new(|| {})));
    for (want, outcome) in [
        (StatusCode::UNPROCESSABLE_ENTITY, "rolled_back"),
        (StatusCode::CONFLICT, "conflict"),
    ] {
        if want == StatusCode::CONFLICT {
            let path = path.clone();
            let hand = original.replace("dbname = 'a_db'", "dbname = 'a_db'\nmax_lag_bytes = 7");
            resolver.refuse_reloads(Some(Arc::new(move || {
                std::fs::write(&path, &hand).unwrap();
            })));
        }
        let etag = api.etag("a").await;
        let (s, h, out) = api
            .call(
                "PATCH",
                "/v1/tenants/a",
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(json!({"max_lag_bytes": 99})),
            )
            .await;
        assert_eq!(s, want, "{out}");
        let records: Vec<_> = api
            .audit()
            .into_iter()
            .filter(|r| r.draft.request_id == h["x-request-id"].to_str().unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].draft.outcome, outcome);
    }
    assert!(api.frag("a").contains("max_lag_bytes = 7"));
    resolver.refuse_reloads(None);
    std::fs::write(&path, &original).unwrap();
    let etag = api.etag("a").await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
    let (s, h, out) = api
        .call(
            "PATCH",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag)],
            Some(json!({"max_lag_bytes": 99})),
        )
        .await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::INTERNAL_SERVER_ERROR, json!("internal"))
    );
    assert_eq!(api.frag("a"), original);
    let records: Vec<_> = api
        .audit()
        .into_iter()
        .filter(|r| r.draft.request_id == h["x-request-id"].to_str().unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].draft.outcome, "failed");
    std::fs::write(
        &api.state.ctx.ch_config,
        "[source]\npassword = 'hidden'\ninvalid = [",
    )
    .unwrap();
    let (s, _, out) = api.get("/v1/tenants", &api.admin).await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (StatusCode::INTERNAL_SERVER_ERROR, json!("internal"))
    );
    assert!(!out.to_string().contains("hidden"));
}

/// H6: a request dropped while its mutation waits still finishes it: the
/// change applies, the operation record says so, and a delete keeps the
/// tombstone that makes it restorable
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_request_still_finishes_its_mutation() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let etag = api.etag("a").await;
    let (_, _, intent) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    let token = intent["confirm_token"].as_str().unwrap().to_string();
    // The mutation waits on the reload lock while its request goes away
    let held = api.state.ctx.reloader.begin().await;
    let mut req = Request::builder()
        .method("DELETE")
        .uri("/v1/tenants/a")
        .header("authorization", format!("Bearer {}", api.admin))
        .header("if-match", etag.as_str())
        .header("x-confirm-delete", token.as_str())
        .body(Body::empty())
        .unwrap();
    req.extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 5], 4000))));
    let router = api.router.clone();
    let request = tokio::spawn(async move { router.oneshot(req).await });
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let op = loop {
        if let Some(id) = api.state.ops.running("tenant.delete").pop() {
            break id;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the delete never started"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    drop(held);
    loop {
        if api.state.ops.get(&op).unwrap()["state"] == "applied" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the dropped delete was left running: {:?}",
            api.state.ops.get(&op)
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(api.frag("a").is_empty(), "the tenant is gone");
    assert!(
        api.state.deletes.latest("a").is_some(),
        "and restorable from its tombstone"
    );
    let results: Vec<_> = api
        .audit()
        .into_iter()
        .filter(|r| r.draft.request_id == op && r.draft.phase == "result")
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].draft.outcome, "applied");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_config_requests_finish_their_operation_records() {
    let api = Api::new(false, generous()).await;
    for (path, action) in [
        ("/v1/stream/pause", "stream.pause"),
        ("/v1/stream/resume", "stream.resume"),
        ("/v1/reload", "config.reload"),
    ] {
        let held = api.state.ctx.reloader.begin().await;
        let mut req = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", format!("Bearer {}", api.admin))
            .body(Body::empty())
            .unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([10, 0, 0, 5], 4000))));
        let router = api.router.clone();
        let request = tokio::spawn(async move { router.oneshot(req).await });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let op = loop {
            if let Some(id) = api.state.ops.running(action).pop() {
                break id;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{action} has no operation"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        drop(held);
        loop {
            let (_, _, operation) = api.get(&format!("/v1/operations/{op}"), &api.admin).await;
            if operation["state"] == "applied" {
                assert_eq!(operation["action"], action);
                assert_eq!(
                    operation["generation"],
                    api.state.ctx.reloader.current().generation
                );
                break;
            }
            assert!(std::time::Instant::now() < deadline, "{operation}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let results: Vec<_> = api
            .audit()
            .into_iter()
            .filter(|r| r.draft.request_id == op && r.draft.phase == "result")
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].draft.outcome, "applied");
    }

    std::fs::create_dir(api.state.ctx.fragment.with_extension("toml.tmp")).unwrap();
    let (status, headers, _) = api
        .call("POST", "/v1/stream/pause", Some(&api.admin), &[], None)
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let id = headers["x-request-id"].to_str().unwrap();
    let (_, _, operation) = api.get(&format!("/v1/operations/{id}"), &api.admin).await;
    assert_eq!(operation["state"], "rejected");
    assert_eq!(operation["error"]["code"], "internal");
}

/// H4: a detach whose detached state cannot be recorded changes nothing.
/// The tenant stays attached and its config untouched; the API answers 503
/// and the socket an error, each audited as rejected. Once the spill
/// directory is writable again, the caller's retry detaches it
#[tokio::test]
async fn an_unrecordable_detach_changes_nothing() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let dir = tenant_dir(&api.dir.path().join("spill"), "a");
    // The state write goes through `tenant.toml.tmp`; a directory there
    // makes it fail
    std::fs::create_dir(dir.join("tenant.toml.tmp")).unwrap();
    let before = api.frag("a");
    let generation = api.state.ctx.reloader.current().generation;
    let etag = api.etag("a").await;
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants/a/detach",
            Some(&api.admin),
            &[("if-match", &etag)],
            None,
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!("detach_not_recorded")
        )
    );
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("stays attached"),
        "{out}"
    );
    let body: toml::Table = "id = 'a'\nstate = 'detached'\n".parse().unwrap();
    let err = crate::ops::tenant_ctl::set_state(
        &api.state.ctx,
        &body,
        &crate::ops::audit::Actor::unix(Some(42)),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("detach_not_recorded"), "{err}");
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants/a/delete-intent",
            Some(&api.admin),
            &[],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::OK);
    let token = out["confirm_token"].as_str().unwrap().to_string();
    let (s, _, out) = api
        .call(
            "DELETE",
            "/v1/tenants/a",
            Some(&api.admin),
            &[("if-match", &etag), ("x-confirm-delete", &token)],
            None,
        )
        .await;
    assert_eq!(
        (s, out["error"]["code"].clone()),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!("detach_not_recorded")
        )
    );
    assert!(
        api.state.deletes.latest("a").is_none(),
        "a delete that did not happen leaves no tombstone"
    );
    assert_eq!(api.frag("a"), before, "config untouched");
    assert_eq!(
        TenantState::load(&dir).await.unwrap().unwrap().phase,
        Phase::Active,
        "still attached"
    );
    assert_eq!(
        api.state.ctx.reloader.current().generation,
        generation,
        "nothing published"
    );
    let results: Vec<_> = api
        .audit()
        .into_iter()
        .filter(|r| r.draft.phase == "result" && r.draft.error.is_some())
        .collect();
    assert_eq!(results.len(), 3, "{results:?}");
    assert!(results.iter().all(|r| r.draft.outcome == "rejected"));
    assert!(results.iter().all(|r| {
        r.draft
            .error
            .as_deref()
            .unwrap()
            .contains("detach_not_recorded")
            || r.draft
                .error
                .as_deref()
                .unwrap()
                .contains("could not record the detach")
    }));
    // Writable again: the retry detaches
    std::fs::remove_dir(dir.join("tenant.toml.tmp")).unwrap();
    let (s, _, out) = api
        .call(
            "POST",
            "/v1/tenants/a/detach",
            Some(&api.admin),
            &[("if-match", &etag)],
            None,
        )
        .await;
    assert_eq!(s, StatusCode::ACCEPTED, "{out}");
    let state = TenantState::load(&dir).await.unwrap().unwrap();
    assert_eq!(state.phase, Phase::Detached);
    assert!(
        state
            .reason
            .unwrap()
            .starts_with("requested by tenant.detach"),
        "recorded before the config changed"
    );
}

/// `ctl tenant remove` of a tenant that exists removes its declaration and
/// audits it like the API does
#[tokio::test]
async fn the_socket_removes_an_existing_tenant() {
    let api = Api::new(false, generous()).await;
    let body: toml::Table = "id = 'a'\n".parse().unwrap();
    crate::ops::tenant_ctl::remove(
        &api.state.ctx,
        &body,
        &crate::ops::audit::Actor::unix(Some(42)),
    )
    .await
    .unwrap();
    assert!(api.frag("a").is_empty());
    let applied: Vec<_> = api
        .audit()
        .into_iter()
        .filter(|r| r.draft.action == "tenant.remove" && r.draft.phase == "result")
        .collect();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].draft.outcome, "applied");
    assert_eq!(applied[0].draft.actor.kind, "unix");
}
