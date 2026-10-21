//! Route handlers. Reads take a snapshot and the config; mutations run in a
//! spawned task through [`TenantService`], so a dropped connection never
//! cancels one midway, and record their outcome under
//! `/v1/operations/{request_id}`.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use axum::Json;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, delete, get, patch, post, put};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::routes::Route;
use super::{ApiState, RequestCtx, error_response};
use crate::ops::redact::{json_to_toml, redact_toml, toml_to_json};
use crate::ops::tenant_service::{Mutation, Outcome, Precondition, ServiceError, ServiceResult};
use crate::tenants::{Desired, TenantDecl, TenantState, TenantsConfig, tenant_dir};

type Reply = std::result::Result<Response, ApiError>;

/// A refusal, rendered as the API's error envelope
#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
    request_id: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        error_response(self.status, &self.code, &self.message, &self.request_id)
    }
}

pub fn method_router(r: &Route) -> MethodRouter<ApiState> {
    match (r.method, r.path) {
        ("GET", "/livez") => get(livez),
        ("GET", "/v1/whoami") => get(whoami),
        ("GET", "/v1/health/live") => get(health_live),
        ("GET", "/v1/health/ready") => get(health_ready),
        ("GET", "/v1/health") => get(health),
        ("GET", "/v1/status") => get(status),
        ("GET", "/v1/metrics") => get(metrics),
        ("GET", "/v1/errors") => get(errors),
        ("GET", "/v1/crashes") => get(crashes),
        ("GET", "/v1/crashes/{id}") => get(crash),
        ("GET", "/v1/jobs") => get(jobs),
        ("GET", "/v1/tenants") => get(tenants),
        ("GET", "/v1/tenants/{id}") => get(tenant),
        ("GET", "/v1/tenants/{id}/jobs") => get(tenant_jobs),
        ("GET", "/v1/tenants/{id}/errors") => get(tenant_errors),
        ("GET", "/v1/tenants/{id}/tables") => get(tenant_tables),
        ("GET", "/v1/config") => get(config),
        ("GET", "/v1/audit") => get(audit),
        ("GET", "/v1/operations/{request_id}") => get(operation),
        ("GET", "/v1/tenants-deleted") => get(deleted_list),
        ("GET", "/v1/tenants-deleted/{id}") => get(deleted_one),
        ("POST", "/v1/tenants") => post(create),
        ("PUT", "/v1/tenants/{id}") => put(replace),
        ("PATCH", "/v1/tenants/{id}") => patch(patch_tenant),
        ("POST", "/v1/tenants/{id}/detach") => post(detach),
        ("POST", "/v1/tenants/{id}/attach") => post(attach),
        ("POST", "/v1/tenants/{id}/delete-intent") => post(delete_intent),
        ("DELETE", "/v1/tenants/{id}") => delete(delete_tenant),
        ("POST", "/v1/tenants-deleted/{id}/restore") => post(restore),
        ("POST", "/v1/stream/pause") => post(pause),
        ("POST", "/v1/stream/resume") => post(resume),
        ("POST", "/v1/reload") => post(reload),
        (method, path) => panic!("route {method} {path} has no handler"),
    }
}

// ---- helpers ----------------------------------------------------------------

fn fail(ctx: &RequestCtx, status: u16, code: &str, message: impl std::fmt::Display) -> ApiError {
    ApiError {
        status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        code: code.to_string(),
        message: message.to_string(),
        request_id: ctx.request_id.clone(),
    }
}

fn service(ctx: &RequestCtx, e: ServiceError) -> ApiError {
    fail(ctx, e.status, e.code, e.message)
}

fn internal(ctx: &RequestCtx, e: impl std::fmt::Display) -> ApiError {
    fail(ctx, 500, "internal", e)
}

async fn root(st: &ApiState, ctx: &RequestCtx) -> std::result::Result<toml::Table, ApiError> {
    crate::ch_emitter::load_effective(&st.ctx.ch_config, st.ctx.cli_base.clone())
        .await
        .map_err(|e| internal(ctx, e))
}

fn tenants_of(
    ctx: &RequestCtx,
    root: &toml::Table,
) -> std::result::Result<Option<TenantsConfig>, ApiError> {
    TenantsConfig::from_table(root).map_err(|e| fail(ctx, 409, "invalid_config", format!("{e:#}")))
}

fn if_match(ctx: &RequestCtx, headers: &HeaderMap) -> std::result::Result<String, ApiError> {
    let raw = headers
        .get(header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            fail(
                ctx,
                428,
                "precondition_required",
                "send If-Match with the tenant's ETag",
            )
        })?;
    Ok(raw
        .trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct MutationQuery {
    dry_run: bool,
    acknowledge_resync: bool,
}

/// A body field or query parameter
fn flag(body: &Value, name: &str, query: bool) -> bool {
    query || body.get(name).and_then(Value::as_bool).unwrap_or(false)
}

fn decl_from_json(
    ctx: &RequestCtx,
    id: &str,
    body: &Value,
) -> std::result::Result<TenantDecl, ApiError> {
    let Value::Object(map) = body else {
        return Err(fail(
            ctx,
            400,
            "invalid_body",
            "the body must be a JSON object",
        ));
    };
    let mut map = map.clone();
    map.remove("acknowledge_resync");
    if let Some(given) = map.remove("id")
        && given.as_str() != Some(id)
    {
        return Err(fail(
            ctx,
            400,
            "invalid_id",
            "the body names another tenant",
        ));
    }
    let table = match json_to_toml(&Value::Object(map)) {
        Ok(toml::Value::Table(t)) => t,
        Ok(_) => unreachable!("an object converts to a table"),
        Err(e) => return Err(fail(ctx, 400, "invalid_body", e)),
    };
    TenantDecl::parse(id, &table).map_err(|e| fail(ctx, 400, "invalid_tenant", format!("{e:#}")))
}

/// A tenant's `[tenant.<id>]` table, as JSON, secrets included: never sent
fn decl_json(decl: &TenantDecl) -> Value {
    let fragment = decl.to_fragment();
    let body = fragment
        .get("tenant")
        .and_then(|t| t.get(&decl.id))
        .cloned()
        .unwrap_or(toml::Value::Table(toml::Table::new()));
    toml_to_json(&body)
}

/// RFC 7396
fn merge_patch(target: &mut Value, patch: &Value) {
    let Value::Object(p) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = Value::Object(Map::new());
    }
    let t = target.as_object_mut().expect("just made an object");
    for (k, v) in p {
        if v.is_null() {
            t.remove(k);
        } else {
            merge_patch(t.entry(k.clone()).or_insert(Value::Null), v);
        }
    }
}

/// In-flight and recent mutations, newest last, for clients that timed out
#[derive(Default)]
pub struct Operations {
    recent: Mutex<VecDeque<(String, Value)>>,
}

const OPERATIONS_KEPT: usize = 1000;

impl Operations {
    fn set(&self, request_id: &str, value: Value) {
        let mut recent = self.recent.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(slot) = recent.iter_mut().find(|(id, _)| id == request_id) {
            slot.1 = value;
            return;
        }
        if recent.len() >= OPERATIONS_KEPT {
            recent.pop_front();
        }
        recent.push_back((request_id.to_string(), value));
    }

    pub fn get(&self, request_id: &str) -> Option<Value> {
        self.recent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(id, _)| id == request_id)
            .map(|(_, v)| v.clone())
    }
}

fn outcome_json(out: &Outcome) -> Value {
    json!({
        "request_id": out.request_id,
        "tenant": out.tenant,
        "classification": out.change.label(),
        "reasons": match &out.change {
            crate::tenants::Change::Rebind { reasons, .. } => reasons.clone(),
            _ => Vec::new(),
        },
        "etag": out.etag,
        "epoch": out.epoch,
        "generation": out.generation,
        "dry_run": out.dry_run,
    })
}

struct Ask {
    id: String,
    action: &'static str,
    precondition: Precondition,
    acknowledge_resync: bool,
    dry_run: bool,
    method: &'static str,
    path: String,
}

/// Run one tenant mutation off the request
async fn mutate<F>(st: &ApiState, ctx: &RequestCtx, ask: Ask, edit: F) -> Reply
where
    F: FnOnce(Option<&TenantDecl>) -> ServiceResult<Option<TenantDecl>> + Send + 'static,
{
    let m = Mutation {
        request_id: ctx.request_id.clone(),
        actor: ctx.actor(),
        action: ask.action.into(),
        method: Some(ask.method.into()),
        path: Some(ask.path.clone()),
        precondition: ask.precondition,
        acknowledge_resync: ask.acknowledge_resync,
        legacy_ack: false,
        dry_run: ask.dry_run,
    };
    st.ops.set(
        &ctx.request_id,
        json!({"request_id": ctx.request_id, "state": "running", "tenant": ask.id, "action": ask.action}),
    );
    let svc = st.service.clone();
    let id = ask.id.clone();
    let res = tokio::spawn(async move { svc.mutate(&id, &m, edit).await })
        .await
        .map_err(|e| internal(ctx, e))?;
    match res {
        Ok(out) => {
            if !out.dry_run {
                st.deletes.invalidate(&ask.id);
            }
            let mut body = outcome_json(&out);
            body["state"] = "applied".into();
            st.ops.set(&ctx.request_id, body.clone());
            let status = if out.dry_run {
                StatusCode::OK
            } else {
                StatusCode::ACCEPTED
            };
            let mut resp = (status, Json(body)).into_response();
            if let Some(etag) = &out.etag
                && let Ok(v) = HeaderValue::from_str(&format!("\"{etag}\""))
            {
                resp.headers_mut().insert(header::ETAG, v);
            }
            Ok(resp)
        }
        Err(e) => {
            st.ops.set(
                &ctx.request_id,
                json!({
                    "request_id": ctx.request_id,
                    "state": match e.code {
                        "apply_refused" => "rolled_back",
                        "conflict_after_failed_apply" => "conflict",
                        _ => "rejected",
                    },
                    "tenant": ask.id,
                    "action": ask.action,
                    "error": {"code": e.code, "message": e.message},
                }),
            );
            Err(service(ctx, e))
        }
    }
}

fn not_found(ctx: &RequestCtx) -> ApiError {
    fail(ctx, 404, "not_found", "no such tenant")
}

// ---- reads ------------------------------------------------------------------

async fn livez() -> &'static str {
    "ok"
}

async fn whoami(Extension(ctx): Extension<RequestCtx>) -> Json<Value> {
    Json(json!({"principal": ctx.principal, "request_id": ctx.request_id}))
}

async fn health_live() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn health_report(st: &ApiState) -> super::health::Report {
    let snap = st.ctx.metrics.snapshot().await;
    let paused = crate::control::status_table(&st.ctx)
        .await
        .ok()
        .and_then(|t| t.get("paused").and_then(toml::Value::as_bool))
        .unwrap_or(false);
    super::health::evaluate(
        &st.health,
        &super::health::Inputs {
            snap: &snap,
            age: st.ctx.metrics.age(),
            paused,
            published_generation: st.ctx.reloader.current().generation,
            keys: st.keys.health(),
            ready_grace: st.cfg.ready_grace,
        },
    )
}

async fn health_ready(State(st): State<ApiState>) -> Response {
    let report = health_report(&st).await;
    let status = if report.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(json!({"ready": report.ready, "status": report.status})),
    )
        .into_response()
}

async fn health(State(st): State<ApiState>) -> Json<Value> {
    Json(serde_json::to_value(health_report(&st).await).unwrap_or_default())
}

async fn status(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    let table = crate::control::status_table(&st.ctx)
        .await
        .map_err(|e| internal(&ctx, format!("{e:#}")))?;
    let mut body = toml_to_json(&toml::Value::Table(table));
    let snap = st.ctx.metrics.snapshot().await;
    let rev = st.ctx.reloader.current();
    body["epoch"] = rev.epoch.to_string().into();
    body["published_generation"] = rev.generation.into();
    body["reconciled_generation"] = snap.reconciled_generation.into();
    body["session_state"] = snap.session_state.into();
    Ok(Json(body).into_response())
}

async fn metrics(State(st): State<ApiState>) -> Response {
    let body = crate::metrics::render(st.ctx.metrics.snapshot().await);
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct ErrorsQuery {
    level: Option<String>,
    tenant: Option<String>,
    target: Option<String>,
    since_seq: Option<u64>,
    limit: usize,
}

async fn errors(Query(q): Query<ErrorsQuery>) -> Json<Value> {
    let events = crate::ops::error_ring::recent(&crate::ops::error_ring::Filter {
        level: q.level,
        tenant: q.tenant,
        target: q.target,
        since_seq: q.since_seq,
        limit: q.limit,
    });
    Json(json!({"events": events}))
}

async fn crashes(State(st): State<ApiState>) -> Json<Value> {
    let records = st.crashes.as_ref().map(|c| c.list()).unwrap_or_default();
    // The list carries no backtraces; one record does
    let records: Vec<Value> = records
        .into_iter()
        .map(|mut r| {
            r.backtrace = None;
            serde_json::to_value(r).unwrap_or_default()
        })
        .collect();
    Json(json!({"crashes": records}))
}

async fn crash(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    st.crashes
        .as_ref()
        .and_then(|c| c.get(&id))
        .map(|r| Json(r).into_response())
        .ok_or_else(|| fail(&ctx, 404, "not_found", "no such crash record"))
}

fn tenant_jobs_json(snap: &crate::metrics::MetricsSnapshot, id: &str) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(t) = snap.tenants.iter().find(|t| t.id == id) {
        if t.phase == "priming" {
            out.push(json!({"kind": "priming", "tenant": id, "state": "running"}));
        }
        for j in &t.jobs {
            let mut v = serde_json::to_value(j).unwrap_or_default();
            v["tenant"] = id.into();
            out.push(v);
        }
    }
    if snap.tearing_down.iter().any(|t| t == id) {
        out.push(json!({"kind": "teardown", "tenant": id, "state": "running"}));
    }
    if snap.stuck_teardowns.iter().any(|t| t == id) {
        out.push(json!({"kind": "teardown", "tenant": id, "state": "stuck"}));
    }
    out
}

async fn jobs(State(st): State<ApiState>) -> Json<Value> {
    let snap = st.ctx.metrics.snapshot().await;
    let mut ids: Vec<String> = snap.tenants.iter().map(|t| t.id.clone()).collect();
    ids.extend(snap.tearing_down.iter().cloned());
    ids.extend(snap.stuck_teardowns.iter().cloned());
    ids.sort();
    ids.dedup();
    let jobs: Vec<Value> = ids
        .iter()
        .flat_map(|id| tenant_jobs_json(&snap, id))
        .collect();
    Json(json!({"jobs": jobs, "backfills_pending": snap.config_backfills_pending}))
}

/// Where a tenant's destination is, without credentials
fn destination_summary(decl: &TenantDecl, root: &toml::Table) -> Value {
    let eff = decl.effective_table(root);
    match crate::destination::config::DestinationConfig::from_table(&eff) {
        Ok(d) => match d.snowflake {
            Some(sf) => json!({
                "kind": "snowflake",
                "account_url": sf.account_url,
                "database": sf.database,
            }),
            None => {
                let scopes = crate::ch_emitter::EmitterConfig::from_table(&eff)
                    .map(|cfg| crate::tenants::clickhouse_scopes(&cfg))
                    .unwrap_or_default();
                json!({
                    "kind": "clickhouse",
                    "databases": scopes
                        .into_iter()
                        .map(|(endpoint, db)| format!("{endpoint}/{db}"))
                        .collect::<Vec<_>>(),
                })
            }
        },
        Err(e) => json!({"error": format!("{e:#}")}),
    }
}

async fn tenant_json(
    st: &ApiState,
    decl: &TenantDecl,
    root: &toml::Table,
    snap: &crate::metrics::MetricsSnapshot,
    detail: bool,
) -> Value {
    let live = snap.tenants.iter().find(|t| t.id == decl.id);
    let persisted = match &st.ctx.spill_dir {
        Some(spill) => TenantState::load(&tenant_dir(spill, &decl.id))
            .await
            .ok()
            .flatten(),
        None => None,
    };
    let phase = if snap.stuck_teardowns.contains(&decl.id) || snap.tearing_down.contains(&decl.id) {
        "detaching"
    } else {
        live.map_or(
            if decl.desired == Desired::Detached {
                "detached"
            } else {
                "pending"
            },
            |t| t.phase,
        )
    };
    let lsn = |n: u64| walrus::pg::backup::format_pg_lsn(n).to_string();
    let mut status = json!({
        "phase": phase,
        "stale": st.ctx.metrics.age().is_none_or(|a| a > super::health::SNAPSHOT_STALE),
    });
    if let Some(t) = live {
        status["lag_bytes"] = t.lag_bytes.into();
        status["ack_lsn"] = lsn(t.ack_lsn).into();
        status["resume_safe_lsn"] = lsn(t.resume_safe_lsn).into();
        status["queue_depth"] = t.queue_depth.into();
        status["xacts_active"] = t.xacts_active.into();
        status["rows_emitted"] = t.rows_emitted.into();
        status["backfill_copy_rows"] = t.backfill_copy_rows.into();
        status["backfills_pending"] = t.backfills_pending.into();
    }
    if let Some(p) = &persisted {
        status["persisted_phase"] = serde_json::to_value(p.phase).unwrap_or_default();
        status["reason"] = p.reason.clone().into();
        status["attached_lsn"] = lsn(p.attached_lsn).into();
        status["start_lsn"] = lsn(p.start_lsn).into();
        status["db_oid"] = p.db_oid.into();
        status["incarnation"] = p.incarnation.clone().into();
    }
    let mut out = json!({
        "id": decl.id,
        "dbname": decl.dbname,
        "desired": match decl.desired {
            Desired::Active => "active",
            Desired::Detached => "detached",
        },
        "initial_load": decl.initial_load,
        "max_lag_bytes": decl.max_lag_bytes,
        "etag": st.service.etag(decl),
        "destination": destination_summary(decl, root),
        "status": status,
    });
    if detail {
        out["spec"] = redact_toml(&decl.body);
        out["jobs"] = Value::Array(tenant_jobs_json(snap, &decl.id));
    }
    out
}

async fn tenants(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    let root = root(&st, &ctx).await?;
    let Some(cfg) = tenants_of(&ctx, &root)? else {
        return Ok(Json(json!({"layout": "single", "tenants": []})).into_response());
    };
    let snap = st.ctx.metrics.snapshot().await;
    let mut out = Vec::new();
    for decl in &cfg.decls {
        out.push(tenant_json(&st, decl, &root, &snap, false).await);
    }
    Ok(Json(json!({
        "layout": "tenants",
        "epoch": st.ctx.reloader.current().epoch.to_string(),
        "reconciled_generation": snap.reconciled_generation,
        "tenants": out,
    }))
    .into_response())
}

async fn find_tenant(
    st: &ApiState,
    ctx: &RequestCtx,
    id: &str,
) -> std::result::Result<(TenantDecl, toml::Table), ApiError> {
    let root = root(st, ctx).await?;
    let decl = tenants_of(ctx, &root)?
        .and_then(|t| t.get(id).cloned())
        .ok_or_else(|| not_found(ctx))?;
    Ok((decl, root))
}

async fn tenant(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    let (decl, root) = find_tenant(&st, &ctx, &id).await?;
    let snap = st.ctx.metrics.snapshot().await;
    let body = tenant_json(&st, &decl, &root, &snap, true).await;
    let mut resp = Json(body).into_response();
    if let Some(etag) = st.service.etag(&decl)
        && let Ok(v) = HeaderValue::from_str(&format!("\"{etag}\""))
    {
        resp.headers_mut().insert(header::ETAG, v);
    }
    Ok(resp)
}

async fn tenant_jobs(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    find_tenant(&st, &ctx, &id).await?;
    let snap = st.ctx.metrics.snapshot().await;
    Ok(Json(json!({"jobs": tenant_jobs_json(&snap, &id)})).into_response())
}

async fn tenant_errors(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<ErrorsQuery>,
) -> Reply {
    let (decl, _) = find_tenant(&st, &ctx, &id).await?;
    let events = crate::ops::error_ring::recent(&crate::ops::error_ring::Filter {
        level: q.level,
        tenant: Some(decl.id.clone()),
        target: q.target,
        since_seq: q.since_seq,
        limit: q.limit,
    });
    let reason = match &st.ctx.spill_dir {
        Some(spill) => TenantState::load(&tenant_dir(spill, &id))
            .await
            .ok()
            .flatten()
            .and_then(|s| s.reason),
        None => None,
    };
    Ok(Json(json!({"tenant": id, "detach_reason": reason, "events": events})).into_response())
}

async fn tenant_tables(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    find_tenant(&st, &ctx, &id).await?;
    let table = crate::control::tenant_tables(&st.ctx, &id)
        .await
        .map_err(|e| fail(&ctx, 503, "source_unavailable", format!("{e:#}")))?;
    Ok(Json(toml_to_json(&toml::Value::Table(table))).into_response())
}

async fn config(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    crate::control::config_redacted(&st.ctx)
        .await
        .map(|v| Json(v).into_response())
        .map_err(|e| internal(&ctx, format!("{e:#}")))
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct AuditQuery {
    since_seq: Option<u64>,
    tenant: Option<String>,
    action: Option<String>,
    limit: usize,
}

async fn audit(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Query(q): Query<AuditQuery>,
) -> Reply {
    let Some(log) = st.ctx.reloader.audit().cloned() else {
        return Err(fail(&ctx, 503, "audit_unavailable", "no audit log"));
    };
    let filter = crate::ops::audit::Filter {
        since_seq: q.since_seq,
        tenant: q.tenant,
        action: q.action,
        limit: q.limit,
    };
    let page = tokio::task::spawn_blocking(move || log.read(&filter))
        .await
        .map_err(|e| internal(&ctx, e))?
        .map_err(|e| internal(&ctx, format!("{e:#}")))?;
    Ok(Json(page).into_response())
}

async fn operation(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(request_id): Path<String>,
) -> Reply {
    st.ops
        .get(&request_id)
        .map(|v| Json(v).into_response())
        .ok_or_else(|| fail(&ctx, 404, "not_found", "no such operation"))
}

fn teardown_state(
    snap: &crate::metrics::MetricsSnapshot,
    id: &str,
    declared: bool,
) -> &'static str {
    if snap.stuck_teardowns.iter().any(|t| t == id) {
        "stuck"
    } else if snap.tearing_down.iter().any(|t| t == id) {
        "draining"
    } else if snap.tenants.iter().any(|t| t.id == id) && !declared {
        "pending"
    } else {
        "done"
    }
}

async fn deleted_list(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    let root = root(&st, &ctx).await?;
    let declared = tenants_of(&ctx, &root)?;
    let snap = st.ctx.metrics.snapshot().await;
    let list: Vec<Value> = st
        .deletes
        .list()
        .into_iter()
        .map(|(t, _)| {
            let live = declared
                .as_ref()
                .is_some_and(|d| d.get(&t.tenant).is_some());
            let mut v = serde_json::to_value(&t).unwrap_or_default();
            v["teardown"] = teardown_state(&snap, &t.tenant, live).into();
            v["redeclared"] = live.into();
            v
        })
        .collect();
    Ok(Json(json!({"deleted": list})).into_response())
}

async fn deleted_one(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    let (t, decl) = st
        .deletes
        .latest(&id)
        .ok_or_else(|| fail(&ctx, 404, "not_found", "no tombstone for that tenant"))?;
    let root = root(&st, &ctx).await?;
    let live = tenants_of(&ctx, &root)?.is_some_and(|d| d.get(&id).is_some());
    let snap = st.ctx.metrics.snapshot().await;
    let mut v = serde_json::to_value(&t).unwrap_or_default();
    v["teardown"] = teardown_state(&snap, &id, live).into();
    v["redeclared"] = live.into();
    v["spec"] = redact_toml(&decl.body);
    Ok(Json(v).into_response())
}

// ---- mutations ----------------------------------------------------------------

async fn create(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Query(q): Query<MutationQuery>,
    Json(body): Json<Value>,
) -> Reply {
    let id = body
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| fail(&ctx, 400, "invalid_body", "the body needs an id"))?
        .to_string();
    crate::tenants::validate_id(&id)
        .map_err(|e| fail(&ctx, 400, "invalid_id", format!("{e:#}")))?;
    let decl = decl_from_json(&ctx, &id, &body)?;
    let ask = Ask {
        id,
        action: "tenant.create",
        precondition: Precondition::Absent,
        acknowledge_resync: flag(&body, "acknowledge_resync", q.acknowledge_resync),
        dry_run: q.dry_run,
        method: "POST",
        path: "/v1/tenants".into(),
    };
    mutate(&st, &ctx, ask, move |_| Ok(Some(decl))).await
}

async fn replace(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<MutationQuery>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Reply {
    let etag = if_match(&ctx, &headers)?;
    let decl = decl_from_json(&ctx, &id, &body)?;
    let ask = Ask {
        path: format!("/v1/tenants/{id}"),
        id,
        action: "tenant.update",
        precondition: Precondition::Etag(etag),
        acknowledge_resync: flag(&body, "acknowledge_resync", q.acknowledge_resync),
        dry_run: q.dry_run,
        method: "PUT",
    };
    mutate(&st, &ctx, ask, move |_| Ok(Some(decl))).await
}

async fn patch_tenant(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<MutationQuery>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Reply {
    let etag = if_match(&ctx, &headers)?;
    if !body.is_object() {
        return Err(fail(
            &ctx,
            400,
            "invalid_body",
            "a merge patch is a JSON object",
        ));
    }
    let ack = flag(&body, "acknowledge_resync", q.acknowledge_resync);
    let mut patch = body;
    if let Some(map) = patch.as_object_mut() {
        map.remove("acknowledge_resync");
    }
    let tenant = id.clone();
    let ask = Ask {
        path: format!("/v1/tenants/{id}"),
        id,
        action: "tenant.patch",
        precondition: Precondition::Etag(etag),
        acknowledge_resync: ack,
        dry_run: q.dry_run,
        method: "PATCH",
    };
    mutate(&st, &ctx, ask, move |current| {
        let current =
            current.ok_or_else(|| ServiceError::new(404, "not_found", "no such tenant"))?;
        let mut doc = decl_json(current);
        merge_patch(&mut doc, &patch);
        let table = match json_to_toml(&doc) {
            Ok(toml::Value::Table(t)) => t,
            Ok(_) => {
                return Err(ServiceError::new(
                    400,
                    "invalid_body",
                    "the patch removed the tenant",
                ));
            }
            Err(e) => return Err(ServiceError::new(400, "invalid_body", e)),
        };
        TenantDecl::parse(&tenant, &table)
            .map(Some)
            .map_err(|e| ServiceError::new(400, "invalid_tenant", format!("{e:#}")))
    })
    .await
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateBody {
    reason: Option<String>,
    acknowledge_resync: bool,
    desired: Option<String>,
}

async fn set_desired(
    st: &ApiState,
    ctx: &RequestCtx,
    id: String,
    headers: &HeaderMap,
    q: MutationQuery,
    body: Option<Json<StateBody>>,
    desired: Desired,
) -> Reply {
    let etag = if_match(ctx, headers)?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    if let Some(reason) = &body.reason {
        tracing::info!(target: "walshadow::api", tenant = %id, reason = %reason, "tenant state change");
    }
    let (action, verb) = match desired {
        Desired::Active => ("tenant.attach", "attach"),
        Desired::Detached => ("tenant.detach", "detach"),
    };
    let ask = Ask {
        path: format!("/v1/tenants/{id}/{verb}"),
        id,
        action,
        precondition: Precondition::Etag(etag),
        acknowledge_resync: q.acknowledge_resync || body.acknowledge_resync,
        dry_run: q.dry_run,
        method: "POST",
    };
    mutate(st, ctx, ask, move |current| {
        let mut next = current
            .cloned()
            .ok_or_else(|| ServiceError::new(404, "not_found", "no such tenant"))?;
        next.desired = desired;
        Ok(Some(next))
    })
    .await
}

async fn detach(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<MutationQuery>,
    headers: HeaderMap,
    body: Option<Json<StateBody>>,
) -> Reply {
    set_desired(&st, &ctx, id, &headers, q, body, Desired::Detached).await
}

async fn attach(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<MutationQuery>,
    headers: HeaderMap,
    body: Option<Json<StateBody>>,
) -> Reply {
    set_desired(&st, &ctx, id, &headers, q, body, Desired::Active).await
}

async fn incarnation(st: &ApiState, id: &str) -> Option<String> {
    let spill = st.ctx.spill_dir.as_ref()?;
    TenantState::load(&tenant_dir(spill, id))
        .await
        .ok()
        .flatten()
        .and_then(|s| s.incarnation)
}

async fn delete_intent(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
) -> Reply {
    let (decl, root) = find_tenant(&st, &ctx, &id).await?;
    let etag = st.service.etag(&decl).unwrap_or_default();
    let incarnation = incarnation(&st, &id).await;
    let key_id = ctx.principal.as_ref().map_or("", |p| p.key_id.as_str());
    let epoch = st.ctx.reloader.current().epoch.to_string();
    let token = st
        .deletes
        .issue(&id, &etag, key_id, &epoch, incarnation.as_deref())
        .map_err(|e| internal(&ctx, format!("{e:#}")))?;
    let snap = st.ctx.metrics.snapshot().await;
    let view = tenant_json(&st, &decl, &root, &snap, false).await;
    let outbox =
        crate::destination::config::DestinationConfig::from_table(&decl.effective_table(&root))
            .ok()
            .and_then(|d| d.snowflake)
            .is_some_and(|sf| sf.ack_after == crate::destination::config::AckAfter::Outbox);
    let mut warnings = Vec::new();
    if outbox {
        warnings.push(
            "ack_after = \"outbox\": the Snowflake state directory may hold rows applied nowhere else; \
             it is retained, never deleted",
        );
    }
    if let Some(log) = st.ctx.reloader.audit() {
        let _ = log
            .append(crate::ops::audit::Draft {
                request_id: ctx.request_id.clone(),
                phase: "result".into(),
                actor: ctx.actor(),
                action: "tenant.delete_intent".into(),
                tenant: Some(id.clone()),
                method: Some("POST".into()),
                path: Some(format!("/v1/tenants/{id}/delete-intent")),
                outcome: "applied".into(),
                ..Default::default()
            })
            .await;
    }
    Ok(Json(json!({
        "confirm_token": token,
        "expires_in_secs": super::deleted::TOKEN_TTL.as_secs(),
        "etag": etag,
        "impact": {
            "tenant": view,
            "detaches": true,
            "retained": [
                "destination databases, tables and rows",
                "Snowflake objects, stage files and the Snowflake state directory",
                "the tenant's spill directory",
            ],
            "removed": ["the tenant's declaration, kept as a tombstone for restore"],
            "replication_slot": "shared by every tenant; it stops holding WAL for this one",
            "warnings": warnings,
        },
        "next": format!("DELETE /v1/tenants/{id} with X-Confirm-Delete and If-Match"),
    }))
    .into_response())
}

async fn delete_tenant(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Reply {
    let token = headers
        .get("x-confirm-delete")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .map(str::to_owned)
        .ok_or_else(|| {
            fail(
                &ctx,
                428,
                "confirmation_required",
                "POST /delete-intent first, then send its token as X-Confirm-Delete",
            )
        })?;
    let etag = if_match(&ctx, &headers)?;
    let (decl, _) = find_tenant(&st, &ctx, &id).await?;
    let current = st.service.etag(&decl).unwrap_or_default();
    let incarnation = incarnation(&st, &id).await;
    let key_id = ctx
        .principal
        .as_ref()
        .map_or(String::new(), |p| p.key_id.clone());
    let epoch = st.ctx.reloader.current().epoch.to_string();
    let redeemed = st.deletes.redeem(
        &token,
        &super::deleted::Redeem {
            tenant: &id,
            etag: &current,
            key_id: &key_id,
            epoch: &epoch,
            incarnation: incarnation.as_deref(),
        },
    );
    match redeemed {
        Ok(()) => {}
        Err(super::deleted::Refusal::Expired) => {
            return Err(fail(
                &ctx,
                410,
                "confirmation_expired",
                "the confirmation token expired; ask again",
            ));
        }
        Err(super::deleted::Refusal::Unknown) => {
            return Err(fail(
                &ctx,
                409,
                "confirmation_invalid",
                "no such confirmation token, or it was used",
            ));
        }
        Err(super::deleted::Refusal::Mismatch(why)) => {
            return Err(fail(
                &ctx,
                409,
                "confirmation_invalid",
                format!("the confirmation token is void: {why}"),
            ));
        }
    }
    let tombstone = st
        .deletes
        .write_tombstone(&decl, &key_id, &ctx.request_id, incarnation.as_deref())
        .map_err(|e| internal(&ctx, format!("{e:#}")))?;
    let ask = Ask {
        path: format!("/v1/tenants/{id}"),
        id,
        action: "tenant.delete",
        precondition: Precondition::Etag(etag),
        acknowledge_resync: false,
        dry_run: false,
        method: "DELETE",
    };
    let res = mutate(&st, &ctx, ask, |current| match current {
        Some(_) => Ok(None),
        None => Err(ServiceError::new(404, "not_found", "no such tenant")),
    })
    .await;
    if res.is_err() {
        st.deletes.remove_tombstone(&tombstone);
    }
    res
}

async fn restore(
    State(st): State<ApiState>,
    Extension(ctx): Extension<RequestCtx>,
    Path(id): Path<String>,
    Query(q): Query<MutationQuery>,
    body: Option<Json<StateBody>>,
) -> Reply {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let (tombstone, mut decl) = st
        .deletes
        .latest(&id)
        .ok_or_else(|| fail(&ctx, 404, "not_found", "no tombstone for that tenant"))?;
    decl.desired = match body.desired.as_deref() {
        None | Some("detached") => Desired::Detached,
        Some("active") => Desired::Active,
        Some(other) => {
            return Err(fail(
                &ctx,
                400,
                "invalid_body",
                format!("desired {other:?}: active or detached"),
            ));
        }
    };
    let ack = q.acknowledge_resync || body.acknowledge_resync;
    if decl.desired == Desired::Active && !ack {
        return Err(fail(
            &ctx,
            409,
            "resync_not_acknowledged",
            "restoring as active attaches the tenant afresh with fresh initial loads; resend with \
             acknowledge_resync",
        ));
    }
    let ask = Ask {
        path: format!("/v1/tenants-deleted/{id}/restore"),
        id,
        action: "tenant.restore",
        precondition: Precondition::Absent,
        acknowledge_resync: ack,
        dry_run: q.dry_run,
        method: "POST",
    };
    let res = mutate(&st, &ctx, ask, move |_| Ok(Some(decl))).await;
    if res.is_ok() && !q.dry_run {
        st.deletes.remove_tombstone(&tombstone.file);
    }
    res
}

async fn config_mutation(
    st: &ApiState,
    ctx: &RequestCtx,
    action: &'static str,
    path: &str,
    edit: impl FnOnce(&mut toml::Table),
) -> Reply {
    let origin = Some(("POST".to_string(), path.to_string()));
    let res = crate::control::apply_fragment(&st.ctx, &ctx.actor(), action, origin, edit).await;
    match res {
        Ok(unfollowed) => {
            let rev = st.ctx.reloader.current();
            Ok((
                StatusCode::ACCEPTED,
                Json(json!({
                    "request_id": ctx.request_id,
                    "epoch": rev.epoch.to_string(),
                    "generation": rev.generation,
                    "unfollowed_databases": unfollowed,
                })),
            )
                .into_response())
        }
        Err(e) => Err(service(ctx, e)),
    }
}

fn set_paused(root: &mut toml::Table, paused: bool) {
    let stream = root
        .entry("stream")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    if let toml::Value::Table(t) = stream {
        t.insert("paused".into(), paused.into());
    }
}

async fn pause(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    config_mutation(&st, &ctx, "stream.pause", "/v1/stream/pause", |r| {
        set_paused(r, true)
    })
    .await
}

async fn resume(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    config_mutation(&st, &ctx, "stream.resume", "/v1/stream/resume", |r| {
        set_paused(r, false)
    })
    .await
}

async fn reload(State(st): State<ApiState>, Extension(ctx): Extension<RequestCtx>) -> Reply {
    config_mutation(&st, &ctx, "config.reload", "/v1/reload", |_| {}).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_patch_follows_rfc_7396() {
        let mut doc = json!({"a": "b", "c": {"d": "e", "f": "g"}});
        merge_patch(&mut doc, &json!({"a": "z", "c": {"f": null}, "n": [1]}));
        assert_eq!(doc, json!({"a": "z", "c": {"d": "e"}, "n": [1]}));
        let mut scalar = json!("x");
        merge_patch(&mut scalar, &json!({"k": 1}));
        assert_eq!(scalar, json!({"k": 1}));
        merge_patch(&mut scalar, &json!(5));
        assert_eq!(scalar, json!(5));
    }

    #[test]
    fn operations_keep_the_newest() {
        let ops = Operations::default();
        for i in 0..(OPERATIONS_KEPT + 5) {
            ops.set(&format!("r{i}"), json!(i));
        }
        assert!(ops.get("r0").is_none());
        assert_eq!(
            ops.get(&format!("r{}", OPERATIONS_KEPT + 4)),
            Some(json!(OPERATIONS_KEPT + 4))
        );
        ops.set("r1004", json!("updated"));
        assert_eq!(ops.get("r1004"), Some(json!("updated")));
    }
}
