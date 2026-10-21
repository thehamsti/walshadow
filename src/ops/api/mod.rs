//! Authenticated HTTP API: health, metrics, crash and error reports, jobs,
//! tenant status, and tenant control
//!
//! Opt-in with `--api-bind`. Every route needs a bearer API key
//! ([`keys`]) with the scope [`routes::ROUTES`] names, except `/livez` with
//! `--api-public-livez`. Requests are rate-limited per client IP and per
//! key ([`rate_limit`]). Mutations go through
//! [`TenantService`] and the
//! audit log. A non-loopback bind needs TLS unless `--api-allow-plaintext`
//! says a proxy in front terminates it.

pub mod deleted;
pub mod handlers;
pub mod health;
pub mod keys;
pub mod metrics;
pub mod rate_limit;
pub mod routes;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use self::keys::{KeyStore, Principal};
use self::rate_limit::{Limiter, Rate};
use self::routes::{Class, Route};
use crate::control::SharedCtx;
use crate::ops::crash::CrashStore;
use crate::ops::tenant_service::{EtagKey, TenantService};

/// Rate-limit settings, per class
#[derive(Clone, Debug)]
pub struct Rates {
    pub preauth: Rate,
    pub auth_failure: Rate,
    pub read: Rate,
    pub expensive: Rate,
    pub mutate: Rate,
}

impl Default for Rates {
    fn default() -> Self {
        Self {
            preauth: Rate::new(50.0, 100.0),
            auth_failure: Rate::new(10.0 / 60.0, 10.0),
            read: Rate::new(20.0, 40.0),
            expensive: Rate::new(1.0, 3.0),
            mutate: Rate::new(1.0, 5.0),
        }
    }
}

impl Rates {
    /// `<class>=<rate>`, class one of `preauth`, `auth_failure`, `read`,
    /// `expensive`, `mutate`
    pub fn set(&mut self, spec: &str) -> Result<()> {
        let (class, rate) = spec
            .split_once('=')
            .with_context(|| format!("--api-rate-limit {spec:?}: expected <class>=<rate>"))?;
        let rate = Rate::parse(rate)?;
        match class.trim() {
            "preauth" => self.preauth = rate,
            "auth_failure" => self.auth_failure = rate,
            "read" => self.read = rate,
            "expensive" => self.expensive = rate,
            "mutate" => self.mutate = rate,
            other => bail!("--api-rate-limit: unknown class {other:?}"),
        }
        Ok(())
    }
}

/// An address range `--api-trusted-proxies` names
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    pub fn parse(spec: &str) -> Result<Self> {
        let (addr, prefix) = match spec.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (spec, None),
        };
        let addr: IpAddr = addr
            .trim()
            .parse()
            .with_context(|| format!("trusted proxy {spec:?}"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix {
            Some(p) => p
                .trim()
                .parse()
                .with_context(|| format!("trusted proxy {spec:?}"))?,
            None => max,
        };
        ensure!(prefix <= max, "trusted proxy {spec:?}: prefix over {max}");
        Ok(Self { addr, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        let bits = |ip: IpAddr| -> (u128, u8) {
            match ip {
                IpAddr::V4(v) => (u128::from(u32::from(v)), 32),
                IpAddr::V6(v) => (u128::from(v), 128),
            }
        };
        let (net, width) = bits(self.addr);
        let (ip, ip_width) = bits(match ip {
            IpAddr::V6(v) if self.addr.is_ipv4() => match v.to_ipv4_mapped() {
                Some(v4) => IpAddr::V4(v4),
                None => return false,
            },
            ip => ip,
        });
        if width != ip_width {
            return false;
        }
        if self.prefix == 0 {
            return true;
        }
        let shift = u32::from(width - self.prefix);
        (net >> shift) == (ip >> shift)
    }
}

#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub bind: SocketAddr,
    pub keys_file: PathBuf,
    pub tls: Option<(PathBuf, PathBuf)>,
    pub allow_plaintext: bool,
    pub state_dir: PathBuf,
    pub public_livez: bool,
    pub trusted_proxies: Vec<Cidr>,
    pub rates: Rates,
    pub deleted_retention: Duration,
    pub ready_grace: Duration,
    /// Requests served at once; more wait for a free slot, then 503
    pub max_in_flight: usize,
    pub max_connections: usize,
}

impl ApiConfig {
    /// Refuse a plaintext bind anyone off the host could reach
    pub fn check(&self) -> Result<()> {
        if self.tls.is_none() && !self.bind.ip().is_loopback() && !self.allow_plaintext {
            bail!(
                "--api-bind {} is not loopback: set --api-tls-cert and --api-tls-key, or \
                 --api-allow-plaintext when a proxy in front terminates TLS",
                self.bind
            );
        }
        Ok(())
    }
}

/// Buckets, per class
pub struct Limits {
    pub preauth: Limiter<IpAddr>,
    pub auth_failure: Limiter<IpAddr>,
    pub read: Limiter<String>,
    pub expensive: Limiter<String>,
    pub mutate: Limiter<String>,
}

const BUCKETS: usize = 10_000;

impl Limits {
    pub fn new(r: &Rates) -> Self {
        Self {
            preauth: Limiter::new(r.preauth, BUCKETS),
            auth_failure: Limiter::new(r.auth_failure, BUCKETS),
            read: Limiter::new(r.read, BUCKETS),
            expensive: Limiter::new(r.expensive, BUCKETS),
            mutate: Limiter::new(r.mutate, BUCKETS),
        }
    }
}

/// Everything a handler reads or drives
#[derive(Clone)]
pub struct ApiState {
    pub cfg: Arc<ApiConfig>,
    pub ctx: SharedCtx,
    pub keys: Arc<KeyStore>,
    pub limits: Arc<Limits>,
    pub service: TenantService,
    pub crashes: Option<Arc<CrashStore>>,
    pub ops: Arc<handlers::Operations>,
    pub deletes: Arc<deleted::Deletes>,
    pub health: Arc<health::Tracker>,
    in_flight: Arc<tokio::sync::Semaphore>,
}

impl ApiState {
    pub async fn new(
        cfg: ApiConfig,
        ctx: SharedCtx,
        keys: Arc<KeyStore>,
        crashes: Option<Arc<CrashStore>>,
    ) -> Result<Self> {
        let etag_key = Arc::new(EtagKey::load_or_create(&cfg.state_dir).await?);
        let deletes = Arc::new(deleted::Deletes::new(
            cfg.state_dir.join("deleted"),
            cfg.deleted_retention,
        )?);
        Ok(Self {
            limits: Arc::new(Limits::new(&cfg.rates)),
            in_flight: Arc::new(tokio::sync::Semaphore::new(cfg.max_in_flight.max(1))),
            health: Arc::new(health::Tracker::default()),
            service: TenantService::new(ctx.clone(), Some(etag_key)),
            ops: Arc::new(handlers::Operations::default()),
            cfg: Arc::new(cfg),
            ctx,
            keys,
            crashes,
            deletes,
        })
    }
}

/// Per request, set by the guard
#[derive(Clone, Debug)]
pub struct RequestCtx {
    pub request_id: String,
    pub ip: IpAddr,
    pub principal: Option<Principal>,
    pub route: Option<&'static Route>,
    audit_handled: Arc<std::sync::atomic::AtomicBool>,
}

impl RequestCtx {
    fn audit_handled(&self) {
        self.audit_handled
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn actor(&self) -> crate::ops::audit::Actor {
        crate::ops::audit::Actor::api_key(
            self.principal.as_ref().map_or("", |p| p.key_id.as_str()),
            Some(self.ip.to_string()),
        )
    }
}

/// The error body every route returns
pub fn error_response(status: StatusCode, code: &str, message: &str, request_id: &str) -> Response {
    let message = crate::ops::redact::redact_text(message);
    (
        status,
        axum::Json(json!({"error": {"code": code, "message": message, "request_id": request_id}})),
    )
        .into_response()
}

fn too_many(wait: Duration, request_id: &str, class: &'static str) -> Response {
    metrics::rate_limited(class);
    let mut resp = error_response(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "too many requests; retry after the indicated delay",
        request_id,
    );
    let secs = wait.as_secs_f64().ceil().max(1.0) as u64;
    let h = resp.headers_mut();
    h.insert(header::RETRY_AFTER, HeaderValue::from(secs));
    h.insert("ratelimit-reset", HeaderValue::from(secs));
    resp
}

fn client_ip(state: &ApiState, peer: SocketAddr, req: &Request) -> IpAddr {
    let peer_ip = peer.ip();
    if !state
        .cfg
        .trusted_proxies
        .iter()
        .any(|c| c.contains(peer_ip))
    {
        return peer_ip;
    }
    req.headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next_back())
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(peer_ip)
}

/// Authenticate, rate-limit and authorize every request, in that order
async fn guard(State(state): State<ApiState>, mut req: Request, next: Next) -> Response {
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0)), |c| c.0);
    let ip = client_ip(&state, peer, &req);
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .and_then(|p| routes::find(req.method().as_str(), p.as_str()));
    let label = route.map_or("unmatched", |r| r.path);
    let method = req.method().as_str().to_string();
    let path = req.uri().path().to_string();
    let resp = authorize(&state, &mut req, ip, route, &request_id).await;
    let mut resp = match resp {
        Ok(()) => match state.in_flight.clone().try_acquire_owned() {
            Ok(permit) => {
                let ctx = req
                    .extensions()
                    .get::<RequestCtx>()
                    .expect("authorized request context")
                    .clone();
                let route = ctx.route.expect("authorized route");
                let mut resp = next.run(req).await;
                drop(permit);
                if resp.status().is_client_error()
                    && !resp
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .is_some_and(|v| v.as_bytes().starts_with(b"application/json"))
                {
                    resp = error_response(
                        resp.status(),
                        "invalid_request",
                        "check the route's JSON body and query parameters; send JSON with Content-Type: application/json",
                        &request_id,
                    );
                }
                if route.class == Class::Mutate
                    && !ctx.audit_handled.load(std::sync::atomic::Ordering::Relaxed)
                    && resp.status().is_client_error()
                    && let Some(log) = state.ctx.reloader.audit()
                {
                    let draft = crate::ops::audit::Draft {
                        request_id: request_id.clone(),
                        phase: "result".into(),
                        actor: ctx.actor(),
                        action: route.action.unwrap_or("unknown").into(),
                        method: Some(method.clone()),
                        path: Some(path.clone()),
                        // `/v1/tenants/{id}…` and `/v1/tenants-deleted/{id}…`
                        // name a tenant; `/v1/stream/pause` does not
                        tenant: path
                            .strip_prefix("/v1/tenants/")
                            .or_else(|| path.strip_prefix("/v1/tenants-deleted/"))
                            .and_then(|rest| rest.split('/').next())
                            .map(str::to_owned),
                        outcome: "rejected".into(),
                        error: Some(format!("HTTP {}", resp.status())),
                        ..Default::default()
                    };
                    if let Err(e) = log.append(draft).await {
                        tracing::error!(target: "walshadow::audit", error = %e, "could not audit a rejected API mutation");
                    }
                }
                resp
            }
            Err(_) => error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "too many requests in flight; retry shortly",
                &request_id,
            ),
        },
        Err(resp) => *resp,
    };
    metrics::request(label, &method, resp.status().as_u16());
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        h.insert("x-request-id", v);
    }
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

async fn authorize(
    state: &ApiState,
    req: &mut Request,
    ip: IpAddr,
    route: Option<&'static Route>,
    request_id: &str,
) -> std::result::Result<(), Box<Response>> {
    if let Err(wait) = state.limits.preauth.check(&ip) {
        return Err(Box::new(too_many(wait, request_id, "preauth")));
    }
    let mut ctx = RequestCtx {
        request_id: request_id.to_string(),
        ip,
        principal: None,
        route,
        audit_handled: Arc::default(),
    };
    if route.is_some_and(|r| r.class == Class::Public) && state.cfg.public_livez {
        req.extensions_mut().insert(ctx);
        return Ok(());
    }
    if let Some(wait) = state.limits.auth_failure.exhausted(&ip) {
        return Err(Box::new(too_many(wait, request_id, "auth_failure")));
    }
    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let query_token = req.uri().query().is_some_and(|q| {
        q.split('&')
            .any(|kv| kv.starts_with("access_token=") || kv.starts_with("token="))
    });
    let principal = match (query_token, state.keys.authenticate(header)) {
        (false, Ok(p)) => p,
        (_, res) => {
            let reason = match res {
                Err(f) => f.label(),
                Ok(_) => "token_in_query",
            };
            metrics::auth_failure(reason);
            let _ = state.limits.auth_failure.check(&ip);
            let mut resp = error_response(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                "a valid API key is required",
                request_id,
            );
            resp.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"walshadow\""),
            );
            return Err(Box::new(resp));
        }
    };
    let Some(route) = route else {
        return Err(Box::new(error_response(
            StatusCode::NOT_FOUND,
            "not_found",
            "no such route",
            request_id,
        )));
    };
    let (limiter, class) = match route.class {
        Class::Read | Class::Public => (&state.limits.read, "read"),
        Class::Expensive => (&state.limits.expensive, "expensive"),
        Class::Mutate => (&state.limits.mutate, "mutate"),
    };
    if let Err(wait) = limiter.check(&principal.key_id) {
        return Err(Box::new(too_many(wait, request_id, class)));
    }
    if !principal.allows(route.scope) {
        if let (Class::Mutate, Some(log)) = (route.class, state.ctx.reloader.audit()) {
            let draft = crate::ops::audit::Draft {
                request_id: request_id.to_string(),
                phase: "result".into(),
                actor: crate::ops::audit::Actor::api_key(&principal.key_id, Some(ip.to_string())),
                action: route.action.unwrap_or("unknown").into(),
                method: Some(req.method().as_str().into()),
                path: Some(req.uri().path().into()),
                outcome: "denied".into(),
                error: Some(format!("needs the {} scope", route.scope.as_str())),
                ..Default::default()
            };
            let _ = log.append(draft).await;
        }
        return Err(Box::new(error_response(
            StatusCode::FORBIDDEN,
            "forbidden",
            &format!("this key lacks the {} scope", route.scope.as_str()),
            request_id,
        )));
    }
    ctx.principal = Some(principal);
    req.extensions_mut().insert(ctx);
    Ok(())
}

/// The router: one entry per [`routes::ROUTES`] row, behind the guard
pub fn router(state: ApiState) -> Router {
    let mut router: Router<ApiState> = Router::new();
    for r in routes::ROUTES {
        router = router.route(r.path, handlers::method_router(r));
    }
    router
        .fallback(|| async { StatusCode::NOT_FOUND })
        .layer(tower_http::limit::RequestBodyLimitLayer::new(1 << 20))
        .layer(axum::middleware::from_fn_with_state(state.clone(), guard))
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(60),
        ))
        .with_state(state)
}

fn tls_acceptor(cert: &PathBuf, key: &PathBuf) -> Result<tokio_rustls::TlsAcceptor> {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let cert_pem = std::fs::read(cert).with_context(|| format!("read {}", cert.display()))?;
    let key_pem = std::fs::read(key).with_context(|| format!("read {}", key.display()))?;
    let certs = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("{}: {e}", cert.display()))?;
    ensure!(!certs.is_empty(), "{} holds no certificate", cert.display());
    let key = PrivateKeyDer::from_pem_slice(&key_pem)
        .map_err(|e| anyhow::anyhow!("{}: {e}", key.display()))?;
    let provider = tokio_rustls::rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()));
    let mut config = tokio_rustls::rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Bind and serve until the runtime ends. Returns the bound address
pub async fn serve(state: ApiState) -> Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    state.cfg.check()?;
    let tls = match &state.cfg.tls {
        Some((cert, key)) => Some(tls_acceptor(cert, key)?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(state.cfg.bind)
        .await
        .with_context(|| format!("bind {}", state.cfg.bind))?;
    let local = listener.local_addr()?;
    let connections = Arc::new(tokio::sync::Semaphore::new(
        state.cfg.max_connections.max(1),
    ));
    let app = router(state.clone());
    state.health.spawn_sampler(state.ctx.metrics.clone());
    let handle = tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(target: "walshadow::api", error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let Ok(permit) = connections.clone().try_acquire_owned() else {
                drop(stream);
                continue;
            };
            let app = app.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let service = hyper::service::service_fn(
                    move |req: hyper::Request<hyper::body::Incoming>| {
                        let mut req = req.map(Body::new);
                        req.extensions_mut().insert(ConnectInfo(peer));
                        tower::ServiceExt::oneshot(app.clone(), req)
                    },
                );
                let mut builder = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                );
                builder
                    .http1()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(10));
                let served = match tls {
                    Some(tls) => {
                        match tokio::time::timeout(Duration::from_secs(10), tls.accept(stream))
                            .await
                        {
                            Ok(Ok(stream)) => {
                                builder
                                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                                    .await
                            }
                            _ => return,
                        }
                    }
                    None => {
                        builder
                            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                            .await
                    }
                };
                if let Err(e) = served {
                    tracing::debug!(target: "walshadow::api", error = %e, "connection ended");
                }
            });
        }
    });
    tracing::info!(
        target: "walshadow::api",
        addr = %local,
        tls = state.cfg.tls.is_some(),
        "HTTP API serving",
    );
    Ok((local, handle))
}

#[cfg(test)]
mod tests;
