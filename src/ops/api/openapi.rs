//! The API's OpenAPI 3.1 document, built from [`ROUTES`]
//!
//! Paths, methods, scopes, rate classes and audit actions come from the
//! route table that also builds the router and drives the guard, so a
//! route cannot exist undocumented and the document cannot describe a
//! route that does not exist. Every [`Route`] carries a [`RouteDoc`]: a
//! route cannot be declared without one. Body schemas are derived with
//! `utoipa` from the types in [`super::schema`] and the types handlers
//! serialize; responses the guard and the outer layers add are documented
//! per operation from what they actually do. Object schemas are closed
//! unless [`OPEN`] lists them, so the conformance tests catch a field a
//! handler sends that the document does not know; a `#[serde(flatten)]`
//! type's `allOf` is merged into one object first, so it closes too and
//! the type it flattens stays closed where it stands alone

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::errors::ERROR_CODES;
use super::keys::Scope;
use super::routes::{Class, ROUTES, Route};

/// Registers a schema with its dependencies, and returns a `$ref` to it
pub type SchemaFn = fn(&mut Components) -> Value;

/// Where a parameter goes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum In {
    Query,
    Header,
}

/// A parameter's type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    String,
    Integer,
    Boolean,
    OneOf(&'static [&'static str]),
}

/// A query or header parameter. Path parameters come from the path
#[derive(Clone, Copy, Debug)]
pub struct Param {
    pub name: &'static str,
    pub at: In,
    pub required: bool,
    pub kind: Kind,
    pub description: &'static str,
}

/// A JSON request body
#[derive(Clone, Copy)]
pub struct Body {
    pub schema: SchemaFn,
    pub required: bool,
    pub description: &'static str,
}

/// A response body's media type
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Media {
    Json,
    Text,
    OpenMetrics,
    Html,
}

impl Media {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Text => "text/plain; charset=utf-8",
            Self::OpenMetrics => "application/openmetrics-text; version=1.0.0; charset=utf-8",
            Self::Html => "text/html; charset=utf-8",
        }
    }
}

/// A success response
#[derive(Clone, Copy)]
pub struct Reply {
    pub status: u16,
    pub description: &'static str,
    pub media: Media,
    /// `None` for text bodies
    pub schema: Option<SchemaFn>,
    /// Carries the tenant's `ETag`
    pub etag: bool,
}

/// What the document says about one route, beyond what [`Route`] says
#[derive(Clone, Copy)]
pub struct RouteDoc {
    /// Unique, stable
    pub operation_id: &'static str,
    pub tag: &'static str,
    pub summary: &'static str,
    pub description: &'static str,
    pub params: &'static [Param],
    pub body: Option<Body>,
    pub replies: &'static [Reply],
    /// The route's own `(code, status)` errors, every one in
    /// [`ERROR_CODES`]; the guard's are added for every route
    pub errors: &'static [(&'static str, u16)],
}

impl std::fmt::Debug for RouteDoc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteDoc")
            .field("operation_id", &self.operation_id)
            .finish()
    }
}

/// Schemas the document refers to, by name
#[derive(Default)]
pub struct Components {
    schemas: BTreeMap<String, Value>,
}

/// [`SchemaFn`] for a `utoipa` type
pub fn component<T: utoipa::ToSchema>(c: &mut Components) -> Value {
    let name = T::name().into_owned();
    if !c.schemas.contains_key(&name) {
        c.schemas.insert(name.clone(), to_json(T::schema()));
        let mut nested = Vec::new();
        T::schemas(&mut nested);
        for (n, s) in nested {
            c.schemas.entry(n).or_insert_with(|| to_json(s));
        }
    }
    json!({"$ref": format!("#/components/schemas/{name}")})
}

fn to_json<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).expect("utoipa schemas serialize")
}

/// Components whose objects take keys beyond those listed: config
/// sections, the status table, the document itself, tenant declarations
pub const OPEN: &[&str] = &[
    "StatusBody",
    "ConfigBody",
    "OpenApiDocument",
    "TenantCreate",
    "TenantReplace",
    "TenantPatch",
];

/// Which routes are reachable without a key
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Exposure {
    pub public_livez: bool,
    pub public_docs: bool,
}

/// The docs routes `--api-public-docs` opens
pub const DOCS_PATHS: [&str; 2] = ["/v1/openapi.json", "/v1/docs"];

impl Exposure {
    pub fn is_public(&self, r: &Route) -> bool {
        (r.class == Class::Public && self.public_livez)
            || (r.method == "GET" && DOCS_PATHS.contains(&r.path) && self.public_docs)
    }
}

const TAGS: &[(&str, &str)] = &[
    (
        "health",
        "Liveness, readiness and every health check behind them",
    ),
    (
        "status",
        "The stream's status, metrics, recent errors, crash records and jobs",
    ),
    ("tenants", "Tenant status, and changes to tenants"),
    (
        "deleted",
        "Deleted tenants: tombstones, restore, teardown progress",
    ),
    (
        "control",
        "Cluster pause and resume, config reload and view",
    ),
    ("audit", "The audit log, and the outcome of each mutation"),
    ("docs", "This document, as JSON and as a web page"),
];

/// The document for `exposure`
pub fn document(exposure: Exposure) -> Value {
    let mut c = Components::default();
    let error_ref = component::<super::schema::ErrorBody>(&mut c);
    let mut paths = Map::new();
    for r in ROUTES {
        let op = operation(r, &mut c, &error_ref, exposure);
        let item = paths
            .entry(r.path.to_string())
            .or_insert_with(|| Value::Object(Map::new()));
        item[r.method.to_ascii_lowercase()] = op;
    }
    let mut schemas: Map<String, Value> = c.schemas.into_iter().collect();
    // The error code is one of the documented codes
    if let Some(code) = schemas
        .get_mut("ErrorDetail")
        .and_then(|d| d.pointer_mut("/properties/code"))
    {
        code["enum"] = json!(super::errors::codes());
    }
    merge_compositions(&mut schemas);
    for (name, s) in schemas.iter_mut() {
        close_value(s, OPEN.contains(&name.as_str()));
    }
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "walshadow HTTP API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Health, metrics, crash and error reports, jobs, tenant status, and \
                tenant control for walshadow-stream. Every route needs a bearer API key with the \
                scope it names (`read` or `admin`; `admin` includes `read`), unless this \
                document marks it public. Requests are rate limited per client IP and per key. \
                Errors share one body, `ErrorBody`.",
        },
        "servers": [{"url": "/"}],
        "tags": TAGS.iter().map(|(n, d)| json!({"name": n, "description": d})).collect::<Vec<_>>(),
        "paths": paths,
        "components": {
            "schemas": schemas,
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "An API key from `walshadow-stream api-key new`, as \
                        `Authorization: Bearer wsk_<id>_<secret>`. Keys carry the `read` or \
                        `admin` scope; each operation's security names the one it needs. A key \
                        in the query string is refused.",
                },
            },
        },
    })
}

fn operation(r: &Route, c: &mut Components, error_ref: &Value, exposure: Exposure) -> Value {
    let d = &r.doc;
    let public = exposure.is_public(r);
    let mut params: Vec<Value> = path_params(r.path)
        .into_iter()
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"},
                "description": match name {
                    "id" if r.path == "/v1/crashes/{id}" => "The crash record id, from `/v1/crashes`",
                    "id" => "The tenant id",
                    "request_id" => "A mutation's request id, from its response or `X-Request-Id`",
                    _ => "",
                },
            })
        })
        .collect();
    for p in d.params {
        params.push(json!({
            "name": p.name,
            "in": match p.at {
                In::Query => "query",
                In::Header => "header",
            },
            "required": p.required,
            "schema": kind_schema(p.kind),
            "description": p.description,
        }));
    }
    let mut responses = Map::new();
    for reply in d.replies {
        let mut headers = guard_headers();
        if reply.media == Media::Html {
            headers.insert(
                "Content-Security-Policy".into(),
                header(super::docs_html::CONTENT_SECURITY_POLICY),
            );
        }
        if reply.etag {
            headers.insert(
                "ETag".into(),
                header("The tenant's ETag; send it back as `If-Match`"),
            );
        }
        let schema = match reply.schema {
            Some(f) => f(c),
            None => json!({"type": "string"}),
        };
        let mut content = Map::new();
        content.insert(reply.media.as_str().into(), json!({"schema": schema}));
        responses.insert(
            reply.status.to_string(),
            json!({"description": reply.description, "headers": headers, "content": content}),
        );
    }
    for (status, codes) in error_statuses(r, d, public) {
        let key = status.to_string();
        let described = codes
            .iter()
            .map(|code| format!("`{code}`: {}", meaning(code, status)))
            .collect::<Vec<_>>()
            .join("; ");
        let mut headers = guard_headers();
        match status {
            401 => {
                headers.insert(
                    "WWW-Authenticate".into(),
                    header("`Bearer realm=\"walshadow\"`"),
                );
            }
            429 => {
                headers.insert(
                    "Retry-After".into(),
                    header("Seconds to wait before retrying"),
                );
                headers.insert(
                    "RateLimit-Reset".into(),
                    header("Seconds until a token is available, the same as `Retry-After`"),
                );
            }
            _ => {}
        }
        match responses.get_mut(&key) {
            // A success status the route also fails with: readiness's 503
            Some(existing) => {
                let media = Media::Json.as_str();
                let ok = existing["content"][media]["schema"].clone();
                existing["content"][media]["schema"] = json!({"oneOf": [ok, error_ref]});
                let text = existing["description"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string();
                existing["description"] = format!("{text} Or an error: {described}").into();
            }
            None => {
                responses.insert(
                    key,
                    json!({
                        "description": described,
                        "headers": headers,
                        "content": {"application/json": {"schema": error_ref}},
                    }),
                );
            }
        }
    }
    // The outer layers answer without the guard's envelope or headers
    responses.insert(
        "408".into(),
        json!({"description": "The request ran past 60 s; the timeout layer answers with an \
            empty body, without the guard's headers. A mutation still finishes: see \
            `/v1/operations/{request_id}`"}),
    );
    responses.insert("500".into(), merge_500(responses.get("500").cloned()));
    let scope = r.scope.as_str();
    let mut security = vec![json!({"bearerAuth": [scope]})];
    if public {
        security.insert(0, json!({}));
    }
    let mut description = d.description.to_string();
    if !description.is_empty() {
        description.push_str("\n\n");
    }
    description.push_str(&format!(
        "Scope: `{scope}`. Rate class: `{}`.",
        class_name(r.class)
    ));
    if public {
        description.push_str(" Public on this daemon: no key needed.");
    }
    let mut op = json!({
        "operationId": d.operation_id,
        "tags": [d.tag],
        "summary": d.summary,
        "description": description,
        "parameters": params,
        "responses": responses,
        "security": security,
        "x-walshadow-scope": scope,
        "x-walshadow-rate-class": class_name(r.class),
    });
    if let Some(action) = r.action {
        op["x-walshadow-audit-action"] = action.into();
    }
    if let Some(body) = &d.body {
        op["requestBody"] = json!({
            "required": body.required,
            "description": body.description,
            "content": {"application/json": {"schema": (body.schema)(c)}},
        });
    }
    op
}

/// 500 is the panic layer's plain body, and on some routes also the
/// `internal` error envelope
fn merge_500(existing: Option<Value>) -> Value {
    let panic = "A panic in the handler: the panic layer answers with a plain-text body, \
        without the guard's headers";
    match existing {
        Some(mut r) => {
            let text = r["description"].as_str().unwrap_or_default().to_string();
            r["description"] = format!("{text}. Or: {panic}").into();
            r["content"]["text/plain; charset=utf-8"] = json!({"schema": {"type": "string"}});
            r
        }
        None => json!({
            "description": panic,
            "content": {"text/plain; charset=utf-8": {"schema": {"type": "string"}}},
        }),
    }
}

/// Every `(status, codes)` a route answers with as errors: its own, and
/// the guard's
pub fn error_statuses(r: &Route, d: &RouteDoc, public: bool) -> BTreeMap<u16, Vec<&'static str>> {
    let mut out: BTreeMap<u16, Vec<&'static str>> = BTreeMap::new();
    let mut add = |code: &'static str, status: u16| {
        let codes = out.entry(status).or_default();
        if !codes.contains(&code) {
            codes.push(code);
        }
    };
    if !public {
        add("unauthenticated", 401);
    }
    if r.scope == Scope::Admin {
        add("forbidden", 403);
    }
    add("rate_limited", 429);
    add("busy", 503);
    add("invalid_request", 413);
    if !d.params.is_empty() || !path_params(r.path).is_empty() {
        add("invalid_request", 400);
    }
    if d.body.is_some() {
        add("invalid_request", 400);
        add("invalid_request", 415);
        add("invalid_request", 422);
    }
    for (code, status) in d.errors {
        add(code, *status);
    }
    out
}

fn meaning(code: &str, status: u16) -> &'static str {
    ERROR_CODES
        .iter()
        .find(|(c, s, _)| *c == code && *s == status)
        .map_or("", |(_, _, m)| m)
}

fn guard_headers() -> Map<String, Value> {
    let mut h = Map::new();
    h.insert(
        "X-Request-Id".into(),
        header("Identifies the request in logs and the audit log"),
    );
    h.insert("Cache-Control".into(), header("`no-store`"));
    h.insert("X-Content-Type-Options".into(), header("`nosniff`"));
    h
}

fn header(description: &str) -> Value {
    json!({"description": description, "schema": {"type": "string"}})
}

fn kind_schema(k: Kind) -> Value {
    match k {
        Kind::String => json!({"type": "string"}),
        Kind::Integer => json!({"type": "integer", "minimum": 0}),
        Kind::Boolean => json!({"type": "boolean", "default": false}),
        Kind::OneOf(values) => json!({"type": "string", "enum": values}),
    }
}

pub fn class_name(c: Class) -> &'static str {
    match c {
        Class::Read => "read",
        Class::Expensive => "expensive",
        Class::Mutate => "mutate",
        Class::Public => "public",
    }
}

/// `{name}` segments of an axum path, which are OpenAPI's too
pub fn path_params(path: &str) -> Vec<&str> {
    path.split('/')
        .filter_map(|s| s.strip_prefix('{').and_then(|s| s.strip_suffix('}')))
        .collect()
}

/// Merge each component's `allOf`, which `utoipa` emits for a
/// `#[serde(flatten)]` field, into one object with every part's
/// properties: a closed object cannot be composed, since each part would
/// refuse the others' keys
fn merge_compositions(schemas: &mut Map<String, Value>) {
    let merged: Vec<(String, Value)> = schemas
        .iter()
        .filter(|(_, s)| s.get("allOf").is_some())
        .map(|(name, s)| (name.clone(), merge(s, schemas, 0)))
        .collect();
    schemas.extend(merged);
}

fn merge(s: &Value, schemas: &Map<String, Value>, depth: usize) -> Value {
    let Some(parts) = s.get("allOf").and_then(Value::as_array) else {
        return s.clone();
    };
    assert!(depth < 8, "allOf nests deeper than 8");
    let mut properties = Map::new();
    let mut required: Vec<Value> = Vec::new();
    for part in parts {
        let part = match part
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|r| r.strip_prefix("#/components/schemas/"))
        {
            Some(name) => schemas
                .get(name)
                .unwrap_or_else(|| panic!("allOf part {name} is not a component")),
            None => part,
        };
        let part = merge(part, schemas, depth + 1);
        assert!(
            part.get("allOf").is_none() && part.get("oneOf").is_none(),
            "only objects merge"
        );
        if let Some(Value::Object(p)) = part.get("properties") {
            properties.extend(p.clone());
        }
        for r in part
            .get("required")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if !required.contains(r) {
                required.push(r.clone());
            }
        }
    }
    let mut out = json!({"type": "object", "properties": properties, "required": required});
    if let Some(d) = s.get("description") {
        out["description"] = d.clone();
    }
    out
}

/// Close an object schema with listed properties unless `open`; nested
/// objects always close
fn close_value(v: &mut Value, open: bool) {
    let Value::Object(m) = v else {
        return;
    };
    if m.contains_key("allOf") {
        m.entry("unevaluatedProperties")
            .or_insert(Value::Bool(open));
        return;
    }
    if m.contains_key("properties") {
        m.entry("additionalProperties").or_insert(Value::Bool(open));
    } else if open && m.get("type").and_then(Value::as_str) == Some("object") {
        m.entry("additionalProperties").or_insert(Value::Bool(true));
    }
    for key in ["properties", "$defs"] {
        if let Some(Value::Object(props)) = m.get_mut(key) {
            props.values_mut().for_each(|p| close_value(p, false));
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(inner) = m.get_mut(key) {
            close_value(inner, false);
        }
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(parts)) = m.get_mut(key) {
            parts.iter_mut().for_each(|p| close_value(p, false));
        }
    }
}
