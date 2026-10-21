//! The OpenAPI document held to the API it describes: drift against the
//! route table and the router, validity against the vendored OpenAPI 3.1
//! schema, every response the in-process API sends ([`Conformance`], run
//! by every [`Api::call`]), requests through both the schema and the API,
//! who may read the docs, and the web page

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, StatusCode, header};
use serde_json::{Value, json};

use super::{Api, concrete, generous};
use crate::metrics::MetricsSnapshot;
use crate::ops::api::errors::{self, ERROR_CODES};
use crate::ops::api::keys::Scope;
use crate::ops::api::openapi::{self, DOCS_PATHS, Exposure, OPEN};
use crate::ops::api::rate_limit::Rate;
use crate::ops::api::routes::{Class, ROUTES, Route};
use crate::ops::api::{Rates, docs_html};

/// Where the retriever serves the document under test from
const DOC_URI: &str = "https://walshadow.invalid/openapi.json";
const OAS_URI: &str = "https://spec.openapis.org/oas/3.1/schema/2022-10-07";
const DIALECT_URI: &str = "https://spec.openapis.org/oas/3.1/dialect/base";
const VENDORED: [(&str, &str); 3] = [
    (
        OAS_URI,
        include_str!("../../../tests/openapi/schema-2022-10-07.json"),
    ),
    (
        DIALECT_URI,
        include_str!("../../../tests/openapi/dialect-base.json"),
    ),
    (
        "https://spec.openapis.org/oas/3.1/meta/base",
        include_str!("../../../tests/openapi/meta-base.json"),
    ),
];

/// Serves the vendored OpenAPI schemas and the document under test, never
/// the network; any other URI is a miss, recorded
#[derive(Clone)]
struct Vendored {
    doc: Arc<Value>,
    misses: Arc<Mutex<Vec<String>>>,
}

impl Vendored {
    fn new(doc: Value) -> Self {
        Self {
            doc: Arc::new(doc),
            misses: Arc::default(),
        }
    }

    fn misses(&self) -> Vec<String> {
        self.misses.lock().unwrap().clone()
    }

    fn validator(&self, schema: &Value) -> Result<jsonschema::Validator, String> {
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .with_retriever(self.clone())
            .build(schema)
            .map_err(|e| e.to_string())
    }
}

impl jsonschema::Retrieve for Vendored {
    fn retrieve(
        &self,
        uri: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let uri = uri.as_str();
        if uri == DOC_URI {
            return Ok((*self.doc).clone());
        }
        if let Some((_, text)) = VENDORED.iter().find(|(u, _)| *u == uri) {
            return Ok(serde_json::from_str(text)?);
        }
        self.misses.lock().unwrap().push(uri.to_string());
        Err(format!("{uri} is not vendored").into())
    }
}

fn errors_of(v: &jsonschema::Validator, instance: &Value) -> Vec<String> {
    v.iter_errors(instance)
        .map(|e| format!("{e} (at {})", e.instance_path()))
        .collect()
}

/// `schema` with its document-relative references made absolute, so it
/// compiles on its own against the document the retriever serves
fn absolute(schema: &Value) -> Value {
    match schema {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| {
                    let v = match (k.as_str(), v) {
                        ("$ref", Value::String(r)) if r.starts_with("#/") => {
                            Value::String(format!("{DOC_URI}{r}"))
                        }
                        _ => absolute(v),
                    };
                    (k.clone(), v)
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(absolute).collect()),
        other => other.clone(),
    }
}

/// Every `$ref` in `doc` that does not name one of its components
fn dangling(doc: &Value) -> Vec<String> {
    fn walk(v: &Value, schemas: &serde_json::Map<String, Value>, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                if let Some(Value::String(r)) = m.get("$ref") {
                    let known = r
                        .strip_prefix("#/components/schemas/")
                        .is_some_and(|n| schemas.contains_key(n));
                    if !known {
                        out.push(r.clone());
                    }
                }
                m.values().for_each(|v| walk(v, schemas, out));
            }
            Value::Array(a) => a.iter().for_each(|v| walk(v, schemas, out)),
            _ => {}
        }
    }
    let schemas = doc["components"]["schemas"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    walk(doc, &schemas, &mut out);
    out
}

/// `(where, schema)` for every schema the document uses: components,
/// parameters, request and response bodies, headers
fn schemas_in(doc: &Value) -> Vec<(String, Value)> {
    fn walk(v: &Value, at: &str, out: &mut Vec<(String, Value)>) {
        match v {
            Value::Object(m) => {
                for (k, v) in m {
                    let here = format!("{at}/{k}");
                    if k == "schema" {
                        out.push((here, v.clone()));
                    } else {
                        walk(v, &here, out);
                    }
                }
            }
            Value::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    walk(v, &format!("{at}/{i}"), out);
                }
            }
            _ => {}
        }
    }
    let mut out: Vec<(String, Value)> = doc["components"]["schemas"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(n, s)| (format!("#/components/schemas/{n}"), s.clone()))
        .collect();
    walk(&doc["paths"], "#/paths", &mut out);
    out
}

/// The route a concrete request path is for: literal segments match,
/// `{name}` matches any non-empty one
fn route_of(method: &str, path: &str) -> Option<&'static Route> {
    let segments: Vec<&str> = path.split('/').collect();
    ROUTES.iter().filter(|r| r.method == method).find(|r| {
        let template: Vec<&str> = r.path.split('/').collect();
        template.len() == segments.len()
            && template
                .iter()
                .zip(&segments)
                .all(|(t, s)| t == s || (t.starts_with('{') && !s.is_empty()))
    })
}

fn operation<'a>(doc: &'a Value, r: &Route) -> &'a Value {
    &doc["paths"][r.path][r.method.to_ascii_lowercase()]
}

const EXPOSURES: [Exposure; 4] = [
    Exposure {
        public_livez: false,
        public_docs: false,
    },
    Exposure {
        public_livez: true,
        public_docs: false,
    },
    Exposure {
        public_livez: false,
        public_docs: true,
    },
    Exposure {
        public_livez: true,
        public_docs: true,
    },
];

/// Holds every response [`Api::call`] sees to the document that API
/// serves: a documented status, media type and headers, a body its schema
/// accepts, and an error code the operation declares for that status
pub(super) struct Conformance {
    doc: Arc<Value>,
    exposure: Exposure,
    vendored: Vendored,
    validators: Mutex<HashMap<String, Arc<jsonschema::Validator>>>,
    seen: Mutex<BTreeSet<(String, u16)>>,
}

impl Conformance {
    pub(super) fn new(served: &str, exposure: Exposure) -> Self {
        let doc: Value = serde_json::from_str(served).expect("the served document parses");
        Self {
            vendored: Vendored::new(doc.clone()),
            doc: Arc::new(doc),
            exposure,
            validators: Mutex::default(),
            seen: Mutex::default(),
        }
    }

    fn validator(&self, schema: &Value) -> Arc<jsonschema::Validator> {
        let key = schema.to_string();
        if let Some(v) = self.validators.lock().unwrap().get(&key) {
            return v.clone();
        }
        let v = Arc::new(
            self.vendored
                .validator(&absolute(schema))
                .unwrap_or_else(|e| panic!("{schema} does not compile: {e}")),
        );
        self.validators.lock().unwrap().insert(key, v.clone());
        v
    }

    fn is_valid(&self, schema: &Value, instance: &Value) -> bool {
        self.validator(schema).is_valid(instance)
    }

    fn assert_valid(&self, what: &str, schema: &Value, instance: &Value) {
        let errors = errors_of(&self.validator(schema), instance);
        assert!(
            errors.is_empty(),
            "{what}: the body does not match the document:\n  {}\nbody: {instance:#}",
            errors.join("\n  ")
        );
    }

    /// `(operationId, status)` of every documented response seen so far
    fn seen(&self) -> BTreeSet<(String, u16)> {
        self.seen.lock().unwrap().clone()
    }

    pub(super) fn check(
        &self,
        method: &str,
        uri: &str,
        status: StatusCode,
        headers: &HeaderMap,
        body: &[u8],
    ) {
        let path = uri.split('?').next().unwrap_or(uri);
        let code = status.as_u16();
        let what = format!("{method} {uri} → {code}");
        let media = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let Some(route) = route_of(method, path) else {
            // No route: the guard answers, authentication first
            assert!(
                matches!(code, 401 | 404 | 429 | 503),
                "{what}: a method or path outside the table answered"
            );
            if !body.is_empty() {
                let v: Value =
                    serde_json::from_slice(body).unwrap_or_else(|e| panic!("{what}: {e}"));
                self.assert_valid(
                    &what,
                    &json!({"$ref": "#/components/schemas/ErrorBody"}),
                    &v,
                );
                let c = v["error"]["code"].as_str().unwrap_or_default();
                assert!(
                    errors::known(c, code),
                    "{what}: ({c}, {code}) is undocumented"
                );
            }
            return;
        };
        let op = operation(&self.doc, route);
        let id = op["operationId"].as_str().expect("an operationId");
        let Some(resp) = op["responses"].get(code.to_string()) else {
            panic!("{what}: {id} does not document {code}");
        };
        let Some(content) = resp.get("content").and_then(Value::as_object) else {
            assert!(body.is_empty(), "{what}: {id} documents no body for {code}");
            self.seen.lock().unwrap().insert((id.into(), code));
            return;
        };
        let Some(documented) = content.get(media) else {
            panic!(
                "{what}: {id} documents {:?} for {code}, not {media:?}",
                content.keys().collect::<Vec<_>>()
            );
        };
        // The panic layer's plain 500 comes without the guard's headers
        if !(code == 500 && media.starts_with("text/plain")) {
            for name in resp["headers"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(n, _)| n)
            {
                if name != "ETag" {
                    assert!(headers.contains_key(name.as_str()), "{what}: no {name}");
                }
            }
        }
        let etag_documented = resp["headers"].get("ETag").is_some();
        if headers.contains_key(header::ETAG) {
            assert!(etag_documented, "{what}: {id} documents no ETag for {code}");
        }
        if media == "application/json" {
            let v: Value = serde_json::from_slice(body).unwrap_or_else(|e| panic!("{what}: {e}"));
            self.assert_valid(&format!("{what} ({id})"), &documented["schema"], &v);
            // An operation record carries an `error` too, with a 200
            if let Some(c) = v.pointer("/error/code").and_then(Value::as_str)
                && code >= 400
            {
                let declared =
                    openapi::error_statuses(route, &route.doc, self.exposure.is_public(route));
                assert!(
                    declared.get(&code).is_some_and(|codes| codes.contains(&c)),
                    "{what}: {id} does not declare ({c}, {code})"
                );
            }
            if etag_documented && v.get("etag").is_some_and(|e| !e.is_null()) {
                assert!(
                    headers.contains_key(header::ETAG),
                    "{what}: {id} documents an ETag for {code} and sent none"
                );
            }
        }
        self.seen.lock().unwrap().insert((id.into(), code));
    }
}

/// Drift: the document lists exactly the route table, with each route's
/// scope, rate class, audit action, path parameters and the guard's
/// responses
#[test]
fn the_document_lists_every_route_and_nothing_else() {
    for exposure in EXPOSURES {
        let doc = openapi::document(exposure);
        let documented: BTreeSet<(String, String)> = doc["paths"]
            .as_object()
            .unwrap()
            .iter()
            .flat_map(|(path, item)| {
                item.as_object()
                    .unwrap()
                    .keys()
                    .map(move |m| (m.to_ascii_uppercase(), path.clone()))
            })
            .collect();
        let routed: BTreeSet<(String, String)> = ROUTES
            .iter()
            .map(|r| (r.method.to_string(), r.path.to_string()))
            .collect();
        assert_eq!(documented, routed);
        let tags: Vec<&str> = doc["tags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        let mut ids = BTreeSet::new();
        for r in ROUTES {
            let op = operation(&doc, r);
            let what = format!("{} {}", r.method, r.path);
            assert!(ids.insert(r.doc.operation_id), "{what}: operationId twice");
            assert_eq!(op["operationId"], r.doc.operation_id, "{what}");
            assert!(tags.contains(&r.doc.tag), "{what}: tag {}", r.doc.tag);
            assert!(!r.doc.summary.is_empty(), "{what}");
            assert_eq!(op["x-walshadow-scope"], r.scope.as_str(), "{what}");
            assert_eq!(
                op["x-walshadow-rate-class"],
                openapi::class_name(r.class),
                "{what}"
            );
            assert_eq!(
                op.get("x-walshadow-audit-action").and_then(Value::as_str),
                r.action,
                "{what}"
            );
            let public = exposure.is_public(r);
            let mut security = vec![json!({"bearerAuth": [r.scope.as_str()]})];
            if public {
                security.insert(0, json!({}));
            }
            assert_eq!(op["security"], json!(security), "{what}");
            let in_path: Vec<&str> = op["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|p| p["in"] == "path")
                .map(|p| p["name"].as_str().unwrap())
                .collect();
            assert_eq!(in_path, openapi::path_params(r.path), "{what}");
            assert_eq!(
                op.get("requestBody").is_some(),
                r.doc.body.is_some(),
                "{what}"
            );
            let responses = op["responses"].as_object().unwrap();
            assert!(
                responses["429"]["headers"].get("RateLimit-Reset").is_some(),
                "{what}"
            );
            assert_eq!(
                op["responses"]["200"]["headers"]
                    .get("Content-Security-Policy")
                    .is_some(),
                r.path == "/v1/docs",
                "{what}"
            );
            for s in ["408", "413", "429", "500", "503"] {
                assert!(responses.contains_key(s), "{what}: no {s}");
            }
            assert_eq!(responses.contains_key("401"), !public, "{what}");
            assert_eq!(
                responses.contains_key("403"),
                r.scope == Scope::Admin,
                "{what}"
            );
            assert!(responses.keys().any(|k| k.starts_with('2')), "{what}");
            for (c, s) in r.doc.errors {
                assert!(
                    errors::known(c, *s),
                    "{what}: ({c}, {s}) not in ERROR_CODES"
                );
            }
        }
        // Objects are closed unless listed open; no composition is left
        for (name, s) in doc["components"]["schemas"].as_object().unwrap() {
            assert!(s.get("allOf").is_none(), "{name}");
            if s.get("properties").is_some() {
                assert_eq!(
                    s["additionalProperties"],
                    json!(OPEN.contains(&name.as_str())),
                    "{name}"
                );
            }
        }
    }
}

/// Drift, the other way: the router answers every documented method of
/// every documented path, and no other
#[tokio::test]
async fn the_router_serves_exactly_what_the_document_lists() {
    let api = Api::new(false, generous()).await;
    let doc: Value = serde_json::from_str(&api.state.docs.json).unwrap();
    for (path, item) in doc["paths"].as_object().unwrap() {
        for method in ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"] {
            let (s, _, body) = api
                .call(method, &concrete(path), Some(&api.admin), &[], None)
                .await;
            let unrouted = s == StatusCode::NOT_FOUND
                && (method == "HEAD" || body["error"]["message"] == "no such route");
            assert_eq!(
                unrouted,
                item.get(method.to_ascii_lowercase()).is_none(),
                "{method} {path}: {s} {body}"
            );
        }
    }
}

/// The error reference in `docs/api.md`, the document's `code` enum and
/// [`ERROR_CODES`] are the same list
#[test]
fn the_error_reference_is_the_table() {
    let md = include_str!("../../../docs/api.md");
    let start = md
        .find("| Status | Code | Meaning |")
        .expect("docs/api.md has the error table");
    let mut rows: Vec<(u16, String, String)> = md[start..]
        .lines()
        .skip(2)
        .take_while(|l| l.starts_with('|'))
        .map(|l| {
            let cells: Vec<&str> = l.trim().trim_matches('|').split(" | ").collect();
            assert_eq!(cells.len(), 3, "{l}");
            (
                cells[0].trim().parse().unwrap(),
                cells[1].trim().trim_matches('`').to_string(),
                cells[2].trim().to_string(),
            )
        })
        .collect();
    let mut table: Vec<(u16, String, String)> = ERROR_CODES
        .iter()
        .map(|(c, s, m)| (*s, c.to_string(), m.to_string()))
        .collect();
    rows.sort();
    table.sort();
    assert_eq!(rows, table);
    let doc = openapi::document(Exposure::default());
    assert_eq!(
        doc["components"]["schemas"]["ErrorDetail"]["properties"]["code"]["enum"],
        json!(errors::codes())
    );
}

/// The document is valid OpenAPI 3.1, every schema in it valid for the
/// OpenAPI dialect and compiling with its references resolved, and nothing
/// fetched from outside; each check refuses a broken document
#[test]
fn the_document_is_valid_openapi_3_1() {
    for exposure in EXPOSURES {
        let doc = openapi::document(exposure);
        let v = Vendored::new(doc.clone());
        let oas = v.validator(&json!({"$ref": OAS_URI})).unwrap();
        assert_eq!(errors_of(&oas, &doc), Vec::<String>::new());
        let dialect = v.validator(&json!({"$ref": DIALECT_URI})).unwrap();
        for (at, schema) in schemas_in(&doc) {
            assert_eq!(errors_of(&dialect, &schema), Vec::<String>::new(), "{at}");
            v.validator(&absolute(&schema))
                .unwrap_or_else(|e| panic!("{at}: {e}"));
        }
        assert_eq!(dangling(&doc), Vec::<String>::new());
        assert_eq!(v.misses(), Vec::<String>::new());
    }
    let doc = openapi::document(Exposure::default());
    assert_eq!(
        doc["paths"]["/v1/crashes/{id}"]["get"]["parameters"][0]["description"],
        "The crash record id, from `/v1/crashes`"
    );
    let v = Vendored::new(doc.clone());
    let oas = v.validator(&json!({"$ref": OAS_URI})).unwrap();
    let dialect = v.validator(&json!({"$ref": DIALECT_URI})).unwrap();
    let mut no_openapi = doc.clone();
    no_openapi.as_object_mut().unwrap().remove("openapi");
    assert!(!oas.is_valid(&no_openapi));
    let mut no_info = doc.clone();
    no_info.as_object_mut().unwrap().remove("info");
    assert!(!oas.is_valid(&no_info));
    let mut bad_status = doc.clone();
    bad_status["paths"]["/v1/whoami"]["get"]["responses"]["2000"] = json!({"description": "x"});
    assert!(!oas.is_valid(&bad_status));
    let mut bad_path = doc.clone();
    bad_path["paths"]["v1/whoami"] = bad_path["paths"]["/v1/whoami"].clone();
    assert!(!oas.is_valid(&bad_path));
    assert!(!dialect.is_valid(&json!({"type": "objekt"})));
    // A dangling reference, in a response and inside a component
    let mut broken = doc.clone();
    broken["paths"]["/v1/whoami"]["get"]["responses"]["200"]["content"]["application/json"]["schema"] =
        json!({"$ref": "#/components/schemas/Nope"});
    broken["components"]["schemas"]["Jobs"]["properties"]["jobs"]["items"] =
        json!({"$ref": "#/components/schemas/Gone"});
    assert_eq!(
        dangling(&broken),
        vec![
            "#/components/schemas/Gone".to_string(),
            "#/components/schemas/Nope".to_string()
        ]
    );
    let v = Vendored::new(broken);
    assert!(
        v.validator(&absolute(&json!({"$ref": "#/components/schemas/Jobs"})))
            .is_err()
    );
}

/// The checker [`Api::call`] runs is not vacuous: it refuses a field the
/// document does not know, a status, an error code and a media type it
/// does not declare, and a missing guard header
#[test]
fn the_conformance_check_refuses_what_the_document_does_not_say() {
    let doc = serde_json::to_string(&openapi::document(Exposure::default())).unwrap();
    let c = Conformance::new(&doc, Exposure::default());
    let mut h = HeaderMap::new();
    for (k, v) in [
        ("content-type", "application/json"),
        ("x-request-id", "r"),
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
    ] {
        h.insert(k, v.parse().unwrap());
    }
    let whoami = json!({"principal": {"key_id": "k", "scopes": ["read"], "expires_at": null}, "request_id": "r"});
    let refused = |method: &str, path: &str, status: u16, headers: &HeaderMap, body: &Value| {
        let bytes = body.to_string();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            c.check(
                method,
                path,
                StatusCode::from_u16(status).unwrap(),
                headers,
                bytes.as_bytes(),
            )
        }))
        .is_err()
    };
    assert!(!refused("GET", "/v1/whoami", 200, &h, &whoami));
    let mut extra = whoami.clone();
    extra["principal"]["secret"] = json!("x");
    assert!(refused("GET", "/v1/whoami", 200, &h, &extra));
    let error = |code: &str| json!({"error": {"code": code, "message": "m", "request_id": "r"}});
    assert!(!refused(
        "GET",
        "/v1/tenants/a",
        404,
        &h,
        &error("not_found")
    ));
    assert!(refused("GET", "/v1/tenants/a", 409, &h, &error("exists")));
    assert!(refused("GET", "/v1/tenants/a", 404, &h, &error("busy")));
    assert!(refused(
        "GET",
        "/v1/tenants/a",
        404,
        &h,
        &error("no_such_code")
    ));
    let mut text = h.clone();
    text.insert("content-type", "text/plain".parse().unwrap());
    assert!(refused("GET", "/v1/whoami", 200, &text, &whoami));
    let mut bare = h.clone();
    bare.remove("x-request-id");
    assert!(refused("GET", "/v1/whoami", 200, &bare, &whoami));
    let mut tagged = h.clone();
    tagged.insert("etag", "\"t\"".parse().unwrap());
    assert!(refused("GET", "/v1/whoami", 200, &tagged, &whoami));
    assert!(refused("PUT", "/v1/whoami", 200, &h, &whoami));
    // An operation that failed is a 200 with an `error`, not an error
    let failed = json!({"request_id": "r", "state": "rejected", "error": {"code": "internal", "message": "m"}});
    assert!(!refused("GET", "/v1/operations/r", 200, &h, &failed));
}

async fn expect(
    api: &Api,
    want: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (HeaderMap, Value) {
    let (s, h, b) = api
        .call(method, path, Some(&api.admin), headers, body)
        .await;
    assert_eq!(s.as_u16(), want, "{method} {path}: {b}");
    (h, b)
}

/// Every operation's documented success responses, each from a request
/// made to produce it, every body checked by [`Conformance`].
/// `/v1/tenants/{id}/tables` needs a source database: `api_e2e` checks it
#[tokio::test]
async fn every_operation_answers_its_documented_successes() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    for path in [
        "/livez",
        "/v1/whoami",
        "/v1/health/live",
        "/v1/health/ready",
        "/v1/health",
        "/v1/status",
        "/v1/metrics",
        "/v1/errors?level=error&limit=5",
        "/v1/jobs",
        "/v1/tenants",
        "/v1/tenants/a",
        "/v1/tenants/a/jobs",
        "/v1/tenants/a/errors",
        "/v1/openapi.json",
        "/v1/docs",
        "/v1/config",
        "/v1/tenants-deleted",
    ] {
        expect(&api, 200, "GET", path, &[], None).await;
    }
    let (_, crashes) = expect(&api, 200, "GET", "/v1/crashes", &[], None).await;
    let crash = crashes["crashes"][0]["id"].as_str().unwrap().to_string();
    expect(&api, 200, "GET", &format!("/v1/crashes/{crash}"), &[], None).await;

    let b = json!({"id": "b", "dbname": "b_db", "ch": {"host": "ch", "database": "b"}});
    expect(
        &api,
        200,
        "POST",
        "/v1/tenants?dry_run=true",
        &[],
        Some(b.clone()),
    )
    .await;
    let (_, out) = expect(&api, 202, "POST", "/v1/tenants", &[], Some(b)).await;
    let created = out["request_id"].as_str().unwrap().to_string();
    expect(
        &api,
        200,
        "GET",
        &format!("/v1/operations/{created}"),
        &[],
        None,
    )
    .await;
    let decl = json!({"dbname": "b_db", "max_lag_bytes": 5, "ch": {"host": "ch", "database": "b"}});
    for (method, path, body) in [
        ("PUT", "/v1/tenants/b?", Some(decl)),
        ("PATCH", "/v1/tenants/b?", Some(json!({"max_lag_bytes": 9}))),
        (
            "POST",
            "/v1/tenants/b/detach?",
            Some(json!({"reason": "maintenance"})),
        ),
        (
            "POST",
            "/v1/tenants/b/attach?acknowledge_resync=true&",
            None,
        ),
    ] {
        let etag = api.etag("b").await;
        let headers = [("if-match", etag.as_str())];
        expect(
            &api,
            200,
            method,
            &format!("{path}dry_run=true"),
            &headers,
            body.clone(),
        )
        .await;
        expect(
            &api,
            202,
            method,
            path.trim_end_matches(['?', '&']),
            &headers,
            body,
        )
        .await;
    }
    let (_, intent) = expect(&api, 200, "POST", "/v1/tenants/b/delete-intent", &[], None).await;
    let headers = [
        ("if-match", intent["etag"].as_str().unwrap()),
        (
            "x-confirm-delete",
            intent["confirm_token"].as_str().unwrap(),
        ),
    ];
    expect(&api, 202, "DELETE", "/v1/tenants/b", &headers, None).await;
    expect(&api, 200, "GET", "/v1/tenants-deleted", &[], None).await;
    expect(&api, 200, "GET", "/v1/tenants-deleted/b", &[], None).await;
    let restore = "/v1/tenants-deleted/b/restore";
    expect(
        &api,
        200,
        "POST",
        &format!("{restore}?dry_run=true"),
        &[],
        None,
    )
    .await;
    expect(&api, 202, "POST", restore, &[], None).await;
    for path in ["/v1/stream/pause", "/v1/stream/resume", "/v1/reload"] {
        expect(&api, 202, "POST", path, &[], None).await;
    }
    expect(&api, 200, "GET", "/v1/audit?tenant=b&limit=50", &[], None).await;
    // Not ready: the session has been restarting past the grace period
    api.state
        .ctx
        .metrics
        .set(MetricsSnapshot {
            session_state: "restarting",
            uptime_seconds: 10_000,
            ..MetricsSnapshot::default()
        })
        .await;
    let (_, ready) = expect(&api, 503, "GET", "/v1/health/ready", &[], None).await;
    assert_eq!(ready["ready"], false);

    let seen = api.conformance.seen();
    for r in ROUTES
        .iter()
        .filter(|r| r.doc.operation_id != "tenant_tables")
    {
        for reply in r.doc.replies {
            assert!(
                seen.contains(&(r.doc.operation_id.to_string(), reply.status)),
                "{} {}: no {} seen",
                r.method,
                r.path,
                reply.status
            );
        }
    }
}

/// A request body, what the operation's schema says of it, and what the
/// API answers
struct Fixture {
    method: &'static str,
    path: &'static str,
    body: Value,
    /// The schema accepts it
    valid: bool,
    /// Status and error code
    want: (u16, Option<&'static str>),
}

const fn fx(
    method: &'static str,
    path: &'static str,
    body: Value,
    valid: bool,
    want: (u16, Option<&'static str>),
) -> Fixture {
    Fixture {
        method,
        path,
        body,
        valid,
        want,
    }
}

async fn run(api: &Api, fixtures: Vec<Fixture>) {
    let doc: Value = serde_json::from_str(&api.state.docs.json).unwrap();
    for f in fixtures {
        let what = format!("{} {} {}", f.method, f.path, f.body);
        let route = route_of(f.method, f.path.split('?').next().unwrap()).expect("a route");
        let schema =
            &operation(&doc, route)["requestBody"]["content"]["application/json"]["schema"];
        assert_eq!(
            api.conformance.is_valid(schema, &f.body),
            f.valid,
            "{what}: the schema {}",
            if f.valid { "refuses it" } else { "accepts it" }
        );
        let etag = if f.path.starts_with("/v1/tenants/a") {
            api.etag("a").await
        } else {
            String::new()
        };
        let (s, _, body) = api
            .call(
                f.method,
                f.path,
                Some(&api.admin),
                &[("if-match", &etag)],
                Some(f.body),
            )
            .await;
        assert_eq!(
            (
                s.as_u16(),
                body.pointer("/error/code").and_then(Value::as_str)
            ),
            f.want,
            "{what}: {body}"
        );
    }
}

/// Request bodies through each operation's schema and through the API:
/// what the schema refuses the API refuses, and each operation has a body
/// both accept. A few refusals are semantic (another tenant's id, a
/// restore as active without the acknowledgement): the schema cannot say
/// them
#[tokio::test]
async fn request_bodies_agree_with_their_schemas() {
    let api = Api::new(false, generous()).await;
    api.attach_on_disk("a").await;
    let ch = json!({"host": "ch", "database": "c"});
    let create = "/v1/tenants?dry_run=true";
    let a = "/v1/tenants/a?dry_run=true";
    run(
        &api,
        vec![
            fx(
                "POST",
                create,
                json!({"id": "c", "dbname": "c_db", "ch": ch}),
                true,
                (200, None),
            ),
            fx(
                "POST",
                create,
                json!({"dbname": "c_db", "ch": ch}),
                false,
                (400, Some("invalid_body")),
            ),
            fx(
                "POST",
                create,
                json!({"id": "C!", "dbname": "c_db", "ch": ch}),
                false,
                (400, Some("invalid_id")),
            ),
            fx(
                "POST",
                create,
                json!([{"id": "c"}]),
                false,
                (400, Some("invalid_body")),
            ),
            fx(
                "POST",
                create,
                json!({"id": "c", "dbname": "c_db", "state": "paused", "ch": ch}),
                false,
                (400, Some("invalid_tenant")),
            ),
            fx(
                "POST",
                create,
                json!({"id": "c", "dbname": "c_db", "max_lag_bytes": -1, "ch": ch}),
                false,
                (400, Some("invalid_tenant")),
            ),
            fx(
                "PUT",
                a,
                json!({"dbname": "a_db", "ch": {"host": "ch", "database": "a"}}),
                true,
                (200, None),
            ),
            fx(
                "PUT",
                a,
                json!({"id": "a", "dbname": "a_db", "ch": {"host": "ch", "database": "a"}}),
                true,
                (200, None),
            ),
            fx(
                "PUT",
                a,
                json!({"id": "zz", "dbname": "a_db", "ch": {"host": "ch", "database": "a"}}),
                true,
                (400, Some("invalid_id")),
            ),
            fx(
                "PUT",
                a,
                json!({"ch": {"host": "ch", "database": "a"}}),
                false,
                (400, Some("invalid_tenant")),
            ),
            fx(
                "PATCH",
                a,
                json!({"max_lag_bytes": 1000}),
                true,
                (200, None),
            ),
            fx("PATCH", a, json!([1]), false, (400, Some("invalid_body"))),
            fx(
                "POST",
                "/v1/tenants/a/detach?dry_run=true",
                json!({"desired": "anything", "reason": "maintenance"}),
                true,
                (200, None),
            ),
            fx(
                "POST",
                "/v1/tenants/a/detach?dry_run=true",
                json!({"desired": null, "reason": null}),
                true,
                (200, None),
            ),
            fx(
                "POST",
                "/v1/tenants/a/detach?dry_run=true",
                json!({"foo": 1}),
                false,
                (422, Some("invalid_request")),
            ),
            fx(
                "POST",
                "/v1/tenants/a/detach?dry_run=true",
                json!({"acknowledge_resync": "yes"}),
                false,
                (422, Some("invalid_request")),
            ),
            fx(
                "POST",
                "/v1/tenants/a/attach?dry_run=true",
                json!({"acknowledge_resync": true}),
                true,
                (200, None),
            ),
        ],
    )
    .await;
    // Restores need a tombstone
    let (_, intent) = expect(&api, 200, "POST", "/v1/tenants/a/delete-intent", &[], None).await;
    let headers = [
        ("if-match", intent["etag"].as_str().unwrap()),
        (
            "x-confirm-delete",
            intent["confirm_token"].as_str().unwrap(),
        ),
    ];
    expect(&api, 202, "DELETE", "/v1/tenants/a", &headers, None).await;
    let restore = "/v1/tenants-deleted/a/restore?dry_run=true";
    run(
        &api,
        vec![
            fx("POST", restore, json!({}), true, (200, None)),
            fx(
                "POST",
                restore,
                json!({"desired": null, "reason": null}),
                true,
                (200, None),
            ),
            fx(
                "POST",
                restore,
                json!({"desired": "active", "acknowledge_resync": true}),
                true,
                (200, None),
            ),
            fx(
                "POST",
                restore,
                json!({"desired": "active"}),
                true,
                (409, Some("resync_not_acknowledged")),
            ),
            fx(
                "POST",
                restore,
                json!({"desired": "bogus"}),
                false,
                (400, Some("invalid_body")),
            ),
            fx(
                "POST",
                restore,
                json!({"extra": 1}),
                false,
                (422, Some("invalid_request")),
            ),
        ],
    )
    .await;
}

/// Without a key only what the flags open answers, and the document says
/// so: `/livez` with `--api-public-livez`, `GET` of the docs with
/// `--api-public-docs`. Other methods on those paths are not routes
#[tokio::test]
async fn the_docs_are_public_only_with_their_flag() {
    for exposure in EXPOSURES {
        let api = Api::exposed(exposure, generous()).await;
        let doc: Value = serde_json::from_str(&api.state.docs.json).unwrap();
        for r in ROUTES {
            let public = (r.class == Class::Public && exposure.public_livez)
                || (DOCS_PATHS.contains(&r.path) && exposure.public_docs);
            assert_eq!(exposure.is_public(r), public, "{}", r.path);
            let security = operation(&doc, r)["security"].as_array().unwrap().clone();
            assert_eq!(
                security.contains(&json!({})),
                public,
                "{exposure:?} {}",
                r.path
            );
            let (s, ..) = api.call(r.method, &concrete(r.path), None, &[], None).await;
            if public {
                assert_eq!(s, StatusCode::OK, "{exposure:?} {}", r.path);
            } else {
                assert_eq!(s, StatusCode::UNAUTHORIZED, "{exposure:?} {}", r.path);
            }
        }
        for path in ["/livez", "/v1/openapi.json", "/v1/docs"] {
            for method in ["HEAD", "POST", "PUT", "DELETE"] {
                let (s, ..) = api.call(method, path, None, &[], None).await;
                assert_eq!(s, StatusCode::UNAUTHORIZED, "{exposure:?} {method} {path}");
                let (s, ..) = api.call(method, path, Some(&api.read), &[], None).await;
                assert_eq!(s, StatusCode::NOT_FOUND, "{exposure:?} {method} {path}");
            }
            // A key reads them whatever the flags
            let (s, ..) = api.get(path, &api.read).await;
            assert_eq!(s, StatusCode::OK, "{exposure:?} {path}");
        }
    }
}

/// Public docs stay behind the per-IP limits: preauth, and their own
/// bucket at the read rate, each exhausted with the other to spare
#[tokio::test(start_paused = true)]
async fn public_docs_are_rate_limited_per_address() {
    let tight = Rate::new(0.01, 2.0);
    let docs = Exposure {
        public_livez: false,
        public_docs: true,
    };
    // Preauth runs out with the docs bucket to spare, and refuses keyed
    // requests from that address too
    let api = Api::exposed(
        docs,
        Rates {
            preauth: tight,
            ..generous()
        },
    )
    .await;
    for _ in 0..2 {
        let (s, ..) = api.call("GET", "/v1/openapi.json", None, &[], None).await;
        assert_eq!(s, StatusCode::OK);
    }
    let (s, h, body) = api.call("GET", "/v1/docs", None, &[], None).await;
    assert_eq!(
        (s, body["error"]["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("rate_limited"))
    );
    assert!(h.contains_key("retry-after"));
    let (s, ..) = api.get("/v1/whoami", &api.read).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    // The docs bucket runs out with preauth to spare; keys and the public
    // `/livez` draw on other buckets
    let api = Api::exposed(
        Exposure {
            public_livez: true,
            public_docs: true,
        },
        Rates {
            read: tight,
            ..generous()
        },
    )
    .await;
    for _ in 0..2 {
        let (s, ..) = api.call("GET", "/v1/docs", None, &[], None).await;
        assert_eq!(s, StatusCode::OK);
    }
    let (s, h, _) = api.call("GET", "/v1/openapi.json", None, &[], None).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert!(h.contains_key("retry-after"));
    // A public route skips the key, so a key does not lift the limit
    let (s, ..) = api.get("/v1/openapi.json", &api.read).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    let (s, ..) = api.get("/v1/whoami", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    let (s, ..) = api.call("GET", "/livez", None, &[], None).await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test(start_paused = true)]
async fn public_docs_charge_both_ip_limits_through_trusted_proxies() {
    for trusted in [false, true] {
        for preauth in [false, true] {
            let tight = Rate::new(0.01, 1.0);
            let mut rates = generous();
            if preauth {
                rates.preauth = tight;
            } else {
                rates.read = tight;
            }
            let mut api = Api::exposed(
                Exposure {
                    public_docs: true,
                    ..Exposure::default()
                },
                rates,
            )
            .await;
            if trusted {
                Arc::make_mut(&mut api.state.cfg).trusted_proxies =
                    vec![crate::ops::api::Cidr::parse("10.0.0.0/8").unwrap()];
                api.router = crate::ops::api::router(api.state.clone());
            }
            for (forwarded, want) in [
                ("192.0.2.9, 192.0.2.1", StatusCode::OK),
                ("192.0.2.8, 192.0.2.1", StatusCode::TOO_MANY_REQUESTS),
                (
                    "192.0.2.8, 192.0.2.2",
                    if trusted {
                        StatusCode::OK
                    } else {
                        StatusCode::TOO_MANY_REQUESTS
                    },
                ),
            ] {
                let (status, _, body) = api
                    .call(
                        "GET",
                        "/v1/docs",
                        None,
                        &[("x-forwarded-for", forwarded)],
                        None,
                    )
                    .await;
                assert_eq!(status, want, "trusted={trusted}, preauth={preauth}: {body}");
                if want == StatusCode::TOO_MANY_REQUESTS {
                    assert_eq!(body["error"]["code"], "rate_limited");
                }
            }
        }
    }
}

/// The served page: the renderer's output for the served document, under
/// the content security policy, self-contained, every operation and
/// schema anchored and every link landing, nested fields linked, and no
/// key or secret in it or in the document
#[tokio::test]
async fn the_page_is_self_contained_and_covers_the_document() {
    let api = Api::new(false, generous()).await;
    let (s, h, page) = api.get("/v1/docs", &api.read).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        h["content-security-policy"],
        docs_html::CONTENT_SECURITY_POLICY
    );
    assert_eq!(h["content-type"], "text/html; charset=utf-8");
    assert_eq!(h["x-content-type-options"], "nosniff");
    let page = page.as_str().unwrap();
    assert_eq!(
        page,
        docs_html::render(&openapi::document(Exposure::default()))
    );
    assert!(page.starts_with("<!doctype html>"));
    assert!(
        page.contains("<code>walshadow-stream api-docs --format json &gt; openapi.json</code>")
    );
    for banned in [
        "<script", "<link", " src=", "url(", "<iframe", "<form", "<img", "http://", "https://",
    ] {
        assert!(!page.contains(banned), "{banned}");
    }
    let doc: Value = serde_json::from_str(&api.state.docs.json).unwrap();
    for r in ROUTES {
        assert!(
            page.contains(&format!("id=\"op-{}\"", r.doc.operation_id)),
            "{}",
            r.doc.operation_id
        );
    }
    for name in doc["components"]["schemas"].as_object().unwrap().keys() {
        assert!(page.contains(&format!("id=\"schema-{name}\"")), "{name}");
    }
    for link in page.split("href=\"#").skip(1) {
        let target = link.split('"').next().unwrap();
        assert!(page.contains(&format!("id=\"{target}\"")), "#{target}");
    }
    for (open, close) in [
        ("<section", "</section>"),
        ("<table>", "</table>"),
        ("<tr>", "</tr>"),
        ("<h2", "</h2>"),
        ("<h3", "</h3>"),
        ("<code>", "</code>"),
        ("<li>", "</li>"),
    ] {
        assert_eq!(
            page.matches(open).count(),
            page.matches(close).count(),
            "{open}"
        );
    }
    // Nested fields link to their own tables
    let detail = page
        .split("id=\"schema-TenantDetail\"")
        .nth(1)
        .unwrap()
        .split("<h3")
        .next()
        .unwrap();
    for field in ["status", "destination", "jobs", "spec"] {
        assert!(detail.contains(&format!("<code>{field}</code>")), "{field}");
    }
    assert!(detail.contains("href=\"#schema-TenantStatus\""));
    assert!(detail.contains("array of <a href=\"#schema-Job\">Job</a>"));
    for secret in [&api.admin, &api.read, &api.admin2, &"hunter2".to_string()] {
        assert!(!page.contains(secret.as_str()));
        assert!(!api.state.docs.json.contains(secret.as_str()));
    }
}

/// Each construct the renderer emits, from a document made to have them,
/// every text escaped
#[test]
fn the_renderer_shows_every_construct_and_escapes_text() {
    let doc = json!({
        "openapi": "3.1.0",
        "info": {
            "title": "T <&> \"q\"",
            "version": "1 <b>",
            "description": "Has `co<de>` and <script>alert(1)</script>",
        },
        "tags": [{"name": "t", "description": "Tag `x`"}, {"name": "empty", "description": ""}],
        "components": {
            "securitySchemes": {
                "bearerAuth": {"type": "http", "scheme": "bearer", "description": "Use <keys>"},
            },
            "schemas": {
                "Leaf": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["n"],
                    "properties": {"n": {
                        "type": "integer", "format": "int64", "minimum": 0,
                        "description": "A <count>",
                    }},
                },
                "Kind": {"type": "string", "enum": ["a", "b<c>"]},
                "Either": {"oneOf": [{"$ref": "#/components/schemas/Leaf"}, {"type": "null"}]},
                "Open": {
                    "type": "object",
                    "properties": {"k": {
                        "type": "string", "default": "d", "pattern": "^[a-z<&]+$",
                        "minLength": 1, "maxLength": 63,
                    }},
                    "additionalProperties": true,
                },
                "Any": {"type": "object", "additionalProperties": true},
                "Composed": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Leaf"},
                        {"type": "object", "properties": {"extra": {"type": ["string", "null"]}}},
                    ],
                    "unevaluatedProperties": false,
                },
                "Cycle": {"allOf": [{"$ref": "#/components/schemas/Cycle"}]},
                "Map": {"type": "object", "additionalProperties": {"$ref": "#/components/schemas/Leaf"}},
                "List": {"type": "array", "items": {"$ref": "#/components/schemas/Kind"}},
            },
        },
        "paths": {"/x/{id}": {"post": {
            "operationId": "op<1>",
            "tags": ["t"],
            "summary": "Sum <i>",
            "description": "First `para`.\n\nSecond <p>.",
            "x-walshadow-scope": "admin",
            "x-walshadow-rate-class": "mutate",
            "x-walshadow-audit-action": "x.<do>",
            "security": [{}, {"bearerAuth": ["admin"]}],
            "parameters": [
                {"name": "id", "in": "path", "required": true, "schema": {"type": "string"}, "description": "The id"},
                {"name": "q<", "in": "query", "required": false, "schema": {"type": "boolean"}, "description": ""},
            ],
            "requestBody": {
                "required": false,
                "description": "Optional",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Composed"}}},
            },
            "responses": {
                "200": {
                    "description": "Fine",
                    "headers": {"ETag": {"description": "A <tag>", "schema": {"type": "string"}}},
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Either"}}},
                },
                "201": {"description": "Map", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Map"}}}},
                "202": {"description": "Open", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Open"}}}},
                "203": {"description": "Any", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Any"}}}},
                "204": {"description": "List", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/List"}}}},
                "205": {"description": "Cycle", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Cycle"}}}},
                "400": {"description": "Bad", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Leaf"}}}},
                "408": {"description": "Late"},
            },
        }}},
    });
    let page = docs_html::render(&doc);
    assert!(!page.contains("<script") && !page.contains("<b>") && !page.contains("<i>"));
    for want in [
        // Header, security, navigation
        "<title>T &lt;&amp;&gt; &quot;q&quot; 1 &lt;b&gt;</title>",
        "Has <code>co&lt;de&gt;</code> and &lt;script&gt;alert(1)&lt;/script&gt;",
        "<code>bearerAuth</code>: HTTP bearer · Use &lt;keys&gt;",
        "<a href=\"#op-op&lt;1&gt;\"><span class=\"m m-post\">POST</span> <span class=\"path\">/x/{id}</span></a>",
        // The operation
        "<section class=\"op\" id=\"op-op&lt;1&gt;\">",
        "<span class=\"badge\">scope: admin</span>",
        "<span class=\"badge\">rate: mutate</span><span class=\"badge public\">public</span>",
        "<p><strong>Sum &lt;i&gt;</strong></p>",
        "<p>First <code>para</code>.</p><p>Second &lt;p&gt;.</p>",
        "Audited as <code>x.&lt;do&gt;</code>",
        "<td class=\"name\"><code>id</code><span class=\"req\">required</span></td><td>path</td><td>string</td>",
        "<td class=\"name\"><code>q&lt;</code></td><td>query</td><td>boolean</td>",
        "<h4>Request body (optional)</h4>",
        // A composition's fields merged, closed
        "<code>n</code><span class=\"req\">required</span></td><td>integer (int64)</td><td>A &lt;count&gt;. min <code>0</code>. format <code>int64</code></td>",
        "<code>extra</code></td><td>string or null</td>",
        // Responses
        "<code>application/json</code>: <a href=\"#schema-Either\">Either</a>",
        "One of: <a href=\"#schema-Leaf\">Leaf</a> · null",
        "Headers: <code>ETag</code> (A &lt;tag&gt;)",
        "<code>application/json</code>: <a href=\"#schema-Map\">Map</a>",
        "Other keys are allowed.",
        "An object; any keys.",
        "Default: <code>d</code>",
        "min length <code>1</code>. max length <code>63</code>. pattern <code>^[a-z&lt;&amp;]+$</code>",
        "<p><strong>408</strong> Late</p>",
        // Schemas
        "<h3 id=\"schema-Kind\">Kind</h3><p>Values: <code>a</code>, <code>b&lt;c&gt;</code></p>",
        "<h3 id=\"schema-Map\">Map</h3>",
        "<h3 id=\"schema-List\">List</h3>",
        "<h3 id=\"schema-Cycle\">Cycle</h3>",
    ] {
        assert!(page.contains(want), "missing {want}\n{page}");
    }
    let leaf = page.split("id=\"schema-Leaf\"").nth(1).unwrap();
    let leaf = leaf.split("<h3").next().unwrap();
    assert!(!leaf.contains("Other keys"));
    let composed = page.split("id=\"schema-Composed\"").nth(1).unwrap();
    let composed = composed.split("<h3").next().unwrap();
    assert!(composed.contains("<code>extra</code>") && !composed.contains("Other keys"));
    // Error bodies get their type, not a field table
    let bad = page.split("<strong>400</strong>").nth(1).unwrap();
    let bad = bad.split("<strong>408</strong>").next().unwrap();
    assert!(bad.contains("href=\"#schema-Leaf\"") && !bad.contains("<table>"));
    // A tag without operations gets no heading
    assert!(!page.contains("id=\"tag-empty\""));
    // `array of` and `map of` types
    let list_type = docs_html::render(&json!({
        "openapi": "3.1.0", "info": {"title": "x", "version": "1"},
        "components": {"schemas": {"L": {
            "type": "object",
            "properties": {
                "l": {"type": "array", "items": {"$ref": "#/components/schemas/L"}},
                "m": {"type": "object", "additionalProperties": {"type": "integer"}},
                "a": {"type": "array"},
                "v": {},
            },
        }}},
        "paths": {},
    }));
    for want in [
        "<code>l</code></td><td>array of <a href=\"#schema-L\">L</a></td>",
        "<code>m</code></td><td>map of string to integer</td>",
        "<code>a</code></td><td>array of any</td>",
        "<code>v</code></td><td>any</td>",
    ] {
        assert!(list_type.contains(want), "missing {want}\n{list_type}");
    }
}
