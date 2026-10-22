//! Every route the API serves, with its scope and rate class
//!
//! The router is built from [`ROUTES`] and nothing else, and the guard
//! middleware authorizes against it, so a route cannot exist without a
//! declared scope.

use super::keys::Scope;
use super::openapi::RouteDoc;
use super::route_docs as d;

/// Rate-limit bucket a route draws from
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Cheap reads
    Read,
    /// Reads that open a source connection
    Expensive,
    /// Mutations
    Mutate,
    /// `/livez`, unauthenticated only with `--api-public-livez`
    Public,
}

#[derive(Clone, Copy, Debug)]
pub struct Route {
    pub method: &'static str,
    /// axum path syntax
    pub path: &'static str,
    pub scope: Scope,
    pub class: Class,
    /// Audit action of a mutation
    pub action: Option<&'static str>,
    /// What the OpenAPI document says about it; required, so no route goes
    /// undocumented
    pub doc: RouteDoc,
}

/// One route per method and path
impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.method == other.method && self.path == other.path
    }
}

impl Eq for Route {}

const fn read(method: &'static str, path: &'static str, doc: RouteDoc) -> Route {
    Route {
        method,
        path,
        scope: Scope::Read,
        class: Class::Read,
        action: None,
        doc,
    }
}

const fn admin_read(path: &'static str, doc: RouteDoc) -> Route {
    Route {
        method: "GET",
        path,
        scope: Scope::Admin,
        class: Class::Read,
        action: None,
        doc,
    }
}

const fn mutate(
    method: &'static str,
    path: &'static str,
    action: &'static str,
    doc: RouteDoc,
) -> Route {
    Route {
        method,
        path,
        scope: Scope::Admin,
        class: Class::Mutate,
        action: Some(action),
        doc,
    }
}

pub const ROUTES: &[Route] = &[
    Route {
        method: "GET",
        path: "/livez",
        scope: Scope::Read,
        class: Class::Public,
        action: None,
        doc: d::LIVEZ,
    },
    read("GET", "/v1/whoami", d::WHOAMI),
    read("GET", "/v1/health/live", d::HEALTH_LIVE),
    read("GET", "/v1/health/ready", d::HEALTH_READY),
    read("GET", "/v1/health", d::HEALTH),
    read("GET", "/v1/status", d::STATUS),
    read("GET", "/v1/metrics", d::METRICS),
    read("GET", "/v1/errors", d::ERRORS),
    read("GET", "/v1/crashes", d::CRASHES),
    read("GET", "/v1/crashes/{id}", d::CRASH),
    read("GET", "/v1/jobs", d::JOBS),
    read("GET", "/v1/tenants", d::TENANTS),
    read("GET", "/v1/tenants/{id}", d::TENANT),
    read("GET", "/v1/tenants/{id}/jobs", d::TENANT_JOBS),
    read("GET", "/v1/tenants/{id}/errors", d::TENANT_ERRORS),
    Route {
        method: "GET",
        path: "/v1/tenants/{id}/tables",
        scope: Scope::Read,
        class: Class::Expensive,
        action: None,
        doc: d::TENANT_TABLES,
    },
    read("GET", "/v1/openapi.json", d::OPENAPI),
    read("GET", "/v1/docs", d::DOCS),
    admin_read("/v1/config", d::CONFIG),
    admin_read("/v1/audit", d::AUDIT),
    admin_read("/v1/operations/{request_id}", d::OPERATION),
    admin_read("/v1/tenants-deleted", d::DELETED),
    admin_read("/v1/tenants-deleted/{id}", d::DELETED_ONE),
    mutate("POST", "/v1/tenants", "tenant.create", d::CREATE),
    mutate("PUT", "/v1/tenants/{id}", "tenant.update", d::REPLACE),
    mutate("PATCH", "/v1/tenants/{id}", "tenant.patch", d::PATCH),
    mutate(
        "POST",
        "/v1/tenants/{id}/detach",
        "tenant.detach",
        d::DETACH,
    ),
    mutate(
        "POST",
        "/v1/tenants/{id}/attach",
        "tenant.attach",
        d::ATTACH,
    ),
    mutate(
        "POST",
        "/v1/tenants/{id}/delete-intent",
        "tenant.delete_intent",
        d::DELETE_INTENT,
    ),
    mutate("DELETE", "/v1/tenants/{id}", "tenant.delete", d::DELETE),
    mutate(
        "POST",
        "/v1/tenants-deleted/{id}/restore",
        "tenant.restore",
        d::RESTORE,
    ),
    mutate("POST", "/v1/stream/pause", "stream.pause", d::PAUSE),
    mutate("POST", "/v1/stream/resume", "stream.resume", d::RESUME),
    mutate("POST", "/v1/reload", "config.reload", d::RELOAD),
];

/// The route `method` and the matched `path` name
pub fn find(method: &str, path: &str) -> Option<&'static Route> {
    ROUTES.iter().find(|r| r.method == method && r.path == path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_match_the_scope_and_audit_contract() {
        for route in ROUTES {
            let expected = match route.class {
                Class::Mutate => mutate(
                    route.method,
                    route.path,
                    route.action.expect("audit action"),
                    route.doc,
                ),
                Class::Read if route.scope == Scope::Admin => admin_read(route.path, route.doc),
                Class::Read => read(route.method, route.path, route.doc),
                Class::Public => {
                    assert_eq!(
                        (route.method, route.path, route.scope),
                        ("GET", "/livez", Scope::Read)
                    );
                    assert!(route.action.is_none());
                    continue;
                }
                Class::Expensive => {
                    assert_eq!(route.scope, Scope::Read);
                    assert!(route.action.is_none());
                    continue;
                }
            };
            assert_eq!(
                (
                    expected.method,
                    expected.path,
                    expected.scope,
                    expected.class,
                    expected.action
                ),
                (
                    route.method,
                    route.path,
                    route.scope,
                    route.class,
                    route.action
                )
            );
        }
    }
}
