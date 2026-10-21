//! Every route the API serves, with its scope and rate class
//!
//! The router is built from [`ROUTES`] and nothing else, and the guard
//! middleware authorizes against it, so a route cannot exist without a
//! declared scope.

use super::keys::Scope;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Route {
    pub method: &'static str,
    /// axum path syntax
    pub path: &'static str,
    pub scope: Scope,
    pub class: Class,
    /// Audit action of a mutation
    pub action: Option<&'static str>,
}

const fn read(method: &'static str, path: &'static str) -> Route {
    Route {
        method,
        path,
        scope: Scope::Read,
        class: Class::Read,
        action: None,
    }
}

const fn admin_read(path: &'static str) -> Route {
    Route {
        method: "GET",
        path,
        scope: Scope::Admin,
        class: Class::Read,
        action: None,
    }
}

const fn mutate(method: &'static str, path: &'static str, action: &'static str) -> Route {
    Route {
        method,
        path,
        scope: Scope::Admin,
        class: Class::Mutate,
        action: Some(action),
    }
}

pub const ROUTES: &[Route] = &[
    Route {
        method: "GET",
        path: "/livez",
        scope: Scope::Read,
        class: Class::Public,
        action: None,
    },
    read("GET", "/v1/whoami"),
    read("GET", "/v1/health/live"),
    read("GET", "/v1/health/ready"),
    read("GET", "/v1/health"),
    read("GET", "/v1/status"),
    read("GET", "/v1/metrics"),
    read("GET", "/v1/errors"),
    read("GET", "/v1/crashes"),
    read("GET", "/v1/crashes/{id}"),
    read("GET", "/v1/jobs"),
    read("GET", "/v1/tenants"),
    read("GET", "/v1/tenants/{id}"),
    read("GET", "/v1/tenants/{id}/jobs"),
    read("GET", "/v1/tenants/{id}/errors"),
    Route {
        method: "GET",
        path: "/v1/tenants/{id}/tables",
        scope: Scope::Read,
        class: Class::Expensive,
        action: None,
    },
    admin_read("/v1/config"),
    admin_read("/v1/audit"),
    admin_read("/v1/operations/{request_id}"),
    admin_read("/v1/tenants-deleted"),
    admin_read("/v1/tenants-deleted/{id}"),
    mutate("POST", "/v1/tenants", "tenant.create"),
    mutate("PUT", "/v1/tenants/{id}", "tenant.update"),
    mutate("PATCH", "/v1/tenants/{id}", "tenant.patch"),
    mutate("POST", "/v1/tenants/{id}/detach", "tenant.detach"),
    mutate("POST", "/v1/tenants/{id}/attach", "tenant.attach"),
    mutate(
        "POST",
        "/v1/tenants/{id}/delete-intent",
        "tenant.delete_intent",
    ),
    mutate("DELETE", "/v1/tenants/{id}", "tenant.delete"),
    mutate("POST", "/v1/tenants-deleted/{id}/restore", "tenant.restore"),
    mutate("POST", "/v1/stream/pause", "stream.pause"),
    mutate("POST", "/v1/stream/resume", "stream.resume"),
    mutate("POST", "/v1/reload", "config.reload"),
];

/// The route `method` and the matched `path` name
pub fn find(method: &str, path: &str) -> Option<&'static Route> {
    ROUTES.iter().find(|r| r.method == method && r.path == path)
}
