//! Every error code the API and the control socket answer with, and the
//! statuses each comes with
//!
//! The error producers ([`error_response`](super::error_response), the
//! handlers' `fail`, [`ServiceError::new`](crate::ops::tenant_service::ServiceError::new))
//! check membership in debug builds, the OpenAPI document's `code` enum
//! and per-route error responses come from here, and a test holds
//! `docs/api.md`'s table to it row for row. Test-enforced, not by
//! construction: a producer no test reaches could still answer a pair
//! missing here

/// `(code, status, meaning)`, one row per pair. A code may carry several
/// statuses
pub const ERROR_CODES: &[(&str, u16, &str)] = &[
    (
        "invalid_body",
        400,
        "The body or query does not have the route's shape",
    ),
    (
        "invalid_id",
        400,
        "A tenant id is not 1–63 characters of `[a-z0-9_-]`, is reserved, or is not the \
         path's",
    ),
    (
        "invalid_tenant",
        400,
        "The tenant declaration does not parse",
    ),
    (
        "invalid_request",
        400,
        "Rejected before the route ran: malformed JSON, or a query or path it cannot read",
    ),
    (
        "invalid_request",
        413,
        "Rejected before the route ran: the body is over 1 MiB",
    ),
    (
        "invalid_request",
        415,
        "Rejected before the route ran: the body is not `Content-Type: application/json`",
    ),
    (
        "invalid_request",
        422,
        "Rejected before the route ran: the JSON does not fit the route's body",
    ),
    ("unauthenticated", 401, "No valid key"),
    ("forbidden", 403, "The key lacks the route's scope"),
    (
        "not_found",
        404,
        "No such route, tenant, tombstone, operation or crash record",
    ),
    (
        "exists",
        409,
        "The tenant exists already, also when restoring onto a live id",
    ),
    (
        "resync_not_acknowledged",
        409,
        "The change attaches the tenant afresh; resend with `acknowledge_resync`",
    ),
    (
        "rebind_pending",
        409,
        "A re-bind of the tenant is still pending",
    ),
    (
        "change_rejected",
        409,
        "The daemon cannot apply this change while running, or the new Snowflake state \
         directory is unusable",
    ),
    (
        "conflict_after_failed_apply",
        409,
        "A change the daemon refused could not be rolled back: someone changed the tenant \
         meanwhile",
    ),
    (
        "confirmation_invalid",
        409,
        "The delete token is unknown, used, or void",
    ),
    (
        "managed_outside_api",
        409,
        "The tenant is declared outside the registry the API edits",
    ),
    (
        "tenants_not_configured",
        409,
        "No `[tenants]`: the daemon runs the single-database layout",
    ),
    (
        "invalid_config",
        409,
        "The running config's tenants do not parse",
    ),
    (
        "confirmation_expired",
        410,
        "The delete token expired; ask again",
    ),
    (
        "precondition_failed",
        412,
        "`If-Match` names an ETag the tenant no longer has",
    ),
    (
        "invalid_config",
        422,
        "The config the change would produce does not validate",
    ),
    (
        "apply_refused",
        422,
        "The tenant's resolver refused the change; it was rolled back",
    ),
    ("precondition_required", 428, "`If-Match` is missing"),
    (
        "confirmation_required",
        428,
        "`DELETE` without `X-Confirm-Delete`",
    ),
    (
        "rate_limited",
        429,
        "Over a rate limit; wait `Retry-After` seconds",
    ),
    (
        "internal",
        500,
        "An unexpected failure; the message says what",
    ),
    (
        "audit_unavailable",
        503,
        "The audit log refused the attempt, so nothing changed",
    ),
    (
        "detach_not_recorded",
        503,
        "The detach could not be recorded, so nothing changed; retry",
    ),
    (
        "registry_unavailable",
        503,
        "The SQL tenant registry is unreachable",
    ),
    (
        "source_unavailable",
        503,
        "The source database is unreachable",
    ),
    ("busy", 503, "Too many requests in flight; retry shortly"),
];

/// Whether `(code, status)` is a pair the API documents
pub fn known(code: &str, status: u16) -> bool {
    ERROR_CODES
        .iter()
        .any(|(c, s, _)| *c == code && *s == status)
}

/// The distinct codes, in table order
pub fn codes() -> Vec<&'static str> {
    let mut out: Vec<&'static str> = Vec::new();
    for (c, ..) in ERROR_CODES {
        if !out.contains(c) {
            out.push(c);
        }
    }
    out
}

/// Panics in debug builds on a pair the table does not list, so a test
/// reaching a new error fails until the table and the docs learn it
pub fn check(code: &str, status: u16) {
    debug_assert!(
        known(code, status),
        "error ({code}, {status}) is missing from ops::api::errors::ERROR_CODES"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_are_unique_and_codes_snake_case() {
        let mut seen = std::collections::BTreeSet::new();
        for (c, s, meaning) in ERROR_CODES {
            assert!(seen.insert((c, s)), "({c}, {s}) twice");
            assert!(
                c.chars().all(|ch| ch.is_ascii_lowercase() || ch == '_'),
                "{c}"
            );
            assert!((400..600).contains(s), "{c} {s}");
            assert!(!meaning.is_empty());
        }
        assert!(known("invalid_config", 409) && known("invalid_config", 422));
        assert!(!known("invalid_config", 400));
        assert_eq!(
            codes().iter().filter(|c| **c == "invalid_request").count(),
            1
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "missing from ops::api::errors::ERROR_CODES")]
    fn an_unlisted_pair_panics_in_debug_builds() {
        check("no_such_code", 418);
    }
}
