//! What the OpenAPI document says about each route: one [`RouteDoc`] per
//! [`ROUTES`](super::routes::ROUTES) row, which cannot be declared without
//! one. Errors are the route's own `(code, status)` pairs; the guard's are
//! added for every route

use super::openapi::{Body, In, Kind, Media, Param, Reply, RouteDoc, component};
use super::schema;

const fn ok(description: &'static str, schema: super::openapi::SchemaFn) -> Reply {
    Reply {
        status: 200,
        description,
        media: Media::Json,
        schema: Some(schema),
        etag: false,
    }
}

const fn text(status: u16, description: &'static str, media: Media) -> Reply {
    Reply {
        status,
        description,
        media,
        schema: None,
        etag: false,
    }
}

const fn query(name: &'static str, kind: Kind, description: &'static str) -> Param {
    Param {
        name,
        at: In::Query,
        required: false,
        kind,
        description,
    }
}

const IF_MATCH: Param = Param {
    name: "If-Match",
    at: In::Header,
    required: true,
    kind: Kind::String,
    description: "The tenant's ETag, from a read or the last mutation's response",
};

const CONFIRM_DELETE: Param = Param {
    name: "X-Confirm-Delete",
    at: In::Header,
    required: true,
    kind: Kind::String,
    description: "The token from `POST /v1/tenants/{id}/delete-intent`",
};

const DRY_RUN: Param = query(
    "dry_run",
    Kind::Boolean,
    "Validate and classify the change without applying it: 200 instead of 202, nothing \
     audited",
);

const ACK: Param = query(
    "acknowledge_resync",
    Kind::Boolean,
    "Accept that the change attaches the tenant afresh with fresh initial loads; the body \
     field of the same name works too",
);

const MUTATION_QUERY: &[Param] = &[DRY_RUN, ACK];

const ERRORS_QUERY: &[Param] = &[
    query(
        "level",
        Kind::String,
        "`error` keeps only errors; anything else keeps warnings and errors",
    ),
    query("tenant", Kind::String, "Only events naming this tenant"),
    query(
        "target",
        Kind::String,
        "Only events whose target starts with this",
    ),
    query(
        "since_seq",
        Kind::Integer,
        "Only events after this sequence number",
    ),
    query(
        "limit",
        Kind::Integer,
        "At most this many; default 100, at most 1000",
    ),
];

const TENANT_ERRORS_QUERY: &[Param] = &[
    ERRORS_QUERY[0],
    ERRORS_QUERY[2],
    ERRORS_QUERY[3],
    ERRORS_QUERY[4],
];

const AUDIT_QUERY: &[Param] = &[
    query(
        "since_seq",
        Kind::Integer,
        "Only records after this sequence number",
    ),
    query("tenant", Kind::String, "Only records about this tenant"),
    query(
        "action",
        Kind::String,
        "Only records of this action, as `tenant.create`",
    ),
    query("limit", Kind::Integer, "At most this many, newest first"),
];

/// The config could not be read, or its tenants do not parse
const CONFIG_READ: &[(&str, u16)] = &[("internal", 500), ("invalid_config", 409)];

const TENANT_READ: &[(&str, u16)] = &[
    ("internal", 500),
    ("invalid_config", 409),
    ("not_found", 404),
];

const OUTCOME_ACCEPTED: Reply = Reply {
    status: 202,
    description: "Applied: persisted and published. The session reconciles it next; the \
        tenant's status shows when it has",
    media: Media::Json,
    schema: Some(component::<schema::Outcome>),
    etag: true,
};

const OUTCOME_DRY_RUN: Reply = Reply {
    status: 200,
    description: "A dry run: what the change would do. Nothing changed",
    media: Media::Json,
    schema: Some(component::<schema::Outcome>),
    etag: true,
};

const OUTCOMES: &[Reply] = &[OUTCOME_ACCEPTED, OUTCOME_DRY_RUN];

/// A deleted tenant has no `ETag`
const OUTCOME_DELETED: Reply = Reply {
    description: "Deleted: the declaration is gone and a tombstone kept. The session detaches \
        the tenant next",
    etag: false,
    ..OUTCOME_ACCEPTED
};

/// What [`TenantService::mutate`](crate::ops::tenant_service::TenantService)
/// can refuse with
macro_rules! service_errors {
    ($($extra:expr),* $(,)?) => {
        &[
            $($extra,)*
            ("invalid_id", 400),
            ("invalid_tenant", 400),
            ("invalid_body", 400),
            ("not_found", 404),
            ("exists", 409),
            ("resync_not_acknowledged", 409),
            ("rebind_pending", 409),
            ("change_rejected", 409),
            ("conflict_after_failed_apply", 409),
            ("managed_outside_api", 409),
            ("tenants_not_configured", 409),
            ("invalid_config", 409),
            ("precondition_failed", 412),
            ("invalid_config", 422),
            ("apply_refused", 422),
            ("internal", 500),
            ("audit_unavailable", 503),
            ("detach_not_recorded", 503),
            ("registry_unavailable", 503),
        ]
    };
}

const CONFIG_MUTATION_ERRORS: &[(&str, u16)] = &[
    ("invalid_config", 422),
    ("apply_refused", 422),
    ("internal", 500),
    ("audit_unavailable", 503),
];

const CONFIG_OUTCOME: &[Reply] = &[Reply {
    status: 202,
    description: "Published; the session applies it next",
    media: Media::Json,
    schema: Some(component::<schema::ConfigOutcome>),
    etag: false,
}];

pub const LIVEZ: RouteDoc = RouteDoc {
    operation_id: "livez",
    tag: "health",
    summary: "Liveness probe",
    description: "A constant `ok`. Public with `--api-public-livez`, for orchestrator probes \
        that cannot present a key",
    params: &[],
    body: None,
    replies: &[text(200, "The process answers: `ok`", Media::Text)],
    errors: &[],
};

pub const WHOAMI: RouteDoc = RouteDoc {
    operation_id: "whoami",
    tag: "health",
    summary: "The key this request presented",
    description: "Its id, scopes and expiry",
    params: &[],
    body: None,
    replies: &[ok("The key", component::<schema::Whoami>)],
    errors: &[],
};

pub const HEALTH_LIVE: RouteDoc = RouteDoc {
    operation_id: "health_live",
    tag: "health",
    summary: "The process answers",
    description: "",
    params: &[],
    body: None,
    replies: &[ok("Alive", component::<schema::Live>)],
    errors: &[],
};

pub const HEALTH_READY: RouteDoc = RouteDoc {
    operation_id: "health_ready",
    tag: "health",
    summary: "Readiness",
    description: "200 while the daemon streams, or is paused on purpose; 503 otherwise. \
        `/v1/health` says why",
    params: &[],
    body: None,
    replies: &[
        ok("Ready", component::<schema::Readiness>),
        Reply {
            status: 503,
            description: "Not ready: the same body, with `ready: false`.",
            media: Media::Json,
            schema: Some(component::<schema::Readiness>),
            etag: false,
        },
    ],
    errors: &[],
};

pub const HEALTH: RouteDoc = RouteDoc {
    operation_id: "health",
    tag: "health",
    summary: "Every health check",
    description: "The worst check decides the overall status; readiness follows from it",
    params: &[],
    body: None,
    replies: &[ok("The checks", component::<super::health::Report>)],
    errors: &[],
};

pub const STATUS: RouteDoc = RouteDoc {
    operation_id: "status",
    tag: "status",
    summary: "`ctl status` as JSON",
    description: "Plus the epoch and generations of the reload pipeline",
    params: &[],
    body: None,
    replies: &[ok("The status", component::<schema::StatusBody>)],
    errors: &[("internal", 500)],
};

pub const METRICS: RouteDoc = RouteDoc {
    operation_id: "metrics",
    tag: "status",
    summary: "OpenMetrics",
    description: "The same exposition as `--metrics-bind`",
    params: &[],
    body: None,
    replies: &[text(200, "The metrics", Media::OpenMetrics)],
    errors: &[],
};

pub const ERRORS: RouteDoc = RouteDoc {
    operation_id: "errors",
    tag: "status",
    summary: "Recent warnings and errors",
    description: "The newest of the last 1000 warnings and errors the daemon logged, \
        credentials scrubbed",
    params: ERRORS_QUERY,
    body: None,
    replies: &[ok("The events", component::<schema::ErrorsPage>)],
    errors: &[],
};

pub const CRASHES: RouteDoc = RouteDoc {
    operation_id: "crashes",
    tag: "status",
    summary: "Crash records",
    description: "Panics, fatal exits and unclean exits the daemon recorded, newest first, \
        without backtraces",
    params: &[],
    body: None,
    replies: &[ok("The records", component::<schema::CrashList>)],
    errors: &[],
};

pub const CRASH: RouteDoc = RouteDoc {
    operation_id: "crash",
    tag: "status",
    summary: "One crash record",
    description: "With its backtrace, when it has one",
    params: &[],
    body: None,
    replies: &[ok(
        "The record",
        component::<crate::ops::crash::CrashRecord>,
    )],
    errors: &[("not_found", 404)],
};

pub const JOBS: RouteDoc = RouteDoc {
    operation_id: "jobs",
    tag: "status",
    summary: "Every tenant's jobs",
    description: "Initial loads, priming and teardowns",
    params: &[],
    body: None,
    replies: &[ok("The jobs", component::<schema::Jobs>)],
    errors: &[],
};

pub const TENANTS: RouteDoc = RouteDoc {
    operation_id: "list_tenants",
    tag: "tenants",
    summary: "Every declared tenant",
    description: "With where each writes and where it stands",
    params: &[],
    body: None,
    replies: &[ok("The tenants", component::<schema::TenantList>)],
    errors: CONFIG_READ,
};

pub const TENANT: RouteDoc = RouteDoc {
    operation_id: "get_tenant",
    tag: "tenants",
    summary: "One tenant",
    description: "Its declaration with secrets masked, its status and its jobs. `ETag` is \
        what mutations take as `If-Match`",
    params: &[],
    body: None,
    replies: &[Reply {
        status: 200,
        description: "The tenant",
        media: Media::Json,
        schema: Some(component::<schema::TenantDetail>),
        etag: true,
    }],
    errors: TENANT_READ,
};

pub const TENANT_JOBS: RouteDoc = RouteDoc {
    operation_id: "tenant_jobs",
    tag: "tenants",
    summary: "One tenant's jobs",
    description: "",
    params: &[],
    body: None,
    replies: &[ok("The jobs", component::<schema::TenantJobs>)],
    errors: TENANT_READ,
};

pub const TENANT_ERRORS: RouteDoc = RouteDoc {
    operation_id: "tenant_errors",
    tag: "tenants",
    summary: "One tenant's recent warnings and errors",
    description: "And why it was detached, if it was",
    params: TENANT_ERRORS_QUERY,
    body: None,
    replies: &[ok("The events", component::<schema::TenantErrors>)],
    errors: TENANT_READ,
};

pub const TENANT_TABLES: RouteDoc = RouteDoc {
    operation_id: "tenant_tables",
    tag: "tenants",
    summary: "One tenant's source tables",
    description: "Read from the source database, with whether the tenant's rules select \
        each. Rate class `expensive`: it opens a source connection",
    params: &[],
    body: None,
    replies: &[ok("The tables", component::<schema::TenantTables>)],
    errors: &[
        ("internal", 500),
        ("invalid_config", 409),
        ("not_found", 404),
        ("source_unavailable", 503),
    ],
};

pub const CONFIG: RouteDoc = RouteDoc {
    operation_id: "get_config",
    tag: "control",
    summary: "The merged config",
    description: "Every config file and fragment merged, as the daemon runs it, secrets \
        masked as `***`",
    params: &[],
    body: None,
    replies: &[ok("The config", component::<schema::ConfigBody>)],
    errors: &[("internal", 500)],
};

pub const AUDIT: RouteDoc = RouteDoc {
    operation_id: "audit",
    tag: "audit",
    summary: "The audit log",
    description: "Every mutation through the API or the control socket, write-ahead: an \
        attempt before it, a result after. Records chain by hash",
    params: AUDIT_QUERY,
    body: None,
    replies: &[ok("A page of records", component::<schema::AuditPage>)],
    errors: &[("audit_unavailable", 503), ("internal", 500)],
};

pub const OPERATION: RouteDoc = RouteDoc {
    operation_id: "operation",
    tag: "audit",
    summary: "How a mutation went",
    description: "For a client whose request timed out or dropped: the mutation runs to the \
        end either way, and this reports it. The last 1000 are kept",
    params: &[],
    body: None,
    replies: &[ok("The operation", component::<schema::Operation>)],
    errors: &[("not_found", 404)],
};

pub const DELETED: RouteDoc = RouteDoc {
    operation_id: "list_deleted",
    tag: "deleted",
    summary: "Deleted tenants",
    description: "Tombstones that can still be restored, with each teardown's progress",
    params: &[],
    body: None,
    replies: &[ok("The tombstones", component::<schema::DeletedList>)],
    errors: CONFIG_READ,
};

pub const DELETED_ONE: RouteDoc = RouteDoc {
    operation_id: "get_deleted",
    tag: "deleted",
    summary: "One deleted tenant",
    description: "Its latest tombstone, with the declaration it would restore",
    params: &[],
    body: None,
    replies: &[ok("The tombstone", component::<schema::DeletedDetail>)],
    errors: &[
        ("not_found", 404),
        ("internal", 500),
        ("invalid_config", 409),
    ],
};

pub const OPENAPI: RouteDoc = RouteDoc {
    operation_id: "openapi",
    tag: "docs",
    summary: "This document, as JSON",
    description: "OpenAPI 3.1. Public with `--api-public-docs`. `walshadow-stream api-docs` \
        prints it without a running daemon",
    params: &[],
    body: None,
    replies: &[ok(
        "The OpenAPI document",
        component::<schema::OpenApiDocument>,
    )],
    errors: &[],
};

pub const DOCS: RouteDoc = RouteDoc {
    operation_id: "docs",
    tag: "docs",
    summary: "This document, as a web page",
    description: "Rendered from the same OpenAPI document, self-contained: no script, no \
        external asset. Public with `--api-public-docs`; otherwise save it with a key and \
        open the file",
    params: &[],
    body: None,
    replies: &[text(200, "The page", Media::Html)],
    errors: &[],
};

pub const CREATE: RouteDoc = RouteDoc {
    operation_id: "create_tenant",
    tag: "tenants",
    summary: "Declare a tenant",
    description: "It attaches mid-stream, primes, initial-loads its tables, then streams",
    params: MUTATION_QUERY,
    body: Some(Body {
        schema: component::<schema::TenantCreate>,
        required: true,
        description: "The declaration, with its `id`",
    }),
    replies: OUTCOMES,
    errors: service_errors!(),
};

pub const REPLACE: RouteDoc = RouteDoc {
    operation_id: "replace_tenant",
    tag: "tenants",
    summary: "Replace a tenant's declaration",
    description: "Applies live where it can; a re-bind needs `acknowledge_resync`, and a \
        setting the daemon cannot change while running is refused",
    params: &[IF_MATCH, DRY_RUN, ACK],
    body: Some(Body {
        schema: component::<schema::TenantReplace>,
        required: true,
        description: "The whole declaration",
    }),
    replies: OUTCOMES,
    errors: service_errors!(("precondition_required", 428)),
};

pub const PATCH: RouteDoc = RouteDoc {
    operation_id: "patch_tenant",
    tag: "tenants",
    summary: "Change part of a tenant's declaration",
    description: "An RFC 7396 merge patch, sent as `application/json`",
    params: &[IF_MATCH, DRY_RUN, ACK],
    body: Some(Body {
        schema: component::<schema::TenantPatch>,
        required: true,
        description: "The merge patch",
    }),
    replies: OUTCOMES,
    errors: service_errors!(("precondition_required", 428)),
};

pub const DETACH: RouteDoc = RouteDoc {
    operation_id: "detach_tenant",
    tag: "tenants",
    summary: "Detach a tenant",
    description: "It stops receiving and holding WAL at once; its pipeline drains in the \
        background. Recorded in `tenant.toml` first: if that fails, nothing changes (503 \
        `detach_not_recorded`)",
    params: &[IF_MATCH, DRY_RUN, ACK],
    body: Some(Body {
        schema: component::<schema::StateChange>,
        required: false,
        description: "Optional",
    }),
    replies: OUTCOMES,
    errors: service_errors!(("precondition_required", 428)),
};

pub const ATTACH: RouteDoc = RouteDoc {
    operation_id: "attach_tenant",
    tag: "tenants",
    summary: "Attach a detached tenant",
    description: "Afresh, with fresh initial loads: needs `acknowledge_resync`",
    params: &[IF_MATCH, DRY_RUN, ACK],
    body: Some(Body {
        schema: component::<schema::StateChange>,
        required: false,
        description: "Optional",
    }),
    replies: OUTCOMES,
    errors: service_errors!(("precondition_required", 428)),
};

pub const DELETE_INTENT: RouteDoc = RouteDoc {
    operation_id: "delete_intent",
    tag: "tenants",
    summary: "Ask to delete a tenant",
    description: "Returns a single-use token, bound to this key, the tenant as it stands and \
        its attachment, valid five minutes; and what deleting keeps and removes",
    params: &[],
    body: None,
    replies: &[ok("The token", component::<schema::DeleteIntent>)],
    errors: &[
        ("not_found", 404),
        ("internal", 500),
        ("invalid_config", 409),
    ],
};

pub const DELETE: RouteDoc = RouteDoc {
    operation_id: "delete_tenant",
    tag: "tenants",
    summary: "Delete a tenant",
    description: "Removes its declaration, keeping a tombstone to restore it from. \
        Destination data, Snowflake state and the spill directory are kept",
    params: &[IF_MATCH, CONFIRM_DELETE],
    body: None,
    replies: &[OUTCOME_DELETED],
    errors: service_errors!(
        ("precondition_required", 428),
        ("confirmation_required", 428),
        ("confirmation_expired", 410),
        ("confirmation_invalid", 409),
    ),
};

pub const RESTORE: RouteDoc = RouteDoc {
    operation_id: "restore_tenant",
    tag: "deleted",
    summary: "Declare a deleted tenant again",
    description: "From its latest tombstone: detached, unless the body asks for `active` \
        with `acknowledge_resync`",
    params: MUTATION_QUERY,
    body: Some(Body {
        schema: component::<schema::Restore>,
        required: false,
        description: "Optional",
    }),
    replies: OUTCOMES,
    errors: service_errors!(),
};

pub const PAUSE: RouteDoc = RouteDoc {
    operation_id: "pause",
    tag: "control",
    summary: "Pause the WAL pump",
    description: "Cluster-wide: every tenant stops receiving until resumed",
    params: &[],
    body: None,
    replies: CONFIG_OUTCOME,
    errors: CONFIG_MUTATION_ERRORS,
};

pub const RESUME: RouteDoc = RouteDoc {
    operation_id: "resume",
    tag: "control",
    summary: "Resume the WAL pump",
    description: "",
    params: &[],
    body: None,
    replies: CONFIG_OUTCOME,
    errors: CONFIG_MUTATION_ERRORS,
};

pub const RELOAD: RouteDoc = RouteDoc {
    operation_id: "reload",
    tag: "control",
    summary: "Reload the config",
    description: "As SIGHUP does: re-read every file and fragment, apply, publish",
    params: &[],
    body: None,
    replies: CONFIG_OUTCOME,
    errors: CONFIG_MUTATION_ERRORS,
};
