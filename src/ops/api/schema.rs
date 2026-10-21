//! Schemas of the JSON bodies handlers build with `json!`, and of the
//! request bodies they parse by hand
//!
//! The OpenAPI document ([`super::openapi`]) takes these with the types
//! handlers serialize directly ([`Principal`], [`Report`](super::health::Report),
//! [`CrashRecord`](crate::ops::crash::CrashRecord), [`ErrorEvent`],
//! [`Record`](crate::ops::audit::Record), [`Tombstone`]). Object schemas are closed
//! unless [`super::openapi`] lists them as open. Nothing here is on the
//! wire: the conformance tests hold every documented response and request
//! to what the handlers actually send and accept

#![allow(dead_code)]

use serde_json::Value;
use utoipa::ToSchema;

use super::deleted::Tombstone;
use super::health::Status;
use super::keys::Principal;
use crate::ops::error_ring::ErrorEvent;

/// A tenant's desired state
#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Desired {
    Active,
    Detached,
}

#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Layout {
    /// No `[tenants]`: one database, no tenants to list
    Single,
    Tenants,
}

/// How a change applies: `live` to the running tenant, `fresh_attach` and
/// `rebind` attach it afresh with fresh initial loads, `rejected` needs a
/// daemon restart, `none` changes nothing
#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Classification {
    None,
    Live,
    FreshAttach,
    Rebind,
    Rejected,
}

#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    InitialLoad,
    Priming,
    Teardown,
}

#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TeardownState {
    Pending,
    Draining,
    Stuck,
    Done,
}

#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Running,
    Applied,
    Rejected,
    RolledBack,
    Conflict,
}

#[derive(ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DestinationKind {
    #[serde(rename = "clickhouse")]
    ClickHouse,
    Snowflake,
}

/// The body of every error the guard and the handlers answer with
#[derive(ToSchema)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(ToSchema)]
pub struct ErrorDetail {
    /// One of the codes in the API reference; the meaning of each is
    /// listed with the response that carries it
    pub code: String,
    /// What went wrong, credentials scrubbed
    pub message: String,
    /// Also in the `X-Request-Id` header
    pub request_id: String,
}

/// The key the request presented
#[derive(ToSchema)]
pub struct Whoami {
    pub principal: Principal,
    pub request_id: String,
}

/// The process answers
#[derive(ToSchema)]
pub struct Live {
    /// Always `ok`
    pub status: String,
}

/// Whether the daemon is ready to serve. A 503 carries the same body with
/// `ready: false`
#[derive(ToSchema)]
pub struct Readiness {
    pub ready: bool,
    pub status: Status,
}

/// `ctl status`, plus the reload pipeline's place. Open: the status table
/// carries more keys, which the control socket documents
#[derive(ToSchema)]
pub struct StatusBody {
    /// Whether the WAL pump is paused cluster-wide
    pub paused: bool,
    /// Process id of the revision stream; generations restart per epoch
    pub epoch: String,
    /// Generation of the last published config revision
    pub published_generation: u64,
    /// Generation the session last reconciled
    pub reconciled_generation: u64,
    /// `starting`, `streaming`, `restarting`, …
    pub session_state: String,
}

/// Recent warnings and errors, newest first
#[derive(ToSchema)]
pub struct ErrorsPage {
    pub events: Vec<ErrorEvent>,
}

/// Crash, fatal-exit and unclean-exit records, newest first, without
/// backtraces
#[derive(ToSchema)]
pub struct CrashList {
    pub crashes: Vec<crate::ops::crash::CrashRecord>,
}

/// One job: an initial load of a table, a tenant priming, or a teardown
#[derive(ToSchema)]
pub struct Job {
    pub kind: JobKind,
    pub tenant: String,
    /// Loads: `queued`, `running`, `pending`, `swapping` or `done`;
    /// priming: `running`; teardown: `running` or `stuck`
    pub state: String,
    /// Loads only
    #[schema(required = false)]
    pub namespace: String,
    /// Loads only
    #[schema(required = false)]
    pub name: String,
    /// Loads only: `copy`, `base_backup`, …
    #[schema(required = false)]
    pub mode: String,
    /// Loads only: the snapshot LSN
    #[schema(required = false)]
    pub s_lsn: String,
    /// COPY loads only: the next block to copy
    #[schema(required = false)]
    pub copy_next_block: u32,
}

#[derive(ToSchema)]
pub struct Jobs {
    pub jobs: Vec<Job>,
    /// Initial loads the config still asks for
    pub backfills_pending: u64,
}

/// A ClickHouse destination: the `endpoint/database` pairs the tenant reaches
#[derive(ToSchema)]
pub struct ClickHouseDestination {
    /// `clickhouse`
    pub kind: DestinationKind,
    pub databases: Vec<String>,
}

#[derive(ToSchema)]
pub struct SnowflakeDestination {
    /// `snowflake`
    pub kind: DestinationKind,
    pub account_url: String,
    pub database: String,
}

/// The destination section does not parse
#[derive(ToSchema)]
pub struct DestinationError {
    pub error: String,
}

/// Where a tenant writes, without credentials
#[derive(ToSchema)]
#[serde(untagged)]
pub enum Destination {
    ClickHouse(ClickHouseDestination),
    Snowflake(SnowflakeDestination),
    Error(DestinationError),
}

/// Where a tenant stands. The live fields are present while the session
/// reports the tenant; the persisted ones once it has state on disk
#[derive(ToSchema)]
pub struct TenantStatus {
    /// `pending`, `priming`, `active`, `detaching`, `detached`, `waiting`, …
    pub phase: String,
    /// The session's snapshot is older than it should be
    pub stale: bool,
    pub lag_bytes: Option<u64>,
    pub ack_lsn: Option<String>,
    pub resume_safe_lsn: Option<String>,
    pub queue_depth: Option<u64>,
    pub xacts_active: Option<u64>,
    pub rows_emitted: Option<u64>,
    pub backfill_copy_rows: Option<u64>,
    pub backfills_pending: Option<u64>,
    /// `priming`, `active` or `detached`, from `tenant.toml`
    pub persisted_phase: Option<String>,
    /// Why it was detached, from `tenant.toml`
    pub reason: Option<String>,
    pub attached_lsn: Option<String>,
    pub start_lsn: Option<String>,
    pub db_oid: Option<u32>,
    /// Id of this attachment; a delete token is bound to it
    pub incarnation: Option<String>,
}

/// A tenant as the list shows it
#[derive(ToSchema)]
pub struct TenantSummary {
    pub id: String,
    pub dbname: String,
    pub desired: Desired,
    pub initial_load: String,
    pub max_lag_bytes: Option<u64>,
    /// Send it back as `If-Match`
    pub etag: Option<String>,
    pub destination: Destination,
    pub status: TenantStatus,
}

/// A tenant in full: its summary, its declaration with secrets masked, and
/// its jobs
#[derive(ToSchema)]
pub struct TenantDetail {
    #[serde(flatten)]
    pub summary: TenantSummary,
    /// The `[tenant.<id>]` table without `dbname` and `state`, secrets
    /// masked as `***`
    pub spec: Value,
    pub jobs: Vec<Job>,
}

/// `layout: single` when there is no `[tenants]`; then the list is empty
#[derive(ToSchema)]
pub struct TenantList {
    pub layout: Layout,
    /// Tenants layout only
    #[schema(required = false)]
    pub epoch: String,
    /// Tenants layout only
    #[schema(required = false)]
    pub reconciled_generation: u64,
    pub tenants: Vec<TenantSummary>,
}

#[derive(ToSchema)]
pub struct TenantJobs {
    pub jobs: Vec<Job>,
}

#[derive(ToSchema)]
pub struct TenantErrors {
    pub tenant: String,
    /// From `tenant.toml`, when the tenant was detached
    pub detach_reason: Option<String>,
    pub events: Vec<ErrorEvent>,
}

/// The tenant's source tables, with whether its rules select each, as
/// `ctl tables` lists them
#[derive(ToSchema)]
pub struct TenantTables {
    pub tables: Vec<SourceTable>,
}

#[derive(ToSchema)]
pub struct SourceTable {
    pub namespace: String,
    pub name: String,
    /// The tenant's rules replicate it
    pub selected: bool,
    /// `pg_class.relreplident`: `d` default, `f` full, `i` index, `n`
    /// nothing
    #[schema(pattern = "^[dfin]$")]
    pub replica_identity: String,
    /// A primary key or replica identity index identifies its rows
    pub has_row_key: bool,
}

/// The merged config, every secret masked as `***`. Open: whatever the
/// config holds
#[derive(ToSchema)]
pub struct ConfigBody {}

/// The OpenAPI 3.1 document of this API. Open
#[derive(ToSchema)]
pub struct OpenApiDocument {
    /// `3.1.0`
    pub openapi: String,
}

#[derive(ToSchema)]
pub struct AuditPage {
    pub records: Vec<crate::ops::audit::Record>,
    /// Whether every record links to the one before it
    pub chain_ok: bool,
}

/// What an operation failed with
#[derive(ToSchema)]
pub struct OperationError {
    pub code: String,
    pub message: String,
}

/// A mutation as `/v1/operations/{request_id}` reports it: running, or how
/// it ended, with the fields of the response it got
#[derive(ToSchema)]
pub struct Operation {
    pub request_id: String,
    pub state: OperationState,
    #[schema(required = false)]
    pub action: String,
    #[schema(required = false)]
    pub tenant: String,
    #[schema(required = false)]
    pub classification: Classification,
    #[schema(required = false)]
    pub reasons: Vec<String>,
    #[schema(required = false)]
    pub etag: Option<String>,
    #[schema(required = false)]
    pub epoch: String,
    #[schema(required = false)]
    pub generation: u64,
    #[schema(required = false)]
    pub dry_run: bool,
    #[schema(required = false)]
    pub unfollowed_databases: Vec<String>,
    #[schema(required = false)]
    pub error: OperationError,
}

/// What a tenant mutation did, or would do on a dry run
#[derive(ToSchema)]
pub struct Outcome {
    pub request_id: String,
    pub tenant: String,
    pub classification: Classification,
    /// Why a re-bind attaches afresh
    pub reasons: Vec<String>,
    /// The tenant's new ETag; null once it is deleted
    pub etag: Option<String>,
    pub epoch: String,
    /// The revision published, or the current one on a dry run
    pub generation: u64,
    pub dry_run: bool,
    /// `applied`
    pub state: String,
}

/// What a pause, resume or reload published
#[derive(ToSchema)]
pub struct ConfigOutcome {
    pub request_id: String,
    /// `stream.pause`, `stream.resume` or `config.reload`
    pub action: String,
    /// `applied`
    pub state: String,
    pub epoch: String,
    pub generation: u64,
    /// Databases the config names that this process does not follow
    pub unfollowed_databases: Vec<String>,
}

#[derive(ToSchema)]
pub struct DeleteImpact {
    pub tenant: TenantSummary,
    pub detaches: bool,
    pub retained: Vec<String>,
    pub removed: Vec<String>,
    pub replication_slot: String,
    pub warnings: Vec<String>,
}

/// A delete token, and what deleting keeps and removes
#[derive(ToSchema)]
pub struct DeleteIntent {
    /// Send it as `X-Confirm-Delete`; single use
    pub confirm_token: String,
    pub expires_in_secs: u64,
    /// Send it as `If-Match`
    pub etag: String,
    pub impact: DeleteImpact,
    /// The request that completes the delete
    pub next: String,
}

/// A deleted tenant, and whether its teardown is done
#[derive(ToSchema)]
pub struct Deleted {
    #[serde(flatten)]
    pub tombstone: Tombstone,
    pub teardown: TeardownState,
    /// The id is declared again
    pub redeclared: bool,
}

#[derive(ToSchema)]
pub struct DeletedList {
    pub deleted: Vec<Deleted>,
}

/// A deleted tenant with its declaration, secrets masked
#[derive(ToSchema)]
pub struct DeletedDetail {
    #[serde(flatten)]
    pub deleted: Deleted,
    pub spec: Value,
}

// ---- requests ---------------------------------------------------------------

/// A tenant declaration as JSON: the `[tenant.<id>]` table. Open: besides
/// these fields it takes the destination and stream sections
/// (`destination`, `ch`, `snowflake`, `stream`, `table`, `namespace`,
/// `system_columns`, `toast`, …) as `docs/configuration.md` describes them
#[derive(ToSchema)]
pub struct TenantCreate {
    /// 1–63 characters of `[a-z0-9_-]`, not a reserved id
    #[schema(pattern = "^[a-z0-9_-]{1,63}$")]
    pub id: String,
    /// The source database the tenant follows
    pub dbname: String,
    /// Default `active`
    #[schema(required = false)]
    pub state: Desired,
    #[schema(required = false)]
    pub max_lag_bytes: u64,
    /// `copy` (default) or `none`
    #[schema(required = false)]
    pub initial_load: String,
    /// Required when the change attaches the tenant afresh
    #[schema(required = false)]
    pub acknowledge_resync: bool,
}

/// The whole declaration, replacing what is there. `id`, if given, must be
/// the path's. Open like [`TenantCreate`]
#[derive(ToSchema)]
pub struct TenantReplace {
    #[schema(required = false, pattern = "^[a-z0-9_-]{1,63}$")]
    pub id: String,
    pub dbname: String,
    #[schema(required = false)]
    pub state: Desired,
    #[schema(required = false)]
    pub max_lag_bytes: u64,
    #[schema(required = false)]
    pub initial_load: String,
    #[schema(required = false)]
    pub acknowledge_resync: bool,
}

/// An RFC 7396 merge patch over the declaration: an object merges, `null`
/// removes a key. Open
#[derive(ToSchema)]
pub struct TenantPatch {
    #[schema(required = false)]
    pub acknowledge_resync: bool,
}

/// Optional. `desired` is accepted and ignored: the route decides
#[derive(ToSchema)]
pub struct StateChange {
    /// Logged with the change
    #[schema(required = false)]
    pub reason: Option<String>,
    #[schema(required = false)]
    pub acknowledge_resync: bool,
    #[schema(required = false)]
    pub desired: Option<String>,
}

/// Optional. Restores detached unless `desired` is `active`, which needs
/// `acknowledge_resync`
#[derive(ToSchema)]
pub struct Restore {
    #[schema(required = false)]
    pub reason: Option<String>,
    #[schema(required = false)]
    pub acknowledge_resync: bool,
    /// `detached` when absent or null
    #[schema(required = false)]
    pub desired: Option<Desired>,
}
