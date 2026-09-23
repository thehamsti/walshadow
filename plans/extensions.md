# Optional capabilities

Keep these ideas scoped to a consumer and a proof of value. Retain proposed
interfaces and dependencies so other workstreams can account for them before
a deployment promotes one into a standalone implementation plan

## Multiple destinations for one database

[Tenants](../docs/tenants.md) already fan one WAL stream out to one
destination per source database, with coupled retention (the slot follows
the least resume floor), per-tenant lag/stall eviction, and per-tenant
durable Snowflake outboxes. What remains is several destinations for *one*
database, and fan-in

Proposed route result is zero or more `(DestinationId, target)` pairs. Give each
stable destination ID its own connection settings, batcher/inserter pool, DDL
applicator, budgets, and contiguous acknowledgement state; a tenant already
bundles those, so a second destination could be a second tenant over the same
database once routing permits it. Replay from the least floor is safe only
while every replayed effect stays idempotent, including DDL and routing
changes

Fan-in needs source identity in destination key when source key domains overlap
`_lsn` and `_xid` do not by themselves prevent equal primary keys from collapsing
Validate sorting key, projection, and DDL ownership before activating route
Tablespace predicates depend on [resolved physical OIDs](tablespaces.md), other
predicates can land independently

## Schema export

Add schema-only SQL export from shadow when a concrete consumer needs it
Prove a consistent catalog snapshot at requested boundary and restore into a
fresh compatible PostgreSQL instance. State extension, ownership, and privilege
requirements explicitly

Do not promise source sequence values from schema-only export; sequence state
is not currently replicated. Exporting a bootable hollow data directory is a
separate physical-recovery feature, not an extension of pg_dump output

Start with SQL payload from `pg_dump --schema-only` against shadow. Exact-LSN
export needs replay pinned at valid catalog boundary while snapshot is acquired;
`wait_for_replay(X)` only proves replay reached X and may already be ahead
Coordinate snapshot lifetime with [capture holds](custom_rmgr.md), cancellation,
and recovery conflicts. Record source identity, actual boundary, PostgreSQL
major, extension versions, and export scope with payload

Test enums, composites, defaults, checks, indexes, foreign keys, and explicit
ACL/ownership policy by restoring and inspecting fresh target. Extension SQL
does not supply extension binaries. Sequence definitions do not supply sequence
values. A later DDL relay needs its own destination lifecycle and recovery policy

Keep hollow-directory export parked as separate investigation: prove valid empty
heaps/indexes, control/WAL consistency, extension storage, and isolation from live
shadow on disposable clone. File truncation and temporary promotion of active
catalog-only shadow are not an implementation recipe. Schema export cannot
replace data-bearing backup for witness recovery

## Synchronous durability witness

A witness must retain unfiltered source WAL durably and make it available to
a data-bearing PostgreSQL survivor. Filtered shadow WAL cannot restore user
data. Existing walsender transport does not establish this durability contract

Before exposing synchronous acknowledgement, prove raw-WAL fsync, retention,
relay, timeline lineage, and external fencing through primary-loss drills
Define disk-full and quorum behavior. Report actual durability positions rather
than claiming replay that never occurred. Do not promote catalog-only shadow
into an application primary

Persist byte-identical raw WAL before filter mutation in separate archive with
validated system identity and timeline history. Advance witness flush only after
fsync, independently of ClickHouse acknowledgement and filtered-shadow replay
Do not advertise replay for raw bytes merely retained on disk; explicitly scope
supported synchronous commit modes in experiment and eventual configuration

On primary loss, external orchestrator fences old primary, identifies durable
witness frontier F1 and surviving data-bearing standby frontier F2, relays complete
history between them, verifies survivor caught up, then authorizes promotion
Prototype archive fetch/restore path first or reuse walsender transport with raw
WAL source. Existing filtered sender is transport substrate, not durability proof

Test witness crash around fsync/ack, disk exhaustion, quorum reconnect, archive
gaps, partial tail, timeline change, and loss of primary while full standby lags
Retain raw history until consumers and recovery contract release it. Rebind shadow
only after [lineage proof](failover.md); source slot availability remains separate
promotion-target precondition

## QBit vector storage

Current type mapping uses Array(Float32) for vector and halfvec. PostgreSQL
conversion has replaced old manual halfvec decoding path; do not reimplement
that removed decoder. Existing vector test covers text output, so add explicit
array-target coverage before changing storage representation

Evaluate QBit only after native-protocol insertion, dimensions, required server
settings, and version compatibility are tested. Distinguish destination storage
type from wire representation. Preserve variable-dimension fallback and existing
tables. Make halfvec precision loss explicit if choosing BFloat16, never treat
IEEE half and BFloat16 bits as interchangeable

## Other opt-in behavior

Add TLS and SCRAM to shadow-facing walsender before supporting untrusted remote
clients. Source connection authentication already exists. Define whether and how
hot-standby feedback from shadow should influence retention; current sender
ignores it. Test authentication, reconnect, timeout, and recovery conflicts

Query-time TOAST reconstruction would move cost from ingestion into destination
queries and require a stable pointer representation plus decompression support
Streaming values beyond current inline cap also needs a destination path that
does not materialize whole values. Keep both separate from current bounded
inline mode and [shadow TOAST](shadow_toast.md)

Sequence-state replication needs a consumer and an explicit consistency contract
Cross-table atomic visibility needs destination support beyond independent
inserts. Debug row sampling and ignoring source truncates change replication
semantics; add them only as explicit policies with replay and lifecycle tests
