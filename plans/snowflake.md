# Snowflake destination implementation

Accepted design: add a selectable Snowflake destination, retain ClickHouse compatibility,
use native Rust REST/SQL APIs and S3 Parquet staging, expose typed current-state views,
require stable unique non-null keys. One destination per daemon. Preserve physical WAL,
shadow catalogs, transaction reconstruction, bounded decoding and contiguous durable ACKs.

## Contract

- Stable events carry source identity/lineage, relation incarnation, schema/load generation,
  commit LSN, record LSN and row ordinal. Snapshot precedence is below later WAL.
- Durable local TOAST history and outbound batches; never acknowledge undelivered rows.
- Streaming offsets alone do not prove completeness: rejected rows must stop progress.
- Stage large rows and snapshots in S3, COPY with ABORT_STATEMENT, verify batch receipts.
- Version-aware MERGE, retained tombstones and public views filtering deleted rows.
- Explicit DDL barriers and journaled view publication; no automatic schema evolution.
- All existing load modes, pending transaction visibility and planned source switchover.
- No active-active operation, keyless multiset replication, or unplanned failover.
- Fail closed on unsupported types/transitions rather than silently changing values.
- Credentials external to config dumps; JWT or refreshed OAuth token file, renewable AWS auth.
- Keep ClickHouse configuration, state and default behavior compatible.

## Milestones

1. Destination configuration/boundary and ClickHouse baseline.
2. Neutral values, Snowflake type mappings and shadow text conversion.
3. Durable TOAST and outbound state with crash recovery.
4. REST/SQL transport, S3 loading and verified delivery.
5. Current-state materialization, receipts and views.
6. Schema/lifecycle barriers.
7. Snapshot modes, pending visibility and publication.
8. Operations, full correctness suite and performance qualification.

## Verification

Run repository nextest, fmt, Clippy, Rustdoc and PostgreSQL module checks. Exercise crashes
at persistence/publication boundaries, out-of-order delivery, deletes/reinsert/key changes,
TOAST history, all initial-load modes, DDL and credentials/service failure. Live jobs fail
if configured credentials/prerequisites are absent; local skips must be explicit.

Release: three repetitions of matched source/daemon workloads; current-state throughput
at least 80% of ClickHouse, p95 freshness <=60 seconds, stable backlog for 30 minutes,
exact values and no missing/resurrected rows. Report ingestion, warehouse, S3/network costs.
Do not describe scaffolding, local mocks, or raw ingestion as production qualification.

## Implementation record

See `snowflake-progress.md` for evidence and remaining work. The full design is the
user-approved plan in the session; these contracts preserve its binding requirements.
