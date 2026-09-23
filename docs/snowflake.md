# Snowflake destination (qualification release)

The Snowflake destination is opt-in. It consumes PostgreSQL WAL into a durable local
queue, sends small batches through Snowpipe Streaming Named Channels, stages larger
batches as Parquet in S3, and applies verified batches to typed current-state tables.
ClickHouse remains the default destination. Each destination needs its own state
directory; many client databases can share one daemon and slot as
[tenants](tenants.md), each with its own Snowflake database.

The integrated path supports `none`, `copy`, `base_backup`, and `object_store`
table loads, direct and object-store bootstrap, pending backup-row visibility,
and journaled snapshot publication. TRUNCATE and compatible schema changes use
new physical generations so a stale batch cannot repopulate a cleared table.
These paths have local tests but have **not been qualified against a live
Snowflake account and source workload**. Automatic source failover, unsupported
key/type changes, and transitions the daemon cannot prove safe stop progress.
Tables require a stable, unique, non-null replica identity or primary key.

Keys support built-in booleans, integers, numeric, finite floats, binary, text,
UUIDs, and finite date/time types. Numeric scale, signed floating zero, and
blank-padded `character` keys are canonicalized. Custom key types and key
definitions requiring non-binary collation equivalence need a supported surrogate
key. Other source columns use native Snowflake booleans, numbers, binary and
date/time where lossless; unconstrained/oversized numerics, JSON, arrays, domains,
and other PostgreSQL-rendered values remain text. Nonfinite native floats,
nonfinite numeric values mapped to NUMBER, and out-of-range native date/time
values stop delivery.

Compatible DDL includes nullable/default column additions and non-key column
drop/rename, plus table rename. Key/type changes require reinitialization. DDL
stops while an initial load or unresolved pending visibility would make its
projection unsafe. The default DROP policy retains the destination; explicit
`drop_table_strategy = "drop"` removes the owned public view and retains internal
history. Superseded generations, landing rows, receipts and journals currently
have no automatic garbage collection.

Table rename is journaled and recoverable, but uses separate Snowflake rename
and view-replacement statements: readers can briefly see the new view name
with its previous projection when both change together.

Current-state view publication explicitly enables change tracking and copies
reader grants. Replacing a view still resets its change history and can make
existing Snowflake streams stale; downstream refresh/reinitialization needs
separate validation.

## Install and configure

```bash
cargo build --release --bin walshadow-stream
cp config/snowflake.toml /etc/walshadow/snowflake.toml
# Edit PostgreSQL/Snowflake identifiers, paths, stage location, and selected tables.
```

The example is [config/snowflake.toml](../config/snowflake.toml). Select exactly
one destination with `[destination] kind = "snowflake"`; do not include `[ch]`.
`walshadow-stream init` currently writes ClickHouse configuration, so edit the
Snowflake TOML directly. Use `--config` (the existing `--ch-config` spelling is
an alias) to supply the file.

The source database and what durable state is bound to (account, database,
internal schema, stage, schema mapping) are fixed for the session; changing
them needs fresh state (with tenants, a detach and re-attach). Role,
warehouse, user, credential file paths and `statement_timeout_secs` reload
live; channel, pool, merge-interval, state and `ack_after` settings apply on
restart. Credential file contents can rotate without restarting. Source
endpoint changes retain the existing planned-switchover checks.

Statements poll until `statement_timeout_secs` (default 6 hours), refreshing
credentials as they go. Transient transport failures (throttling, 5xx,
timeouts, dropped connections) retry the batch with backoff; the batch stays
durable and every retry checks its apply receipt first. A full outbox
(`state.max_bytes`) holds deliveries until applied batches release space
instead of stopping. Restarts replace a public view only when its definition
changed, so change tracking and downstream streams survive them. Applied
manifests and TOAST history below the durable resume floor are reclaimed
once a minute. Live batches seal at 50,000 rows, 8 MiB, or 250 ms by
default; verified live batches then coalesce up to 64 MiB or 15 seconds before
MERGE, with at most four tables merging concurrently. Snapshot and pending-row
applies bypass that wait; snapshot batches are verified outside the table lock
and every verified batch of the same generation joins one MERGE and one receipt
transaction, so a large table's initial load does not apply one file at a time.
Every grouped receipt is read back before any batch in it is marked applied.
Shutdown/DDL drains wait for outstanding receipts.

Table setup, snapshot-generation preparation, publication and the restart
prewarm are metadata round trips rather than warehouse work. They run up to
`metadata_concurrency` relations at a time (default 32, 1..=256), independent
of `max_in_flight`. Each relation keeps its own table lock and journal.

A completed greenfield bootstrap records its published relations as finished
initial loads in the backfill ledger, so the `initial_load` opt-in seed after
bootstrap or any restart does not copy them again.

Keep Snowflake credentials out of TOML. Choose one of:

- `method = "oauth"` with `token_file` pointing to a file atomically replaced
  by an external OAuth refresh process. The transport reads it for each request.
- `method = "jwt"` with `account`, `private_key_file`, and the corresponding
  `SHA256:` public-key fingerprint. Use an unencrypted RSA PEM key readable only
  by the daemon. The transport signs a short-lived JWT from the key file.

Use the AWS SDK's renewable credential chain (instance/task role, web identity,
or configured profile) for S3; no AWS access key belongs in the Snowflake
configuration. The daemon needs `s3:PutObject` and `s3:GetObject` on its dedicated
prefix. It writes checksum-named objects with conditional create and verifies
an existing object's bytes before reusing it. Protect and back up the local
state directory: outbox batches, pending rows, generation journals, and TOAST
history must survive restarts. `snowflake.state.max_bytes` bounds logical
outbox, pending-row, and TOAST bytes; RocksDB files, indexes, compaction space,
and temporary Parquet files need additional free disk space. It is not a disk
quota. Stage files are immutable and currently have no automatic deletion;
keep them available through replay and apply-receipt verification, then manage
retention deliberately.

## Snowflake prerequisites

Provision a database, warehouse, role, and S3-backed external stage before the
daemon starts. The stage URL must be exactly the configured bucket and prefix,
with a trailing slash. For example, adapt these statements and the IAM role
trust policy to your account:

```sql
CREATE STORAGE INTEGRATION WALSHADOW_S3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-walshadow-read'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://replace-with-dedicated-bucket/walshadow/replica-db/');

CREATE STAGE REPLICA_DB.WALSHADOW_INTERNAL.WALSHADOW_STAGE
  URL = 's3://replace-with-dedicated-bucket/walshadow/replica-db/'
  STORAGE_INTEGRATION = WALSHADOW_S3
  FILE_FORMAT = (TYPE = PARQUET);
```

Grant the runtime role warehouse usage, database usage and schema creation,
table/view/pipe creation in the target and internal schemas, DML and SELECT on
its managed tables, and stage usage. The Snowflake storage-integration role
needs read access to the same S3 prefix. Follow Snowflake's
[S3 storage integration setup](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
for the generated IAM principal and external ID, and its
[SQL API authentication guide](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating)
for key-pair or OAuth setup. Set the Snowflake user's `AUTOCOMMIT` to `TRUE`, as
required by the [SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/intro).

## Run and observe

Provide PostgreSQL authentication externally (for example, through a protected
`WALSHADOW_PG_URL` environment variable) and start with the configured slot:

```bash
walshadow-stream --config /etc/walshadow/snowflake.toml --bootstrap-mode off
```

The example selects `public.orders` with `initial_load = "copy"`, so existing
rows are loaded from a PostgreSQL SQL snapshot while WAL continues. `none`
only copies future changes. `base_backup` reads a fresh physical backup;
`object_store` requires `[backup]` with a continuous wal-g-compatible archive.
For an uninitialized shadow, choose direct or object-store bootstrap as in
[configuration](configuration.md#backup-archive). Backup modes retain undecided
rows with both deciding transaction IDs; they promote a surviving row only
after the durable outcome and Snowflake apply receipt are known.

The first start performs Snowflake preflight and replays pending durable batches.
Do not run a second daemon against the same slot, Snowflake tables, or state
directory. Monitor process errors, source-to-destination freshness, queue size,
Named Channel rejected-row counts, and S3/COPY failures. A streaming offset alone
does not prove a complete batch; the daemon checks the landing batch and an
apply receipt before acknowledging source progress. Stop and investigate any
rejected row or unsupported schema transition. Keep the S3 prefix and local
state together across restarts. A new daemon must use the same source identity,
configuration fingerprint, and state directory; do not delete a staged object
while its batch may still be retried.

## Verification and qualification

Local checks exercise protocol responses and deterministic Parquet without live
credentials:

```bash
cargo test --lib destination::snowflake
cargo nextest run --workspace --all-targets --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --lib --bins
make -C pgext
make -C pgext faultshim.so
```

Ignored live tests use an explicit config path and create then drop isolated
Snowflake schemas. They fail if invoked without that path or credentials. The
runtime test also requires the configured external S3 stage and renewable AWS
credentials for upload:

```bash
WALSHADOW_SNOWFLAKE_LIVE_CONFIG=/etc/walshadow/snowflake.toml \
  cargo test --test snowflake_live -- --ignored --nocapture
```

The SQL test covers typed version ordering. The runtime test exercises Named
Channel streaming, Parquet/S3 COPY, snapshot publication with later WAL,
delete/reinsert ordering, durable reopen, and TRUNCATE. It uses synthetic
source rows, so it does not prove PostgreSQL WAL decoding or the full daemon
path. It retains checksum-addressed S3 files for the configured retention
policy; it drops only its own Snowflake schemas.

The local benchmark runner can poll the public Snowflake view for single-row,
sustained, and interleaved workloads while writing to the real PostgreSQL
source. Start the daemon with a dedicated test table first, then run, for
example:

```bash
cargo run -p walshadow-bench --bin walshadow-local-bench -- \
  --bench sustained --dest snowflake \
  --snowflake-config /etc/walshadow/snowflake.toml \
  --snowflake-table REPLICA_DB.PUBLIC.USERS \
  --table demo.users --rate 500 --duration-secs 1800
```

The runner issues source TRUNCATE and waits for the public view to empty; it
does not directly clear Snowflake storage. Its current-state count and probe
measurements do not establish full value equality, initial-load or bootstrap
performance, or cloud cost. A production qualification still requires three
matched source/daemon workload runs against ClickHouse and Snowflake, at least
80% current-state throughput,
p95 freshness at most 60 seconds, stable backlog for 30 minutes, exact values
with no missing or resurrected rows, and ingestion/warehouse/S3/network costs.
Do not treat local mocks or a passing SQL-only live test as that qualification.

The result gate accepts measured JSON and refuses omitted fields:

```bash
cargo run -p walshadow-bench --bin walshadow-snowflake-qualify -- measurements.json
```

Use a `workload_id` and a `runs` array with at least three entries. Each run
needs a distinct `run_id`, the same `source_input_sha256` (SHA-256 of the
source workload definition and seed), and `clickhouse` and `snowflake` metric
objects. Each metric object needs `throughput_rows_per_sec`, `expected_rows`,
`actual_rows`, `missing_rows`, `resurrected_rows`, `value_mismatches`, and
`ingest_usd`, `warehouse_usd`, `s3_usd`, `network_usd`. Snowflake additionally
needs `p95_freshness_ms`, `backlog_window_secs`, `backlog_start_rows`, and
`backlog_end_rows`, sampled after warmup. The validator requires at least 1800
seconds of backlog observation with no net growth, all costs present, exact
rows, and the throughput/freshness thresholds in every repetition. It checks
the supplied measurements; operators must retain source queries, time series,
and billing exports that substantiate them.

The local and EC2 benchmark engines now query the public current-state view
through the Snowflake SQL API. Use isolated source tables and schemas for each
run; capture full row/value equality, backlog time series, and billing data
separately. Until live workloads and those checks are complete, the validator
remains a gate for supplied measurements rather than proof of qualification.
