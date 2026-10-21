# Snowflake implementation progress

Base: cf77c2c. Branch: codex/snowflake-destination.

No milestone is complete until its integration and verification are recorded below.

## Preflight

- Clean initial checkout. Rust 1.97.1 available; pg_config present; ClickHouse absent.
- Initial implementation had no Snowflake/AWS environment credentials. The
  2026-09-22 tenant experiment resolves credentials through AWS Secrets Manager.
- Ruling: work on an isolated branch in the supplied checkout, preserving workspace paths.
- Shared interfaces: config feeds transport/storage; neutral types feed tail/DDL;
  storage and transport receipts feed ACK collector; bootstrap shares destination tail.
- Ruling: partial functionality must reject unsupported operations before advancing WAL.
  It must not be advertised as the completed full-replication release.

## Work and evidence

- Baseline: unmodified library suite passed 1,085 tests.
- Added destination selection, typed source values, neutral PostgreSQL bridge,
  durable outbox/TOAST/pending state, REST/SQL transport, Parquet staging,
  transactional MERGE receipts, parallel channel delivery, and snapshot journals.
- Local PostgreSQL 18.6 and the pinned C dependencies were built under `target`;
  the extension and fault shim compiled, and all six protocol tests passed.
- HTTP mock tests cover asynchronous SQL, pagination, retry UUIDs, transaction
  child failures, scoped streaming tokens, rejected rows and redirects.
- Runtime fault tests prove incomplete batches and failed applies retain their
  durable payload, and receipt readback is required before payload retirement.
- COPY, base-backup, object-store and greenfield generation delivery are wired,
  including durable pending reconciliation and TRUNCATE/schema/DROP journals.
  Publication is fenced against concurrent opt-out/remap, including resumed loads.
- TOAST now uses a value index plus physical TID history, shares the state byte
  budget with the outbox, and retains as-of history across truncate barriers.
- Live batches coalesce to 64 MiB or the configured interval, with four concurrent
  table merges. The transaction applies state and receipts together; every batch
  requires receipt readback before ACK. Snapshot/pending batches apply immediately.
- State format 1 binds source system/database and destination configuration,
  verifies payload/TOAST indexes and accounting at startup, and completes interrupted
  temporary-payload publication. Recovery reads bounded batches.
- A configuration example, operational guide, SQL-only and runtime/S3 ignored live
  tests, Snowflake benchmark adapter, and measured qualification validator are
  included. The validator checks supplied measurements; it does not fabricate them.
- Isolated tenant resources and runtime credentials are provisioned. Both live
  Snowflake tests passed, including Named Channels, S3/Parquet COPY, row-version
  ordering, restart, delete/reinsert, truncate, grants/change tracking, JSON/TIME
  and timestamp precision, and eight concurrent snapshot generations. These use
  synthetic descriptors; the full physical-source path remains in progress.
- Live service findings fixed REST scalar responses/region hostnames, explicit
  role scope, pipe-name case, success status, unsupported streaming ON_ERROR,
  SQL retry receipt collisions across destinations, and serial bootstrap setup.
- Corrected Snowflake bootstrap helper selection to inspect source decoding
  requirements instead of parsing Snowflake SQL types as ClickHouse types.
  The selected tenant columns/defaults require no schema-dump helper.
- Latest default-feature library run: 1,167 passed, zero failed. Benchmark crate:
  47 passed across library/CLI tests. Snowflake live-test target: configuration
  example passed; two credential-dependent tests explicitly run and passed.
- Full PostgreSQL 18.6 / ClickHouse 26.3.19.3 Nextest run: 1,483 executed,
  1,480 passed, three failed, five skipped. Both ClickHouse failures were equivalent
  single-column PRIMARY KEY formatting and now accept both forms. The remaining
  failure needed SSL in the locally built PostgreSQL. All three passed targeted
  reruns after those fixes. Subsequent publication/state changes passed focused
  tests and the final full library suite; the entire integration suite was not
  repeated after those changes.
- Strict workspace/all-target Clippy, strict workspace Rustdoc, rustfmt and
  `git diff --check` passed. Benchmark CLI help builds and exposes Snowflake options.
- CI/container dependency lists now include libclang/CMake and required C++ runtime
  libraries. Docker image builds were not exercised: local daemon access was denied.

## Remaining qualification and operating limits

- Complete a full physical bootstrap against a production-scale source with
  table count, value, and analytics compatibility checks, using an isolated
  source slot and destination.
- Broaden physical-source coverage to service failures and crash/restart drills;
  live runtime reopen is proven, full physical-source crash recovery is not.
- Run three matched ClickHouse/Snowflake workloads and retain value equality,
  30-minute backlog, p95 freshness, throughput and cost evidence. Comparable speed
  has not been demonstrated by local mocks or the benchmark adapter's compilation.
- Key/type changes, unsupported key encodings and unsafe schema transitions stop
  progress; they need controlled reinitialization. Table rename has a brief
  name/projection transition window. Internal history and staged objects need an
  operator retention policy; automatic garbage collection is not implemented.
