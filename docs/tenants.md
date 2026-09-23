# Replicate many databases through one daemon

A cluster that hosts one database per client needs neither one daemon nor
one replication slot per client. Physical WAL already carries every
database, and walshadow's shadow already replays every database's catalogs.
With tenants, one daemon reads the cluster through **one physical slot** and
**one shadow**, and each tenant follows one database into its own
destination.

Each tenant owns its descriptor history, transaction buffer, pipeline,
destination, state directory and table rules. A router hands every record
to the tenant whose database wrote it. The slot's flush position is the
least any attached tenant can resume from.

## Configure

Declare `[tenants]` and one `[tenant.<id>]` table per client. A tenant table
carries `dbname` plus the sections a single-database config would carry at
the top level: `[destination]`, `[snowflake]` or `[ch]`, `[stream]`,
`[table.*]`, `[namespace.*]`, `[runtime_config]`, `[system_columns]`,
`[toast]`. Cluster-wide sections stay at the top: `[source]` (endpoint,
credentials, slot and the **admin database** in `dbname`), `[memory]`,
`[bootstrap]`, `[backup]`.

```toml
[source]
host = "pg.internal"
user = "walshadow_repl"
dbname = "postgres"          # admin database the pump connects to
sslmode = "verify-full"
slot = "walshadow_cluster"

[tenants]
stall_timeout_secs = 300     # evict a tenant whose queue refuses records this long
max_lag_bytes = 21474836480  # evict a tenant trailing the pump by more (optional)
decoder_pool_size = 2        # per-tenant pool defaults
inserter_pool_size = 2
bridge_workers = 2           # shadow bridge workers per tenant database
capacity = 64                # tenant pools shadow reserves worker slots for
```

Put each tenant in its own fragment, `<config>.d/60-tenant-<id>.toml`, so
`ctl tenant` can manage it:

```toml
[tenant.acme]
dbname = "acme"
initial_load = "copy"        # for tables in scope when it attaches
# max_lag_bytes = …           # overrides [tenants]
# state = "detached"          # keep configured, stop replicating

[tenant.acme.destination]
kind = "snowflake"

[tenant.acme.snowflake]
account_url = "https://org-acct.snowflakecomputing.com/"
user = "WALSHADOW_ACME"
role = "WALSHADOW_ACME"
warehouse = "WALSHADOW_WH"
database = "ACME"
ack_after = "apply"          # or "outbox", see below
[tenant.acme.snowflake.auth]
method = "jwt"
account = "ORG-ACCT"
private_key_file = "/run/secrets/walshadow/acme.p8"
public_key_fingerprint = "SHA256:…"
[tenant.acme.snowflake.state]
directory = "/var/lib/walshadow/snowflake/acme"
max_bytes = 10737418240
[tenant.acme.snowflake.stage]
bucket = "walshadow-stage"
prefix = "acme"
region = "us-east-2"
name = "ACME.WALSHADOW_INTERNAL.WALSHADOW_STAGE"

[tenant.acme.stream]
replicate_all = true
```

Tenant ids are 1–63 characters of `[a-z0-9_-]`. Two tenants may not follow
one database, write one Snowflake database and internal schema, or share a
state directory; the daemon and `ctl` refuse such configs. With no
`[tenants]` and no `[tenant.*]` the daemon runs the single-database layout
unchanged.

The replication role needs `CONNECT` on every tenant database and the
privileges a single-database deployment needs there (runtime-config tables,
`COPY` initial loads).

## Manage tenants

```bash
walshadow-stream ctl tenant list
walshadow-stream ctl tenant add acme --dbname acme --spec acme.toml
walshadow-stream ctl tenant show acme
walshadow-stream ctl tenant update acme --dbname acme --spec acme.toml
walshadow-stream ctl tenant detach acme
walshadow-stream ctl tenant attach acme
walshadow-stream ctl tenant remove acme
walshadow-stream ctl --tenant acme tables          # table commands in one tenant
walshadow-stream ctl --tenant acme add public orders --initial-load copy
```

A spec holds the tenant's sections without the `tenant.<id>.` prefix
(`[snowflake]`, `[stream]`, `[table.public.orders]`, …); `--spec -` reads
stdin. Every change is validated against the whole merged config before it
lands, then reloaded; SIGHUP after editing fragments by hand does the same.

| Change | Effect |
|---|---|
| New tenant | Attaches mid-stream, primes, initial-loads its tables, then streams |
| Table rules, pause-free tuning | Applies live through the tenant's own reload |
| Snowflake role, warehouse, user, credential files, statement timeout | Applies live |
| `dbname`, or Snowflake account, database, internal schema, stage, schema mapping | Detaches and re-attaches with fresh initial loads: durable state is bound to them |
| Snowflake channels, pools, merge interval, state, `ack_after` | Rejected until restart |
| `state = "detached"` / `ctl tenant detach` | Stops routing, stops holding WAL, keeps the destination |
| Removal | Same as detach, and forgets the tenant |

### SQL registry

Instead of config fragments, the registry can be rows in the admin database,
which fits provisioning systems that already write SQL:

```toml
[tenants]
registry = "sql"
registry_schema = "walshadow"   # table walshadow.tenant, created on first use
registry_poll_secs = 10
```

`ctl tenant` then writes rows; rows written directly take effect within one
poll. Columns: `id`, `dbname`, `spec` (the tenant table as TOML, without
`dbname`), `state` (`active` or `detached`). The daemon mirrors the rows into
`<config>.d/70-registry.toml`; edit the table, not the mirror. Keep
credentials as file references in the spec, as in config.

## How attaching works

A tenant added while the daemon streams gets no special slot or snapshot:

1. At a position the pump has routed every earlier record up to and shadow
   has replayed, walshadow seeds the tenant's descriptor history from
   shadow's catalog and starts routing its records, with nothing in scope.
2. It reads `F = pg_snapshot_xmax(pg_current_snapshot())`, waits until
   `pg_snapshot_xmin(...) ≥ F`, and takes `S = pg_current_wal_lsn()`. Every
   commit after `S` belongs to a transaction the tenant saw whole.
3. Once the pump passes `S`, the tenant's tables opt in with their initial
   loads, bounded at the next barrier past `S`, exactly like
   [table opt-ins](table-selection.md#choose-initial-load). Commits before
   `S` drain as nothing; the initial load covers them.

Tables in scope at `S` and their initial loads persist in
`<spill-dir>/tenants/<id>/tenant.toml`, so a restart resumes unfinished
loads and never re-primes an active tenant. A long-running or prepared
transaction delays priming; the daemon logs what it waits for every minute.

Use `copy` (default) or `none` for tenants. `base_backup` and `object_store`
initial loads read the whole cluster, which for one client database is
rarely worth it.

## Isolation and WAL retention

One slot is shared, so a tenant that cannot keep up would hold WAL on the
primary for every tenant. Three limits keep one client from affecting the
rest:

- A tenant whose queue refuses records for `stall_timeout_secs` is evicted.
- A tenant whose resume floor trails the pump by more than `max_lag_bytes`
  is evicted.
- A tenant whose pipeline fails is evicted. In the single-database layout a
  pipeline error still stops the daemon, as before.

An evicted tenant is detached: it stops holding WAL, and
`tenant.toml` records why. Fix the cause, then `ctl tenant attach`; the
tenant re-attaches with fresh initial loads. The other tenants never stop.

`ack_after = "outbox"` (Snowflake) acknowledges a batch once it is fsynced
in the tenant's local outbox and applies it in the background. A Snowflake
outage then fills that tenant's state directory, bounded by
`state.max_bytes`, instead of holding the primary's WAL. The state
directory then holds rows no other copy has: put it on durable, backed-up
storage. The default, `apply`, acknowledges after the verified apply
receipt.

## Monitor

Every tenant reports under a `tenant` label:

| Metric | Meaning |
|---|---|
| `walshadow_tenant_info{tenant,dbname,phase}` | `priming`, `active`, `pending` or `detached` |
| `walshadow_tenant_lag_bytes` | Pump position minus the tenant's resume floor |
| `walshadow_tenant_resume_safe_lsn` | What the slot holds back to for this tenant |
| `walshadow_tenant_ack_lsn` | Contiguous destination acknowledgment |
| `walshadow_tenant_queue_depth` | Records waiting for the tenant's decoder |
| `walshadow_tenant_xacts_active` | Transactions the tenant buffers |
| `walshadow_tenant_rows_emitted_total` | Rows delivered |
| `walshadow_tenant_backfill_copy_rows` | Rows its COPY initial loads shipped |

Alert on `walshadow_tenant_lag_bytes` approaching `max_lag_bytes`, and on
`phase="detached"` for a tenant you expect active.

## Shadow

Each tenant database gets its own pool of bridge workers in shadow, started
and stopped by a launcher as tenants come and go (`walshadow.tenant_databases`
in `walshadow_tenants.conf`, reloaded live). `capacity` reserves
`max_worker_processes` slots up front, since that setting needs a shadow
restart to change. An external, operator-managed shadow must load the
module and list tenant databases itself.

## Current limits

- A tenant follows exactly one database. `[database.*]` entries
  ([several databases](multi-database.md)) belong to the single-tenant
  layout, where every database shares one pipeline and destination; the
  daemon and `ctl` refuse them alongside `[tenants]`.
- `[toast] mode = "shadow"` is single-database only.
- Greenfield bootstrap with tenants builds shadow only (`direct` mode);
  tenants then initial-load with `copy`.
- `--start-lsn` is single-database only.
- Moving from the single-database layout to tenants starts tenants afresh:
  declare the database as a tenant and let it initial-load.
