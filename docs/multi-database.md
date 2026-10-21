# Replicate several databases in one cluster

PostgreSQL writes one WAL stream for all databases in a cluster, and walshadow
reads that stream once. To replicate several of its databases, declare a
[tenant](tenants.md) per database: one replication slot, one shadow
PostgreSQL and one decode of each WAL byte, while every tenant keeps its own
pipeline, destination, acknowledgement and failure isolation

`[source] dbname` alone names the database the single-database layout
replicates. `[database.<dbname>]` sections, which followed further databases
through that one shared pipeline and destination, are not supported: the
daemon, `SIGHUP`/`ctl reload` and `ctl apply` refuse a config holding them

## Move a `[database.*]` config to tenants

Give each database a tenant, with the tables it listed under
`[database.<dbname>]` moved to `[tenant.<id>.table.*]`, and keep cluster-wide
sections (`[source]`, `[memory]`, `[bootstrap]`, `[backup]`) at the top.
Point `[source] dbname` at an admin database the pump connects to. Tenants
start afresh and initial-load with `copy`, see [tenants](tenants.md)
