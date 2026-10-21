# HTTP API

An authenticated API for health checks, metrics, crash points and recent
errors, jobs and tenant status, and full tenant control: create, edit,
detach, attach, delete and restore. Every route needs an API key except
`/livez` when you ask for it to be public.

## Enable

```bash
walshadow-stream api-key new --id ops --scope admin   >ops.token 2>>api-keys.toml
walshadow-stream api-key new --id dash --scope read   >dash.token 2>>api-keys.toml
chmod 600 api-keys.toml

walshadow-stream … \
    --api-bind 0.0.0.0:9485 \
    --api-keys-file /run/secrets/walshadow/api-keys.toml \
    --api-tls-cert /run/secrets/walshadow/api.crt \
    --api-tls-key /run/secrets/walshadow/api.key
```

`api-key new` prints the token once on stdout and the key file stanza on
stderr. The key file holds only a SHA-256 of each secret, never a token:

```toml
[[key]]
id = "ops"                            # 1-32 of [a-z0-9-]; audit records name it
scopes = ["admin"]                    # "read" or "admin"; admin implies read
sha256 = "<64 hex>"
expires_at = "2027-01-01T00:00:00Z"   # optional
disabled = false                      # optional
description = "deployer"              # optional
```

The daemon refuses a key file readable by group or others. It reloads the
file on SIGHUP and when its modification time changes (checked every 5 s), so
adding, rotating or revoking a key needs no restart. A reload that fails
rejects every key until the file is fixed: a botched edit never leaves a
revoked key working. Replication is unaffected either way.

Keep the key file, tokens and TLS key out of the repository. Mount them as
secrets.

| Flag | Env | Default |
|---|---|---|
| `--api-bind` | `WALSHADOW_API_BIND` | off |
| `--api-keys-file` | `WALSHADOW_API_KEYS_FILE` | required with `--api-bind` |
| `--api-tls-cert`, `--api-tls-key` | `WALSHADOW_API_TLS_CERT`, `WALSHADOW_API_TLS_KEY` | required off loopback |
| `--api-allow-plaintext` | `WALSHADOW_API_ALLOW_PLAINTEXT` | false |
| `--api-state-dir` | `WALSHADOW_API_STATE_DIR` | `<spill-dir>/api` |
| `--api-public-livez` | `WALSHADOW_API_PUBLIC_LIVEZ` | false |
| `--api-trusted-proxies` | `WALSHADOW_API_TRUSTED_PROXIES` | none |
| `--api-rate-limit <class>=<n>/<s\|m\|h>:<burst>` | | see below |
| `--api-deleted-retention-days` | | 30 |
| `--api-ready-grace-secs` | | 600 |

A bind off loopback without TLS is refused at startup. Set
`--api-allow-plaintext` only when a proxy in front terminates TLS, and list
that proxy in `--api-trusted-proxies` so the client address in audit records
and rate limits comes from its `X-Forwarded-For`. These settings are flags
on purpose, never config keys: the API can edit the merged config, so its own
authentication stays outside it.

## Authenticate

Send `Authorization: Bearer <token>`. A token in the query string is
refused. A missing, malformed, unknown, wrong, expired or disabled key gets
`401` with `WWW-Authenticate: Bearer realm="walshadow"`; a valid key without
the route's scope gets `403`. Paths that do not exist answer `401` before
`404`, so nothing enumerates without a key.

Every response carries `X-Request-Id`, `Cache-Control: no-store` and
`X-Content-Type-Options: nosniff`. Errors share one body:

```json
{"error": {"code": "resync_not_acknowledged", "message": "…", "request_id": "…"}}
```

| Status | Code | Meaning |
|---|---|---|
| 400 | `invalid_body` | The body or query does not have the route's shape, or names another tenant |
| 400 | `invalid_id` | A tenant id is not 1–63 characters of `[a-z0-9_-]`, or is reserved |
| 400 | `invalid_tenant` | The tenant declaration does not parse |
| 4xx | `invalid_request` | Rejected before the route ran: malformed JSON, a missing `Content-Type`, an oversized body |
| 401 | `unauthenticated` | No valid key |
| 403 | `forbidden` | The key lacks the route's scope |
| 404 | `not_found` | No such tenant, tombstone, operation or crash record |
| 409 | `exists` | The tenant exists already, also when restoring onto a live id |
| 409 | `resync_not_acknowledged` | The change attaches the tenant afresh; resend with `acknowledge_resync` |
| 409 | `rebind_pending` | A re-bind of the tenant is still pending |
| 409 | `change_rejected` | The daemon cannot apply this change while running, or the new Snowflake state directory is unusable |
| 409 | `conflict_after_failed_apply` | A change the daemon refused could not be rolled back: someone changed the tenant meanwhile |
| 409 | `confirmation_invalid` | The delete token is unknown, used, or void |
| 409 | `managed_outside_api` | The tenant is declared outside the registry the API edits |
| 409 | `tenants_not_configured` | No `[tenants]`: the daemon runs the single-database layout |
| 409 | `invalid_config` | The running config's tenants do not parse |
| 410 | `confirmation_expired` | The delete token expired; ask again |
| 412 | `precondition_failed` | `If-Match` names an ETag the tenant no longer has |
| 422 | `invalid_config` | The config the change would produce does not validate |
| 422 | `apply_refused` | The tenant's resolver refused the change; it was rolled back |
| 428 | `precondition_required` | `If-Match` is missing |
| 428 | `confirmation_required` | `DELETE` without `X-Confirm-Delete` |
| 429 | `rate_limited` | Over a rate limit; wait `Retry-After` seconds |
| 500 | `internal` | An unexpected failure; the message says what |
| 503 | `audit_unavailable` | The audit log refused the attempt, so nothing changed |
| 503 | `detach_not_recorded` | The detach could not be recorded, so nothing changed; retry |
| 503 | `registry_unavailable` | The SQL tenant registry is unreachable |
| 503 | `source_unavailable` | The source database is unreachable |
| 503 | `busy` | Too many requests in flight; retry shortly |

## Endpoints

| Method | Path | Scope | |
|---|---|---|---|
| GET | `/livez` | none with `--api-public-livez`, else read | Constant `ok` |
| GET | `/v1/whoami` | read | Key id, scopes, expiry |
| GET | `/v1/health/live` | read | The process answers |
| GET | `/v1/health/ready` | read | `200` ready, `503` not |
| GET | `/v1/health` | read | Every check, below |
| GET | `/v1/status` | read | `ctl status` as JSON, plus `epoch` and generations |
| GET | `/v1/metrics` | read | OpenMetrics, same as `--metrics-bind` |
| GET | `/v1/errors` | read | Recent warnings and errors: `level`, `tenant`, `target`, `since_seq`, `limit` |
| GET | `/v1/crashes`, `/v1/crashes/{id}` | read | Panic, fatal-exit and unclean-exit records |
| GET | `/v1/jobs` | read | Initial loads, priming, teardowns |
| GET | `/v1/tenants` | read | Every tenant, its phase, lag and destination |
| GET | `/v1/tenants/{id}` | read | One tenant, redacted spec, jobs; `ETag` header |
| GET | `/v1/tenants/{id}/jobs`, `/v1/tenants/{id}/errors` | read | |
| GET | `/v1/tenants/{id}/tables` | read | Source catalog and selection (opens a source connection) |
| GET | `/v1/config` | admin | Merged config, secrets masked |
| GET | `/v1/audit` | admin | Audit records: `since_seq`, `tenant`, `action`, `limit` |
| GET | `/v1/operations/{request_id}` | admin | What a mutation did, for a client that timed out |
| POST | `/v1/tenants` | admin | Create |
| PUT | `/v1/tenants/{id}` | admin | Replace; `If-Match` |
| PATCH | `/v1/tenants/{id}` | admin | JSON merge patch (RFC 7396); `If-Match` |
| POST | `/v1/tenants/{id}/detach` | admin | `If-Match` |
| POST | `/v1/tenants/{id}/attach` | admin | `If-Match`, `acknowledge_resync` |
| POST | `/v1/tenants/{id}/delete-intent` | admin | Impact and a confirmation token |
| DELETE | `/v1/tenants/{id}` | admin | `X-Confirm-Delete`, `If-Match` |
| GET | `/v1/tenants-deleted`, `/v1/tenants-deleted/{id}` | admin | Restorable tombstones |
| POST | `/v1/tenants-deleted/{id}/restore` | admin | Declare it again |
| POST | `/v1/stream/pause`, `/v1/stream/resume` | admin | Cluster pause, as `ctl pause` |
| POST | `/v1/reload` | admin | As `ctl reload` or SIGHUP |

```bash
curl -s https://walshadow:9485/v1/tenants -H "Authorization: Bearer $(cat dash.token)"
```

Prometheus scrapes `/v1/metrics` with a read key:

```yaml
scrape_configs:
  - job_name: walshadow
    scheme: https
    metrics_path: /v1/metrics
    authorization:
      credentials_file: /etc/prometheus/walshadow.token
    static_configs:
      - targets: ["walshadow:9485"]
```

`--metrics-bind` still serves the same metrics without authentication. Bind
it to loopback, or drop it once Prometheus scrapes the API.

## Change tenants

A tenant is its `[tenant.<id>]` table as JSON: `dbname`, optional `state`,
`initial_load`, `max_lag_bytes`, and the sections a single-database config
carries (`ch` or `snowflake` and `destination`, `stream`, `table`,
`namespace`, …).

```bash
curl -s -X POST https://walshadow:9485/v1/tenants -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{
    "id": "acme", "dbname": "acme",
    "ch": {"host": "ch.internal", "database": "acme", "user": "acme", "password": "…"},
    "stream": {"replicate_all": true}
  }'
```

An inline password is accepted, as with `ctl tenant add`, and masked in every
response, audit record and log line. Snowflake credentials are file
references only (`token_file`, `private_key_file`).

Edits need the tenant's `ETag` in `If-Match`, from `GET /v1/tenants/{id}`:
a change made since, by the API, `ctl` or a hand edit, answers `412`. A
mutation returns `202` once the change is persisted, every resolver applied
it, and its config revision published:

```json
{"request_id": "…", "classification": "live", "etag": "r-…", "epoch": "…", "generation": 42}
```

Attaching and detaching then happen in the daemon's session loop. Poll the
tenant until `GET /v1/status` reports `reconciled_generation` at least the
returned `generation` in the same `epoch`, then read `status.phase`. A new
`epoch` means the daemon restarted: read the tenant again.

`?dry_run=true` validates and classifies without changing anything. Every
mutation classifies its change first:

| Classification | Meaning |
|---|---|
| `live` | Applies to the running tenant, no resync |
| `fresh_attach` | A detached tenant becomes active: it attaches afresh, with fresh initial loads |
| `rebind` | What the running tenant was built with changes: it detaches and attaches afresh, with fresh initial loads |
| `rejected` | The daemon cannot apply it while running; `409` and nothing changes |

A `fresh_attach` or `rebind` needs `"acknowledge_resync": true` in the body
or `?acknowledge_resync=true`, or answers `409 resync_not_acknowledged`. A
setting the daemon does not classify is refused.

| Setting | Class |
|---|---|
| `dbname` | rebind |
| Snowflake account, database, internal schema, stage, schema mapping | rebind, with `snowflake.state.directory` moved to a fresh, empty path in the same edit; otherwise rejected |
| Snowflake role, warehouse, user, `auth`, `statement_timeout_secs`, batch and flush sizes | live |
| Snowflake channels, pools, `merge_interval_ms`, `ack_after`, `state` alone | rejected (restart) |
| `destination` kind | rejected (restart) |
| ClickHouse databases the tenant reaches (`ch` host, port, database, instances, namespace and table targets, tees) | rebind |
| ClickHouse user, password, TLS, batch, flush, retry, compression | live |
| `ch.decoder_pool_size`, `ch.inserter_pool_size`, `ch.soft_delete` | rebind |
| `ch.decoder_batch_size`, `ch.decoder_queue_capacity` | rejected (restart) |
| Table, namespace and column rules | live |
| `stream.replicate_all`, `runtime_config.schema`, `system_columns`, `toast.mode` | rebind |
| `max_lag_bytes`, `initial_load` | live |

While one re-bind of a tenant is pending, until it has re-attached, a
further re-bind of that tenant answers `409 rebind_pending`. A change the
tenant's own resolver refuses is rolled back and answers `422
apply_refused`, unless someone changed the tenant meanwhile, then `409
conflict_after_failed_apply` and nothing is restored.

Detaching stops routing to the tenant and holding WAL for it at once; its
pipeline drains in the background, for up to `[tenants] stall_timeout_secs`,
then is stopped. Its destination and directories stay reserved until every
one of its writers has stopped, so no other tenant attaches to them sooner;
such a tenant reports `waiting`.

A detach, delete or remove of an attached tenant first records it as
detached in `<spill-dir>/tenants/<id>/tenant.toml`. If that write fails,
nothing changes: the tenant stays attached and replicating, the request
answers `503 detach_not_recorded` with the cause, the audit log records it
as rejected, and the caller retries once the spill directory is writable.

A re-bind stays pending until the tenant has re-attached with the new
binding, whatever is published meanwhile. `tenant.toml` records the binding
each attachment was built with, so a restart that finds the config asking
for another one attaches the tenant afresh instead of resuming.

Once a mutation starts, it runs to completion, operation record and
tombstone included, even if the client disconnects or the request times out.
`GET /v1/operations/{request_id}` reports how it ended.

## Delete and restore

Deleting takes two requests and keeps everything but the declaration:

```bash
curl -s -X POST https://walshadow:9485/v1/tenants/acme/delete-intent -H "Authorization: Bearer $TOKEN"
# {"confirm_token": "…", "expires_in_secs": 300, "etag": "r-…", "impact": {…}}
curl -s -X DELETE https://walshadow:9485/v1/tenants/acme -H "Authorization: Bearer $TOKEN" \
  -H 'If-Match: "r-…"' -H 'X-Confirm-Delete: …'
```

The token is single use, lasts five minutes, and is bound to the key that
asked, the tenant as it stood, and its current attachment: any change to the
tenant, or a daemon restart, voids it.

| On delete | |
|---|---|
| Replication slot | Shared by every tenant; it stops holding WAL for this one |
| Publications | None: walshadow reads physical WAL |
| ClickHouse databases, tables and rows | Kept |
| Snowflake objects, stage files and the Snowflake state directory | Kept |
| `<spill-dir>/tenants/<id>` | Kept, cleared only if the id attaches again |
| The declaration | Kept as a tombstone in `<api-state-dir>/deleted` |

`POST /v1/tenants-deleted/{id}/restore` declares the tenant again, detached
unless the body says `{"desired": "active", "acknowledge_resync": true}`.
Tombstones expire after `--api-deleted-retention-days`; expiry removes the
tombstone file only. Reclaiming a deleted tenant's disk is a manual step,
once its id is no longer declared.

## Health

| Check | Says |
|---|---|
| `snapshot` | The session published within 5 s; otherwise every check is `unknown` |
| `api_keys` | The key file loaded |
| `session` | Streaming; `failing` when still starting after `--api-ready-grace-secs` |
| `source` | `blocked` while an endpoint swap waits on a proof |
| `shadow_replay` | `degraded` over 1 GiB behind |
| `destination` | `failing` when no acknowledgement arrived for 5 minutes while work waits; idle is healthy |
| `crossing` | `blocked` while a timeline crossing waits |
| `paused` | `paused` while an operator paused the source |
| `tenants` | `degraded` when a teardown left a writer running |
| `reconcile` | `degraded` when the session trails the published config for a minute |

`/v1/health/ready` answers `503` when a check is `failing` or `unknown`, or
before the session streams. Paused, blocked and degraded stay ready.

## Crash points and errors

`/v1/crashes` lists, newest first and across restarts: every panic, every
fatal exit with its error chain, and every unclean exit, a process that
stopped without either (SIGKILL, the OOM killer, a host failure). Read
`/v1/crashes/{id}` for a panic's thread, location and bounded backtrace.
`/v1/errors` keeps the last 1000 warnings and
errors with their tenant, database, error and reason fields. A tenant's
detach and eviction reasons are on `/v1/tenants/{id}` and
`/v1/tenants/{id}/errors`. Secrets are scrubbed from all of them.

## Audit log

Every control-plane mutation, from the API and the control socket alike, is
appended to `<api-state-dir>/audit/audit.jsonl` (0600):

```json
{"v": 1, "seq": 42, "ts": "2026-10-21T15:04:05.123Z", "request_id": "…", "phase": "result",
 "actor": {"kind": "api_key", "id": "ops", "ip": "10.0.0.5"}, "action": "tenant.patch",
 "tenant": "acme", "method": "PATCH", "path": "/v1/tenants/acme",
 "before": {"ch": {"password": "***"}}, "after": {"ch": {"password": "***(changed)"}},
 "classification": "live", "outcome": "applied", "epoch": "…", "generation": 57,
 "prev_hash": "sha256:…", "hash": "sha256:…"}
```

A mutation that runs writes an `attempt` record, durably, before it changes
anything, then a `result`; one refused before it runs writes one `result`
with `outcome: rejected`; a key without the scope gets one `denied`. Dry runs
are not recorded. Changes nobody made through the API or the socket, a hand
edit and SIGHUP or a row written into the SQL registry, are recorded as
`tenant_set.external_change`. Secrets are masked; a changed one reads
`***(changed)`. Each record carries the hash of the one before it, so an edited
or dropped record breaks the chain `GET /v1/audit` reports as `chain_ok`.
Files rotate at 64 MiB, ten kept. Records also go to the log under the
`walshadow::audit` target.

## Rate limits

| Class | Per | Default |
|---|---|---|
| `preauth` | client address, before authentication | 50/s, burst 100 |
| `auth_failure` | client address, after a failed authentication | 10/min, burst 10 |
| `read` | key | 20/s, burst 40 |
| `expensive` | key (`/tables`) | 1/s, burst 3 |
| `mutate` | key | 1/s, burst 5 |

Over a limit answers `429` with `Retry-After`. Mutations also run one at a
time, socket and API together. Override with
`--api-rate-limit mutate=5/s:10` (repeatable).

## Docker

```yaml
services:
  walshadow:
    environment:
      WALSHADOW_API_BIND: 0.0.0.0:9485
      WALSHADOW_API_KEYS_FILE: /run/secrets/walshadow_api_keys
      WALSHADOW_API_TLS_CERT: /run/secrets/walshadow_api_crt
      WALSHADOW_API_TLS_KEY: /run/secrets/walshadow_api_key
    ports: ["9485:9485"]
    secrets: [walshadow_api_keys, walshadow_api_crt, walshadow_api_key]
secrets:
  walshadow_api_keys: {file: ./secrets/api-keys.toml}
  walshadow_api_crt: {file: ./secrets/api.crt}
  walshadow_api_key: {file: ./secrets/api.key}
```

Compose mounts file secrets with the host file's mode: `chmod 600` the key
file, owned by the user the container runs as. A Swarm secret needs
`mode: 0400`.
