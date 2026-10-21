//! Control-socket verbs that manage tenants
//!
//! With the config registry each tenant is one operator-visible fragment,
//! `<config>.d/60-tenant-<id>.toml`, holding its `[tenant.<id>]` table.
//! With the SQL registry the rows of `<schema>.tenant` in the source's admin
//! database are the record, mirrored into `<config>.d/70-registry.toml` so
//! every reload path reads one merged config. Either way a change is
//! validated against the whole merged config before it lands, then
//! reloaded, and the session reconciles its tenant set
//!
//! Verbs (TOML bodies):
//! - `tenants`: every declared tenant with its live phase and lag
//! - `tenant-put`: `[tenant.<id>]` fragment, creating or replacing it
//! - `tenant-remove`: `id = "…"`
//! - `tenant-state`: `id = "…"`, `state = "active" | "detached"`

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail, ensure};
use toml::{Table, Value};

use crate::control::{PublishOpts, ReloadCause, ReloadTx, SharedCtx, ok, ok_toml};
use crate::tenants::{
    Registry, TenantDecl, TenantsConfig, decls_from_rows, registry_ddl, validate_id,
};

/// Fragment the config registry keeps for tenant `id`
pub fn tenant_fragment_path(config: &Path, id: &str) -> PathBuf {
    config
        .with_extension("d")
        .join(format!("60-tenant-{id}.toml"))
}

/// Mirror of the SQL registry
pub fn registry_mirror_path(config: &Path) -> PathBuf {
    config.with_extension("d").join("70-registry.toml")
}

async fn merged(ctx: &SharedCtx) -> Result<Table> {
    Ok(crate::ch_emitter::load_effective(&ctx.ch_config, ctx.cli_base.clone()).await?)
}

fn tenants_of(root: &Table) -> Result<TenantsConfig> {
    TenantsConfig::from_table(root)?
        .context("no tenants configured: add a [tenants] section to the config to manage tenants")
}

/// Every tenant parses and every tenant's effective config parses: what
/// the daemon checks at boot, so an accepted change stays restart-safe
pub fn validate(root: &Table) -> Result<()> {
    crate::tenants::reject_database_sections(root)?;
    let Some(tenants) = TenantsConfig::from_table(root)? else {
        return check_destination(root);
    };
    for decl in &tenants.decls {
        check_destination(&decl.effective_table(root))
            .with_context(|| format!("tenant {}", decl.id))?;
    }
    ensure_distinct_destinations(root, &tenants)
}

/// The destination section and table rules parse, and a Snowflake
/// destination uses no ClickHouse-only routing
fn check_destination(root: &Table) -> Result<()> {
    let dest =
        crate::destination::config::DestinationConfig::from_table(root).context("destination")?;
    let cfg = crate::ch_emitter::EmitterConfig::from_table(root).map_err(|e| anyhow!("{e}"))?;
    if dest.snowflake.is_some() {
        crate::destination::config::check_snowflake_routing(&cfg)?;
    }
    Ok(())
}

/// Tenants own disjoint destinations: two tenants writing one Snowflake
/// database (whatever their internal schemas), one ClickHouse database or
/// one state directory would collide on tables, channels, receipts and
/// state locks
fn ensure_distinct_destinations(root: &Table, tenants: &TenantsConfig) -> Result<()> {
    let mut seen_snowflake = std::collections::BTreeMap::new();
    let mut seen_state = std::collections::BTreeMap::new();
    let mut seen_ch = std::collections::BTreeMap::new();
    for decl in &tenants.decls {
        let eff = decl.effective_table(root);
        let dest = crate::destination::config::DestinationConfig::from_table(&eff)?;
        if let Some(sf) = dest.snowflake {
            let key = (
                sf.account_url.to_ascii_lowercase(),
                sf.database.to_ascii_uppercase(),
            );
            if let Some(other) = seen_snowflake.insert(key, decl.id.clone()) {
                bail!(
                    "tenants {other} and {} share Snowflake database {}; give each tenant \
                     its own database",
                    decl.id,
                    sf.database,
                );
            }
            if let Some(other) = seen_state.insert(sf.state.directory.clone(), decl.id.clone()) {
                bail!(
                    "tenants {other} and {} share Snowflake state directory {}",
                    decl.id,
                    sf.state.directory.display()
                );
            }
        } else if eff.contains_key("ch") {
            let cfg = crate::ch_emitter::EmitterConfig::from_table(&eff)
                .map_err(|e| anyhow!("tenant {}: {e}", decl.id))?;
            for (endpoint, database) in crate::tenants::clickhouse_scopes(&cfg) {
                let key = (endpoint.clone(), database.clone());
                if let Some(other) = seen_ch.insert(key, decl.id.clone()) {
                    bail!(
                        "tenants {other} and {} both reach ClickHouse database {database} on \
                         {endpoint}; give each tenant its own databases, including \
                         `[ch] database`, namespace and table overrides, instances and tees",
                        decl.id,
                    );
                }
            }
        }
    }
    Ok(())
}

pub async fn list(ctx: &SharedCtx) -> Result<String> {
    let root = merged(ctx).await?;
    let tenants = tenants_of(&root)?;
    let live = ctx.metrics.snapshot().await.tenants;
    let mut rows = Vec::new();
    for decl in &tenants.decls {
        let mut row = Table::new();
        row.insert("id".into(), decl.id.clone().into());
        row.insert("dbname".into(), decl.dbname.clone().into());
        row.insert(
            "desired".into(),
            match decl.desired {
                crate::tenants::Desired::Active => "active",
                crate::tenants::Desired::Detached => "detached",
            }
            .into(),
        );
        match live.iter().find(|t| t.id == decl.id) {
            Some(t) => {
                row.insert("phase".into(), t.phase.into());
                row.insert("lag_bytes".into(), (t.lag_bytes as i64).into());
                row.insert(
                    "ack_lsn".into(),
                    walrus::pg::backup::format_pg_lsn(t.ack_lsn)
                        .to_string()
                        .into(),
                );
                row.insert("rows_emitted".into(), (t.rows_emitted as i64).into());
            }
            None => {
                row.insert("phase".into(), "pending".into());
            }
        }
        rows.push(Value::Table(row));
    }
    let mut out = Table::new();
    out.insert("tenants".into(), Value::Array(rows));
    Ok(ok_toml(&out))
}

pub async fn show(ctx: &SharedCtx, body: &Table) -> Result<String> {
    let id = id_of(body)?;
    let root = merged(ctx).await?;
    let tenants = tenants_of(&root)?;
    let decl = tenants
        .get(&id)
        .with_context(|| format!("no tenant {id}"))?;
    Ok(ok_toml(&crate::control::masked(decl.to_fragment())))
}

pub async fn put(ctx: &SharedCtx, body: &Table) -> Result<String> {
    let (id, tenant_body) = single_tenant(body)?;
    let decl = TenantDecl::parse(&id, &tenant_body)?;
    let _mutation = ctx.mutation_lock.lock().await;
    let mut tx = ctx.reloader.begin().await;
    let root = merged(ctx).await?;
    let tenants = tenants_of(&root)?;
    match tenants.registry {
        Registry::Config => {
            let path = tenant_fragment_path(&ctx.ch_config, &id);
            write_validated(ctx, &tx, &path, Some(decl.to_fragment())).await?
        }
        Registry::Sql { schema, .. } => {
            let mut spec = decl.to_fragment();
            let spec = spec
                .get_mut("tenant")
                .and_then(|t| t.get_mut(&id))
                .and_then(Value::as_table_mut)
                .map(std::mem::take)
                .unwrap_or_default();
            let mut spec = spec;
            spec.remove("dbname");
            let state = match spec
                .remove("state")
                .and_then(|v| v.as_str().map(str::to_owned))
            {
                Some(s) => s,
                None => "active".into(),
            };
            // Validate the would-be merged config before the row lands
            let mut preview = root.clone();
            crate::ch_emitter::merge_tables(&mut preview, decl.to_fragment());
            validate(&preview)?;
            let client = crate::control::pg_connect(&root, None).await?;
            client.batch_execute(&registry_ddl(&schema)).await?;
            let s = crate::pg::quote_ident(&schema);
            client
                .execute(
                    &format!(
                        "INSERT INTO {s}.tenant (id, dbname, spec, state, updated_at) \
                         VALUES ($1, $2, $3, $4, now()) \
                         ON CONFLICT (id) DO UPDATE SET dbname = EXCLUDED.dbname, \
                         spec = EXCLUDED.spec, state = EXCLUDED.state, updated_at = now()"
                    ),
                    &[&id, &decl.dbname, &toml::to_string(&spec)?, &state],
                )
                .await
                .context("upsert registry row")?;
            mirror_registry(&tx, &ctx.ch_config, &root, &schema).await?;
        }
    }
    publish(&mut tx).await
}

/// Publish what a tenant verb wrote, still under its reload lock
async fn publish(tx: &mut ReloadTx<'_>) -> Result<String> {
    let published = tx
        .publish(ReloadCause::Control, PublishOpts::default())
        .await
        .map_err(|e| anyhow!("reload: {e}"))?;
    if let Some((id, e)) = published.tenant_errors.first() {
        bail!("reload tenant {id}: {e}");
    }
    Ok(ok())
}

pub async fn remove(ctx: &SharedCtx, body: &Table) -> Result<String> {
    let id = id_of(body)?;
    let _mutation = ctx.mutation_lock.lock().await;
    let mut tx = ctx.reloader.begin().await;
    let root = merged(ctx).await?;
    let tenants = tenants_of(&root)?;
    ensure!(tenants.get(&id).is_some(), "no tenant {id}");
    match tenants.registry {
        Registry::Config => {
            let path = tenant_fragment_path(&ctx.ch_config, &id);
            ensure!(
                tokio::fs::try_exists(&path).await.unwrap_or(false),
                "tenant {id} is declared outside {}; edit the operator config to remove it",
                path.display()
            );
            write_validated(ctx, &tx, &path, None).await?
        }
        Registry::Sql { schema, .. } => {
            let client = crate::control::pg_connect(&root, None).await?;
            let s = crate::pg::quote_ident(&schema);
            client
                .execute(&format!("DELETE FROM {s}.tenant WHERE id = $1"), &[&id])
                .await?;
            mirror_registry(&tx, &ctx.ch_config, &root, &schema).await?;
        }
    }
    publish(&mut tx).await
}

pub async fn set_state(ctx: &SharedCtx, body: &Table) -> Result<String> {
    let id = id_of(body)?;
    let state = body
        .get("state")
        .and_then(Value::as_str)
        .context("state = \"active\" or \"detached\" required")?;
    ensure!(
        matches!(state, "active" | "detached"),
        "state must be active or detached"
    );
    let _mutation = ctx.mutation_lock.lock().await;
    let mut tx = ctx.reloader.begin().await;
    let root = merged(ctx).await?;
    let tenants = tenants_of(&root)?;
    let decl = tenants
        .get(&id)
        .with_context(|| format!("no tenant {id}"))?;
    let mut next = decl.clone();
    next.desired = if state == "active" {
        crate::tenants::Desired::Active
    } else {
        crate::tenants::Desired::Detached
    };
    match tenants.registry {
        Registry::Config => {
            let path = tenant_fragment_path(&ctx.ch_config, &id);
            write_validated(ctx, &tx, &path, Some(next.to_fragment())).await?
        }
        Registry::Sql { schema, .. } => {
            let client = crate::control::pg_connect(&root, None).await?;
            let s = crate::pg::quote_ident(&schema);
            client
                .execute(
                    &format!("UPDATE {s}.tenant SET state = $2, updated_at = now() WHERE id = $1"),
                    &[&id, &state],
                )
                .await?;
            mirror_registry(&tx, &ctx.ch_config, &root, &schema).await?;
        }
    }
    publish(&mut tx).await
}

/// Write or delete `path`, keeping the change only if the merged config
/// still validates. `_tx` proves the reload lock is held
async fn write_validated(
    ctx: &SharedCtx,
    _tx: &ReloadTx<'_>,
    path: &Path,
    fragment: Option<Table>,
) -> Result<()> {
    let prev = tokio::fs::read(path).await.ok();
    match &fragment {
        Some(f) => {
            if let Some(dir) = path.parent() {
                tokio::fs::create_dir_all(dir).await?;
            }
            tokio::fs::write(path, toml::to_string(f)?).await?;
        }
        None => {
            let _ = tokio::fs::remove_file(path).await;
        }
    }
    let check = async { validate(&merged(ctx).await?) }.await;
    if let Err(e) = check {
        match prev {
            Some(bytes) => tokio::fs::write(path, bytes).await?,
            None => {
                let _ = tokio::fs::remove_file(path).await;
            }
        }
        return Err(e).context("rejected: merged config invalid");
    }
    Ok(())
}

/// Rewrite the registry mirror from the rows; returns whether it changed.
/// The daemon polls this, so rows edited straight in SQL also take effect.
/// `_tx` proves the reload lock is held: every mirror writer serializes on it
pub async fn mirror_registry(
    _tx: &ReloadTx<'_>,
    config: &Path,
    root: &Table,
    schema: &str,
) -> Result<bool> {
    let client = crate::control::pg_connect(root, None).await?;
    client.batch_execute(&registry_ddl(schema)).await?;
    let s = crate::pg::quote_ident(schema);
    let rows = client
        .query(
            &format!("SELECT id, dbname, spec, state FROM {s}.tenant ORDER BY id"),
            &[],
        )
        .await
        .context("read tenant registry")?;
    let rows: Vec<(String, String, String, String)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    let decls = decls_from_rows(&rows)?;
    let mut mirror = Table::new();
    for decl in &decls {
        crate::ch_emitter::merge_tables(&mut mirror, decl.to_fragment());
    }
    let text = format!(
        "# Mirror of {schema}.tenant, rewritten by walshadow; edit the table, not this file\n{}",
        toml::to_string(&mirror)?
    );
    let path = registry_mirror_path(config);
    if tokio::fs::read_to_string(&path).await.ok().as_deref() == Some(text.as_str()) {
        return Ok(false);
    }
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, text).await?;
    tokio::fs::rename(&tmp, &path).await?;
    Ok(true)
}

fn id_of(body: &Table) -> Result<String> {
    let id = body
        .get("id")
        .and_then(Value::as_str)
        .context("id = \"<tenant>\" required")?;
    validate_id(id)?;
    Ok(id.to_string())
}

/// The one `[tenant.<id>]` table a put carries
fn single_tenant(body: &Table) -> Result<(String, Table)> {
    let tenants = body
        .get("tenant")
        .and_then(Value::as_table)
        .context("body must be one [tenant.<id>] table")?;
    ensure!(
        tenants.len() == 1,
        "body must hold exactly one [tenant.<id>] table"
    );
    let (id, t) = tenants.iter().next().unwrap();
    let t = t
        .as_table()
        .with_context(|| format!("[tenant.{id}] must be a table"))?;
    Ok((id.clone(), t.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(text: &str) -> Table {
        toml::from_str(text).unwrap()
    }

    fn snowflake_tenant(id: &str, database: &str, internal_schema: &str, extra: &str) -> String {
        format!(
            r#"
[tenant.{id}]
dbname = "{id}_db"
[tenant.{id}.destination]
kind = "snowflake"
[tenant.{id}.snowflake]
account_url = "https://acme-test.snowflakecomputing.com"
user = "REPLICATOR"
role = "REPLICATION"
warehouse = "INGEST"
database = "{database}"
internal_schema = "{internal_schema}"
[tenant.{id}.snowflake.auth]
method = "oauth"
token_file = "/run/secrets/snowflake"
[tenant.{id}.snowflake.state]
directory = "/var/lib/walshadow/snowflake/{id}"
max_bytes = 10737418240
[tenant.{id}.snowflake.stage]
bucket = "replication"
prefix = "{id}/"
region = "us-east-1"
name = "{database}.INTERNAL.LOAD_STAGE"
{extra}
"#
        )
    }

    const CLUSTER: &str = "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n";

    #[test]
    fn database_sections_are_refused_in_either_layout() {
        let single = "[source]\ndbname = 'app'\n[ch]\nhost = 'h'\n\
                      [database.billing.table.public.ledger]\nreplicate = true\n";
        let err = validate(&root(single)).unwrap_err().to_string();
        assert!(err.contains("[tenant.<id>]"), "{err}");
        assert!(validate(&root(&format!("{CLUSTER}[database.b.table.public.t]\n"))).is_err());
    }

    #[test]
    fn snowflake_tenants_own_whole_databases() {
        let distinct = format!(
            "{CLUSTER}{}{}",
            snowflake_tenant("a", "A_DB", "WALSHADOW_INTERNAL", ""),
            snowflake_tenant("b", "B_DB", "WALSHADOW_INTERNAL", "")
        );
        validate(&root(&distinct)).unwrap();
        // A separate internal schema leaves public views colliding
        let shared = format!(
            "{CLUSTER}{}{}",
            snowflake_tenant("a", "SHARED", "A_INTERNAL", ""),
            snowflake_tenant("b", "SHARED", "B_INTERNAL", "")
        );
        let err = validate(&root(&shared)).unwrap_err().to_string();
        assert!(err.contains("share Snowflake database SHARED"), "{err}");
    }

    #[test]
    fn snowflake_tenants_refuse_clickhouse_only_routing() {
        for extra in [
            "[tenant.a.table.public.orders]\nreplicate = true\n\
             tee = [{ table = 'orders_audit' }]\n",
            "[tenant.a.table.public.events_2026]\nroute = 'self'\n",
        ] {
            let cfg = format!(
                "{CLUSTER}{}",
                snowflake_tenant("a", "A_DB", "WALSHADOW_INTERNAL", extra)
            );
            assert!(validate(&root(&cfg)).is_err(), "{extra}");
        }
    }

    #[test]
    fn clickhouse_tenants_reaching_one_database_are_refused() {
        let ch = |id: &str, body: &str| {
            format!("[tenant.{id}]\ndbname = '{id}_db'\n[tenant.{id}.ch]\nhost = 'ch'\n{body}")
        };
        validate(&root(&format!(
            "{CLUSTER}{}{}",
            ch("a", "database = 'a'\n"),
            ch("b", "database = 'b'\n")
        )))
        .unwrap();
        // Different hosts may reuse a database name
        validate(&root(&format!(
            "{CLUSTER}{}[tenant.b]\ndbname = 'b_db'\n[tenant.b.ch]\nhost = 'other'\ndatabase = 'a'\n",
            ch("a", "database = 'a'\n"),
        )))
        .unwrap();
        let layered_route = format!(
            "{CLUSTER}{}[tenant.b]\ndbname = 'b_db'\n[tenant.b.ch]\nhost = 'other'\ndatabase = 'b'\n\
             [tenant.b.ch.instances.mirror]\nhost = 'ch'\ndatabase = 'mirror'\n\
             [tenant.b.table.public.\"*\"]\nmatch = 'glob'\ntarget_database = 'a'\n\
             [tenant.b.table.public.orders]\nreplicate = true\ntarget_instance = 'mirror'\n\
             tee = [{{ table = 'audit' }}]\n",
            ch("a", "database = 'a'\n"),
        );
        let err = validate(&root(&layered_route)).unwrap_err().to_string();
        assert!(err.contains("both reach ClickHouse database a"), "{err}");
        for converging in [
            // Both left at the default database
            String::new(),
            "database = 'b'\n[tenant.b.namespace.public]\ntarget_database = 'a'\n".into(),
            "database = 'b'\n[tenant.b.table.public.orders]\nreplicate = true\n\
             target_database = 'a'\n"
                .into(),
            "database = 'b'\n[tenant.b.table.public.orders]\nreplicate = true\n\
             tee = [{ database = 'a', table = 'orders_audit' }]\n"
                .into(),
            "database = 'b'\n[tenant.b.ch.instances.mirror]\nhost = 'ch'\ndatabase = 'a'\n".into(),
        ] {
            let a = if converging.is_empty() {
                ch("a", "")
            } else {
                ch("a", "database = 'a'\n")
            };
            let cfg = format!("{CLUSTER}{a}{}", ch("b", &converging));
            let err = validate(&root(&cfg)).unwrap_err().to_string();
            assert!(
                err.contains("both reach ClickHouse database"),
                "{converging}: {err}"
            );
        }
    }
}
