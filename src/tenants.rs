//! Tenants: one followed source database each, sharing one physical slot,
//! WAL pump and shadow.
//!
//! A cluster holding many client databases would otherwise need one daemon,
//! slot and full-cluster shadow per client. Every tenant gets its own
//! descriptor history, transaction buffer, pipeline, destination and state
//! directory; the pump routes each record to the tenant whose database wrote
//! it (see [`TenantRoute`]).
//!
//! Configuration is `[tenant.<id>]` tables in the merged config, usually one
//! `<config>.d/60-tenant-<id>.toml` each so `ctl tenant` can add, replace and
//! remove them. A tenant table holds `dbname` plus the sections a
//! single-database config would carry at the top level (`[destination]`,
//! `[snowflake]` or `[ch]`, `[stream]`, `[table.*]`, …). Without any tenant
//! table the daemon runs the classic single-database layout as tenant
//! [`LEGACY_TENANT`], with unchanged paths.
//!
//! Durable per-tenant state ([`TenantState`]) lives in
//! `<spill-dir>/tenants/<id>/tenant.toml` beside the tenant's own descriptor
//! log, backfill ledger and transaction spill.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// Tenant id of the single-database layout
pub const LEGACY_TENANT: &str = "default";

/// Top-level sections a tenant owns. Everything else (`[source]` endpoint
/// and slot, `[memory]`, `[bootstrap]`, `[backup]`) stays cluster-wide
pub const TENANT_SECTIONS: &[&str] = &[
    "destination",
    "ch",
    "snowflake",
    "stream",
    "table",
    "namespace",
    "runtime_config",
    "system_columns",
    "toast",
];

const STATE_FILE: &str = "tenant.toml";
const STATE_VERSION: u32 = 1;

/// What the operator wants a tenant to be
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Desired {
    /// Routed and holding the slot back to its progress
    #[default]
    Active,
    /// Configured but neither routed nor holding WAL. Re-activating resyncs
    /// every table with a fresh initial load
    Detached,
}

/// One `[tenant.<id>]` declaration
#[derive(Clone, Debug, PartialEq)]
pub struct TenantDecl {
    pub id: String,
    pub dbname: String,
    pub desired: Desired,
    /// Evict the tenant once its acknowledged position trails the pump by
    /// more than this. Overrides `[tenants] max_lag_bytes`
    pub max_lag_bytes: Option<u64>,
    /// Initial load applied to every in-scope table when the tenant attaches
    /// mid-stream (`copy` unless set; `none` skips existing rows)
    pub initial_load: String,
    /// Tenant-owned sections, shaped like a single-database config
    pub body: toml::Table,
}

/// Registry the daemon reconciles against
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Registry {
    /// `[tenant.<id>]` tables in the merged config
    Config,
    /// Rows of `<schema>.tenant` in the source's admin database, polled
    Sql { schema: String, poll: Duration },
}

/// `[tenants]` defaults plus every declaration
#[derive(Clone, Debug)]
pub struct TenantsConfig {
    pub decls: Vec<TenantDecl>,
    pub registry: Registry,
    /// Evict a tenant whose ack trails the pump by more than this. `None`
    /// never evicts on lag
    pub max_lag_bytes: Option<u64>,
    /// Evict a tenant whose queue refuses records this long: one stuck
    /// destination otherwise stalls the shared pump for everyone
    pub stall_timeout: Duration,
    /// Per-tenant pool sizes; tenants multiply every pool
    pub decoder_pool_size: usize,
    pub inserter_pool_size: usize,
    /// Bridge workers each tenant database gets in shadow
    pub bridge_workers: usize,
    /// Tenant bridge pools shadow reserves worker slots for
    pub capacity: usize,
}

impl TenantsConfig {
    /// `None`: no `[tenants]` and no `[tenant.*]`, the single-database layout
    pub fn from_table(root: &toml::Table) -> Result<Option<Self>> {
        if !root.contains_key("tenants") && !root.contains_key("tenant") {
            return Ok(None);
        }
        for section in TENANT_SECTIONS {
            ensure!(
                !root.contains_key(*section),
                "[{section}] belongs inside a [tenant.<id>] table once tenants are configured"
            );
        }
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct Defaults {
            registry: Option<String>,
            registry_schema: Option<String>,
            registry_poll_secs: Option<u64>,
            max_lag_bytes: Option<u64>,
            stall_timeout_secs: Option<u64>,
            decoder_pool_size: Option<usize>,
            inserter_pool_size: Option<usize>,
            bridge_workers: Option<usize>,
            capacity: Option<usize>,
        }
        let d: Defaults = root
            .get("tenants")
            .cloned()
            .map(|v| v.try_into())
            .transpose()
            .context("[tenants]")?
            .unwrap_or_default();
        let registry = match d.registry.as_deref().unwrap_or("config") {
            "config" => Registry::Config,
            "sql" => Registry::Sql {
                schema: d.registry_schema.unwrap_or_else(|| "walshadow".into()),
                poll: Duration::from_secs(d.registry_poll_secs.unwrap_or(10).max(1)),
            },
            other => bail!("[tenants] registry must be 'config' or 'sql', not {other:?}"),
        };
        let mut decls = Vec::new();
        if let Some(tenants) = root.get("tenant") {
            let tenants = tenants
                .as_table()
                .context("[tenant] must hold [tenant.<id>] tables")?;
            for (id, body) in tenants {
                let body = body
                    .as_table()
                    .with_context(|| format!("[tenant.{id}] must be a table"))?;
                decls.push(TenantDecl::parse(id, body)?);
            }
        }
        let cfg = Self {
            decls,
            registry,
            max_lag_bytes: d.max_lag_bytes,
            stall_timeout: Duration::from_secs(d.stall_timeout_secs.unwrap_or(300).max(1)),
            decoder_pool_size: d.decoder_pool_size.unwrap_or(2).max(1),
            inserter_pool_size: d.inserter_pool_size.unwrap_or(2).max(1),
            bridge_workers: d
                .bridge_workers
                .unwrap_or(2)
                .clamp(1, crate::ops::bridge::MAX_BRIDGE_WORKERS),
            capacity: d.capacity.unwrap_or(64).max(1),
        };
        cfg.validate()?;
        Ok(Some(cfg))
    }

    fn validate(&self) -> Result<()> {
        let mut dbs = BTreeMap::new();
        for decl in &self.decls {
            if let Some(other) = dbs.insert(decl.dbname.clone(), decl.id.clone()) {
                bail!(
                    "tenants {other} and {} both follow database {}",
                    decl.id,
                    decl.dbname
                );
            }
        }
        ensure!(
            self.decls.len() <= self.capacity,
            "{} tenants exceed [tenants] capacity {}",
            self.decls.len(),
            self.capacity
        );
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<&TenantDecl> {
        self.decls.iter().find(|d| d.id == id)
    }
}

impl TenantDecl {
    pub fn parse(id: &str, body: &toml::Table) -> Result<Self> {
        validate_id(id)?;
        let mut body = body.clone();
        let dbname = match body.remove("dbname") {
            Some(toml::Value::String(s)) if !s.is_empty() => s,
            _ => bail!("[tenant.{id}] requires dbname"),
        };
        ensure!(
            !dbname.contains('\0') && dbname.len() < 64,
            "[tenant.{id}] dbname is not a PostgreSQL database name"
        );
        let desired = match body.remove("state") {
            None => Desired::Active,
            Some(v) => v
                .try_into()
                .with_context(|| format!("[tenant.{id}] state: active or detached"))?,
        };
        let max_lag_bytes = match body.remove("max_lag_bytes") {
            None => None,
            Some(toml::Value::Integer(n)) if n > 0 => Some(n as u64),
            Some(_) => bail!("[tenant.{id}] max_lag_bytes must be a positive integer"),
        };
        let initial_load = match body.remove("initial_load") {
            None => "copy".to_string(),
            Some(toml::Value::String(mode)) => {
                mode.parse::<crate::runtime_config::InitialLoadMode>()
                    .map_err(|e| anyhow::anyhow!("[tenant.{id}] initial_load: {e}"))?;
                mode
            }
            Some(_) => bail!("[tenant.{id}] initial_load must be a string"),
        };
        for key in body.keys() {
            ensure!(
                TENANT_SECTIONS.contains(&key.as_str()),
                "[tenant.{id}] has unknown key {key:?}; tenant tables carry dbname, state, \
                 max_lag_bytes, initial_load and the sections {}",
                TENANT_SECTIONS.join(", ")
            );
        }
        Ok(Self {
            id: id.into(),
            dbname,
            desired,
            max_lag_bytes,
            initial_load,
            body,
        })
    }

    /// The tenant's effective config: cluster-wide sections of `root`, the
    /// tenant's own sections over them, `[source] dbname` pointing at the
    /// tenant's database. Shaped exactly like a single-database config, so
    /// every existing parser reads it unchanged
    pub fn effective_table(&self, root: &toml::Table) -> toml::Table {
        let mut out = root.clone();
        out.remove("tenant");
        out.remove("tenants");
        for (k, v) in &self.body {
            out.insert(k.clone(), v.clone());
        }
        let source = out
            .entry("source")
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if let toml::Value::Table(source) = source {
            source.insert("dbname".into(), self.dbname.clone().into());
        }
        out
    }

    /// Declaration as a `[tenant.<id>]` fragment, the form `ctl tenant add`
    /// writes and the SQL registry stores
    pub fn to_fragment(&self) -> toml::Table {
        let mut body = self.body.clone();
        body.insert("dbname".into(), self.dbname.clone().into());
        if self.desired == Desired::Detached {
            body.insert("state".into(), "detached".into());
        }
        if let Some(n) = self.max_lag_bytes {
            body.insert("max_lag_bytes".into(), (n as i64).into());
        }
        body.insert("initial_load".into(), self.initial_load.clone().into());
        let mut tenant = toml::Table::new();
        tenant.insert(self.id.clone(), toml::Value::Table(body));
        let mut root = toml::Table::new();
        root.insert("tenant".into(), toml::Value::Table(tenant));
        root
    }

    /// Changing any of these changes what the tenant's durable state is
    /// bound to, so it needs a detach and a fresh attach, never a reload
    pub fn identity_differs(&self, other: &Self, root: &toml::Table) -> Result<bool> {
        self.identity_differs_across(other, root, root)
    }

    /// [`identity_differs`](Self::identity_differs) with `self` read against
    /// `root` and `other` against `other_root`, as across a reload
    pub fn identity_differs_across(
        &self,
        other: &Self,
        root: &toml::Table,
        other_root: &toml::Table,
    ) -> Result<bool> {
        if self.dbname != other.dbname {
            return Ok(true);
        }
        let a =
            crate::destination::config::DestinationConfig::from_table(&self.effective_table(root))?;
        let b = crate::destination::config::DestinationConfig::from_table(
            &other.effective_table(other_root),
        )?;
        Ok(match (a.snowflake, b.snowflake) {
            (Some(a), Some(b)) => a.fingerprint() != b.fingerprint(),
            (None, None) => false,
            _ => true,
        })
    }
}

/// Lower-case letters, digits, `-` and `_`: used in paths, metric labels and
/// Snowflake object names
pub fn validate_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 63
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_'),
        "tenant id {id:?} must be 1-63 of [a-z0-9_-]"
    );
    Ok(())
}

/// Where a tenant's durable artifacts live. The legacy tenant keeps the
/// spill dir itself, so single-database deployments see no path change
pub fn tenant_dir(spill_dir: &Path, id: &str) -> PathBuf {
    if id == LEGACY_TENANT {
        spill_dir.to_path_buf()
    } else {
        spill_dir.join("tenants").join(id)
    }
}

/// Lifecycle a tenant has reached, persisted across restarts
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Routed from `attached_lsn`, waiting for every transaction older than
    /// the attachment to finish; nothing is delivered yet. A restart
    /// restarts priming
    Priming,
    /// Delivering commits from `start_lsn` on
    Active,
    /// Not routed, holding no WAL. Holds why in `reason`
    Detached,
}

/// `<tenant-dir>/tenant.toml`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantState {
    pub version: u32,
    pub id: String,
    pub dbname: String,
    /// Database OID the tenant's descriptor log is bound to. A database
    /// dropped and recreated under the same name gets a new OID, and is a
    /// different tenant
    pub db_oid: u32,
    pub phase: Phase,
    /// First record position routed to the tenant; its descriptor history
    /// starts here, so earlier records are never routed to it
    pub attached_lsn: u64,
    /// Commits before this drain as nothing: the initial load covers them
    pub start_lsn: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Tables in scope when the tenant started, with the initial load each
    /// was opted in with. Re-applied at every boot so an unfinished load
    /// resumes; the backfill ledger no-ops finished ones
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub start_tables: Vec<StartTable>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartTable {
    pub namespace: String,
    pub name: String,
    pub initial_load: String,
}

impl TenantState {
    pub fn new(id: &str, dbname: &str, db_oid: u32) -> Self {
        Self {
            version: STATE_VERSION,
            id: id.into(),
            dbname: dbname.into(),
            db_oid,
            phase: Phase::Priming,
            attached_lsn: 0,
            start_lsn: 0,
            reason: None,
            start_tables: Vec::new(),
        }
    }

    pub async fn load(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(STATE_FILE);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let state: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        ensure!(
            state.version == STATE_VERSION,
            "{} has unsupported version {}",
            path.display(),
            state.version
        );
        Ok(Some(state))
    }

    /// Write-temp, fsync, rename, fsync-dir: a crash leaves the old or the
    /// new state, never a torn one
    pub async fn store(&self, dir: &Path) -> Result<()> {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("create {}", dir.display()))?;
        let text = toml::to_string(self)?;
        let path = dir.join(STATE_FILE);
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        let dir = dir.to_path_buf();
        tokio::task::spawn_blocking(move || -> Result<()> {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(text.as_bytes())?;
            f.sync_all()?;
            std::fs::rename(&tmp, &path)?;
            std::fs::File::open(&dir)?.sync_all()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

/// Registry table the SQL registry reads and `ctl tenant` writes, in the
/// source's admin database (`[source] dbname`)
pub fn registry_ddl(schema: &str) -> String {
    let s = crate::pg::quote_ident(schema);
    format!(
        "CREATE SCHEMA IF NOT EXISTS {s};\n\
         CREATE TABLE IF NOT EXISTS {s}.tenant (\n\
         \x20   id text PRIMARY KEY CHECK (id ~ '^[a-z0-9_-]{{1,63}}$'),\n\
         \x20   dbname text NOT NULL UNIQUE,\n\
         \x20   -- the [tenant.<id>] body as TOML, without dbname\n\
         \x20   spec text NOT NULL DEFAULT '',\n\
         \x20   state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'detached')),\n\
         \x20   updated_at timestamptz NOT NULL DEFAULT now()\n\
         );\n"
    )
}

/// Declarations from registry rows `(id, dbname, spec, state)`
pub fn decls_from_rows(rows: &[(String, String, String, String)]) -> Result<Vec<TenantDecl>> {
    rows.iter()
        .map(|(id, dbname, spec, state)| {
            let mut body: toml::Table = toml::from_str(spec)
                .with_context(|| format!("registry tenant {id}: spec is not TOML"))?;
            body.insert("dbname".into(), dbname.clone().into());
            body.insert("state".into(), state.clone().into());
            TenantDecl::parse(id, &body)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(text: &str) -> toml::Table {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn no_tenant_tables_keeps_the_single_database_layout() {
        let r = root("[source]\ndbname = 'app'\n[ch]\nhost = 'h'\n");
        assert!(TenantsConfig::from_table(&r).unwrap().is_none());
        assert_eq!(tenant_dir(Path::new("/s"), LEGACY_TENANT), Path::new("/s"));
        assert_eq!(
            tenant_dir(Path::new("/s"), "acme"),
            Path::new("/s/tenants/acme")
        );
    }

    #[test]
    fn tenant_table_overlays_cluster_config_and_points_at_its_database() {
        let r = root(
            "[source]\nhost = 'pg'\ndbname = 'admin'\nslot = 's'\n\
             [memory]\nresident_payload_max = 1\n\
             [tenants]\nstall_timeout_secs = 5\n\
             [tenant.acme]\ndbname = 'acme_db'\nmax_lag_bytes = 100\n\
             [tenant.acme.stream]\nreplicate_all = false\n\
             [tenant.acme.table.public.orders]\nreplicate = true\n",
        );
        let cfg = TenantsConfig::from_table(&r).unwrap().unwrap();
        assert_eq!(cfg.stall_timeout, Duration::from_secs(5));
        let acme = cfg.get("acme").unwrap();
        assert_eq!(acme.dbname, "acme_db");
        assert_eq!(acme.max_lag_bytes, Some(100));
        assert_eq!(acme.initial_load, "copy");
        let eff = acme.effective_table(&r);
        assert!(!eff.contains_key("tenant") && !eff.contains_key("tenants"));
        assert_eq!(eff["source"]["dbname"].as_str(), Some("acme_db"));
        assert_eq!(eff["source"]["slot"].as_str(), Some("s"));
        assert_eq!(eff["memory"]["resident_payload_max"].as_integer(), Some(1));
        assert_eq!(eff["stream"]["replicate_all"].as_bool(), Some(false));
        let reparsed = TenantDecl::parse(
            "acme",
            &acme.to_fragment()["tenant"]["acme"]
                .as_table()
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(&reparsed, acme);
    }

    #[test]
    fn misplaced_and_conflicting_declarations_are_refused() {
        assert!(TenantsConfig::from_table(&root("[tenants]\n[ch]\nhost = 'h'\n")).is_err());
        assert!(
            TenantsConfig::from_table(&root(
                "[tenant.a]\ndbname = 'x'\n[tenant.b]\ndbname = 'x'\n"
            ))
            .is_err()
        );
        assert!(TenantsConfig::from_table(&root("[tenant.A]\ndbname = 'x'\n")).is_err());
        assert!(TenantsConfig::from_table(&root("[tenant.a]\n")).is_err());
        assert!(
            TenantsConfig::from_table(&root("[tenant.a]\ndbname = 'x'\nslot = 'y'\n")).is_err()
        );
        assert!(
            TenantsConfig::from_table(&root("[tenant.a]\ndbname = 'x'\nstate = 'paused'\n"))
                .is_err()
        );
    }

    #[test]
    fn registry_rows_parse_like_config_tables() {
        let decls = decls_from_rows(&[(
            "acme".into(),
            "acme_db".into(),
            "initial_load = 'none'\n[stream]\nreplicate_all = true\n".into(),
            "detached".into(),
        )])
        .unwrap();
        assert_eq!(decls[0].desired, Desired::Detached);
        assert_eq!(decls[0].initial_load, "none");
        assert!(registry_ddl("walshadow").contains("\"walshadow\".tenant"));
    }

    #[tokio::test]
    async fn state_round_trips_atomically() {
        let dir = tempfile::tempdir().unwrap();
        assert!(TenantState::load(dir.path()).await.unwrap().is_none());
        let mut s = TenantState::new("acme", "acme_db", 16500);
        s.phase = Phase::Active;
        s.attached_lsn = 10;
        s.start_lsn = 20;
        s.start_tables.push(StartTable {
            namespace: "public".into(),
            name: "orders".into(),
            initial_load: "copy".into(),
        });
        s.store(dir.path()).await.unwrap();
        assert_eq!(TenantState::load(dir.path()).await.unwrap(), Some(s));
    }
}
