//! Tenants: one followed source database each, sharing one physical slot,
//! WAL pump and shadow.
//!
//! A cluster holding many client databases would otherwise need one daemon,
//! slot and full-cluster shadow per client. Every tenant gets its own
//! descriptor history, transaction buffer, pipeline, destination and state
//! directory; the pump routes each record to the tenant whose database wrote
//! it (see [`TenantRouter`](crate::source::tenant_router::TenantRouter)).
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

/// ClickHouse `(host:port, database)` pairs, each owned by one tenant
pub type ChScopes = std::collections::BTreeSet<(String, String)>;

/// One ClickHouse scope: the endpoint `conn` names and `database` on it
pub fn ch_scope(conn: &crate::ch_emitter::InstanceConfig, database: &str) -> (String, String) {
    (
        format!("{}:{}", conn.host.to_ascii_lowercase(), conn.port),
        database.to_owned(),
    )
}

/// Every ClickHouse database a config can route to: each connection's
/// default, namespace and table overrides, and tees. Over-reserves where an
/// override could land on several connections, so two tenants never share
/// a table through any route (runtime `config_*` rows included, which the
/// DDL applicator holds to these scopes)
pub fn clickhouse_scopes(cfg: &crate::ch_emitter::EmitterConfig) -> ChScopes {
    let conn_of = |instance: Option<&str>| {
        instance
            .and_then(|name| cfg.instances.get(name))
            .unwrap_or(&cfg.conn)
    };
    let conns: Vec<_> = std::iter::once(&cfg.conn)
        .chain(cfg.instances.values())
        .collect();
    let mut scopes: ChScopes = conns.iter().map(|c| ch_scope(c, &c.database)).collect();
    for db in cfg
        .namespaces
        .values()
        .filter_map(|ns| ns.target_database.as_deref())
    {
        scopes.extend(conns.iter().map(|c| ch_scope(c, db)));
    }
    for (_, _, rule) in &cfg.table_entries {
        let conn = conn_of(rule.target_instance.as_deref());
        if let Some(db) = &rule.target_database {
            // A narrower rule may select another instance
            scopes.extend(conns.iter().map(|c| ch_scope(c, db)));
        }
        for tee in rule.tee.iter().flatten() {
            let tee_conn = tee.instance.as_deref().map_or(conn, |n| conn_of(Some(n)));
            if let Some(db) = &tee.database {
                scopes.insert(ch_scope(tee_conn, db));
            }
        }
    }
    scopes
}

/// Every further database is a tenant: `[database.<dbname>]` sections, which
/// followed several databases through one shared pipeline, destination and
/// ack, are rejected wherever a config is read (boot, reload, `ctl apply`)
pub fn reject_database_sections(root: &toml::Table) -> Result<()> {
    ensure!(
        !root.contains_key("database"),
        "`[database.<dbname>]` sections are not supported: declare a `[tenant.<id>]` \
         with its own `dbname` for each further database (docs/tenants.md)"
    );
    Ok(())
}

impl TenantsConfig {
    /// `None`: no `[tenants]` and no `[tenant.*]`, the single-database layout
    pub fn from_table(root: &toml::Table) -> Result<Option<Self>> {
        if !root.contains_key("tenants") && !root.contains_key("tenant") {
            return Ok(None);
        }
        for section in TENANT_SECTIONS {
            // Pause is cluster-wide: `ctl pause` writes a top-level
            // `[stream] paused`, the one stream key tenants leave to the
            // cluster
            if *section == "stream"
                && root
                    .get("stream")
                    .and_then(toml::Value::as_table)
                    .is_some_and(|t| t.keys().all(|k| k == "paused"))
            {
                continue;
            }
            ensure!(
                !root.contains_key(*section),
                "[{section}] belongs inside a [tenant.<id>] table once tenants are configured{}",
                if *section == "stream" {
                    "; only `paused` stays cluster-wide"
                } else {
                    ""
                }
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
        ensure!(
            id != LEGACY_TENANT,
            "tenant id {id:?} is reserved for the single-database layout"
        );
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

    /// The `[tenant.<id>]` table itself, `dbname` and all
    pub fn to_table(&self) -> toml::Table {
        self.to_fragment()
            .remove("tenant")
            .and_then(|t| match t {
                toml::Value::Table(mut t) => t.remove(&self.id),
                _ => None,
            })
            .and_then(|t| match t {
                toml::Value::Table(t) => Some(t),
                _ => None,
            })
            .unwrap_or_default()
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

impl TenantDecl {
    /// What durable state is bound to: the database, destination kind and
    /// Snowflake fingerprint, ie the inputs of
    /// [`identity_differs_across`](Self::identity_differs_across). Carries no
    /// credential
    pub fn identity_digest(&self, root: &toml::Table) -> Result<String> {
        let (kind, fingerprint, _) = self.destination_binding(root)?;
        Ok(digest(&serde_json::json!({
            "v": 1,
            "dbname": self.dbname,
            "destination": kind,
            "snowflake_fingerprint": fingerprint,
        })))
    }

    /// Everything a running tenant was built with that a reload cannot
    /// change in place: identity, ClickHouse scopes, Snowflake state dir and
    /// the settings fixed when the tenant opens. A recovery fence records
    /// the binding an accepted re-bind targets. Carries no credential
    pub fn binding_digest(&self, root: &toml::Table) -> Result<String> {
        let (kind, fingerprint, state_dir) = self.destination_binding(root)?;
        let eff = self.effective_table(root);
        let cfg = crate::ch_emitter::EmitterConfig::from_table(&eff)
            .map_err(|e| anyhow::anyhow!("tenant {}: {e}", self.id))?;
        let scopes: Vec<_> = if kind == "clickhouse" {
            clickhouse_scopes(&cfg).into_iter().collect()
        } else {
            Vec::new()
        };
        let cols = &cfg.system_columns;
        Ok(digest(&serde_json::json!({
            "v": 1,
            "dbname": self.dbname,
            "destination": kind,
            "snowflake_fingerprint": fingerprint,
            "snowflake_state_dir": state_dir,
            "clickhouse_scopes": scopes,
            "decoder_pool_size": cfg.decoder_pool_size,
            "inserter_pool_size": cfg.inserter_pool_size,
            "runtime_config_schema": cfg.runtime_config_schema,
            "replicate_all": cfg.replicate_all,
            "soft_delete": cfg.soft_delete,
            "system_columns": [&cols.lsn, &cols.xid, &cols.commit_ts, &cols.is_deleted, &cols.leaf],
            "toast_mode": format!("{:?}", cfg.toast.mode),
        })))
    }

    fn destination_binding(
        &self,
        root: &toml::Table,
    ) -> Result<(&'static str, Option<String>, Option<String>)> {
        let dest =
            crate::destination::config::DestinationConfig::from_table(&self.effective_table(root))
                .with_context(|| format!("tenant {}: destination", self.id))?;
        Ok(match dest.snowflake {
            Some(sf) => (
                "snowflake",
                Some(sf.fingerprint()),
                Some(sf.state.directory.display().to_string()),
            ),
            None => ("clickhouse", None, None),
        })
    }
}

fn digest(value: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}

/// `<tenant-dir>/rebind.toml`: an accepted re-bind of an attached tenant,
/// written before the config that triggers it. A boot that finds the
/// config already carrying `target_binding` attaches the tenant afresh
/// instead of resuming state bound to the old binding; otherwise the
/// change never landed and the fence is dropped. The fresh attach clears
/// the tenant directory, and with it the fence. While one is pending a
/// further re-bind of the same tenant is refused (R3-M1)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebindFence {
    pub version: u32,
    pub request_id: String,
    pub target_binding: String,
    /// Seconds since the Unix epoch
    pub created_at: u64,
}

pub const REBIND_FENCE: &str = "rebind.toml";
const FENCE_VERSION: u32 = 1;

impl RebindFence {
    pub fn new(request_id: &str, target_binding: String) -> Self {
        Self {
            version: FENCE_VERSION,
            request_id: request_id.to_string(),
            target_binding,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs()),
        }
    }

    pub async fn load(dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(REBIND_FENCE);
        let text = match tokio::fs::read_to_string(&path).await {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
        };
        let fence: Self =
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        ensure!(
            fence.version == FENCE_VERSION,
            "{} has unsupported version {}",
            path.display(),
            fence.version
        );
        Ok(Some(fence))
    }

    /// Durable before it returns: temp file at 0600, fsync, rename, dir fsync
    pub async fn store(&self, dir: &Path) -> Result<()> {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("create {}", dir.display()))?;
        crate::fs::write_private(dir, REBIND_FENCE, toml::to_string(self)?.as_bytes())
            .await
            .with_context(|| format!("write {}", dir.join(REBIND_FENCE).display()))
    }

    pub async fn remove(dir: &Path) -> Result<()> {
        match tokio::fs::remove_file(dir.join(REBIND_FENCE)).await {
            Ok(()) => crate::fs::fsync_dir(dir).await.map_err(Into::into),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => {
                Err(e).with_context(|| format!("remove {}", dir.join(REBIND_FENCE).display()))
            }
        }
    }
}

/// What a boot does with a tenant it finds active on disk
#[derive(Debug, PartialEq, Eq)]
pub enum BootResume {
    /// Resume from the persisted state
    Resume,
    /// Resume, after dropping a fence whose re-bind never landed
    DropFenceAndResume,
    /// Do not resume: attach afresh, with fresh initial loads
    Fresh(String),
}

/// Decide whether `state` may resume under `decl`, the boot's config. The
/// caller has already checked the phase and database name
pub fn boot_resume(
    state: &TenantState,
    decl: &TenantDecl,
    root: &toml::Table,
    fence: Option<&RebindFence>,
) -> Result<BootResume> {
    if let Some(identity) = &state.identity
        && *identity != decl.identity_digest(root)?
    {
        return Ok(BootResume::Fresh(
            "the database or destination identity changed while stopped".into(),
        ));
    }
    Ok(match fence {
        Some(fence) if fence.target_binding == decl.binding_digest(root)? => {
            BootResume::Fresh(format!(
                "re-bind {} was accepted before the restart",
                fence.request_id
            ))
        }
        Some(_) => BootResume::DropFenceAndResume,
        None => BootResume::Resume,
    })
}

/// Something only one tenant may write at a time
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Resource {
    /// ClickHouse `(host:port, database)`
    ClickHouse(String, String),
    /// Snowflake `(account url, database)`
    Snowflake(String, String),
    /// A Snowflake state directory
    StateDir(PathBuf),
    /// `<spill-dir>/tenants/<id>`
    TenantDir(PathBuf),
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ClickHouse(endpoint, db) => write!(f, "ClickHouse database {db} on {endpoint}"),
            Self::Snowflake(account, db) => write!(f, "Snowflake database {db} on {account}"),
            Self::StateDir(dir) => write!(f, "state directory {}", dir.display()),
            Self::TenantDir(dir) => write!(f, "tenant directory {}", dir.display()),
        }
    }
}

impl TenantDecl {
    /// What attaching this tenant writes exclusively
    pub fn resources(
        &self,
        root: &toml::Table,
        spill_dir: &Path,
    ) -> Result<std::collections::BTreeSet<Resource>> {
        let mut out = std::collections::BTreeSet::new();
        out.insert(Resource::TenantDir(tenant_dir(spill_dir, &self.id)));
        let eff = self.effective_table(root);
        let dest = crate::destination::config::DestinationConfig::from_table(&eff)
            .with_context(|| format!("tenant {}: destination", self.id))?;
        if let Some(sf) = dest.snowflake {
            out.insert(Resource::Snowflake(
                sf.account_url.to_ascii_lowercase(),
                sf.database.to_ascii_uppercase(),
            ));
            out.insert(Resource::StateDir(normalize(&sf.state.directory)));
        } else if eff.contains_key("ch") {
            let cfg = crate::ch_emitter::EmitterConfig::from_table(&eff)
                .map_err(|e| anyhow::anyhow!("tenant {}: {e}", self.id))?;
            out.extend(
                clickhouse_scopes(&cfg)
                    .into_iter()
                    .map(|(endpoint, db)| Resource::ClickHouse(endpoint, db)),
            );
        }
        Ok(out)
    }
}

/// Lexical normal form, so `/a/./b/` and `/a/b` are one directory
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

/// Directories nest: one inside another tenant's is that tenant's too
fn overlaps(a: &Resource, b: &Resource) -> bool {
    match (a, b) {
        (
            Resource::StateDir(x) | Resource::TenantDir(x),
            Resource::StateDir(y) | Resource::TenantDir(y),
        ) => x.starts_with(y) || y.starts_with(x),
        _ => a == b,
    }
}

#[derive(Debug)]
struct Holding {
    resources: std::collections::BTreeSet<Resource>,
    tearing_down: bool,
}

/// Exclusive resources of attached and tearing-down tenants. A tenant
/// keeps its claim through teardown until every one of its writers has
/// joined, so no other tenant, whatever its id, attaches to a destination
/// or directory something may still write. A leaf lock
#[derive(Debug, Default)]
pub struct Reservations {
    held: std::sync::Mutex<BTreeMap<String, Holding>>,
}

/// A resource another tenant holds
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub resource: Resource,
    pub holder: String,
    pub tearing_down: bool,
}

impl std::fmt::Display for Conflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is held by tenant {}{}",
            self.resource,
            self.holder,
            if self.tearing_down {
                " until its teardown joins every writer"
            } else {
                ""
            }
        )
    }
}

impl Reservations {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Holding>> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// What of `resources` a tenant other than `id` holds
    pub fn conflicts(
        &self,
        id: &str,
        resources: &std::collections::BTreeSet<Resource>,
    ) -> Vec<Conflict> {
        let held = self.lock();
        let mut out = Vec::new();
        for (holder, holding) in held.iter().filter(|(h, _)| h.as_str() != id) {
            for theirs in &holding.resources {
                if let Some(ours) = resources.iter().find(|r| overlaps(r, theirs)) {
                    out.push(Conflict {
                        resource: ours.clone(),
                        holder: holder.clone(),
                        tearing_down: holding.tearing_down,
                    });
                }
            }
        }
        out
    }

    /// All or nothing. A tenant still tearing down blocks its own id too:
    /// a re-attach must not race the old pipeline into the directory it
    /// clears
    pub fn claim(
        &self,
        id: &str,
        resources: std::collections::BTreeSet<Resource>,
    ) -> std::result::Result<(), Conflict> {
        let mut held = self.lock();
        if let Some(own) = held.get(id)
            && own.tearing_down
        {
            let resource = own
                .resources
                .iter()
                .find(|r| matches!(r, Resource::TenantDir(_)))
                .or_else(|| own.resources.iter().next())
                .cloned()
                .unwrap_or_else(|| Resource::TenantDir(PathBuf::from(id)));
            return Err(Conflict {
                resource,
                holder: id.to_string(),
                tearing_down: true,
            });
        }
        for (holder, holding) in held.iter().filter(|(h, _)| h.as_str() != id) {
            for theirs in &holding.resources {
                if let Some(ours) = resources.iter().find(|r| overlaps(r, theirs)) {
                    return Err(Conflict {
                        resource: ours.clone(),
                        holder: holder.clone(),
                        tearing_down: holding.tearing_down,
                    });
                }
            }
        }
        held.insert(
            id.to_string(),
            Holding {
                resources,
                tearing_down: false,
            },
        );
        Ok(())
    }

    /// The tenant's claim now outlives it until [`release`](Self::release)
    pub fn tearing_down(&self, id: &str) {
        if let Some(h) = self.lock().get_mut(id) {
            h.tearing_down = true;
        }
    }

    /// Every writer of `id` has joined
    pub fn release(&self, id: &str) {
        self.lock().remove(id);
    }

    pub fn is_tearing_down(&self, id: &str) -> bool {
        self.lock().get(id).is_some_and(|h| h.tearing_down)
    }
}

/// How a change to one tenant reaches a running daemon
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// Nothing changes
    None,
    /// Applies to the running tenant through its resolver, or takes effect
    /// at its next attach
    Live,
    /// A detached tenant becomes active: it attaches afresh, with fresh
    /// initial loads
    FreshAttach,
    /// Detach, then attach afresh with fresh initial loads: what the
    /// running tenant was built with changes
    Rebind {
        reasons: Vec<String>,
        /// The edit moves the Snowflake state directory here, which must be
        /// absent or empty: a fresh runtime cannot reopen state bound to
        /// another fingerprint
        fresh_state_dir: Option<PathBuf>,
    },
    /// Refused while the daemon runs
    Rejected(Vec<String>),
}

impl Change {
    pub fn label(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Live => "live",
            Self::FreshAttach => "fresh_attach",
            Self::Rebind { .. } => "rebind",
            Self::Rejected(_) => "rejected",
        }
    }

    /// Starts a fresh attachment of an existing tenant, so the caller must
    /// acknowledge the resync
    pub fn resyncs(&self) -> bool {
        matches!(self, Self::FreshAttach | Self::Rebind { .. })
    }
}

/// Class of one changed setting, by its dotted path inside `[tenant.<id>]`
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Rule {
    Live,
    /// Live unless the ClickHouse scope set moves, which the scope check
    /// catches
    Scoped,
    /// Snowflake fingerprint: re-binds only with a fresh state directory
    Fingerprint,
    StateDir,
    Rebind,
    Rejected,
}

fn rule_for(path: &[&str]) -> (Rule, &'static str) {
    use Rule::*;
    match path {
        ["dbname"] => (Rebind, "dbname"),
        ["max_lag_bytes"] | ["initial_load"] | ["state"] => (Live, ""),
        ["destination", ..] => (
            Rejected,
            "changing the destination kind needs a daemon restart and separate durable state",
        ),
        ["table", ..] | ["namespace", ..] => (Scoped, ""),
        ["ch", "host" | "port" | "database"] => (Scoped, ""),
        ["ch", "instances", _, "host" | "port" | "database"] | ["ch", "instances", _] => {
            (Scoped, "")
        }
        [
            "ch",
            "instances",
            _,
            "user" | "password" | "secure" | "tls_server_name" | "compression",
        ] => (Live, ""),
        [
            "ch",
            "user"
            | "password"
            | "secure"
            | "tls_server_name"
            | "compression"
            | "row_budget"
            | "byte_budget"
            | "drain_batch_rows"
            | "drain_batch_bytes"
            | "plan_disk_max"
            | "flush_timeout_ms"
            | "retry_max_attempts"
            | "retry_initial_backoff_ms"
            | "retry_max_backoff_ms"
            | "drop_table_strategy"
            | "detach_strategy",
        ] => (Live, ""),
        [
            "ch",
            "decoder_pool_size" | "inserter_pool_size" | "soft_delete",
        ] => (Rebind, "a setting fixed when the tenant opens"),
        ["ch", "decoder_batch_size" | "decoder_queue_capacity"] => (
            Rejected,
            "decoder batch and queue sizes are session-wide and need a daemon restart",
        ),
        [
            "snowflake",
            "role"
            | "warehouse"
            | "user"
            | "statement_timeout_secs"
            | "batch_rows"
            | "batch_bytes"
            | "flush_interval_ms",
        ]
        | ["snowflake", "auth", ..] => (Live, ""),
        ["snowflake", "account_url" | "database" | "internal_schema"]
        | ["snowflake", "stage", ..]
        | ["snowflake", "schema_mapping", ..] => (Fingerprint, ""),
        ["snowflake", "state", "directory"] => (StateDir, ""),
        [
            "snowflake",
            "channels_per_table"
            | "max_in_flight"
            | "merge_interval_ms"
            | "metadata_concurrency"
            | "merge_concurrency"
            | "ack_after",
        ]
        | ["snowflake", "state", "max_bytes"] => (
            Rejected,
            "Snowflake channel, pool, merge-interval, state and ack_after settings need a daemon restart",
        ),
        [
            "stream",
            "skip_attribution" | "auto_create_name_all" | "paused",
        ] => (Live, ""),
        ["stream", "replicate_all"] => (Rebind, "replicate_all is fixed when the tenant opens"),
        ["runtime_config", "schema"] => (
            Rebind,
            "the runtime-config schema is fixed when the tenant opens",
        ),
        ["system_columns", ..] => (
            Rebind,
            "system column names are fixed when the tenant opens",
        ),
        ["toast", "mode"] => (Rebind, "the TOAST mode is fixed when the tenant opens"),
        _ => (Rejected, ""),
    }
}

/// Dotted paths of every leaf that differs between `a` and `b`
fn changed_paths(a: &toml::Table, b: &toml::Table) -> Vec<Vec<String>> {
    fn table<'v>(v: Option<&'v toml::Value>, empty: &'v toml::Table) -> Option<&'v toml::Table> {
        match v {
            Some(toml::Value::Table(t)) => Some(t),
            None => Some(empty),
            Some(_) => None,
        }
    }
    fn walk(
        prefix: &mut Vec<String>,
        a: Option<&toml::Value>,
        b: Option<&toml::Value>,
        out: &mut Vec<Vec<String>>,
    ) {
        // A table added or removed whole changes each of its leaves
        let empty = toml::Table::new();
        match (table(a, &empty), table(b, &empty)) {
            (Some(x), Some(y))
                if a.is_some_and(toml::Value::is_table) || b.is_some_and(toml::Value::is_table) =>
            {
                let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
                for k in keys {
                    prefix.push(k.clone());
                    walk(prefix, x.get(k), y.get(k), out);
                    prefix.pop();
                }
            }
            _ if a == b => {}
            _ => out.push(prefix.clone()),
        }
    }
    let mut out = Vec::new();
    let (a, b) = (toml::Value::Table(a.clone()), toml::Value::Table(b.clone()));
    walk(&mut Vec::new(), Some(&a), Some(&b), &mut out);
    out
}

/// Classify replacing tenant `old` with `new`, the whole config going from
/// `old_root` to `new_root`. Fails closed: a changed setting no rule names
/// is refused. `None` for `old` is a new tenant
pub fn classify_change(
    old: Option<&TenantDecl>,
    new: &TenantDecl,
    old_root: &toml::Table,
    new_root: &toml::Table,
) -> Result<Change> {
    let Some(old) = old else {
        return Ok(Change::Live);
    };
    let (a, b) = (old.to_fragment(), new.to_fragment());
    let body = |t: &toml::Table, id: &str| -> toml::Table {
        t.get("tenant")
            .and_then(|t| t.get(id))
            .and_then(toml::Value::as_table)
            .cloned()
            .unwrap_or_default()
    };
    let (a, b) = (body(&a, &old.id), body(&b, &new.id));
    let paths = changed_paths(&a, &b);
    let mut rejected = Vec::new();
    let mut rebind = Vec::new();
    let mut fingerprint = false;
    let mut state_dir = false;
    for path in &paths {
        let parts: Vec<&str> = path.iter().map(String::as_str).collect();
        let (rule, why) = rule_for(&parts);
        let dotted = path.join(".");
        match rule {
            Rule::Live | Rule::Scoped => {}
            Rule::Fingerprint => fingerprint = true,
            Rule::StateDir => state_dir = true,
            Rule::Rebind => rebind.push(if why.is_empty() {
                dotted
            } else {
                format!("{dotted}: {why}")
            }),
            Rule::Rejected if why.is_empty() => rejected.push(format!(
                "{dotted}: unclassified setting, refused while the daemon runs"
            )),
            Rule::Rejected => rejected.push(format!("{dotted}: {why}")),
        }
    }
    let mut fresh_state_dir = None;
    if fingerprint {
        let dir = crate::destination::config::DestinationConfig::from_table(
            &new.effective_table(new_root),
        )
        .ok()
        .and_then(|d| d.snowflake)
        .map(|sf| sf.state.directory);
        match (state_dir, dir) {
            (true, Some(dir)) => {
                rebind.push(
                    "Snowflake account, database, internal schema, stage or schema mapping, with a fresh state directory"
                        .into(),
                );
                fresh_state_dir = Some(dir);
            }
            _ => rejected.push(
                "the Snowflake account, database, internal schema, stage or schema mapping is bound to its \
                 state directory: move snowflake.state.directory to a fresh, empty path in the same edit"
                    .into(),
            ),
        }
    } else if state_dir {
        rejected.push("snowflake.state.directory alone needs a daemon restart".into());
    }
    // A ClickHouse route the tenant reaches differently re-binds: its
    // reservations are fixed at attach
    let scopes = |d: &TenantDecl, root: &toml::Table| -> Option<ChScopes> {
        let eff = d.effective_table(root);
        eff.contains_key("ch")
            .then(|| crate::ch_emitter::EmitterConfig::from_table(&eff).ok())
            .flatten()
            .map(|cfg| clickhouse_scopes(&cfg))
    };
    if scopes(old, old_root) != scopes(new, new_root) {
        rebind.push("the ClickHouse databases the tenant reaches".into());
    }
    if !rejected.is_empty() {
        return Ok(Change::Rejected(rejected));
    }
    let was_active = old.desired == Desired::Active;
    let is_active = new.desired == Desired::Active;
    Ok(match (was_active, is_active) {
        (true, true) if !rebind.is_empty() => Change::Rebind {
            reasons: rebind,
            fresh_state_dir,
        },
        (false, true) => Change::FreshAttach,
        _ if paths.is_empty() => Change::None,
        _ => Change::Live,
    })
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
    /// [`TenantDecl::identity_digest`] at attach. A boot whose config has
    /// another identity attaches afresh rather than resume state bound to
    /// the old one. Absent in state written before it existed, which boot
    /// accepts and fills in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    /// Random id of this attachment, new at every attach: a confirmation
    /// issued for one attachment cannot act on the next
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<String>,
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
            identity: None,
            incarnation: None,
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

    fn ch_tenant(body: &str) -> toml::Table {
        root(&format!(
            "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n\
             [tenant.a]\ndbname = 'app'\n[tenant.a.ch]\nhost = 'ch'\n{body}"
        ))
    }

    fn decl_a(r: &toml::Table) -> TenantDecl {
        TenantsConfig::from_table(r)
            .unwrap()
            .unwrap()
            .get("a")
            .unwrap()
            .clone()
    }

    #[test]
    fn digests_carry_no_credentials_and_track_their_inputs() {
        let base = ch_tenant("database = 'a'\npassword = 'one'\n");
        let (identity, binding) = {
            let d = decl_a(&base);
            (
                d.identity_digest(&base).unwrap(),
                d.binding_digest(&base).unwrap(),
            )
        };
        // A rotated password changes neither
        let rotated = ch_tenant("database = 'a'\npassword = 'two'\n");
        let d = decl_a(&rotated);
        assert_eq!(d.identity_digest(&rotated).unwrap(), identity);
        assert_eq!(d.binding_digest(&rotated).unwrap(), binding);
        assert!(!binding.contains("one") && binding.len() == 64);
        // Another ClickHouse database re-binds, but is the same identity
        let moved = ch_tenant("database = 'b'\n");
        let d = decl_a(&moved);
        assert_eq!(d.identity_digest(&moved).unwrap(), identity);
        assert_ne!(d.binding_digest(&moved).unwrap(), binding);
        // So does a setting fixed when the tenant opens
        let pools = ch_tenant("database = 'a'\ninserter_pool_size = 4\n");
        assert_ne!(decl_a(&pools).binding_digest(&pools).unwrap(), binding);
        // Another source database is another identity
        let other = root(
            "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n\
             [tenant.a]\ndbname = 'other'\n[tenant.a.ch]\nhost = 'ch'\ndatabase = 'a'\n",
        );
        assert_ne!(decl_a(&other).identity_digest(&other).unwrap(), identity);
    }

    /// One row per class of the capability matrix (plan §9.4), and the
    /// fail-closed fallback
    #[test]
    fn classify_change_follows_the_capability_matrix() {
        let tenant = |top: &str, ch: &str, rest: &str| -> toml::Table {
            root(&format!(
                "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n\
                 [tenant.a]\ndbname = 'app'\n{top}[tenant.a.ch]\nhost = 'ch'\ndatabase = 'a'\n{ch}{rest}"
            ))
        };
        let base = tenant("", "", "");
        let class = |next: &toml::Table| {
            classify_change(Some(&decl_a(&base)), &decl_a(next), &base, next)
                .unwrap()
                .label()
        };
        let cases = [
            (tenant("", "", ""), "none"),
            (tenant("max_lag_bytes = 9\n", "", ""), "live"),
            (tenant("initial_load = 'none'\n", "", ""), "live"),
            (
                tenant("", "password = 'x'\nretry_max_attempts = 3\n", ""),
                "live",
            ),
            (
                tenant("", "", "[tenant.a.table.public.t]\nreplicate = true\n"),
                "live",
            ),
            (
                tenant("", "", "[tenant.a.stream]\nskip_attribution = true\n"),
                "live",
            ),
            (tenant("", "inserter_pool_size = 4\n", ""), "rebind"),
            (tenant("", "soft_delete = true\n", ""), "rebind"),
            (
                tenant("", "", "[tenant.a.stream]\nreplicate_all = true\n"),
                "rebind",
            ),
            (
                tenant("", "", "[tenant.a.runtime_config]\nschema = 'cfg'\n"),
                "rebind",
            ),
            (
                tenant(
                    "",
                    "",
                    "[tenant.a.namespace.public]\ntarget_database = 'elsewhere'\n",
                ),
                "rebind",
            ),
            (
                root(
                    "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n\
                     [tenant.a]\ndbname = 'app'\n[tenant.a.ch]\nhost = 'ch2'\ndatabase = 'a'\n",
                ),
                "rebind",
            ),
            (tenant("", "decoder_queue_capacity = 10\n", ""), "rejected"),
            (tenant("", "made_up = 1\n", ""), "rejected"),
            (
                tenant("", "", "[tenant.a.toast]\nconnections = 2\n"),
                "rejected",
            ),
            (
                tenant("", "", "[tenant.a.destination]\nkind = 'clickhouse'\n"),
                "rejected",
            ),
        ];
        for (next, want) in &cases {
            assert_eq!(class(next), *want, "{}", toml::to_string(next).unwrap());
        }
        // A dbname change is an identity re-bind
        let moved = root(
            "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n\
             [tenant.a]\ndbname = 'app2'\n[tenant.a.ch]\nhost = 'ch'\ndatabase = 'a'\n",
        );
        assert_eq!(class(&moved), "rebind");
        // Detached to active attaches afresh; active to detached is live
        let detached = tenant("state = 'detached'\n", "", "");
        assert_eq!(class(&detached), "live");
        let back =
            classify_change(Some(&decl_a(&detached)), &decl_a(&base), &detached, &base).unwrap();
        assert_eq!(back, Change::FreshAttach);
        assert!(back.resyncs());
        // A re-bind of a tenant staying detached waits for its next attach
        let both = tenant("state = 'detached'\n", "inserter_pool_size = 4\n", "");
        assert_eq!(
            classify_change(Some(&decl_a(&detached)), &decl_a(&both), &detached, &both)
                .unwrap()
                .label(),
            "live"
        );
        // A new tenant
        assert_eq!(
            classify_change(None, &decl_a(&base), &base, &base).unwrap(),
            Change::Live
        );
    }

    #[test]
    fn capability_matrix_covers_snowflake_and_instance_settings() {
        let sf: toml::Table = r#"
[source]
dbname = 'postgres'
[tenants]
[tenant.a]
dbname = 'app'
[tenant.a.destination]
kind = 'snowflake'
[tenant.a.snowflake]
account_url = 'https://acme-test.snowflakecomputing.com'
user = 'REPLICATOR'
role = 'REPLICATION'
warehouse = 'INGEST'
database = 'DB'
[tenant.a.snowflake.auth]
method = 'oauth'
token_file = '/run/secrets/token'
[tenant.a.snowflake.state]
directory = '/state/a'
max_bytes = 10737418240
[tenant.a.snowflake.stage]
bucket = 'replication'
prefix = 'a/'
region = 'us-east-1'
name = 'DB.INTERNAL.LOAD_STAGE'
"#
        .parse()
        .unwrap();
        let old = decl_a(&sf);
        for key in [
            "channels_per_table",
            "max_in_flight",
            "merge_interval_ms",
            "metadata_concurrency",
            "merge_concurrency",
            "ack_after",
            "max_bytes",
            "directory",
        ] {
            let mut next = sf.clone();
            let tbl = if matches!(key, "max_bytes" | "directory") {
                next["tenant"]["a"]["snowflake"]["state"]
                    .as_table_mut()
                    .unwrap()
            } else {
                next["tenant"]["a"]["snowflake"].as_table_mut().unwrap()
            };
            tbl.insert(
                key.into(),
                match key {
                    "ack_after" => "outbox".into(),
                    "directory" => "/state/new".into(),
                    "max_bytes" => 20_000_000_000_i64.into(),
                    _ => 17_i64.into(),
                },
            );
            let change = classify_change(Some(&old), &decl_a(&next), &sf, &next).unwrap();
            assert!(matches!(change, Change::Rejected(_)), "{key}: {change:?}");
        }
        let resources = old.resources(&sf, Path::new("/spill")).unwrap();
        assert!(resources.contains(&Resource::Snowflake(
            "https://acme-test.snowflakecomputing.com".into(),
            "DB".into()
        )));
        assert!(resources.contains(&Resource::StateDir("/state/a".into())));
        let r = Reservations::default();
        r.claim("a", resources.clone()).unwrap();
        assert!(!r.is_tearing_down("a"));
        let conflict = r.claim("b", resources).unwrap_err();
        assert!(conflict.to_string().contains("held by tenant a"));
        r.tearing_down("a");
        assert!(r.is_tearing_down("a"));
        r.release("a");
        assert!(!r.is_tearing_down("a"));
        for (resource, want) in [
            (
                Resource::ClickHouse("ch:9000".into(), "a".into()),
                "ClickHouse database a on ch:9000",
            ),
            (
                Resource::Snowflake("sf".into(), "DB".into()),
                "Snowflake database DB on sf",
            ),
            (
                Resource::StateDir("/state/a".into()),
                "state directory /state/a",
            ),
            (
                Resource::TenantDir("/spill/tenants/a".into()),
                "tenant directory /spill/tenants/a",
            ),
        ] {
            assert_eq!(resource.to_string(), want);
        }
        assert_eq!(
            normalize(Path::new("/state/old/.././a")),
            PathBuf::from("/state/a")
        );
        let base = ch_tenant(
            "database = 'a'\n[tenant.a.ch.instances.mirror]\nhost = 'mirror'\ndatabase = 'm'\n",
        );
        for (key, value) in [("user", "reader"), ("password", "pw")] {
            let mut next = base.clone();
            next["tenant"]["a"]["ch"]["instances"]["mirror"]
                .as_table_mut()
                .unwrap()
                .insert(key.into(), value.into());
            assert_eq!(
                classify_change(Some(&decl_a(&base)), &decl_a(&next), &base, &next).unwrap(),
                Change::Live
            );
        }
        let mut next = base.clone();
        next["tenant"]["a"].as_table_mut().unwrap().insert(
            "system_columns".into(),
            toml::Value::Table(root("lsn = '_position'")),
        );
        assert!(matches!(
            classify_change(Some(&decl_a(&base)), &decl_a(&next), &base, &next).unwrap(),
            Change::Rebind { .. }
        ));
    }

    #[tokio::test]
    async fn unreadable_and_unknown_fences_are_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(REBIND_FENCE);
        std::fs::create_dir(&path).unwrap();
        assert!(
            RebindFence::load(dir.path())
                .await
                .unwrap_err()
                .to_string()
                .contains("read")
        );
        assert!(
            RebindFence::remove(dir.path())
                .await
                .unwrap_err()
                .to_string()
                .contains("remove")
        );
        std::fs::remove_dir(&path).unwrap();
        let mut fence = RebindFence::new("r", "binding".into());
        fence.version = 99;
        std::fs::write(&path, toml::to_string(&fence).unwrap()).unwrap();
        assert!(
            RebindFence::load(dir.path())
                .await
                .unwrap_err()
                .to_string()
                .contains("unsupported version 99")
        );
    }

    #[tokio::test]
    async fn fence_round_trips_privately_and_removes() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let tdir = dir.path().join("tenants/a");
        assert!(RebindFence::load(&tdir).await.unwrap().is_none());
        let fence = RebindFence::new("req-1", "b".repeat(64));
        fence.store(&tdir).await.unwrap();
        assert_eq!(RebindFence::load(&tdir).await.unwrap(), Some(fence));
        let mode = std::fs::metadata(tdir.join(REBIND_FENCE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        RebindFence::remove(&tdir).await.unwrap();
        RebindFence::remove(&tdir).await.unwrap();
        assert!(RebindFence::load(&tdir).await.unwrap().is_none());
    }

    #[test]
    fn boot_resumes_only_state_bound_to_the_boot_config() {
        let a = ch_tenant("database = 'a'\n");
        let b = ch_tenant("database = 'b'\n");
        let decl = decl_a(&b);
        let mut state = TenantState::new("a", "app", 7);
        state.phase = Phase::Active;
        // Pre-identity state is accepted
        assert_eq!(
            boot_resume(&state, &decl, &b, None).unwrap(),
            BootResume::Resume
        );
        state.identity = Some(decl.identity_digest(&b).unwrap());
        assert_eq!(
            boot_resume(&state, &decl, &b, None).unwrap(),
            BootResume::Resume
        );
        // An accepted re-bind to b that landed: attach afresh against b
        let landed = RebindFence::new("req-b", decl.binding_digest(&b).unwrap());
        assert!(matches!(
            boot_resume(&state, &decl, &b, Some(&landed)).unwrap(),
            BootResume::Fresh(why) if why.contains("req-b")
        ));
        // A fence whose change never landed: the config is still a's
        let decl_on_a = decl_a(&a);
        assert_eq!(
            boot_resume(&state, &decl_on_a, &a, Some(&landed)).unwrap(),
            BootResume::DropFenceAndResume
        );
        // Identity changed while stopped
        state.identity = Some("0".repeat(64));
        assert!(matches!(
            boot_resume(&state, &decl, &b, None).unwrap(),
            BootResume::Fresh(_)
        ));
    }

    #[test]
    fn reservations_hold_through_teardown_across_ids() {
        let spill = Path::new("/spill");
        let a = ch_tenant("database = 'a'\n");
        let resources = decl_a(&a).resources(&a, spill).unwrap();
        assert!(resources.contains(&Resource::ClickHouse("ch:9000".into(), "a".into())));
        let r = Reservations::default();
        r.claim("a", resources.clone()).unwrap();
        // Tenant b wants a's database
        let mut theirs = std::collections::BTreeSet::new();
        theirs.insert(Resource::ClickHouse("ch:9000".into(), "a".into()));
        theirs.insert(Resource::TenantDir(spill.join("tenants/b")));
        let err = r.claim("b", theirs.clone()).unwrap_err();
        assert_eq!(err.holder, "a");
        assert!(!err.tearing_down);
        // a tears down: still held, by a, and a may not re-attach either
        r.tearing_down("a");
        assert!(r.claim("b", theirs.clone()).unwrap_err().tearing_down);
        assert!(r.claim("a", resources.clone()).is_err());
        assert_eq!(r.conflicts("b", &theirs).len(), 1);
        // Writers joined
        r.release("a");
        assert!(r.conflicts("b", &theirs).is_empty());
        r.claim("b", theirs).unwrap();
    }

    #[test]
    fn nested_directories_conflict() {
        let r = Reservations::default();
        let mut outer = std::collections::BTreeSet::new();
        outer.insert(Resource::StateDir(PathBuf::from("/var/lib/sf/a")));
        r.claim("a", outer).unwrap();
        let mut inner = std::collections::BTreeSet::new();
        inner.insert(Resource::StateDir(normalize(Path::new(
            "/var/lib/sf/./a/nested",
        ))));
        assert!(r.claim("b", inner).is_err());
        let mut sibling = std::collections::BTreeSet::new();
        sibling.insert(Resource::StateDir(PathBuf::from("/var/lib/sf/b")));
        r.claim("b", sibling).unwrap();
    }

    #[test]
    fn older_state_without_identity_or_incarnation_still_loads() {
        let text = "version = 1\nid = 'a'\ndbname = 'app'\ndb_oid = 7\nphase = 'active'\n\
                    attached_lsn = 1\nstart_lsn = 2\n";
        let state: TenantState = toml::from_str(text).unwrap();
        assert!(state.identity.is_none() && state.incarnation.is_none());
        let written = toml::to_string(&state).unwrap();
        assert!(!written.contains("identity") && !written.contains("incarnation"));
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
        let err = TenantsConfig::from_table(&root("[tenant.default]\ndbname = 'x'\n"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("reserved for the single-database layout"),
            "{err}"
        );
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
