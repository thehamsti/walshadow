//! One serialized reload pipeline for every config change
//!
//! The control socket, SIGHUP, the SQL registry poller, resolver wiring and
//! the HTTP API all reload through [`Reloader`]. Each reload holds the
//! reload lock ([`RELOAD`](crate::ops::lock_rank::RELOAD)), reads the merged
//! config once, applies it to every resolver and only then publishes an
//! immutable [`ConfigRevision`]. The session reconciles exactly the
//! revisions published, never re-reading the files, so a candidate a caller
//! rejects and rolls back never reaches replication.
//!
//! A writer that persists config holds the lock across the write too
//! ([`Reloader::begin`]), so no other reload can publish its candidate first.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::anyhow;
use tokio::sync::watch;
use toml::Table;

use crate::config::ConfigResolver;
use crate::ops::lock_rank::{RELOAD, RankedGuard, RankedMutex};

/// What prompted a reload, carried on the revision it publishes
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadCause {
    /// The revision a session boots from
    Boot,
    /// SIGHUP after the operator edited config files
    Sighup,
    /// Rows changed in the SQL tenant registry
    SqlRegistry,
    /// A control-socket verb (`reload`, `apply`, `unset`, `tenant-*`)
    Control,
    /// Resolvers wired after the session opened them
    Sweep,
    /// A control-plane mutation (HTTP API or socket tenant verb)
    Mutation {
        request_id: String,
        tenant: Option<String>,
    },
}

impl ReloadCause {
    /// Changes nobody audited at the source: hand edits and SQL rows
    pub fn is_external(&self) -> bool {
        matches!(self, Self::Sighup | Self::SqlRegistry)
    }
}

/// One accepted config, as every resolver applied it
#[derive(Debug)]
pub struct ConfigRevision {
    /// Process id of this revision stream; generations restart per epoch
    pub epoch: Arc<str>,
    /// Monotonic within an epoch. A reload that applied but did not publish
    /// still consumes one, so published generations may skip
    pub generation: u64,
    pub merged: Arc<Table>,
    /// Tenants this revision re-binds whatever `identity_differs_across`
    /// says: detached, then attached afresh
    pub force_rebind: BTreeSet<String>,
    pub cause: ReloadCause,
}

/// Options a mutation passes to [`ReloadTx::publish`]
#[derive(Debug, Default)]
pub struct PublishOpts {
    pub force_rebind: BTreeSet<String>,
    /// A tenant whose refusal aborts the reload: nothing publishes, and the
    /// caller, still holding the lock, rolls its write back
    pub abort_on: Option<String>,
}

/// What a published reload did
#[derive(Debug)]
pub struct Published {
    pub revision: Arc<ConfigRevision>,
    /// Databases the config names that this process does not follow
    pub unfollowed: Vec<String>,
    /// Tenants whose resolver refused this revision and kept their last one
    pub tenant_errors: Vec<(String, String)>,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The merged config does not parse or validate; nothing applied
    #[error("{0:#}")]
    Invalid(anyhow::Error),
    /// The tenant named by [`PublishOpts::abort_on`] refused it; nothing
    /// published
    #[error("tenant {tenant}: {error}")]
    TargetRefused { tenant: String, error: String },
}

#[derive(Default)]
struct TenantSlot {
    resolver: Option<Arc<ConfigResolver>>,
    /// Generation of the revision re-binding this tenant: its resolver is
    /// skipped until a replacement registers
    rebinding_since: Option<u64>,
}

struct State {
    generation: u64,
}

pub struct Reloader {
    lock: RankedMutex<State>,
    /// Config file and CLI base; `None` borrows the first resolver's
    source: Mutex<Option<(PathBuf, Table)>>,
    /// Cluster resolvers, or one per database in the single-database layout
    resolvers: Mutex<Vec<Arc<ConfigResolver>>>,
    tenants: Mutex<BTreeMap<String, TenantSlot>>,
    epoch: Arc<str>,
    tx: watch::Sender<Arc<ConfigRevision>>,
    /// Exclusive resources of attached and tearing-down tenants, shared by
    /// the session and the control plane across session restarts
    reservations: Arc<crate::tenants::Reservations>,
}

impl Default for Reloader {
    fn default() -> Self {
        Self::new(None, Table::new())
    }
}

fn leaf<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Reloader {
    pub fn new(config: Option<PathBuf>, cli_base: Table) -> Self {
        let epoch: Arc<str> = uuid::Uuid::new_v4().simple().to_string().into();
        let (tx, _) = watch::channel(Arc::new(ConfigRevision {
            epoch: epoch.clone(),
            generation: 0,
            merged: Arc::new(Table::new()),
            force_rebind: BTreeSet::new(),
            cause: ReloadCause::Boot,
        }));
        Self {
            lock: RankedMutex::new(RELOAD, "reload", State { generation: 0 }),
            source: Mutex::new(config.map(|p| (p, cli_base))),
            resolvers: Mutex::new(Vec::new()),
            tenants: Mutex::new(BTreeMap::new()),
            epoch,
            tx,
            reservations: Arc::default(),
        }
    }

    pub fn reservations(&self) -> &Arc<crate::tenants::Reservations> {
        &self.reservations
    }

    pub fn epoch(&self) -> &Arc<str> {
        &self.epoch
    }

    /// Revisions as they publish; the session reconciles each
    pub fn subscribe(&self) -> watch::Receiver<Arc<ConfigRevision>> {
        self.tx.subscribe()
    }

    /// Last published revision
    pub fn current(&self) -> Arc<ConfigRevision> {
        self.tx.borrow().clone()
    }

    /// Seed the revision a session boots from, without applying it: the
    /// session built its resolvers from this config
    pub async fn seed(&self, merged: Table) -> Arc<ConfigRevision> {
        let mut state = self.lock.lock().await;
        state.generation += 1;
        let rev = Arc::new(ConfigRevision {
            epoch: self.epoch.clone(),
            generation: state.generation,
            merged: Arc::new(merged),
            force_rebind: BTreeSet::new(),
            cause: ReloadCause::Boot,
        });
        self.tx.send_replace(rev.clone());
        rev
    }

    pub async fn set_resolvers(&self, r: Vec<Arc<ConfigResolver>>) {
        *leaf(&self.resolvers) = r;
        // Control socket serves before the session wires a resolver;
        // apply/reload in that window persist fragments but republish
        // nothing. Sweep once at wiring so file state and published config
        // converge
        if let Err(e) = self.reload_with(ReloadCause::Sweep).await {
            tracing::warn!(
                target: "walshadow::control",
                error = %format!("{e:#}"),
                "config sweep at resolver wiring failed",
            );
        }
    }

    /// Register or drop a tenant's resolver. Registering a replacement ends
    /// any re-binding skip; dropping keeps no resolver to skip
    pub async fn set_tenant_resolver(&self, id: &str, r: Option<Arc<ConfigResolver>>) {
        let mut tenants = leaf(&self.tenants);
        match r {
            Some(r) => {
                let slot = tenants.entry(id.to_string()).or_default();
                slot.resolver = Some(r);
                slot.rebinding_since = None;
            }
            None => {
                tenants.remove(id);
            }
        }
    }

    /// Whether `id`'s registered resolver is being replaced
    pub fn is_rebinding(&self, id: &str) -> bool {
        leaf(&self.tenants)
            .get(id)
            .is_some_and(|s| s.rebinding_since.is_some())
    }

    /// Take the reload lock. Hold it from a config write until the revision
    /// publishes, or the write is rolled back
    pub async fn begin(&self) -> ReloadTx<'_> {
        ReloadTx {
            state: self.lock.lock().await,
            reloader: self,
        }
    }

    /// Live reconfigure, as the control socket's `reload` does. Returns
    /// databases the config names that this process does not follow; a
    /// tenant whose reload fails keeps its last snapshot, and the first
    /// error is reported after the revision published
    pub async fn reload(&self) -> anyhow::Result<Vec<String>> {
        self.reload_with(ReloadCause::Control).await
    }

    pub async fn reload_with(&self, cause: ReloadCause) -> anyhow::Result<Vec<String>> {
        let published = self
            .begin()
            .await
            .publish(cause, PublishOpts::default())
            .await
            .map_err(|e| anyhow!("reload: {e}"))?;
        if let Some((id, e)) = published.tenant_errors.first() {
            anyhow::bail!("reload tenant {id}: {e}");
        }
        Ok(published.unfollowed)
    }

    fn config_source(&self) -> Option<(PathBuf, Table)> {
        if let Some(source) = leaf(&self.source).clone() {
            return Some(source);
        }
        leaf(&self.resolvers).iter().find_map(|r| r.config_source())
    }
}

/// The reload lock, held. Proof for writers that config and publication
/// cannot interleave with another reload
pub struct ReloadTx<'a> {
    state: RankedGuard<'a, State>,
    reloader: &'a Reloader,
}

impl ReloadTx<'_> {
    /// Last published revision
    pub fn current(&self) -> Arc<ConfigRevision> {
        self.reloader.current()
    }

    /// Read the merged config once, apply it to every resolver, publish.
    /// With [`PublishOpts::abort_on`] that tenant applies first, and its
    /// refusal returns before any other resolver sees the candidate
    pub async fn publish(
        &mut self,
        cause: ReloadCause,
        opts: PublishOpts,
    ) -> Result<Published, PublishError> {
        let reloader = self.reloader;
        let merged = match reloader.config_source() {
            Some((path, cli_base)) => crate::ch_emitter::load_effective(&path, cli_base)
                .await
                .map_err(|e| PublishError::Invalid(anyhow!("{e}")))?,
            None => Table::new(),
        };
        crate::tenants::reject_database_sections(&merged).map_err(PublishError::Invalid)?;
        let tenants_cfg = if merged.contains_key("tenants") || merged.contains_key("tenant") {
            crate::ops::tenant_ctl::validate(&merged).map_err(PublishError::Invalid)?;
            crate::tenants::TenantsConfig::from_table(&merged).map_err(PublishError::Invalid)?
        } else {
            None
        };
        let merged = Arc::new(merged);
        let generation = self.state.generation + 1;
        // Tenants whose durable state binding changes skip their old
        // resolver, which would refuse the new settings, until re-attached
        let prev = reloader.current();
        let mut rebinding = opts.force_rebind.clone();
        if let (Some(next), Ok(Some(old))) = (
            &tenants_cfg,
            crate::tenants::TenantsConfig::from_table(&prev.merged),
        ) {
            for decl in &next.decls {
                if let Some(was) = old.get(&decl.id)
                    && was
                        .identity_differs_across(decl, &prev.merged, &merged)
                        .unwrap_or(true)
                {
                    rebinding.insert(decl.id.clone());
                }
            }
        }
        let skipped =
            |id: &str, slot: &TenantSlot| rebinding.contains(id) || slot.rebinding_since.is_some();
        let slots: Vec<(String, Arc<ConfigResolver>, bool)> = leaf(&reloader.tenants)
            .iter()
            .filter_map(|(id, slot)| {
                slot.resolver
                    .clone()
                    .map(|r| (id.clone(), r, skipped(id, slot)))
            })
            .collect();
        if let Some(target) = &opts.abort_on
            && let Some((_, r, false)) = slots.iter().find(|(id, ..)| id == target)
            && let Err(e) = r.reload_from(&merged, Some(generation)).await
        {
            return Err(PublishError::TargetRefused {
                tenant: target.clone(),
                error: e.to_string(),
            });
        }
        // From here resolvers move to `generation`, published or not
        self.state.generation = generation;
        let resolvers = leaf(&reloader.resolvers).clone();
        for r in &resolvers {
            r.reload_from(&merged, Some(generation))
                .await
                .map_err(|e| PublishError::Invalid(anyhow!("{e}")))?;
        }
        let mut tenant_errors = Vec::new();
        for (id, r, skip) in &slots {
            if *skip || opts.abort_on.as_deref() == Some(id.as_str()) {
                continue;
            }
            if let Err(e) = r.reload_from(&merged, Some(generation)).await {
                tracing::warn!(
                    target: "walshadow::control",
                    tenant = %id,
                    error = %e,
                    "tenant reload failed; its last config stays in effect",
                );
                tenant_errors.push((id.clone(), e.to_string()));
            }
        }
        {
            let mut tenants = leaf(&reloader.tenants);
            for id in &rebinding {
                if let Some(slot) = tenants.get_mut(id)
                    && slot.resolver.is_some()
                {
                    slot.rebinding_since.get_or_insert(generation);
                }
            }
        }
        let revision = Arc::new(ConfigRevision {
            epoch: reloader.epoch.clone(),
            generation,
            merged,
            force_rebind: opts.force_rebind,
            cause,
        });
        reloader.tx.send_replace(revision.clone());
        let unfollowed = match resolvers.first() {
            Some(first) => {
                let mut dbs = first.configured_databases().await;
                dbs.retain(|db| !resolvers.iter().any(|r| r.dbname() == db));
                dbs
            }
            None => Vec::new(),
        };
        Ok(Published {
            revision,
            unfollowed,
            tenant_errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolver(path: PathBuf, doc: &str, db: &str) -> Arc<ConfigResolver> {
        let doc: Table = doc.parse().unwrap();
        let base = crate::ch_emitter::EmitterConfig::for_database(&doc, db).unwrap();
        ConfigResolver::new(
            &base,
            crate::config::CliOverrides::default(),
            Some(path),
            Table::new(),
            crate::mapping::mapping_handle(Default::default()),
            crate::config::ResolverBoot::default(),
        )
        .0
    }

    const DOC: &str = "[ch]\nhost = \"ch\"\n[source]\ndbname = \"app\"\n\
         [table.public.orders]\nreplicate = true\n";

    #[tokio::test]
    async fn every_reload_publishes_one_revision_after_resolvers_apply() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ch-config.toml");
        tokio::fs::write(&path, DOC).await.unwrap();
        let r = resolver(path.clone(), DOC, "app");
        let reloader = Reloader::new(Some(path.clone()), Table::new());
        let mut revs = reloader.subscribe();
        reloader.set_resolvers(vec![r.clone()]).await;
        let first = revs.borrow_and_update().clone();
        assert_eq!(first.cause, ReloadCause::Sweep);
        assert_eq!(r.applied_generation().await, Some(first.generation));

        tokio::fs::write(
            &path,
            format!("{DOC}[table.public.items]\nreplicate = true\n"),
        )
        .await
        .unwrap();
        reloader.reload_with(ReloadCause::Sighup).await.unwrap();
        assert!(revs.has_changed().unwrap());
        let second = revs.borrow_and_update().clone();
        assert!(second.generation > first.generation);
        assert!(second.cause.is_external());
        assert_eq!(&*second.epoch, &**reloader.epoch());
        assert!(
            second.merged["table"]["public"]
                .as_table()
                .unwrap()
                .contains_key("items")
        );
        assert_eq!(r.applied_generation().await, Some(second.generation));
    }

    #[tokio::test]
    async fn an_invalid_config_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ch-config.toml");
        tokio::fs::write(&path, DOC).await.unwrap();
        let reloader = Reloader::new(Some(path.clone()), Table::new());
        reloader
            .set_resolvers(vec![resolver(path.clone(), DOC, "app")])
            .await;
        let before = reloader.current().generation;
        tokio::fs::write(&path, format!("{DOC}[database.b.table.public.t]\n"))
            .await
            .unwrap();
        let err = reloader.reload().await.unwrap_err().to_string();
        assert!(err.contains("[tenant.<id>]"), "{err}");
        assert_eq!(reloader.current().generation, before);
    }

    /// A writer holding the lock across its write keeps every other reload
    /// out until it publishes
    #[tokio::test]
    async fn the_lock_spans_write_and_publish() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ch-config.toml");
        tokio::fs::write(&path, DOC).await.unwrap();
        let reloader = Arc::new(Reloader::new(Some(path.clone()), Table::new()));
        let mut tx = reloader.begin().await;
        tokio::fs::write(
            &path,
            format!("{DOC}[table.public.items]\nreplicate = true\n"),
        )
        .await
        .unwrap();
        let other = {
            let reloader = reloader.clone();
            tokio::spawn(async move { reloader.reload_with(ReloadCause::Sighup).await })
        };
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!other.is_finished(), "SIGHUP waited for the writer");
        // The writer rolls back before publishing anything
        tokio::fs::write(&path, DOC).await.unwrap();
        let published = tx
            .publish(ReloadCause::Control, PublishOpts::default())
            .await
            .unwrap();
        drop(tx);
        other.await.unwrap().unwrap();
        assert!(
            !published.revision.merged["table"]["public"]
                .as_table()
                .unwrap()
                .contains_key("items")
        );
    }
}
