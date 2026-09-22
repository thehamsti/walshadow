//! Per-table opt-in dispatch shared by the live WAL apply path (the reorder
//! coordinator, [`crate::emit::pipeline::reorder`]) and the boot seed
//! (`stream` binary's `seed_runtime_config` follow-up). A `config_table` row's
//! `replicate` flag brings a rel into or out of replication scope; the actual
//! work — resolve the descriptor, create the CH table, register the mapping —
//! needs the catalog + a CH client the [`crate::config::ConfigResolver`] lacks,
//! so it lives here rather than on the resolver.
//!
//! Boot must re-run this for seeded `replicate=true` rows: on a normal restart
//! the resume cursor is already past the config row's commit LSN, so WAL replay
//! never re-delivers it — the seed is the only chance to re-materialise the
//! opt-in mapping (the CH table itself persists across restarts).
//!
//! `initial_load` on a non-empty rel hands off to the
//! [`crate::backfill::copy_backfill::CopyBackfiller`]: `'copy'` issues a snapshot-free
//! COPY of pre-opt-in rows at `_lsn = S` (the opt-in LSN); `'base_backup'` /
//! `'object_store'` coalesce into a backup-sourced page-walk pass
//! ([`crate::backfill::backup_backfill`], architecture/bootstrap.md). All
//! converge with the WAL stream via `ReplacingMergeTree(_lsn)` dedup, and the
//! backfiller's ledger dedups restarts. `None` (backfiller not wired) streams
//! from the opt-in LSN only; so do unknown mode strings (validate-late, never
//! crash).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::ch::EmitterError;
use crate::config::ConfigResolver;
use crate::emit::ch_ddl::DdlApplicator;
use crate::runtime_config::{InitialLoadMode, TableRow};
use crate::schema::{RelDescriptor, RelName};

#[async_trait]
pub trait Backfiller: Send + Sync {
    async fn note_opt_in(
        self: Arc<Self>,
        desc: Arc<RelDescriptor>,
        mode: InitialLoadMode,
        opt_in_lsn: u64,
    );

    async fn note_opt_out(&self, rel: &RelName);
}

/// Dispatch one `config_table` row's inclusion intent. `opt_in_lsn` is the
/// backfill boundary `S`: the row's commit LSN on the live path, the WAL
/// resume LSN on the boot seed (both satisfy COPY-snapshot ≥ `S`).
///
/// - `replicate=true`, rel known → create the CH table + register a
///   descriptor-derived mapping (+ backfill per `initial_load` mode).
/// - `replicate=true`, rel unknown → park a forward-declaration, materialised
///   by [`materialize_pending_on_added`] when its `CREATE TABLE` arrives.
/// - `replicate=false` → exclude (mid-stream drain).
/// - `replicate=None` → no scope change (legacy `target`-override rows, handled
///   by the resolver's overlay merge).
#[allow(clippy::too_many_arguments)]
pub async fn apply_table_opt_in(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    backfiller: Option<&Arc<dyn Backfiller>>,
    rel: &RelName,
    row: &TableRow,
    opt_in_lsn: u64,
) -> Result<(), EmitterError> {
    let mut deferred = DeferredBackfills::default();
    apply_table_opt_in_deferred(
        resolver,
        applicator,
        catalog,
        backfiller.is_some(),
        rel,
        row,
        opt_in_lsn,
        &mut deferred,
    )
    .await?;
    deferred.start(backfiller).await;
    Ok(())
}

/// Backfills opt-ins requested, started only once every mapping of a batch
/// of opt-ins is published: a running backfill guards the mapping while it
/// prepares and publishes, so starting each one as its opt-in lands would
/// make every later opt-in wait on the earlier loads
#[derive(Default)]
pub struct DeferredBackfills {
    starts: Vec<(Arc<RelDescriptor>, InitialLoadMode, u64)>,
    opt_outs: Vec<RelName>,
}

impl DeferredBackfills {
    pub fn len(&self) -> usize {
        self.starts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.starts.is_empty() && self.opt_outs.is_empty()
    }

    pub async fn start(self, backfiller: Option<&Arc<dyn Backfiller>>) {
        let Some(b) = backfiller else {
            return;
        };
        for rel in &self.opt_outs {
            b.note_opt_out(rel).await;
        }
        for (desc, mode, lsn) in self.starts {
            b.clone().note_opt_in(desc, mode, lsn).await;
        }
    }
}

/// [`apply_table_opt_in`] that queues the backfill into `deferred`
#[allow(clippy::too_many_arguments)]
pub async fn apply_table_opt_in_deferred(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    has_backfiller: bool,
    rel: &RelName,
    row: &TableRow,
    opt_in_lsn: u64,
    deferred: &mut DeferredBackfills,
) -> Result<(), EmitterError> {
    match row.replicate {
        Some(true) => {
            let desc = catalog
                .lock()
                .await
                .descriptor_by_name(rel)
                .await
                .map_err(|error| EmitterError::Catalog(error.to_string()))?;
            match desc {
                Some(desc) => {
                    if shadow_serves_toast(resolver, catalog, &desc).await? {
                        opt_in_known(
                            resolver,
                            applicator,
                            has_backfiller,
                            &desc,
                            row,
                            opt_in_lsn,
                            deferred,
                        )
                        .await?;
                    }
                }
                None => {
                    tracing::warn!(
                        target: "walshadow::config",
                        qname = %rel,
                        "config_table.replicate=true for unknown rel; parked as forward-declaration",
                    );
                    resolver.park_pending_decl(rel.clone(), row.clone()).await;
                }
            }
        }
        Some(false) => {
            // Opt-outs apply at once; the caller's backfiller notes them
            resolver.exclude_table(rel).await;
            deferred.starts.retain(|(desc, _, _)| desc.rel_name != *rel);
            deferred.opt_outs.push(rel.clone());
        }
        None => {}
    }
    Ok(())
}

/// Check whether shadow holds this relation's external values
/// Always true outside shadow mode, which uses destination mirror
async fn shadow_serves_toast(
    resolver: &ConfigResolver,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    desc: &RelDescriptor,
) -> Result<bool, EmitterError> {
    let Some(held) = resolver.shadow_toast() else {
        return Ok(true);
    };
    crate::toast::shadow_landing::serves_toast(catalog, held, desc)
        .await
        .map_err(|error| EmitterError::Catalog(error.to_string()))
}

/// When a `CREATE TABLE` lands, materialise a parked forward-declaration for
/// that qname (no-op otherwise). Runs from the catalog-event apply, inside the
/// same barrier fence, so trailing rows in the creating xact route.
///
/// No backfill: the rel was born after the declaration, so nothing pre-dates
/// its WAL coverage — any xact that can see the table commits after the
/// `CREATE`, and its rows were buffered inclusion-agnostically. Skip shadow
/// TOAST admission check because `CREATE` already admitted its TOAST heap
pub async fn materialize_pending_on_added(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    desc: &Arc<RelDescriptor>,
) -> Result<(), EmitterError> {
    if let Some(row) = resolver.take_pending_decl(&desc.rel_name).await {
        if row.initial_load.is_some() {
            tracing::info!(
                target: "walshadow::config",
                qname = %desc.rel_name,
                "forward-declared opt-in: initial_load unnecessary (rel born after the declaration)",
            );
        }
        let mut none = DeferredBackfills::default();
        opt_in_known(resolver, applicator, false, desc, &row, 0, &mut none).await?;
    }
    Ok(())
}

/// Create the CH table (idempotent) then register the opt-in mapping. Skips the
/// mapping if the shape can't be bridged so no rows route to a missing table.
async fn opt_in_known(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    has_backfiller: bool,
    desc: &Arc<RelDescriptor>,
    row: &TableRow,
    opt_in_lsn: u64,
    deferred: &mut DeferredBackfills,
) -> Result<(), EmitterError> {
    let snowflake_mapping = applicator
        .snowflake_opt_in_mapping(
            desc,
            row.target_database.as_deref(),
            row.target_table.as_deref(),
        )
        .await?;
    if snowflake_mapping.is_some()
        && row
            .initial_load
            .as_deref()
            .is_some_and(|mode| mode != "none")
    {
        applicator.defer_snowflake_publication(desc)?;
    }
    if !applicator.ensure_ch_table(desc).await? {
        return Ok(());
    }
    if let Some(mapping) = snowflake_mapping {
        resolver.materialize_prepared_opt_in(desc, mapping).await;
    } else {
        resolver
            .materialize_opt_in(desc, row.target_database.clone(), row.target_table.clone())
            .await;
    }
    if let Some(mode) = row.initial_load.as_deref() {
        match mode.parse() {
            Ok(InitialLoadMode::None) => {}
            Ok(parsed) => match has_backfiller {
                true => deferred.starts.push((desc.clone(), parsed, opt_in_lsn)),
                false => tracing::info!(
                    target: "walshadow::config",
                    qname = %desc.rel_name,
                    mode,
                    "initial_load requested but no backfiller wired; streaming from opt-in LSN only",
                ),
            },
            Err(_) => tracing::warn!(
                target: "walshadow::config",
                qname = %desc.rel_name,
                mode,
                "unknown initial_load mode; streaming from opt-in LSN only",
            ),
        }
    }
    Ok(())
}
