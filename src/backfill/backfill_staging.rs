//! Staging-table coherence for backup-sourced initial loads
//! (architecture/bootstrap.md).
//!
//! A pass's rows land in `<table>__wsstg`, never the destination; success
//! publishes atomically via `EXCHANGE TABLES` then copies the live-window
//! rows (`_lsn > S`) back from the swapped-out storage; failure leaves the
//! destination untouched and a retry rebuilds staging from scratch. No
//! partial pass can leak rows a retry's source cannot tombstone (LATEST
//! re-resolution drift) and a re-opt-in purges stale rows wholesale.
//!
//! Statement discipline: DROP/CREATE/INSERT..SELECT are idempotent (dedup
//! absorbs a copy-back resend) and retry like the inserter pool; `EXCHANGE`
//! is single-shot — a blind resend after an ambiguous timeout would swap
//! back. Ambiguity resolves through the ledger instead: the staging table's
//! uuid is persisted before the exchange, so recovery can tell "not yet
//! swapped" (uuid unchanged under the staging name) from "swapped" (uuid
//! differs) from "already copied back" (staging name gone).

use std::sync::Arc;

use anyhow::{Context, Result, bail};

use crate::backfill::backfill_types::BackupRequest;
use crate::backfill::copy_backfill::SwapPermit;
use crate::ch::{ChConn, EmitterError, exec_drain, query_strings, quote_ident};
use crate::config::{DestEmitter, ResolvedConfig};
use crate::destination::snowflake::runtime::SnowflakeRuntime;
use crate::destination::snowflake::state::GenerationPhase;
use crate::emit::ch_emitter::EmitterConfig;
use crate::emit::route::RouteSnapshot;
use crate::mapping::{MappingHandle, MappingSnapshot, TableMapping, TableTarget};
use crate::runtime_config::InitialLoadMode;
use crate::schema::{RelDescriptor, RelName};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use futures::{StreamExt, TryStreamExt};

/// `orders` loads into `orders__wsstg`; deterministic so a retry or boot
/// recovery finds the prior attempt's table
pub const STAGING_SUFFIX: &str = "__wsstg";

/// One rel's swap identities; `s_lsn` drives the copy-back filter
#[derive(Debug, Clone)]
pub struct StagingRel {
    pub rel: RelName,
    pub target: TableTarget,
    pub s_lsn: u64,
}

impl StagingRel {
    pub fn staging_table(&self) -> String {
        format!("{}{STAGING_SUFFIX}", self.target.table)
    }

    pub fn real_sql(&self) -> String {
        self.target.sql()
    }

    pub fn staging_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.target.database),
            quote_ident(&self.staging_table())
        )
    }
}

/// Per-pass staging setup: routing snapshot targeting the staging tables
/// plus the rels to publish on success. Rels unmapped at prepare are absent
/// from both — their rows walk-and-skip exactly as without staging.
pub struct StagingPlan {
    pub mapping: MappingHandle,
    pub rels: Vec<StagingRel>,
}

/// One backup-walk snapshot generation. `Replayed` means a prior successful
/// attempt was recovered and its relation must not be reread into this pass.
#[derive(Clone)]
pub struct SnowflakeSnapshotRel {
    pub desc: Arc<RelDescriptor>,
    pub operation_id: String,
    pub phase: GenerationPhase,
}

/// Frozen source-shaped routes and journaled generation targets for a pass.
pub struct SnowflakeSnapshotPlan {
    pub mapping: MappingHandle,
    pub source_mapping: MappingSnapshot,
    pub rels: Vec<SnowflakeSnapshotRel>,
    pub operations: HashMap<RelName, String>,
}

pub(crate) fn route_config_matches(
    expected: &ResolvedConfig,
    current: &ResolvedConfig,
    desc: &RelDescriptor,
) -> bool {
    let rel = &desc.rel_name;
    expected.tables.get(rel) == current.tables.get(rel)
        && expected.table_opt_ins.get(rel) == current.table_opt_ins.get(rel)
        && expected.rules.settings(rel) == current.rules.settings(rel)
        && desc.attributes.iter().filter(|a| !a.dropped).all(|a| {
            expected.column_rules.settings(rel, &a.name)
                == current.column_rules.settings(rel, &a.name)
                && expected.column_rules.accepted_type(rel, &a.name)
                    == current.column_rules.accepted_type(rel, &a.name)
        })
}

pub async fn prepare_snowflake(
    runtime: &SnowflakeRuntime,
    emitter: &EmitterConfig,
    live: &MappingHandle,
    reqs: &[BackupRequest],
    mode: InitialLoadMode,
    config: Option<&Arc<ResolvedConfig>>,
    config_rx: Option<&tokio::sync::watch::Receiver<Arc<ResolvedConfig>>>,
) -> Result<SnowflakeSnapshotPlan> {
    prepare_snowflake_with_kind(
        runtime,
        emitter,
        live,
        reqs,
        mode.as_str(),
        config.map(Arc::as_ref),
        config.zip(config_rx),
    )
    .await
}

pub async fn prepare_snowflake_with_kind(
    runtime: &SnowflakeRuntime,
    emitter: &EmitterConfig,
    live: &MappingHandle,
    reqs: &[BackupRequest],
    kind: &str,
    config: Option<&ResolvedConfig>,
    current_config: Option<(
        &Arc<ResolvedConfig>,
        &tokio::sync::watch::Receiver<Arc<ResolvedConfig>>,
    )>,
) -> Result<SnowflakeSnapshotPlan> {
    let mut relation_oids = HashSet::with_capacity(reqs.len());
    anyhow::ensure!(
        reqs.iter().all(|req| relation_oids.insert(req.desc.oid)),
        "Snowflake snapshot pass contains duplicate relations"
    );
    let live_map = live.snapshot().await;
    // begin_snapshot_attempt can finish a previously loaded generation, so
    // the route must remain current during setup as well as final publication.
    let selected = reqs
        .iter()
        .map(|r| r.desc.rel_name.clone())
        .collect::<Vec<_>>();
    let _mapping_guard = live
        .guard_matches(&live_map, &selected)
        .await
        .context("Snowflake snapshot mapping changed before preparation")?;
    if let Some((expected, rx)) = current_config {
        let current = rx.borrow();
        anyhow::ensure!(
            reqs.iter()
                .all(|r| route_config_matches(expected, &current, &r.desc)),
            "Snowflake snapshot routing config changed before preparation"
        );
    }
    let mut walk_map = HashMap::with_capacity(reqs.len());
    let mut rels = Vec::with_capacity(reqs.len());
    let mut operations = HashMap::with_capacity(reqs.len());
    let prepared = futures::stream::iter(reqs.to_vec())
        .map(|req| {
            let live_map = &live_map;
            async move {
                let name = &req.desc.rel_name;
                let Some(mapping) = live_map.get(name) else {
                    tracing::warn!(target: "walshadow::backfill_staging", qname = %name,
                "Snowflake snapshot relation unmapped at pass start");
                    return Ok::<_, anyhow::Error>(None);
                };
                let route = RouteSnapshot::freeze(
                    mapping.clone(),
                    config.map_or_else(Arc::default, |c| c.column_rules.clone()),
                    emitter.row_policy().for_rel(config, name),
                );
                runtime.defer_publication(&req.desc)?;
                runtime.schema_for(&req.desc, &route).await?;
                let logical_id = super::copy_backfill::snowflake_snapshot_logical_id(
                    kind,
                    &runtime.source_identity,
                    &req.desc,
                    req.s_lsn,
                );
                let generation = runtime
                    .begin_snapshot_attempt(&req.desc, req.s_lsn, &logical_id)
                    .await?;
                if generation.phase != GenerationPhase::Replayed {
                    anyhow::ensure!(
                        generation.phase == GenerationPhase::Prepared,
                        "Snowflake snapshot generation is not prepared for {name}"
                    );
                }
                Ok(Some((req, mapping.clone(), generation)))
            }
        })
        .buffered(runtime.config.metadata_concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    for (req, mapping, generation) in prepared.into_iter().flatten() {
        if generation.phase != GenerationPhase::Replayed {
            operations.insert(req.desc.rel_name.clone(), generation.operation_id.clone());
            walk_map.insert(req.desc.rel_name.clone(), (*mapping).clone());
        }
        rels.push(SnowflakeSnapshotRel {
            desc: req.desc.clone(),
            operation_id: generation.operation_id,
            phase: generation.phase,
        });
    }
    tracing::info!(target: "walshadow::backfill_staging", tables = rels.len(),
        "Snowflake snapshot generations prepared");
    Ok(SnowflakeSnapshotPlan {
        mapping: crate::mapping::mapping_handle(walk_map),
        source_mapping: live_map,
        rels,
        operations,
    })
}

/// Rebuild one staging table per mapped rel (`DROP` + `CREATE .. AS` clones
/// structure and engine) and snapshot the routing map against them.
///
/// `reuse` keeps the existing tables: a resumed pass is vouched for by rows
/// already in them, so rebuilding would throw its own progress away
pub async fn prepare(
    dest: Arc<DestEmitter>,
    live: &MappingHandle,
    reqs: &[BackupRequest],
    reuse: bool,
) -> Result<StagingPlan> {
    let mut sess = StagingSession::connect(dest).await?;
    // Freeze routing for entire staging plan
    let live_map = live.snapshot().await;
    let mut staged: HashMap<RelName, TableMapping> = HashMap::with_capacity(reqs.len());
    let mut rels = Vec::with_capacity(reqs.len());
    for r in reqs {
        let name = &r.desc.rel_name;
        let Some(m) = live_map.get(name) else {
            tracing::warn!(
                target: "walshadow::backfill_staging",
                qname = %name,
                "no mapping at pass start; rows will skip",
            );
            continue;
        };
        let rel = StagingRel {
            rel: name.clone(),
            target: m.target.clone(),
            s_lsn: r.s_lsn,
        };
        if !reuse {
            sess.rebuild_staging(&rel)
                .await
                .with_context(|| format!("backfill_staging: rebuild staging for {name}"))?;
        }
        staged.insert(
            name.clone(),
            TableMapping {
                target: TableTarget {
                    table: rel.staging_table(),
                    ..m.target.clone()
                },
                columns: m.columns.clone(),
                // Write load rows directly to tees; staging swap replaces only main destination
                tees: m.tees.clone(),
            },
        );
        rels.push(rel);
    }
    Ok(StagingPlan {
        mapping: crate::mapping::mapping_handle(staged),
        rels,
    })
}

/// One CH control connection for staging DDL + swap statements, with the
/// inserter pool's bounded per-attempt timeout.
pub struct StagingSession {
    /// `None` for Snowflake, which stages through its own runtime
    client: Option<ChConn>,
    dest: Arc<DestEmitter>,
    /// Per-relation destination rules, for the promote's `_lsn` predicate when
    /// a `[table.*]` block or `config_table` row renamed that column
    rules: Option<Arc<crate::table_rules::TableRules>>,
}

impl StagingSession {
    pub async fn connect(dest: Arc<DestEmitter>) -> Result<Self> {
        if dest.base().snowflake.is_some() {
            return Ok(Self {
                client: None,
                dest,
                rules: None,
            });
        }
        let client = ChConn::connect(dest.clone())
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: connect: {e}"))?;
        Ok(Self {
            client: Some(client),
            dest,
            rules: None,
        })
    }

    pub fn snowflake_runtime(&self) -> Option<&Arc<SnowflakeRuntime>> {
        self.dest.base().snowflake.as_ref()
    }

    pub fn select(&mut self, instance: Option<&str>) -> Result<()> {
        self.client
            .as_mut()
            .context("ClickHouse staging SQL is unavailable for Snowflake")?
            .select(instance)?;
        Ok(())
    }

    pub fn with_rules(mut self, rules: Option<Arc<crate::table_rules::TableRules>>) -> Self {
        self.rules = rules;
        self
    }

    /// LSN column of one relation's destination
    fn lsn_column(&self, rel: &RelName) -> String {
        match &self.rules {
            Some(rules) => rules
                .settings(rel)
                .system_columns(&self.dest.current().system_columns)
                .lsn
                .clone(),
            None => self.dest.current().system_columns.lsn.clone(),
        }
    }

    async fn attempt_write(&mut self, sql: &str) -> Result<(), EmitterError> {
        let timeout = self.dest.current().insert_timeout;
        let client = self
            .client
            .as_mut()
            .ok_or_else(|| {
                EmitterError::Config("ClickHouse staging SQL is unavailable for Snowflake".into())
            })?
            .ready()
            .await?;
        exec_drain(client, sql, timeout).await
    }

    /// Statement safe to re-apply (DROP/CREATE IF NOT EXISTS, dedup-absorbed
    /// INSERT..SELECT): reconnect + resend on retryable failure.
    pub(crate) async fn exec_retry(&mut self, sql: &str) -> Result<()> {
        let timeout = self.dest.current().insert_timeout;
        self.client
            .as_mut()
            .context("ClickHouse staging SQL is unavailable for Snowflake")?
            .retry(
                |mut client| async move {
                    let result = exec_drain(&mut client, sql, timeout).await;
                    (client, result)
                },
                |e, attempt| {
                    tracing::warn!(
                        target: "walshadow::backfill_staging",
                        error = %e, attempt, sql,
                        "statement failed; reconnecting + retrying",
                    );
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: {sql}: {e}"))
    }

    /// Single attempt, no resend: an ambiguous timeout may have applied
    /// server-side. Callers resolve through the ledger's staging uuid.
    async fn exec_once(&mut self, sql: &str) -> Result<()> {
        self.attempt_write(sql)
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: {sql}: {e}"))
    }

    /// Single-column String SELECT, one attempt under the timeout.
    pub(crate) async fn query_strings(&mut self, sql: &str) -> Result<Vec<String>> {
        let timeout = self.dest.current().insert_timeout;
        let client = self
            .client
            .as_mut()
            .context("ClickHouse staging SQL is unavailable for Snowflake")?;
        async { query_strings(client.ready().await?, sql, timeout).await }
            .await
            .map_err(|e| anyhow::anyhow!("backfill_staging: {sql}: {e}"))
    }

    pub async fn rebuild_staging(&mut self, rel: &StagingRel) -> Result<()> {
        self.select(rel.target.instance.as_deref())?;
        self.exec_retry(&format!("DROP TABLE IF EXISTS {}", rel.staging_sql()))
            .await?;
        // IF NOT EXISTS only shields an ambiguous-timeout resend; the table
        // is fresh from the DROP above either way
        self.exec_retry(&format!(
            "CREATE TABLE IF NOT EXISTS {} AS {}",
            rel.staging_sql(),
            rel.real_sql()
        ))
        .await
    }

    pub async fn drop_staging(&mut self, rel: &StagingRel) -> Result<()> {
        self.select(rel.target.instance.as_deref())?;
        self.exec_retry(&format!("DROP TABLE IF EXISTS {}", rel.staging_sql()))
            .await
    }

    /// Ordered `name type` list; equality across real/staging gates the swap
    /// (a mid-pass DDL means the loaded copy has the pre-DDL shape).
    pub async fn schema_fingerprint(&mut self, database: &str, table: &str) -> Result<Vec<String>> {
        self.query_strings(&format!(
            "SELECT concat(name, ' ', type) FROM system.columns \
             WHERE database = {} AND table = {} ORDER BY position",
            sql_str(database),
            sql_str(table)
        ))
        .await
    }

    /// `None` when the table doesn't exist.
    pub async fn table_uuid(&mut self, database: &str, table: &str) -> Result<Option<String>> {
        let rows = self
            .query_strings(&format!(
                "SELECT toString(uuid) FROM system.tables \
                 WHERE database = {} AND name = {}",
                sql_str(database),
                sql_str(table)
            ))
            .await?;
        Ok(rows.into_iter().next())
    }

    /// Atomic publish; requires an Atomic/Replicated database engine.
    pub async fn exchange(&mut self, permit: SwapPermit<'_>) -> Result<()> {
        let rel = permit.rel();
        self.select(rel.target.instance.as_deref())?;
        self.exec_once(&format!(
            "EXCHANGE TABLES {} AND {}",
            rel.real_sql(),
            rel.staging_sql()
        ))
        .await
    }

    /// Recover the live window from the swapped-out storage: rows the live
    /// stream delivered during the pass carry `_lsn > S`; anything at or
    /// below `S` is prior-life state the swap just purged and must not come
    /// back. Column list is the intersection (destination order) so DDL
    /// applied to the destination after the swap can't block the copy-back.
    pub async fn copy_back(&mut self, rel: &StagingRel) -> Result<()> {
        self.select(rel.target.instance.as_deref())?;
        let real_cols = self
            .query_strings(&format!(
                "SELECT name FROM system.columns WHERE database = {} AND table = {} \
                 ORDER BY position",
                sql_str(&rel.target.database),
                sql_str(&rel.target.table)
            ))
            .await?;
        let staging_cols: HashSet<String> = self
            .query_strings(&format!(
                "SELECT name FROM system.columns WHERE database = {} AND table = {} \
                 ORDER BY position",
                sql_str(&rel.target.database),
                sql_str(&rel.staging_table())
            ))
            .await?
            .into_iter()
            .collect();
        let cols: Vec<String> = real_cols
            .iter()
            .filter(|c| staging_cols.contains(*c))
            .map(|c| quote_ident(c))
            .collect();
        if cols.is_empty() {
            bail!(
                "backfill_staging: no shared columns between {} and {}",
                rel.real_sql(),
                rel.staging_sql()
            );
        }
        let list = cols.join(", ");
        let lsn = quote_ident(&self.lsn_column(&rel.rel));
        self.exec_retry(&format!(
            "INSERT INTO {} ({list}) SELECT {list} FROM {} WHERE {lsn} > {}",
            rel.real_sql(),
            rel.staging_sql(),
            rel.s_lsn
        ))
        .await
    }
}

/// CH single-quoted string literal.
pub(crate) fn sql_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\\' || c == '\'' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_config_fence_ignores_unrelated_tables_but_rejects_own_opt_out() {
        let rel = RelName::new("public", "orders");
        let other = RelName::new("public", "customers");
        let desc = RelDescriptor {
            ancestors: Vec::new(),
            rfn: Default::default(),
            oid: 1,
            toast_oid: 0,
            namespace_oid: 1,
            rel_name: rel.clone(),
            kind: 'r',
            persistence: 'p',
            replident: crate::schema::ReplIdent::Nothing,
            attributes: vec![],
        };
        let mut expected = ResolvedConfig::default();
        expected.tables.insert(
            rel.clone(),
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("db", "orders"),
                columns: vec![],
            },
        );
        let mut current = expected.clone();
        current.tables.insert(
            other,
            TableMapping {
                tees: Vec::new(),
                target: TableTarget::new("db", "customers"),
                columns: vec![],
            },
        );
        assert!(route_config_matches(&expected, &current, &desc));
        current.tables.remove(&rel);
        assert!(!route_config_matches(&expected, &current, &desc));
    }

    #[tokio::test]
    async fn staging_retry_includes_failed_reconnect() {
        let sql = "DROP TABLE IF EXISTS staging";
        for retries in 0..=2 {
            let (config, server) =
                crate::ch::test_support::retry_server(retries, sql, false, false).await;
            let mut session = StagingSession::connect(DestEmitter::new(Arc::new(config), None))
                .await
                .unwrap();
            assert_eq!(session.exec_retry(sql).await.is_ok(), retries == 2);
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn exchange_ambiguity_does_not_retry() {
        let sql = "EXCHANGE TABLES staging AND live";
        let (config, server) = crate::ch::test_support::retry_server(2, sql, false, false).await;
        let mut session = StagingSession::connect(DestEmitter::new(Arc::new(config), None))
            .await
            .unwrap();
        assert!(session.exec_once(sql).await.is_err());
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[test]
    fn staging_rel_renders_sql_names() {
        let rel = StagingRel {
            rel: RelName::new("public", "orders"),
            target: TableTarget::new("db", "orders"),
            s_lsn: 0x5000,
        };
        assert_eq!(rel.real_sql(), "`db`.`orders`");
        assert_eq!(rel.staging_table(), "orders__wsstg");
        assert_eq!(rel.staging_sql(), "`db`.`orders__wsstg`");
    }

    #[test]
    fn sql_str_escapes_quotes_and_backslashes() {
        assert_eq!(sql_str("plain"), "'plain'");
        assert_eq!(sql_str("o'brien"), "'o\\'brien'");
        assert_eq!(sql_str("a\\b"), "'a\\\\b'");
    }
}
