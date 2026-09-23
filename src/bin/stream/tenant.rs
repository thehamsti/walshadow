//! One tenant's half of a session: everything bound to its source
//! databases. The pump, filter, shadow, slot and manifest stay shared in
//! `run_session`; each tenant brings its own shadow catalog and bridge
//! connections, descriptor logs, transaction buffer, pipeline, destination
//! and state directory, fed by the [`TenantRouter`](walshadow::tenant_router).
//!
//! The single-tenant layout is one tenant, [`LEGACY_TENANT`], whose
//! directory is the spill dir itself and which follows every database the
//! config names (`[source] dbname` plus `[database.*]`). A declared tenant
//! follows exactly one database

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt};
use anyhow::{Context, Result};
use tokio::sync::{Mutex, watch};
use tokio_postgres::types::Oid;
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::conn::PgConfig;
use walshadow::boundary_hold::{
    BoundaryGateConfig, BoundaryHoldSink, BoundaryHoldStats, CatalogBoundaryGate,
};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{ConfigResolver, SourceConn};
use walshadow::desc_log::DescriptorLog;
use walshadow::filter::shadow_relations::ShadowHeld;
use walshadow::pg::socket_conninfo;
use walshadow::pipeline::ack::AckSnapshot;
use walshadow::pipeline::{Fatal, PipelineConfig, PipelineHandle, TailKind};
use walshadow::pos::{EmitterAck, FilterDurable, Floor, Monotone, Pos};
use walshadow::queueing_record_sink::QueueingRecordSink;
use walshadow::record::WAL_SEG_SIZE;
use walshadow::schema::RelName;
use walshadow::shadow_catalog::ShadowCatalog;
use walshadow::source_db::{DbLink, DbLinkConfig, SourceDb, SourceDbs};
use walshadow::tenants::LEGACY_TENANT;
use walshadow::timeline::TimelineHistory;
use walshadow::wal_stream::WalStream;
use walshadow::xact_buffer::{BufferingDecoderSink, SubxactTracker, XactBuffer, XactBufferConfig};

use crate::args::{Args, TENANT_BRIDGES, build_emitter_config, positive_usize};
use crate::housekeeping::{spawn_desc_log_gc, spawn_snowflake_maintenance};
use crate::metrics_publish::DbMetricSources;
use crate::session::SessionTasks;
use crate::shadow_proc::{ShadowLifecycle, open_shadow_sql_client};
use crate::sinks::DecoderXactPair;
use crate::source_db::{
    DescLogInputs, SourceDbInputs, build_source_db, metrics_only_db, open_db_desc_log,
};

/// One database a tenant follows
pub(crate) struct BootDb {
    pub name: String,
    /// Parsed config scoped to this database with its destination opened;
    /// `None` runs the metrics-only null tail
    pub emitter: Option<EmitterConfig>,
}

/// Everything a tenant needs from the session to open
pub(crate) struct TenantBoot<'a> {
    pub args: &'a Args,
    pub id: String,
    /// Followed databases in bridge socket order
    pub databases: Vec<BootDb>,
    /// Index into `databases` of the one whose settings the pipeline runs
    /// with and whose logs keep `dir` itself
    pub primary: usize,
    pub dir: PathBuf,
    pub emitter_stats: Arc<EmitterStats>,
    pub source_conn: SourceConn,
    /// Slot the pre-flight checks; the session's own, or none for a tenant
    pub preflight_slot: Option<String>,
    pub sysid: String,
    pub sysid_num: u64,
    pub source_major: u32,
    pub source_version_num: i32,
    pub start_timeline: u32,
    pub lineage: Vec<u32>,
    /// Where the pump resumes (boot) or the attachment position (attach)
    pub raw_start: Pos<Floor>,
    pub aligned: Pos<Floor>,
    /// Prior progress exists, so the descriptor logs must too
    pub expect_log: bool,
    pub start_lsn_override: Option<Pos<Floor>>,
    pub history_rx: watch::Receiver<Arc<TimelineHistory>>,
    pub shadow_state: Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
    pub smgr_markers: Arc<std::sync::Mutex<walshadow::filter::SmgrMarkers>>,
    pub xid_ceiling: Arc<walshadow::toast::xid_ceiling::XidCeiling>,
    /// Bridge socket base; database `i` of a multi-database tenant listens
    /// on its `i`th slice
    pub bridge_path: PathBuf,
    pub bridge_workers: usize,
    pub resume_floor: Arc<Monotone<Floor>>,
    pub shadow_toast_held: Option<ShadowHeld>,
    pub decoder_batch_size: usize,
    pub decoder_queue_capacity: usize,
    pub span_tracing: bool,
    /// Commits below this drain as nothing
    pub min_commit_lsn: u64,
    /// Attach with no table in scope; the table set arrives once the tenant
    /// is primed past every transaction it saw only part of
    pub priming: bool,
    /// Tables the tenant started with and their initial loads, re-applied
    /// so an unfinished load resumes
    pub activation_opt_ins: ahash::HashMap<RelName, walshadow::runtime_config::TableRow>,
}

/// Session parts every tenant shares, cloned into each [`TenantBoot`]
pub(crate) struct SessionShared {
    pub sysid: String,
    pub sysid_num: u64,
    pub source_conn: SourceConn,
    pub source_major: u32,
    pub source_version_num: i32,
    pub start_timeline: u32,
    pub lineage: Vec<u32>,
    pub history_rx: watch::Receiver<Arc<TimelineHistory>>,
    pub shadow_state: Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
    pub smgr_markers: Arc<std::sync::Mutex<walshadow::filter::SmgrMarkers>>,
    pub xid_ceiling: Arc<walshadow::toast::xid_ceiling::XidCeiling>,
    pub resume_floor: Arc<Monotone<Floor>>,
    pub decoder_batch_size: usize,
    pub decoder_queue_capacity: usize,
    pub span_tracing: bool,
}

impl SessionShared {
    /// A boot with the session's defaults, following one database over a
    /// tenant bridge pool
    #[allow(clippy::too_many_arguments)]
    pub fn boot<'a>(
        &self,
        args: &'a Args,
        id: String,
        db: BootDb,
        dir: PathBuf,
        emitter_stats: Arc<EmitterStats>,
        raw_start: Pos<Floor>,
        aligned: Pos<Floor>,
    ) -> TenantBoot<'a> {
        let tenant_workers = TENANT_BRIDGES.get().map_or(2, |&(w, _)| w);
        TenantBoot {
            args,
            bridge_path: walshadow::shadow::tenant_bridge_socket(
                &args.bridge_socket_path(),
                &db.name,
            ),
            bridge_workers: tenant_workers,
            id,
            databases: vec![db],
            primary: 0,
            dir,
            emitter_stats,
            source_conn: self.source_conn.clone(),
            preflight_slot: None,
            sysid: self.sysid.clone(),
            sysid_num: self.sysid_num,
            source_major: self.source_major,
            source_version_num: self.source_version_num,
            start_timeline: self.start_timeline,
            lineage: self.lineage.clone(),
            raw_start,
            aligned,
            expect_log: false,
            start_lsn_override: None,
            history_rx: self.history_rx.clone(),
            shadow_state: self.shadow_state.clone(),
            smgr_markers: self.smgr_markers.clone(),
            xid_ceiling: self.xid_ceiling.clone(),
            resume_floor: self.resume_floor.clone(),
            shadow_toast_held: None,
            decoder_batch_size: self.decoder_batch_size,
            decoder_queue_capacity: self.decoder_queue_capacity,
            span_tracing: self.span_tracing,
            min_commit_lsn: 0,
            priming: false,
            activation_opt_ins: Default::default(),
        }
    }

    /// Parse a tenant's effective config and open its destination
    pub async fn tenant_emitter(
        &self,
        args: &Args,
        merged: &toml::Table,
        tenants: &walshadow::tenants::TenantsConfig,
        decl: &walshadow::tenants::TenantDecl,
    ) -> Result<BootDb> {
        let effective = decl.effective_table(merged);
        let cfg = build_emitter_config(
            args,
            &effective,
            self.sysid_num,
            &decl.dbname,
            Some((tenants.decoder_pool_size, tenants.inserter_pool_size)),
        )
        .await
        .with_context(|| format!("tenant {}: config", decl.id))?;
        if let Some(cfg) = &cfg {
            anyhow::ensure!(
                !cfg.toast.mode.is_shadow(),
                "tenant {}: [toast] mode = shadow is single-database only",
                decl.id
            );
            anyhow::ensure!(
                cfg.databases.len() <= 1,
                "tenant {}: a tenant follows one database; `[database.*]` entries belong \
                 to the single-tenant layout",
                decl.id
            );
            if cfg.snowflake.is_none() {
                walshadow::ch_ddl::ensure_boot_database(cfg)
                    .await
                    .with_context(|| {
                        format!(
                            "tenant {}: reach ClickHouse {}:{}",
                            decl.id, cfg.host, cfg.port
                        )
                    })?;
            }
        }
        Ok(BootDb {
            name: decl.dbname.clone(),
            emitter: cfg,
        })
    }
}

/// Name every tenant database to shadow, whose launcher starts or stops their
/// bridge pools on reload
pub(crate) async fn publish_tenant_bridges(
    lifecycle: &ShadowLifecycle,
    dbnames: Vec<String>,
) -> Result<()> {
    let shadow = lifecycle.guard.shadow.clone();
    tokio::task::spawn_blocking(move || shadow.set_tenant_databases(&dbnames))
        .await
        .context("tenant bridge list task")?
        .context("publish tenant databases to shadow")?;
    Ok(())
}

/// Start tables as the resolver's opt-in rows
pub(crate) fn start_opt_ins(
    tables: &[walshadow::tenants::StartTable],
) -> ahash::HashMap<RelName, walshadow::runtime_config::TableRow> {
    tables
        .iter()
        .map(|t| {
            (
                RelName::new(&t.namespace, &t.name),
                walshadow::runtime_config::TableRow {
                    replicate: Some(true),
                    initial_load: Some(t.initial_load.clone()),
                    ..Default::default()
                },
            )
        })
        .collect()
}

pub(crate) struct Tenant {
    pub id: String,
    /// Primary database: the one a declared tenant follows
    pub dbname: String,
    pub db_oid: u32,
    /// Every followed database, primary included
    pub db_oids: Vec<u32>,
    pub dir: PathBuf,
    /// Shadow catalog session in the primary database
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    /// Bridge pools, one per followed database, for the status line
    pub bridges: Vec<(String, Arc<walshadow::bridge::Bridge>)>,
    pub oracle: Option<Arc<walshadow::oracle::Oracle>>,
    pub xact_buffer: Arc<Mutex<XactBuffer>>,
    pub emitter_ack: Arc<Monotone<EmitterAck>>,
    /// Primary database's descriptor log
    pub desc_log: Arc<DescriptorLog>,
    pub decoder_stats: Arc<walshadow::decoder_sink::DecoderStats>,
    pub emitter_stats: Option<Arc<EmitterStats>>,
    pub boundary_hold_stats: Arc<BoundaryHoldStats>,
    /// `database=`-labelled metric sources, one per followed database
    pub metrics_dbs: Vec<DbMetricSources>,
    pub pipeline: Option<PipelineHandle>,
    pub ack_probe: watch::Receiver<AckSnapshot>,
    /// Primary database's resolver: pause, source endpoint, tenant reloads
    pub config_resolver: Option<Arc<ConfigResolver>>,
    /// Every followed database's resolver, primary first
    pub config_resolvers: Vec<Arc<ConfigResolver>>,
    pub snowflake: Option<Arc<walshadow::destination::snowflake::runtime::SnowflakeRuntime>>,
    pub span_registry: Option<walshadow::trace::TxnSpanRegistry>,
    /// Pruners' floor: the session joins every persisted resume floor
    gc_floor: Option<Monotone<Floor>>,
    gc_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub gc_fatal: Fatal,
    snowflake_maintenance: Option<tokio::task::JoinHandle<()>>,
}

impl Tenant {
    /// Lowest position a restart must replay from for this tenant
    pub async fn resume_safe(&self) -> Pos<walshadow::pos::ResumeSafe> {
        let mut b = self.xact_buffer.lock().await;
        b.resume_safe_lsn(self.emitter_ack.get())
    }

    /// Hand a persisted resume floor to this tenant's pruners
    pub fn publish_floor(&self, floor: Pos<Floor>) {
        if let Some(gc) = &self.gc_floor {
            gc.join(floor);
        }
    }

    pub fn rebase_floor(&self, floor: Pos<Floor>) {
        if let Some(gc) = &self.gc_floor {
            gc.rebase(floor);
        }
    }

    /// First fatal error the tenant's pipeline or pruners raised
    pub fn fatal(&self) -> Option<String> {
        self.pipeline
            .as_ref()
            .and_then(|p| p.fatal.message())
            .or_else(|| self.gc_fatal.message())
    }

    /// Bridge status per followed database
    pub fn bridge_line(&self) -> String {
        self.bridges
            .iter()
            .map(|(name, bridge)| format!("{name}={}", bridge.stats.summary()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Drain the tenant: its queue (when still attached), then the pipeline
    /// cascade, then its pruners. Returns the final resume-safe position
    pub async fn shutdown(
        mut self,
        sink: Option<BoundaryHoldSink>,
    ) -> Result<Pos<walshadow::pos::ResumeSafe>> {
        if let Some(sink) = sink {
            sink.close()
                .await
                .with_context(|| format!("tenant {}: drain queueing decoder sink", self.id))?;
        }
        if let Some(pipeline) = self.pipeline.take() {
            pipeline
                .join()
                .await
                .map_err(|m| anyhow::anyhow!("tenant {}: pipeline drain failed: {m}", self.id))?;
        }
        let resume_safe = self.resume_safe().await;
        // Close the floor channel and join: nothing else may own a
        // desc_log.ckpt after the session returns
        drop(self.gc_floor.take());
        for task in self.gc_tasks.drain(..) {
            task.await.ok();
        }
        if let Some(task) = self.snowflake_maintenance.take() {
            task.abort();
        }
        if let Some(msg) = self.gc_fatal.message() {
            anyhow::bail!("tenant {}: {msg}", self.id);
        }
        Ok(resume_safe)
    }

    /// Stop without draining: an evicted tenant's destination is what
    /// stalled, so waiting for it would stall the session too
    pub fn abandon(mut self) {
        drop(self.gc_floor.take());
        if let Some(task) = self.snowflake_maintenance.take() {
            task.abort();
        }
        for task in self.gc_tasks.drain(..) {
            task.abort();
        }
        drop(self.pipeline.take());
    }
}

/// Open a tenant: connect each database's catalog and bridge, open their
/// descriptor logs, the buffer and pipeline, and return the hold sink the
/// router feeds
pub(crate) async fn open_tenant(
    boot: TenantBoot<'_>,
    tasks: Option<&mut SessionTasks>,
) -> Result<(Tenant, BoundaryHoldSink)> {
    let TenantBoot {
        args,
        id,
        databases,
        primary,
        dir,
        emitter_stats,
        source_conn,
        preflight_slot,
        sysid,
        sysid_num,
        source_major,
        source_version_num,
        start_timeline,
        lineage,
        raw_start,
        aligned,
        expect_log,
        start_lsn_override,
        history_rx,
        shadow_state,
        smgr_markers,
        xid_ceiling,
        bridge_path,
        bridge_workers,
        resume_floor,
        shadow_toast_held,
        decoder_batch_size,
        decoder_queue_capacity,
        span_tracing,
        min_commit_lsn,
        priming,
        activation_opt_ins,
    } = boot;
    let mut tasks = tasks;
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("create tenant dir {}", dir.display()))?;
    let legacy = id == LEGACY_TENANT;

    // Connect bridge and shadow catalog before START_REPLICATION so the
    // tracker→drain wire is hot from the first record. One pair per followed
    // database: catalog reads and value conversion answer from the database
    // that wrote the bytes
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    let socket_dir = args
        .shadow_socket_dir
        .to_str()
        .context("shadow-socket-dir not UTF-8")?;
    let mut db_conns: Vec<DbLink> = Vec::with_capacity(databases.len());
    for (index, db) in databases.into_iter().enumerate() {
        let conninfo = socket_conninfo(socket_dir, args.shadow_port, &args.shadow_user, &db.name);
        db_conns.push(
            DbLink::connect(DbLinkConfig {
                name: &db.name,
                index,
                workers: bridge_workers,
                bridge_path: &bridge_path,
                shadow_conninfo: &conninfo,
                budget: connect_budget,
                emitter: db.emitter,
            })
            .await
            .with_context(|| format!("tenant {id}: connect database {}", db.name))?,
        );
    }
    let primary_oid = db_conns[primary].oid;
    let dbname = db_conns[primary].name.clone();

    // Every followed database's source SQL session: runtime-config seeds and
    // pre-flight read the tenant's database, not the admin one the pump
    // connects to
    if !args.skip_preflight {
        let mut primary_cfg = source_conn.to_pg_config();
        primary_cfg.database = dbname.clone();
        let source_sql = open_sql_client_waiting(&primary_cfg, connect_budget)
            .await
            .with_context(|| format!("tenant {id}: source SQL session on database {dbname}"))?;
        let shadow_sql = open_shadow_sql_client(
            &args.shadow_socket_dir,
            args.shadow_port,
            &args.shadow_user,
            &dbname,
        )
        .await?;
        let mut report = walshadow::preflight::run(walshadow::preflight::Inputs {
            source_version_num,
            source_sql: &source_sql,
            shadow_sql: &shadow_sql,
            slot: preflight_slot.as_deref(),
            ch_config: db_conns[primary].emitter.as_ref(),
        })
        .await
        .with_context(|| format!("tenant {id}: pre-flight probe"))?;
        // Relations of another database resolve over a connection to it
        for (i, conn) in db_conns.iter().enumerate() {
            if i == primary {
                continue;
            }
            let Some(cfg) = &conn.emitter else {
                continue;
            };
            let client = crate::source_db::open_source_sql_client(&source_conn, &conn.name)
                .await
                .with_context(|| format!("source sql for database {}", conn.name))?;
            report.errors.extend(
                walshadow::preflight::mapped_relations(&client, cfg)
                    .await
                    .with_context(|| format!("pre-flight probe for database {}", conn.name))?,
            );
        }
        report
            .into_result()
            .with_context(|| format!("tenant {id}: pre-flight rejected"))?;
        tracing::info!(target: "walshadow::preflight", tenant = %id, "pre-flight passed");
    }

    let oracle = Some(Arc::new(
        walshadow::oracle::Oracle::per_database(
            db_conns
                .iter()
                .map(|conn| (conn.oid, conn.bridge.clone()))
                .collect(),
        )
        .with_xid_ceiling(xid_ceiling),
    ));

    // Spill dir wiped every startup: cursor file commits drains
    // atomically, so leftover spill from a prior crash is redundant or stale.
    let xact_buf_cfg = XactBufferConfig {
        xact_buffer_max: args.xact_buffer_max,
        ..XactBufferConfig::new(dir.clone())
    };
    let mut xact_buffer = XactBuffer::new(xact_buf_cfg).context("init xact buffer / spill dir")?;
    xact_buffer
        .clear_spill_dir()
        .await
        .context("clear stale spill files")?;
    xact_buffer.set_min_commit_lsn(min_commit_lsn);
    let xact_buffer = Arc::new(Mutex::new(xact_buffer));
    tracing::info!(
        target: "walshadow",
        tenant = %id,
        spill_dir = %dir.display(),
        xact_buffer_max = args.xact_buffer_max,
        min_commit_lsn = %format_pg_lsn(min_commit_lsn),
        "spill dir ready",
    );

    let pending_cfg = db_conns[primary]
        .emitter
        .as_ref()
        .map(|c| c.pending_capture)
        .unwrap_or_default();
    let pending_catalog = Arc::new(walshadow::pending::PendingCatalog::default());
    // One log per database, each in its own subdirectory; the primary keeps
    // the tenant dir so a single-database resume reads where it wrote
    let db_dir = |i: usize, oid: Oid| {
        if i == primary {
            dir.clone()
        } else {
            dir.join(format!("db-{oid}"))
        }
    };
    let mut desc_logs: Vec<Arc<DescriptorLog>> = Vec::with_capacity(db_conns.len());
    for (i, conn) in db_conns.iter().enumerate() {
        let log_dir = db_dir(i, conn.oid);
        tokio::fs::create_dir_all(&log_dir)
            .await
            .with_context(|| format!("create descriptor log dir {}", log_dir.display()))?;
        desc_logs.push(
            open_db_desc_log(DescLogInputs {
                args,
                dir: &log_dir,
                dbname: &conn.name,
                catalog: &conn.catalog,
                identity: walshadow::desc_log::DescLogIdentity {
                    pg_major: source_major,
                    system_id: sysid.clone(),
                    // Resume branch, which a crossing moves without moving the
                    // log: the stored header names wherever the log last
                    // rewrote itself, so `lineage` is what places it
                    timeline: start_timeline,
                    db_oid: conn.oid,
                    wal_seg_size: WAL_SEG_SIZE as u32,
                },
                lineage: &lineage,
                manifest_present: expect_log,
                start_lsn_override,
                raw_start,
                aligned,
            })
            .await
            .with_context(|| format!("tenant {id}: descriptor log"))?,
        );
    }
    let desc_log = desc_logs[primary].clone();
    let all_desc_logs = walshadow::desc_log::DescriptorLogs::new(desc_logs.clone());

    // Txn-span registry, shared by pump + decoder; `Some` only with OTLP on.
    let span_registry = if span_tracing {
        Some(xact_buffer.lock().await.span_registry())
    } else {
        None
    };
    let mut decoder = BufferingDecoderSink::new(all_desc_logs, xact_buffer.clone());
    if let Some(schema) = db_conns[primary]
        .emitter
        .as_ref()
        .and_then(|c| c.runtime_config_schema.as_deref())
    {
        decoder = decoder.with_config_schema(Arc::from(schema));
    }
    if let Some(reg) = &span_registry {
        decoder = decoder.with_span_registry(reg.clone());
    }
    let decoder_stats_handle = decoder.stats_handle();

    let mut emitter_stats_handle: Option<Arc<EmitterStats>> = None;
    // Seed at resume point so first status write cannot replace persisted ack
    // with zero before WAL re-read catches up
    let emitter_ack = Arc::new(Monotone::<EmitterAck>::new(raw_start.retag()));
    // Deferred retires queued before a stop; entries below `aligned` never
    // replay their drop, so the post-spawn flush below is their only route
    // to the wipe. Loaded in metrics-only runs too (inert without a chunk
    // store), preserved for a later CH run over the same spill dir.
    let retires = walshadow::toast_retire::RetireLedger::load(&dir, sysid_num)
        .await
        .context("load toast retire ledger")?;
    // Pending tables a bootstrap or backup pass left holding undecided rows.
    // Settling needs ClickHouse, so a metrics-only run leaves the ledger for
    // a later CH run over the same spill dir
    let pending_rows = walshadow::visibility_pending::PendingLedger::load(&dir, sysid_num)
        .await
        .context("load pending visibility ledger")?
        .shared();
    let mut config_resolvers: Vec<Arc<ConfigResolver>> = Vec::new();
    let mut resolvers_by_db: HashMap<Oid, Arc<ConfigResolver>> = HashMap::new();
    let mut copy_backfillers: HashMap<Oid, Arc<walshadow::copy_backfill::CopyBackfiller>> =
        HashMap::new();

    let snowflake = db_conns[primary]
        .emitter
        .as_ref()
        .and_then(|c| c.snowflake.clone());
    let pcfg = if db_conns[primary].emitter.is_some() {
        // Cluster-wide knobs come off the primary's copy: every database
        // parsed the same `[ch]`, `[memory]` and `[stream]` document
        let mut emitter_cfg = db_conns[primary]
            .emitter
            .clone()
            .expect("destination present");
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        let stats = emitter_stats.clone();
        emitter_stats_handle = Some(stats.clone());
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, emitter_cfg.decoder_pool_size)
                .map_err(|e| anyhow::anyhow!("memory budget: {e}"))?;
        let mut dbs: Vec<Arc<SourceDb>> = Vec::with_capacity(db_conns.len());
        let mut applicators: HashMap<Oid, walshadow::ch_ddl::DdlApplicator> = HashMap::new();
        let mut backfillers: HashMap<Oid, Arc<dyn walshadow::opt_in::Backfiller>> = HashMap::new();
        // One claim per destination across every database, so `replicate_all`
        // cannot name one ClickHouse table from two source tables
        let targets = Arc::new(walshadow::mapping::TargetOwners::default());
        let activation = walshadow::config::Activation {
            priming,
            opt_ins: activation_opt_ins,
        };
        for (i, conn) in db_conns.iter().enumerate() {
            let built = build_source_db(SourceDbInputs {
                args,
                conn,
                primary: i == primary,
                targets: &targets,
                desc_log: &desc_logs[i],
                spill_dir: db_dir(i, conn.oid),
                source: &source_conn,
                oracle: &oracle,
                history_rx: history_rx.clone(),
                budget: &pipeline_budget,
                stats: &stats,
                source_major,
                raw_start,
                shadow_toast_held: shadow_toast_held.as_ref(),
                system_id: sysid_num,
                pending_rows: &pending_rows,
                tasks: tasks.as_deref_mut(),
                tenant: (!legacy).then_some(id.as_str()),
                activation: activation.clone(),
            })
            .await
            .with_context(|| format!("tenant {id}: wire source database {}", conn.name))?;
            if i == primary {
                // Seeded + CLI values the initial batcher/inserter run with;
                // they track the watch channel live thereafter
                let rc = built.db.config_rx.as_ref().expect("resolver wired");
                let rc = rc.borrow();
                emitter_cfg.row_budget = rc.row_budget;
                emitter_cfg.byte_budget = rc.byte_budget;
                emitter_cfg.flush_timeout = rc.flush_timeout;
                emitter_cfg.compression = rc.compression;
                emitter_cfg.retry.max_attempts = rc.retry_max_attempts;
            }
            if let Some(applicator) = built.applicator {
                applicators.insert(conn.oid, applicator);
            }
            if let Some(backfiller) = built.backfiller.clone() {
                backfillers.insert(conn.oid, backfiller as _);
                copy_backfillers.insert(conn.oid, built.backfiller.expect("just cloned"));
            }
            if let Some(resolver) = &built.db.resolver {
                if i == primary {
                    config_resolvers.insert(0, resolver.clone());
                } else {
                    config_resolvers.push(resolver.clone());
                }
                resolvers_by_db.insert(conn.oid, resolver.clone());
            }
            dbs.push(built.db);
        }
        let (decoders, inserters) = (
            emitter_cfg.decoder_pool_size,
            emitter_cfg.inserter_pool_size,
        );
        tracing::info!(
            target: "walshadow::pipeline",
            tenant = %id,
            addr = %addr,
            decoders,
            inserters,
            databases = dbs.len(),
            resolvers = db_conns[primary].bridge.pool_size(),
            "parallel decode+insert pipeline starting",
        );
        PipelineConfig {
            emitter: emitter_cfg,
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            dbs: Arc::new(SourceDbs::new(dbs, primary_oid)),
            oracle: oracle.clone(),
            applicators,
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: stats.clone(),
            span_registry: span_registry.clone(),
            backfillers,
            retires,
            pending_rows,
            resume_floor: resume_floor.clone(),
            budget: Some(pipeline_budget),
        }
    } else {
        // Metrics-only (no destination): the identical pipeline with a null
        // tail — zero CH connections, no DDL applicator, no oracle (nothing
        // ships, PgPending stays raw). The empty mapping routes nothing, so
        // seqs complete at placement and the watermark + slot advance move
        // as in a CH run. Emitter stats stay unexported
        // (`emitter_stats_handle` None), matching the old serial surface.
        // No `[ch]` here, so the CLI layers straight onto the constants.
        let decoders = positive_usize(
            "decoder_pool_size",
            args.decoder_pool_size,
            walshadow::ch_emitter::DEFAULT_DECODER_POOL,
        );
        let inserters = positive_usize(
            "inserter_pool_size",
            args.inserter_pool_size,
            walshadow::ch_emitter::default_inserter_pool(),
        );
        tracing::info!(
            target: "walshadow::pipeline",
            tenant = %id,
            decoders,
            "metrics-only pipeline (null tail) starting",
        );
        let dbs: Vec<Arc<SourceDb>> = db_conns
            .iter()
            .zip(&desc_logs)
            .map(|(conn, desc_log)| Arc::new(metrics_only_db(conn, desc_log)))
            .collect();
        PipelineConfig {
            emitter: EmitterConfig::default(),
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            dbs: Arc::new(SourceDbs::new(dbs, primary_oid)),
            oracle: None,
            applicators: HashMap::new(),
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            pending: pending_catalog.clone(),
            stats: Arc::new(EmitterStats::default()),
            span_registry: span_registry.clone(),
            backfillers: HashMap::new(),
            retires,
            pending_rows: walshadow::visibility_pending::PendingLedger::empty().shared(),
            resume_floor: resume_floor.clone(),
            budget: None,
        }
    };
    let (mut reorder_sink, pipeline_handle) = pcfg
        .spawn(emitter_ack.clone())
        .await
        .context("spawn decode+insert pipeline")?;
    let ack_probe = pipeline_handle.ack_probe.clone();
    reorder_sink
        .flush_due_retires()
        .await
        .context("boot flush of due toast-mirror retires")?;
    reorder_sink
        .settle_pending_boot(Some(&args.bootstrap_shadow_data_dir))
        .await
        .context("boot settle of pending backup rows")?;
    reorder_sink
        .apply_boot_events(
            desc_logs
                .iter()
                .flat_map(|log| log.active_present_at(raw_start.get()))
                .collect(),
            raw_start.get(),
        )
        .await
        .context("boot Added pass over descriptor log")?;
    let decoder_xact = QueueingRecordSink::spawn(
        DecoderXactPair {
            decoder,
            xact_drain: reorder_sink,
        },
        decoder_batch_size,
        decoder_queue_capacity,
        span_registry.clone(),
    );
    let boundary_gate = CatalogBoundaryGate::new(
        shadow_state,
        BoundaryGateConfig {
            hold_timeout: Duration::from_secs(args.catalog_hold_timeout),
            ..BoundaryGateConfig::default()
        },
    );
    let boundary_hold_stats = boundary_gate.stats.clone();
    // One capture per database: a boundary's catalog reads answer over the
    // connection to the database that wrote them
    let mut captures = walshadow::catalog_capture::CaptureSet::default();
    for (conn, log) in db_conns.iter().zip(&desc_logs) {
        captures.insert(
            conn.oid,
            walshadow::catalog_capture::CatalogCapture::new(
                log.clone(),
                conn.catalog.clone(),
                xact_buffer.clone(),
                smgr_markers.clone(),
                pending_catalog.clone(),
                pending_cfg,
            ),
        );
    }
    let capture_stats: HashMap<Oid, Arc<walshadow::catalog_capture::CaptureStats>> =
        captures.stats_handles().collect();
    let metrics_dbs: Vec<DbMetricSources> = db_conns
        .iter()
        .zip(&desc_logs)
        .map(|(conn, log)| DbMetricSources {
            database: conn.name.clone(),
            bridge: [Some(conn.bridge.stats.clone()), None],
            desc_log: Some(log.clone()),
            capture: capture_stats.get(&conn.oid).cloned(),
            resolver: resolvers_by_db.get(&conn.oid).cloned(),
            backfiller: copy_backfillers.get(&conn.oid).cloned(),
        })
        .collect();
    let sink = BoundaryHoldSink::new(decoder_xact, boundary_gate).with_capture(captures);

    // Descriptor-log GC off the pump task: the session publishes each
    // persisted floor, the tasks compact
    let gc_fatal = walshadow::pipeline::Fatal::new();
    let gc_floor = Monotone::<Floor>::default();
    let gc_tasks = desc_logs
        .iter()
        .map(|log| spawn_desc_log_gc(log.clone(), gc_floor.watch(), gc_fatal.clone()))
        .collect();
    let snowflake_maintenance = snowflake
        .clone()
        .map(|runtime| spawn_snowflake_maintenance(runtime, gc_floor.watch()));

    Ok((
        Tenant {
            id,
            dbname,
            db_oid: primary_oid,
            db_oids: db_conns.iter().map(|conn| conn.oid).collect(),
            dir,
            catalog: db_conns[primary].catalog.clone(),
            bridges: db_conns
                .iter()
                .map(|conn| (conn.name.clone(), conn.bridge.clone()))
                .collect(),
            oracle,
            xact_buffer,
            emitter_ack,
            desc_log,
            decoder_stats: decoder_stats_handle,
            emitter_stats: emitter_stats_handle,
            boundary_hold_stats,
            metrics_dbs,
            pipeline: Some(pipeline_handle),
            ack_probe,
            config_resolver: config_resolvers.first().cloned(),
            config_resolvers,
            snowflake,
            span_registry,
            gc_floor: Some(gc_floor),
            gc_tasks,
            gc_fatal,
            snowflake_maintenance,
        },
        sink,
    ))
}

/// A tenant's SQL session can race the source becoming reachable after a
/// restart; retry within the shadow connect budget
async fn open_sql_client_waiting(
    cfg: &PgConfig,
    budget: Duration,
) -> Result<tokio_postgres::Client> {
    let deadline = Instant::now() + budget;
    loop {
        match walshadow::source_feed::open_sql_client(cfg).await {
            Ok(client) => return Ok(client),
            Err(e) if Instant::now() < deadline => {
                tracing::warn!(
                    target: "walshadow::tenant",
                    database = %cfg.database,
                    error = %format!("{e:#}"),
                    "source SQL session failed; retrying",
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Where a mid-stream attachment waits for every transaction older than it
struct Priming {
    attached_lsn: u64,
    start: tokio::sync::oneshot::Receiver<Result<u64>>,
    cfg: PgConfig,
    since: Instant,
    logged: Instant,
}

/// Tenant lifecycle the session drives between pump iterations: attaching
/// new tenants, priming them past partially seen transactions, activating
/// their table scope, detaching failed or unwanted ones
#[derive(Default)]
pub(crate) struct Supervisor {
    attach_queue: std::collections::VecDeque<String>,
    priming: std::collections::HashMap<String, Priming>,
}

impl Supervisor {
    pub fn queue_attach(&mut self, id: &str) {
        if !self.attach_queue.iter().any(|q| q == id) {
            self.attach_queue.push_back(id.to_string());
        }
    }

    pub fn next_attach(&mut self) -> Option<String> {
        self.attach_queue.pop_front()
    }

    pub fn forget(&mut self, id: &str) {
        self.attach_queue.retain(|q| q != id);
        self.priming.remove(id);
    }

    /// Reopen a tenant that was active before this boot. `None`: it has no
    /// usable state (new, priming, detached, or its database changed) and
    /// attaches afresh once the pump runs
    #[allow(clippy::too_many_arguments)]
    pub async fn boot_existing(
        &mut self,
        shared: &SessionShared,
        args: &Args,
        merged: &toml::Table,
        tenants: &walshadow::tenants::TenantsConfig,
        decl: &walshadow::tenants::TenantDecl,
        raw_start: Pos<Floor>,
        aligned: Pos<Floor>,
        reloader: &Arc<walshadow::control::Reloader>,
    ) -> Result<Option<(Tenant, BoundaryHoldSink, u64)>> {
        use walshadow::tenants::{Phase, TenantState, tenant_dir};
        let dir = tenant_dir(&args.spill_dir, &decl.id);
        let Some(state) = TenantState::load(&dir).await? else {
            return Ok(None);
        };
        if state.phase != Phase::Active || state.dbname != decl.dbname || args.ignore_cursor {
            return Ok(None);
        }
        let emitter = shared.tenant_emitter(args, merged, tenants, decl).await?;
        let from = state.attached_lsn;
        let mut boot = shared.boot(
            args,
            decl.id.clone(),
            emitter,
            dir,
            Arc::new(EmitterStats::default()),
            Pos::new(raw_start.get().max(from)),
            Pos::new(aligned.get().max(from)),
        );
        boot.expect_log = true;
        boot.min_commit_lsn = state.start_lsn;
        boot.activation_opt_ins = start_opt_ins(&state.start_tables);
        let (t, sink) = open_tenant(boot, None).await?;
        if t.db_oid != state.db_oid {
            let (id, now, was) = (t.id.clone(), t.db_oid, state.db_oid);
            t.abandon();
            anyhow::bail!(
                "tenant {id}: database {} has oid {now}, not {was}: it was recreated; \
                 detach and re-attach the tenant",
                decl.dbname
            );
        }
        if let Some(r) = &t.config_resolver {
            reloader.set_tenant_resolver(&t.id, Some(r.clone())).await;
        }
        tracing::info!(
            target: "walshadow::tenant",
            tenant = %t.id,
            dbname = %t.dbname,
            db_oid = t.db_oid,
            attached = %format_pg_lsn(from),
            start = %format_pg_lsn(state.start_lsn),
            "tenant resumed",
        );
        Ok(Some((t, sink, from)))
    }

    /// Attach at `p0`, where the pump is paused and shadow has replayed:
    /// fresh state, descriptor history seeded from shadow at `p0`, nothing
    /// in scope until priming ends. Resolves the start point in the
    /// background
    #[allow(clippy::too_many_arguments)]
    pub async fn attach(
        &mut self,
        shared: &SessionShared,
        args: &Args,
        merged: &toml::Table,
        tenants: &walshadow::tenants::TenantsConfig,
        decl: &walshadow::tenants::TenantDecl,
        p0: u64,
        reloader: &Arc<walshadow::control::Reloader>,
    ) -> Result<(Tenant, BoundaryHoldSink)> {
        use walshadow::tenants::{Phase, TenantState, tenant_dir};
        let dir = tenant_dir(&args.spill_dir, &decl.id);
        // A fresh attachment owns none of an earlier one's history or ledgers
        if tokio::fs::try_exists(&dir).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&dir)
                .await
                .with_context(|| format!("clear tenant dir {}", dir.display()))?;
        }
        let emitter = shared.tenant_emitter(args, merged, tenants, decl).await?;
        let at = Pos::new(p0);
        let mut boot = shared.boot(
            args,
            decl.id.clone(),
            emitter,
            dir.clone(),
            Arc::new(EmitterStats::default()),
            at,
            at,
        );
        boot.priming = true;
        let mut cfg = boot.source_conn.to_pg_config();
        cfg.database = decl.dbname.clone();
        let (t, sink) = open_tenant(boot, None).await?;
        let mut state = TenantState::new(&decl.id, &decl.dbname, t.db_oid);
        state.phase = Phase::Priming;
        state.attached_lsn = p0;
        state.store(&dir).await?;
        if let Some(r) = &t.config_resolver {
            reloader.set_tenant_resolver(&t.id, Some(r.clone())).await;
        }
        self.priming.insert(
            decl.id.clone(),
            Priming {
                attached_lsn: p0,
                start: spawn_start_resolution(cfg.clone()),
                cfg,
                since: Instant::now(),
                logged: Instant::now(),
            },
        );
        tracing::info!(
            target: "walshadow::tenant",
            tenant = %decl.id,
            dbname = %decl.dbname,
            db_oid = t.db_oid,
            attached = %format_pg_lsn(p0),
            "tenant attached; priming past transactions older than the attachment",
        );
        Ok((t, sink))
    }

    /// Tenants whose start point is known and which the pump has passed
    pub fn primed(&mut self, pump_lsn: u64) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        for (id, p) in &mut self.priming {
            match p.start.try_recv() {
                Ok(Ok(s)) if pump_lsn >= s => out.push((id.clone(), s)),
                Ok(Ok(s)) => {
                    // Keep it until the pump passes: re-arm a resolved channel
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let _ = tx.send(Ok(s));
                    p.start = rx;
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        target: "walshadow::tenant",
                        tenant = %id,
                        error = %format!("{e:#}"),
                        "resolving the start point failed; retrying",
                    );
                    p.start = spawn_start_resolution(p.cfg.clone());
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    if p.logged.elapsed() >= Duration::from_secs(60) {
                        p.logged = Instant::now();
                        tracing::warn!(
                            target: "walshadow::tenant",
                            tenant = %id,
                            attached = %format_pg_lsn(p.attached_lsn),
                            waited = ?p.since.elapsed(),
                            "still waiting for transactions older than the attachment \
                             (long-running or prepared transactions hold it)",
                        );
                    }
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    p.start = spawn_start_resolution(p.cfg.clone());
                }
            }
        }
        for (id, _) in &out {
            self.priming.remove(id);
        }
        out
    }

    /// Persist `reason` as the tenant's detached state
    pub async fn record_detached(
        &mut self,
        spill_dir: &Path,
        decl: &walshadow::tenants::TenantDecl,
        reason: String,
    ) {
        use walshadow::tenants::{Phase, TenantState, tenant_dir};
        self.forget(&decl.id);
        let dir = tenant_dir(spill_dir, &decl.id);
        let mut state = TenantState::load(&dir)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| TenantState::new(&decl.id, &decl.dbname, 0));
        state.phase = Phase::Detached;
        state.reason = Some(reason);
        if let Err(e) = state.store(&dir).await {
            tracing::error!(
                target: "walshadow::tenant",
                tenant = %decl.id,
                error = %format!("{e:#}"),
                "could not persist detached state",
            );
        }
    }
}

/// `S`: once every transaction assigned before the attachment has finished,
/// the WAL insert position. Every commit after `S` belongs to a transaction
/// that started after routing began, so the tenant saw all of it
fn spawn_start_resolution(cfg: PgConfig) -> tokio::sync::oneshot::Receiver<Result<u64>> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = tx.send(resolve_start_lsn(&cfg).await);
    });
    rx
}

async fn resolve_start_lsn(cfg: &PgConfig) -> Result<u64> {
    let client = walshadow::source_feed::open_sql_client(cfg).await?;
    // xid8: 64-bit, so the comparison cannot wrap
    let horizon: i64 = client
        .query_one(
            "SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint",
            &[],
        )
        .await?
        .get(0);
    loop {
        let row = client
            .query_one(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint, \
                 pg_current_wal_lsn()::text",
                &[],
            )
            .await?;
        let xmin: i64 = row.get(0);
        if xmin >= horizon {
            let lsn: String = row.get(1);
            return walshadow::pg::parse_pg_lsn(&lsn).map_err(|e| anyhow::anyhow!("{e}"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Decide a primed tenant's start scope from its full config and publish it:
/// every in-scope table opts in with its initial load, bounded at the next
/// barrier past `start`. Persisted before publication, so a restart resumes
/// unfinished loads
pub(crate) async fn activate(
    t: &Tenant,
    args: &Args,
    merged: &toml::Table,
    decl: &walshadow::tenants::TenantDecl,
    start: u64,
) -> Result<usize> {
    use walshadow::tenants::{Phase, StartTable, TenantState};
    t.xact_buffer.lock().await.set_min_commit_lsn(start);
    let mut tables = Vec::new();
    if let Some(resolver) = &t.config_resolver {
        let full = EmitterConfig::from_table(&decl.effective_table(merged))
            .with_context(|| format!("tenant {}: config", t.id))?;
        let snap = resolver.preview(&full).await;
        let schema = full.runtime_config_schema.as_deref();
        for rel in t.desc_log.user_rel_names_at(start, schema) {
            let rule = snap.rules.settings(&rel);
            let explicit = snap.table_opt_ins.get(&rel);
            let replicate = explicit
                .and_then(|row| row.replicate)
                .or(rule.replicate)
                .unwrap_or(
                    snap.replicate_all
                        && !walshadow::ch_ddl::is_system_namespace(&rel.namespace, schema),
                );
            if !replicate {
                continue;
            }
            let initial_load = explicit
                .and_then(|row| row.initial_load.clone())
                .or(rule.initial_load)
                .unwrap_or_else(|| decl.initial_load.clone());
            tables.push(StartTable {
                namespace: rel.namespace.to_string(),
                name: rel.name.to_string(),
                initial_load,
            });
        }
        tables.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    }
    prewarm_start_tables(t, &tables).await;
    let mut state = TenantState::load(&t.dir)
        .await?
        .unwrap_or_else(|| TenantState::new(&t.id, &t.dbname, t.db_oid));
    state.phase = Phase::Active;
    state.start_lsn = start;
    state.start_tables = tables.clone();
    state.reason = None;
    state.store(&t.dir).await?;
    if let Some(resolver) = &t.config_resolver {
        resolver.set_activation(walshadow::config::Activation {
            priming: false,
            opt_ins: start_opt_ins(&tables),
        });
        resolver
            .reload()
            .await
            .map_err(|e| anyhow::anyhow!("tenant {}: publish start scope: {e}", t.id))?;
    }
    let _ = args;
    tracing::info!(
        target: "walshadow::tenant",
        tenant = %t.id,
        start = %format_pg_lsn(start),
        tables = tables.len(),
        "tenant primed; start scope published",
    );
    Ok(tables.len())
}

/// Progress across every attached tenant: the session's floor is the least
/// any of them can resume from
pub(crate) struct Progress {
    pub drain: Pos<walshadow::pos::Drain>,
    pub resume_safe: Pos<walshadow::pos::ResumeSafe>,
    pub emitter_ack: Pos<EmitterAck>,
    pub open_xacts: usize,
}

/// With no tenant, nothing needs WAL past what the filter made durable
pub(crate) async fn aggregate(tenants: &[Tenant], durable: Pos<FilterDurable>) -> Progress {
    let mut out = Progress {
        drain: Pos::new(durable.get()),
        resume_safe: Pos::new(durable.get()),
        emitter_ack: Pos::new(durable.get()),
        open_xacts: 0,
    };
    let mut first = true;
    for t in tenants {
        let mut b = t.xact_buffer.lock().await;
        let ea = t.emitter_ack.get();
        // Read acknowledgment first so no transaction escapes floor
        let safe = b.resume_safe_lsn(ea);
        let stats = b.stats();
        if first {
            out.drain = stats.drain_lsn;
            out.resume_safe = safe;
            out.emitter_ack = ea;
            first = false;
        } else {
            out.drain = out.drain.min(stats.drain_lsn);
            out.resume_safe = out.resume_safe.min(safe);
            out.emitter_ack = out.emitter_ack.min(ea);
        }
        out.open_xacts += stats.xacts_active as usize;
    }
    out
}

/// First tenant whose ack is held by buffered transactions or unfinished
/// pipeline work, with its ack snapshot
pub(crate) async fn pinning(tenants: &[Tenant]) -> Option<(&Tenant, AckSnapshot)> {
    for t in tenants {
        let ack = *t.ack_probe.borrow();
        let active = t.xact_buffer.lock().await.stats().xacts_active;
        if active > 0 || !ack.all_done() || ack.wedged != 0 {
            return Some((t, ack));
        }
    }
    None
}

/// Take a tenant out of the session: stop routing to it, stop following its
/// database, stop its pipeline (drained when `graceful` and it finishes
/// within `drain_limit`), and persist it detached with `reason`
#[allow(clippy::too_many_arguments)]
pub(crate) async fn detach(
    id: &str,
    reason: String,
    graceful: bool,
    drain_limit: Duration,
    tenants: &mut Vec<Tenant>,
    router: &mut walshadow::tenant_router::TenantRouter,
    stream: &mut WalStream,
    supervisor: &mut Supervisor,
    reloader: &walshadow::control::Reloader,
    spill_dir: &Path,
    decl: Option<&walshadow::tenants::TenantDecl>,
) {
    let routed = router.detach(id);
    reloader.set_tenant_resolver(id, None).await;
    let Some(at) = tenants.iter().position(|t| t.id == id) else {
        supervisor.forget(id);
        return;
    };
    let t = tenants.remove(at);
    for db in &t.db_oids {
        stream.filter_mut().remove_target_db(*db);
    }
    let dbname = t.dbname.clone();
    let sink = routed.map(|r| r.sink);
    if graceful {
        match tokio::time::timeout(drain_limit, t.shutdown(sink)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(
                target: "walshadow::tenant",
                tenant = %id,
                error = %format!("{e:#}"),
                "tenant drain failed while detaching",
            ),
            Err(_) => tracing::warn!(
                target: "walshadow::tenant",
                tenant = %id,
                "tenant drain exceeded the stall limit while detaching; abandoned",
            ),
        }
    } else {
        drop(sink);
        t.abandon();
    }
    let fallback;
    let decl = match decl {
        Some(d) => d,
        None => {
            fallback = walshadow::tenants::TenantDecl {
                id: id.to_string(),
                dbname,
                desired: walshadow::tenants::Desired::Detached,
                max_lag_bytes: None,
                initial_load: "copy".into(),
                body: toml::Table::new(),
            };
            &fallback
        }
    };
    tracing::warn!(
        target: "walshadow::tenant",
        tenant = %id,
        reason = %reason,
        "tenant detached; it no longer holds WAL, and re-attaching resyncs it",
    );
    supervisor.record_detached(spill_dir, decl, reason).await;
}

/// What a reload changes in the tenant set
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ReconcilePlan {
    pub detach: Vec<(String, String)>,
    pub attach: Vec<String>,
}

/// Diff the old and new declarations against the attached tenants. A
/// changed database or destination identity detaches and re-attaches: the
/// tenant's durable state is bound to both
pub(crate) fn reconcile_plan<'a>(
    old: &walshadow::tenants::TenantsConfig,
    next: &walshadow::tenants::TenantsConfig,
    old_root: &toml::Table,
    next_root: &toml::Table,
    attached: impl Iterator<Item = &'a str>,
) -> ReconcilePlan {
    use walshadow::tenants::Desired;
    let attached: std::collections::BTreeSet<&str> = attached.collect();
    let mut plan = ReconcilePlan::default();
    for id in &attached {
        match next.get(id) {
            None => plan
                .detach
                .push((id.to_string(), "removed from config".into())),
            Some(d) if d.desired == Desired::Detached => plan
                .detach
                .push((id.to_string(), "detached by operator".into())),
            Some(d) => {
                let rebind = old
                    .get(id)
                    .map(|o| {
                        o.identity_differs_across(d, old_root, next_root)
                            .unwrap_or(true)
                    })
                    .unwrap_or(false);
                if rebind {
                    plan.detach.push((
                        id.to_string(),
                        "database or destination identity changed; re-attaching".into(),
                    ));
                    plan.attach.push(id.to_string());
                }
            }
        }
    }
    for d in &next.decls {
        if d.desired == Desired::Active && !attached.contains(d.id.as_str()) {
            plan.attach.push(d.id.clone());
        }
    }
    plan
}

/// Databases shadow should serve bridges for
pub(crate) fn wanted_databases(tenants: &walshadow::tenants::TenantsConfig) -> Vec<String> {
    tenants
        .decls
        .iter()
        .filter(|d| d.desired == walshadow::tenants::Desired::Active)
        .map(|d| d.dbname.clone())
        .collect()
}

/// Wait until shadow replayed through `lsn`: a tenant's descriptor history
/// is seeded from shadow's catalog, which must already show every catalog
/// change before the attachment
pub(crate) async fn wait_shadow_replay(
    shadow_state: &Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
    lsn: u64,
    limit: Duration,
) -> Result<()> {
    let deadline = Instant::now() + limit;
    loop {
        {
            let mut state = shadow_state.lock().await;
            if state
                .aggregate()
                .min_apply_lsn
                .is_some_and(|applied| applied.get() >= lsn)
            {
                return Ok(());
            }
            state.request_status();
        }
        anyhow::ensure!(
            Instant::now() < deadline,
            "shadow did not replay to {} within {limit:?}",
            format_pg_lsn(lsn)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

impl Supervisor {
    pub fn is_priming(&self, id: &str) -> bool {
        self.priming.contains_key(id)
    }
}

/// Every declared tenant as the metrics endpoint and `ctl tenant list` see it
pub(crate) async fn metrics_view(
    tenants: &[Tenant],
    config: &walshadow::tenants::TenantsConfig,
    supervisor: &Supervisor,
    router: &walshadow::tenant_router::TenantRouter,
) -> Vec<walshadow::metrics::TenantMetrics> {
    let head = router.last_record_end();
    let mut out = Vec::with_capacity(config.decls.len());
    for decl in &config.decls {
        let Some(t) = tenants.iter().find(|t| t.id == decl.id) else {
            out.push(walshadow::metrics::TenantMetrics {
                id: decl.id.clone(),
                dbname: decl.dbname.clone(),
                phase: match decl.desired {
                    walshadow::tenants::Desired::Detached => "detached",
                    walshadow::tenants::Desired::Active => "pending",
                },
                ..Default::default()
            });
            continue;
        };
        let (xacts_active, resume_safe) = {
            let mut b = t.xact_buffer.lock().await;
            let safe = b.resume_safe_lsn(t.emitter_ack.get()).get();
            (b.stats().xacts_active, safe)
        };
        let emitted = t.emitter_stats.as_ref().map_or((0, 0), |s| {
            (
                s.rows_emitted.load(Ordering::Relaxed),
                s.backfill_copy_rows.load(Ordering::Relaxed),
            )
        });
        let queue_depth = router
            .tenants()
            .iter()
            .find(|r| r.id == t.id)
            .map_or(0, |r| r.sink.in_flight());
        out.push(walshadow::metrics::TenantMetrics {
            id: t.id.clone(),
            dbname: t.dbname.clone(),
            phase: if supervisor.is_priming(&t.id) {
                "priming"
            } else {
                "active"
            },
            ack_lsn: t.emitter_ack.get().get(),
            resume_safe_lsn: resume_safe,
            lag_bytes: head.saturating_sub(resume_safe),
            queue_depth,
            xacts_active,
            rows_emitted: emitted.0,
            backfill_copy_rows: emitted.1,
        });
    }
    out
}

/// Mirror the SQL registry into its config fragment every poll, reloading
/// when rows changed, so a row written straight into the table takes effect
pub(crate) fn spawn_registry_poller(
    config: PathBuf,
    cli_base: toml::Table,
    schema: String,
    poll: Duration,
    reloader: Arc<walshadow::control::Reloader>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let pass = async {
                let root = walshadow::ch_emitter::load_effective(&config, cli_base.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                walshadow::ops::tenant_ctl::mirror_registry(&config, &root, &schema).await
            };
            match pass.await {
                Ok(true) => {
                    if let Err(e) = reloader.reload().await {
                        tracing::warn!(target: "walshadow::tenant", error = %format!("{e:#}"), "reload after registry change failed");
                    }
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    target: "walshadow::tenant",
                    error = %format!("{e:#}"),
                    "tenant registry poll failed; keeping the last mirror",
                ),
            }
            tokio::time::sleep(poll).await;
        }
    })
}

/// Snowflake storage setup is metadata round trips; do it for every start
/// table concurrently rather than one per opt-in inside the coordinator. A
/// table loading its rows keeps its view unpublished until the load lands
async fn prepare_start_table(
    runtime: &walshadow::destination::snowflake::runtime::SnowflakeRuntime,
    desc: &walshadow::schema::RelDescriptor,
    initial_load: &str,
) -> Result<()> {
    if initial_load != "none" {
        runtime.defer_publication(desc)?;
    }
    runtime.ensure_table(desc).await
}

async fn prewarm_start_tables(t: &Tenant, tables: &[walshadow::tenants::StartTable]) {
    use futures::StreamExt;
    let Some(runtime) = t.snowflake.clone() else {
        return;
    };
    let started = Instant::now();
    let mut descs = Vec::with_capacity(tables.len());
    for table in tables {
        let rel = RelName::new(&table.namespace, &table.name);
        if let Ok(Some(desc)) = t.catalog.lock().await.descriptor_by_name(&rel).await {
            descs.push((desc, table.initial_load.clone()));
        }
    }
    let total = descs.len();
    let failed = futures::stream::iter(descs)
        .map(|(desc, mode)| {
            let runtime = runtime.clone();
            async move {
                prepare_start_table(&runtime, &desc, &mode)
                    .await
                    .inspect_err(|e| {
                        tracing::warn!(
                            target: "walshadow::tenant",
                            table = %desc.rel_name,
                            error = %format!("{e:#}"),
                            "start table prewarm failed; its opt-in retries",
                        )
                    })
                    .is_err()
            }
        })
        .buffer_unordered(runtime.config.metadata_concurrency)
        .filter(|failed| std::future::ready(*failed))
        .count()
        .await;
    tracing::info!(
        target: "walshadow::tenant",
        tenant = %t.id,
        tables = total,
        failed,
        elapsed_secs = started.elapsed().as_secs_f64(),
        "start tables prewarmed",
    );
}
