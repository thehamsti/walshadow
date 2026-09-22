//! One tenant's half of a session: everything bound to a single source
//! database. The pump, filter, shadow, slot and manifest stay shared in
//! `run_session`; each tenant brings its own shadow catalog and bridge
//! connection, descriptor log, transaction buffer, pipeline, destination and
//! state directory, fed by the [`TenantRouter`](walshadow::tenant_router).
//!
//! The single-database layout is one tenant, [`LEGACY_TENANT`], whose
//! directory is the spill dir itself: its code path here is the one the
//! daemon always ran

use super::*;

use walshadow::desc_log::DescriptorLog;
use walshadow::filter::shadow_relations::ShadowHeld;
use walshadow::pipeline::PipelineHandle;
use walshadow::pipeline::ack::AckSnapshot;
use walshadow::tenants::LEGACY_TENANT;

/// Everything a tenant needs from the session to open
pub(super) struct TenantBoot<'a> {
    pub args: &'a Args,
    pub id: String,
    pub dbname: String,
    pub dir: PathBuf,
    /// Parsed tenant config with its destination opened; `None` runs the
    /// metrics-only null tail
    pub emitter: Option<EmitterConfig>,
    pub emitter_stats: Arc<EmitterStats>,
    /// Source connection pointed at the tenant's database
    pub source_cfg: PgConfig,
    pub sysid: String,
    pub source_major: u32,
    pub source_version_num: i32,
    pub start_timeline: u32,
    pub lineage: Vec<u32>,
    /// Where the pump resumes (boot) or the attachment position (attach)
    pub raw_start: Pos<Floor>,
    pub aligned: Pos<Floor>,
    /// Prior progress exists, so the descriptor log must too
    pub expect_log: bool,
    /// `--ignore-cursor`: discard the descriptor log
    pub discard_log: bool,
    pub start_lsn_override: Option<Pos<Floor>>,
    pub history_rx: watch::Receiver<Arc<TimelineHistory>>,
    pub shadow_state: Arc<Mutex<walshadow::shadow_stream::ShadowStreamState>>,
    pub smgr_markers: Arc<std::sync::Mutex<walshadow::filter::SmgrMarkers>>,
    pub xid_ceiling: Arc<walshadow::toast::xid_ceiling::XidCeiling>,
    pub bridge_path: PathBuf,
    pub bridge_workers: usize,
    pub resume_floor: Arc<Monotone<Floor>>,
    /// Control-socket reloads go to this tenant's resolver (single-database)
    pub reloader: Option<Arc<walshadow::control::Reloader>>,
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
pub(super) struct SessionShared {
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
    /// A boot with the session's defaults, pointed at `dbname`
    #[allow(clippy::too_many_arguments)]
    pub fn boot<'a>(
        &self,
        args: &'a Args,
        id: String,
        dbname: String,
        dir: PathBuf,
        emitter: Option<EmitterConfig>,
        emitter_stats: Arc<EmitterStats>,
        raw_start: Pos<Floor>,
        aligned: Pos<Floor>,
    ) -> TenantBoot<'a> {
        let mut source_cfg = self.source_conn.to_pg_config();
        source_cfg.database = dbname.clone();
        let tenant_workers = TENANT_BRIDGES.get().map_or(2, |&(w, _)| w);
        TenantBoot {
            args,
            bridge_path: walshadow::shadow::tenant_bridge_socket(
                &args.bridge_socket_path(),
                &dbname,
            ),
            bridge_workers: tenant_workers,
            id,
            dbname,
            dir,
            emitter,
            emitter_stats,
            source_cfg,
            sysid: self.sysid.clone(),
            source_major: self.source_major,
            source_version_num: self.source_version_num,
            start_timeline: self.start_timeline,
            lineage: self.lineage.clone(),
            raw_start,
            aligned,
            expect_log: false,
            discard_log: false,
            start_lsn_override: None,
            history_rx: self.history_rx.clone(),
            shadow_state: self.shadow_state.clone(),
            smgr_markers: self.smgr_markers.clone(),
            xid_ceiling: self.xid_ceiling.clone(),
            resume_floor: self.resume_floor.clone(),
            reloader: None,
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
    ) -> Result<Option<EmitterConfig>> {
        let effective = decl.effective_table(merged);
        let destination = walshadow::destination::config::DestinationConfig::from_table(&effective)
            .with_context(|| format!("tenant {}: destination", decl.id))?;
        let cfg = build_emitter_config(
            args,
            &effective,
            destination,
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
        Ok(cfg)
    }
}

/// Name every tenant database to shadow, whose launcher starts or stops their
/// bridge pools on reload. An external shadow is the operator's to configure
pub(super) async fn publish_tenant_bridges(
    lifecycle: Option<&ShadowLifecycle>,
    dbnames: Vec<String>,
) -> Result<()> {
    let Some(lifecycle) = lifecycle else {
        return Ok(());
    };
    let shadow = lifecycle.shadow.clone();
    tokio::task::spawn_blocking(move || shadow.set_tenant_databases(&dbnames))
        .await
        .context("tenant bridge list task")?
        .context("publish tenant databases to shadow")?;
    Ok(())
}

/// Start tables as the resolver's opt-in rows
pub(super) fn start_opt_ins(
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

pub(super) struct Tenant {
    pub id: String,
    pub dbname: String,
    pub db_oid: u32,
    pub dir: PathBuf,
    /// Shadow catalog session in the tenant's database
    pub catalog: Arc<Mutex<ShadowCatalog>>,
    pub bridge: Arc<walshadow::bridge::Bridge>,
    pub oracle: Option<Arc<walshadow::oracle::Oracle>>,
    pub xact_buffer: Arc<Mutex<XactBuffer>>,
    pub emitter_ack: Arc<Monotone<EmitterAck>>,
    pub desc_log: Arc<DescriptorLog>,
    pub decoder_stats: Arc<walshadow::decoder_sink::DecoderStats>,
    pub emitter_stats: Option<Arc<EmitterStats>>,
    pub capture_stats: Arc<walshadow::catalog_capture::CaptureStats>,
    pub boundary_hold_stats: Arc<BoundaryHoldStats>,
    pub pipeline: Option<PipelineHandle>,
    pub ack_probe: watch::Receiver<AckSnapshot>,
    pub config_resolver: Option<Arc<ConfigResolver>>,
    pub copy_backfiller: Option<Arc<walshadow::copy_backfill::CopyBackfiller>>,
    pub snowflake: Option<Arc<walshadow::destination::snowflake::runtime::SnowflakeRuntime>>,
    pub span_registry: Option<walshadow::trace::TxnSpanRegistry>,
    /// Pruners' floor: the session joins every persisted resume floor
    gc_floor: Option<Monotone<Floor>>,
    gc_task: Option<tokio::task::JoinHandle<()>>,
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

    /// First fatal error the tenant's pipeline or pruner raised
    pub fn fatal(&self) -> Option<String> {
        self.pipeline
            .as_ref()
            .and_then(|p| p.fatal.message())
            .or_else(|| self.gc_fatal.message())
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
        drop(self.gc_floor.take());
        if let Some(task) = self.gc_task.take() {
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
        if let Some(task) = self.gc_task.take() {
            task.abort();
        }
        drop(self.pipeline.take());
    }
}

/// Open a tenant: connect its catalog and bridge, open its descriptor log,
/// buffer and pipeline, and return the hold sink the router feeds
pub(super) async fn open_tenant(boot: TenantBoot<'_>) -> Result<(Tenant, BoundaryHoldSink)> {
    let TenantBoot {
        args,
        id,
        dbname,
        dir,
        emitter,
        emitter_stats,
        source_cfg,
        sysid,
        source_major,
        source_version_num,
        start_timeline,
        lineage,
        raw_start,
        aligned,
        expect_log,
        discard_log,
        start_lsn_override,
        history_rx,
        shadow_state,
        smgr_markers,
        xid_ceiling,
        bridge_path,
        bridge_workers,
        resume_floor,
        reloader,
        shadow_toast_held,
        decoder_batch_size,
        decoder_queue_capacity,
        span_tracing,
        min_commit_lsn,
        priming,
        activation_opt_ins,
    } = boot;
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("create tenant dir {}", dir.display()))?;
    let legacy = id == LEGACY_TENANT;

    // Connect bridge and shadow catalog before START_REPLICATION so the
    // tracker→drain wire is hot from the first record.
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &dbname,
    );
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    let bridge = Arc::new(
        walshadow::bridge::connect_with_budget(&bridge_path, bridge_workers, connect_budget)
            .await
            .with_context(|| {
                format!(
                    "tenant {id}: connect bridge at {} for database {dbname}",
                    bridge_path.display()
                )
            })?,
    );
    let info = bridge.info();
    tracing::info!(
        target: "walshadow::bridge",
        tenant = %id,
        socket = %bridge_path.display(),
        workers = bridge.pool_size(),
        pg_version = info.map(|i| i.pg_version_num).unwrap_or(0),
        in_recovery = info.map(|i| i.in_recovery).unwrap_or(false),
        "bridge connected",
    );
    let cat_cfg = ShadowCatalogConfig::default();
    let backoff_initial = cat_cfg.reconnect_backoff_initial;
    let backoff_max = cat_cfg.reconnect_backoff_max;
    let catalog = with_transient_retry(connect_budget, backoff_initial, backoff_max, async || {
        ShadowCatalog::connect(&shadow_conninfo, cat_cfg.clone(), bridge.clone()).await
    })
    .await
    .with_context(|| format!("tenant {id}: connect to shadow PG database {dbname}"))?;
    let catalog = Arc::new(Mutex::new(catalog));
    tracing::info!(
        target: "walshadow",
        tenant = %id,
        socket = %args.shadow_socket_dir.display(),
        port = args.shadow_port,
        user = %args.shadow_user,
        dbname = %dbname,
        "shadow connected",
    );
    // The tenant's own sidecar: runtime-config seeds and preflight read the
    // tenant's database, not the admin one the pump connects to
    let source_sql = open_sql_client_waiting(&source_cfg, connect_budget)
        .await
        .with_context(|| format!("tenant {id}: source SQL session on database {dbname}"))?;

    if !args.skip_preflight {
        let shadow_sql = open_shadow_sql_client(
            &args.shadow_socket_dir,
            args.shadow_port,
            &args.shadow_user,
            &dbname,
        )
        .await?;
        let report = walshadow::preflight::run(walshadow::preflight::Inputs {
            source_version_num,
            source_sql: &source_sql,
            shadow_sql: &shadow_sql,
            // The pump's slot is checked once by the session
            slot: None,
            ch_config: emitter.as_ref(),
        })
        .await
        .with_context(|| format!("tenant {id}: pre-flight probe"))?;
        report
            .into_result()
            .with_context(|| format!("tenant {id}: pre-flight rejected"))?;
        tracing::info!(target: "walshadow::preflight", tenant = %id, "pre-flight passed");
    }

    let oracle = Some(Arc::new(
        walshadow::oracle::Oracle::new(bridge.clone()).with_xid_ceiling(xid_ceiling),
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

    let db_oid = catalog
        .lock()
        .await
        .current_database_oid()
        .await
        .context("shadow database oid")?;
    let pending_cfg = emitter
        .as_ref()
        .map(|c| c.pending_capture)
        .unwrap_or_default();
    let pending_catalog = Arc::new(walshadow::pending::PendingCatalog::default());
    // A resumed manifest implies prior progress whose records the log must
    // cover; an empty/missing log there means it was lost — decode would
    // read uncovered intervals. `--ignore-cursor` discards both.
    let log_files_present = dir.join(walshadow::desc_log::TAIL_FILE).exists()
        || dir.join(walshadow::desc_log::CKPT_FILE).exists();
    anyhow::ensure!(
        !expect_log || log_files_present || discard_log,
        "tenant {id}: progress recorded but descriptor log missing in {}; \
         re-bootstrap, re-attach the tenant, or pass --ignore-cursor",
        dir.display(),
    );
    if discard_log {
        for f in [
            walshadow::desc_log::CKPT_FILE,
            walshadow::desc_log::TAIL_FILE,
        ] {
            let _ = tokio::fs::remove_file(dir.join(f)).await;
        }
    }
    let desc_log = Arc::new(
        DescriptorLog::open_on_branch(
            &dir,
            walshadow::desc_log::DescLogIdentity {
                pg_major: source_major,
                system_id: sysid.clone(),
                // Resume branch, which a crossing moves without moving the log:
                // the stored header names wherever the log last rewrote itself,
                // so `lineage` is what places it
                timeline: start_timeline,
                db_oid,
                wal_seg_size: WAL_SEG_SIZE as u32,
            },
            &lineage,
        )
        .await
        .with_context(|| format!("tenant {id}: open descriptor log"))?,
    );
    if let Some(lsn) = start_lsn_override {
        anyhow::ensure!(
            lsn >= desc_log.floor_at_write(),
            "--start-lsn {} below descriptor log floor {}; no shape history \
             survives there — --ignore-cursor or re-bootstrap",
            lsn,
            desc_log.floor_at_write(),
        );
        let head = desc_log.head();
        anyhow::ensure!(
            head == 0 || lsn.get() <= head,
            "--start-lsn {} beyond descriptor log head {}; boundaries in \
             between were never captured — --ignore-cursor re-baselines",
            lsn,
            format_pg_lsn(head),
        );
    }
    if desc_log.is_empty() {
        // Baseline snapshot: every eligible rel as of shadow's position,
        // valid from the aligned start so the prefix re-read decodes
        // (newest-shape reader of older tuples — the safe bias direction).
        // Boundaries at or below covered_through are baked in and skip.
        let (replay_lsn, descs) = catalog
            .lock()
            .await
            .fetch_all_descriptors()
            .await
            .context("descriptor log boot seed")?;
        let covered_through = raw_start.get().max(replay_lsn);
        let entries = descs
            .into_iter()
            .map(|d| {
                Arc::new(walshadow::desc_log::LogEntry {
                    valid_from: aligned.get(),
                    oid: d.oid,
                    rfn: d.rfn,
                    value: walshadow::desc_log::LogValue::Present(Arc::new(d)),
                })
            })
            .collect();
        desc_log
            .seed(
                walshadow::desc_log::BatchRecord {
                    captured_at: covered_through,
                    commit_lsn: 0,
                    observations: Vec::new(),
                    ambiguities: Vec::new(),
                    entries,
                },
                covered_through,
            )
            .await
            .context("seed descriptor log")?;
        tracing::info!(
            target: "walshadow::desc_log",
            tenant = %id,
            covered_through = format_pg_lsn(covered_through).to_string(),
            "descriptor log seeded",
        );
    }

    // Txn-span registry, shared by pump + decoder; `Some` only with OTLP on.
    let span_registry = if span_tracing {
        Some(xact_buffer.lock().await.span_registry())
    } else {
        None
    };
    let mut decoder = BufferingDecoderSink::new(desc_log.clone(), xact_buffer.clone());
    if let Some(schema) = emitter
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
    let retires = walshadow::toast_retire::RetireLedger::load(&dir)
        .await
        .context("load toast retire ledger")?;
    // Pending tables a bootstrap or backup pass left holding undecided rows.
    // Settling needs ClickHouse, so a metrics-only run leaves the ledger for
    // a later CH run over the same spill dir
    let pending_rows = walshadow::visibility_pending::PendingLedger::load(&dir)
        .await
        .context("load pending visibility ledger")?;
    let mut config_resolver: Option<Arc<ConfigResolver>> = None;
    let mut copy_backfiller: Option<Arc<walshadow::copy_backfill::CopyBackfiller>> = None;

    let snowflake = emitter.as_ref().and_then(|c| c.snowflake.clone());
    let pcfg = if let Some(mut emitter_cfg) = emitter {
        let activation = walshadow::config::Activation {
            priming,
            opt_ins: activation_opt_ins,
        };
        if priming {
            // Nothing in scope until every transaction the tenant saw only
            // part of has finished; the session then publishes its tables
            emitter_cfg.replicate_all = false;
            emitter_cfg.table_opt_ins.clear();
            emitter_cfg.table_initial_loads.clear();
            emitter_cfg.table_entries.clear();
        }
        for (rel, row) in &activation.opt_ins {
            emitter_cfg
                .table_opt_ins
                .entry(rel.clone())
                .or_insert_with(|| row.clone());
        }
        let addr = format!("{}:{}", emitter_cfg.host, emitter_cfg.port);
        // Live routing map shared by DDL applicator + route planning. The
        // refresher below rewrites it on every republished snapshot.
        let mapping = walshadow::mapping::mapping_handle(emitter_cfg.tables.clone());
        // Resolver merges CLI over TOML and publishes ResolvedConfig on
        // the watch substrate; SIGHUP re-reads TOML and republishes. The
        // mapping refresher + DDL applicator subscribe.
        let cli_overrides = CliOverrides {
            drop_table_strategy: args.drop_table_strategy,
            flush_timeout: args
                .ch_flush_timeout_ms
                .map(std::time::Duration::from_millis),
            source_slot: args.slot.clone(),
        };
        let (resolver, config_rx) = ConfigResolver::new(
            &emitter_cfg,
            cli_overrides,
            args.ch_config.clone(),
            cli_base(args),
            mapping.clone(),
        );
        if !legacy {
            resolver.bind_tenant(&id);
        }
        resolver.set_activation(activation);
        if let Some(held) = &shadow_toast_held {
            resolver.bind_shadow_toast(held.clone());
            // Check configured tables here because they bypass opt-in
            // Preserve exclusions across SIGHUP reloads
            let descs = catalog
                .lock()
                .await
                .descriptors_by_name(emitter_cfg.tables.keys())
                .await?;
            for rel in
                walshadow::toast::shadow_landing::unserved_rels(&catalog, held, &descs).await?
            {
                resolver.exclude_table(&rel).await;
            }
        }
        if let Some(reloader) = &reloader {
            reloader.set_resolver(Some(resolver.clone())).await;
        }
        spawn_mapping_refresher(config_rx.clone(), mapping.clone());
        // Runtime-config overlay (§7): before the pump consumes WAL, seed the
        // resolver from source PG's config_* tables via the sidecar libpq
        // connection. Post-seed writes arrive live off the WAL stream. Refuse
        // to start if the named schema is not installed — explicit opt-in
        // means the operator expects the overlay present.
        let mut seeded_table_rows: Vec<(RelName, walshadow::runtime_config::TableRow)> = Vec::new();
        if let Some(schema) = emitter_cfg.runtime_config_schema.clone() {
            seeded_table_rows = seed_runtime_config(&source_sql, &schema, &resolver)
                .await
                .context("seed runtime config overlay")?;
            if priming {
                seeded_table_rows.clear();
            }
        }
        // Fold the resolved emitter knobs back onto the boot config so the
        // pipeline's initial batcher/inserter match the seeded + CLI values;
        // they track the watch channel live thereafter.
        {
            let rc = config_rx.borrow();
            emitter_cfg.row_budget = rc.row_budget;
            emitter_cfg.byte_budget = rc.byte_budget;
            emitter_cfg.flush_timeout = rc.flush_timeout;
            emitter_cfg.compression = rc.compression;
            emitter_cfg.retry.max_attempts = rc.retry_max_attempts;
        }
        // DDL applicator owned by the reorder coordinator so ALTER /
        // CREATE / DROP / TRUNCATE apply inside the barrier, after
        // earlier data is durable. Seeds DDL config from the resolved
        // snapshot; refreshes per apply as the resolver republishes.
        let ddl_cfg = walshadow::ch_ddl::DdlConfig::from_resolved(
            &config_rx.borrow(),
            emitter_cfg.database.clone(),
            emitter_cfg.soft_delete,
            emitter_cfg.system_columns.clone(),
            emitter_cfg.replicate_all,
            emitter_cfg.runtime_config_schema.clone(),
        );
        let mut applicator = walshadow::ch_ddl::DdlApplicator::new(
            &emitter_cfg,
            ddl_cfg,
            mapping.clone(),
            config_rx.clone(),
        )
        .await
        .context("init DDL applicator")?
        .with_resolver(resolver.clone())
        .with_oracle(oracle.clone());
        let stats = emitter_stats.clone();
        emitter_stats_handle = Some(stats.clone());
        // Backfiller for `initial_load` opt-ins (COPY / backup-sourced):
        // own source session + CH tail per backfill or pass, spill-dir
        // ledger dedups restarts. Wired whenever the emitter runs, since an
        // opt-in arriving later over the control socket or the overlay would
        // otherwise silently skip its backfill; idle it costs one ledger read.
        // One validated resident-payload pool for the pipeline and every
        // concurrent backup pass
        let pipeline_budget =
            walshadow::pipeline::build_budget(&emitter_cfg, emitter_cfg.decoder_pool_size)
                .map_err(|e| anyhow::anyhow!("memory budget: {e}"))?;
        copy_backfiller = Some(Arc::new(
            walshadow::copy_backfill::CopyBackfiller::new(
                source_cfg.clone(),
                emitter_cfg.clone(),
                mapping.clone(),
                stats.clone(),
                catalog.clone(),
                desc_log.clone(),
                &dir,
                Some(config_rx.clone()),
                history_rx,
                Some(pipeline_budget.clone()),
                oracle.clone(),
                source_major,
            )
            .await,
        ));
        let backfiller_effects: Option<Arc<dyn walshadow::opt_in::Backfiller>> =
            copy_backfiller.clone().map(|backfiller| backfiller as _);
        // Re-materialise per-table opt-in scope from the seeded config_table
        // rows. Live edits arrive off WAL via the reorder coordinator, but a
        // restart replays WAL from past these rows' commit LSN, so the seed
        // is the only chance to rebuild their scope (the CH tables persist).
        // `raw_start` is the backfill boundary S for a first-seen
        // `initial_load` row: COPY covers commits before it, WAL the rest;
        // the ledger resumes/no-ops rows seen on an earlier boot.
        prewarm_snowflake_opt_ins(
            &emitter_cfg,
            &mut applicator,
            &catalog,
            seeded_table_rows
                .iter()
                .filter(|(_, row)| !row.is_pattern())
                .map(|(rel, row)| (rel, row))
                .chain(emitter_cfg.table_opt_ins.iter()),
        )
        .await;
        let mut deferred = walshadow::opt_in::DeferredBackfills::default();
        for (rel, row) in &seeded_table_rows {
            if row.replicate.is_some() && !row.is_pattern() {
                walshadow::opt_in::apply_table_opt_in_deferred(
                    &resolver,
                    &mut applicator,
                    &catalog,
                    backfiller_effects.is_some(),
                    rel,
                    row,
                    raw_start.get(),
                    &mut deferred,
                )
                .await
                .with_context(|| format!("seed opt-in for {rel}"))?;
            }
        }
        for (rel, row) in &emitter_cfg.table_opt_ins {
            if row.replicate.is_some() {
                walshadow::opt_in::apply_table_opt_in_deferred(
                    &resolver,
                    &mut applicator,
                    &catalog,
                    backfiller_effects.is_some(),
                    rel,
                    row,
                    raw_start.get(),
                    &mut deferred,
                )
                .await
                .with_context(|| format!("config opt-in for {rel}"))?;
            }
        }
        let pattern_scoped: Vec<(RelName, walshadow::runtime_config::TableRow)> = {
            let snap = config_rx.borrow();
            let config_schema = emitter_cfg.runtime_config_schema.as_deref();
            snap.rules.pattern_scoped(
                || desc_log.user_rel_names_at(raw_start.get(), config_schema),
                |rel| snap.tables.contains_key(rel),
            )
        };
        for (rel, row) in &pattern_scoped {
            walshadow::opt_in::apply_table_opt_in_deferred(
                &resolver,
                &mut applicator,
                &catalog,
                backfiller_effects.is_some(),
                rel,
                row,
                raw_start.get(),
                &mut deferred,
            )
            .await
            .with_context(|| format!("pattern opt-in for {rel}"))?;
        }
        // Every mapping is in place: start the loads without each waiting on
        // the next opt-in's mapping publication
        deferred.start(backfiller_effects.as_ref()).await;
        let sql_scoped_tables: HashSet<RelName> = seeded_table_rows
            .iter()
            .filter(|(_, row)| row.replicate.is_some() && !row.is_pattern())
            .chain(pattern_scoped.iter())
            .map(|(rel, _)| rel.clone())
            .collect();
        let active_tables: HashSet<RelName> = config_rx.borrow().tables.keys().cloned().collect();
        apply_toml_initial_loads(
            &catalog,
            copy_backfiller.as_ref(),
            &emitter_cfg.table_initial_loads,
            &active_tables,
            &sql_scoped_tables,
            raw_start.get(),
        )
        .await?;
        // Baseline seeding suppresses the Added event for pinned mappings, so a
        // plain TOML mapping (no initial_load, no opt-in) would tail into a
        // missing CH table. Ensure those dests here; the others own their copy.
        let pinned = active_tables.iter().filter(|rel| {
            let has_initial_load = emitter_cfg
                .table_initial_loads
                .get(*rel)
                .and_then(|mode| mode.parse::<InitialLoadMode>().ok())
                .is_some_and(|m| m != InitialLoadMode::None);
            !sql_scoped_tables.contains(*rel) && !has_initial_load
        });
        let descs = catalog
            .lock()
            .await
            .descriptors_by_name(pinned)
            .await
            .context("resolve descriptors for pinned mappings")?;
        for desc in descs {
            let rel = desc.rel_name.clone();
            applicator
                .apply(&SchemaEvent::Added {
                    desc: Arc::new(desc),
                })
                .await
                .with_context(|| format!("ensure CH dest for pinned mapping {rel}"))?;
        }
        config_resolver = Some(resolver);
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
            resolvers = bridge.pool_size(),
            "parallel decode+insert pipeline starting",
        );
        PipelineConfig {
            emitter: emitter_cfg,
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            catalog: catalog.clone(),
            mapping,
            oracle: oracle.clone(),
            applicator: Some(applicator),
            tail: TailKind::ClickHouse,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            log: desc_log.clone(),
            pending: pending_catalog.clone(),
            stats: stats.clone(),
            span_registry: span_registry.clone(),
            config_resolver: config_resolver.clone(),
            backfiller: backfiller_effects,
            retires,
            pending_rows,
            resume_floor: resume_floor.clone(),
            budget: Some(pipeline_budget),
        }
    } else {
        // Metrics-only (no CH): the identical pipeline with a null tail —
        // zero CH connections, no DDL applicator, no oracle (nothing ships,
        // PgPending stays raw). The empty mapping routes nothing, so seqs
        // complete at placement and the watermark + slot advance move as in
        // a CH run. Emitter stats stay unexported (`emitter_stats_handle`
        // None), matching the old serial surface.
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
        PipelineConfig {
            emitter: EmitterConfig::default(),
            decoder_pool_size: decoders,
            inserter_pool_size: inserters,
            catalog: catalog.clone(),
            mapping: walshadow::mapping::mapping_handle(Default::default()),
            oracle: None,
            applicator: None,
            tail: TailKind::Null,
            buffer: xact_buffer.clone(),
            subxact_tracker: Arc::new(Mutex::new(SubxactTracker::new())),
            log: desc_log.clone(),
            pending: pending_catalog.clone(),
            stats: Arc::new(EmitterStats::default()),
            span_registry: span_registry.clone(),
            config_resolver: None,
            backfiller: None,
            retires,
            pending_rows: walshadow::visibility_pending::PendingLedger::empty(),
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
        .settle_pending_boot(args.bootstrap_shadow_data_dir.as_deref())
        .await
        .context("boot settle of pending backup rows")?;
    reorder_sink
        .apply_boot_events(desc_log.active_present_at(raw_start.get()), raw_start.get())
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
    let capture = walshadow::catalog_capture::CatalogCapture::new(
        desc_log.clone(),
        catalog.clone(),
        xact_buffer.clone(),
        smgr_markers,
        pending_catalog.clone(),
        pending_cfg,
    );
    let capture_stats = capture.stats_handle();
    let sink = BoundaryHoldSink::new(decoder_xact, boundary_gate).with_capture(capture);

    // Descriptor-log GC off the pump task: the session publishes each
    // persisted floor, the task compacts
    let gc_fatal = walshadow::pipeline::Fatal::new();
    let gc_floor = Monotone::<Floor>::default();
    let gc_task = spawn_desc_log_gc(desc_log.clone(), gc_floor.watch(), gc_fatal.clone());
    let snowflake_maintenance = snowflake
        .clone()
        .map(|runtime| spawn_snowflake_maintenance(runtime, gc_floor.watch()));

    Ok((
        Tenant {
            id,
            dbname,
            db_oid,
            dir,
            catalog,
            bridge,
            oracle,
            xact_buffer,
            emitter_ack,
            desc_log,
            decoder_stats: decoder_stats_handle,
            emitter_stats: emitter_stats_handle,
            capture_stats,
            boundary_hold_stats,
            pipeline: Some(pipeline_handle),
            ack_probe,
            config_resolver,
            copy_backfiller,
            snowflake,
            span_registry,
            gc_floor: Some(gc_floor),
            gc_task: Some(gc_task),
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
pub(super) struct Supervisor {
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
            decl.dbname.clone(),
            dir,
            emitter,
            Arc::new(EmitterStats::default()),
            Pos::new(raw_start.get().max(from)),
            Pos::new(aligned.get().max(from)),
        );
        boot.expect_log = true;
        boot.min_commit_lsn = state.start_lsn;
        boot.activation_opt_ins = start_opt_ins(&state.start_tables);
        let (t, sink) = open_tenant(boot).await?;
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
            decl.dbname.clone(),
            dir.clone(),
            emitter,
            Arc::new(EmitterStats::default()),
            at,
            at,
        );
        boot.priming = true;
        let cfg = boot.source_cfg.clone();
        let (t, sink) = open_tenant(boot).await?;
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
pub(super) async fn activate(
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
pub(super) struct Progress {
    pub drain: Pos<walshadow::pos::Drain>,
    pub resume_safe: Pos<walshadow::pos::ResumeSafe>,
    pub emitter_ack: Pos<EmitterAck>,
    pub open_xacts: usize,
}

/// With no tenant, nothing needs WAL past what the filter made durable
pub(super) async fn aggregate(tenants: &[Tenant], durable: Pos<FilterDurable>) -> Progress {
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
pub(super) async fn pinning<'a>(
    tenants: &'a [Tenant],
    _primary_stats: &walshadow::xact_buffer::XactBufferStats,
) -> Option<(&'a Tenant, AckSnapshot)> {
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
pub(super) async fn detach(
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
    stream.filter_mut().remove_target_db(t.db_oid);
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
pub(super) struct ReconcilePlan {
    pub detach: Vec<(String, String)>,
    pub attach: Vec<String>,
}

/// Diff the old and new declarations against the attached tenants. A
/// changed database or destination identity detaches and re-attaches: the
/// tenant's durable state is bound to both
pub(super) fn reconcile_plan<'a>(
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
pub(super) fn wanted_databases(tenants: &walshadow::tenants::TenantsConfig) -> Vec<String> {
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
pub(super) async fn wait_shadow_replay(
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
pub(super) async fn metrics_view(
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
pub(super) fn spawn_registry_poller(
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
