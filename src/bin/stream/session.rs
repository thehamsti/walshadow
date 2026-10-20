//! One streaming session — source connect through pump loop, shutdown, and
//! everything the status tick publishes along the way.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use backon::{ExponentialBuilder, Retryable};
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use walrus::pg::backup::format_pg_lsn;
use walshadow::archive::Archive;
use walshadow::boundary_hold::{AttachSlot, PendingDatabase, PendingDatabases};
use walshadow::ch_emitter::{EmitterConfig, EmitterStats};
use walshadow::config::{CliOverrides, ConfigResolver, SourceConn};
#[cfg(feature = "test-support")]
use walshadow::crash_test;
use walshadow::manifest;
use walshadow::metrics::{MetricsRegistry, RateEstimator};
use walshadow::pg::socket_conninfo;
use walshadow::pos::{
    EmitterAck, FilterDurable, Floor, Monotone, Pos, RawStart, ShadowFlush, ShadowReplay,
    SourceReceived,
};
use walshadow::queueing_record_sink::{
    DEFAULT_QUEUEING_BATCH_SIZE, DEFAULT_QUEUEING_RECORD_SINK_CAPACITY,
};
use walshadow::record::{MetricsRecordSink, WAL_SEG_SIZE};
use walshadow::retention::max_segment_end;
use walshadow::segment_sink::{DirSegmentSink, SegFsync};
use walshadow::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use walshadow::timeline::TimelineHistory;
use walshadow::transition::{
    CrossingState, ForkGuards, PrefixOrigin, Switchover, TimelineStats, load_boot_history,
    seed_shadow_branches,
};
use walshadow::wal_stream::WalStream;

use crate::args::{Args, TENANT_BRIDGES, build_emitter_config, cli_base, positive_usize};
use crate::bootstrap::{
    BootstrapHandoff, BootstrapMetrics, BootstrapObservers, ShadowStart, resolve_bootstrap,
    resolve_shadow_start, run_bootstrap,
};
use crate::housekeeping::{SEGMENT_FSYNC_QUEUE, spawn_segment_fsync, trim_retention};
use crate::metrics_publish::{
    DbMetricSources, DrainResident, ShadowMetricsView, SourceSwap, StageCounters, TimelineView,
    populate_metrics,
};
use crate::runtime_cfg::{or_signal, sighup_reload};
use crate::shadow_proc::{
    OwnedShadow, ShadowLifecycle, bridge_pool_size, build_owned_shadow, open_shadow_sql_client,
    probe_blocking, start_owned_shadow, walsender_primary_conninfo,
};
use crate::sinks::DaemonSinks;
use crate::source_db::{ATTACH_MARKER, list_databases};
use crate::source_recovery::{
    BARRIER_LOG_INTERVAL, FORK_FENCE_DRAIN, PROMOTION_POLL, PauseState, PromotionGate,
    ReconnectBackoff, ResumePoint, SOURCE_SWAP_RETRY, SourcePath, SourceRecovery,
    commit_fork_resume, connect_source_waiting, promotion_gate, resume_manifest,
    resume_source_feed, stream_branch, swap_reason,
};
use crate::tenant;

/// Stall limit without tenants: a single pipeline has always been allowed
/// to backpressure the pump indefinitely
const NO_STALL_LIMIT: Duration = Duration::from_secs(100 * 365 * 24 * 3600);

/// Wait out an unreachable ClickHouse at boot rather than exit, which leaves
/// a supervisor restart-looping the daemon until the destination returns
async fn ensure_boot_database_waiting(cfg: &EmitterConfig) -> Result<()> {
    (|| walshadow::ch_ddl::ensure_boot_database(cfg))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(10))
                .without_max_times(),
        )
        .when(walshadow::ch::is_retryable)
        .notify(|e, _| {
            tracing::warn!(
                target: "walshadow",
                error = %e,
                host = %cfg.conn.host,
                port = cfg.conn.port,
                "ClickHouse unreachable, waiting for it",
            );
        })
        .await?;
    Ok(())
}

/// Wait out tracked I/O on every exit, attach restart reopens same files
/// in-process
pub(crate) async fn run_session(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    sighup: tokio::signal::unix::Signal,
    shutdown: &CancellationToken,
    attach: &AttachSlot,
) -> Result<()> {
    let io = TaskTracker::new();
    let res = session_body(args, metrics, reloader, sighup, shutdown, attach, &io).await;
    io.close();
    io.wait().await;
    res
}

async fn session_body(
    args: &Args,
    metrics: &MetricsRegistry,
    reloader: &Arc<walshadow::control::Reloader>,
    sighup: tokio::signal::unix::Signal,
    shutdown: &CancellationToken,
    attach: &AttachSlot,
    io: &TaskTracker,
) -> Result<()> {
    // Clone the Arc-backed registry so the body's `&metrics` uses are unchanged.
    let metrics = metrics.clone();
    let mut tasks = SessionTasks::default();
    tasks.spawn("sighup reload", sighup_reload(sighup, reloader.clone()));

    let mut merged: toml::Table = match args.ch_config.as_deref() {
        Some(p) => walshadow::ch_emitter::load_effective(p, cli_base(args))
            .await
            .with_context(|| format!("load config {}", p.display()))?,
        None => cli_base(args),
    };
    // Applied source endpoint. Boot resolves it file-over-CLI; a later reload
    // republishes it on the config watch and the pump swaps its feed.
    let mut source_conn =
        SourceConn::from_table(&merged).map_err(|e| anyhow::anyhow!("[source] {e}"))?;
    if args.slot.is_some() {
        source_conn.slot = args.slot.clone();
    }
    let mut cfg = source_conn.to_pg_config();
    let mut feed = or_signal(
        shutdown,
        connect_source_waiting(args, &mut source_conn, &mut cfg),
    )
    .await?;

    let ident = feed.identify_system().await.context("IDENTIFY_SYSTEM")?;
    tracing::info!(
        target: "walshadow",
        sysid = %ident.sysid,
        timeline = ident.timeline,
        xlogpos = format_pg_lsn(ident.xlogpos).to_string(),
        "source identified",
    );

    walshadow::ops::tenant_ctl::validate(&merged)?;
    let mut tenants_cfg = walshadow::tenants::TenantsConfig::from_table(&merged)
        .context("parse [tenants] / [tenant.*]")?;
    if let Some(tcfg) = &tenants_cfg {
        let _ = TENANT_BRIDGES.set((tcfg.bridge_workers, tcfg.capacity));
        anyhow::ensure!(
            args.start_lsn.is_none(),
            "--start-lsn applies to the single-tenant layout only"
        );
    }
    let sysid: u64 = ident.sysid.parse().context("source system identifier")?;
    // Single-tenant layout: its config drives bootstrap too. With tenants
    // each opens its own below, and bootstrap builds shadow only
    let ch_config = match &tenants_cfg {
        None => build_emitter_config(args, &merged, sysid, &source_conn.dbname, None).await?,
        Some(_) => None,
    };
    if let Some(cfg) = ch_config.as_ref()
        && cfg.databases.len() > 1
    {
        tracing::info!(
            target: "walshadow::config",
            dbname = %cfg.source.dbname,
            databases = ?cfg.databases,
            "following several source databases",
        );
    }
    // Before anything dials CH naming that database in its handshake — the
    // bootstrap insert tail is first, and its failure there reads as a
    // bootstrap fault rather than a missing destination
    if let Some(cfg) = ch_config.as_ref().filter(|cfg| cfg.snowflake.is_none()) {
        or_signal(shutdown, ensure_boot_database_waiting(cfg)).await?;
    }
    // QueueingRecordSink knobs feed both the CH and metrics-only pipelines,
    // so resolve here while `ch_config` is still in scope (it is consumed
    // into `emitter_cfg` below). CLI over `[ch]` over the built-in default.
    let decoder_batch_size = positive_usize(
        "decoder_batch_size",
        args.decoder_batch_size,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_BATCH_SIZE, |c| c.decoder_batch_size),
    );
    let decoder_queue_capacity = positive_usize(
        "decoder_queue_capacity",
        args.decoder_queue_capacity,
        ch_config
            .as_ref()
            .map_or(DEFAULT_QUEUEING_RECORD_SINK_CAPACITY, |c| {
                c.decoder_queue_capacity
            }),
    );
    // Followed databases, in bridge socket order: `[source] dbname` plus
    // every database-prefixed config key
    let source_databases: Vec<String> = match ch_config.as_ref() {
        Some(cfg) if !cfg.databases.is_empty() => cfg.databases.clone(),
        _ => vec![source_conn.dbname.clone()],
    };
    anyhow::ensure!(
        source_databases.len() <= walshadow::bridge::MAX_BRIDGE_DATABASES,
        "config names {} source databases, the shadow bridge seats {}",
        source_databases.len(),
        walshadow::bridge::MAX_BRIDGE_DATABASES,
    );
    // Assign older TOAST mirrors and retire entries to sole followed database
    // TODO(0.2.0): remove with adopt_legacy_mirrors and legacy ledger load
    let legacy_owner: Option<u32> = if source_databases.len() == 1 {
        let row = feed
            .sql_client()
            .await?
            .query_one(
                "SELECT oid::oid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await
            .context("source database oid")?;
        Some(row.get(0))
    } else {
        None
    };
    if let Some(cfg) = ch_config
        .as_ref()
        .filter(|c| c.snowflake.is_none())
        .filter(|c| c.toast.mode == walshadow::ch_emitter::ToastMode::Clickhouse)
    {
        let renamed = walshadow::toast::adopt_legacy_mirrors(cfg, legacy_owner)
            .await
            .context("rename TOAST mirrors")?;
        if renamed > 0 {
            tracing::info!(
                target: "walshadow",
                renamed,
                "renamed TOAST mirrors to include source database oid",
            );
        }
    }
    // Cluster-wide sections ([bootstrap], [backup], [memory]) with tenants
    let cluster_cfg = match &tenants_cfg {
        Some(_) => Some(EmitterConfig::from_table(&merged).context("parse cluster config")?),
        None => None,
    };
    let bootstrap_plan = resolve_bootstrap(args, ch_config.as_ref().or(cluster_cfg.as_ref()))?;
    let shadow_start = resolve_shadow_start(args, bootstrap_plan.mode)?;
    if ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow())
        && let ShadowStart::Resume(dir) = &shadow_start
    {
        walshadow::filter::shadow_relations::ShadowRelations::load(dir).await?;
    }
    let bridge_workers = bridge_pool_size(ch_config.as_ref());
    // Slot before bootstrap
    if let Some(slot) = source_conn.slot.as_deref() {
        feed.ensure_physical_slot(slot)
            .await
            .with_context(|| format!("ensure physical replication slot {slot}"))?;
        tracing::info!(target: "walshadow", slot, "physical replication slot ready");
    }
    // Uptime anchors here, ahead of bootstrap: an initial load is part of the
    // session, and a `t0` that only starts at the status loop reads as a
    // counter reset to a scraper watching through it
    let start_instant = Instant::now();
    // One emitter-counter handle for both phases. Bootstrap's insert tail and
    // the streaming pipeline write the same series, so sharing it is what
    // keeps inserter and TOAST totals from resetting at handoff
    let emitter_stats = Arc::new(EmitterStats::default());
    let mut bootstrap_metrics: Option<BootstrapMetrics> = None;
    let mut bootstrap_handoff: Option<BootstrapHandoff> = if shadow_start.bootstraps() {
        if !args.skip_preflight {
            let source_sql = feed
                .sql_client()
                .await
                .context("source sidecar sql for bootstrap pre-flight")?;
            walshadow::preflight::bootstrap(walshadow::preflight::BootstrapInputs {
                source_sql,
                wal_from_archive: args.bootstrap_wal_from_archive,
                window_leg: bootstrap_plan.live_window_leg(args),
            })
            .await
            .context("bootstrap pre-flight probe")?
            .into_result()
            .context("pre-flight rejected bootstrap")?;
        }
        let previous = if let ShadowStart::Rebootstrap(_, marker) = &shadow_start {
            Some(marker.clone())
        } else {
            None
        };
        let (handoff, stage) = or_signal(
            shutdown,
            run_bootstrap(
                &cfg,
                source_conn.replica_pg_config().as_ref(),
                &mut feed,
                args,
                &bootstrap_plan,
                previous,
                ch_config.clone(),
                BootstrapObservers {
                    metrics: &metrics,
                    emitter_stats: emitter_stats.clone(),
                    uptime_from: start_instant,
                },
            ),
        )
        .await
        .context("bootstrap")?;
        bootstrap_metrics = Some(stage);
        Some(handoff)
    } else {
        None
    };
    let bootstrap_end_lsn: Option<u64> = bootstrap_handoff.as_ref().map(|h| h.end_lsn);
    let bootstrap_resume_lsn: Option<u64> =
        bootstrap_handoff.as_ref().map(BootstrapHandoff::resume_lsn);
    let bootstrap_timeline: Option<u32> = bootstrap_handoff.as_ref().map(|h| h.timeline);
    // Regenerate config because shadow's port, socket, and GUC floor may change
    // Keep shadow alive until pipeline teardown finishes
    // Reuse shadow instance started during bootstrap
    let owned = match bootstrap_handoff.as_mut().and_then(|h| h.shadow.take()) {
        Some(running) => running,
        None => {
            let owned = OwnedShadow::new(
                build_owned_shadow(
                    args,
                    &source_conn.dbname,
                    &source_databases,
                    shadow_start.data_dir().to_path_buf(),
                    bridge_workers,
                ),
                args.keep_shadow_running,
            );
            owned
                .shadow
                .write_standby_signal()
                .context("write standby.signal")?;
            walshadow::ops::stages::SHADOW_REPLAY
                .measure(start_owned_shadow(
                    &owned.shadow,
                    bootstrap_end_lsn,
                    Duration::from_secs(args.bootstrap_shadow_replay_timeout),
                    args.keep_shadow_running,
                ))
                .await?;
            owned
        }
    };
    let stand_in = stand_in_primary(&owned.shadow, &source_conn.dbname).await;
    let (owned, source_databases) = if let Some(name) = &stand_in {
        let substituted: Vec<String> = source_databases
            .iter()
            .map(|db| {
                if *db == source_conn.dbname {
                    name.clone()
                } else {
                    db.clone()
                }
            })
            .collect();
        tracing::warn!(
            target: "walshadow",
            absent = source_conn.dbname.as_str(),
            stand_in = name.as_str(),
            "shadow holds no such database; following {} until replay creates it",
            name,
        );
        owned
            .shadow
            .stop()
            .context("stop shadow to restand its bridge")?;
        let owned = OwnedShadow::new(
            build_owned_shadow(
                args,
                name,
                &substituted,
                shadow_start.data_dir().to_path_buf(),
                bridge_workers,
            ),
            args.keep_shadow_running,
        );
        owned
            .shadow
            .write_standby_signal()
            .context("write standby.signal")?;
        start_owned_shadow(
            &owned.shadow,
            None,
            Duration::from_secs(args.bootstrap_shadow_replay_timeout),
            args.keep_shadow_running,
        )
        .await?;
        (owned, substituted)
    } else {
        (owned, source_databases)
    };
    let shadow_lifecycle =
        ShadowLifecycle::spawn(owned, walsender_primary_conninfo(args.walsender_bind));
    let archive = ch_config
        .as_ref()
        .or(cluster_cfg.as_ref())
        .and_then(|c| c.backup.clone())
        .map(Archive::open)
        .transpose()?;
    let start_lsn_override: Option<Pos<RawStart>> = args
        .start_lsn
        .as_deref()
        .map(|s| walshadow::pg::parse_pg_lsn(s).context("--start-lsn"))
        .transpose()?
        .map(Pos::new);

    let live_identity = manifest::SourceIdentity {
        system_id: ident.sysid.parse().context("IDENTIFY_SYSTEM sysid")?,
        timeline: ident.timeline,
        timeline_begin: Pos::ZERO,
    };
    // Identity gate runs before `--ignore-cursor`: the flag discards resume
    // LSNs, not artifact ownership. Foreign system_id is fatal regardless
    // (retire/backfill ledgers would act on another cluster's state). A newer
    // live timeline is a promotion, proved against the source's history below.
    let manifest_at_boot: Option<manifest::Manifest> =
        match manifest::load(&args.spill_dir, &live_identity).await {
            Ok(m) => m,
            Err(e @ manifest::ManifestError::ForeignSource { .. }) => {
                anyhow::bail!("{e}");
            }
            Err(e) if args.ignore_cursor || start_lsn_override.is_some() => {
                tracing::warn!(
                    target: "walshadow::manifest",
                    error = %e,
                    spill_dir = %args.spill_dir.display(),
                    "manifest unreadable; operator override discards it",
                );
                None
            }
            Err(e) => {
                anyhow::bail!(
                    "manifest at {} unreadable: {e}; restore it, or authorize \
                     recovery with --ignore-cursor / --start-lsn",
                    manifest::manifest_path(&args.spill_dir).display(),
                );
            }
        };
    // Precedence: explicit > bootstrap > manifest > greenfield head
    let manifest_at_boot = if args.ignore_cursor {
        None
    } else {
        manifest_at_boot
    };
    let raw_start = manifest::resolve_resume_lsn(
        start_lsn_override,
        bootstrap_resume_lsn.map(Pos::new),
        manifest_at_boot.as_ref().map(|m| m.lsn.emitter_ack),
        Pos::new(ident.xlogpos),
    );
    let pinned = bootstrap_end_lsn.is_some() || start_lsn_override.is_some();
    let shadow_holds_data = ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow());
    let shadow_replay_seed = manifest_at_boot
        .as_ref()
        .map(|m| m.lsn.shadow_replay.get().max(m.lsn.shadow_flush.get()))
        .unwrap_or_default();
    let boot_shadow_floor = if pinned {
        manifest::ShadowFloor::unbounded()
    } else {
        manifest::ShadowFloor::new(shadow_holds_data, 0, shadow_replay_seed)
    };
    let raw_start = boot_shadow_floor.bound(raw_start);
    let floor_at_boot = manifest_at_boot
        .as_ref()
        .map(|m| m.floor)
        .filter(|f| !f.is_zero());
    // Archive-end scan only feeds the greenfield clamp (keep archive
    // continuous until live streaming begins: starting after last sealed
    // segment leaves shadow missing WAL; re-read from earlier LSN, CH
    // removes duplicates using `_lsn`). A persisted floor folded the clamp
    // at write time.
    let archive_end = if !pinned && floor_at_boot.is_none() {
        max_segment_end(&args.out_dir)
            .await
            .context("scan out-dir for sealed archive end")?
    } else {
        None
    };
    let aligned = manifest::resolve_start(
        raw_start,
        floor_at_boot,
        pinned,
        archive_end,
        boot_shadow_floor,
    );
    tracing::info!(
        target: "walshadow",
        raw = %raw_start,
        aligned = %aligned,
        from_bootstrap = bootstrap_end_lsn.is_some() && args.start_lsn.is_none(),
        from_floor = floor_at_boot.is_some() && !pinned,
        "start LSN",
    );

    // Branch selection is per segment, through the source's history: a floor
    // stored on an ancestor is served by that ancestor, whatever the live head
    // reports, and a floor at a fork segment's start is served by the descendant
    // whose file holds the ancestor prefix (architecture/recovery.md).
    let stored_timeline = bootstrap_timeline
        .or_else(|| manifest_at_boot.as_ref().map(|m| m.source.timeline))
        .unwrap_or(ident.timeline);
    let mut history = load_boot_history(&mut feed, ident.timeline, stored_timeline).await?;
    let start_timeline = match history.resume_branch(stored_timeline, aligned.get(), WAL_SEG_SIZE) {
        Some(tli) => tli,
        None if args.ignore_cursor => {
            let found = history.tli_of_segment(aligned.get(), WAL_SEG_SIZE);
            tracing::warn!(
                target: "walshadow",
                stored_timeline,
                live_timeline = ident.timeline,
                serves_start = found,
                "--ignore-cursor adopts the live timeline without a lineage proof",
            );
            history = TimelineHistory::root(ident.timeline);
            ident.timeline
        }
        None => anyhow::bail!(
            "timeline_not_descendant: stored timeline {stored_timeline} does not reach \
             {} on live timeline {}'s history (it serves {:?}); \
             --ignore-cursor re-baselines onto the live branch",
            aligned,
            ident.timeline,
            history.tli_of_segment(aligned.get(), WAL_SEG_SIZE),
        ),
    };
    // Backup passes replay archived WAL off the same branch, and outlive a
    // crossing, so they read the chain here rather than re-deriving it
    let (history_tx, history_rx) = watch::channel(Arc::new(history.clone()));
    // Same number, different branch: the chain places a sibling exactly where it
    // places a descendant, and only the switchpoint separates them. A stored
    // begin is the chain a previous run proved, carried forward
    // (architecture/recovery.md)
    let live_begin = history.begin_of(stored_timeline).unwrap_or(0);
    let stored_begin = if bootstrap_timeline.is_some() {
        live_begin
    } else {
        manifest_at_boot
            .as_ref()
            .map(|m| m.source.timeline_begin.get())
            .unwrap_or(0)
    };
    match stored_begin {
        0 if stored_timeline > 1 => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            live_begin = %format_pg_lsn(live_begin),
            "manifest records no switchpoint for its branch, so a sibling sharing \
             that number cannot be refused until the next manifest write",
        ),
        0 => {}
        begin if begin != live_begin && !args.ignore_cursor => anyhow::bail!(
            "sibling_branch: source places timeline {stored_timeline} at {}, \
             walshadow's artifacts came off it from {}; the branch behind them is \
             absent from this source's history",
            format_pg_lsn(live_begin),
            format_pg_lsn(begin),
        ),
        begin if begin != live_begin => tracing::warn!(
            target: "walshadow",
            stored_timeline,
            stored_begin = %format_pg_lsn(begin),
            live_begin = %format_pg_lsn(live_begin),
            "--ignore-cursor adopts a branch that begins somewhere else",
        ),
        _ => {}
    }
    if start_timeline != ident.timeline {
        tracing::info!(
            target: "walshadow",
            start_timeline,
            live_timeline = ident.timeline,
            switch_lsn = history
                .switchpoint_of(start_timeline)
                .map(|l| format_pg_lsn(l).to_string()),
            "resuming on an ancestor timeline; the crossing follows its fork",
        );
    }
    // Branches a spill-dir artifact may carry: the resume branch plus every
    // ancestor the chain places below it. A crossing moves the resume branch
    // while the artifacts stay where they were written
    let lineage: Vec<u32> = history
        .entries()
        .iter()
        .map(|e| e.tli)
        .filter(|tli| *tli <= start_timeline)
        .collect();

    let mut stream = WalStream::builder(start_timeline, WAL_SEG_SIZE, aligned)?;
    let prefix_dirs = [args.out_dir.clone(), shadow_start.data_dir().join("pg_wal")];
    stream.preserve_resume_prefix(&prefix_dirs).await?;
    // Shadow must attach to this listener before catalog replay can advance
    let mut shadow_boot = walshadow::shadow_stream::ShadowStreamState::new(
        history.shadow_boot_branch(stored_timeline, aligned.get(), start_timeline),
        ident.sysid.clone(),
        aligned.get(),
        args.walsender_slow_threshold,
    );
    seed_shadow_branches(&mut shadow_boot, &history, &args.out_dir, start_timeline).await?;
    let shadow_state = Arc::new(Mutex::new(shadow_boot));
    let walsender_addr = args.walsender_bind;
    let walsender_task = walshadow::shadow_stream::spawn_listener(
        walshadow::shadow_stream::WalSenderAddr::Tcp(walsender_addr),
        shadow_state.clone(),
        Duration::from_millis(50),
    )
    .await
    .with_context(|| format!("bind walsender at {walsender_addr}"))?;
    tasks.adopt("walsender listener", walsender_task);
    tracing::info!(target: "walshadow", addr = %walsender_addr, "walsender listening");
    stream.set_bytes_sink(Box::new(walshadow::shadow_stream::ShadowStreamSink::new(
        shadow_state.clone(),
    )));
    // Set address after bind so first connection succeeds
    // Supervisor restarts a shadow that is down, with the address in its conf
    let conninfo = walsender_primary_conninfo(walsender_addr);
    probe_blocking(&shadow_lifecycle.guard.shadow, move |s| {
        s.point_at_walsender(&conninfo)
    })
    .await;

    // Seed catalog tracker from source's current pg_class before
    // START_REPLICATION. Closes the "source rotated a mapped catalog above
    // 16384 pre-attach" hole the < 16384 bootstrap rule misses. Idempotent.
    {
        let source_cfg = feed.pg_config().clone();
        let sql_client = feed
            .sql_client()
            .await
            .context("open sidecar sql client for seed_from_source")?;
        let added = walshadow::source_feed::seed_all_databases(
            stream.filter_mut().tracker_mut(),
            sql_client,
            &source_cfg,
        )
        .await
        .context("seed_from_source")?;
        let observed_from = stream
            .filter_mut()
            .seed_observed_from_source(sql_client)
            .await
            .context("seed observed-from xid")?;
        tracing::info!(
            target: "walshadow",
            observed_from,
            "transactions from this xid on are observed whole",
        );
        tracing::info!(
            target: "walshadow",
            added,
            "seeded catalog filenodes from source pg_class"
        );
    }

    // Cluster-wide shadow session (retention sweeper): the database the pump
    // connects to, the followed one or, with tenants, the admin one
    let primary_dbname = stand_in
        .clone()
        .unwrap_or_else(|| source_conn.dbname.clone());
    let connect_budget = Duration::from_secs(args.shadow_connect_timeout);
    // `[source] dbname` created after the shadow's backup: the single-database
    // layout follows the stand-in until replay creates it, then the session
    // restarts to follow it. Tenants attach their databases themselves
    let mut pending_dbs: Vec<PendingDatabase> = Vec::new();
    if stand_in.is_some() && tenants_cfg.is_none() {
        tokio::fs::write(args.spill_dir.join(ATTACH_MARKER), b"")
            .await
            .with_context(|| format!("mark {} as attaching", args.spill_dir.display()))?;
        let source_sql = feed
            .sql_client()
            .await
            .context("source sidecar sql for the absent database")?;
        let visible_at = source_sql
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .context("source WAL position for the absent database")?;
        let visible_at = walshadow::pg::parse_pg_lsn(visible_at.get::<_, &str>(0))
            .context("parse source WAL position")?;
        let oid = *list_databases(source_sql)
            .await
            .context("list source databases")?
            .get(source_conn.dbname.as_str())
            .with_context(|| {
                format!(
                    "database {} is configured but not on the source",
                    source_conn.dbname
                )
            })?;
        pending_dbs.push(PendingDatabase {
            oid,
            name: source_conn.dbname.clone(),
            visible_at,
            // The primary's bridge slots come first
            bridge_socket: walshadow::bridge::slot_path(&args.bridge_socket_path(), 0),
        });
    }
    let shadow_conninfo = socket_conninfo(
        args.shadow_socket_dir
            .to_str()
            .context("shadow-socket-dir not UTF-8")?,
        args.shadow_port,
        &args.shadow_user,
        &primary_dbname,
    );

    // Share pump's xid samples with shadow TOAST reads to detect reused IDs
    let xid_ceiling = Arc::new(walshadow::toast::xid_ceiling::XidCeiling::default());
    stream.filter_mut().set_xid_ceiling(xid_ceiling.clone());

    // Persist handoff before streaming can advance manifest
    if let (Some(end_lsn), Some(resume)) = (bootstrap_end_lsn, bootstrap_resume_lsn) {
        let initial = manifest::Manifest {
            version: manifest::MANIFEST_VERSION,
            // Shadow replayed through end_lsn before handoff, so its bound
            // never cuts below resume ≤ end_lsn
            floor: manifest::FloorInputs {
                resume_safe: Pos::new(resume),
                filter_durable: Pos::new(end_lsn),
                shadow: manifest::ShadowFloor::new(shadow_holds_data, end_lsn, 0),
                ..manifest::FloorInputs::default()
            }
            .floor(),
            source: manifest::SourceIdentity {
                system_id: live_identity.system_id,
                timeline: start_timeline,
                timeline_begin: Pos::new(history.begin_of(start_timeline).unwrap_or(0)),
            },
            wal: manifest::WalBranch {
                stream_timeline: start_timeline,
            },
            lsn: manifest::LsnSet {
                source_received: Pos::new(end_lsn),
                filter_durable: Pos::new(end_lsn),
                shadow_replay: Pos::new(end_lsn),
                drain: Pos::new(resume),
                emitter_ack: Pos::new(resume),
                shadow_flush: Pos::new(end_lsn),
            },
        };
        manifest::write(&args.spill_dir, &initial)
            .await
            .context("write initial resume manifest after bootstrap")?;
    }

    let source_major = (feed.server_version_num() / 10000) as u32;
    anyhow::ensure!(
        (16..=19).contains(&source_major),
        "source PG major {source_major} unsupported (commit-record sinval layout audited for 16-19)",
    );
    let shadow_toast = ch_config.as_ref().is_some_and(|c| c.toast.mode.is_shadow());
    // Check TOAST availability for opt-in and configured relations
    let mut shadow_toast_held = None;
    if shadow_toast {
        stream
            .filter_mut()
            .load_shadow_rels(shadow_start.data_dir())
            .await?;
        let rels = stream
            .filter()
            .shadow_rels()
            .context("shadow replay eligibility missing after load")?;
        tracing::info!(
            target: "walshadow::toast",
            rels = rels.len(),
            "[toast] mode = shadow: loaded durable replay eligibility",
        );
        shadow_toast_held = Some(rels.held());
    }
    let mut stream = stream.start();
    // Persisted resolved floor. Seed with the resolved start: aligned +
    // archive-clamped, the exact position a crash-now restart replays from.
    // Any Dropped queued during the boot re-read of [aligned, raw_start] has
    // commit_lsn ≥ aligned, so its retire holds until a later manifest write
    // moves the floor past it.
    let resume_floor = Arc::new(Monotone::<Floor>::new(aligned));
    let shared = tenant::SessionShared {
        sysid: ident.sysid.clone(),
        sysid_num: sysid,
        source_conn: source_conn.clone(),
        source_major,
        source_version_num: feed.server_version_num(),
        start_timeline,
        lineage: lineage.clone(),
        history_rx: history_rx.clone(),
        shadow_state: shadow_state.clone(),
        smgr_markers: stream.filter_mut().smgr_markers(),
        xid_ceiling: xid_ceiling.clone(),
        resume_floor: resume_floor.clone(),
        decoder_batch_size,
        decoder_queue_capacity,
        span_tracing: args.otlp_endpoint.is_some()
            || std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok(),
        io: io.clone(),
    };
    drop(history_rx);
    let mut router = walshadow::tenant_router::TenantRouter::new(
        tenants_cfg
            .as_ref()
            .map_or(NO_STALL_LIMIT, |t| t.stall_timeout),
        tenants_cfg.is_some(),
    );
    let mut tenants: Vec<tenant::Tenant> = Vec::new();
    let mut supervisor = tenant::Supervisor::default();
    // Cluster knobs (pause, source endpoint) with tenants: a destination-less
    // resolver over the cluster sections, which the control socket reloads
    let mut cluster_resolver: Option<Arc<ConfigResolver>> = None;
    let mut registry_poller: Option<tokio::task::JoinHandle<()>> = None;
    match &tenants_cfg {
        None => {
            // Same document per database, scoped to that database's entries;
            // the primary's copy already carries its opened destination
            let primary_index = source_databases
                .iter()
                .position(|db| *db == source_conn.dbname)
                .unwrap_or(0);
            let mut databases = Vec::with_capacity(source_databases.len());
            for (i, name) in source_databases.iter().enumerate() {
                let emitter = if i == primary_index {
                    ch_config.clone()
                } else if ch_config.is_some() {
                    build_emitter_config(args, &merged, sysid, name, None).await?
                } else {
                    None
                };
                databases.push(tenant::BootDb {
                    name: name.clone(),
                    emitter,
                });
            }
            let boot = tenant::TenantBoot {
                args,
                id: walshadow::tenants::LEGACY_TENANT.into(),
                databases,
                primary: primary_index,
                dir: args.spill_dir.clone(),
                emitter_stats: emitter_stats.clone(),
                source_conn: source_conn.clone(),
                preflight_slot: source_conn.slot.clone(),
                sysid: shared.sysid.clone(),
                sysid_num: sysid,
                source_major,
                source_version_num: shared.source_version_num,
                start_timeline,
                lineage: lineage.clone(),
                raw_start,
                aligned,
                expect_log: manifest_at_boot.is_some(),
                start_lsn_override,
                history_rx: shared.history_rx.clone(),
                shadow_state: shadow_state.clone(),
                smgr_markers: shared.smgr_markers.clone(),
                xid_ceiling: xid_ceiling.clone(),
                bridge_path: args.bridge_socket_path(),
                bridge_workers,
                resume_floor: resume_floor.clone(),
                shadow_toast_held: shadow_toast_held.clone(),
                decoder_batch_size,
                decoder_queue_capacity,
                span_tracing: shared.span_tracing,
                min_commit_lsn: 0,
                priming: false,
                activation_opt_ins: Default::default(),
                legacy_owner,
                pending: PendingDatabases::new(
                    std::mem::take(&mut pending_dbs),
                    connect_budget,
                    attach.clone(),
                ),
                io: io.clone(),
            };
            let (mut t, sink) = tenant::open_tenant(boot, Some(&mut tasks)).await?;
            // SIGHUP and `ctl reload` republish every database's scope
            reloader.set_resolvers(t.config_resolvers.clone()).await;
            // Bootstrap's throwaway oracle bridge counts under the database it
            // restored, so its totals carry across handoff rather than reading
            // as a reset once the live bridge takes over
            if let Some(b) = &bootstrap_metrics
                && let Some(db) = t
                    .metrics_dbs
                    .iter_mut()
                    .find(|db| db.database == source_conn.dbname)
            {
                db.bridge[1] = Some(b.bridge.clone());
            }
            for db in &t.db_oids {
                stream.filter_mut().add_target_db(*db);
            }
            router.attach(walshadow::tenant_router::RoutedTenant::new(
                t.id.clone(),
                t.db_oids.clone(),
                0,
                sink,
            ));
            tenants.push(t);
        }
        Some(tcfg) => {
            let cluster = cluster_cfg.clone().unwrap_or_default();
            let (resolver, _rx) = ConfigResolver::new(
                &cluster,
                CliOverrides {
                    drop_table_strategy: args.drop_table_strategy,
                    flush_timeout: None,
                    source_slot: args.slot.clone(),
                },
                args.ch_config.clone(),
                cli_base(args),
                walshadow::mapping::mapping_handle(Default::default()),
                walshadow::config::ResolverBoot::default(),
            );
            reloader.set_resolvers(vec![resolver.clone()]).await;
            cluster_resolver = Some(resolver);
            if let (walshadow::tenants::Registry::Sql { schema, poll }, Some(path)) =
                (&tcfg.registry, args.ch_config.clone())
            {
                registry_poller = Some(tenant::spawn_registry_poller(
                    path,
                    cli_base(args),
                    schema.clone(),
                    *poll,
                    reloader.clone(),
                ));
            }
            let active: Vec<_> = tcfg
                .decls
                .iter()
                .filter(|d| d.desired == walshadow::tenants::Desired::Active)
                .collect();
            tenant::publish_tenant_bridges(
                &shadow_lifecycle,
                active.iter().map(|d| d.dbname.clone()).collect(),
            )
            .await?;
            for decl in active {
                match supervisor
                    .boot_existing(
                        &shared, args, &merged, tcfg, decl, raw_start, aligned, reloader,
                    )
                    .await
                {
                    Ok(Some((t, sink, from_lsn))) => {
                        for db in &t.db_oids {
                            stream.filter_mut().add_target_db(*db);
                        }
                        router.attach(walshadow::tenant_router::RoutedTenant::new(
                            t.id.clone(),
                            t.db_oids.clone(),
                            from_lsn,
                            sink,
                        ));
                        tenants.push(t);
                    }
                    Ok(None) => supervisor.queue_attach(&decl.id),
                    Err(e) => {
                        tracing::error!(
                            target: "walshadow::tenant",
                            tenant = %decl.id,
                            error = %format!("{e:#}"),
                            "tenant failed to open; detaching it, other tenants continue",
                        );
                        supervisor
                            .record_detached(&args.spill_dir, decl, format!("open failed: {e:#}"))
                            .await;
                    }
                }
            }
        }
    }
    // Records the filter drops count under the boot tenant's decoder: the
    // single-database layout's one, or the first tenant's, since the filter
    // sees the stream before any routing
    if let Some(t) = tenants.first() {
        stream.filter_mut().set_skip_stats(t.decoder_stats.clone());
    }
    let config_resolver = tenants.first().and_then(|t| t.config_resolver.clone());
    let mut record_sink = DaemonSinks {
        metrics: MetricsRecordSink::default(),
        decoder_xact: router,
        span_registry: tenants.first().and_then(|t| t.span_registry.clone()),
    };
    // Segment fsync off the hot path: sink writes+renames, the task fsyncs and
    // publishes `durable_lsn`. Seed at the resume point.
    let durable_lsn = Arc::new(Monotone::<FilterDurable>::new(Pos::new(
        stream.dispatched_lsn(),
    )));
    let fsync_fatal = walshadow::pipeline::Fatal::new();
    let (fsync_tx, fsync_rx) = tokio::sync::mpsc::channel::<SegFsync>(SEGMENT_FSYNC_QUEUE);
    let mut fsync_task = spawn_segment_fsync(
        args.out_dir.clone(),
        fsync_rx,
        durable_lsn.clone(),
        fsync_fatal.clone(),
    );
    let mut segment_sink =
        DirSegmentSink::with_durability(args.out_dir.clone(), WAL_SEG_SIZE, fsync_tx, io.clone())
            .context("open out-dir")?;
    // Pruners' floor for the crossing commit; each tenant's descriptor-log
    // GC follows the floors the pump publishes to it
    let gc_floor = Monotone::<Floor>::default();
    let mut chunk_buf = Vec::with_capacity(64 * 1024);

    // Metrics endpoint + control socket + SIGHUP are process-lifetime (bound in
    // `run`); the session only writes into the shared registry.

    // Walreceiver apply LSN (shadow's `GetXLogReplayRecPtr`), joined each pump
    // iteration; feeds the manifest's `shadow_replay`, the shadow floor, the
    // standby-status `apply_lsn` ceiling and the retention cut
    let shadow_replay_lsn = Arc::new(Monotone::<ShadowReplay>::default());
    // Aggregate flush across ShadowStreamSink connections, fed into the
    // cursor for shadow's `START_REPLICATION PHYSICAL` resume on restart.
    let shadow_flush_lsn = Arc::new(Monotone::<ShadowFlush>::default());

    // Retention sweeper drops filtered segments more than `retention_bytes`
    // behind shadow's replay LSN
    if args.retention_bytes > 0 {
        tasks.spawn(
            "retention",
            trim_retention(
                args.out_dir.clone(),
                args.retention_bytes,
                shadow_conninfo.clone(),
                shadow_replay_lsn.clone(),
            ),
        );
    }

    // Block until shadow's walreceiver attaches. `ShadowStreamSink::
    // on_wire_chunk` drops bytes with no connection registered, so a pump
    // racing past `START_REPLICATION`'s LSN before walreceiver arrives
    // leaves an unrecoverable gap: post-conn frames carry LSNs past
    // walreceiver's expected continuity, shadow's apply stalls, the catalog
    // gate times out (pgbench_acceptance / kill_restart failure mode). No
    // attachment fails startup: catalog-boundary holds require a live wire,
    // and archive-only operation can't stop publication at a mid-segment
    // commit (restore_command must never observe unreleased bytes).
    {
        let timeout = Duration::from_secs(args.walsender_connect_timeout);
        let start = Instant::now();
        loop {
            let agg = shadow_state.lock().await.aggregate();
            if agg.active_connections > 0 {
                break;
            }
            // `accepted` separates "shadow never dialed" from "shadow dialed
            // and stalled in the handshake" — the latter reads as the former
            // without it, since only START_REPLICATION registers a connection
            anyhow::ensure!(
                start.elapsed() < timeout,
                "no walreceiver streaming from walsender {walsender_addr} within \
                 {}s (accepted {}, none sent START_REPLICATION); catalog-boundary \
                 holds require a live wire — point shadow's primary_conninfo here \
                 or raise --walsender-connect-timeout",
                args.walsender_connect_timeout,
                agg.accepted_total,
            );
            or_signal(shutdown, async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            })
            .await?;
        }
        tracing::info!(
            target: "walshadow",
            wait = ?start.elapsed(),
            "walsender connected — starting pump",
        );
    }

    let mut source_recovery = SourceRecovery {
        system_id: live_identity.system_id,
        status_interval: Duration::from_secs(args.status_interval),
        backup: archive.as_ref(),
        floor: &resume_floor,
        prefetch: usize::from(args.archive_prefetch),
        backoff: ReconnectBackoff::default(),
    };
    let mut timeline_stats = TimelineStats {
        // Off the chain, so a restart after a crossing keeps reporting the fork
        // it resumed across instead of zero
        switch_lsn: history.begin_of(start_timeline).unwrap_or(0),
        ..TimelineStats::default()
    };
    let mut path = SourcePath::Live;
    if let Err(e) = feed
        .start_physical_replication(
            source_conn.slot.as_deref(),
            stream.next_lsn().get(),
            start_timeline,
        )
        .await
    {
        path = source_recovery
            .attempt(
                Some(e),
                &source_conn,
                &history,
                &stream,
                &mut feed,
                &mut timeline_stats,
            )
            .await?;
    }

    let mut segments_shipped = 0u64;
    let mut prev_dispatched = stream.dispatched_lsn();
    let mut rate_estimator = RateEstimator::default();
    // Manifest write cadence. Slot safety doesn't ride on it: advertised
    // flush_lsn is capped at the persisted floor below, so a lagging write
    // only delays slot advance, never overshoots it.
    let cursor_write_interval = Duration::from_secs(args.status_interval);
    let mut last_cursor_write: Option<Instant> = None;
    // A shadow catching up through `restore_command` holds no stream, so its
    // replay has to be polled or the floor freezes
    let mut shadow_replay_poll = walshadow::ticker::Ticker::new(cursor_write_interval);
    // Fast metrics-refresh tick (decoupled from cursor/status): an idle source
    // would otherwise freeze the /metrics snapshot while the pipeline drains.
    let metrics_tick = Duration::from_millis(250);
    // Streaming publish cadence; idle ticks and segment advances always publish
    const PUBLISH_MIN_INTERVAL: Duration = Duration::from_millis(50);
    let mut last_publish = Instant::now();
    // Inflight-stall watchdog: xacts_active > 0 with stalled
    // `emitter_ack_lsn` dumps the parked xids holding the slot. One-shot
    // per stall, re-arms when ack advances.
    let mut last_emitter_ack_observed = Pos::<EmitterAck>::ZERO;
    let mut inflight_stall_since: Option<Instant> = None;
    let mut inflight_stall_logged = false;
    // Pump reads `paused` and the source endpoint live off the resolver watch;
    // when paused it idles (stops consuming source WAL) without tearing
    // anything down, and a moved `[source]` swaps the feed in place.
    // Pause and the source endpoint are cluster-wide, so the primary's
    // resolver is the one the pump watches
    let pump_config_rx = config_resolver
        .as_ref()
        .or(cluster_resolver.as_ref())
        .map(|r| r.subscribe());
    let mut swap = SourceSwap::default();
    // Frontier frozen when the pump observes a pause, so a promotion decision
    // reads one that cannot move under it. Cleared on resume: a value left over
    // from an earlier pause is as misleading as a live one
    let mut pause = PauseState::Running;
    let mut pause_left_stream = false;
    let mut resume_restreams = false;
    let mut ever_unpaused = false;
    // Step 5's answer, refreshed while paused off the endpoint the pump holds
    let mut promotion = PromotionGate::blocked("not_paused");
    let switchover = Switchover {
        system_id: live_identity.system_id,
        out_dir: &args.out_dir,
        shadow_state: &shadow_state,
        backup: archive.as_ref(),
    };
    // The ancestor ended and the descendant has not been adopted yet. Survives
    // iterations so a source error mid-crossing retries the crossing: at the
    // ancestor's switchpoint an ordinary reconnect has nothing to ask for
    let mut crossing = CrossingState::default();
    let mut barrier_logged: Option<Instant> = None;
    let shutdown_reason = 'pump: loop {
        // Switch timelines at a switchpoint. Reconnecting may find a switchpoint
        // missing from `history`; use that connection to fetch updated history.
        if matches!(path, SourcePath::AtSwitchpoint) {
            path = SourcePath::Live;
            crossing.ancestor_ended(false);
        } else if !matches!(path, SourcePath::Live)
            && history.branch_exhausted(stream.timeline(), stream.next_lsn().get())
        {
            path = SourcePath::Live;
            crossing.ancestor_ended(true);
        }
        let paused = pump_config_rx
            .as_ref()
            .map(|rx| rx.borrow().paused)
            .unwrap_or(false);
        if paused && !pause_left_stream && matches!(path, SourcePath::Live) && !crossing.pending() {
            pause_left_stream = true;
            resume_restreams = true;
            if let Err(e) = feed.end_stream().await {
                tracing::warn!(
                    target: "walshadow",
                    error = %format!("{e:#}"),
                    "leaving the source stream for a pause failed; resume redials",
                );
            } else {
                tracing::info!(
                    target: "walshadow",
                    resume_lsn = %stream.next_lsn(),
                    "pause left the source stream so the source can shut down",
                );
            }
        } else if !paused && pause_left_stream {
            pause_left_stream = false;
            if std::mem::take(&mut resume_restreams) {
                let restarted = feed
                    .start_physical_replication(
                        source_conn.slot.as_deref(),
                        stream.next_lsn().get(),
                        stream.timeline(),
                    )
                    .await;
                if let Err(e) = restarted {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        resume_lsn = %stream.next_lsn(),
                        "resuming the source stream failed — redialing",
                    );
                    path = SourcePath::Redial;
                    source_recovery.backoff.reset();
                } else {
                    tracing::info!(
                        target: "walshadow",
                        resume_lsn = %stream.next_lsn(),
                        "resume re-entered the source stream",
                    );
                }
            }
        }
        // Slot changes require reconnect because START_REPLICATION binds slot
        if let Some(rx) = pump_config_rx.as_ref() {
            let desired = rx.borrow().source.clone();
            if desired != source_conn {
                tracing::info!(
                    target: "walshadow",
                    from = source_conn.endpoint(),
                    to = desired.endpoint(),
                    from_slot = source_conn.slot.as_deref(),
                    to_slot = desired.slot.as_deref(),
                    "source changed — swapping feed",
                );
                source_conn = desired;
                cfg = source_conn.to_pg_config();
                swap.requested();
            }
        }
        // Lost source redials whatever `[source]` names now, so a repoint made
        // during an outage is what the next attempt dials
        if matches!(path, SourcePath::Redial) && source_recovery.backoff.due() {
            path = source_recovery
                .attempt(
                    None,
                    &source_conn,
                    &history,
                    &stream,
                    &mut feed,
                    &mut timeline_stats,
                )
                .await?;
            swap.settled();
        }
        // Swap between chunks, so the resume point is the byte-contiguous
        // `next_lsn` and no WalStream state is rebuilt. Old feed stays up
        // until the new endpoint proves same cluster and branch, and until the
        // named slot answers: a wrong address or a slot the target never got
        // costs a warning, not the stream.
        //
        // Not while a crossing is pending: the stream sits at a switchpoint no
        // branch resumes from, and the crossing dials the live endpoint and slot
        // itself, so a repoint made mid-crossing lands there instead.
        if swap.due(Instant::now()) && !crossing.pending() && !matches!(path, SourcePath::Redial) {
            match resume_source_feed(
                &cfg,
                source_conn.slot.as_deref(),
                stream.next_lsn(),
                stream_branch(&history, live_identity.system_id, &stream),
                resume_floor.get(),
                Duration::from_secs(args.status_interval),
            )
            .await
            {
                Ok((swapped, point)) => {
                    feed = swapped;
                    path = match point {
                        ResumePoint::OnBranch => SourcePath::Live,
                        ResumePoint::AtSwitchpoint => SourcePath::AtSwitchpoint,
                    };
                    resume_restreams = false;
                    swap.settled();
                    if point == ResumePoint::OnBranch {
                        swap.swaps += 1;
                    }
                    tracing::info!(
                        target: "walshadow",
                        endpoint = source_conn.endpoint(),
                        resume_lsn = %stream.next_lsn(),
                        resume_point = ?point,
                        slot = source_conn.slot.as_deref(),
                        "source feed swapped",
                    );
                }
                Err(e) => {
                    swap.failed(swap_reason(&e));
                    timeline_stats.record_reason(swap.blocked_on);
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = swap.blocked_on,
                        endpoint = source_conn.endpoint(),
                        "source endpoint swap failed — staying on current feed",
                    );
                }
            }
        }
        // Tenant lifecycle, between chunks where no record is mid-flight
        if tenants_cfg.is_some() {
            // A reload (control socket, SIGHUP) may add, remove, detach or
            // re-bind tenants; tenant resolvers already took their own knobs
            if reloader.take_reconcile()
                && let Some(path) = args.ch_config.as_deref()
            {
                match walshadow::ch_emitter::load_effective(path, cli_base(args))
                    .await
                    .map_err(|e| anyhow::anyhow!("{e}"))
                    .and_then(|m| {
                        walshadow::ops::tenant_ctl::validate(&m)?;
                        walshadow::tenants::TenantsConfig::from_table(&m).map(|t| (m, t))
                    }) {
                    Ok((next_merged, Some(next))) => {
                        let old = tenants_cfg.take().expect("tenants mode");
                        let plan = tenant::reconcile_plan(
                            &old,
                            &next,
                            &merged,
                            &next_merged,
                            tenants.iter().map(|t| t.id.as_str()),
                        );
                        merged = next_merged;
                        tenants_cfg = Some(next);
                        let tcfg = tenants_cfg.as_ref().expect("just set");
                        for (id, reason) in plan.detach {
                            tenant::detach(
                                &id,
                                reason,
                                true,
                                tcfg.stall_timeout,
                                &mut tenants,
                                &mut record_sink.decoder_xact,
                                &mut stream,
                                &mut supervisor,
                                reloader,
                                &args.spill_dir,
                                tcfg.get(&id),
                            )
                            .await;
                        }
                        for id in plan.attach {
                            supervisor.queue_attach(&id);
                        }
                        if let Err(e) = tenant::publish_tenant_bridges(
                            &shadow_lifecycle,
                            tenant::wanted_databases(tcfg),
                        )
                        .await
                        {
                            tracing::warn!(target: "walshadow::tenant", error = %format!("{e:#}"), "publishing tenant bridges failed");
                        }
                    }
                    Ok((_, None)) => tracing::warn!(
                        target: "walshadow::tenant",
                        "config no longer declares tenants; restart to change layouts",
                    ),
                    Err(e) => tracing::warn!(
                        target: "walshadow::tenant",
                        error = %format!("{e:#}"),
                        "tenant reconcile skipped: config does not parse",
                    ),
                }
            }
            let tcfg = tenants_cfg.as_ref().expect("tenants mode");
            // Evict tenants the router gave up on, or that trail too far
            let mut evict = record_sink.decoder_xact.evictions();
            let head = record_sink.decoder_xact.last_record_end();
            for t in &tenants {
                let limit = tcfg
                    .get(&t.id)
                    .and_then(|d| d.max_lag_bytes)
                    .or(tcfg.max_lag_bytes);
                if let Some(limit) = limit
                    && !evict.iter().any(|(id, _)| id == &t.id)
                {
                    let lag = head.saturating_sub(t.resume_safe().await.get());
                    if lag > limit {
                        evict.push((
                            t.id.clone(),
                            format!("fell {lag} bytes behind the pump, over its {limit} limit"),
                        ));
                    }
                }
            }
            for (id, reason) in evict {
                tenant::detach(
                    &id,
                    reason,
                    false,
                    tcfg.stall_timeout,
                    &mut tenants,
                    &mut record_sink.decoder_xact,
                    &mut stream,
                    &mut supervisor,
                    reloader,
                    &args.spill_dir,
                    tcfg.get(&id),
                )
                .await;
            }
            // Attach at most one queued tenant per iteration, at a position
            // every earlier record has been routed up to and shadow replayed
            if !paused
                && !crossing.pending()
                && let Some(id) = supervisor.next_attach()
                && let Some(decl) = tcfg
                    .get(&id)
                    .filter(|d| d.desired == walshadow::tenants::Desired::Active)
                && !tenants.iter().any(|t| t.id == id)
            {
                let attached = async {
                    record_sink.decoder_xact.flush().await?;
                    let p0 = match record_sink.decoder_xact.last_record_end() {
                        0 => stream.next_lsn().get(),
                        end => end,
                    };
                    tenant::wait_shadow_replay(
                        &shadow_state,
                        record_sink.decoder_xact.last_record_start(),
                        Duration::from_secs(args.catalog_hold_timeout),
                    )
                    .await?;
                    let shadow_sql = open_shadow_sql_client(
                        &args.shadow_socket_dir,
                        args.shadow_port,
                        &args.shadow_user,
                        &primary_dbname,
                    )
                    .await?;
                    if !tenant::attachment_ready(
                        feed.sql_client().await?,
                        &shadow_sql,
                        &decl.dbname,
                    )
                    .await?
                    {
                        return Ok(None);
                    }
                    tenant::publish_tenant_bridges(
                        &shadow_lifecycle,
                        tenant::wanted_databases(tcfg),
                    )
                    .await?;
                    let (t, sink) = supervisor
                        .attach(&shared, args, &merged, tcfg, decl, p0, reloader)
                        .await?;
                    anyhow::Ok(Some((t, sink, p0)))
                }
                .await;
                match attached {
                    Ok(None) => supervisor.retry_attach(&id, Duration::from_secs(1)),
                    Ok(Some((t, sink, p0))) => {
                        for db in &t.db_oids {
                            stream.filter_mut().add_target_db(*db);
                        }
                        record_sink.decoder_xact.attach(
                            walshadow::tenant_router::RoutedTenant::new(
                                t.id.clone(),
                                t.db_oids.clone(),
                                p0,
                                sink,
                            ),
                        );
                        tenants.push(t);
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "walshadow::tenant",
                            tenant = %id,
                            error = %format!("{e:#}"),
                            "tenant attach failed; it stays detached",
                        );
                        supervisor
                            .record_detached(&args.spill_dir, decl, format!("attach failed: {e:#}"))
                            .await;
                    }
                }
            }
            // Primed tenants publish their start scope
            let head = record_sink.decoder_xact.last_record_end();
            for (id, start) in supervisor.primed(head) {
                let Some(t) = tenants.iter().find(|t| t.id == id) else {
                    continue;
                };
                let Some(decl) = tcfg.get(&id) else {
                    continue;
                };
                if let Err(e) = tenant::activate(t, args, &merged, decl, start).await {
                    tracing::error!(
                        target: "walshadow::tenant",
                        tenant = %id,
                        error = %format!("{e:#}"),
                        "tenant activation failed; evicting",
                    );
                    record_sink
                        .decoder_xact
                        .mark_evicted(&id, format!("activation failed: {e:#}"));
                }
            }
        }
        // `durable` (fsynced) lags `dispatched`; advertise it as flush/cursor.
        let dispatched = stream.dispatched_lsn();
        let durable = durable_lsn.get();
        let received: Pos<SourceReceived> = Pos::new(feed.last_server_wal_end().max(dispatched));
        // Two frontiers, two questions. `consumed` is where resume asks the
        // promoted target to start; `received` is the source head last heard
        // about, which the target must reach before promotion. Bytes cannot
        // have been consumed without being received, so a source that has not
        // reported a head yet reads as level with the consumed frontier
        if !paused {
            pause = PauseState::Running;
        } else if let PauseState::Running = pause {
            let consumed = stream.next_lsn().get();
            let head = received.get().max(consumed);
            // A pause this process never saw lifted was taken before it
            // booted, so these two numbers replace ones an operator may
            // already hold. Both re-freeze conservatively — consumed drops
            // back to the floor, received re-derives from the live head —
            // but a promotion decision has to be taken from the pair on
            // offer now (architecture/recovery.md)
            let refrozen = !ever_unpaused;
            tracing::info!(
                target: "walshadow",
                pause_consumed_lsn = %format_pg_lsn(consumed),
                pause_received_lsn = %format_pg_lsn(head),
                refrozen,
                "pause observed — frontier frozen",
            );
            pause = PauseState::Paused {
                frontier: (consumed, head),
                refrozen,
                polled_at: None,
            };
        }
        ever_unpaused |= !paused;
        // Step 5 of the protocol, answered off the connection step 4's repoint
        // already moved onto the target: replay, receive, and recovery state
        // beside the frozen frontier they have to reach
        // (architecture/recovery.md)
        if let PauseState::Paused {
            frontier: (_, pause_received),
            polled_at,
            ..
        } = &mut pause
        {
            if polled_at.is_none_or(|t| t.elapsed() >= PROMOTION_POLL) {
                *polled_at = Some(Instant::now());
                promotion = match tokio::time::timeout(
                    PROMOTION_POLL,
                    promotion_gate(&mut feed, *pause_received),
                )
                .await
                {
                    Ok(gate) => gate,
                    Err(_) => {
                        feed.drop_sql_client();
                        PromotionGate::unreachable()
                    }
                };
            }
        } else {
            promotion = PromotionGate::blocked("not_paused");
        }
        let (shadow_agg, shadow_served_tli) = {
            let state = shadow_state.lock().await;
            (state.aggregate(), state.timeline)
        };
        if let Some(apply) = shadow_agg.min_apply_lsn {
            shadow_replay_lsn.join(apply);
        } else if shadow_replay_poll.fire()
            && let Some(Some(lsn)) =
                probe_blocking(&shadow_lifecycle.guard.shadow, |s| s.last_replay_lsn()).await
        {
            shadow_replay_lsn.join(Pos::new(lsn));
        }
        let shadow_replay = shadow_replay_lsn.get();
        if let Some(flush) = shadow_agg.min_flush_lsn {
            shadow_flush_lsn.join(flush);
        }
        // Keep every tenant's undurable transactions reachable after restart
        let progress = tenant::aggregate(&tenants, durable).await;
        let (drain_lsn, resume_safe_lsn) = (progress.drain, progress.resume_safe);
        let shadow_floor =
            manifest::ShadowFloor::new(shadow_toast, shadow_replay.get(), shadow_replay_seed);
        let cur = resume_manifest(
            &history,
            &live_identity,
            resume_floor.get(),
            shadow_floor,
            stream.timeline(),
            manifest::LsnSet {
                source_received: received,
                filter_durable: durable,
                shadow_replay,
                drain: drain_lsn,
                emitter_ack: resume_safe_lsn,
                shadow_flush: shadow_flush_lsn.get(),
            },
        );
        if last_cursor_write.is_none_or(|t| t.elapsed() >= cursor_write_interval) {
            let persisted = manifest::write(&args.spill_dir, &cur)
                .await
                .context("write resume manifest")?;
            #[cfg(feature = "test-support")]
            crash_test::hit(crash_test::Point::AfterManifest, cur.floor.get());
            last_cursor_write = Some(Instant::now());
            resume_floor.publish(persisted);
            // Descriptor logs prune against the same floor, off this task: a
            // compaction rewrites the whole ckpt inline and would stall WAL
            // consumption past the source's wal_sender_timeout
            gc_floor.publish(persisted);
            for t in &tenants {
                t.publish_floor(persisted);
            }
        }
        // flush caps physical slot's restart_lsn.
        // Manifest writes are cadence-gated above while keepalive replies inside
        // next_event can send this status at any time.
        let status = StandbyStatus::bounded(
            received,
            resume_floor.get(),
            resume_safe_lsn,
            shadow_replay,
            shadow_floor,
        );
        let dispatched_before = stream.dispatched_lsn();
        // Set inside the select arm, acted on once the chunk borrow is released
        let mut ancestor_ended = false;
        let archived_bytes;
        let mut archived_segment = false;
        let chunk = tokio::select! {
            biased;
            () = shutdown.cancelled() => break "signal",
            Some(err) = tasks.exited() => return Err(err),
            res = &mut fsync_task => return Err(task_stopped("segment fsync", res, &fsync_fatal)),
            // Idle tick so metrics/cursor keep tracking, and so a `paused` flip
            // is picked up promptly.
            _ = tokio::time::sleep(metrics_tick) => None,
            // Paused: stop consuming source WAL (idle); resume re-enables this
            // arm and the pump continues from the same LSN. A pending crossing
            // also parks it — that connection is out of COPY until the
            // descendant is requested.
            result = async { path.archive()?.next().await },
                if matches!(path, SourcePath::Archive(_)) && !paused && !crossing.pending() => {
                match result {
                    Some(Ok((start_lsn, bytes))) => {
                        source_recovery.backoff.reset();
                        anyhow::ensure!(start_lsn == stream.next_lsn().get(), "archive WAL discontinuity");
                        archived_bytes = bytes;
                        archived_segment = true;
                        Some(walshadow::source_feed::WalChunk {
                            start_lsn,
                            server_wal_end: start_lsn + archived_bytes.len() as u64,
                            data: &archived_bytes,
                        })
                    }
                    ended => {
                        let reason = ended.and_then(Result::err).map_or_else(
                            || "archive reader stopped".to_string(),
                            |e| format!("{e:#}"),
                        );
                        tracing::info!(target: "walshadow", reason, "archive ended, reconnecting source");
                        path = SourcePath::Redial;
                        None
                    }
                }
            },
            res = feed.next_event(status, &mut chunk_buf),
                if matches!(path, SourcePath::Live) && !paused && !crossing.pending() => match res {
                Ok(SourceEvent::Wal(c)) => Some(c),
                Ok(SourceEvent::TimelineEnd) => {
                    ancestor_ended = true;
                    None
                }
                // Dropped where the chain says the branch ends: nothing is
                // resumable there, so this is the crossing arriving as a socket
                // close rather than as a next-timeline result
                Ok(SourceEvent::Shutdown) | Err(_)
                if history.branch_exhausted(stream.timeline(), stream.next_lsn().get()) =>
            {
                    tracing::info!(
                        target: "walshadow",
                        switch_lsn = %stream.next_lsn(),
                        finished_timeline = stream.timeline(),
                        "source stream ended where the branch does — crossing",
                    );
                    crossing.ancestor_ended(true);
                    None
                }
                // The source stopped, this consumer did not: reconnect, which
                // is also how a switchover's demoted primary hands over
                res => {
                    let err = match res {
                        Err(e) => {
                            tracing::warn!(
                                target: "walshadow",
                                error = %e,
                                resume_lsn = %stream.next_lsn(),
                                "source stream error — recovering",
                            );
                            e
                        }
                        _ => {
                            tracing::info!(
                                target: "walshadow",
                                resume_lsn = %stream.next_lsn(),
                                "source shut down its walsender — reconnecting",
                            );
                            anyhow::anyhow!("source walsender exited")
                        }
                    };
                    path = source_recovery
                        .attempt(
                            Some(err),
                            &source_conn,
                            &history,
                            &stream,
                            &mut feed,
                            &mut timeline_stats,
                        )
                        .await?;
                    // Recovery dials the live endpoint, so a queued swap is done
                    swap.settled();
                    None
                }
            },
        };
        let idle = chunk.is_none();
        let server_end = chunk
            .as_ref()
            .map(|c| c.server_wal_end)
            .unwrap_or(received.get());
        if let Some(chunk) = chunk {
            let replay_started = Instant::now();
            stream
                .push(
                    chunk.start_lsn,
                    chunk.data,
                    &mut record_sink,
                    &mut segment_sink,
                )
                .await?;
            if archived_segment {
                metrics
                    .update(|snap| {
                        snap.archive_wal_segments_total += 1;
                        snap.archive_replay_seconds_total += replay_started.elapsed().as_secs_f64();
                    })
                    .await;
            }
        }
        metrics
            .update(|snap| {
                snap.pump_queue_wait_seconds_total = record_sink.decoder_xact.send_wait_seconds();
                snap.archive_restore_active = 0;
                if let SourcePath::Archive(reader) = &path {
                    snap.archive_restore_active = 1;
                    snap.archive_fetch_seconds_total +=
                        reader.fetch_nanos.swap(0, Ordering::Relaxed) as f64 / 1e9;
                    snap.archive_wait_seconds_total +=
                        reader.wait_nanos.swap(0, Ordering::Relaxed) as f64 / 1e9;
                }
            })
            .await;
        if ancestor_ended {
            // Answer the backend's CopyDone now, leaving the connection in
            // simple-query mode: that is the state the crossing reads history
            // from, and the state a retry can rebuild by reconnecting
            let ended = feed.end_stream().await;
            if let Err(e) = &ended {
                tracing::warn!(
                    target: "walshadow",
                    error = %format!("{e:#}"),
                    "ending the historic stream failed — reconnecting to cross",
                );
            }
            crossing.ancestor_ended(ended.is_err());
        }
        // Nothing left to stream on the ancestor at its own switchpoint, so
        // only the crossing moves the stream forward. Attempts pace themselves
        // and leave the rest of the loop publishing meanwhile
        // A pause takes the crossing decision back from the pump, so it also
        // clears a wedge: the operator fixes what the refusal named, then
        // resumes and the proof runs again from the untouched ancestor
        if paused && let Some(wedge) = crossing.unpark() {
            tracing::info!(
                target: "walshadow",
                reason = wedge.reason,
                "pause clears the parked crossing — resume re-proves the fork",
            );
        }
        let crossing_due = !paused && crossing.due(Instant::now());
        if crossing_due && crossing.awaiting_connection() {
            match SourceFeed::connect(&cfg).await {
                Ok(fresh) => {
                    feed = fresh.with_status_interval(Duration::from_secs(args.status_interval));
                    crossing.connected();
                }
                Err(e) => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        endpoint = source_conn.endpoint(),
                        "cannot reach the source to cross the fork — retrying",
                    );
                    crossing.retry_at(Instant::now() + SOURCE_SWAP_RETRY);
                }
            }
        }
        if crossing_due && !crossing.awaiting_connection() && crossing.fork().is_none() {
            match switchover
                .probe(
                    &mut feed,
                    &stream,
                    history.begin_of(stream.timeline()).unwrap_or(0),
                    &mut timeline_stats,
                )
                .await
            {
                Ok(probed) => {
                    tracing::info!(
                        target: "walshadow",
                        finished_timeline = probed.finished_tli,
                        next_timeline = probed.next_tli,
                        live_timeline = probed.live_tli,
                        switch_lsn = %format_pg_lsn(probed.switch_lsn),
                        "source fork proved — draining the pipeline to it",
                    );
                    crossing.proved(probed);
                }
                Err(e) if e.retryable() => {
                    tracing::warn!(
                        target: "walshadow",
                        error = %format!("{e:#}"),
                        reason = e.reason(),
                        "proving the source fork failed — retrying",
                    );
                    crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                }
                Err(e) => crossing.park(e, stream.next_lsn().get(), None),
            }
        }
        if crossing_due
            && !crossing.awaiting_connection()
            && let Some(probed) = crossing.fork().cloned()
        {
            // Both fork proofs read the decoder's view, so the pump-side queue
            // drains first: a record still in flight answers for a frontier the
            // decoder has not reached, which would read as a transaction left
            // open at the fork
            record_sink
                .decoder_xact
                .flush()
                .await
                .context("flush queueing decoder sink at the fork")?;
            let fence = Instant::now();
            let in_flight = loop {
                let n = record_sink.decoder_xact.in_flight();
                if n == 0 || fence.elapsed() >= FORK_FENCE_DRAIN {
                    break n;
                }
                tokio::select! {
                    () = shutdown.cancelled() => break 'pump "signal",
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            };
            // Timeout stops queue drain only, fork guards remain authoritative
            if in_flight != 0 {
                tracing::warn!(
                    target: "walshadow",
                    in_flight,
                    waited = ?fence.elapsed(),
                    "fork fence gave up draining the pump queue — guards decide",
                );
            }
            let (guards, resume_safe) = {
                let progress = tenant::aggregate(&tenants, durable).await;
                (
                    ForkGuards {
                        drain_lsn: progress.drain,
                        open_xacts: progress.open_xacts,
                    },
                    progress.resume_safe,
                )
            };
            // Barrier: every consumer past the position about to be committed,
            // so a restart from it loses nothing. The loop keeps publishing
            // meanwhile, so a wait reads as a wait rather than a stall, and the
            // source has stopped producing so nothing queues up behind it
            let open = match (walshadow::transition::ForkBarrier {
                resume_safe_lsn: resume_safe,
                shadow_apply_lsn: (shadow_replay.get() != 0).then_some(shadow_replay),
                filter_durable: durable,
                floor: resume_floor.get(),
            })
            .open(Pos::new(probed.switch_lsn), WAL_SEG_SIZE)
            {
                Ok(open) => Some(open),
                Err(wait) => {
                    // Prod the walreceiver: non-forced replies fire only on flush
                    // progress, and the ancestor's tail may be the last thing left
                    shadow_state.lock().await.request_status();
                    if barrier_logged.is_none_or(|t| t.elapsed() >= BARRIER_LOG_INTERVAL) {
                        tracing::info!(
                            target: "walshadow",
                            switch_lsn = %format_pg_lsn(probed.switch_lsn),
                            waiting_on = wait.label(),
                            "fork barrier: {wait}",
                        );
                        barrier_logged = Some(Instant::now());
                    }
                    None
                }
            };
            if let Some(open) = open {
                barrier_logged = None;
                // Every tenant's descriptor-log pruning rebases with the session's
                let gc_floors: Vec<&Monotone<Floor>> = std::iter::once(&gc_floor)
                    .chain(tenants.iter().filter_map(tenant::Tenant::gc_floor))
                    .collect();
                let commit = async |resume: walshadow::transition::ForkResume| {
                    commit_fork_resume(
                        &args.spill_dir,
                        &live_identity,
                        resume,
                        manifest::LsnSet {
                            // Fork cannot precede last observed source head
                            source_received: received.max(resume.switch_lsn().retag()),
                            filter_durable: durable,
                            shadow_replay,
                            drain: guards.drain_lsn,
                            emitter_ack: resume_safe,
                            shadow_flush: shadow_flush_lsn.get(),
                        },
                        &resume_floor,
                        &gc_floors,
                    )
                    .await
                };
                match switchover
                    .cross(
                        &mut feed,
                        source_conn.slot.as_deref(),
                        &mut stream,
                        &mut record_sink,
                        &mut segment_sink,
                        status,
                        guards,
                        &probed,
                        open,
                        commit,
                        &mut timeline_stats,
                    )
                    .await
                {
                    Ok(crossed) => {
                        tracing::info!(
                            target: "walshadow",
                            system_id = live_identity.system_id,
                            finished_timeline = crossed.finished_tli,
                            next_timeline = crossed.next_tli,
                            live_timeline = crossed.live_tli,
                            switch_lsn = %format_pg_lsn(crossed.switch_lsn),
                            resume_lsn = %stream.next_lsn(),
                            floor_lsn = %resume_floor.get(),
                            drain_lsn = %guards.drain_lsn,
                            prefix_bytes_verified = crossed.prefix_bytes,
                            slot = source_conn.slot.as_deref(),
                            "crossed source timeline",
                        );
                        if crossed.prefix_origin == PrefixOrigin::Archive {
                            source_recovery.backoff.reset();
                            path = SourcePath::Redial;
                        }
                        history = crossed.history;
                        history_tx.send_replace(Arc::new(history.clone()));
                        crossing.committed();
                        swap.settled();
                    }
                    // Lineage, prefix, and publication proofs need an operator; a
                    // source or storage error is worth another attempt. Every
                    // retryable failure lands before the commit, so the retry
                    // starts from the same proof against an untouched ancestor
                    Err(e) if e.retryable() => {
                        tracing::warn!(
                            target: "walshadow",
                            error = %format!("{e:#}"),
                            reason = e.reason(),
                            stream_timeline = stream.timeline(),
                            "timeline crossing failed — retrying",
                        );
                        crossing.retry_from_source(Instant::now() + SOURCE_SWAP_RETRY);
                    }
                    Err(e) => crossing.park(e, stream.next_lsn().get(), Some(probed.switch_lsn)),
                }
            }
        }
        // Flush pump-side accumulator so partial batches don't strand
        // commits in `decoder_xact.buf` when source goes idle (kill-restart
        // post-catchup quiescence).
        record_sink
            .decoder_xact
            .flush()
            .await
            .context("flush queueing decoder sink")?;
        // Surface a pipeline-stage failure as a clean daemon exit with the
        // root cause rather than a silently pinned watermark. With tenants a
        // failure evicts only its tenant
        for t in &tenants {
            if let Some(msg) = t.fatal() {
                if tenants_cfg.is_none() {
                    anyhow::bail!("decode+insert pipeline failed: {msg}");
                }
                record_sink
                    .decoder_xact
                    .mark_evicted(&t.id, format!("pipeline failed: {msg}"));
            }
        }
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
        // Streaming iterates per walsender message; publishing each time reads
        // /proc and takes the xact buffer lock from the worker
        if !idle && !advanced && last_publish.elapsed() < PUBLISH_MIN_INTERVAL {
            continue;
        }
        last_publish = Instant::now();
        // Re-read rather than reuse the top-of-iteration pair: a crossing commits
        // a new floor and branch mid-iteration, and this is what an operator
        // watches to know the crossing is durable
        let published_floor = cur.floor.max(resume_floor.get());
        let published_branch = history.floor_branch(
            published_floor.get(),
            live_identity.timeline,
            stream.timeline(),
            WAL_SEG_SIZE,
        );
        let now_dispatched = stream.dispatched_lsn();
        let advanced = now_dispatched != prev_dispatched;
        // Pipeline-shaped metrics describe the first tenant; every tenant
        // also reports under its own label, and every followed database
        // under its `database` label
        let primary = tenants.first();
        let (xact_stats, drain_resident, xact_line) = match primary {
            Some(t) => {
                let b = t.xact_buffer.lock().await;
                let stats = b.stats().clone();
                let line = stats.summary();
                let resident = DrainResident::from_buffer(&b);
                (stats, resident, line)
            }
            None => Default::default(),
        };
        let oracle = primary.and_then(|t| t.oracle.as_ref());
        let oracle_line = oracle.map(|o| o.stats.summary()).unwrap_or_default();
        let oracle_stats = oracle.map(|o| o.stats.as_ref());
        // Per database, since each dials its own sockets and one can be down
        // while the rest answer
        let bridge_line = tenants
            .iter()
            .map(|t| t.bridge_line())
            .collect::<Vec<_>>()
            .join(" ");
        let decoder_stats_default = walshadow::decoder_sink::DecoderStats::default();
        let decoder_stats: &walshadow::decoder_sink::DecoderStats =
            primary.map_or(&decoder_stats_default, |t| &*t.decoder_stats);
        let emitter_stats: Option<&walshadow::ch_emitter::EmitterStats> =
            primary.and_then(|t| t.emitter_stats.as_deref());
        let boundary_hold_default = walshadow::boundary_hold::BoundaryHoldStats::default();
        let boundary_hold_stats =
            primary.map_or(&boundary_hold_default, |t| &*t.boundary_hold_stats);
        let shadow_apply_lsn = shadow_agg.min_apply_lsn.map_or(0, Pos::get);
        let lag_bytes = received.get().saturating_sub(shadow_apply_lsn);
        rate_estimator.observe(Instant::now(), received.get());
        let lag_seconds = rate_estimator.seconds_for(lag_bytes);
        // Post-worker snapshots so the metric reflects what the worker
        // drained, not the top-of-iteration values.
        let emitter_ack_for_metric = progress.emitter_ack;
        let drain_for_metric = xact_stats.drain_lsn;
        populate_metrics(
            &metrics,
            received,
            Pos::new(now_dispatched),
            shadow_replay,
            drain_for_metric,
            emitter_ack_for_metric,
            &record_sink.metrics,
            record_sink.decoder_xact.in_flight(),
            record_sink.decoder_xact.processed(),
            &xact_stats,
            drain_resident,
            primary.and_then(|t| t.pipeline.as_ref()).map(|p| &p.budget),
            decoder_stats,
            &swap,
            TimelineView {
                source_system_id: live_identity.system_id,
                source_timeline: stream.timeline(),
                floor_timeline: published_branch,
                shadow_served_timeline: shadow_served_tli,
                shadow_replay_timeline: shadow_agg.replay_timeline.unwrap_or(0),
                floor_lsn: published_floor,
                stats: timeline_stats,
                pause_frontier: pause.frontier(),
                pause_refrozen: pause.refrozen(),
                wedge: crossing.wedge().cloned(),
                promotion,
            },
            ShadowMetricsView {
                apply_lag_bytes: lag_bytes,
                apply_lag_seconds: lag_seconds,
                active_connections: shadow_agg.active_connections as u64,
                dropped_total: shadow_agg.dropped_total,
            },
            boundary_hold_stats,
            tenants
                .iter()
                .flat_map(|t| &t.metrics_dbs)
                .map(DbMetricSources::series)
                .collect(),
            StageCounters {
                emitter: emitter_stats,
                db_name: &|oid| {
                    tenants
                        .iter()
                        .find(|t| t.db_oids.contains(&oid))
                        .map(|t| t.dbname.clone())
                        .unwrap_or_default()
                },
                oracle: [oracle_stats, bootstrap_metrics.as_ref().map(|b| &*b.oracle)],
                bootstrap: bootstrap_metrics.as_ref().map(|b| &b.progress),
                bootstrap_attempt: 0,
                uptime_secs: start_instant.elapsed().as_secs(),
            },
        )
        .await;
        if let Some(tcfg) = &tenants_cfg {
            let view =
                tenant::metrics_view(&tenants, tcfg, &supervisor, &record_sink.decoder_xact).await;
            metrics.update(|snap| snap.tenants = view).await;
        }
        if advanced {
            let new_segs = (now_dispatched - prev_dispatched) / WAL_SEG_SIZE;
            segments_shipped += new_segs;
            prev_dispatched = now_dispatched;
            let ahead = server_end.saturating_sub(dispatched_before);
            let filter = stream.filter();
            let filter_stats = filter.stats();
            let tracker_stats = filter.tracker().stats();
            tracing::info!(
                target: "walshadow",
                segments_shipped,
                last_lsn = format_pg_lsn(now_dispatched).to_string(),
                shadow_apply = format_pg_lsn(shadow_apply_lsn).to_string(),
                source_ahead_bytes = ahead,
                metrics = %record_sink.metrics.summary(),
                kept = filter_stats.kept,
                dropped = filter_stats.dropped,
                relmap_updates = tracker_stats.relmap_updates,
                pg_class_undecoded = tracker_stats.pg_class_writes_undecoded,
                pg_class_oid_in_prefix = tracker_stats.pg_class_writes_oid_in_prefix,
                decoder = %decoder_stats.summary(),
                xact_buffer = %xact_line,
                oracle = %oracle_line,
                bridge = %bridge_line,
                "status",
            );
            if args.max_segments != 0 && segments_shipped >= args.max_segments {
                break "max-segments";
            }
        }
        // Re-arm on ack move; else after 5s of stall with parked xacts dump
        // the xids once. Runs independent of `advanced` so a fully-quiescent
        // pump still surfaces who's holding the slot.
        if emitter_ack_for_metric != last_emitter_ack_observed {
            last_emitter_ack_observed = emitter_ack_for_metric;
            inflight_stall_since = None;
            inflight_stall_logged = false;
        }
        // Ack can pin after transaction leaves buffer
        if let Some((pin, ack_snap)) = tenant::pinning(&tenants).await {
            let since = inflight_stall_since.get_or_insert(Instant::now());
            if !inflight_stall_logged && since.elapsed() >= Duration::from_secs(5) {
                let snap = pin.xact_buffer.lock().await.inflight_snapshot();
                let summary: String = snap
                    .iter()
                    .map(|e| {
                        format!(
                            "xid={} lsn={}..{} heap={} chunk={} bytes={} spill={} cat={} rels=[{}]",
                            e.xid,
                            format_pg_lsn(e.first_lsn),
                            format_pg_lsn(e.last_lsn),
                            e.heap_count,
                            e.chunk_count,
                            e.in_mem_bytes,
                            if e.spilled { "y" } else { "n" },
                            e.catalog_events,
                            e.rels,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                tracing::warn!(
                    target: "walshadow",
                    tenant = %pin.id,
                    xacts_active = xact_stats.xacts_active,
                    emitter_ack_lsn = %emitter_ack_for_metric,
                    drain_lsn = %xact_stats.drain_lsn,
                    source_received = %received,
                    filter_dispatched = format_pg_lsn(now_dispatched).to_string(),
                    inflight = %summary,
                    ack = ?ack_snap,
                    waiting_on = ack_snap.stall_reason().unwrap_or("buffered xacts"),
                    "emitter ack pinned",
                );
                inflight_stall_logged = true;
            }
        } else {
            inflight_stall_since = None;
            inflight_stall_logged = false;
        }
    };
    drop(path);
    tracing::info!(
        target: "walshadow",
        reason = shutdown_reason,
        out_dir = %args.out_dir.display(),
        "stopping",
    );
    let final_timeline = stream.timeline();
    let final_received = stream.next_lsn().get();
    // Close the sink (finishes its write, closes the fsync queue) and drain
    // the fsync task so sealed segments are durable
    segment_sink
        .close()
        .await
        .context("write final filtered segment")?;
    let res = fsync_task.await;
    if res.is_err() || fsync_fatal.is_set() {
        return Err(task_stopped("segment fsync", res, &fsync_fatal));
    }
    drop(gc_floor);
    if let Some(task) = registry_poller.take() {
        task.abort();
    }
    // Drain every tenant: queueing worker so enqueued-but-undispatched
    // records run through decoder + xact_drain, then the pipeline cascade
    // (batcher force-flush → inserters to EndOfStream → ack collector) so no
    // rows are lost + final watermark durable. Nothing else may own a
    // tenant's desc_log.ckpt after the session returns
    let DaemonSinks {
        decoder_xact: mut router,
        ..
    } = record_sink;
    let final_durable = durable_lsn.get();
    let mut drain = Pos::<walshadow::pos::Drain>::new(final_durable.get());
    let mut resume_safe = Pos::<walshadow::pos::ResumeSafe>::new(final_durable.get());
    let mut first_err = None;
    for t in tenants {
        let sink = router.detach(&t.id).map(|r| r.sink);
        let tenant_drain = t.xact_buffer.lock().await.stats().drain_lsn;
        match t.shutdown(sink).await {
            Ok(safe) => {
                drain = drain.min(tenant_drain);
                resume_safe = resume_safe.min(safe);
            }
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e.context("drain tenants on shutdown"));
    }
    let shadow_replay = shadow_replay_lsn.get();
    let durable = durable_lsn.get();
    manifest::write(
        &args.spill_dir,
        &resume_manifest(
            &history,
            &live_identity,
            resume_floor.get(),
            manifest::ShadowFloor::new(shadow_toast, shadow_replay.get(), shadow_replay_seed),
            final_timeline,
            manifest::LsnSet {
                source_received: Pos::new(final_received),
                filter_durable: durable,
                shadow_replay,
                drain,
                emitter_ack: resume_safe,
                shadow_flush: shadow_flush_lsn.get(),
            },
        ),
    )
    .await
    .context("write shutdown resume manifest")?;
    shadow_lifecycle.shutdown().await;
    tasks.shutdown().await
}

/// Session tasks meant to run until shutdown; any exit before it is fatal
#[derive(Default)]
pub(crate) struct SessionTasks {
    set: tokio::task::JoinSet<()>,
    names: ahash::HashMap<tokio::task::Id, &'static str>,
}

impl SessionTasks {
    pub(crate) fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = ()> + Send + 'static,
    ) {
        let id = self.set.spawn(task).id();
        self.names.insert(id, name);
    }

    /// Supervise a task spawned elsewhere, aborting it with the set
    pub(crate) fn adopt(&mut self, name: &'static str, handle: tokio::task::JoinHandle<()>) {
        let handle = tokio_util::task::AbortOnDropHandle::new(handle);
        self.spawn(name, async move {
            if let Err(e) = handle.await
                && e.is_panic()
            {
                std::panic::resume_unwind(e.into_panic());
            }
        });
    }

    fn name(&self, id: tokio::task::Id) -> &'static str {
        self.names.get(&id).copied().unwrap_or("session")
    }

    /// First task to stop, as the error naming it. `None` once the set is empty
    async fn exited(&mut self) -> Option<anyhow::Error> {
        let res = self.set.join_next_with_id().await?;
        Some(match res {
            Ok((id, ())) => anyhow::anyhow!("{} task exited", self.name(id)),
            Err(e) => anyhow::anyhow!("{} task failed: {e}", self.name(e.id())),
        })
    }

    /// Abort what still runs, surfacing a task that panicked
    async fn shutdown(mut self) -> Result<()> {
        self.set.abort_all();
        while let Some(res) = self.set.join_next_with_id().await {
            if let Err(e) = res
                && e.is_panic()
            {
                anyhow::bail!("{} task failed: {e}", self.name(e.id()));
            }
        }
        Ok(())
    }
}

/// Error for a task that stopped before its shutdown, preferring the fatal
/// it set on the way out
pub(crate) fn task_stopped(
    name: &str,
    res: Result<(), tokio::task::JoinError>,
    fatal: &walshadow::pipeline::Fatal,
) -> anyhow::Error {
    match (fatal.message(), res) {
        (Some(msg), _) => anyhow::anyhow!("{name} failed: {msg}"),
        (None, Ok(())) => anyhow::anyhow!("{name} task exited"),
        (None, Err(e)) => anyhow::anyhow!("{name} task failed: {e}"),
    }
}

async fn stand_in_primary(shadow: &Arc<walshadow::shadow::Shadow>, absent: &str) -> Option<String> {
    if probe_blocking(shadow, |s| s.has_database()).await != Some(false) {
        return None;
    }
    let want = absent.to_owned();
    probe_blocking(shadow, move |s| {
        s.probe_one(
            "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate \
             ORDER BY datname = 'postgres' DESC, oid LIMIT 1",
        )
    })
    .await
    .filter(|name| !name.is_empty() && *name != want)
}

#[cfg(test)]
mod tests {
    use super::*;
    use walshadow::ch_emitter::InstanceConfig;

    #[tokio::test]
    async fn boot_redials_clickhouse_that_drops_connections() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = EmitterConfig {
            conn: InstanceConfig {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
                ..Default::default()
            },
            ..EmitterConfig::default()
        };
        let accepts = async {
            for _ in 0..2 {
                drop(listener.accept().await.unwrap());
            }
        };
        tokio::select! {
            _ = ensure_boot_database_waiting(&cfg) => panic!("boot proceeded without ClickHouse"),
            () = accepts => {}
        }
    }

    #[tokio::test]
    async fn session_tasks_name_the_task_that_stopped() {
        let mut tasks = SessionTasks::default();
        tasks.spawn("idle", std::future::pending());
        tasks.spawn("quits", async {});
        let err = tasks.exited().await.unwrap().to_string();
        assert!(err.contains("quits task exited"), "{err}");
        tasks.spawn("panics", async { panic!("boom") });
        let err = tasks.exited().await.unwrap().to_string();
        assert!(
            err.contains("panics task failed") && err.contains("boom"),
            "{err}"
        );
        tasks.shutdown().await.expect("idle task aborts cleanly");
    }
}
