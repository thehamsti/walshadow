//! Direct base-backup source. Wraps wal-rus's
//! `pg::replication::base_backup::run_base_backup` to drive walshadow's
//! [`BackupSource`] trait. Tablespace symlinks ride inside the data-dir
//! archive in PG protocol order, surfacing as `FileKind::Symlink`.
//!
//! Single-pass: seed the page-walk
//! [`CatalogMap`](crate::backfill::backup_page_walk::CatalogMap) from source PG
//! before this runs so all routing decisions are known when bytes land.
//! See [architecture/bootstrap.md](../../architecture/bootstrap.md).
//!
//! A standby can serve the data files while the backup window's WAL streams
//! from the primary into `pg_wal` beside them ([`WalLeg`]).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use walrus::pg::backup::format_pg_lsn;
use walrus::pg::replication::base_backup::{
    BackupEvent, BaseBackupOpts, ChannelReader, run_base_backup,
};
use walrus::pg::replication::conn::{PgConfig, ReplicationConn};
use walrus::pg::wal::segment::SegmentName;

use crate::backfill::backup_source::{
    BackupSink, BackupSource, EndInfo, PumpStats, PumpTarget, StartInfo, pump_tar_to_sink,
};
use crate::record::WAL_SEG_SIZE;
use crate::source::source_feed::{SourceEvent, SourceFeed, StandbyStatus};
use crate::source::timeline::history_filename;

/// Replication-protocol BASE_BACKUP issuer
pub struct DirectSource {
    pub source: PgConfig,
    pub opts: BaseBackupOpts,
    pub wal_leg: Option<WalLeg>,
}

/// Backup-window WAL streamed live into the landed `pg_wal`, the
/// `pg_basebackup -X stream` shape, for a BASE_BACKUP run with `WAL false`.
/// A standby recycles the window's segments long before a multi-hour copy
/// ends; the primary's slot keeps them, and reading them as they are written
/// costs the primary no disk reads
#[derive(Clone)]
pub struct WalLeg {
    /// Server holding the window's WAL, normally the slot's primary
    pub source: PgConfig,
}

impl DirectSource {
    pub fn new(source: PgConfig, opts: BaseBackupOpts) -> Self {
        Self {
            source,
            opts,
            wal_leg: None,
        }
    }

    pub fn with_wal_leg(mut self, leg: WalLeg) -> Self {
        self.wal_leg = Some(leg);
        self
    }
}

#[async_trait]
impl BackupSource for DirectSource {
    async fn run(
        self: Box<Self>,
        data_dir: PathBuf,
        sink: Arc<dyn BackupSink>,
        stats: Arc<PumpStats>,
    ) -> Result<(StartInfo, EndInfo)> {
        let DirectSource {
            source,
            opts,
            wal_leg,
        } = *self;
        ensure!(
            wal_leg.is_none() || !opts.wal,
            "DirectSource: a WAL leg replaces the archive's WAL, request WAL false"
        );

        let conn = ReplicationConn::connect(&source)
            .await
            .context("DirectSource: connect to source PG for BASE_BACKUP")?;

        // depth=8 matches wal-rus internal sizing; producer back-pressures
        // on slow archive drain
        let (tx, mut rx) = mpsc::channel::<Result<BackupEvent>>(8);
        let pump = tokio::spawn(async move {
            run_base_backup(conn, opts, tx).await;
        });

        let mut start: Option<StartInfo> = None;
        let mut end: Option<EndInfo> = None;
        let pg_wal = data_dir.join("pg_wal");
        let target = PumpTarget::new(data_dir, sink.clone(), stats);
        let (stop_tx, stop_rx) = watch::channel(None);
        let mut leg: Option<tokio_util::task::AbortOnDropHandle<Result<u64>>> = None;

        while let Some(ev) = rx.recv().await {
            let ev = ev.context("DirectSource: BASE_BACKUP event channel")?;
            match ev {
                BackupEvent::Start(s) => {
                    let s = StartInfo {
                        start_lsn: s.start_lsn,
                        timeline: s.timeline,
                        tablespaces: s.tablespaces,
                    };
                    sink.start(&s).await?;
                    if let Some(wal_leg) = &wal_leg {
                        leg = Some(tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
                            stream_window(
                                wal_leg.source.clone(),
                                s.timeline,
                                s.start_lsn,
                                pg_wal.clone(),
                                stop_rx.clone(),
                            ),
                        )));
                    }
                    start = Some(s);
                }
                BackupEvent::Archive { meta, body } => {
                    tracing::debug!(
                        target = "walshadow::backup_source_direct",
                        name = %meta.name,
                        oid = meta.oid,
                        "archive open",
                    );
                    // A leg that ends before the stop signal has failed; don't
                    // copy the rest of the cluster to learn that at the end
                    match leg.as_mut() {
                        Some(handle) => tokio::select! {
                            res = drive_archive(body, &target) => res?,
                            res = handle => {
                                let err = match res {
                                    Ok(Ok(_)) => anyhow::anyhow!("returned before the backup finished"),
                                    Ok(Err(e)) => e,
                                    Err(e) => e.into(),
                                };
                                return Err(err.context("DirectSource: backup-window WAL leg ended early"));
                            }
                        },
                        None => drive_archive(body, &target).await?,
                    }
                }
                BackupEvent::Finish(e) => {
                    let e = EndInfo {
                        end_lsn: e.end_lsn,
                        timeline: e.timeline,
                    };
                    if let Some(handle) = leg.take() {
                        stop_tx.send_replace(Some(e.end_lsn));
                        let through = handle
                            .await
                            .context("DirectSource: WAL leg join")?
                            .context("DirectSource: backup-window WAL leg")?;
                        tracing::info!(
                            target: "walshadow::backup_source_direct",
                            end_lsn = %format_pg_lsn(e.end_lsn),
                            through = %format_pg_lsn(through),
                            "backup-window WAL streamed into pg_wal",
                        );
                    }
                    sink.finish(&e).await?;
                    end = Some(e);
                }
            }
        }

        if let Err(e) = pump.await {
            bail!("DirectSource: BASE_BACKUP pump task panicked: {e:#}");
        }

        let start = start.ok_or_else(|| anyhow::anyhow!("DirectSource: no StartInfo emitted"))?;
        let end = end.ok_or_else(|| anyhow::anyhow!("DirectSource: no EndInfo emitted"))?;
        Ok((start, end))
    }
}

/// Stream `[start_lsn, end)` from `source` into segment files under `pg_wal`,
/// `end` arriving on `stop` once the backup finishes. Slotless: the caller's
/// slot already retains the window, and a slotless status never moves it.
/// Returns the position written through
async fn stream_window(
    source: PgConfig,
    timeline: u32,
    start_lsn: u64,
    pg_wal: PathBuf,
    mut stop: watch::Receiver<Option<u64>>,
) -> Result<u64> {
    tokio::fs::create_dir_all(&pg_wal)
        .await
        .with_context(|| format!("create {}", pg_wal.display()))?;
    let mut feed = SourceFeed::connect(&source)
        .await
        .context("connect WAL source")?;
    // The tar would have carried it; recovery on a promoted branch needs it
    if timeline > 1
        && let Some(history) = feed.timeline_history(timeline).await?
    {
        let path = pg_wal.join(history_filename(timeline));
        tokio::fs::write(&path, history)
            .await
            .with_context(|| format!("write {}", path.display()))?;
    }
    let begin = start_lsn - start_lsn % WAL_SEG_SIZE;
    feed.start_physical_replication(None, begin, timeline)
        .await
        .with_context(|| format!("START_REPLICATION at {}", format_pg_lsn(begin)))?;
    let mut seg = SegmentName {
        timeline,
        log_id: (begin >> 32) as u32,
        seg_no: ((begin & 0xFFFF_FFFF) / WAL_SEG_SIZE) as u32,
    };
    let mut page = vec![0u8; WAL_SEG_SIZE as usize];
    let mut next = begin;
    let mut frame = Vec::new();
    loop {
        if stop.borrow_and_update().is_some_and(|end| next >= end) {
            break;
        }
        let event = tokio::select! {
            biased;
            res = stop.changed() => {
                ensure!(res.is_ok(), "backup ended without a stop position");
                continue;
            }
            res = feed.next_event(StandbyStatus::collapsed(next), &mut frame) => res?,
        };
        let chunk = match event {
            SourceEvent::Wal(chunk) => chunk,
            SourceEvent::TimelineEnd => {
                bail!("source ended timeline {timeline} inside the backup window; it was promoted")
            }
            SourceEvent::Shutdown => bail!("source shut down inside the backup window"),
        };
        ensure!(
            chunk.start_lsn == next,
            "WAL frame at {} where {} was expected",
            format_pg_lsn(chunk.start_lsn),
            format_pg_lsn(next),
        );
        let mut data = chunk.data;
        while !data.is_empty() {
            let off = (next - seg.start_lsn(WAL_SEG_SIZE)) as usize;
            let n = data.len().min(page.len() - off);
            page[off..off + n].copy_from_slice(&data[..n]);
            data = &data[n..];
            next += n as u64;
            if off + n == page.len() {
                write_segment(&pg_wal, &seg, &page).await?;
                page.fill(0);
                seg = seg.next(WAL_SEG_SIZE);
            }
        }
    }
    // Zero tail past the last record, as a walreceiver's open segment has
    if next > seg.start_lsn(WAL_SEG_SIZE) {
        write_segment(&pg_wal, &seg, &page).await?;
    }
    Ok(next)
}

async fn write_segment(pg_wal: &Path, seg: &SegmentName, page: &[u8]) -> Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let name = seg.format();
    let tmp = pg_wal.join(format!("{name}.tmp"));
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;
    file.write_all(page).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&tmp, pg_wal.join(&name))
        .await
        .with_context(|| format!("rename {name} into pg_wal"))?;
    Ok(())
}

/// Drain one archive body through `pump_tar_to_sink`. wal-rus's
/// `ChannelReader` is `AsyncRead`, so tokio_tar takes it directly, no
/// SyncIoBridge / spawn_blocking.
async fn drive_archive(
    body: mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    target: &PumpTarget,
) -> Result<()> {
    let reader = ChannelReader::new(body);
    let mut archive = tokio_tar::Archive::new(reader);
    pump_tar_to_sink(&mut archive, target)
        .await
        .context("DirectSource: tar unpack")?;
    Ok(())
}
