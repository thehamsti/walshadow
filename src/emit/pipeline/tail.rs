//! Reusable insert tail: batcher + inserter pool + ack collector.
//!
//! Both the WAL pipeline
//! ([`PipelineConfig::spawn`](crate::emit::pipeline::PipelineConfig::spawn)) and
//! greenfield bootstrap ([`bootstrap::drain`](crate::emit::pipeline::bootstrap))
//! feed this identical tail — one shipping path so bootstrap inherits the
//! N-connection inserter pool, reconnect + retry, the durable watermark, and
//! backpressure for free.
//!
//! Drains in cascade once every `BatcherMsg` sender drops: batcher
//! final-flushes and exits → inserters drain to `EndOfStream` and exit → ack
//! collector exits.

use std::sync::Arc;

use clickhouse_c::Allocator;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::task::AbortOnDropHandle;

use crate::ch::EmitterError;
use crate::config::ResolvedConfig;
use crate::decode::heap_decoder::HeapOp;
use crate::destination::snowflake::runtime::SnowflakeRuntime;
use crate::destination::snowflake::types::{SnowflakeRow, TableSchema};
use crate::emit::ch_emitter::{EmitterConfig, EmitterStats};
use crate::emit::pipeline::ack::{self, AckHandle};
use crate::emit::pipeline::batcher::{self, BatcherConfig, BatcherMsg, InsertBatch};
use crate::emit::pipeline::batcher::{RoutedRow, RowChunk};
use crate::emit::pipeline::inserter;
use crate::emit::pipeline::resolver::{self, ResolvedBatch};
use crate::emit::pipeline::{DEFAULT_PIPELINE_FLUSH, Fatal};
use crate::ops::oracle::Oracle;
use crate::pos::{EmitterAck, Monotone};
use crate::schema::RelName;
use ahash::{HashMap, HashMapExt};
use std::time::Duration;
use tokio::time::Instant;

/// Spawned tail stages; holding this keeps the tasks owned by the caller.
pub struct TailParts {
    collector: AbortOnDropHandle<()>,
    batcher: AbortOnDropHandle<()>,
    resolvers: Vec<AbortOnDropHandle<()>>,
    inserters: Vec<AbortOnDropHandle<()>>,
}

impl TailParts {
    /// Await the drain cascade. Call only after every producer-held `msg_tx`
    /// and `AckHandle` clone has dropped, else the batcher never sees its
    /// channel close and this hangs.
    pub async fn join(self) {
        let _ = self.batcher.await;
        for h in self.resolvers {
            let _ = h.await;
        }
        for h in self.inserters {
            let _ = h.await;
        }
        let _ = self.collector.await;
    }

    /// Bootstrap completion + teardown: seal partial batches, wait every seq
    /// < `through` durable on CH, then drain the tail. Consumes producer
    /// handles so the drop-before-join ordering can't be gotten wrong; `fatal`
    /// short-circuits a CH outage instead of hanging.
    pub async fn finish(
        self,
        msg_tx: mpsc::Sender<BatcherMsg>,
        ack: AckHandle,
        through: u64,
        fatal: &Fatal,
    ) -> Result<(), String> {
        flush_and_prove(&msg_tx, &ack, through, fatal).await?;
        drop(msg_tx);
        drop(ack);
        self.join().await;
        if let Some(msg) = fatal.message() {
            return Err(msg);
        }
        Ok(())
    }
}

async fn flush_and_prove(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    ack: &AckHandle,
    through: u64,
    fatal: &Fatal,
) -> Result<(), String> {
    let (reply_tx, reply_rx) = oneshot::channel();
    if msg_tx.send(BatcherMsg::FlushAll(reply_tx)).await.is_err() {
        return Err(fatal
            .message()
            .unwrap_or_else(|| "tail closed before flush".into()));
    }
    // Prefer concurrent fatal over successful completion
    tokio::select! {
        biased;
        _ = fatal.wait() => {
            return Err(fatal.message().unwrap_or_else(|| "tail fatal during flush".into()));
        }
        r = reply_rx => r.map_err(|_| "batcher dropped flush ack".to_string())?,
    }
    tokio::select! {
        biased;
        _ = fatal.wait() => {
            return Err(fatal.message().unwrap_or_else(|| "tail fatal during drain".into()));
        }
        r = ack.wait_through(through) => r.map_err(|e| format!("tail drain: {e}"))?,
    }
    Ok(())
}

/// Tail plus the producer handles a single bootstrap leg holds
///
/// The leg owns its seq space, so the durable watermark is throwaway:
/// completion is `wait_through(next_seq)`, not a resume position
pub struct OwnedTail {
    pub msg_tx: mpsc::Sender<BatcherMsg>,
    pub ack: AckHandle,
    parts: TailParts,
    fatal: Fatal,
    /// Prefix for the errors this leg reports
    context: &'static str,
}

impl OwnedTail {
    #[cfg(test)]
    pub(crate) fn null() -> Self {
        let (msg_tx, ack, parts) = spawn_null(Arc::new(Monotone::new(0)));
        Self {
            msg_tx,
            ack,
            parts,
            fatal: Fatal::new(),
            context: "test",
        }
    }

    pub async fn spawn(
        emitter: &EmitterConfig,
        inserter_pool_size: usize,
        stats: Arc<EmitterStats>,
        fatal: Fatal,
        config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
        oracle: Option<Arc<crate::ops::oracle::Oracle>>,
        context: &'static str,
    ) -> Result<Self, String> {
        let (msg_tx, ack, parts) = spawn_with_config(
            emitter,
            inserter_pool_size,
            stats,
            Arc::new(Monotone::<EmitterAck>::new(0)),
            fatal.clone(),
            config_rx,
            oracle,
        )
        .await
        .map_err(|e| format!("{context}: spawn insert tail: {e}"))?;
        Ok(Self {
            msg_tx,
            ack,
            parts,
            fatal,
            context,
        })
    }

    /// [`Self::finish`] without closing the tail: what licenses recording
    /// resume progress mid-pass
    pub async fn checkpoint(&self, through: u64) -> Result<(), String> {
        flush_and_prove(&self.msg_tx, &self.ack, through, &self.fatal)
            .await
            .map_err(|m| format!("{}: {m}", self.context))
    }

    /// Seal batches and prove every seq below `through` durable
    pub async fn finish(self, through: u64) -> Result<(), String> {
        self.parts
            .finish(self.msg_tx, self.ack, through, &self.fatal)
            .await
            .map_err(|m| format!("{}: {m}", self.context))
    }

    /// Failed-leg teardown: dropping the last producer handles closes the
    /// batcher, which final-flushes and cascades the inserters + collector
    /// down. Bounded by the inserters' retry policy (a CH outage trips their
    /// fatal, not a hang)
    pub async fn quiesce(self) {
        drop(self.msg_tx);
        drop(self.ack);
        self.parts.join().await;
    }
}

/// Stand up the tail: ack collector, inserter pool (`n` connections),
/// batcher. Returns the `BatcherMsg` sender + [`AckHandle`] (clone into
/// producers) and join handles. Fails only if an inserter connection can't
/// open — inserters spin up first (consume-only) so a connect failure aborts
/// before any other stage starts.
pub async fn spawn(
    emitter: &EmitterConfig,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    spawn_with_config(
        emitter,
        inserter_pool_size,
        stats,
        emitter_ack,
        fatal,
        None,
        None,
    )
    .await
}

/// Metrics-only tail: ack collector + one swallow task, zero CH
/// connections. Every routed row acks at swallow (permits release on
/// drop) so nothing can pin the watermark; `FlushAll` replies
/// immediately — nothing buffers. Keeps the placed/acked protocol
/// identical to the CH tail, so reorder/decode stages run unchanged.
pub fn spawn_null(
    emitter_ack: Arc<Monotone<EmitterAck>>,
) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(emitter_ack);
    let collector = AbortOnDropHandle::new(collector);
    let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let swallow_ack = ack.clone();
    let batcher = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            match msg {
                BatcherMsg::Row(r) => swallow_ack.acked(vec![(r.seq, 1)]),
                BatcherMsg::Rows(chunk) => {
                    let mut counts: HashMap<u64, u64> = HashMap::new();
                    for r in &chunk.rows {
                        *counts.entry(r.seq).or_insert(0) += 1;
                    }
                    swallow_ack.acked(counts.into_iter().collect());
                }
                BatcherMsg::FlushAll(reply) => {
                    let _ = reply.send(());
                }
            }
        }
    });
    (
        msg_tx,
        ack,
        TailParts {
            collector,
            batcher: AbortOnDropHandle::new(batcher),
            resolvers: Vec::new(),
            inserters: Vec::new(),
        },
    )
}

/// [`spawn`] plus a live config receiver: the batcher re-reads
/// budgets/flush and the inserter pool re-reads compression/retry from it on
/// each republish. `None` == boot values only (bootstrap + tests use [`spawn`]).
pub async fn spawn_with_config(
    emitter: &EmitterConfig,
    inserter_pool_size: usize,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
    config_rx: Option<watch::Receiver<Arc<ResolvedConfig>>>,
    oracle: Option<Arc<crate::ops::oracle::Oracle>>,
) -> Result<(mpsc::Sender<BatcherMsg>, AckHandle, TailParts), EmitterError> {
    if let Some(runtime) = emitter.snowflake.clone() {
        return Ok(spawn_snowflake_tail(
            emitter,
            runtime,
            stats,
            emitter_ack,
            fatal,
            oracle,
        ));
    }
    let n = inserter_pool_size.max(1);

    let (ack, collector) = ack::spawn(emitter_ack);
    let collector = AbortOnDropHandle::new(collector);

    // Rows and FlushAll share one FIFO channel so a flush can't overtake rows
    // enqueued before it
    let (msg_tx, msg_rx) = mpsc::channel::<BatcherMsg>(256);
    let (batches_tx, batches_rx) = async_channel::bounded::<InsertBatch>((n * 2).max(4));
    // One resolved batch per inserter is what keeps every one of them fed
    let (resolved_tx, resolved_rx) = async_channel::bounded::<ResolvedBatch>(n);

    let inserters = inserter::spawn_pool(
        n,
        emitter,
        resolved_rx,
        ack.clone(),
        stats.clone(),
        fatal.clone(),
        inserter::PoolOptions {
            config_rx: config_rx.clone(),
        },
    )
    .await?;

    // As wide as the bridge: a narrower pool leaves shadow workers idle, a
    // wider one only queues on their sockets
    let resolvers = resolver::spawn_pool(
        oracle.as_ref().map_or(1, |o| o.concurrency()),
        batches_rx,
        resolved_tx,
        resolver::ResolverOptions {
            oracle,
            retry: emitter.retry.clone(),
            stats: stats.clone(),
            fatal: fatal.clone(),
            config_rx: config_rx.clone(),
        },
    );

    // Boot fallback for the batcher; the live path re-reads from `config_rx`.
    let flush_timeout = if emitter.flush_timeout.is_zero() {
        DEFAULT_PIPELINE_FLUSH
    } else {
        emitter.flush_timeout
    };
    let batcher = batcher::spawn(
        msg_rx,
        batches_tx,
        BatcherConfig {
            row_budget: emitter.row_budget,
            byte_budget: emitter.byte_budget,
            inserters: n,
            flush_timeout,
        },
        Allocator::global(&mimalloc::MiMalloc),
        fatal,
        stats.clone(),
        config_rx,
    );

    Ok((
        msg_tx,
        ack,
        TailParts {
            collector,
            batcher: AbortOnDropHandle::new(batcher),
            resolvers: resolvers.into_iter().map(AbortOnDropHandle::new).collect(),
            inserters: inserters.into_iter().map(AbortOnDropHandle::new).collect(),
        },
    ))
}

struct SnowflakeLookup {
    rel: Arc<crate::schema::RelDescriptor>,
    route: Arc<crate::emit::route::RouteSnapshot>,
    schema: TableSchema,
    lineage: (u64, u64),
}

struct SnowflakeBatch {
    snapshot: Option<String>,
    schema: TableSchema,
    rows: Vec<SnowflakeRow>,
    per_seq: HashMap<u64, u64>,
    permits: Vec<Arc<crate::budget::MemoryPermit>>,
    bytes: usize,
    deadline: Instant,
}

enum SnowflakeTableMsg {
    Row(RoutedRow, Option<Arc<crate::budget::MemoryPermit>>),
    Flush(oneshot::Sender<()>),
}

fn spawn_snowflake_tail(
    emitter: &EmitterConfig,
    runtime: Arc<SnowflakeRuntime>,
    stats: Arc<EmitterStats>,
    emitter_ack: Arc<Monotone<EmitterAck>>,
    fatal: Fatal,
    oracle: Option<Arc<Oracle>>,
) -> (mpsc::Sender<BatcherMsg>, AckHandle, TailParts) {
    let (ack, collector) = ack::spawn(emitter_ack);
    let (msg_tx, mut rx) = mpsc::channel::<BatcherMsg>(256);
    let worker_ack = ack.clone();
    let snapshot_operations = emitter.snowflake_snapshots.clone();
    let row_budget = runtime.config.batch_rows;
    let byte_budget = runtime.config.batch_bytes;
    let timeout = Duration::from_millis(runtime.config.flush_interval_ms);
    let worker = tokio::spawn(async move {
        let mut senders: HashMap<RelName, mpsc::Sender<SnowflakeTableMsg>> = HashMap::new();
        let mut workers = tokio::task::JoinSet::new();
        while let Some(msg) = rx.recv().await {
            match msg {
                BatcherMsg::Row(row) => {
                    if let Err(e) = snowflake_route(
                        &mut senders,
                        &mut workers,
                        &runtime,
                        &snapshot_operations,
                        &stats,
                        oracle.as_ref(),
                        &worker_ack,
                        &fatal,
                        row,
                        None,
                        row_budget,
                        byte_budget,
                        timeout,
                    )
                    .await
                    {
                        fatal.set(format!("snowflake tail route: {e}"));
                        break;
                    }
                }
                BatcherMsg::Rows(RowChunk { rows, permit }) => {
                    let mut error = None;
                    for row in rows {
                        if let Err(e) = snowflake_route(
                            &mut senders,
                            &mut workers,
                            &runtime,
                            &snapshot_operations,
                            &stats,
                            oracle.as_ref(),
                            &worker_ack,
                            &fatal,
                            row,
                            permit.clone(),
                            row_budget,
                            byte_budget,
                            timeout,
                        )
                        .await
                        {
                            error = Some(e);
                            break;
                        }
                    }
                    if let Some(e) = error {
                        fatal.set(format!("snowflake tail route: {e}"));
                        break;
                    }
                }
                BatcherMsg::FlushAll(reply) => {
                    let mut replies = Vec::with_capacity(senders.len());
                    let mut error = None;
                    for tx in senders.values() {
                        let (done_tx, done_rx) = oneshot::channel();
                        if tx.send(SnowflakeTableMsg::Flush(done_tx)).await.is_err() {
                            error = Some("table worker closed before flush".to_string());
                            break;
                        }
                        replies.push(done_rx);
                    }
                    if error.is_none() {
                        for result in futures::future::join_all(replies).await {
                            if result.is_err() {
                                error = Some("table worker failed during flush".into());
                                break;
                            }
                        }
                    }
                    if let Some(e) = error {
                        fatal.set(format!("snowflake tail flush: {e}"));
                        break;
                    }
                    let _ = reply.send(());
                }
            }
        }
        drop(senders);
        while let Some(result) = workers.join_next().await {
            if let Err(e) = result {
                fatal.set(format!("snowflake table worker panicked: {e}"));
            }
        }
    });
    (
        msg_tx,
        ack,
        TailParts {
            collector: AbortOnDropHandle::new(collector),
            batcher: AbortOnDropHandle::new(worker),
            resolvers: Vec::new(),
            inserters: Vec::new(),
        },
    )
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_route(
    senders: &mut HashMap<RelName, mpsc::Sender<SnowflakeTableMsg>>,
    workers: &mut tokio::task::JoinSet<()>,
    runtime: &Arc<SnowflakeRuntime>,
    snapshot_operations: &HashMap<RelName, String>,
    stats: &Arc<EmitterStats>,
    oracle: Option<&Arc<Oracle>>,
    ack: &AckHandle,
    fatal: &Fatal,
    row: RoutedRow,
    chunk_permit: Option<Arc<crate::budget::MemoryPermit>>,
    row_budget: usize,
    byte_budget: usize,
    timeout: Duration,
) -> Result<(), String> {
    let key = row.rel.rel_name.clone();
    let tx = if let Some(tx) = senders.get(&key) {
        tx.clone()
    } else {
        let (tx, rx) = mpsc::channel(32);
        workers.spawn(snowflake_table_worker(
            runtime.clone(),
            snapshot_operations.get(&key).cloned(),
            stats.clone(),
            oracle.cloned(),
            ack.clone(),
            fatal.clone(),
            rx,
            row_budget,
            byte_budget,
            timeout,
        ));
        senders.insert(key, tx.clone());
        tx
    };
    tx.send(SnowflakeTableMsg::Row(row, chunk_permit))
        .await
        .map_err(|_| "Snowflake table worker closed".to_string())
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_table_worker(
    runtime: Arc<SnowflakeRuntime>,
    snapshot: Option<String>,
    stats: Arc<EmitterStats>,
    oracle: Option<Arc<Oracle>>,
    ack: AckHandle,
    fatal: Fatal,
    mut rx: mpsc::Receiver<SnowflakeTableMsg>,
    row_budget: usize,
    byte_budget: usize,
    timeout: Duration,
) {
    let mut batch: Option<SnowflakeBatch> = None;
    let mut lookup: Option<SnowflakeLookup> = None;
    let mut deliveries = tokio::task::JoinSet::new();
    let max_parallel = runtime.config.channels_per_table;
    loop {
        let next = batch.as_ref().map(|b| b.deadline);
        tokio::select! {
            msg=rx.recv()=>match msg {
                Some(SnowflakeTableMsg::Row(row,permit))=>{
                    if let Err(e)=snowflake_add(&runtime,snapshot.as_deref(),oracle.as_ref(),&mut batch,&mut lookup,row,permit,timeout).await {
                        fatal.set(format!("snowflake table: {e}"));break;
                    }
                    if batch.as_ref().is_some_and(|b| b.rows.len() >= row_budget || b.bytes >= byte_budget)
                        && let Err(e)=snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await {
                            fatal.set(format!("snowflake table delivery: {e}"));break;
                        }
                }
                Some(SnowflakeTableMsg::Flush(reply))=>{
                    let result=async {
                        snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await?;
                        snowflake_drain_deliveries(&mut deliveries).await
                    }.await;
                    if let Err(e)=result {fatal.set(format!("snowflake table flush: {e}"));break;}
                    let _=reply.send(());
                }
                None=>{
                    let result=async {
                        snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await?;
                        snowflake_drain_deliveries(&mut deliveries).await
                    }.await;
                    if let Err(e)=result {fatal.set(format!("snowflake table final flush: {e}"));}
                    break;
                }
            },
            _=async {if let Some(at)=next {tokio::time::sleep_until(at).await}},if next.is_some()=>{
                if let Err(e)=snowflake_start_delivery(&runtime,&stats,&ack,&mut batch,&mut deliveries,max_parallel).await {fatal.set(format!("snowflake table deadline: {e}"));break;}
            },
            result=deliveries.join_next(),if !deliveries.is_empty()=>{
                if let Err(e)=snowflake_delivery_result(result) {fatal.set(format!("snowflake table delivery: {e}"));break;}
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn snowflake_add(
    runtime: &SnowflakeRuntime,
    snapshot: Option<&str>,
    oracle: Option<&Arc<Oracle>>,
    batch: &mut Option<SnowflakeBatch>,
    lookup: &mut Option<SnowflakeLookup>,
    mut row: RoutedRow,
    chunk_permit: Option<Arc<crate::budget::MemoryPermit>>,
    timeout: Duration,
) -> Result<(), String> {
    if !lookup.as_ref().is_some_and(|cached| {
        Arc::ptr_eq(&cached.rel, &row.rel) && Arc::ptr_eq(&cached.route, &row.route)
    }) {
        let schema = runtime
            .schema_for(&row.rel, &row.route)
            .await
            .map_err(|e| e.to_string())?;
        let mut lineage = runtime.lineage(&row.rel).await.map_err(|e| e.to_string())?;
        if let Some(operation) = snapshot {
            lineage.1 = runtime
                .state
                .generation(operation)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "missing snapshot generation".to_owned())?
                .generation_id;
        }
        *lookup = Some(SnowflakeLookup {
            rel: row.rel.clone(),
            route: row.route.clone(),
            schema,
            lineage,
        });
    }
    let cached = lookup.as_ref().expect("lookup populated");
    let schema = cached.schema.clone();
    let (incarnation, generation) = cached.lineage;
    if let Some(oracle) = oracle {
        oracle
            .render_text_columns(&mut row.committed, &row.rel)
            .await
            .map_err(|e| e.to_string())?;
    }
    let encoded = match row.committed.decoded.op {
        HeapOp::Insert => vec![SnowflakeRow::from_committed_with_lineage(
            &schema,
            &row.committed,
            0,
            false,
            &runtime.source_identity,
            incarnation,
            generation,
        )?],
        HeapOp::Update | HeapOp::HotUpdate => SnowflakeRow::update_rows_with_lineage(
            &schema,
            &row.committed,
            0,
            &runtime.source_identity,
            incarnation,
            generation,
        )?,
        HeapOp::Delete => vec![SnowflakeRow::from_committed_with_lineage(
            &schema,
            &row.committed,
            0,
            true,
            &runtime.source_identity,
            incarnation,
            generation,
        )?],
        HeapOp::Truncate => return Err("TRUNCATE requires a Snowflake generation barrier".into()),
    };
    let bytes = row.committed.decoded.approx_bytes();
    let held = batch.get_or_insert_with(|| SnowflakeBatch {
        snapshot: snapshot.map(str::to_owned),
        schema: schema.clone(),
        rows: Vec::new(),
        per_seq: HashMap::new(),
        permits: Vec::new(),
        bytes: 0,
        deadline: Instant::now() + timeout,
    });
    if held.schema != schema {
        return Err("Snowflake schema changed inside open batch".into());
    }
    held.rows.extend(encoded);
    *held.per_seq.entry(row.seq).or_insert(0) += 1;
    held.bytes = held.bytes.saturating_add(bytes);
    if let Some(p) = row.value_permit {
        held.permits.push(p);
    }
    if let Some(p) = chunk_permit {
        held.permits.push(p);
    }
    Ok(())
}
async fn snowflake_start_delivery(
    runtime: &Arc<SnowflakeRuntime>,
    stats: &Arc<EmitterStats>,
    ack: &AckHandle,
    batch: &mut Option<SnowflakeBatch>,
    deliveries: &mut tokio::task::JoinSet<Result<(), String>>,
    max_parallel: usize,
) -> Result<(), String> {
    if batch.is_none() {
        return Ok(());
    }
    while deliveries.len() >= max_parallel {
        snowflake_delivery_result(deliveries.join_next().await)?;
    }
    let Some(held) = batch.take() else {
        return Ok(());
    };
    let runtime = runtime.clone();
    let ack = ack.clone();
    let stats = stats.clone();
    deliveries.spawn(async move {
        complete_after_delivery(
            async {
                let n_rows = held.rows.len() as u64;
                let started = std::time::Instant::now();
                match &held.snapshot {
                    Some(operation) => runtime
                        .deliver_snapshot(held.schema, held.rows, operation)
                        .await
                        .map(|_| ()),
                    None => runtime.deliver(held.schema, held.rows).await,
                }?;
                stats
                    .rows_emitted
                    .fetch_add(n_rows, std::sync::atomic::Ordering::Relaxed);
                stats
                    .blocks_sent
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stats.inserter_ch_nanos.fetch_add(
                    started.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                Ok(())
            },
            &ack,
            held.per_seq.into_iter().collect(),
            held.permits,
        )
        .await
    });
    Ok(())
}
fn snowflake_delivery_result(
    result: Option<Result<Result<(), String>, tokio::task::JoinError>>,
) -> Result<(), String> {
    match result {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(format!("delivery task failed: {e}")),
        None => Err("delivery task disappeared".into()),
    }
}
async fn snowflake_drain_deliveries(
    deliveries: &mut tokio::task::JoinSet<Result<(), String>>,
) -> Result<(), String> {
    while !deliveries.is_empty() {
        snowflake_delivery_result(deliveries.join_next().await)?;
    }
    Ok(())
}

async fn complete_after_delivery<F>(
    delivery: F,
    ack: &AckHandle,
    per_seq: Vec<(u64, u64)>,
    permits: Vec<Arc<crate::budget::MemoryPermit>>,
) -> Result<(), String>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    delivery.await.map_err(|e| e.to_string())?;
    ack.acked(per_seq);
    drop(permits);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dropped_tail_closes_workers_with_live_producers() {
        let (tx, ack, parts) = spawn_null(Arc::new(Monotone::new(0)));
        drop(parts);
        tokio::time::timeout(std::time::Duration::from_secs(1), tx.closed())
            .await
            .unwrap();
        drop(ack);
    }
    #[tokio::test]
    async fn snowflake_delivery_failure_does_not_advance_ack() {
        let watermark = Arc::new(Monotone::<EmitterAck>::new(0));
        let (ack, collector) = ack::spawn(watermark.clone());
        ack.register(0, 100);
        ack.register(1, 200);
        ack.placed(0, 1);
        ack.placed(1, 1);
        let failed = complete_after_delivery(
            async { anyhow::bail!("warehouse rejected batch") },
            &ack,
            vec![(0, 1)],
            vec![],
        )
        .await;
        assert!(failed.is_err());
        complete_after_delivery(async { Ok(()) }, &ack, vec![(1, 1)], vec![])
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), ack.wait_through(2))
                .await
                .is_err()
        );
        complete_after_delivery(async { Ok(()) }, &ack, vec![(0, 1)], vec![])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), ack.wait_through(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark.get(), 200);
        drop(ack);
        collector.await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_snowflake_batches_ack_contiguously() {
        let watermark = Arc::new(Monotone::<EmitterAck>::new(0));
        let (ack, collector) = ack::spawn(watermark.clone());
        ack.register(0, 100);
        ack.register(1, 200);
        ack.placed(0, 1);
        ack.placed(1, 1);
        let (first_tx, first_rx) = oneshot::channel::<()>();
        let (second_tx, second_rx) = oneshot::channel::<()>();
        let mut deliveries = tokio::task::JoinSet::new();
        for (seq, ready) in [(0, first_rx), (1, second_rx)] {
            let ack = ack.clone();
            deliveries.spawn(async move {
                complete_after_delivery(
                    async move { ready.await.map_err(anyhow::Error::from) },
                    &ack,
                    vec![(seq, 1)],
                    vec![],
                )
                .await
            });
        }
        second_tx.send(()).unwrap();
        snowflake_delivery_result(deliveries.join_next().await).unwrap();
        assert_eq!(watermark.get(), 0, "later batch cannot cross the gap");
        first_tx.send(()).unwrap();
        snowflake_drain_deliveries(&mut deliveries).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), ack.wait_through(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(watermark.get(), 200);
        drop(ack);
        collector.await.unwrap();
    }
}
