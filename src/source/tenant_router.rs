//! Fans the one WAL stream out to per-tenant pipelines.
//!
//! A PostgreSQL transaction writes one database, and every record that
//! touches a relation names the relation's database, so the database decides
//! the tenant: block locators and truncate payloads for data, the commit's
//! `xl_xact_dbinfo` for its end, the boundary's own database for catalog
//! captures. Records that name no database (xact assignment, running-xacts,
//! checkpoints, shared catalogs, commits without dbinfo) go to every tenant,
//! exactly as the single-database pump fed them all to its one worker.
//!
//! A tenant that receives nothing would never move its ack: the queueing
//! worker advances only past records it saw. [`TenantRouter::flush`] sends
//! each such tenant a no-op record at the stream position, which the
//! decoder and reorder sinks ignore and the worker's idle advance turns into
//! progress when the tenant holds no open transaction.
//!
//! One stuck destination must not stall the shared pump. A tenant whose
//! queue refuses a record, or whose flush, idle or close callback blocks,
//! for `stall_timeout` is marked evicted and never fed again; so is one that
//! fails any callback when tenants are isolated. The session tears it down
//! and detaches it

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use walrus::pg::walparser::{RmId, XLogRecord};

use crate::decode::heap_decoder::XLOG_HEAP_TRUNCATE;
use crate::filter::main_data::{
    XLOG_SMGR_CREATE, parse_xl_heap_truncate, parse_xl_smgr_create, relation_for_empty,
};
use crate::record::{Record, RecordSink, Route, SinkError};
use crate::source::boundary_hold::BoundaryHoldSink;

/// Tenants a record belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Only the tenant following this database, if any
    Db(u32),
    /// Every tenant
    All,
}

const XLOG_SMGR_TRUNCATE: u8 = 0x20;
const XLOG_HEAP_OPMASK: u8 = 0x70;
const XLOG_XACT_OPMASK: u8 = 0x70;
const XLOG_XACT_COMMIT: u8 = 0x00;
const XLOG_XACT_ABORT: u8 = 0x20;
const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;

/// Which tenants `record` belongs to
pub fn scope_of(record: &Record<'_>) -> Scope {
    if let Some(info) = &record.boundary_info {
        return match info.db_oid {
            0 => Scope::All,
            db => Scope::Db(db),
        };
    }
    let parsed = &record.parsed;
    let rm = parsed.header.resource_manager_id;
    if rm == RmId::Xact as u8 {
        let op = parsed.header.info & XLOG_XACT_OPMASK;
        let ends = matches!(
            op,
            XLOG_XACT_COMMIT
                | XLOG_XACT_ABORT
                | XLOG_XACT_COMMIT_PREPARED
                | XLOG_XACT_ABORT_PREPARED
        );
        return match record.xact_db {
            Some(db) if ends && db != 0 => Scope::Db(db),
            _ => Scope::All,
        };
    }
    if let Some(block) = parsed.blocks.first() {
        return db_scope(block.header.location.rel.db_node);
    }
    if rm == RmId::Heap as u8
        && parsed.header.info & XLOG_HEAP_OPMASK == XLOG_HEAP_TRUNCATE
        && let Some(t) = parse_xl_heap_truncate(&parsed.main_data)
    {
        return db_scope(t.db_oid);
    }
    if rm == RmId::Smgr as u8 {
        let op = parsed.header.info & 0xF0;
        if op == XLOG_SMGR_CREATE
            && let Some((rfn, _)) = parse_xl_smgr_create(&parsed.main_data)
        {
            return db_scope(rfn.db_node);
        }
        // xl_smgr_truncate: BlockNumber blkno, RelFileLocator rlocator, int flags
        if op == XLOG_SMGR_TRUNCATE && parsed.main_data.len() >= 16 {
            let db = u32::from_le_bytes(parsed.main_data[8..12].try_into().unwrap());
            return db_scope(db);
        }
    }
    if let Some(rel) = relation_for_empty(parsed) {
        return db_scope(rel.db_node);
    }
    Scope::All
}

fn db_scope(db: u32) -> Scope {
    match db {
        0 => Scope::All,
        db => Scope::Db(db),
    }
}

/// What the router needs of a tenant's entry point beyond [`RecordSink`]:
/// shipping its pending batch and queue depth for metrics
pub trait TenantSink: RecordSink + Send {
    fn flush(&mut self) -> impl Future<Output = Result<(), SinkError>> + Send;
    fn in_flight(&self) -> u64;
    fn processed(&self) -> u64;
    fn send_wait_seconds(&self) -> f64;
}

impl TenantSink for BoundaryHoldSink {
    fn flush(&mut self) -> impl Future<Output = Result<(), SinkError>> + Send {
        BoundaryHoldSink::flush(self)
    }

    fn in_flight(&self) -> u64 {
        BoundaryHoldSink::in_flight(self)
    }

    fn processed(&self) -> u64 {
        BoundaryHoldSink::processed(self)
    }

    fn send_wait_seconds(&self) -> f64 {
        self.inner.send_wait_seconds()
    }
}

/// One tenant's entry point: its hold sink (capture + queueing worker)
pub struct RoutedTenant<S = BoundaryHoldSink> {
    pub id: String,
    /// Databases the tenant follows
    pub db_oids: Vec<u32>,
    /// Records before this are never routed: the tenant's descriptor
    /// history starts here
    pub from_lsn: u64,
    pub sink: S,
    /// Highest record position the tenant has been sent, real or heartbeat
    last_lsn: u64,
    evicted: Option<String>,
}

impl<S: TenantSink> RoutedTenant<S> {
    pub fn new(id: String, db_oids: Vec<u32>, from_lsn: u64, sink: S) -> Self {
        Self {
            id,
            db_oids,
            from_lsn,
            sink,
            last_lsn: 0,
            evicted: None,
        }
    }

    pub fn evicted(&self) -> Option<&str> {
        self.evicted.as_deref()
    }

    /// Fed: not evicted, and `lsn` is inside its descriptor history
    fn takes(&self, lsn: u64) -> bool {
        self.evicted.is_none() && lsn >= self.from_lsn
    }

    fn in_scope(&self, scope: Scope) -> bool {
        match scope {
            Scope::Db(db) => self.db_oids.contains(&db),
            Scope::All => true,
        }
    }

    /// Settle one callback under the stall bound. A stall always evicts: it
    /// would hold the shared pump. An error evicts with `isolate`, else it is
    /// the session's
    fn settle(
        &mut self,
        what: &str,
        stall: Duration,
        isolate: bool,
        out: Result<Result<(), SinkError>, tokio::time::error::Elapsed>,
    ) -> Result<(), SinkError> {
        match out {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) if !isolate => Err(e),
            Ok(Err(e)) => {
                tracing::error!(
                    target: "walshadow::tenant",
                    tenant = %self.id,
                    error = %e,
                    "tenant pipeline failed in {what}; evicting",
                );
                self.evicted = Some(format!("pipeline failed: {e}"));
                Ok(())
            }
            Err(_) => {
                tracing::error!(
                    target: "walshadow::tenant",
                    tenant = %self.id,
                    "tenant stalled the shared pump in {what}; evicting",
                );
                self.evicted = Some(format!(
                    "{what} blocked for {}s (destination stalled)",
                    stall.as_secs()
                ));
                Ok(())
            }
        }
    }

    async fn send(
        &mut self,
        record: &Record<'_>,
        stall: Duration,
        isolate: bool,
    ) -> Result<(), SinkError> {
        if !self.takes(record.source_lsn) {
            return Ok(());
        }
        let out = tokio::time::timeout(stall, self.sink.on_record(record)).await;
        if matches!(out, Ok(Ok(()))) {
            self.last_lsn = self.last_lsn.max(record.source_lsn);
        }
        self.settle("record delivery", stall, isolate, out)
    }

    async fn send_owned(
        &mut self,
        record: Record<'static>,
        stall: Duration,
        isolate: bool,
    ) -> Result<(), SinkError> {
        let lsn = record.source_lsn;
        if !self.takes(lsn) {
            return Ok(());
        }
        let out = tokio::time::timeout(stall, self.sink.on_record_owned(record)).await;
        if matches!(out, Ok(Ok(()))) {
            self.last_lsn = self.last_lsn.max(lsn);
        }
        self.settle("record delivery", stall, isolate, out)
    }
}

pub struct TenantRouter<S = BoundaryHoldSink> {
    tenants: Vec<RoutedTenant<S>>,
    stall_timeout: Duration,
    /// Last record routed anywhere: (source_lsn, next_lsn)
    last: (u64, u64),
    /// Tenant errors don't fail the pump when set: the failing tenant is
    /// evicted instead. Off for the single-database layout, where a pipeline
    /// error has always been the daemon's
    isolate: bool,
}

impl<S: TenantSink> TenantRouter<S> {
    pub fn new(stall_timeout: Duration, isolate: bool) -> Self {
        Self {
            tenants: Vec::new(),
            stall_timeout,
            last: (0, 0),
            isolate,
        }
    }

    pub fn attach(&mut self, tenant: RoutedTenant<S>) {
        self.tenants.push(tenant);
    }

    /// Remove a tenant, returning its sink for the caller to drain or drop
    pub fn detach(&mut self, id: &str) -> Option<RoutedTenant<S>> {
        let at = self.tenants.iter().position(|t| t.id == id)?;
        Some(self.tenants.remove(at))
    }

    pub fn tenants(&self) -> &[RoutedTenant<S>] {
        &self.tenants
    }

    pub fn tenants_mut(&mut self) -> &mut [RoutedTenant<S>] {
        &mut self.tenants
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut RoutedTenant<S>> {
        self.tenants.iter_mut().find(|t| t.id == id)
    }

    /// Tenants the router evicted, with the reason
    pub fn evictions(&self) -> Vec<(String, String)> {
        self.tenants
            .iter()
            .filter_map(|t| t.evicted.clone().map(|why| (t.id.clone(), why)))
            .collect()
    }

    pub fn mark_evicted(&mut self, id: &str, reason: String) {
        if let Some(t) = self.get_mut(id)
            && t.evicted.is_none()
        {
            t.evicted = Some(reason);
        }
    }

    /// End of the last record routed anywhere: every record before it has
    /// reached its tenant's queue. Zero before the first record
    pub fn last_record_end(&self) -> u64 {
        self.last.1
    }

    /// Start of the last record routed anywhere. Shadow replay at or past it
    /// means every earlier record applied; a catalog commit there was itself
    /// held until replayed. Its end can sit at a segment boundary a switch
    /// jumped to, which replay never reports
    pub fn last_record_start(&self) -> u64 {
        self.last.0
    }

    pub fn in_flight(&self) -> u64 {
        self.tenants.iter().map(|t| t.sink.in_flight()).sum()
    }

    pub fn processed(&self) -> u64 {
        self.tenants.iter().map(|t| t.sink.processed()).sum()
    }

    pub fn send_wait_seconds(&self) -> f64 {
        self.tenants
            .iter()
            .map(|t| t.sink.send_wait_seconds())
            .sum()
    }

    async fn deliver(&mut self, record: &Record<'_>) -> Result<(), SinkError> {
        let scope = scope_of(record);
        let (stall, isolate) = (self.stall_timeout, self.isolate);
        for t in self.tenants.iter_mut().filter(|t| t.in_scope(scope)) {
            t.send(record, stall, isolate).await?;
        }
        self.last = (record.source_lsn, record.next_lsn);
        Ok(())
    }

    /// [`Self::deliver`] for a record the pump is done with: a record one
    /// tenant takes moves into its queue uncopied, one several take is
    /// copied per tenant as before
    async fn deliver_owned(&mut self, record: Record<'static>) -> Result<(), SinkError> {
        let scope = scope_of(&record);
        let lsn = record.source_lsn;
        let only = {
            let mut fed = self
                .tenants
                .iter()
                .enumerate()
                .filter(|(_, t)| t.in_scope(scope) && t.takes(lsn))
                .map(|(i, _)| i);
            match (fed.next(), fed.next()) {
                (Some(only), None) => Some(only),
                _ => None,
            }
        };
        let Some(only) = only else {
            return self.deliver(&record).await;
        };
        let (stall, isolate) = (self.stall_timeout, self.isolate);
        self.last = (record.source_lsn, record.next_lsn);
        self.tenants[only].send_owned(record, stall, isolate).await
    }

    /// Ship every tenant's pending batch, first giving tenants that saw
    /// nothing of the latest stretch a position marker so their ack moves
    pub async fn flush(&mut self) -> Result<(), SinkError> {
        let (lsn, next_lsn) = self.last;
        let (stall, isolate) = (self.stall_timeout, self.isolate);
        for t in &mut self.tenants {
            if t.evicted.is_some() {
                continue;
            }
            if lsn > t.last_lsn && lsn >= t.from_lsn {
                t.send(&heartbeat(lsn, next_lsn), stall, isolate).await?;
            }
            if t.evicted.is_none() {
                let out = tokio::time::timeout(stall, t.sink.flush()).await;
                t.settle("flush", stall, isolate, out)?;
            }
        }
        Ok(())
    }
}

/// XLOG_NOOP: no rmgr sink acts on it, but the queueing worker's idle
/// advance carries its position
fn heartbeat(lsn: u64, next_lsn: u64) -> Record<'static> {
    let mut parsed = XLogRecord::default();
    parsed.header.resource_manager_id = RmId::Xlog as u8;
    // XLOG_NOOP
    parsed.header.info = 0x20;
    Record {
        parsed,
        source_lsn: lsn,
        next_lsn,
        route: Route::ToShadow,
        ..Default::default()
    }
}

impl<S: TenantSink> RecordSink for TenantRouter<S> {
    /// Union over the tenants the record reaches: the pump must not ship
    /// past a record any of them may hold on
    fn may_park(&self, record: &Record<'_>) -> bool {
        let scope = scope_of(record);
        self.tenants
            .iter()
            .any(|t| t.in_scope(scope) && t.takes(record.source_lsn) && t.sink.may_park(record))
    }

    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(self.deliver(record))
    }

    fn on_record_owned(
        &mut self,
        record: Record<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
        Box::pin(self.deliver_owned(record))
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let (stall, isolate) = (self.stall_timeout, self.isolate);
            for t in self.tenants.iter_mut().filter(|t| t.evicted.is_none()) {
                let out = tokio::time::timeout(stall, t.sink.on_idle()).await;
                t.settle("idle tick", stall, isolate, out)?;
            }
            Ok(())
        })
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let (stall, isolate) = (self.stall_timeout, self.isolate);
            for t in self.tenants.iter_mut().filter(|t| t.evicted.is_none()) {
                let out = tokio::time::timeout(stall, t.sink.on_close()).await;
                t.settle("close", stall, isolate, out)?;
            }
            Ok(())
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            let (stall, isolate) = (self.stall_timeout, self.isolate);
            for t in self.tenants.iter_mut().filter(|t| t.takes(lsn)) {
                let out = tokio::time::timeout(stall, t.sink.on_idle_advance(lsn)).await;
                t.settle("idle advance", stall, isolate, out)?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::BoundaryInfo;
    use std::sync::Arc;
    use walrus::pg::walparser::{RelFileNode, XLogRecordBlock};

    fn heap_in(db: u32) -> Record<'static> {
        let mut parsed = XLogRecord::default();
        parsed.header.resource_manager_id = RmId::Heap as u8;
        let mut block = XLogRecordBlock::default();
        block.header.location.rel = RelFileNode {
            spc_node: 1663,
            db_node: db,
            rel_node: 16400,
        };
        parsed.blocks.push(block);
        Record {
            parsed,
            route: Route::ToDecoder,
            ..Default::default()
        }
    }

    #[test]
    fn data_records_follow_their_block_database() {
        assert_eq!(scope_of(&heap_in(5)), Scope::Db(5));
        assert_eq!(scope_of(&heap_in(0)), Scope::All, "shared catalog");
    }

    #[test]
    fn xact_ends_follow_dbinfo_and_everything_else_broadcasts() {
        let mut commit = Record::default();
        commit.parsed.header.resource_manager_id = RmId::Xact as u8;
        commit.parsed.header.info = XLOG_XACT_COMMIT;
        assert_eq!(scope_of(&commit), Scope::All, "no dbinfo");
        commit.xact_db = Some(7);
        assert_eq!(scope_of(&commit), Scope::Db(7));
        let mut assignment = commit.clone();
        assignment.parsed.header.info = 0x50;
        assert_eq!(scope_of(&assignment), Scope::All);
        let mut checkpoint = Record::default();
        checkpoint.parsed.header.resource_manager_id = RmId::Xlog as u8;
        assert_eq!(scope_of(&checkpoint), Scope::All);
    }

    #[test]
    fn boundaries_go_to_their_tenant_or_all_for_shared_scope() {
        let mut rec = heap_in(5);
        rec.boundary_info = Some(Arc::new(BoundaryInfo {
            db_oid: 9,
            ..Default::default()
        }));
        assert_eq!(scope_of(&rec), Scope::Db(9));
        rec.boundary_info = Some(Arc::new(BoundaryInfo::default()));
        assert_eq!(scope_of(&rec), Scope::All);
    }

    #[test]
    fn heap_truncate_uses_its_payload_database() {
        let mut rec = Record::default();
        rec.parsed.header.resource_manager_id = RmId::Heap as u8;
        rec.parsed.header.info = XLOG_HEAP_TRUNCATE;
        let mut md = Vec::new();
        md.extend_from_slice(&11u32.to_le_bytes());
        md.extend_from_slice(&1u32.to_le_bytes());
        md.extend_from_slice(&[0, 0, 0, 0]);
        md.extend_from_slice(&16400u32.to_le_bytes());
        rec.parsed.main_data = std::borrow::Cow::Owned(md);
        rec.parsed.main_data_len = 16;
        assert_eq!(scope_of(&rec), Scope::Db(11));
    }

    /// Records a tenant received, and the callback it fails or blocks in
    #[derive(Default)]
    struct Fake {
        seen: Vec<u64>,
        owned: usize,
        idles: usize,
        advances: Vec<u64>,
        fail: Option<&'static str>,
        stall: Option<&'static str>,
        parks: bool,
    }

    impl Fake {
        fn act(
            &self,
            at: &'static str,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send>> {
            let (fail, stall) = (self.fail == Some(at), self.stall == Some(at));
            Box::pin(async move {
                if stall {
                    std::future::pending::<()>().await;
                }
                if fail {
                    return Err(SinkError::Other(format!("{at} failed")));
                }
                Ok(())
            })
        }
    }

    impl RecordSink for Fake {
        fn may_park(&self, _record: &Record<'_>) -> bool {
            self.parks
        }

        fn on_record<'a>(
            &'a mut self,
            record: &'a Record<'a>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            self.seen.push(record.source_lsn);
            self.act("record")
        }

        fn on_record_owned(
            &mut self,
            record: Record<'static>,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + '_>> {
            self.owned += 1;
            self.seen.push(record.source_lsn);
            self.act("record")
        }

        fn on_idle<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            self.idles += 1;
            self.act("idle")
        }

        fn on_close<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            self.act("close")
        }

        fn on_idle_advance<'a>(
            &'a mut self,
            lsn: u64,
        ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
            self.advances.push(lsn);
            self.act("advance")
        }
    }

    impl TenantSink for Fake {
        fn flush(&mut self) -> impl Future<Output = Result<(), SinkError>> + Send {
            self.act("flush")
        }

        fn in_flight(&self) -> u64 {
            0
        }

        fn processed(&self) -> u64 {
            self.seen.len() as u64
        }

        fn send_wait_seconds(&self) -> f64 {
            0.0
        }
    }

    fn router(isolate: bool, tenants: Vec<(&str, u32, Fake)>) -> TenantRouter<Fake> {
        let mut r = TenantRouter::new(Duration::from_millis(50), isolate);
        for (id, db, sink) in tenants {
            r.attach(RoutedTenant::new(id.into(), vec![db], 0, sink));
        }
        r
    }

    fn at(db: u32, lsn: u64) -> Record<'static> {
        Record {
            source_lsn: lsn,
            next_lsn: lsn + 8,
            ..heap_in(db)
        }
    }

    fn sink<'r>(r: &'r TenantRouter<Fake>, id: &str) -> &'r Fake {
        &r.tenants().iter().find(|t| t.id == id).unwrap().sink
    }

    /// A tenant failing or blocking in any callback is evicted; the others
    /// keep getting records, ticks and advances
    #[tokio::test]
    async fn every_callback_isolates_a_failing_or_stalled_tenant() {
        for (cb, stalls) in [
            ("idle", false),
            ("idle", true),
            ("advance", false),
            ("advance", true),
            ("flush", false),
            ("flush", true),
            ("close", false),
            ("close", true),
        ] {
            let bad = Fake {
                fail: (!stalls).then_some(cb),
                stall: stalls.then_some(cb),
                ..Fake::default()
            };
            let mut r = router(true, vec![("bad", 5, bad), ("good", 6, Fake::default())]);
            r.on_record(&at(5, 0x10)).await.unwrap();
            r.on_record(&at(6, 0x18)).await.unwrap();
            match cb {
                "idle" => r.on_idle().await.unwrap(),
                "advance" => r.on_idle_advance(0x20).await.unwrap(),
                "flush" => r.flush().await.unwrap(),
                _ => r.on_close().await.unwrap(),
            }
            let evicted = r.evictions();
            assert_eq!(evicted.len(), 1, "{cb} stall={stalls}: {evicted:?}");
            assert_eq!(evicted[0].0, "bad");
            assert_eq!(evicted[0].1.contains("stalled"), stalls, "{evicted:?}");
            // Healthy tenant keeps flowing; the evicted one gets nothing more
            r.on_record(&at(6, 0x30)).await.unwrap();
            r.on_record(&at(5, 0x38)).await.unwrap();
            r.on_idle().await.unwrap();
            r.on_idle_advance(0x40).await.unwrap();
            r.flush().await.unwrap();
            assert!(sink(&r, "good").seen.contains(&0x30));
            assert!(sink(&r, "good").advances.contains(&0x40));
            assert!(!sink(&r, "bad").seen.contains(&0x38));
            assert!(!sink(&r, "bad").advances.contains(&0x40));
        }
    }

    #[tokio::test]
    async fn single_database_layout_keeps_callback_errors_fatal() {
        let bad = Fake {
            fail: Some("idle"),
            ..Fake::default()
        };
        let mut r = router(false, vec![("default", 5, bad)]);
        assert!(r.on_idle().await.is_err());
        assert!(r.evictions().is_empty());
    }

    /// Same relation OID in two databases reaches only the tenant following
    /// the database that wrote it, in order, and moves rather than copies
    /// when one tenant takes the record
    #[tokio::test]
    async fn owned_records_route_by_database_in_order_uncopied() {
        let mut r = router(
            true,
            vec![("a", 5, Fake::default()), ("b", 6, Fake::default())],
        );
        for (db, lsn) in [(5, 0x10), (6, 0x18), (5, 0x20), (6, 0x28)] {
            r.on_record_owned(at(db, lsn)).await.unwrap();
        }
        assert_eq!(sink(&r, "a").seen, [0x10, 0x20]);
        assert_eq!(sink(&r, "b").seen, [0x18, 0x28]);
        assert_eq!((sink(&r, "a").owned, sink(&r, "b").owned), (2, 2));
        // Broadcast copies to each tenant
        let mut checkpoint = at(0, 0x30);
        checkpoint.parsed.blocks.clear();
        checkpoint.parsed.header.resource_manager_id = RmId::Xlog as u8;
        r.on_record_owned(checkpoint).await.unwrap();
        assert_eq!(sink(&r, "a").seen.last(), Some(&0x30));
        assert_eq!(sink(&r, "b").seen.last(), Some(&0x30));
        assert_eq!(r.last_record_start(), 0x30);
    }

    #[test]
    fn may_park_is_the_union_over_tenants_the_record_reaches() {
        let parks = Fake {
            parks: true,
            ..Fake::default()
        };
        let r = router(true, vec![("a", 5, parks), ("b", 6, Fake::default())]);
        assert!(r.may_park(&at(5, 0x10)));
        assert!(!r.may_park(&at(6, 0x10)), "b never parks");
    }
}
