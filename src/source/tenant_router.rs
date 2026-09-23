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
//! queue refuses a record for `stall_timeout` is marked evicted and never
//! fed again; the session tears it down and detaches it

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

/// One tenant's entry point: its hold sink (capture + queueing worker)
pub struct RoutedTenant {
    pub id: String,
    pub db_oid: u32,
    /// Records before this are never routed: the tenant's descriptor
    /// history starts here
    pub from_lsn: u64,
    pub sink: BoundaryHoldSink,
    /// Highest record position the tenant has been sent, real or heartbeat
    last_lsn: u64,
    evicted: Option<String>,
}

impl RoutedTenant {
    pub fn new(id: String, db_oid: u32, from_lsn: u64, sink: BoundaryHoldSink) -> Self {
        Self {
            id,
            db_oid,
            from_lsn,
            sink,
            last_lsn: 0,
            evicted: None,
        }
    }

    pub fn evicted(&self) -> Option<&str> {
        self.evicted.as_deref()
    }

    async fn send(&mut self, record: &Record<'_>, stall: Duration) -> Result<(), SinkError> {
        if self.evicted.is_some() || record.source_lsn < self.from_lsn {
            return Ok(());
        }
        match tokio::time::timeout(stall, self.sink.on_record(record)).await {
            Ok(Ok(())) => {
                self.last_lsn = self.last_lsn.max(record.source_lsn);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                self.evicted = Some(format!(
                    "queue refused records for {}s (destination stalled)",
                    stall.as_secs()
                ));
                tracing::error!(
                    target: "walshadow::tenant",
                    tenant = %self.id,
                    lsn = record.source_lsn,
                    "tenant stalled the shared pump; evicting",
                );
                Ok(())
            }
        }
    }
}

pub struct TenantRouter {
    tenants: Vec<RoutedTenant>,
    stall_timeout: Duration,
    /// Last record routed anywhere: (source_lsn, next_lsn)
    last: (u64, u64),
    /// Tenant errors don't fail the pump when set: the failing tenant is
    /// evicted instead. Off for the single-database layout, where a pipeline
    /// error has always been the daemon's
    isolate: bool,
}

impl TenantRouter {
    pub fn new(stall_timeout: Duration, isolate: bool) -> Self {
        Self {
            tenants: Vec::new(),
            stall_timeout,
            last: (0, 0),
            isolate,
        }
    }

    pub fn attach(&mut self, tenant: RoutedTenant) {
        self.tenants.push(tenant);
    }

    /// Remove a tenant, returning its sink for the caller to drain or drop
    pub fn detach(&mut self, id: &str) -> Option<RoutedTenant> {
        let at = self.tenants.iter().position(|t| t.id == id)?;
        Some(self.tenants.remove(at))
    }

    pub fn tenants(&self) -> &[RoutedTenant] {
        &self.tenants
    }

    pub fn tenants_mut(&mut self) -> &mut [RoutedTenant] {
        &mut self.tenants
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut RoutedTenant> {
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
            .map(|t| t.sink.inner.send_wait_seconds())
            .sum()
    }

    async fn deliver(&mut self, record: &Record<'_>) -> Result<(), SinkError> {
        let scope = scope_of(record);
        let stall = self.stall_timeout;
        let isolate = self.isolate;
        for t in &mut self.tenants {
            if let Scope::Db(db) = scope
                && db != t.db_oid
            {
                continue;
            }
            if let Err(e) = t.send(record, stall).await {
                if !isolate {
                    return Err(e);
                }
                tracing::error!(
                    target: "walshadow::tenant",
                    tenant = %t.id,
                    error = %e,
                    "tenant pipeline failed; evicting",
                );
                t.evicted = Some(format!("pipeline failed: {e}"));
            }
        }
        self.last = (record.source_lsn, record.next_lsn);
        Ok(())
    }

    /// Ship every tenant's pending batch, first giving tenants that saw
    /// nothing of the latest stretch a position marker so their ack moves
    pub async fn flush(&mut self) -> Result<(), SinkError> {
        let (lsn, next_lsn) = self.last;
        let stall = self.stall_timeout;
        let isolate = self.isolate;
        for t in &mut self.tenants {
            if t.evicted.is_some() {
                continue;
            }
            let mut out = Ok(());
            if lsn > t.last_lsn && lsn >= t.from_lsn {
                let beat = heartbeat(lsn, next_lsn);
                out = t.send(&beat, stall).await;
            }
            if out.is_ok() {
                out = t.sink.flush().await;
            }
            if let Err(e) = out {
                if !isolate {
                    return Err(e);
                }
                t.evicted = Some(format!("pipeline failed: {e}"));
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

impl RecordSink for TenantRouter {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(self.deliver(record))
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for t in &mut self.tenants {
                if t.evicted.is_none() {
                    t.sink.on_idle().await?;
                }
            }
            Ok(())
        })
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for t in &mut self.tenants {
                if t.evicted.is_none() {
                    t.sink.on_close().await?;
                }
            }
            Ok(())
        })
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for t in &mut self.tenants {
                if t.evicted.is_none() && lsn >= t.from_lsn {
                    t.sink.on_idle_advance(lsn).await?;
                }
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
}
