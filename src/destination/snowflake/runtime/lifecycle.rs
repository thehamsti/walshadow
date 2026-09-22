//! WAL-ordered TRUNCATE publication and physical relation handoff.

use super::*;
use crate::destination::snowflake::state::GenerationPhase;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TruncateBarrier {
    operation_id: String,
    record_lsn: u64,
    old_physical: String,
    new_physical: Option<String>,
}

impl SnowflakeRuntime {
    pub async fn truncate_at(&self, desc: &RelDescriptor, record_lsn: u64) -> Result<()> {
        self.quiesce().await?;
        ensure!(record_lsn != 0, "TRUNCATE WAL record LSN is missing");
        let operation_id = truncate_operation_id(&self.source_identity, desc.oid, record_lsn);
        if let Some(record) = self.state.generation(&operation_id)?
            && record.phase == GenerationPhase::Replayed
        {
            ensure!(
                record.relation_oid == desc.oid && record.snapshot_lsn == record_lsn,
                "conflicting TRUNCATE operation retry"
            );
            return Ok(());
        }
        self.lineage_with_truncate_barrier(desc)?;
        let marker_key = format!("truncate-generation/{operation_id}");
        self.state
            .put_metadata(&marker_key, &record_lsn.to_le_bytes())?;
        self.register_truncate_barrier(desc, record_lsn, &operation_id)?;
        let record = self
            .prepare_snapshot(desc, record_lsn, &operation_id)
            .await?;
        match record.phase {
            GenerationPhase::Prepared | GenerationPhase::Loaded => {
                self.mark_snapshot_loaded(desc, &operation_id, &[], true)
                    .await?;
            }
            GenerationPhase::Published | GenerationPhase::Replayed => {}
            GenerationPhase::Aborted | GenerationPhase::Retired => {
                bail!("TRUNCATE generation cannot be resumed in this phase")
            }
        }
        self.publish_snapshot(desc, &operation_id).await
    }

    /// Called by `lineage` for every descriptor. A changed relfilenode is
    /// accepted once, and only after its exact TRUNCATE generation is public.
    pub fn lineage_with_truncate_barrier(&self, desc: &RelDescriptor) -> Result<(u64, u64)> {
        let key = physical_key(desc.oid);
        let physical = format!("{:?}", desc.rfn);
        let Some(stored) = self.state.get_metadata(&key)? else {
            self.state.put_metadata(&key, physical.as_bytes())?;
            return Ok((u64::from(desc.rfn.rel_node), 0));
        };
        let stored = std::str::from_utf8(&stored).context("stored relation identity is invalid")?;
        let barrier_key = truncate_barrier_key(desc.oid);
        let barrier_bytes = self.state.get_metadata(&barrier_key)?;
        if stored == physical {
            if let Some(bytes) = barrier_bytes {
                let mut barrier: TruncateBarrier = serde_json::from_slice(&bytes)?;
                if barrier.new_physical.is_none() && barrier.old_physical != physical {
                    barrier.new_physical = Some(physical.clone());
                    ensure!(
                        self.state.compare_exchange_metadata(
                            &barrier_key,
                            Some(&bytes),
                            &serde_json::to_vec(&barrier)?
                        )?,
                        "TRUNCATE relation handoff changed concurrently"
                    );
                }
            }
            return Ok((u64::from(desc.rfn.rel_node), 0));
        }
        // A finished handoff guards only the storage it retired: returning to
        // it would let pre-TRUNCATE rows back in
        let barrier_bytes = match barrier_bytes {
            Some(bytes) => {
                let barrier: TruncateBarrier = serde_json::from_slice(&bytes)?;
                ensure!(
                    barrier.old_physical != physical,
                    "relation returned to storage its TRUNCATE retired"
                );
                barrier.new_physical.is_none().then_some(bytes)
            }
            None => None,
        };
        let Some(bytes) = barrier_bytes else {
            // No TRUNCATE registered its barrier ahead of this storage, and a
            // TRUNCATE always does (it replays in WAL order before any row of
            // the new file): this is a rewrite that keeps every row
            // (VACUUM FULL, CLUSTER). Rows only change incarnation, which
            // feeds event ids, not keys, so the current state stays valid
            ensure!(
                self.state.compare_exchange_metadata(
                    &key,
                    Some(stored.as_bytes()),
                    physical.as_bytes()
                )?,
                "physical relation identity changed concurrently"
            );
            tracing::info!(
                target: "walshadow::snowflake",
                relation = %desc.rel_name,
                from = stored,
                to = %physical,
                "relation storage rewritten; rows keep their state",
            );
            return Ok((u64::from(desc.rfn.rel_node), 0));
        };
        let mut barrier: TruncateBarrier = serde_json::from_slice(&bytes)?;
        ensure!(
            barrier.old_physical == stored && barrier.new_physical.is_none(),
            "physical relation changed outside its one-time TRUNCATE handoff"
        );
        let generation = self
            .state
            .generation(&barrier.operation_id)?
            .context("TRUNCATE generation journal missing")?;
        ensure!(
            generation.phase == GenerationPhase::Replayed
                && generation.relation_oid == desc.oid
                && generation.snapshot_lsn == barrier.record_lsn,
            "physical relation rotated before TRUNCATE publication"
        );
        ensure!(
            self.state.compare_exchange_metadata(
                &key,
                Some(stored.as_bytes()),
                physical.as_bytes()
            )?,
            "physical relation identity changed concurrently"
        );
        barrier.new_physical = Some(physical);
        ensure!(
            self.state.compare_exchange_metadata(
                &barrier_key,
                Some(&bytes),
                &serde_json::to_vec(&barrier)?
            )?,
            "TRUNCATE relation handoff changed concurrently"
        );
        Ok((u64::from(desc.rfn.rel_node), 0))
    }

    fn register_truncate_barrier(
        &self,
        desc: &RelDescriptor,
        record_lsn: u64,
        operation_id: &str,
    ) -> Result<()> {
        let key = truncate_barrier_key(desc.oid);
        let old_physical = format!("{:?}", desc.rfn);
        let next = TruncateBarrier {
            operation_id: operation_id.into(),
            record_lsn,
            old_physical: old_physical.clone(),
            new_physical: None,
        };
        loop {
            let previous = self.state.get_metadata(&key)?;
            if let Some(bytes) = &previous {
                let old: TruncateBarrier = serde_json::from_slice(bytes)?;
                if old.operation_id == operation_id {
                    ensure!(
                        old.record_lsn == record_lsn && old.old_physical == old_physical,
                        "conflicting TRUNCATE retry"
                    );
                    return Ok(());
                }
                ensure!(
                    old.record_lsn < record_lsn,
                    "TRUNCATE barriers are out of WAL order"
                );
                let generation = self
                    .state
                    .generation(&old.operation_id)?
                    .context("prior TRUNCATE journal missing")?;
                ensure!(
                    generation.phase == GenerationPhase::Replayed,
                    "prior TRUNCATE publication is incomplete"
                );
                ensure!(
                    old.new_physical.as_deref() == Some(old_physical.as_str()),
                    "prior TRUNCATE physical handoff is incomplete"
                );
            }
            if self.state.compare_exchange_metadata(
                &key,
                previous.as_deref(),
                &serde_json::to_vec(&next)?,
            )? {
                return Ok(());
            }
        }
    }
}

fn truncate_operation_id(source_identity: &str, oid: u32, record_lsn: u64) -> String {
    let digest = Sha256::digest(format!("truncate/{source_identity}/{oid}/{record_lsn}"));
    format!("truncate_{}", &hex::encode(digest)[..40])
}

fn truncate_barrier_key(oid: u32) -> String {
    format!("truncate-barrier/{oid}")
}
fn physical_key(oid: u32) -> String {
    format!("relation-physical/{oid}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::snowflake::state::StateIdentity;
    use crate::schema::{RelName, ReplIdent};
    use serde_json::json;
    use walrus::pg::walparser::RelFileNode;

    #[test]
    fn truncate_operation_is_stable_and_wal_scoped() {
        assert_eq!(
            truncate_operation_id("s", 1, 10),
            truncate_operation_id("s", 1, 10)
        );
        assert_ne!(
            truncate_operation_id("s", 1, 10),
            truncate_operation_id("s", 1, 11)
        );
        assert_ne!(
            truncate_operation_id("s", 1, 10),
            truncate_operation_id("s", 2, 10)
        );
    }

    #[test]
    fn physical_handoff_requires_published_barrier_and_rotates_once() {
        let dir = tempfile::tempdir().unwrap();
        let config: SnowflakeConfig = serde_json::from_value(json!({
            "account_url":"https://test.snowflakecomputing.com", "user":"USER", "role":"ROLE", "warehouse":"WH", "database":"DB",
            "auth":{"method":"oauth","token_file":"unused"},
            "state":{"directory":dir.path(),"max_bytes":1_000_000},
            "stage":{"bucket":"test","region":"us-east-1","prefix":"test/","name":"DB.INTERNAL.STAGE"}
        })).unwrap();
        let state = Arc::new(
            StateStore::open(
                dir.path(),
                StateIdentity {
                    source_system_id: 1,
                    destination_fingerprint: "test".into(),
                },
                1_000_000,
            )
            .unwrap(),
        );
        let runtime = SnowflakeRuntime {
            http: SnowflakeHttp::new(config.http_config().unwrap()).unwrap(),
            config,
            state,
            source_identity: "1".into(),
            stage: OnceCell::new(),
            tables: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            ready: Mutex::new(HashMap::new()),
            shared_ddl: Mutex::new(std::collections::HashSet::new()),
            in_flight: Semaphore::new(1),
            merge_in_flight: Semaphore::new(1),
            merge_notifies: Mutex::new(HashMap::new()),
            appliers: std::sync::OnceLock::new(),
            outstanding: std::sync::atomic::AtomicU64::new(0),
            applied: Notify::new(),
            apply_failed: std::sync::OnceLock::new(),
            landed: Default::default(),
            last_cleanup: Default::default(),
        };
        let mut desc = RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 1,
            },
            oid: 17,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Nothing,
            attributes: vec![],
        };
        runtime.lineage_with_truncate_barrier(&desc).unwrap();
        // A rewrite with no TRUNCATE barrier (VACUUM FULL, CLUSTER) keeps rows
        desc.rfn.rel_node = 2;
        runtime.lineage_with_truncate_barrier(&desc).unwrap();
        let original = desc.clone();
        desc.rfn.rel_node = 5;
        let op = truncate_operation_id("1", desc.oid, 100);
        runtime
            .state
            .prepare_generation(&op, desc.oid, 100, "schema")
            .unwrap();
        runtime
            .register_truncate_barrier(&original, 100, &op)
            .unwrap();
        assert!(runtime.lineage_with_truncate_barrier(&desc).is_err());
        runtime.state.mark_generation_loaded(&op).unwrap();
        runtime.state.mark_generation_published(&op).unwrap();
        runtime.state.mark_generation_replayed(&op).unwrap();
        runtime.lineage_with_truncate_barrier(&desc).unwrap();
        runtime.lineage_with_truncate_barrier(&desc).unwrap();
        // The storage a TRUNCATE retired never comes back
        assert!(runtime.lineage_with_truncate_barrier(&original).is_err());
        // A later rewrite after the finished handoff is accepted
        desc.rfn.rel_node = 3;
        runtime.lineage_with_truncate_barrier(&desc).unwrap();
    }
}
