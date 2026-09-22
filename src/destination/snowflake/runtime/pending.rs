//! Durable pending-visibility rows. A decided row enters the normal outbox and
//! receives a Snowflake apply receipt before its carry record is retired.

use super::*;
use crate::destination::snowflake::state::{PendingPhase, PendingRow};

#[derive(Serialize, Deserialize)]
struct PendingPayload {
    source_namespace: String,
    source_relname: String,
    schema: TableSchema,
    row: SnowflakeRow,
    captured_generation: u64,
}

#[derive(Debug)]
pub struct PendingCarryManifest {
    pub source_namespace: String,
    pub source_relname: String,
    pub database: String,
    pub table: String,
    pub relation_oid: u32,
    pub start_lsn: u64,
    pub xids: Vec<u32>,
    pub rows: u64,
}

impl SnowflakeRuntime {
    pub fn pending_capture_generation(&self, relation_oid: u32) -> Result<u64> {
        let key = format!("snapshot-route/{relation_oid}");
        let Some(bytes) = self.state.get_metadata(&key)? else {
            return Ok(0);
        };
        let operation_id = std::str::from_utf8(&bytes).context("snapshot route is not UTF-8")?;
        let generation = self
            .state
            .generation(operation_id)?
            .context("snapshot route lacks generation journal")?;
        ensure!(
            generation.relation_oid == relation_oid,
            "snapshot route relation mismatch"
        );
        Ok(generation.generation_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn capture_pending(
        &self,
        source_namespace: &str,
        source_relname: &str,
        schema: TableSchema,
        row: SnowflakeRow,
        id: String,
        xmin: u32,
        xmax: u32,
        captured_generation: u64,
    ) -> Result<()> {
        ensure!(schema.relation_oid != 0, "pending relation OID is missing");
        ensure!(
            !source_namespace.is_empty() && !source_relname.is_empty(),
            "pending source relation is missing"
        );
        ensure!(
            row.source_identity == self.source_identity,
            "pending source identity changed"
        );
        ensure!(row.event_id == id, "pending event ID differs from carry ID");
        let payload = serde_json::to_vec(&PendingPayload {
            source_namespace: source_namespace.into(),
            source_relname: source_relname.into(),
            schema: schema.clone(),
            row: row.clone(),
            captured_generation,
        })?;
        self.state.enqueue_pending(&PendingRow {
            id,
            relation_oid: schema.relation_oid,
            xmin,
            xmax,
            record_lsn: row.record_lsn,
            payload,
        })
    }

    pub fn pending_manifests(&self) -> Result<Vec<PendingCarryManifest>> {
        use std::collections::{BTreeMap, BTreeSet};
        let mut by_relation: BTreeMap<u32, (PendingCarryManifest, BTreeSet<u32>)> = BTreeMap::new();
        for record in self.state.all_pending()? {
            if record.phase == PendingPhase::Retired {
                continue;
            }
            let payload: PendingPayload = serde_json::from_slice(&record.row.payload)
                .context("decode durable pending row")?;
            ensure!(
                payload.schema.relation_oid == record.row.relation_oid
                    && payload.row.event_id == record.row.id
                    && payload.row.source_identity == self.source_identity,
                "durable pending row identity mismatch"
            );
            let entry = by_relation
                .entry(record.row.relation_oid)
                .or_insert_with(|| {
                    (
                        PendingCarryManifest {
                            source_namespace: payload.source_namespace.clone(),
                            source_relname: payload.source_relname.clone(),
                            database: payload.schema.database.clone(),
                            table: payload.schema.table.clone(),
                            relation_oid: record.row.relation_oid,
                            start_lsn: payload.row.commit_lsn,
                            xids: Vec::new(),
                            rows: 0,
                        },
                        BTreeSet::new(),
                    )
                });
            ensure!(
                entry.0.source_namespace == payload.source_namespace
                    && entry.0.source_relname == payload.source_relname
                    && entry.0.database == payload.schema.database
                    && entry.0.table == payload.schema.table,
                "pending relation metadata changed"
            );
            entry.0.start_lsn = entry.0.start_lsn.min(payload.row.commit_lsn);
            entry.0.rows += 1;
            for xid in [record.row.xmin, record.row.xmax] {
                if xid != 0 {
                    entry.1.insert(xid);
                }
            }
        }
        Ok(by_relation
            .into_values()
            .map(|(mut manifest, xids)| {
                manifest.xids = xids.into_iter().collect();
                manifest
            })
            .collect())
    }

    /// Decisions are persisted by the caller before invoking this method.
    /// Unknown outcomes stay carried; aborted inserts or committed deletes
    /// retire without delivery. Replaying a completed delivery is safe because
    /// the normal outbox verifies its transactional apply receipt.
    pub async fn settle_pending_relation(
        &self,
        relation_oid: u32,
        committed: &[u32],
        aborted: &[u32],
    ) -> Result<u64> {
        let records = self.state.pending_for_relation(relation_oid)?;
        let mut settled = 0;
        for record in records {
            if record.phase == PendingPhase::Retired {
                continue;
            }
            let row = &record.row;
            let insert_committed = row.xmin == 0 || committed.contains(&row.xmin);
            let insert_aborted = row.xmin != 0 && aborted.contains(&row.xmin);
            let delete_aborted = row.xmax == 0 || aborted.contains(&row.xmax);
            let delete_committed = row.xmax != 0 && committed.contains(&row.xmax);
            ensure!(
                !(insert_committed && insert_aborted) && !(delete_aborted && delete_committed),
                "contradictory pending transaction outcomes"
            );
            if record.phase == PendingPhase::Promoted {
                ensure!(
                    record.receipt.is_some(),
                    "promoted pending row lacks receipt"
                );
                self.state.retire_pending(row)?;
                settled += 1;
                continue;
            }
            if insert_aborted || delete_committed {
                self.state.retire_pending_aborted(row)?;
                settled += 1;
                continue;
            }
            if !(insert_committed && delete_aborted) {
                continue;
            }
            let payload: PendingPayload =
                serde_json::from_slice(&row.payload).context("decode pending Snowflake row")?;
            ensure!(
                payload.schema.relation_oid == row.relation_oid
                    && payload.row.event_id == row.id
                    && payload.row.record_lsn == row.record_lsn
                    && payload.row.source_identity == self.source_identity,
                "pending row payload identity mismatch"
            );
            use crate::destination::snowflake::state::GenerationPhase;
            if self.pending_generation_phase(&payload.schema, payload.captured_generation)?
                == Some(GenerationPhase::Aborted)
            {
                self.state.retire_pending_aborted(row)?;
                settled += 1;
                continue;
            }
            let active = self.active_generation(&payload.schema).await?;
            if active < payload.captured_generation {
                continue;
            }
            if active > payload.captured_generation {
                // A complete newer snapshot supersedes the old physical row.
                ensure!(
                    matches!(
                        self.pending_generation_phase(&payload.schema, active)?,
                        Some(GenerationPhase::Published | GenerationPhase::Replayed)
                    ),
                    "active generation lacks a published journal"
                );
                self.state.retire_pending_aborted(row)?;
                settled += 1;
                continue;
            }
            if self.snapshot_route_is_loading(&payload.schema).await? {
                continue;
            }
            let batch_id = self
                .enqueue_pending_delivery(
                    payload.schema,
                    vec![payload.row],
                    payload.captured_generation,
                )
                .await?;
            self.state.mark_pending_promoted(row, batch_id.as_bytes())?;
            self.state.retire_pending(row)?;
            settled += 1;
        }
        Ok(settled)
    }

    async fn snapshot_route_is_loading(&self, schema: &TableSchema) -> Result<bool> {
        use crate::destination::snowflake::state::GenerationPhase;
        let key = format!("snapshot-route/{}", schema.relation_oid);
        let Some(bytes) = self.state.get_metadata(&key)? else {
            return Ok(false);
        };
        let operation_id = std::str::from_utf8(&bytes).context("snapshot route is not UTF-8")?;
        let generation = self
            .state
            .generation(operation_id)?
            .context("snapshot route lacks generation journal")?;
        Ok(matches!(
            generation.phase,
            GenerationPhase::Prepared | GenerationPhase::Loaded
        ))
    }

    fn pending_generation_phase(
        &self,
        schema: &TableSchema,
        generation_id: u64,
    ) -> Result<Option<crate::destination::snowflake::state::GenerationPhase>> {
        if generation_id == 0 {
            return Ok(None);
        }
        let bytes = self
            .state
            .get_metadata(&format!("generation-index/{generation_id}"))?
            .context("pending generation index missing")?;
        let binding: serde_json::Value = serde_json::from_slice(&bytes)?;
        let operation_id = binding
            .get("operation_id")
            .and_then(serde_json::Value::as_str)
            .context("pending generation binding lacks operation ID")?;
        let record = self
            .state
            .generation(operation_id)?
            .context("pending generation journal missing")?;
        ensure!(
            record.generation_id == generation_id && record.relation_oid == schema.relation_oid,
            "pending generation belongs to another relation"
        );
        Ok(Some(record.phase))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::snowflake::state::{PendingPhase, StateIdentity};
    use crate::destination::snowflake::types::{SnowflakeColumn, SnowflakeType, SnowflakeValue};
    use serde_json::json;

    #[tokio::test]
    async fn pending_capture_is_idempotent_and_unknown_outcomes_do_not_promote() {
        let dir = tempfile::tempdir().unwrap();
        let config: SnowflakeConfig = serde_json::from_value(json!({
            "account_url":"https://test.snowflakecomputing.com", "user":"USER", "role":"ROLE",
            "warehouse":"WH", "database":"DB", "auth":{"method":"oauth","token_file":"unused"},
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
        let http = super::super::super::http::tests::client(
            reqwest::Url::parse("http://127.0.0.1/").unwrap(),
        )
        .await;
        let runtime = SnowflakeRuntime {
            config,
            state,
            source_identity: "1".into(),
            http,
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
        };
        let schema = TableSchema {
            database: "DB".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "id".into(),
                type_oid: 23,
                data_type: SnowflakeType::Number {
                    precision: 10,
                    scale: 0,
                },
                not_null: true,
            }],
            key_indexes: vec![0],
        };
        let row = SnowflakeRow {
            values: vec![SnowflakeValue::Number("1".into())],
            key: "key".into(),
            source_identity: "1".into(),
            relation_incarnation: 1,
            load_generation: 0,
            commit_lsn: 100,
            record_lsn: 100,
            row_ordinal: 1,
            event_id: "pending1".into(),
            deleted: false,
        };
        runtime
            .capture_pending(
                "public",
                "t",
                schema.clone(),
                row.clone(),
                "pending1".into(),
                7,
                9,
                0,
            )
            .unwrap();
        let manifests = runtime.pending_manifests().unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].source_namespace, "public");
        assert_eq!(manifests[0].source_relname, "t");
        assert_eq!(manifests[0].xids, [7, 9]);
        runtime
            .capture_pending(
                "public",
                "t",
                schema.clone(),
                row.clone(),
                "pending1".into(),
                7,
                9,
                0,
            )
            .unwrap();
        let mut different = row;
        different.values = vec![SnowflakeValue::Number("2".into())];
        assert!(
            runtime
                .capture_pending("public", "t", schema, different, "pending1".into(), 7, 9, 0)
                .is_err()
        );
        assert_eq!(
            runtime.settle_pending_relation(1, &[], &[]).await.unwrap(),
            0
        );
        assert_eq!(
            runtime.state.pending_for_relation(1).unwrap()[0].phase,
            PendingPhase::Raw
        );
        assert_eq!(
            runtime.settle_pending_relation(1, &[], &[7]).await.unwrap(),
            1
        );
        assert_eq!(
            runtime.state.pending_for_relation(1).unwrap()[0].phase,
            PendingPhase::Retired
        );
    }
}
