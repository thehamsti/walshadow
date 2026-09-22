//! Durable delivery coordinator. A source row is acknowledged only after a
//! verified, transactional current-state apply receipt exists in Snowflake.
use super::{
    http::{ChannelRef, SnowflakeHttp},
    sql::{TableSqlPlan, quote_ident, quote_qualified_ident},
    stage::{self, StageWriter},
    state::{DurableBatch, StateIdentity, StateStore},
    types::{SnowflakeRow, TableSchema},
};
use crate::{
    destination::config::SnowflakeConfig,
    emit::route::RouteSnapshot,
    schema::{RelDescriptor, RelName, SchemaEvent},
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify, OnceCell, Semaphore};
use uuid::Uuid;
mod lifecycle;
mod merge_queue;
mod pending;
mod publication;
mod schema_changes;

#[derive(Serialize, Deserialize)]
struct Payload {
    schema: TableSchema,
    rows: Vec<SnowflakeRow>,
    #[serde(default)]
    snapshot: Option<SnapshotTarget>,
    #[serde(default)]
    pending_generation: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotTarget {
    operation_id: String,
    generation_id: u64,
}

pub struct SnowflakeRuntime {
    pub config: SnowflakeConfig,
    pub state: Arc<StateStore>,
    pub source_identity: String,
    http: SnowflakeHttp,
    stage: OnceCell<StageWriter>,
    tables: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    channels: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    ready: Mutex<HashMap<String, Vec<u8>>>,
    /// Idempotent DDL shared by many tables (schemas, receipts), run once.
    shared_ddl: Mutex<std::collections::HashSet<String>>,
    in_flight: Semaphore,
    merge_in_flight: Semaphore,
    merge_notifies: Mutex<HashMap<String, Arc<Notify>>>,
}

impl std::fmt::Debug for SnowflakeRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeRuntime")
            .field("source_identity", &self.source_identity)
            .finish_non_exhaustive()
    }
}

impl SnowflakeRuntime {
    pub async fn open(
        config: SnowflakeConfig,
        source_system_id: u64,
        source_database: &str,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        ensure!(
            !source_database.is_empty(),
            "Snowflake replication requires an explicit source database"
        );
        let state = Arc::new(StateStore::open(
            &config.state.directory,
            StateIdentity {
                source_system_id,
                destination_fingerprint: config.fingerprint(),
            },
            config.state.max_bytes,
        )?);
        state.put_metadata("source-database", source_database.as_bytes())?;
        let http = SnowflakeHttp::new(config.http_config()?)?;
        let runtime = Arc::new(Self {
            in_flight: Semaphore::new(config.max_in_flight),
            merge_in_flight: Semaphore::new(4),
            merge_notifies: Mutex::new(HashMap::new()),
            config,
            state,
            source_identity: format!("{source_system_id}:{source_database}"),
            http,
            stage: OnceCell::new(),
            tables: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            ready: Mutex::new(HashMap::new()),
            shared_ddl: Mutex::new(std::collections::HashSet::new()),
        });
        runtime.preflight().await?;
        runtime.recover_schema_changes().await?;
        runtime.recover().await?;
        Ok(runtime)
    }

    pub async fn preflight(&self) -> Result<()> {
        self.http
            .execute_sql(
                &format!(
                    "CREATE SCHEMA IF NOT EXISTS {}",
                    ident(&self.config.internal_schema)?
                ),
                Uuid::new_v4(),
            )
            .await?;
        // The external stage is provisioned with a Snowflake storage integration;
        // daemon credentials never appear in SQL or generated configuration.
        let result = self
            .http
            .execute_sql(
                &format!(
                    "DESC STAGE {}",
                    quote_qualified_ident(&self.config.stage.name).map_err(anyhow::Error::msg)?
                ),
                Uuid::new_v4(),
            )
            .await?;
        let expected = format!(
            "s3://{}/{}/",
            self.config.stage.bucket,
            self.config.stage.prefix.trim_end_matches('/')
        );
        validate_stage_url(&result, &expected)?;
        Ok(())
    }

    fn schema(&self, desc: &RelDescriptor) -> Result<TableSchema> {
        let namespace = self
            .config
            .schema_mapping
            .get(desc.rel_name.namespace.as_ref())
            .map(String::as_str)
            .unwrap_or(&desc.rel_name.namespace);
        ensure!(
            namespace != self.config.internal_schema,
            "source schema collides with Snowflake internal schema"
        );
        TableSchema::from_descriptor(desc, namespace, &desc.rel_name.name, &[])
            .map_err(anyhow::Error::msg)
    }

    pub async fn schema_for(
        &self,
        desc: &RelDescriptor,
        route: &RouteSnapshot,
    ) -> Result<TableSchema> {
        let schema = self.schema(desc)?;
        ensure!(
            route.mapping.target.database == schema.database
                && route.mapping.target.table == schema.table,
            "Snowflake route target differs from the declared source-shaped schema"
        );
        ensure!(
            route.mapping.columns.len() == schema.columns.len(),
            "Snowflake requires a complete source-shaped mapping"
        );
        for column in &schema.columns {
            ensure!(
                route
                    .column_rules
                    .settings(&desc.rel_name, &column.source_name)
                    .target_type
                    .is_none(),
                "Snowflake explicit column casts require a schema rebuild"
            );
            ensure!(
                route
                    .mapping
                    .columns
                    .iter()
                    .any(|m| m.src_attnum == column.attnum && m.target_name == column.name),
                "Snowflake column remapping is not supported by this schema generation"
            );
        }
        self.ensure_table(desc).await?;
        Ok(schema)
    }

    pub async fn lineage(&self, desc: &RelDescriptor) -> Result<(u64, u64)> {
        self.lineage_with_truncate_barrier(desc)
    }

    async fn table_lock(&self, schema: &TableSchema) -> Arc<Mutex<()>> {
        self.tables
            .lock()
            .await
            .entry(table_key(schema))
            .or_default()
            .clone()
    }

    pub async fn ensure_table(&self, desc: &RelDescriptor) -> Result<()> {
        self.lineage(desc).await?;
        self.ensure_schema(&self.schema(desc)?).await
    }

    /// Initial loads keep base storage hidden until their complete generation
    /// is published. Live WAL may continue writing into that hidden storage.
    pub fn defer_publication(&self, desc: &RelDescriptor) -> Result<()> {
        self.state
            .put_metadata(&format!("publication-pending/{}", desc.oid), b"1")
    }

    async fn ensure_schema(&self, schema: &TableSchema) -> Result<()> {
        let key = table_key(schema);
        ensure!(
            self.state
                .get_metadata(&format!("schema-retired/{key}"))?
                .is_none(),
            "Snowflake descriptor belongs to a retired schema generation"
        );
        let bytes = serde_json::to_vec(schema)?;
        if let Some(previous) = self.ready.lock().await.get(&key) {
            ensure!(
                previous == &bytes,
                "Snowflake schema changed without a schema barrier"
            );
            return Ok(());
        }
        let lock = self.table_lock(schema).await;
        let _guard = lock.lock().await;
        // Persist before remote effects; a changed schema on restart is rejected.
        self.state
            .put_metadata(&format!("table-schema/{key}"), &bytes)?;
        let plan =
            TableSqlPlan::new(schema, &self.config.internal_schema).map_err(anyhow::Error::msg)?;
        self.shared_ddl(format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            ident(&schema.database)?
        ))
        .await?;
        self.shared_ddl(plan.create_receipts_sql.clone()).await?;
        // Storage chain and the view listing are independent round trips
        let storage = async {
            tokio::try_join!(
                self.http
                    .execute_sql(&plan.create_landing_sql, Uuid::new_v4()),
                self.http
                    .execute_sql(&plan.create_state_sql, Uuid::new_v4()),
            )?;
            self.http
                .execute_sql(
                    &plan
                        .create_streaming_pipe_sql(schema, &pipe_name(schema))
                        .map_err(anyhow::Error::msg)?,
                    Uuid::new_v4(),
                )
                .await?;
            let foreign = self
                .http
                .execute_sql(
                    &format!(
                        "SELECT COUNT(*) FROM {} WHERE _WS_SOURCE_IDENTITY <> {}",
                        plan.state_table,
                        super::sql::quote_literal(&self.source_identity)
                    ),
                    Uuid::new_v4(),
                )
                .await?;
            ensure!(
                receipt_exists(&foreign.rows, 0)?,
                "Snowflake state storage belongs to another PostgreSQL source"
            );
            Ok(())
        };
        let views = async {
            self.http
                .execute_sql(
                    &format!(
                        "SHOW VIEWS LIKE {} IN SCHEMA {}",
                        super::sql::quote_literal(&schema.table),
                        ident(&schema.database)?
                    ),
                    Uuid::new_v4(),
                )
                .await
        };
        let ((), views) = tokio::try_join!(storage, views)?;
        // Reconcile a view replacement committed before the local active
        // pointer. Recreating the base view here would undo a published load.
        let _active_plan = self.active_plan(schema).await?;
        let active = self.active_generation(schema).await?;
        let owner = if active == 0 {
            format!("walshadow:source:{}:table:{key}", self.source_identity)
        } else {
            super::generation::GenerationSqlPlan::new(
                schema,
                &self.config.internal_schema,
                active,
                &self.source_identity,
            )
            .map_err(anyhow::Error::msg)?
            .view_marker
        };
        let names = result_column(&views.row_type, "name")?;
        let comments = result_column(&views.row_type, "comment")?;
        for row in &views.rows {
            if row.get(names).and_then(serde_json::Value::as_str) == Some(&schema.table) {
                ensure!(
                    row.get(comments).and_then(serde_json::Value::as_str) == Some(&owner),
                    "refusing to replace a Snowflake view owned by another source or application"
                );
            }
        }
        if active != 0
            || self
                .state
                .get_metadata(&format!("publication-pending/{}", schema.relation_oid))?
                .is_none()
        {
            let view = self.publication_view(schema).await?;
            self.http.execute_sql(&view, Uuid::new_v4()).await?;
        }
        self.ready.lock().await.insert(key, bytes);
        Ok(())
    }

    async fn shared_ddl(&self, sql: String) -> Result<()> {
        if self.shared_ddl.lock().await.contains(&sql) {
            return Ok(());
        }
        // Concurrent first callers may both run it; the DDL is idempotent
        self.http.execute_sql(&sql, Uuid::new_v4()).await?;
        self.shared_ddl.lock().await.insert(sql);
        Ok(())
    }

    pub async fn apply_schema(&self, event: &SchemaEvent) -> Result<()> {
        self.apply_schema_at(event, 0).await
    }

    pub async fn apply_schema_at(&self, event: &SchemaEvent, commit_lsn: u64) -> Result<()> {
        match event {
            SchemaEvent::Added { desc } => self.ensure_table(desc).await,
            SchemaEvent::Changed { old, new, diff }
                if diff.is_empty()
                    && old.replident == new.replident
                    && old.rel_name == new.rel_name =>
            {
                self.ensure_table(new).await
            }
            SchemaEvent::Changed { old, new, diff } => {
                self.apply_schema_change(old, new, diff, commit_lsn).await
            }
            SchemaEvent::Dropped { oid, rel_name } => {
                self.drop_relation_at(*oid, rel_name, commit_lsn).await
            }
        }
    }

    pub async fn truncate(&self, _rel: &RelName) -> Result<()> {
        bail!(
            "Snowflake TRUNCATE requires a journaled generation publication; source progress stopped"
        )
    }

    pub async fn recover(&self) -> Result<()> {
        for id in self.state.pending_ids()? {
            let Some(batch) = self.state.get_batch(&id)? else {
                continue;
            };
            let payload: Payload =
                serde_json::from_slice(&batch.payload).context("decode durable Snowflake batch")?;
            self.ensure_schema(&payload.schema).await?;
            self.apply(&batch, &payload).await?;
        }
        Ok(())
    }

    pub async fn deliver(&self, schema: TableSchema, rows: Vec<SnowflakeRow>) -> Result<()> {
        self.enqueue_delivery(schema, rows, None, None)
            .await
            .map(|_| ())
    }

    pub async fn deliver_snapshot(
        &self,
        schema: TableSchema,
        rows: Vec<SnowflakeRow>,
        operation_id: &str,
    ) -> Result<String> {
        let generation = self
            .state
            .generation(operation_id)?
            .context("snapshot generation not prepared")?;
        ensure!(
            generation.relation_oid == schema.relation_oid,
            "snapshot relation mismatch"
        );
        ensure!(
            rows.iter()
                .all(|row| row.load_generation == generation.generation_id),
            "snapshot rows have wrong generation"
        );
        self.enqueue_delivery(
            schema,
            rows,
            Some(SnapshotTarget {
                operation_id: operation_id.into(),
                generation_id: generation.generation_id,
            }),
            None,
        )
        .await
    }

    async fn enqueue_pending_delivery(
        &self,
        schema: TableSchema,
        rows: Vec<SnowflakeRow>,
        captured_generation: u64,
    ) -> Result<String> {
        self.enqueue_delivery(schema, rows, None, Some(captured_generation))
            .await
    }

    async fn enqueue_delivery(
        &self,
        schema: TableSchema,
        rows: Vec<SnowflakeRow>,
        snapshot: Option<SnapshotTarget>,
        pending_generation: Option<u64>,
    ) -> Result<String> {
        ensure!(!rows.is_empty(), "cannot deliver an empty Snowflake batch");
        self.ensure_schema(&schema).await?;
        let channel = pipe_name(&schema);
        let sequence = self.state.allocate_sequence(&channel)?;
        let payload = Payload {
            schema,
            rows,
            snapshot,
            pending_generation,
        };
        let bytes = serde_json::to_vec(&payload)?;
        let mut hash = Sha256::new();
        hash.update(sequence.to_be_bytes());
        hash.update(&bytes);
        let id = hex::encode(hash.finalize());
        let batch = DurableBatch {
            id,
            channel,
            sequence,
            expected_rows: payload.rows.len() as u64,
            payload: bytes,
        };
        self.state.enqueue(&batch)?;
        if let Some(snapshot) = &payload.snapshot {
            self.register_snapshot_batch(&snapshot.operation_id, &batch.id)
                .await?;
        }
        self.apply(&batch, &payload).await?;
        Ok(batch.id)
    }

    async fn apply(&self, batch: &DurableBatch, payload: &Payload) -> Result<()> {
        if self.state.phase(&batch.id)? == Some(super::state::BatchPhase::Applied) {
            return Ok(());
        }
        let _permit = self.in_flight.acquire().await?;
        let plan = TableSqlPlan::new(&payload.schema, &self.config.internal_schema)
            .map_err(anyhow::Error::msg)?;
        let receipt = self
            .http
            .execute_sql(&plan.receipt_sql(&batch.id), Uuid::new_v4())
            .await?;
        if receipt_exists(&receipt.rows, batch.expected_rows)? {
            self.mark_verified_or_applied(&batch.id)?;
            self.state.mark_applied(&batch.id)?;
            return Ok(());
        }
        let values = payload
            .rows
            .iter()
            .map(|r| stage::to_json(&payload.schema, r, &batch.id))
            .collect::<Result<Vec<_>>>()?;
        let size = values
            .iter()
            .try_fold(0usize, |total, row| -> Result<usize> {
                Ok(total + serde_json::to_vec(row)?.len() + 1)
            })?;
        if size >= 3_500_000 || payload.snapshot.is_some() {
            let stage = self
                .stage
                .get_or_try_init(|| {
                    StageWriter::new(stage::StageConfig {
                        bucket: self.config.stage.bucket.clone(),
                        prefix: self.config.stage.prefix.trim_end_matches('/').to_owned(),
                        region: self.config.stage.region.clone(),
                        stage_name: self.config.stage.name.clone(),
                        max_file_bytes: 128 << 20,
                    })
                })
                .await?;
            let file = stage
                .stage_batch(&batch.id, &payload.schema, &payload.rows)
                .await?;
            let sql = plan
                .copy_into_sql(
                    &payload.schema,
                    &self.config.stage.name,
                    &[file.relative_path],
                )
                .map_err(anyhow::Error::msg)?;
            self.http
                .execute_sql(&sql, request_id(&batch.id, "copy"))
                .await?;
        } else {
            let channel = ChannelRef {
                database: self.config.database.clone(),
                schema: self.config.internal_schema.clone(),
                pipe: batch.channel.clone(),
                channel: format!(
                    "WS_{}",
                    batch.sequence % self.config.channels_per_table as u64
                ),
            };
            let channel_lock = self
                .channels
                .lock()
                .await
                .entry(format!("{}/{}", channel.pipe, channel.channel))
                .or_default()
                .clone();
            let _channel_guard = channel_lock.lock().await;
            // Every row remains in the fsynced outbox until its apply receipt.
            // Discarding an uncertain uncommitted channel tail is safe because
            // the exact immutable batch is replayed and checked below.
            let opened = self
                .http
                .reopen_channel_with_durable_replay(&channel, None)
                .await?;
            ensure!(
                opened.status.rows_errors == 0,
                "Snowflake channel has rejected rows"
            );
            if opened.status.last_committed_offset_token.as_deref() != Some(batch.id.as_str()) {
                self.http
                    .append_rows(
                        &channel,
                        &opened.continuation_token,
                        &batch.id,
                        &batch.id,
                        &values,
                        request_id(&batch.id, "append"),
                    )
                    .await?;
            }
            let mut committed = false;
            for _ in 0..120 {
                let status = self.http.channel_status(&channel).await?;
                ensure!(
                    status.rows_errors == 0,
                    "Snowflake rejected one or more batch rows"
                );
                if status.last_committed_offset_token.as_deref() == Some(batch.id.as_str()) {
                    committed = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            ensure!(
                committed,
                "Snowflake channel did not commit batch before timeout"
            );
        }
        drop(_permit);
        if payload.snapshot.is_none() && payload.pending_generation.is_none() {
            let landing_plan = TableSqlPlan::new(&payload.schema, &self.config.internal_schema)
                .map_err(anyhow::Error::msg)?;
            self.verify_batch(batch, &landing_plan).await?;
            let key = table_key(&payload.schema);
            let notify = self
                .merge_notifies
                .lock()
                .await
                .entry(key)
                .or_default()
                .clone();
            notify.notify_one();
            let lock = self.table_lock(&payload.schema).await;
            let _table_guard = lock.lock().await;
            return self
                .materialize_live_group(batch, &payload.schema, &notify)
                .await;
        }
        if let Some(snapshot) = &payload.snapshot {
            // Verify outside the table lock so sibling batches become group
            // candidates while an earlier group's MERGE holds it.
            let landing_plan = TableSqlPlan::new(&payload.schema, &self.config.internal_schema)
                .map_err(anyhow::Error::msg)?;
            self.verify_batch(batch, &landing_plan).await?;
            let lock = self.table_lock(&payload.schema).await;
            let _table_guard = lock.lock().await;
            if self.state.phase(&batch.id)? == Some(super::state::BatchPhase::Applied) {
                // A sibling's group committed and read back this receipt
                return Ok(());
            }
            let plan = self
                .generation_plan(&payload.schema, snapshot.generation_id)
                .await?;
            return self
                .materialize_snapshot_group(batch, snapshot, &payload.schema, &plan)
                .await;
        }
        let lock = self.table_lock(&payload.schema).await;
        let _table_guard = lock.lock().await;
        let mut plan = self.active_plan(&payload.schema).await?;
        if let Some(expected) = payload.pending_generation {
            ensure!(
                self.active_generation(&payload.schema).await? == expected,
                "pending row generation was superseded during delivery"
            );
            plan.min_commit_lsn = None;
            plan.min_record_lsn = None;
        }
        self.materialize(batch, &plan).await
    }

    async fn materialize(&self, batch: &DurableBatch, plan: &TableSqlPlan) -> Result<()> {
        self.verify_batch(batch, plan).await?;
        let _merge_permit = self.merge_in_flight.acquire().await?;
        self.http
            .execute_sql_multi(
                &[
                    "BEGIN TRANSACTION".into(),
                    plan.merge_sql(&batch.id),
                    plan.insert_receipt_sql(&batch.id, batch.expected_rows),
                    "COMMIT".into(),
                ],
                request_id(&batch.id, &format!("apply:{}", plan.state_table)),
            )
            .await?;
        let receipt = self
            .http
            .execute_sql(&plan.receipt_sql(&batch.id), Uuid::new_v4())
            .await?;
        ensure!(
            receipt_exists(&receipt.rows, batch.expected_rows)?,
            "Snowflake apply receipt missing after commit"
        );
        self.state.mark_applied(&batch.id)?;
        Ok(())
    }

    async fn verify_batch(&self, batch: &DurableBatch, plan: &TableSqlPlan) -> Result<()> {
        let complete = self
            .http
            .execute_sql(
                &plan.batch_completeness_sql(&batch.id, batch.expected_rows),
                Uuid::new_v4(),
            )
            .await?;
        ensure!(
            receipt_exists(&complete.rows, batch.expected_rows)?,
            "Snowflake batch is incomplete or has conflicting event payloads"
        );
        self.mark_verified_or_applied(&batch.id)
    }

    fn mark_verified_or_applied(&self, id: &str) -> Result<()> {
        if self.state.phase(id)? == Some(super::state::BatchPhase::Applied) {
            return Ok(());
        }
        match self.state.mark_verified(id) {
            Ok(()) => Ok(()),
            Err(_) if self.state.phase(id)? == Some(super::state::BatchPhase::Applied) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Caller holds the table lock. This blocks a generation swap while
    /// verified live batches coalesce and the state-table MERGE commits.
    async fn materialize_live_group(
        &self,
        first: &DurableBatch,
        schema: &TableSchema,
        notify: &Notify,
    ) -> Result<()> {
        if self.state.phase(&first.id)? == Some(super::state::BatchPhase::Applied) {
            // An earlier group committed and read back this receipt
            return Ok(());
        }
        let plan = self.active_plan(schema).await?;
        let receipt = self
            .http
            .execute_sql(&plan.receipt_sql(&first.id), Uuid::new_v4())
            .await?;
        if receipt_exists(&receipt.rows, first.expected_rows)? {
            self.state.mark_applied(&first.id)?;
            return Ok(());
        }
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.config.merge_interval_ms);
        let selected = loop {
            let (candidates, budget_full) = self.state.verified_candidates_with_fullness(
                &first.channel,
                first.sequence,
                merge_queue::MAX_MERGE_BYTES,
            )?;
            let selected = merge_queue::select(first, candidates, schema)?;
            if budget_full || tokio::time::Instant::now() >= deadline {
                break selected;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break selected,
                _ = notify.notified() => {},
            }
        };
        let _merge_permit = self.merge_in_flight.acquire().await?;
        let ids = selected.iter().map(|b| b.id.clone()).collect::<Vec<_>>();
        let receipts = selected
            .iter()
            .map(|b| (b.id.clone(), b.expected_rows))
            .collect::<Vec<_>>();
        self.http
            .execute_sql_multi(
                &[
                    "BEGIN TRANSACTION".into(),
                    plan.merge_sql_group(&ids).map_err(anyhow::Error::msg)?,
                    plan.merge_receipts_sql(&receipts)
                        .map_err(anyhow::Error::msg)?,
                    "COMMIT".into(),
                ],
                merge_queue::request_id(&plan.state_table, &selected),
            )
            .await?;
        self.read_back_group(&plan, &selected).await
    }

    /// Caller holds the table lock and has verified `first`. Coalesce every
    /// verified batch of the same snapshot generation into one MERGE and one
    /// receipt transaction. The MERGE keeps the newest version per key, so a
    /// group reaches the same state as applying its batches one at a time.
    async fn materialize_snapshot_group(
        &self,
        first: &DurableBatch,
        target: &SnapshotTarget,
        schema: &TableSchema,
        plan: &TableSqlPlan,
    ) -> Result<()> {
        let candidates =
            self.state
                .verified_candidates(&first.channel, 0, merge_queue::MAX_MERGE_BYTES)?;
        let selected = merge_queue::select_snapshot(first, candidates, schema, target)?;
        let _merge_permit = self.merge_in_flight.acquire().await?;
        let ids = selected.iter().map(|b| b.id.clone()).collect::<Vec<_>>();
        let receipts = selected
            .iter()
            .map(|b| (b.id.clone(), b.expected_rows))
            .collect::<Vec<_>>();
        self.http
            .execute_sql_multi(
                &[
                    "BEGIN TRANSACTION".into(),
                    plan.merge_sql_group(&ids).map_err(anyhow::Error::msg)?,
                    plan.merge_receipts_sql(&receipts)
                        .map_err(anyhow::Error::msg)?,
                    "COMMIT".into(),
                ],
                merge_queue::request_id(&plan.state_table, &selected),
            )
            .await?;
        self.read_back_group(plan, &selected).await
    }

    /// Read every receipt of a committed group back in one statement before
    /// any batch is marked applied.
    async fn read_back_group(&self, plan: &TableSqlPlan, selected: &[DurableBatch]) -> Result<()> {
        let ids = selected.iter().map(|b| b.id.clone()).collect::<Vec<_>>();
        let result = self
            .http
            .execute_sql(
                &plan.receipts_sql_group(&ids).map_err(anyhow::Error::msg)?,
                Uuid::new_v4(),
            )
            .await?;
        let counts = group_receipts(&result.rows)?;
        for batch in selected {
            ensure!(
                counts.get(batch.id.as_str()) == Some(&batch.expected_rows),
                "Snowflake grouped apply receipt missing after commit"
            );
        }
        for batch in selected {
            self.state.mark_applied(&batch.id)?;
        }
        Ok(())
    }
}

/// Parse `(BATCH_ID, EXPECTED_COUNT)` rows; a duplicate id is corruption.
fn group_receipts(rows: &[Vec<serde_json::Value>]) -> Result<HashMap<&str, u64>> {
    let mut counts = HashMap::with_capacity(rows.len());
    for row in rows {
        ensure!(row.len() == 2, "invalid Snowflake grouped receipt row");
        let id = row[0]
            .as_str()
            .context("invalid Snowflake receipt batch id")?;
        let count = row[1]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| row[1].as_u64())
            .context("invalid Snowflake receipt count")?;
        ensure!(
            counts.insert(id, count).is_none(),
            "duplicate Snowflake batch receipt"
        );
    }
    Ok(counts)
}

fn ident(value: &str) -> Result<String> {
    quote_ident(value).map_err(anyhow::Error::msg)
}

fn validate_stage_url(result: &super::http::SqlResult, expected: &str) -> Result<()> {
    let parent = result_column(&result.row_type, "parent_property")?;
    let property = result_column(&result.row_type, "property")?;
    let value = result_column(&result.row_type, "property_value")?;
    let matches = result
        .rows
        .iter()
        .filter(|row| {
            row.get(parent).and_then(serde_json::Value::as_str) == Some("STAGE_LOCATION")
                && row.get(property).and_then(serde_json::Value::as_str) == Some("URL")
        })
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "external stage must report exactly one location URL"
    );
    // DESCRIBE STAGE encodes URL as a JSON array in its VARCHAR result cell.
    let urls: Vec<String> = serde_json::from_str(
        matches[0]
            .get(value)
            .and_then(serde_json::Value::as_str)
            .context("missing stage URL property value")?,
    )
    .context("invalid stage URL property value")?;
    ensure!(
        urls.len() == 1 && urls[0] == expected,
        "external stage URL does not match configured S3 bucket/prefix"
    );
    Ok(())
}
fn result_column(row_type: &serde_json::Value, name: &str) -> Result<usize> {
    row_type
        .as_array()
        .and_then(|columns| {
            columns.iter().position(|column| {
                column
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|n| n.eq_ignore_ascii_case(name))
            })
        })
        .with_context(|| format!("Snowflake result is missing column {name}"))
}
fn table_key(schema: &TableSchema) -> String {
    let bytes = serde_json::to_vec(&(schema.relation_oid, &schema.database, &schema.table))
        .expect("serializable table identity");
    hex::encode(Sha256::digest(bytes))
}
fn pipe_name(schema: &TableSchema) -> String {
    let shape = serde_json::to_vec(schema).expect("serializable schema");
    // Streaming REST resolves pipe names case-insensitively, even when the
    // SQL API created them with quoted identifiers.
    format!("WS_{}_PIPE", hex::encode_upper(Sha256::digest(shape)))
}
fn request_id(batch: &str, operation: &str) -> Uuid {
    let bytes = Sha256::digest(format!("{batch}/{operation}"));
    let mut id = [0; 16];
    id.copy_from_slice(&bytes[..16]);
    Uuid::from_bytes(id)
}
fn receipt_exists(rows: &[Vec<serde_json::Value>], expected: u64) -> Result<bool> {
    if rows.is_empty() {
        return Ok(false);
    }
    ensure!(
        rows.len() == 1 && rows[0].len() == 1,
        "invalid or duplicate Snowflake batch receipt"
    );
    let value = &rows[0][0];
    let count = value
        .as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| value.as_u64())
        .context("invalid Snowflake receipt count")?;
    ensure!(count == expected, "Snowflake receipt row count mismatch");
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn streaming_pipe_name_survives_case_insensitive_rest_resolution() {
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "source_table".into(),
            columns: vec![],
            key_indexes: vec![],
            relation_oid: 42,
        };
        let name = pipe_name(&schema);
        assert_eq!(name, name.to_ascii_uppercase());
        assert_eq!(name, pipe_name(&schema));
    }
    #[test]
    fn stage_location_uses_describe_array_and_exact_property() {
        let mut result = super::super::http::SqlResult {
            statement_handle: String::new(),
            row_type: json!([{"name":"parent_property"},{"name":"property"},{"name":"property_value"}]),
            rows: vec![vec![
                json!("STAGE_LOCATION"),
                json!("URL"),
                json!("[\"s3://bucket/prefix/\"]"),
            ]],
        };
        assert!(validate_stage_url(&result, "s3://bucket/prefix/").is_ok());
        assert!(validate_stage_url(&result, "s3://other/prefix/").is_err());
        result.rows[0][0] = json!("STAGE_FILE_FORMAT");
        assert!(validate_stage_url(&result, "s3://bucket/prefix/").is_err());
        result.rows[0][0] = json!("STAGE_LOCATION");
        result.rows[0][2] = json!("[\"s3://bucket/prefix/\",\"s3://other/\"]");
        assert!(validate_stage_url(&result, "s3://bucket/prefix/").is_err());
    }
    #[test]
    fn receipts_require_one_exact_count() {
        assert!(!receipt_exists(&[], 2).unwrap());
        assert!(receipt_exists(&[vec![json!("2")]], 2).unwrap());
        assert!(receipt_exists(&[vec![json!("1")]], 2).is_err());
        assert!(receipt_exists(&[vec![json!(2)], vec![json!(2)]], 2).is_err());
        assert!(receipt_exists(&[vec![json!("2.0")]], 2).is_err());
    }
    #[test]
    fn retry_identifiers_are_stable_and_operation_scoped() {
        assert_eq!(request_id("a", "copy"), request_id("a", "copy"));
        assert_ne!(request_id("a", "copy"), request_id("a", "apply"));
    }

    fn sql_result(rows: serde_json::Value) -> &'static str {
        Box::leak(
            json!({"statementHandle":"00000000-0000-0000-0000-000000000001",
            "resultSetMetaData":{"rowType":[{"name":"COUNT"}],"partitionInfo":[{}]},"data":rows})
            .to_string()
            .into_boxed_str(),
        )
    }

    fn child_result(id: u8) -> &'static str {
        Box::leak(
            sql_result(json!([]))
                .replace("000000000001", &format!("{id:012}"))
                .into_boxed_str(),
        )
    }

    async fn fixture(
        responses: Vec<(u16, &'static str)>,
    ) -> (
        tempfile::TempDir,
        SnowflakeRuntime,
        DurableBatch,
        TableSqlPlan,
        tokio::task::JoinHandle<Vec<String>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (url, requests) = super::super::http::tests::server(responses).await;
        let http = super::super::http::tests::client(url).await;
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
            merge_in_flight: Semaphore::new(4),
            merge_notifies: Mutex::new(HashMap::new()),
        };
        let batch = DurableBatch {
            id: "batch1".into(),
            channel: "c".into(),
            sequence: 1,
            expected_rows: 2,
            payload: vec![1, 2, 3],
        };
        runtime.state.enqueue(&batch).unwrap();
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            columns: vec![super::super::types::SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "id".into(),
                type_oid: 23,
                data_type: super::super::types::SnowflakeType::Number {
                    precision: 10,
                    scale: 0,
                },
                not_null: true,
            }],
            key_indexes: vec![0],
            relation_oid: 1,
        };
        let plan = TableSqlPlan::new(&schema, "INTERNAL").unwrap();
        (dir, runtime, batch, plan, requests)
    }

    #[tokio::test]
    async fn incomplete_ingestion_never_submits_merge_or_retires_payload() {
        let (_dir, runtime, batch, plan, requests) =
            fixture(vec![(200, sql_result(json!([])))]).await;
        assert!(runtime.materialize(&batch, &plan).await.is_err());
        assert_eq!(
            runtime.state.phase(&batch.id).unwrap(),
            Some(super::super::state::BatchPhase::Queued)
        );
        assert_eq!(runtime.state.pending().unwrap().len(), 1);
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(!requests[0].contains("MERGE INTO"));
    }

    #[tokio::test]
    async fn failed_apply_keeps_verified_batch_for_recovery() {
        let (_dir, runtime, batch, plan, requests) = fixture(vec![
            (200, sql_result(json!([["2"]]))),
            (422, r#"{"code":"1001","message":"MERGE failed"}"#),
        ])
        .await;
        assert!(runtime.materialize(&batch, &plan).await.is_err());
        assert_eq!(
            runtime.state.phase(&batch.id).unwrap(),
            Some(super::super::state::BatchPhase::Verified)
        );
        assert_eq!(runtime.state.pending().unwrap().len(), 1);
        let requests = requests.await.unwrap();
        assert!(requests[1].contains("BEGIN TRANSACTION"));
        assert!(requests[1].contains("MERGE INTO"));
        assert!(requests[1].contains("INSERT INTO"));
        assert!(requests[1].contains("COMMIT"));
    }

    #[tokio::test]
    async fn committed_apply_requires_read_back_receipt_before_retiring_payload() {
        let parent = r#"{"statementHandle":"00000000-0000-0000-0000-000000000001","statementHandles":["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004","00000000-0000-0000-0000-000000000005"]}"#;
        for receipt in [json!([]), json!([["2"]])] {
            let success = !receipt.as_array().unwrap().is_empty();
            let (_dir, runtime, batch, plan, requests) = fixture(vec![
                (200, sql_result(json!([["2"]]))),
                (200, parent),
                (200, child_result(2)),
                (200, child_result(3)),
                (200, child_result(4)),
                (200, child_result(5)),
                (200, sql_result(receipt)),
            ])
            .await;
            assert_eq!(runtime.materialize(&batch, &plan).await.is_ok(), success);
            assert_eq!(runtime.state.pending().unwrap().is_empty(), success);
            assert_eq!(requests.await.unwrap().len(), 7);
        }
    }

    fn live_batch(
        runtime: &SnowflakeRuntime,
        schema: &TableSchema,
        id: &str,
        sequence: u64,
    ) -> DurableBatch {
        let payload = Payload {
            schema: schema.clone(),
            rows: vec![],
            snapshot: None,
            pending_generation: None,
        };
        let batch = DurableBatch {
            id: id.into(),
            channel: "c".into(),
            sequence,
            expected_rows: 1,
            payload: serde_json::to_vec(&payload).unwrap(),
        };
        runtime.state.enqueue(&batch).unwrap();
        runtime.state.mark_verified(id).unwrap();
        batch
    }

    #[tokio::test]
    async fn live_merge_groups_verified_batches_and_followers_return_immediately() {
        let parent = r#"{"statementHandle":"00000000-0000-0000-0000-000000000001","statementHandles":["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004","00000000-0000-0000-0000-000000000005"]}"#;
        let responses = vec![
            (200, sql_result(json!([]))),
            (200, parent),
            (200, child_result(2)),
            (200, child_result(3)),
            (200, child_result(4)),
            (200, child_result(5)),
            (200, sql_result(json!([["first", "1"], ["second", "1"]]))),
        ];
        let (_dir, mut runtime, _, _plan, requests) = fixture(responses).await;
        runtime.config.merge_interval_ms = 1;
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![super::super::types::SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "id".into(),
                type_oid: 23,
                data_type: super::super::types::SnowflakeType::Number {
                    precision: 10,
                    scale: 0,
                },
                not_null: true,
            }],
            key_indexes: vec![0],
        };
        let first = live_batch(&runtime, &schema, "first", 2);
        let second = live_batch(&runtime, &schema, "second", 3);
        let notify = Notify::new();
        runtime
            .materialize_live_group(&first, &schema, &notify)
            .await
            .unwrap();
        assert_eq!(
            runtime.state.phase(&first.id).unwrap(),
            Some(super::super::state::BatchPhase::Applied)
        );
        assert_eq!(
            runtime.state.phase(&second.id).unwrap(),
            Some(super::super::state::BatchPhase::Applied)
        );
        runtime.config.merge_interval_ms = 15_000;
        tokio::time::timeout(
            Duration::from_millis(100),
            runtime.materialize_live_group(&second, &schema, &notify),
        )
        .await
        .unwrap()
        .unwrap();
        let requests = requests.await.unwrap();
        assert!(requests[1].contains("MERGE INTO"));
        assert!(requests[1].contains("_WS_BATCH_ID IN ('first', 'second')"));
        assert!(requests[1].contains("WHEN NOT MATCHED THEN INSERT"));
        // One grouped read-back; the follower issued no SQL of its own
        assert_eq!(requests.len(), 7);
        assert!(requests[6].contains("BATCH_ID IN ('first', 'second')"));
    }

    #[tokio::test]
    async fn grouped_read_back_refuses_a_missing_receipt() {
        let parent = r#"{"statementHandle":"00000000-0000-0000-0000-000000000001","statementHandles":["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004","00000000-0000-0000-0000-000000000005"]}"#;
        let (_dir, mut runtime, _, _plan, _requests) = fixture(vec![
            (200, sql_result(json!([]))),
            (200, parent),
            (200, child_result(2)),
            (200, child_result(3)),
            (200, child_result(4)),
            (200, child_result(5)),
            (200, sql_result(json!([["first", "1"]]))),
        ])
        .await;
        runtime.config.merge_interval_ms = 1;
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![],
            key_indexes: vec![],
        };
        let first = live_batch(&runtime, &schema, "first", 2);
        let second = live_batch(&runtime, &schema, "second", 3);
        assert!(
            runtime
                .materialize_live_group(&first, &schema, &Notify::new())
                .await
                .is_err()
        );
        for id in [&first.id, &second.id] {
            assert_eq!(
                runtime.state.phase(id).unwrap(),
                Some(super::super::state::BatchPhase::Verified)
            );
        }
    }

    fn snapshot_batch(
        runtime: &SnowflakeRuntime,
        schema: &TableSchema,
        id: &str,
        sequence: u64,
        operation_id: &str,
    ) -> DurableBatch {
        let payload = Payload {
            schema: schema.clone(),
            rows: vec![],
            snapshot: Some(SnapshotTarget {
                operation_id: operation_id.into(),
                generation_id: 7,
            }),
            pending_generation: None,
        };
        let batch = DurableBatch {
            id: id.into(),
            channel: "c".into(),
            sequence,
            expected_rows: 1,
            payload: serde_json::to_vec(&payload).unwrap(),
        };
        runtime.state.enqueue(&batch).unwrap();
        runtime.state.mark_verified(id).unwrap();
        batch
    }

    #[tokio::test]
    async fn snapshot_group_merges_only_its_generation_including_earlier_sequences() {
        let parent = r#"{"statementHandle":"00000000-0000-0000-0000-000000000001","statementHandles":["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004","00000000-0000-0000-0000-000000000005"]}"#;
        let (_dir, runtime, _, plan, requests) = fixture(vec![
            (200, parent),
            (200, child_result(2)),
            (200, child_result(3)),
            (200, child_result(4)),
            (200, child_result(5)),
            (200, sql_result(json!([["a", "1"], ["b", "1"], ["c", "1"]]))),
        ])
        .await;
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![],
            key_indexes: vec![],
        };
        let earlier = snapshot_batch(&runtime, &schema, "a", 11, "op");
        let first = snapshot_batch(&runtime, &schema, "b", 12, "op");
        let later = snapshot_batch(&runtime, &schema, "c", 13, "op");
        let other = snapshot_batch(&runtime, &schema, "z", 14, "other-op");
        let live = live_batch(&runtime, &schema, "live", 15);
        let target = SnapshotTarget {
            operation_id: "op".into(),
            generation_id: 7,
        };
        runtime
            .materialize_snapshot_group(&first, &target, &schema, &plan)
            .await
            .unwrap();
        for batch in [&earlier, &first, &later] {
            assert_eq!(
                runtime.state.phase(&batch.id).unwrap(),
                Some(super::super::state::BatchPhase::Applied)
            );
        }
        for batch in [&other, &live] {
            assert_eq!(
                runtime.state.phase(&batch.id).unwrap(),
                Some(super::super::state::BatchPhase::Verified)
            );
        }
        let requests = requests.await.unwrap();
        assert!(requests[0].contains("_WS_BATCH_ID IN ('b', 'a', 'c')"));
        assert_eq!(requests.len(), 6);
        assert!(requests[5].contains("BATCH_ID IN ('b', 'a', 'c')"));
    }

    #[test]
    fn grouped_receipts_reject_duplicates_and_parse_counts() {
        let rows = vec![vec![json!("a"), json!("2")], vec![json!("b"), json!(3)]];
        let counts = group_receipts(&rows).unwrap();
        assert_eq!(counts.get("a"), Some(&2));
        assert_eq!(counts.get("b"), Some(&3));
        let dup = vec![vec![json!("a"), json!("2")], vec![json!("a"), json!("2")]];
        assert!(group_receipts(&dup).is_err());
        assert!(group_receipts(&[vec![json!("a")]]).is_err());
    }

    #[tokio::test]
    async fn failed_live_group_keeps_every_batch_verified() {
        let (_dir, mut runtime, _, _plan, _requests) = fixture(vec![
            (200, sql_result(json!([]))),
            (422, r#"{"code":"1001","message":"MERGE failed"}"#),
        ])
        .await;
        runtime.config.merge_interval_ms = 1;
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![],
            key_indexes: vec![],
        };
        let first = live_batch(&runtime, &schema, "first", 2);
        let second = live_batch(&runtime, &schema, "second", 3);
        assert!(
            runtime
                .materialize_live_group(&first, &schema, &Notify::new())
                .await
                .is_err()
        );
        assert_eq!(
            runtime.state.phase(&first.id).unwrap(),
            Some(super::super::state::BatchPhase::Verified)
        );
        assert_eq!(
            runtime.state.phase(&second.id).unwrap(),
            Some(super::super::state::BatchPhase::Verified)
        );
    }

    #[tokio::test]
    async fn single_live_batch_waits_merge_interval_then_applies() {
        let parent = r#"{"statementHandle":"00000000-0000-0000-0000-000000000001","statementHandles":["00000000-0000-0000-0000-000000000002","00000000-0000-0000-0000-000000000003","00000000-0000-0000-0000-000000000004","00000000-0000-0000-0000-000000000005"]}"#;
        let (_dir, mut runtime, _, _plan, _requests) = fixture(vec![
            (200, sql_result(json!([]))),
            (200, parent),
            (200, child_result(2)),
            (200, child_result(3)),
            (200, child_result(4)),
            (200, child_result(5)),
            (200, sql_result(json!([["single", "1"]]))),
        ])
        .await;
        runtime.config.merge_interval_ms = 20;
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            columns: vec![],
            key_indexes: vec![],
        };
        let first = live_batch(&runtime, &schema, "single", 2);
        let start = tokio::time::Instant::now();
        runtime
            .materialize_live_group(&first, &schema, &Notify::new())
            .await
            .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
