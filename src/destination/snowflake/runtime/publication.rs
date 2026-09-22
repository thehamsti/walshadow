//! Journaled snapshot generation publication and crash reconciliation.

use super::*;
use crate::destination::snowflake::generation::{GenerationSqlPlan, ReplaySource};
use crate::destination::snowflake::state::{BatchPhase, GenerationPhase, GenerationRecord};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub(super) struct GenerationBinding {
    pub operation_id: String,
    pub relation_oid: u32,
    pub schema_fingerprint: String,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct SnapshotLoadSeal {
    batch_ids: Vec<String>,
    explicitly_empty: bool,
}

impl SnowflakeRuntime {
    pub fn registered_snapshot_batch_ids(&self, operation_id: &str) -> Result<Vec<String>> {
        self.state
            .generation(operation_id)?
            .context("unknown snapshot operation")?;
        let ids = self
            .state
            .get_metadata(&snapshot_batches_key(operation_id))?;
        Ok(ids
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default())
    }

    pub async fn prepare_snapshot(
        &self,
        desc: &RelDescriptor,
        snapshot_lsn: u64,
        operation_id: &str,
    ) -> Result<GenerationRecord> {
        self.ensure_table(desc).await?;
        let schema = self.schema(desc)?;
        let lock = self.table_lock(&schema).await;
        let _guard = lock.lock().await;
        let record = self.prepare_generation_journal(&schema, snapshot_lsn, operation_id)?;
        let plan = self.generation_sql_plan(&schema, record.generation_id)?;
        self.http
            .execute_sql(
                &plan.create_storage_sql,
                request_id(operation_id, "prepare-storage"),
            )
            .await?;
        Ok(record)
    }

    fn prepare_generation_journal(
        &self,
        schema: &TableSchema,
        snapshot_lsn: u64,
        operation_id: &str,
    ) -> Result<GenerationRecord> {
        let fingerprint = schema_fingerprint(schema)?;
        let route_key = snapshot_route_key(schema.relation_oid);
        let existing = self.state.generation(operation_id)?;
        if let Some(previous) = self.state.get_metadata(&route_key)? {
            let prior_id = std::str::from_utf8(&previous).context("snapshot route is not UTF-8")?;
            if prior_id != operation_id {
                let prior = self
                    .state
                    .generation(prior_id)?
                    .context("snapshot route has no journal")?;
                ensure!(
                    matches!(
                        prior.phase,
                        GenerationPhase::Replayed
                            | GenerationPhase::Retired
                            | GenerationPhase::Aborted
                    ),
                    "previous snapshot generation has not finished publication"
                );
                if let Some(record) = &existing {
                    ensure!(
                        record.phase == GenerationPhase::Prepared
                            && record.generation_id > prior.generation_id,
                        "older snapshot operation cannot replace the current route"
                    );
                }
            }
        }
        let record = self.state.prepare_generation(
            operation_id,
            schema.relation_oid,
            snapshot_lsn,
            &fingerprint,
        )?;
        ensure!(
            record.phase != GenerationPhase::Aborted,
            "aborted snapshot attempt cannot be prepared again"
        );
        let binding = GenerationBinding {
            operation_id: operation_id.into(),
            relation_oid: schema.relation_oid,
            schema_fingerprint: fingerprint,
        };
        self.state.put_metadata(
            &generation_index_key(record.generation_id),
            &serde_json::to_vec(&binding)?,
        )?;
        let expected = self.state.get_metadata(&route_key)?;
        if expected.as_deref() != Some(operation_id.as_bytes()) {
            ensure!(
                self.state.compare_exchange_metadata(
                    &route_key,
                    expected.as_deref(),
                    operation_id.as_bytes()
                )?,
                "snapshot route changed while preparing generation"
            );
        }
        Ok(record)
    }

    /// Bind a durable outbox batch to its snapshot operation before transport.
    /// The caller must invoke this after enqueue and before remote delivery.
    pub async fn register_snapshot_batch(&self, operation_id: &str, batch_id: &str) -> Result<()> {
        let batch = self
            .state
            .get_batch(batch_id)?
            .context("snapshot batch is not pending in durable outbox")?;
        let payload: Payload = serde_json::from_slice(&batch.payload)?;
        let lock = self.table_lock(&payload.schema).await;
        let _guard = lock.lock().await;
        let generation = self.generation_for_schema(&payload.schema, operation_id)?;
        ensure!(
            generation.phase == GenerationPhase::Prepared,
            "snapshot batch registered after load seal"
        );
        ensure!(
            self.state
                .get_metadata(&snapshot_load_key(operation_id))?
                .is_none(),
            "snapshot load is sealed"
        );
        let target = payload
            .snapshot
            .as_ref()
            .context("batch is not a snapshot batch")?;
        ensure!(
            target.operation_id == operation_id
                && target.generation_id == generation.generation_id
                && payload.schema.relation_oid == generation.relation_oid
                && payload
                    .rows
                    .iter()
                    .all(|row| row.load_generation == generation.generation_id),
            "snapshot batch does not match its generation"
        );
        self.state.put_metadata(
            &snapshot_batch_key(operation_id, batch_id),
            &generation.generation_id.to_le_bytes(),
        )?;
        let list_key = snapshot_batches_key(operation_id);
        loop {
            let old = self.state.get_metadata(&list_key)?;
            let mut ids: Vec<String> = old
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()?
                .unwrap_or_default();
            if ids.iter().any(|id| id == batch_id) {
                return Ok(());
            }
            ids.push(batch_id.to_owned());
            ids.sort();
            if self.state.compare_exchange_metadata(
                &list_key,
                old.as_deref(),
                &serde_json::to_vec(&ids)?,
            )? {
                return Ok(());
            }
        }
    }

    /// Start a fresh physical attempt for a logical snapshot. A partially
    /// loaded generation cannot be reused after a failed COPY because the
    /// remote table may contain a strict subset of its intended rows.
    pub async fn begin_snapshot_attempt(
        &self,
        desc: &RelDescriptor,
        snapshot_lsn: u64,
        logical_id: &str,
    ) -> Result<GenerationRecord> {
        ensure!(!logical_id.is_empty(), "empty logical snapshot id");
        let schema = self.schema(desc)?;
        if let Some(route) = self
            .state
            .get_metadata(&snapshot_route_key(schema.relation_oid))?
        {
            let prior_id = std::str::from_utf8(&route)?;
            let prior = self
                .state
                .generation(prior_id)?
                .context("snapshot route journal missing")?;
            ensure!(
                prior.relation_oid == schema.relation_oid,
                "snapshot route relation changed"
            );
            if prior.schema_fingerprint != schema_fingerprint(&schema)? {
                ensure!(
                    matches!(
                        prior.phase,
                        GenerationPhase::Replayed
                            | GenerationPhase::Retired
                            | GenerationPhase::Aborted
                    ) && self.active_generation(&schema).await? != 0
                        && self
                            .state
                            .get_metadata(&generation_floor_key(
                                self.active_generation(&schema).await?
                            ))?
                            .is_some(),
                    "prior snapshot schema changed without a published schema barrier"
                );
            } else {
                if self
                    .state
                    .get_metadata(&snapshot_logical_key(prior_id))?
                    .as_deref()
                    == Some(logical_id.as_bytes())
                {
                    ensure!(
                        prior.snapshot_lsn == snapshot_lsn,
                        "logical snapshot LSN changed"
                    );
                    match prior.phase {
                        GenerationPhase::Prepared => self.abort_snapshot(desc, prior_id).await?,
                        GenerationPhase::Loaded
                        | GenerationPhase::Published
                        | GenerationPhase::Replayed => {
                            self.publish_snapshot(desc, prior_id).await?;
                            return self
                                .state
                                .generation(prior_id)?
                                .context("published snapshot journal missing");
                        }
                        GenerationPhase::Aborted => {}
                        GenerationPhase::Retired => bail!("logical snapshot was already retired"),
                    }
                } else {
                    ensure!(
                        matches!(
                            prior.phase,
                            GenerationPhase::Aborted
                                | GenerationPhase::Replayed
                                | GenerationPhase::Retired
                        ),
                        "another snapshot operation owns this relation"
                    );
                }
            }
        }
        let sequence = self
            .state
            .allocate_sequence(&format!("snapshot-attempt/{}", schema.relation_oid))?;
        let digest = hex::encode(Sha256::digest(logical_id.as_bytes()));
        let operation_id = format!("snap_{}_{}", &digest[..24], sequence);
        self.state
            .put_metadata(&snapshot_logical_key(&operation_id), logical_id.as_bytes())?;
        self.prepare_snapshot(desc, snapshot_lsn, &operation_id)
            .await
    }

    /// Abandon only a generation that has never been exposed by the public
    /// view. Retain its storage for explicit cleanup; never drop it here.
    pub async fn abort_snapshot(&self, desc: &RelDescriptor, operation_id: &str) -> Result<()> {
        let schema = self.schema(desc)?;
        let lock = self.table_lock(&schema).await;
        let _guard = lock.lock().await;
        let record = self.generation_for_schema(&schema, operation_id)?;
        ensure!(
            matches!(
                record.phase,
                GenerationPhase::Prepared | GenerationPhase::Loaded | GenerationPhase::Aborted
            ),
            "published snapshot generation cannot be abandoned"
        );
        ensure!(
            self.active_generation(&schema).await? != record.generation_id,
            "active snapshot generation cannot be abandoned"
        );
        let plan = self.generation_sql_plan(&schema, record.generation_id)?;
        ensure!(
            self.view_comment(&schema, &plan).await?.as_deref() != Some(plan.view_marker.as_str()),
            "snapshot generation is already visible"
        );
        for id in self.state.pending_ids()? {
            let Some(batch) = self.state.get_batch(&id)? else {
                continue;
            };
            let payload: Payload = serde_json::from_slice(&batch.payload)?;
            ensure!(
                payload
                    .snapshot
                    .as_ref()
                    .is_none_or(|target| target.operation_id != operation_id),
                "snapshot has an unapplied batch; recover it before abandonment"
            );
        }
        self.state.abort_generation(operation_id)
    }

    /// Seal the complete batch set after every batch has a Snowflake apply
    /// receipt. An empty snapshot must be asserted explicitly by the caller.
    pub async fn mark_snapshot_loaded(
        &self,
        desc: &RelDescriptor,
        operation_id: &str,
        batch_ids: &[String],
        explicitly_empty: bool,
    ) -> Result<()> {
        let schema = self.schema(desc)?;
        let lock = self.table_lock(&schema).await;
        let _guard = lock.lock().await;
        let record = self.generation_for_schema(&schema, operation_id)?;
        ensure!(
            matches!(
                record.phase,
                GenerationPhase::Prepared | GenerationPhase::Loaded
            ),
            "snapshot load cannot be sealed in this phase"
        );
        let mut expected = batch_ids.to_vec();
        expected.sort();
        expected.dedup();
        ensure!(
            expected.len() == batch_ids.len(),
            "duplicate snapshot batch id"
        );
        ensure!(
            !expected.is_empty() || explicitly_empty,
            "snapshot load requires a batch or explicit empty assertion"
        );
        ensure!(
            !explicitly_empty || expected.is_empty(),
            "nonempty snapshot cannot be marked empty"
        );
        let registered: Vec<String> = self
            .state
            .get_metadata(&snapshot_batches_key(operation_id))?
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default();
        ensure!(
            expected == registered,
            "snapshot batch set does not match durable registrations"
        );
        // Enqueue and registration are separate durable writes. A concurrent
        // producer may be between them when this load barrier is requested.
        for id in self.state.pending_ids()? {
            let Some(pending) = self.state.get_batch(&id)? else {
                continue;
            };
            let payload: Payload = serde_json::from_slice(&pending.payload)?;
            ensure!(
                payload.snapshot.as_ref().is_none_or(|target| {
                    target.operation_id != operation_id || expected.contains(&pending.id)
                }),
                "snapshot batch was enqueued but not included in the load barrier"
            );
        }
        for id in &expected {
            ensure!(
                self.state.phase(id)? == Some(BatchPhase::Applied),
                "snapshot batch {id} lacks a verified Snowflake apply receipt"
            );
            ensure!(
                self.state
                    .get_metadata(&snapshot_batch_key(operation_id, id))?
                    .as_deref()
                    == Some(record.generation_id.to_le_bytes().as_slice()),
                "snapshot batch generation binding missing"
            );
        }
        let seal = SnapshotLoadSeal {
            batch_ids: expected,
            explicitly_empty,
        };
        self.state.put_metadata(
            &snapshot_load_key(operation_id),
            &serde_json::to_vec(&seal)?,
        )?;
        self.state.mark_generation_loaded(operation_id)?;
        Ok(())
    }

    pub async fn publish_snapshot(&self, desc: &RelDescriptor, operation_id: &str) -> Result<()> {
        // Live rows written into the hidden generation must land first
        self.quiesce().await?;
        self.ensure_table(desc).await?;
        let schema = self.schema(desc)?;
        let lock = self.table_lock(&schema).await;
        let _guard = lock.lock().await;
        let record = self.generation_for_schema(&schema, operation_id)?;
        ensure!(
            matches!(
                record.phase,
                GenerationPhase::Loaded | GenerationPhase::Published | GenerationPhase::Replayed
            ),
            "snapshot generation is not loaded"
        );
        let seal: SnapshotLoadSeal = serde_json::from_slice(
            &self
                .state
                .get_metadata(&snapshot_load_key(operation_id))?
                .context("snapshot load seal missing")?,
        )?;
        let registered: Vec<String> = serde_json::from_slice(
            &self
                .state
                .get_metadata(&snapshot_batches_key(operation_id))?
                .unwrap_or_else(|| b"[]".to_vec()),
        )?;
        ensure!(
            seal.batch_ids == registered && (!seal.batch_ids.is_empty() || seal.explicitly_empty),
            "snapshot load seal no longer matches registered batches"
        );
        for id in &seal.batch_ids {
            ensure!(
                self.state.phase(id)? == Some(BatchPhase::Applied),
                "snapshot batch {id} is no longer applied"
            );
        }
        let plan = self.generation_sql_plan(&schema, record.generation_id)?;
        ensure!(
            self.storage_exists(&plan).await?,
            "snapshot generation storage is missing"
        );
        let active = self.active_generation(&schema).await?;
        let view = self.view_comment(&schema, &plan).await?;
        if view.as_deref() != Some(&plan.view_marker) {
            ensure!(
                active != record.generation_id,
                "active pointer advanced but public view did not"
            );
            ensure!(
                record.phase == GenerationPhase::Loaded,
                "published generation view marker missing"
            );
            self.ensure_expected_old_view(&schema, active, view.as_deref())?;
            let old = (active != 0)
                .then(|| self.generation_sql_plan(&schema, active))
                .transpose()?;
            let source = old
                .as_ref()
                .map(ReplaySource::Generation)
                .unwrap_or(ReplaySource::Base);
            let sql = if let Some(marker) = self
                .state
                .get_metadata(&truncate_generation_key(operation_id))?
            {
                ensure!(
                    marker == record.snapshot_lsn.to_le_bytes(),
                    "truncate cutoff changed"
                );
                plan.replay_live_after_record_lsn_sql(source, record.snapshot_lsn)
            } else {
                plan.replay_live_sql(source, record.snapshot_lsn)
            }
            .map_err(anyhow::Error::msg)?;
            self.http
                .execute_sql(&sql, request_id(operation_id, "replay-live"))
                .await?;
            self.switch_view(operation_id, &schema, &plan).await?;
        }
        self.finish_publication(&schema, operation_id, active, &record, &plan)
            .await
    }

    pub async fn active_generation(&self, schema: &TableSchema) -> Result<u64> {
        let key = active_key(schema);
        let Some(bytes) = self.state.get_metadata(&key)? else {
            return Ok(0);
        };
        ensure!(bytes.len() == 8, "active generation pointer is corrupt");
        Ok(u64::from_le_bytes(bytes.as_slice().try_into()?))
    }

    /// Resolve the physical table used by live WAL. The caller holds the
    /// table lock; this reconciles a crash after view switch but before CAS.
    pub async fn active_plan(&self, schema: &TableSchema) -> Result<TableSqlPlan> {
        let mut active = self.active_generation(schema).await?;
        if let Some(route) = self
            .state
            .get_metadata(&snapshot_route_key(schema.relation_oid))?
        {
            let operation_id = std::str::from_utf8(&route)?;
            let record = self
                .state
                .generation(operation_id)?
                .context("snapshot route journal missing")?;
            if record.schema_fingerprint != schema_fingerprint(schema)? {
                ensure!(
                    active != 0
                        && self
                            .state
                            .get_metadata(&generation_floor_key(active))?
                            .is_some(),
                    "snapshot route schema differs without a published schema generation"
                );
            } else {
                if record.generation_id != active
                    && matches!(
                        record.phase,
                        GenerationPhase::Loaded
                            | GenerationPhase::Published
                            | GenerationPhase::Replayed
                    )
                {
                    let plan = self.generation_sql_plan(schema, record.generation_id)?;
                    if self.view_comment(schema, &plan).await?.as_deref() == Some(&plan.view_marker)
                    {
                        ensure!(
                            self.storage_exists(&plan).await?,
                            "published generation storage missing"
                        );
                        self.finish_publication(schema, operation_id, active, &record, &plan)
                            .await?;
                        active = record.generation_id;
                    }
                }
            }
        }
        if active == 0 {
            return TableSqlPlan::new(schema, &self.config.internal_schema)
                .map_err(anyhow::Error::msg);
        }
        let mut plan = self.generation_plan(schema, active).await?;
        let binding: GenerationBinding = serde_json::from_slice(
            &self
                .state
                .get_metadata(&generation_index_key(active))?
                .context("active generation index missing")?,
        )?;
        let record = self
            .state
            .generation(&binding.operation_id)?
            .context("active generation journal missing")?;
        if let Some(floor) = self.state.get_metadata(&generation_floor_key(active))? {
            let floor: GenerationFloor = serde_json::from_slice(&floor)?;
            plan.min_commit_lsn = floor.min_commit_lsn;
            plan.min_record_lsn = floor.min_record_lsn;
        } else if self
            .state
            .get_metadata(&truncate_generation_key(&binding.operation_id))?
            .is_some()
        {
            plan.min_record_lsn = Some(record.snapshot_lsn);
        } else {
            plan.min_commit_lsn = Some(record.snapshot_lsn);
        }
        Ok(plan)
    }

    pub async fn generation_plan(
        &self,
        schema: &TableSchema,
        generation_id: u64,
    ) -> Result<TableSqlPlan> {
        let bytes = self
            .state
            .get_metadata(&generation_index_key(generation_id))?
            .context("unknown snapshot generation")?;
        let binding: GenerationBinding = serde_json::from_slice(&bytes)?;
        ensure!(
            binding.relation_oid == schema.relation_oid
                && binding.schema_fingerprint == schema_fingerprint(schema)?,
            "snapshot generation belongs to another schema"
        );
        let record = self
            .state
            .generation(&binding.operation_id)?
            .context("generation journal missing")?;
        ensure!(
            record.generation_id == generation_id,
            "generation index mismatches journal"
        );
        ensure!(
            record.phase != GenerationPhase::Aborted,
            "snapshot generation was abandoned"
        );
        let mut plan =
            TableSqlPlan::new(schema, &self.config.internal_schema).map_err(anyhow::Error::msg)?;
        plan.state_table = self
            .generation_sql_plan(schema, generation_id)?
            .storage_table;
        Ok(plan)
    }

    pub async fn publication_view(&self, schema: &TableSchema) -> Result<String> {
        let active = self.active_generation(schema).await?;
        if active == 0 {
            let plan = TableSqlPlan::new(schema, &self.config.internal_schema)
                .map_err(anyhow::Error::msg)?;
            let owner = base_view_marker(&self.source_identity, schema);
            let view = plan.view_sql.replacen(
                " COPY GRANTS AS SELECT",
                &format!(
                    " COPY GRANTS COMMENT = {} AS SELECT",
                    super::super::sql::quote_literal(&owner)
                ),
                1,
            );
            ensure!(
                view != plan.view_sql,
                "base view ownership marker was not added"
            );
            return Ok(view);
        }
        Ok(self.generation_sql_plan(schema, active)?.view_switch_sql)
    }

    fn generation_for_schema(
        &self,
        schema: &TableSchema,
        operation_id: &str,
    ) -> Result<GenerationRecord> {
        let record = self
            .state
            .generation(operation_id)?
            .context("unknown snapshot operation")?;
        ensure!(
            record.relation_oid == schema.relation_oid
                && record.schema_fingerprint == schema_fingerprint(schema)?,
            "snapshot operation belongs to another schema"
        );
        ensure!(
            self.state
                .get_metadata(&snapshot_route_key(schema.relation_oid))?
                .as_deref()
                == Some(operation_id.as_bytes()),
            "snapshot operation is not the current route"
        );
        Ok(record)
    }

    fn generation_sql_plan(
        &self,
        schema: &TableSchema,
        generation_id: u64,
    ) -> Result<GenerationSqlPlan> {
        GenerationSqlPlan::new(
            schema,
            &self.config.internal_schema,
            generation_id,
            &self.source_identity,
        )
        .map_err(anyhow::Error::msg)
    }

    fn ensure_expected_old_view(
        &self,
        schema: &TableSchema,
        active: u64,
        current: Option<&str>,
    ) -> Result<()> {
        if active == 0 && current.is_none() {
            ensure!(
                self.state
                    .get_metadata(&format!("publication-pending/{}", schema.relation_oid))?
                    .is_some(),
                "initial public view is missing without a publication barrier"
            );
            return Ok(());
        }
        let expected = if active == 0 {
            base_view_marker(&self.source_identity, schema)
        } else {
            self.generation_sql_plan(schema, active)?.view_marker
        };
        ensure!(
            current == Some(expected.as_str()),
            "public view has an unexpected owner or generation"
        );
        Ok(())
    }

    async fn switch_view(
        &self,
        operation_id: &str,
        schema: &TableSchema,
        plan: &GenerationSqlPlan,
    ) -> Result<()> {
        if let Err(error) = self
            .http
            .execute_sql(
                &plan.view_switch_sql,
                request_id(operation_id, "switch-view"),
            )
            .await
        {
            let observed = self.view_comment(schema, plan).await?;
            ensure!(
                observed.as_deref() == Some(plan.view_marker.as_str()),
                "Snowflake view publication failed or outcome is ambiguous: {error}"
            );
        }
        ensure!(
            self.view_comment(schema, plan).await?.as_deref() == Some(plan.view_marker.as_str()),
            "Snowflake view marker missing after publication"
        );
        self.record_published_view(schema, &plan.view_switch_sql)
    }

    /// Caller holds the table lock and has already observed the generation
    /// storage and the public view marker under it, so no remote recheck.
    async fn finish_publication(
        &self,
        schema: &TableSchema,
        operation_id: &str,
        old_active: u64,
        record: &GenerationRecord,
        _plan: &GenerationSqlPlan,
    ) -> Result<()> {
        let key = active_key(schema);
        let previous = if old_active == 0 {
            None
        } else {
            Some(old_active.to_le_bytes())
        };
        if old_active != record.generation_id {
            let changed = self.state.compare_exchange_metadata(
                &key,
                previous.as_ref().map(|v| v.as_slice()),
                &record.generation_id.to_le_bytes(),
            )?;
            if !changed {
                ensure!(
                    self.active_generation(schema).await? == record.generation_id,
                    "active generation changed during publication"
                );
            }
        }
        self.state.mark_generation_published(operation_id)?;
        self.state.mark_generation_replayed(operation_id)?;
        Ok(())
    }

    pub(super) async fn view_comment(
        &self,
        schema: &TableSchema,
        _plan: &GenerationSqlPlan,
    ) -> Result<Option<String>> {
        let result = self
            .http
            .execute_sql(
                &format!(
                    "SHOW VIEWS LIKE {} IN SCHEMA {}",
                    super::super::sql::quote_literal(&schema.table),
                    super::super::sql::quote_ident(&schema.database).map_err(anyhow::Error::msg)?
                ),
                Uuid::new_v4(),
            )
            .await?;
        let names = result_column(&result.row_type, "name")?;
        let comments = result_column(&result.row_type, "comment")?;
        let mut found = None;
        for row in &result.rows {
            if row.get(names).and_then(serde_json::Value::as_str) == Some(schema.table.as_str()) {
                ensure!(found.is_none(), "duplicate public view result");
                found = Some(
                    row.get(comments)
                        .and_then(serde_json::Value::as_str)
                        .context("public view comment missing")?
                        .to_owned(),
                );
            }
        }
        Ok(found)
    }

    pub(super) async fn storage_exists(&self, plan: &GenerationSqlPlan) -> Result<bool> {
        let result = self
            .http
            .execute_sql(&plan.show_storage_sql, Uuid::new_v4())
            .await?;
        let names = result_column(&result.row_type, "name")?;
        let expected = plan
            .storage_table
            .rsplit_once('.')
            .context("invalid generation storage name")?
            .1
            .trim_matches('"');
        Ok(result
            .rows
            .iter()
            .any(|row| row.get(names).and_then(serde_json::Value::as_str) == Some(expected)))
    }
}

pub(super) fn schema_fingerprint(schema: &TableSchema) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(schema)?)))
}
pub(super) fn active_key(schema: &TableSchema) -> String {
    format!("active-generation/{}", table_key(schema))
}
fn snapshot_route_key(oid: u32) -> String {
    format!("snapshot-route/{oid}")
}
pub(super) fn generation_index_key(id: u64) -> String {
    format!("generation-index/{id}")
}
fn snapshot_batch_key(operation_id: &str, batch_id: &str) -> String {
    format!("snapshot-batch/{operation_id}/{batch_id}")
}
fn snapshot_batches_key(operation_id: &str) -> String {
    format!("snapshot-batches/{operation_id}")
}
fn snapshot_load_key(operation_id: &str) -> String {
    format!("snapshot-load/{operation_id}")
}
fn snapshot_logical_key(operation_id: &str) -> String {
    format!("snapshot-logical/{operation_id}")
}
fn truncate_generation_key(operation_id: &str) -> String {
    format!("truncate-generation/{operation_id}")
}
pub(super) fn generation_floor_key(id: u64) -> String {
    format!("generation-floor/{id}")
}

#[derive(Serialize, Deserialize)]
pub(super) struct GenerationFloor {
    pub min_commit_lsn: Option<u64>,
    pub min_record_lsn: Option<u64>,
}
fn base_view_marker(source_identity: &str, schema: &TableSchema) -> String {
    format!(
        "walshadow:source:{source_identity}:table:{}",
        table_key(schema)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::snowflake::{
        state::StateIdentity,
        types::{SnowflakeColumn, SnowflakeType},
    };
    use serde_json::json;

    #[tokio::test]
    async fn active_pointer_and_aborted_generation_fail_closed() {
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
        };
        let schema = TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 1,
            key_indexes: vec![0],
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
        };
        assert_eq!(runtime.active_generation(&schema).await.unwrap(), 0);
        assert!(
            runtime
                .state
                .compare_exchange_metadata(&active_key(&schema), None, &1u64.to_le_bytes())
                .unwrap()
        );
        assert_eq!(runtime.active_generation(&schema).await.unwrap(), 1);
        let fingerprint = schema_fingerprint(&schema).unwrap();
        let record = runtime
            .state
            .prepare_generation("attempt_1", 1, 10, &fingerprint)
            .unwrap();
        runtime
            .state
            .put_metadata(
                &generation_index_key(record.generation_id),
                &serde_json::to_vec(&GenerationBinding {
                    operation_id: record.operation_id.clone(),
                    relation_oid: 1,
                    schema_fingerprint: fingerprint,
                })
                .unwrap(),
            )
            .unwrap();
        assert!(
            runtime
                .generation_plan(&schema, record.generation_id)
                .await
                .is_ok()
        );
        runtime
            .state
            .abort_generation(&record.operation_id)
            .unwrap();
        assert!(
            runtime
                .generation_plan(&schema, record.generation_id)
                .await
                .is_err()
        );
        let fingerprint = schema_fingerprint(&schema).unwrap();
        let interrupted = runtime
            .state
            .prepare_generation("attempt_2", 1, 20, &fingerprint)
            .unwrap();
        let resumed = runtime
            .prepare_generation_journal(&schema, 20, "attempt_2")
            .unwrap();
        assert_eq!(interrupted.generation_id, resumed.generation_id);
        assert_eq!(
            runtime
                .state
                .get_metadata(&snapshot_route_key(1))
                .unwrap()
                .as_deref(),
            Some(b"attempt_2".as_slice())
        );
        runtime.state.mark_generation_loaded("attempt_2").unwrap();
        runtime
            .state
            .mark_generation_published("attempt_2")
            .unwrap();
        runtime.state.mark_generation_replayed("attempt_2").unwrap();
        let next = runtime
            .state
            .prepare_generation("attempt_3", 1, 30, &fingerprint)
            .unwrap();
        assert_eq!(
            runtime
                .prepare_generation_journal(&schema, 30, "attempt_3")
                .unwrap()
                .generation_id,
            next.generation_id
        );
        assert!(
            runtime
                .prepare_generation_journal(&schema, 20, "attempt_2")
                .is_err()
        );
    }
}
