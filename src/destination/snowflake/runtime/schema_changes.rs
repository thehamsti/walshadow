//! Journaled source-schema barriers. A new physical state table carries a
//! projected copy of the previous generation before the public view changes.

use super::publication::{
    GenerationBinding, GenerationFloor, active_key, generation_floor_key, generation_index_key,
    schema_fingerprint,
};
use super::*;
use crate::decode::heap_decoder::missing_value_for;
use crate::destination::snowflake::generation::GenerationSqlPlan;
use crate::destination::snowflake::sql::{quote_ident, quote_literal};
use crate::destination::snowflake::state::{GenerationPhase, PendingPhase};
use crate::destination::snowflake::types::{SnowflakeValue, convert};
use crate::schema::RelName;
use crate::schema::{RelAttr, SchemaDiff};
use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};

#[derive(Clone, Serialize, Deserialize)]
struct SchemaProjection {
    expressions: Vec<String>,
    columns: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct SchemaChangeRecord {
    operation_id: String,
    commit_lsn: u64,
    old: TableSchema,
    new: TableSchema,
    old_active: u64,
    projection: SchemaProjection,
}

#[derive(Clone, Serialize, Deserialize)]
struct DropRecord {
    operation_id: String,
    commit_lsn: u64,
    schema: TableSchema,
    expected_marker: String,
}

impl SnowflakeRuntime {
    pub async fn apply_schema_change(
        &self,
        old_desc: &RelDescriptor,
        new_desc: &RelDescriptor,
        diff: &SchemaDiff,
        commit_lsn: u64,
    ) -> Result<()> {
        ensure!(
            commit_lsn != 0,
            "Snowflake schema change requires its source commit LSN"
        );
        let old = self.schema(old_desc)?;
        let new = self.schema(new_desc)?;
        let projection = SchemaProjection::new(&old, &new, new_desc, diff)?;
        self.ensure_no_open_snapshot(old.relation_oid)?;
        let digest = Sha256::digest(format!(
            "schema/{}/{}/{commit_lsn}",
            self.source_identity, old.relation_oid
        ));
        let operation_id = format!("schema_{}", &hex::encode(digest)[..40]);
        if let Some(bytes) = self
            .state
            .get_metadata(&schema_change_key(old.relation_oid))?
        {
            let existing: SchemaChangeRecord = serde_json::from_slice(&bytes)?;
            if existing.operation_id == operation_id {
                ensure!(
                    existing.old == old && existing.new == new,
                    "conflicting schema change retry"
                );
                if self
                    .state
                    .generation(&operation_id)?
                    .is_some_and(|generation| generation.phase == GenerationPhase::Replayed)
                {
                    return Ok(());
                }
                let _guards = self.lock_schema_pair(&old, &new).await;
                return self.run_schema_change(&existing).await;
            }
        }
        if table_key(&old) != table_key(&new) {
            ensure!(
                self.state
                    .get_metadata(&format!("table-schema/{}", table_key(&new)))?
                    .is_none()
                    && self.state.get_metadata(&active_key(&new))?.is_none(),
                "Snowflake rename target was previously used; a controlled rebuild is required"
            );
        }
        self.ensure_table(old_desc).await?;
        let _guards = self.lock_schema_pair(&old, &new).await;
        let record = SchemaChangeRecord {
            operation_id,
            commit_lsn,
            old: old.clone(),
            new,
            old_active: self.active_generation(&old).await?,
            projection,
        };
        let record = self.register_schema_change(record)?;
        self.run_schema_change(&record).await
    }

    pub async fn recover_schema_changes(&self) -> Result<()> {
        let relations: Vec<u32> = self
            .state
            .get_metadata("schema-change-relations")?
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default();
        for oid in relations {
            let bytes = self
                .state
                .get_metadata(&schema_change_key(oid))?
                .context("schema change journal index is incomplete")?;
            let record: SchemaChangeRecord = serde_json::from_slice(&bytes)?;
            let generation = self.state.generation(&record.operation_id)?;
            if generation
                .as_ref()
                .is_some_and(|g| g.phase == GenerationPhase::Replayed)
            {
                continue;
            }
            let _guards = self.lock_schema_pair(&record.old, &record.new).await;
            self.run_schema_change(&record).await?;
        }
        self.recover_dropped_relations().await?;
        Ok(())
    }

    pub async fn drop_relation_at(
        &self,
        oid: u32,
        rel_name: &RelName,
        commit_lsn: u64,
    ) -> Result<()> {
        ensure!(
            commit_lsn != 0,
            "Snowflake DROP requires its source commit LSN"
        );
        self.ensure_no_open_snapshot(oid)?;
        let database = self
            .config
            .schema_mapping
            .get(rel_name.namespace.as_ref())
            .map(String::as_str)
            .unwrap_or(&rel_name.namespace);
        let identity = TableSchema {
            database: database.into(),
            table: rel_name.name.to_string(),
            relation_oid: oid,
            columns: vec![],
            key_indexes: vec![],
        };
        let Some(bytes) = self
            .state
            .get_metadata(&format!("table-schema/{}", table_key(&identity)))?
        else {
            return Ok(());
        };
        let schema: TableSchema = serde_json::from_slice(&bytes)?;
        ensure!(
            schema.relation_oid == oid
                && schema.database == database
                && schema.table == rel_name.name.as_ref(),
            "DROP relation identity changed"
        );
        let lock = self.table_lock(&schema).await;
        let _guard = lock.lock().await;
        let digest = Sha256::digest(format!("drop/{}/{oid}/{commit_lsn}", self.source_identity));
        let operation_id = format!("drop_{}", &hex::encode(digest)[..40]);
        let active = self.active_generation(&schema).await?;
        let marker = if active == 0 {
            format!(
                "walshadow:source:{}:table:{}",
                self.source_identity,
                table_key(&schema)
            )
        } else {
            GenerationSqlPlan::new(
                &schema,
                &self.config.internal_schema,
                active,
                &self.source_identity,
            )
            .map_err(anyhow::Error::msg)?
            .view_marker
        };
        let record = DropRecord {
            operation_id,
            commit_lsn,
            schema,
            expected_marker: marker,
        };
        let record = self.register_drop(record)?;
        self.run_drop(&record).await
    }

    async fn recover_dropped_relations(&self) -> Result<()> {
        let relations: Vec<u32> = self
            .state
            .get_metadata("drop-relations")?
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default();
        for oid in relations {
            let bytes = self
                .state
                .get_metadata(&drop_key(oid))?
                .context("DROP journal index is incomplete")?;
            let record: DropRecord = serde_json::from_slice(&bytes)?;
            if self
                .state
                .get_metadata(&drop_complete_key(&record.operation_id))?
                .is_some()
            {
                continue;
            }
            let lock = self.table_lock(&record.schema).await;
            let _guard = lock.lock().await;
            self.run_drop(&record).await?;
        }
        Ok(())
    }

    fn register_drop(&self, candidate: DropRecord) -> Result<DropRecord> {
        let key = drop_key(candidate.schema.relation_oid);
        let current = self.state.get_metadata(&key)?;
        if let Some(bytes) = &current {
            let old: DropRecord = serde_json::from_slice(bytes)?;
            ensure!(
                old.operation_id == candidate.operation_id
                    && old.schema == candidate.schema
                    && old.expected_marker == candidate.expected_marker,
                "conflicting DROP operation or relation OID reuse"
            );
            return Ok(old);
        }
        ensure!(
            self.state
                .compare_exchange_metadata(&key, None, &serde_json::to_vec(&candidate)?)?,
            "DROP journal changed concurrently"
        );
        loop {
            let previous = self.state.get_metadata("drop-relations")?;
            let mut relations: Vec<u32> = previous
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()?
                .unwrap_or_default();
            if relations.contains(&candidate.schema.relation_oid) {
                return Ok(candidate);
            }
            relations.push(candidate.schema.relation_oid);
            relations.sort_unstable();
            if self.state.compare_exchange_metadata(
                "drop-relations",
                previous.as_deref(),
                &serde_json::to_vec(&relations)?,
            )? {
                return Ok(candidate);
            }
        }
    }

    async fn run_drop(&self, record: &DropRecord) -> Result<()> {
        self.ensure_no_open_snapshot(record.schema.relation_oid)?;
        ensure!(
            self.state
                .pending_for_relation(record.schema.relation_oid)?
                .iter()
                .all(|row| row.phase == PendingPhase::Retired),
            "DROP has unresolved pending visibility rows"
        );
        for id in self.state.pending_ids()? {
            let Some(batch) = self.state.get_batch(&id)? else {
                continue;
            };
            let payload: Payload = serde_json::from_slice(&batch.payload)?;
            ensure!(
                payload.schema.relation_oid != record.schema.relation_oid,
                "DROP has an unapplied batch for this relation"
            );
        }
        let plan = GenerationSqlPlan::new(
            &record.schema,
            &self.config.internal_schema,
            1,
            &self.source_identity,
        )
        .map_err(anyhow::Error::msg)?;
        let current = self.view_comment(&record.schema, &plan).await?;
        if let Some(marker) = current {
            ensure!(
                marker == record.expected_marker,
                "DROP view owner or generation changed"
            );
            let name = TableSqlPlan::new(&record.schema, &self.config.internal_schema)
                .map_err(anyhow::Error::msg)?
                .public_view;
            let sql = format!("DROP VIEW IF EXISTS {name}");
            if let Err(error) = self
                .http
                .execute_sql(&sql, request_id(&record.operation_id, "drop-view"))
                .await
            {
                ensure!(
                    self.view_comment(&record.schema, &plan).await?.is_none(),
                    "DROP view failed or was ambiguous: {error}"
                );
            }
        }
        ensure!(
            self.view_comment(&record.schema, &plan).await?.is_none(),
            "dropped view still exists"
        );
        self.state.put_metadata(
            &format!("schema-retired/{}", table_key(&record.schema)),
            &record.commit_lsn.to_le_bytes(),
        )?;
        self.state
            .put_metadata(&drop_complete_key(&record.operation_id), b"1")?;
        self.ready.lock().await.remove(&table_key(&record.schema));
        Ok(())
    }

    async fn lock_schema_pair(
        &self,
        old: &TableSchema,
        new: &TableSchema,
    ) -> (
        tokio::sync::OwnedMutexGuard<()>,
        Option<tokio::sync::OwnedMutexGuard<()>>,
    ) {
        let old_key = table_key(old);
        let new_key = table_key(new);
        let old_lock = self.table_lock(old).await;
        if old_key == new_key {
            return (old_lock.lock_owned().await, None);
        }
        let new_lock = self.table_lock(new).await;
        if old_key < new_key {
            (
                old_lock.lock_owned().await,
                Some(new_lock.lock_owned().await),
            )
        } else {
            (
                new_lock.lock_owned().await,
                Some(old_lock.lock_owned().await),
            )
        }
    }

    fn register_schema_change(&self, candidate: SchemaChangeRecord) -> Result<SchemaChangeRecord> {
        let key = schema_change_key(candidate.old.relation_oid);
        loop {
            let previous = self.state.get_metadata(&key)?;
            if let Some(bytes) = &previous {
                let old: SchemaChangeRecord = serde_json::from_slice(bytes)?;
                if old.operation_id == candidate.operation_id {
                    ensure!(
                        old.commit_lsn == candidate.commit_lsn
                            && old.old == candidate.old
                            && old.new == candidate.new
                            && old.projection.columns == candidate.projection.columns
                            && old.projection.expressions == candidate.projection.expressions,
                        "conflicting schema change retry"
                    );
                    return Ok(old);
                }
                let generation = self
                    .state
                    .generation(&old.operation_id)?
                    .context("previous schema change generation missing")?;
                ensure!(
                    generation.phase == GenerationPhase::Replayed
                        && old.commit_lsn < candidate.commit_lsn,
                    "previous schema change is incomplete or WAL order regressed"
                );
            }
            if self.state.compare_exchange_metadata(
                &key,
                previous.as_deref(),
                &serde_json::to_vec(&candidate)?,
            )? {
                break;
            }
        }
        loop {
            let previous = self.state.get_metadata("schema-change-relations")?;
            let mut relations: Vec<u32> = previous
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()?
                .unwrap_or_default();
            if relations.contains(&candidate.old.relation_oid) {
                return Ok(candidate);
            }
            relations.push(candidate.old.relation_oid);
            relations.sort_unstable();
            if self.state.compare_exchange_metadata(
                "schema-change-relations",
                previous.as_deref(),
                &serde_json::to_vec(&relations)?,
            )? {
                return Ok(candidate);
            }
        }
    }

    async fn run_schema_change(&self, record: &SchemaChangeRecord) -> Result<()> {
        self.ensure_no_open_snapshot(record.old.relation_oid)?;
        ensure!(
            self.state
                .pending_for_relation(record.old.relation_oid)?
                .iter()
                .all(|row| row.phase == PendingPhase::Retired),
            "schema barrier has unresolved pending visibility rows"
        );
        for id in self.state.pending_ids()? {
            let Some(batch) = self.state.get_batch(&id)? else {
                continue;
            };
            let payload: Payload = serde_json::from_slice(&batch.payload)?;
            ensure!(
                payload.schema.relation_oid != record.old.relation_oid,
                "schema barrier has an unapplied batch for this relation"
            );
        }
        let generation = self.state.prepare_generation(
            &record.operation_id,
            record.new.relation_oid,
            record.commit_lsn,
            &schema_fingerprint(&record.new)?,
        )?;
        let binding = GenerationBinding {
            operation_id: record.operation_id.clone(),
            relation_oid: record.new.relation_oid,
            schema_fingerprint: schema_fingerprint(&record.new)?,
        };
        self.state.put_metadata(
            &generation_index_key(generation.generation_id),
            &serde_json::to_vec(&binding)?,
        )?;
        let old_plan = if record.old_active == 0 {
            TableSqlPlan::new(&record.old, &self.config.internal_schema)
                .map_err(anyhow::Error::msg)?
        } else {
            self.generation_plan(&record.old, record.old_active).await?
        };
        if self
            .state
            .get_metadata(&generation_floor_key(generation.generation_id))?
            .is_none()
        {
            let prior_floor = self.floor_before_schema_change(record.old_active)?;
            self.state.put_metadata(
                &generation_floor_key(generation.generation_id),
                &serde_json::to_vec(&prior_floor)?,
            )?;
        }
        let plan = GenerationSqlPlan::new(
            &record.new,
            &self.config.internal_schema,
            generation.generation_id,
            &self.source_identity,
        )
        .map_err(anyhow::Error::msg)?;
        match generation.phase {
            GenerationPhase::Prepared => {
                self.prepare_schema_storage(record, &plan, &old_plan)
                    .await?;
                self.state.mark_generation_loaded(&record.operation_id)?;
            }
            GenerationPhase::Loaded | GenerationPhase::Published | GenerationPhase::Replayed => {}
            GenerationPhase::Aborted | GenerationPhase::Retired => {
                bail!("schema generation cannot resume in this phase")
            }
        }
        if self
            .state
            .generation(&record.operation_id)?
            .context("schema journal disappeared")?
            .phase
            == GenerationPhase::Loaded
        {
            self.switch_schema_view(record, &plan).await?;
            self.advance_schema_active(record, generation.generation_id)?;
            self.state.mark_generation_published(&record.operation_id)?;
        }
        ensure!(
            self.storage_exists(&plan).await?
                && self.view_comment(&record.new, &plan).await?.as_deref()
                    == Some(plan.view_marker.as_str()),
            "published schema generation is no longer visible"
        );
        ensure!(
            self.active_generation(&record.new).await? == generation.generation_id,
            "schema generation active pointer is missing"
        );
        self.finish_schema_metadata(record, generation.generation_id)
            .await?;
        self.state.mark_generation_replayed(&record.operation_id)?;
        Ok(())
    }

    async fn prepare_schema_storage(
        &self,
        record: &SchemaChangeRecord,
        plan: &GenerationSqlPlan,
        old_plan: &TableSqlPlan,
    ) -> Result<()> {
        let new_plan = TableSqlPlan::new(&record.new, &self.config.internal_schema)
            .map_err(anyhow::Error::msg)?;
        for sql in [
            plan.create_storage_sql.clone(),
            new_plan.create_landing_sql.clone(),
            new_plan.create_receipts_sql.clone(),
            new_plan
                .create_streaming_pipe_sql(&record.new, &pipe_name(&record.new))
                .map_err(anyhow::Error::msg)?,
        ] {
            self.http.execute_sql(&sql, Uuid::new_v4()).await?;
        }
        let sql = record.projection.merge_sql(
            &old_plan.state_table,
            &plan.storage_table,
            plan.generation_id,
        );
        self.http
            .execute_sql(&sql, request_id(&record.operation_id, "copy-schema"))
            .await?;
        let old_count = self.count_rows(&old_plan.state_table).await?;
        let new_count = self.count_rows(&plan.storage_table).await?;
        ensure!(
            old_count == new_count,
            "schema generation copy is incomplete"
        );
        Ok(())
    }

    async fn count_rows(&self, table: &str) -> Result<u64> {
        let result = self
            .http
            .execute_sql(
                &format!("SELECT COUNT(*) AS C FROM {table}"),
                Uuid::new_v4(),
            )
            .await?;
        ensure!(
            result.rows.len() == 1,
            "schema count query returned unexpected rows"
        );
        let index = result_column(&result.row_type, "C")?;
        let value = result.rows[0]
            .get(index)
            .context("schema count column missing")?;
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .context("invalid schema count")
    }

    async fn switch_schema_view(
        &self,
        record: &SchemaChangeRecord,
        plan: &GenerationSqlPlan,
    ) -> Result<()> {
        ensure!(
            self.storage_exists(plan).await?,
            "schema generation storage missing"
        );
        let new_comment = self.view_comment(&record.new, plan).await?;
        if new_comment.as_deref() == Some(plan.view_marker.as_str()) {
            return Ok(());
        }
        let old_marker = if record.old_active == 0 {
            format!(
                "walshadow:source:{}:table:{}",
                self.source_identity,
                table_key(&record.old)
            )
        } else {
            GenerationSqlPlan::new(
                &record.old,
                &self.config.internal_schema,
                record.old_active,
                &self.source_identity,
            )
            .map_err(anyhow::Error::msg)?
            .view_marker
        };
        if record.old.table != record.new.table {
            let old_comment = self.view_comment(&record.old, plan).await?;
            match (old_comment.as_deref(), new_comment.as_deref()) {
                (Some(marker), None) if marker == old_marker => {
                    let old_name = TableSqlPlan::new(&record.old, &self.config.internal_schema)
                        .map_err(anyhow::Error::msg)?
                        .public_view;
                    let new_name = TableSqlPlan::new(&record.new, &self.config.internal_schema)
                        .map_err(anyhow::Error::msg)?
                        .public_view;
                    let rename = format!("ALTER VIEW {old_name} RENAME TO {new_name}");
                    if let Err(error) = self
                        .http
                        .execute_sql(&rename, request_id(&record.operation_id, "rename-view"))
                        .await
                    {
                        ensure!(
                            self.view_comment(&record.new, plan).await?.as_deref()
                                == Some(old_marker.as_str()),
                            "view rename failed or was ambiguous: {error}"
                        );
                    }
                }
                (None, Some(marker)) if marker == old_marker => {}
                _ => bail!("schema rename view ownership is ambiguous"),
            }
        } else {
            ensure!(
                new_comment.as_deref() == Some(old_marker.as_str()),
                "schema view owner or prior generation changed"
            );
        }
        if let Err(error) = self
            .http
            .execute_sql(
                &plan.view_switch_sql,
                request_id(&record.operation_id, "switch-schema-view"),
            )
            .await
        {
            ensure!(
                self.view_comment(&record.new, plan).await?.as_deref()
                    == Some(plan.view_marker.as_str()),
                "schema view replacement failed or was ambiguous: {error}"
            );
        }
        ensure!(
            self.view_comment(&record.new, plan).await?.as_deref()
                == Some(plan.view_marker.as_str()),
            "schema view marker missing after replacement"
        );
        Ok(())
    }

    fn advance_schema_active(&self, record: &SchemaChangeRecord, next: u64) -> Result<()> {
        let key = active_key(&record.new);
        let expected = if record.old.table == record.new.table && record.old_active != 0 {
            Some(record.old_active.to_le_bytes())
        } else {
            None
        };
        if !self.state.compare_exchange_metadata(
            &key,
            expected.as_ref().map(|v| v.as_slice()),
            &next.to_le_bytes(),
        )? {
            ensure!(
                self.state.get_metadata(&key)?.as_deref() == Some(next.to_le_bytes().as_slice()),
                "active generation changed during schema publication"
            );
        }
        Ok(())
    }

    async fn finish_schema_metadata(&self, record: &SchemaChangeRecord, next: u64) -> Result<()> {
        let old_key = format!("table-schema/{}", table_key(&record.old));
        let new_key = format!("table-schema/{}", table_key(&record.new));
        let old_bytes = serde_json::to_vec(&record.old)?;
        let new_bytes = serde_json::to_vec(&record.new)?;
        if old_key == new_key {
            if !self
                .state
                .compare_exchange_metadata(&old_key, Some(&old_bytes), &new_bytes)?
            {
                ensure!(
                    self.state.get_metadata(&old_key)?.as_deref() == Some(new_bytes.as_slice()),
                    "schema metadata changed during publication"
                );
            }
        } else {
            self.state.put_metadata(&new_key, &new_bytes)?;
            self.state.put_metadata(
                &format!("schema-retired/{}", table_key(&record.old)),
                &next.to_le_bytes(),
            )?;
        }
        // `ready` is a cache. A fresh ensure_schema creates the versioned
        // landing/pipe resources idempotently and verifies the view marker.
        self.ready.lock().await.remove(&table_key(&record.old));
        Ok(())
    }

    fn floor_before_schema_change(&self, old_active: u64) -> Result<GenerationFloor> {
        if old_active == 0 {
            return Ok(GenerationFloor {
                min_commit_lsn: None,
                min_record_lsn: None,
            });
        }
        if let Some(bytes) = self.state.get_metadata(&generation_floor_key(old_active))? {
            return Ok(serde_json::from_slice(&bytes)?);
        }
        let binding: GenerationBinding = serde_json::from_slice(
            &self
                .state
                .get_metadata(&generation_index_key(old_active))?
                .context("previous active generation index missing")?,
        )?;
        let record = self
            .state
            .generation(&binding.operation_id)?
            .context("previous active generation missing")?;
        if self
            .state
            .get_metadata(&format!("truncate-generation/{}", binding.operation_id))?
            .is_some()
        {
            Ok(GenerationFloor {
                min_commit_lsn: None,
                min_record_lsn: Some(record.snapshot_lsn),
            })
        } else {
            Ok(GenerationFloor {
                min_commit_lsn: Some(record.snapshot_lsn),
                min_record_lsn: None,
            })
        }
    }

    fn ensure_no_open_snapshot(&self, relation_oid: u32) -> Result<()> {
        if let Some(route) = self
            .state
            .get_metadata(&format!("snapshot-route/{relation_oid}"))?
        {
            let operation_id = std::str::from_utf8(&route)?;
            let snapshot = self
                .state
                .generation(operation_id)?
                .context("snapshot route journal missing")?;
            ensure!(
                matches!(
                    snapshot.phase,
                    GenerationPhase::Replayed | GenerationPhase::Retired | GenerationPhase::Aborted
                ),
                "schema change cannot overtake an incomplete snapshot publication"
            );
        }
        Ok(())
    }
}

fn schema_change_key(oid: u32) -> String {
    format!("schema-change/{oid}")
}
fn drop_key(oid: u32) -> String {
    format!("drop-relation/{oid}")
}
fn drop_complete_key(operation_id: &str) -> String {
    format!("drop-complete/{operation_id}")
}

impl SchemaProjection {
    fn new(
        old: &TableSchema,
        new: &TableSchema,
        desc: &RelDescriptor,
        diff: &SchemaDiff,
    ) -> Result<Self> {
        ensure!(
            old.relation_oid == new.relation_oid && old.database == new.database,
            "schema change cannot move a relation across Snowflake schemas"
        );
        ensure!(
            diff.type_changes.is_empty(),
            "Snowflake type/nullability change requires a rebuild"
        );
        let old_keys: Vec<_> = old
            .key_indexes
            .iter()
            .map(|&i| old.columns[i].attnum)
            .collect();
        let new_keys: Vec<_> = new
            .key_indexes
            .iter()
            .map(|&i| new.columns[i].attnum)
            .collect();
        ensure!(old_keys == new_keys, "Snowflake replica identity changed");
        let old_by_attnum: StdHashMap<_, _> = old.columns.iter().map(|c| (c.attnum, c)).collect();
        let new_attrs: StdHashMap<_, _> = desc
            .attributes
            .iter()
            .filter(|a| !a.dropped)
            .map(|a| (a.attnum, a))
            .collect();
        let mut expressions = Vec::with_capacity(new.columns.len());
        let mut columns = Vec::with_capacity(new.columns.len());
        for column in &new.columns {
            let name = quote_ident(&column.name).map_err(anyhow::Error::msg)?;
            let expr = if let Some(previous) = old_by_attnum.get(&column.attnum) {
                ensure!(
                    previous.type_oid == column.type_oid
                        && previous.data_type == column.data_type
                        && previous.not_null == column.not_null,
                    "existing Snowflake column changed type or nullability"
                );
                ensure!(
                    !old_keys.contains(&column.attnum) || previous.name == column.name,
                    "replica identity column rename needs a separate barrier"
                );
                quote_ident(&previous.name).map_err(anyhow::Error::msg)?
            } else {
                let attr = new_attrs
                    .get(&column.attnum)
                    .context("added column descriptor missing")?;
                ensure!(!attr.not_null, "new Snowflake column must be nullable");
                default_expression(attr, column)?
            };
            expressions.push(format!("{expr} AS {name}"));
            columns.push(name);
        }
        let kept: StdHashSet<_> = new.columns.iter().map(|c| c.attnum).collect();
        ensure!(
            old_keys.iter().all(|n| kept.contains(n)),
            "replica identity column was dropped"
        );
        Ok(Self {
            expressions,
            columns,
        })
    }

    fn merge_sql(&self, source_table: &str, target_table: &str, generation_id: u64) -> String {
        const META: [&str; 8] = [
            "_WS_KEY",
            "_WS_SOURCE_IDENTITY",
            "_WS_RELATION_INCARNATION",
            "_WS_LOAD_GENERATION",
            "_WS_COMMIT_LSN",
            "_WS_RECORD_LSN",
            "_WS_ROW_ORDINAL",
            "_WS_EVENT_ID",
        ];
        let mut columns = self.columns.clone();
        columns.extend(META.iter().map(|s| (*s).to_owned()));
        columns.push("_WS_DELETED".into());
        let mut projected = self.expressions.clone();
        projected.extend(META.iter().map(|s| {
            if *s == "_WS_LOAD_GENERATION" {
                format!("{generation_id} AS _WS_LOAD_GENERATION")
            } else {
                (*s).to_owned()
            }
        }));
        projected.push("_WS_DELETED".into());
        let update = columns
            .iter()
            .map(|c| format!("t.{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let values = columns
            .iter()
            .map(|c| format!("s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "MERGE INTO {target_table} t USING (SELECT {} FROM {source_table}) s ON t._WS_KEY = s._WS_KEY WHEN MATCHED THEN UPDATE SET {update} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({values})",
            projected.join(", "),
            columns.join(", ")
        )
    }
}

fn default_expression(
    attr: &RelAttr,
    column: &crate::destination::snowflake::types::SnowflakeColumn,
) -> Result<String> {
    if attr.missing_default.is_none() {
        return Ok("NULL".into());
    }
    let value = convert(&missing_value_for(attr), column).map_err(anyhow::Error::msg)?;
    let text = match value {
        SnowflakeValue::Null => return Ok("NULL".into()),
        SnowflakeValue::Boolean(v) => v.to_string(),
        SnowflakeValue::Number(v) => v,
        SnowflakeValue::Float(v) if v.is_finite() => v.to_string(),
        SnowflakeValue::Float(_) => bail!("nonfinite fast default is unsupported"),
        SnowflakeValue::Binary(v) => {
            return Ok(format!(
                "TO_BINARY({}, 'HEX')",
                quote_literal(&hex::encode(v))
            ));
        }
        SnowflakeValue::Text(v)
        | SnowflakeValue::Date(v)
        | SnowflakeValue::Time(v)
        | SnowflakeValue::TimestampNtz(v)
        | SnowflakeValue::TimestampTz(v) => v,
    };
    Ok(format!(
        "CAST({} AS {})",
        quote_literal(&text),
        column.data_type.sql()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::snowflake::state::{PendingRow, StateIdentity};
    use crate::destination::snowflake::types::{SnowflakeColumn, SnowflakeType};
    use crate::schema::{MissingDefault, RelName, ReplIdent};
    use serde_json::json;

    fn schema(columns: Vec<(i16, &str)>) -> TableSchema {
        TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            relation_oid: 7,
            key_indexes: vec![0],
            columns: columns
                .into_iter()
                .map(|(attnum, name)| SnowflakeColumn {
                    attnum,
                    source_name: name.into(),
                    name: name.into(),
                    type_oid: 23,
                    data_type: SnowflakeType::Number {
                        precision: 10,
                        scale: 0,
                    },
                    not_null: attnum == 1,
                })
                .collect(),
        }
    }
    fn desc() -> RelDescriptor {
        RelDescriptor {
            rfn: Default::default(),
            oid: 7,
            toast_oid: 0,
            namespace_oid: 1,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default {
                pk_attnums: Some(vec![1]),
            },
            attributes: vec![],
        }
    }
    #[test]
    fn projection_renames_and_adds_nullable_default_without_ch_dialect() {
        let old = schema(vec![(1, "id"), (2, "old")]);
        let mut new = schema(vec![(1, "id"), (2, "renamed"), (3, "added")]);
        new.columns[1].source_name = "renamed".into();
        let mut desc = desc();
        desc.attributes.push(RelAttr {
            attnum: 3,
            name: "added".into(),
            type_oid: 23,
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "int4".into(),
            type_byval: true,
            type_len: 4,
            type_align: 'i',
            type_storage: 'p',
            missing_default: Some(MissingDefault::Text("7".into())),
        });
        let projection = SchemaProjection::new(&old, &new, &desc, &SchemaDiff::default()).unwrap();
        let sql = projection.merge_sql("OLD", "NEW", 4);
        assert!(sql.contains("\"old\" AS \"renamed\""));
        assert!(sql.contains("CAST('7' AS NUMBER(10,0)) AS \"added\""));
        assert!(sql.contains("4 AS _WS_LOAD_GENERATION"));
    }

    #[test]
    fn projection_rejects_key_and_existing_type_changes() {
        let old = schema(vec![(1, "id"), (2, "value")]);
        let mut key_rename = schema(vec![(1, "renamed_id"), (2, "value")]);
        assert!(SchemaProjection::new(&old, &key_rename, &desc(), &SchemaDiff::default()).is_err());
        key_rename.columns[0].name = "id".into();
        key_rename.columns[1].data_type = SnowflakeType::Float;
        assert!(SchemaProjection::new(&old, &key_rename, &desc(), &SchemaDiff::default()).is_err());
        let mut dropped_key = schema(vec![(2, "value")]);
        dropped_key.key_indexes = vec![0];
        assert!(
            SchemaProjection::new(&old, &dropped_key, &desc(), &SchemaDiff::default()).is_err()
        );
    }

    #[tokio::test]
    async fn unresolved_pending_visibility_blocks_schema_generation_before_remote_effects() {
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
        state
            .enqueue_pending(&PendingRow {
                id: "pending_1".into(),
                relation_oid: 7,
                xmin: 10,
                xmax: 0,
                record_lsn: 20,
                payload: vec![1, 2, 3],
            })
            .unwrap();
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
        };
        let old = schema(vec![(1, "id")]);
        let record = SchemaChangeRecord {
            operation_id: "schema_pending".into(),
            commit_lsn: 100,
            old: old.clone(),
            new: old.clone(),
            old_active: 0,
            projection: SchemaProjection::new(&old, &old, &desc(), &SchemaDiff::default()).unwrap(),
        };
        assert!(runtime.run_schema_change(&record).await.is_err());
        assert!(
            runtime
                .state
                .generation(&record.operation_id)
                .unwrap()
                .is_none()
        );
        runtime
            .state
            .prepare_generation("snapshot_open", 7, 50, "schema")
            .unwrap();
        runtime
            .state
            .put_metadata("snapshot-route/7", b"snapshot_open")
            .unwrap();
        assert!(runtime.ensure_no_open_snapshot(7).is_err());
    }

    #[tokio::test]
    async fn drop_deletes_only_an_exactly_owned_view_and_journals_completion() {
        let dir = tempfile::tempdir().unwrap();
        let config: SnowflakeConfig = serde_json::from_value(json!({
            "account_url":"https://test.snowflakecomputing.com", "user":"USER", "role":"ROLE", "warehouse":"WH", "database":"DB",
            "auth":{"method":"oauth","token_file":"unused"},
            "state":{"directory":dir.path(),"max_bytes":1_000_000},
            "stage":{"bucket":"test","region":"us-east-1","prefix":"test/","name":"DB.INTERNAL.STAGE"}
        })).unwrap();
        let schema = schema(vec![(1, "id")]);
        let marker = format!("walshadow:source:1:table:{}", table_key(&schema));
        let reply = |rows: serde_json::Value| -> &'static str {
            Box::leak(json!({"statementHandle":"00000000-0000-0000-0000-000000000001",
                "resultSetMetaData":{"rowType":[{"name":"NAME"},{"name":"COMMENT"}],"partitionInfo":[{}]},
                "data":rows}).to_string().into_boxed_str())
        };
        let (url, requests) = crate::destination::snowflake::http::tests::server(vec![
            (200, reply(json!([["T", marker]]))),
            (200, reply(json!([]))),
            (200, reply(json!([]))),
        ])
        .await;
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
        state
            .put_metadata(
                &format!("table-schema/{}", table_key(&schema)),
                &serde_json::to_vec(&schema).unwrap(),
            )
            .unwrap();
        let runtime = SnowflakeRuntime {
            http: crate::destination::snowflake::http::tests::client(url).await,
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
        };
        runtime
            .drop_relation_at(7, &RelName::new("PUBLIC", "T"), 100)
            .await
            .unwrap();
        assert!(
            runtime
                .state
                .get_metadata(&drop_complete_key(&format!(
                    "drop_{}",
                    &hex::encode(Sha256::digest("drop/1/7/100"))[..40]
                )))
                .unwrap()
                .is_some()
        );
        assert!(
            runtime
                .state
                .get_metadata(&format!("schema-retired/{}", table_key(&schema)))
                .unwrap()
                .is_some()
        );
        let mut recreated = schema.clone();
        recreated.relation_oid = 8;
        assert_ne!(table_key(&recreated), table_key(&schema));
        assert!(
            runtime
                .state
                .get_metadata(&format!("schema-retired/{}", table_key(&recreated)))
                .unwrap()
                .is_none()
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[1].contains("DROP VIEW IF EXISTS"));
    }
}
