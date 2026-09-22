//! SQL for a journaled, immutable snapshot generation.
//!
//! Callers fence live writes and verify remote receipts around these
//! statements. SQL construction alone does not publish a generation.

use sha2::{Digest, Sha256};

use super::sql::{TableSqlPlan, quote_ident, quote_literal};
use super::types::TableSchema;

#[derive(Debug, Clone)]
pub struct GenerationSqlPlan {
    pub generation_id: u64,
    pub storage_table: String,
    pub create_storage_sql: String,
    pub view_switch_sql: String,
    pub show_view_sql: String,
    pub show_storage_sql: String,
    pub view_marker: String,
    pub retire_storage_sql: String,
    base: TableSqlPlan,
    source_hash: String,
}

pub enum ReplaySource<'a> {
    Base,
    Generation(&'a GenerationSqlPlan),
}

impl GenerationSqlPlan {
    pub fn new(
        schema: &TableSchema,
        internal_schema: &str,
        generation_id: u64,
        source_identity: &str,
    ) -> Result<Self, String> {
        if generation_id == 0 || source_identity.is_empty() {
            return Err("generation id and source identity are required".into());
        }
        let base = TableSqlPlan::new(schema, internal_schema)?;
        let identity = serde_json::to_vec(&(
            source_identity,
            schema.relation_oid,
            &schema.database,
            &schema.table,
        ))
        .map_err(|e| e.to_string())?;
        let hash = hex::encode(Sha256::digest(identity));
        let physical_name = format!("_WS_{}_G_{generation_id:020}", &hash[..24]);
        let storage_table = format!(
            "{}.{}",
            quote_ident(internal_schema)?,
            quote_ident(&physical_name)?
        );
        let create_storage_sql =
            base.create_state_sql
                .replacen(&base.state_table, &storage_table, 1);
        if create_storage_sql == base.create_state_sql {
            return Err("generation storage replacement failed".into());
        }
        let source_hash = hex::encode(Sha256::digest(source_identity.as_bytes()));
        let source_hash = source_hash[..32].to_owned();
        let view_marker = format!("walshadow:generation:{generation_id}:{source_hash}");
        let view_switch_sql = format!(
            "CREATE OR REPLACE VIEW {} CHANGE_TRACKING = TRUE COPY GRANTS COMMENT = {} AS SELECT {} FROM {storage_table} WHERE _WS_DELETED = FALSE",
            base.public_view,
            quote_literal(&view_marker),
            base.data_columns.join(", ")
        );
        let show_view_sql = format!(
            "SHOW VIEWS LIKE {} IN SCHEMA {}",
            quote_literal(&schema.table),
            quote_ident(&schema.database)?
        );
        let show_storage_sql = format!(
            "SHOW TABLES LIKE {} IN SCHEMA {}",
            quote_literal(&physical_name),
            quote_ident(internal_schema)?
        );
        let retire_storage_sql = format!("DROP TABLE IF EXISTS {storage_table}");
        Ok(Self {
            generation_id,
            storage_table,
            create_storage_sql,
            view_switch_sql,
            show_view_sql,
            show_storage_sql,
            view_marker,
            retire_storage_sql,
            base,
            source_hash,
        })
    }

    /// The staged snapshot batch must have a verified completeness receipt
    /// before this MERGE is sent. The generation table is separate from the
    /// public state table until view publication.
    pub fn snapshot_merge_sql(&self, batch_id: &str) -> String {
        let mut plan = self.base.clone();
        plan.state_table.clone_from(&self.storage_table);
        plan.merge_sql(batch_id)
    }

    /// Replay current live versions newer than the snapshot's source LSN.
    /// Rewrites their load generation so source-version ordering compares
    /// against snapshot rows inside this physical generation.
    pub fn replay_live_sql(
        &self,
        source: ReplaySource<'_>,
        snapshot_lsn: u64,
    ) -> Result<String, String> {
        self.replay_live_sql_by(source, snapshot_lsn, "_WS_COMMIT_LSN")
    }

    /// TRUNCATE splits a transaction at its WAL record. Earlier row records
    /// may share a later commit LSN, so a commit-LSN cutoff would resurrect
    /// them when an empty generation is published.
    pub fn replay_live_after_record_lsn_sql(
        &self,
        source: ReplaySource<'_>,
        truncate_record_lsn: u64,
    ) -> Result<String, String> {
        self.replay_live_sql_by(source, truncate_record_lsn, "_WS_RECORD_LSN")
    }

    fn replay_live_sql_by(
        &self,
        source: ReplaySource<'_>,
        snapshot_lsn: u64,
        cutoff_column: &str,
    ) -> Result<String, String> {
        let source_table = match source {
            ReplaySource::Base => &self.base.state_table,
            ReplaySource::Generation(previous) => {
                if previous.base.public_view != self.base.public_view
                    || previous.source_hash != self.source_hash
                    || previous.generation_id >= self.generation_id
                {
                    return Err("live replay source is not a prior generation of this view".into());
                }
                &previous.storage_table
            }
        };
        let mut columns = self.base.data_columns.clone();
        columns.extend(
            [
                "_WS_KEY",
                "_WS_SOURCE_IDENTITY",
                "_WS_RELATION_INCARNATION",
                "_WS_LOAD_GENERATION",
                "_WS_COMMIT_LSN",
                "_WS_RECORD_LSN",
                "_WS_ROW_ORDINAL",
                "_WS_EVENT_ID",
                "_WS_DELETED",
            ]
            .map(str::to_string),
        );
        let projected = columns
            .iter()
            .map(|c| {
                if c == "_WS_LOAD_GENERATION" {
                    format!("{} AS _WS_LOAD_GENERATION", self.generation_id)
                } else {
                    c.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let updates = columns
            .iter()
            .map(|c| format!("t.{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_values = columns
            .iter()
            .map(|c| format!("s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "MERGE INTO {} t USING (SELECT {projected} FROM {} WHERE {cutoff_column} > {snapshot_lsn} QUALIFY ROW_NUMBER() OVER (PARTITION BY _WS_KEY ORDER BY _WS_COMMIT_LSN DESC, _WS_RECORD_LSN DESC, _WS_ROW_ORDINAL DESC, _WS_EVENT_ID DESC) = 1) s ON t._WS_KEY = s._WS_KEY WHEN MATCHED AND (s._WS_COMMIT_LSN > t._WS_COMMIT_LSN OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN > t._WS_RECORD_LSN) OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN = t._WS_RECORD_LSN AND s._WS_ROW_ORDINAL > t._WS_ROW_ORDINAL) OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN = t._WS_RECORD_LSN AND s._WS_ROW_ORDINAL = t._WS_ROW_ORDINAL AND s._WS_EVENT_ID > t._WS_EVENT_ID)) THEN UPDATE SET {updates} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({insert_values})",
            self.storage_table,
            source_table,
            columns.join(", ")
        ))
    }

    /// `SHOW VIEWS` exposes the comment; the caller must also confirm the
    /// exact generation table exists via `show_storage_sql` before treating an
    /// ambiguous view replacement as published.
    pub fn matches_published_marker(&self, comment: &str, storage_exists: bool) -> bool {
        storage_exists && comment == self.view_marker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::snowflake::types::{SnowflakeColumn, SnowflakeType};

    fn schema() -> TableSchema {
        TableSchema {
            database: "APP".into(),
            table: "ORDERS".into(),
            relation_oid: 17,
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
        }
    }

    #[test]
    fn generation_names_and_marker_are_stable_and_source_bound() {
        let a = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 42, "source-a").unwrap();
        let same = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 42, "source-a").unwrap();
        let other = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 43, "source-a").unwrap();
        let foreign = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 42, "source-b").unwrap();
        assert_eq!(a.storage_table, same.storage_table);
        assert_ne!(a.storage_table, other.storage_table);
        assert_ne!(a.storage_table, foreign.storage_table);
        assert!(a.create_storage_sql.contains(&a.storage_table));
        assert!(a.view_switch_sql.contains(&a.storage_table));
        assert!(a.view_switch_sql.contains("COPY GRANTS COMMENT ="));
        assert!(a.view_switch_sql.contains("CHANGE_TRACKING = TRUE"));
        assert!(a.matches_published_marker(&a.view_marker, true));
        assert!(!a.matches_published_marker(&a.view_marker, false));
        assert!(!a.matches_published_marker(&other.view_marker, true));
        assert!(GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 0, "source-a").is_err());
    }

    #[test]
    fn replay_rewrites_generation_and_filters_at_snapshot_lsn() {
        let plan = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 42, "source-a").unwrap();
        let sql = plan.replay_live_sql(ReplaySource::Base, 500).unwrap();
        assert!(sql.contains("42 AS _WS_LOAD_GENERATION"));
        assert!(sql.contains("WHERE _WS_COMMIT_LSN > 500"));
        let truncate_sql = plan
            .replay_live_after_record_lsn_sql(ReplaySource::Base, 501)
            .unwrap();
        assert!(truncate_sql.contains("WHERE _WS_RECORD_LSN > 501"));
        assert!(!truncate_sql.contains("WHERE _WS_COMMIT_LSN > 501"));
        assert!(sql.contains(&format!("MERGE INTO {}", plan.storage_table)));
        assert!(
            plan.snapshot_merge_sql("batch-a")
                .contains(&plan.storage_table)
        );
        assert!(plan.retire_storage_sql.contains(&plan.storage_table));
        let newer = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 43, "source-a").unwrap();
        assert!(
            newer
                .replay_live_sql(ReplaySource::Generation(&plan), 500)
                .unwrap()
                .contains(&format!("FROM {}", plan.storage_table))
        );
        assert!(
            plan.replay_live_sql(ReplaySource::Generation(&newer), 500)
                .is_err()
        );
        let foreign = GenerationSqlPlan::new(&schema(), "_WS_INTERNAL", 41, "source-b").unwrap();
        assert!(
            newer
                .replay_live_sql(ReplaySource::Generation(&foreign), 500)
                .is_err()
        );
    }
}
