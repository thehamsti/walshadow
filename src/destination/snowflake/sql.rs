//! Identifier-safe Snowflake DDL and version-aware materialization statements.
use super::types::{SnowflakeType, TableSchema};

pub fn quote_ident(s: &str) -> Result<String, String> {
    if s.is_empty() || s.contains('\0') {
        return Err("empty or NUL Snowflake identifier".into());
    }
    Ok(format!("\"{}\"", s.replace('"', "\"\"")))
}
/// Quote a configured database.schema.object name one component at a time.
/// Configuration uses three unquoted components; embedded dots are rejected.
pub fn quote_qualified_ident(name: &str) -> Result<String, String> {
    let parts: Vec<_> = name.split('.').collect();
    if parts.len() != 3 {
        return Err(format!("expected database.schema.object, got {name:?}"));
    }
    parts
        .into_iter()
        .map(quote_ident)
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}
pub fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace("'", "''"))
}
fn qualified(db: &str, name: &str) -> Result<String, String> {
    Ok(format!("{}.{}", quote_ident(db)?, quote_ident(name)?))
}
#[derive(Debug, Clone)]
pub struct TableSqlPlan {
    pub min_commit_lsn: Option<u64>,
    pub min_record_lsn: Option<u64>,
    pub internal_schema: String,
    pub landing_table: String,
    pub state_table: String,
    pub public_view: String,
    pub receipts_table: String,
    pub create_landing_sql: String,
    pub create_state_sql: String,
    pub create_receipts_sql: String,
    pub view_sql: String,
    pub data_columns: Vec<String>,
    pub key_columns: Vec<String>,
}
impl TableSqlPlan {
    pub fn new(schema: &TableSchema, internal_schema: &str) -> Result<Self, String> {
        use sha2::{Digest, Sha256};
        let identity = serde_json::to_vec(&(schema.relation_oid, &schema.database, &schema.table))
            .map_err(|e| e.to_string())?;
        let base = format!("_WS_{}", &hex::encode(Sha256::digest(identity))[..24]);
        let shape = serde_json::to_vec(schema).map_err(|e| e.to_string())?;
        let landing_name = format!("_WS_{}_LANDING", &hex::encode(Sha256::digest(shape))[..24]);
        let landing_table = qualified(internal_schema, &landing_name)?;
        let state_table = qualified(internal_schema, &format!("{base}_STATE"))?;
        let receipts_table = qualified(internal_schema, "_WS_APPLY_RECEIPTS")?;
        let public_view = qualified(&schema.database, &schema.table)?;
        let data_columns: Vec<_> = schema
            .columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Result<_, _>>()?;
        let key_columns = schema
            .key_indexes
            .iter()
            .map(|i| data_columns[*i].clone())
            .collect();
        let defs = schema
            .columns
            .iter()
            .zip(data_columns.iter())
            .map(|(c, n)| format!("{n} {}", c.data_type.sql()))
            .collect::<Vec<_>>()
            .join(", ");
        let meta = "_WS_KEY VARCHAR NOT NULL, _WS_SOURCE_IDENTITY VARCHAR NOT NULL, _WS_RELATION_INCARNATION NUMBER(20,0) NOT NULL, _WS_LOAD_GENERATION NUMBER(20,0) NOT NULL, _WS_COMMIT_LSN NUMBER(20,0) NOT NULL, _WS_RECORD_LSN NUMBER(20,0) NOT NULL, _WS_ROW_ORDINAL NUMBER(10,0) NOT NULL, _WS_EVENT_ID VARCHAR NOT NULL, _WS_DELETED BOOLEAN NOT NULL";
        let create_landing_sql = format!(
            "CREATE TABLE IF NOT EXISTS {landing_table} ({defs}, {meta}, _WS_BATCH_ID VARCHAR NOT NULL)"
        );
        let create_state_sql = format!("CREATE TABLE IF NOT EXISTS {state_table} ({defs}, {meta})");
        let create_receipts_sql = format!(
            "CREATE TABLE IF NOT EXISTS {receipts_table} (BATCH_ID VARCHAR NOT NULL, EXPECTED_COUNT NUMBER(20,0) NOT NULL, APPLIED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP())"
        );
        let view_sql = format!(
            "CREATE OR REPLACE VIEW {public_view} CHANGE_TRACKING = TRUE COPY GRANTS AS SELECT {} FROM {state_table} WHERE _WS_DELETED = FALSE",
            data_columns.join(", ")
        );
        Ok(Self {
            min_commit_lsn: None,
            min_record_lsn: None,
            internal_schema: internal_schema.into(),
            landing_table,
            state_table,
            public_view,
            receipts_table,
            create_landing_sql,
            create_state_sql,
            create_receipts_sql,
            view_sql,
            data_columns,
            key_columns,
        })
    }
    /// Run only after `batch_completeness_sql` verifies the expected distinct events.
    /// Caller executes MERGE and `insert_receipt_sql` in one Snowflake transaction.
    pub fn merge_sql(&self, batch_id: &str) -> String {
        self.merge_sql_where(&format!("_WS_BATCH_ID = {}", quote_literal(batch_id)))
    }

    pub fn merge_sql_group(&self, batch_ids: &[String]) -> Result<String, String> {
        if batch_ids.is_empty() {
            return Err("MERGE group is empty".into());
        }
        let ids = batch_ids
            .iter()
            .map(|id| quote_literal(id))
            .collect::<Vec<_>>();
        Ok(self.merge_sql_where(&format!("_WS_BATCH_ID IN ({})", ids.join(", "))))
    }

    fn merge_sql_where(&self, batch_predicate: &str) -> String {
        let mut cols = self.data_columns.clone();
        cols.extend(
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
        let update = cols
            .iter()
            .map(|c| format!("t.{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_values = cols
            .iter()
            .map(|c| format!("s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let floor = self
            .min_commit_lsn
            .map(|lsn| format!(" AND _WS_COMMIT_LSN > {lsn}"))
            .unwrap_or_default()
            + &self
                .min_record_lsn
                .map(|lsn| format!(" AND _WS_RECORD_LSN > {lsn}"))
                .unwrap_or_default();
        format!(
            "MERGE INTO {} t USING (SELECT {} FROM {} WHERE {batch_predicate}{floor} QUALIFY ROW_NUMBER() OVER (PARTITION BY _WS_KEY ORDER BY _WS_COMMIT_LSN DESC, _WS_RECORD_LSN DESC, _WS_ROW_ORDINAL DESC, _WS_EVENT_ID DESC) = 1) s ON t._WS_KEY = s._WS_KEY WHEN MATCHED AND (s._WS_COMMIT_LSN > t._WS_COMMIT_LSN OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN > t._WS_RECORD_LSN) OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN = t._WS_RECORD_LSN AND s._WS_ROW_ORDINAL > t._WS_ROW_ORDINAL) OR (s._WS_COMMIT_LSN = t._WS_COMMIT_LSN AND s._WS_RECORD_LSN = t._WS_RECORD_LSN AND s._WS_ROW_ORDINAL = t._WS_ROW_ORDINAL AND s._WS_EVENT_ID > t._WS_EVENT_ID)) THEN UPDATE SET {update} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({insert_values})",
            self.state_table,
            cols.join(", "),
            self.landing_table,
            cols.join(", ")
        )
    }
    pub fn batch_completeness_sql(&self, batch_id: &str, expected_count: u64) -> String {
        let batch = quote_literal(batch_id);
        format!(
            "SELECT COUNT(*) AS DISTINCT_EVENTS FROM (SELECT _WS_EVENT_ID, COUNT(*) AS PAYLOAD_VERSIONS FROM (SELECT DISTINCT * FROM {} WHERE _WS_BATCH_ID = {batch}) GROUP BY _WS_EVENT_ID) HAVING COUNT(*) = {expected_count} AND COALESCE(COUNT_IF(PAYLOAD_VERSIONS <> 1), 0) = 0",
            self.landing_table
        )
    }
    pub fn insert_receipt_sql(&self, batch_id: &str, expected_count: u64) -> String {
        format!(
            "INSERT INTO {} (BATCH_ID, EXPECTED_COUNT) VALUES ({}, {expected_count})",
            self.receipts_table,
            quote_literal(batch_id)
        )
    }
    pub fn merge_receipts_sql(&self, batches: &[(String, u64)]) -> Result<String, String> {
        if batches.is_empty() {
            return Err("receipt group is empty".into());
        }
        let values = batches
            .iter()
            .map(|(id, count)| format!("({}, {count})", quote_literal(id)))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "MERGE INTO {} t USING (SELECT COLUMN1 AS BATCH_ID, COLUMN2 AS EXPECTED_COUNT FROM VALUES {values}) s ON t.BATCH_ID = s.BATCH_ID WHEN NOT MATCHED THEN INSERT (BATCH_ID, EXPECTED_COUNT) VALUES (s.BATCH_ID, s.EXPECTED_COUNT)",
            self.receipts_table
        ))
    }
    /// One read-back for a committed group; each id must appear exactly once.
    pub fn receipts_sql_group(&self, batch_ids: &[String]) -> Result<String, String> {
        if batch_ids.is_empty() {
            return Err("receipt group is empty".into());
        }
        let ids = batch_ids
            .iter()
            .map(|id| quote_literal(id))
            .collect::<Vec<_>>();
        Ok(format!(
            "SELECT BATCH_ID, EXPECTED_COUNT FROM {} WHERE BATCH_ID IN ({})",
            self.receipts_table,
            ids.join(", ")
        ))
    }
    pub fn receipt_sql(&self, batch_id: &str) -> String {
        format!(
            "SELECT EXPECTED_COUNT FROM {} WHERE BATCH_ID = {}",
            self.receipts_table,
            quote_literal(batch_id)
        )
    }
    /// Streaming uses CONTINUE semantics implicitly; some accounts reject an
    /// explicit ON_ERROR clause on streaming pipes.
    /// Batch completeness must be verified before any apply receipt is committed.
    pub fn create_streaming_pipe_sql(
        &self,
        schema: &TableSchema,
        pipe: &str,
    ) -> Result<String, String> {
        let pipe = qualified(&self.internal_schema, pipe)?;
        let (columns, exprs) = self.copy_projection(schema);
        Ok(format!(
            "CREATE PIPE IF NOT EXISTS {pipe} AS COPY INTO {} ({}) FROM (SELECT {} FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')))",
            self.landing_table,
            columns.join(", "),
            exprs.join(", ")
        ))
    }
    /// Explicit S3 Parquet COPY. The caller supplies exact immutable staged files.
    pub fn copy_into_sql(
        &self,
        schema: &TableSchema,
        stage: &str,
        files: &[String],
    ) -> Result<String, String> {
        if files.is_empty() {
            return Err("COPY requires at least one file".into());
        }
        let stage = quote_qualified_ident(stage)?;
        let files = files
            .iter()
            .map(|f| {
                if f.is_empty() || f.starts_with('/') || f.contains("..") || f.contains('\0') {
                    Err(format!("unsafe staged file {f:?}"))
                } else {
                    Ok(quote_literal(f))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (columns, exprs) = self.copy_projection(schema);
        Ok(format!(
            "COPY INTO {} ({}) FROM (SELECT {} FROM @{stage}) FILES = ({}) FILE_FORMAT = (TYPE = PARQUET) ON_ERROR = ABORT_STATEMENT",
            self.landing_table,
            columns.join(", "),
            exprs.join(", "),
            files.join(", ")
        ))
    }
    fn copy_projection(&self, schema: &TableSchema) -> (Vec<String>, Vec<String>) {
        let mut columns = self.data_columns.clone();
        let mut exprs = Vec::new();
        for c in &schema.columns {
            let src = format!("GET($1, {})::VARCHAR", quote_literal(&c.name));
            let expression = match c.data_type {
                SnowflakeType::Boolean => format!("{src}::BOOLEAN"),
                SnowflakeType::Number { precision, scale } => {
                    format!("TO_DECIMAL({src}, {precision}, {scale})")
                }
                SnowflakeType::Float => format!("{src}::FLOAT"),
                SnowflakeType::Binary => format!("TO_BINARY({src}, 'HEX')"),
                SnowflakeType::Text => src,
                SnowflakeType::Date => format!("TO_DATE({src}, 'YYYY-MM-DD')"),
                SnowflakeType::Time => format!("TO_TIME({src}, 'HH24:MI:SS.FF6')"),
                SnowflakeType::TimestampNtz => {
                    format!("TO_TIMESTAMP_NTZ({src}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
                }
                SnowflakeType::TimestampTz => {
                    format!("TO_TIMESTAMP_TZ({src}, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')")
                }
            };
            exprs.push(expression);
        }
        for (name, cast) in [
            ("_WS_KEY", "VARCHAR"),
            ("_WS_SOURCE_IDENTITY", "VARCHAR"),
            ("_WS_RELATION_INCARNATION", "NUMBER(20,0)"),
            ("_WS_LOAD_GENERATION", "NUMBER(20,0)"),
            ("_WS_COMMIT_LSN", "NUMBER(20,0)"),
            ("_WS_RECORD_LSN", "NUMBER(20,0)"),
            ("_WS_ROW_ORDINAL", "NUMBER(10,0)"),
            ("_WS_EVENT_ID", "VARCHAR"),
            ("_WS_DELETED", "BOOLEAN"),
            ("_WS_BATCH_ID", "VARCHAR"),
        ] {
            columns.push(name.into());
            exprs.push(format!("GET($1, {})::{cast}", quote_literal(name)));
        }
        (columns, exprs)
    }
}
