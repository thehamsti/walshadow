//! Immutable Parquet objects for explicit Snowflake S3-stage COPY.
use super::types::{SnowflakeRow, SnowflakeType, SnowflakeValue, TableSchema};
use anyhow::{Context, Result, bail, ensure};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use parquet::{arrow::ArrowWriter, basic::ZstdLevel, file::properties::WriterProperties};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::{io::Cursor, sync::Arc};

const HARD_MAX_FILE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StageConfig {
    pub bucket: String,
    /// S3 prefix of the external Snowflake stage, without leading slash.
    pub prefix: String,
    pub region: String,
    /// Fully qualified Snowflake stage identifier, e.g. DB.SCHEMA.WS_STAGE.
    pub stage_name: String,
    pub max_file_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct StagedFile {
    pub key: String,
    pub rows: usize,
    pub sha256: String,
    /// Path relative to the external stage prefix, for COPY FILES.
    pub relative_path: String,
}

pub struct StageWriter {
    client: Client,
    config: StageConfig,
}

impl StageWriter {
    pub async fn new(config: StageConfig) -> Result<Self> {
        validate_config(&config)?;
        let aws = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;
        Ok(Self {
            client: Client::new(&aws),
            config,
        })
    }

    pub async fn stage_batch(
        &self,
        batch_id: &str,
        schema: &TableSchema,
        rows: &[SnowflakeRow],
    ) -> Result<StagedFile> {
        let (file, descriptor) = encode_batch(&self.config, batch_id, schema, rows)?;
        let bytes = ByteStream::from(file.clone());
        let result = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&descriptor.key)
            .if_none_match("*")
            .metadata("sha256", &descriptor.sha256)
            .body(bytes)
            .send()
            .await;
        match result {
            Ok(_) => Ok(descriptor),
            Err(error) => {
                let status = error.raw_response().map(|r| r.status().as_u16());
                if status != Some(412) {
                    return Err(error.into());
                }
                // Existing immutable key must contain the same bytes. A checksum in
                // object metadata alone is insufficient because it can be spoofed.
                let existing = self
                    .client
                    .get_object()
                    .bucket(&self.config.bucket)
                    .key(&descriptor.key)
                    .send()
                    .await?;
                let collected = existing.body.collect().await?;
                let existing = collected.into_bytes();
                ensure!(
                    existing.len() == file.len()
                        && Sha256::digest(&existing) == Sha256::digest(&file),
                    "staged object key collision or corrupt existing object"
                );
                Ok(descriptor)
            }
        }
    }
}

/// Lossless row representation shared by Parquet staging and streaming NDJSON.
/// NUMBER and all 64-bit lineage counters are strings to avoid JSON precision loss.
pub fn to_json(schema: &TableSchema, row: &SnowflakeRow, batch_id: &str) -> Result<Value> {
    ensure!(
        row.values.len() == schema.columns.len(),
        "Snowflake row/schema column count mismatch"
    );
    ensure!(
        !batch_id.is_empty()
            && !row.event_id.is_empty()
            && !row.key.is_empty()
            && !row.source_identity.is_empty(),
        "missing Snowflake row identity"
    );
    let mut object = Map::new();
    for (column, value) in schema.columns.iter().zip(&row.values) {
        ensure!(
            !column.name.is_empty() && !column.name.to_ascii_uppercase().starts_with("_WS_"),
            "invalid or reserved Snowflake column name"
        );
        let encoded = match (column.data_type, value) {
            (_, SnowflakeValue::Null) => Value::Null,
            (SnowflakeType::Boolean, SnowflakeValue::Boolean(value)) => Value::Bool(*value),
            (SnowflakeType::Number { precision, scale }, SnowflakeValue::Number(value)) => {
                ensure!(
                    decimal_fits(value, precision, scale),
                    "invalid or out-of-range Snowflake decimal"
                );
                Value::String(value.clone())
            }
            (SnowflakeType::Float, SnowflakeValue::Float(value)) if value.is_finite() => {
                Value::Number(
                    serde_json::Number::from_f64(*value).context("invalid Snowflake float")?,
                )
            }
            (SnowflakeType::Binary, SnowflakeValue::Binary(value)) => {
                Value::String(hex::encode(value))
            }
            (SnowflakeType::Text, SnowflakeValue::Text(value))
            | (SnowflakeType::Date, SnowflakeValue::Date(value))
            | (SnowflakeType::Time, SnowflakeValue::Time(value))
            | (SnowflakeType::TimestampNtz, SnowflakeValue::TimestampNtz(value))
            | (SnowflakeType::TimestampTz, SnowflakeValue::TimestampTz(value)) => {
                Value::String(value.clone())
            }
            _ => bail!(
                "Snowflake row value does not match column type {}",
                column.name
            ),
        };
        ensure!(
            object.insert(column.name.clone(), encoded).is_none(),
            "duplicate Snowflake column name"
        );
    }
    for (name, value) in [
        ("_WS_KEY", Value::String(row.key.clone())),
        (
            "_WS_SOURCE_IDENTITY",
            Value::String(row.source_identity.clone()),
        ),
        (
            "_WS_RELATION_INCARNATION",
            Value::String(row.relation_incarnation.to_string()),
        ),
        (
            "_WS_LOAD_GENERATION",
            Value::String(row.load_generation.to_string()),
        ),
        ("_WS_COMMIT_LSN", Value::String(row.commit_lsn.to_string())),
        ("_WS_RECORD_LSN", Value::String(row.record_lsn.to_string())),
        (
            "_WS_ROW_ORDINAL",
            Value::String(row.row_ordinal.to_string()),
        ),
        ("_WS_EVENT_ID", Value::String(row.event_id.clone())),
        ("_WS_DELETED", Value::Bool(row.deleted)),
        ("_WS_BATCH_ID", Value::String(batch_id.to_owned())),
    ] {
        object.insert(name.into(), value);
    }
    Ok(Value::Object(object))
}

fn decimal_fits(value: &str, precision: u8, scale: u8) -> bool {
    if precision == 0 || precision > 38 || scale > precision {
        return false;
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or("");
    let fraction = parts.next().unwrap_or("");
    parts.next().is_none()
        && !whole.is_empty()
        && whole.bytes().all(|b| b.is_ascii_digit())
        && fraction.bytes().all(|b| b.is_ascii_digit())
        && fraction.len() <= usize::from(scale)
        && whole.trim_start_matches('0').len() <= usize::from(precision - scale)
}

/// Build bytes separately from S3 so exact artifacts are tested without credentials.
pub fn encode_batch(
    config: &StageConfig,
    batch_id: &str,
    schema: &TableSchema,
    rows: &[SnowflakeRow],
) -> Result<(Vec<u8>, StagedFile)> {
    validate_config(config)?;
    ensure!(!rows.is_empty(), "cannot stage an empty batch");
    ensure!(
        !batch_id.is_empty()
            && batch_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "unsafe batch ID for staged path"
    );
    let max = config.max_file_bytes;
    let names: Vec<String> = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .chain(
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
                "_WS_BATCH_ID",
            ]
            .map(str::to_owned),
        )
        .collect();
    let boolean_columns: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| c.data_type == SnowflakeType::Boolean)
        .chain([
            false, false, false, false, false, false, false, false, true, false,
        ])
        .collect();
    let mut vectors = vec![Vec::<Value>::with_capacity(rows.len()); names.len()];
    let mut uncompressed_size = 0usize;
    for row in rows {
        let object = to_json(schema, row, batch_id)?;
        let object = object.as_object().context("Snowflake row not an object")?;
        for (name, vector) in names.iter().zip(&mut vectors) {
            let value = object.get(name).context("missing staged field")?;
            let length = match value {
                Value::Null => 0,
                Value::String(s) => s.len(),
                Value::Bool(_) => 1,
                Value::Number(n) => n.to_string().len(),
                _ => bail!("unsupported staged JSON field"),
            };
            uncompressed_size = uncompressed_size
                .checked_add(length)
                .context("staged row size overflow")?;
            ensure!(
                uncompressed_size <= max,
                "Snowflake staged batch exceeds configured size limit"
            );
            vector.push(value.clone());
        }
    }
    let fields = names
        .iter()
        .zip(&boolean_columns)
        .map(|(name, boolean)| {
            Field::new(
                name,
                if *boolean {
                    DataType::Boolean
                } else {
                    DataType::Utf8
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    let arrow_schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = vectors
        .into_iter()
        .zip(&boolean_columns)
        .map(|(values, boolean)| {
            if *boolean {
                let values: Result<Vec<Option<bool>>> = values
                    .into_iter()
                    .map(|value| match value {
                        Value::Null => Ok(None),
                        Value::Bool(value) => Ok(Some(value)),
                        _ => bail!("invalid Boolean staged field"),
                    })
                    .collect();
                Ok(Arc::new(BooleanArray::from(values?)) as ArrayRef)
            } else {
                let values: Result<Vec<Option<String>>> = values
                    .into_iter()
                    .map(|value| match value {
                        Value::Null => Ok(None),
                        Value::String(value) => Ok(Some(value)),
                        Value::Number(value) => Ok(Some(value.to_string())),
                        _ => bail!("invalid string staged field"),
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(values?)) as ArrayRef)
            }
        })
        .collect::<Result<_>>()?;
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;
    let properties = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut cursor, arrow_schema, Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let file = cursor.into_inner();
    ensure!(
        file.len() <= max,
        "Snowflake Parquet file exceeds configured size limit"
    );
    let sha256 = hex::encode(Sha256::digest(&file));
    let relative_path = format!("{}/{}-{}.parquet", schema.relation_oid, batch_id, sha256);
    let key = if config.prefix.is_empty() {
        relative_path.clone()
    } else {
        format!("{}/{}", config.prefix.trim_end_matches('/'), relative_path)
    };
    Ok((
        file,
        StagedFile {
            key,
            rows: rows.len(),
            sha256,
            relative_path,
        },
    ))
}

fn validate_config(config: &StageConfig) -> Result<()> {
    ensure!(
        !config.bucket.is_empty()
            && config
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-'),
        "invalid S3 bucket"
    );
    ensure!(
        !config.region.is_empty()
            && config
                .region
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid AWS region"
    );
    ensure!(config.stage_name.split('.').count() == 3 && config.stage_name.split('.').all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')), "invalid qualified Snowflake stage name");
    ensure!(
        config.prefix.is_empty()
            || (!config.prefix.starts_with('/')
                && !config.prefix.ends_with('/')
                && config.prefix.split('/').all(|s| !s.is_empty()
                    && s != "."
                    && s != ".."
                    && s.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))),
        "unsafe S3 stage prefix"
    );
    ensure!(
        (1..=HARD_MAX_FILE_BYTES).contains(&config.max_file_bytes),
        "invalid staged file size limit"
    );
    Ok(())
}

#[cfg(test)]
#[path = "stage_tests.rs"]
mod tests;
