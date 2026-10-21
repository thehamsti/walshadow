use super::super::types::SnowflakeColumn;
use super::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn config() -> StageConfig {
    StageConfig {
        bucket: "test-bucket".into(),
        prefix: "walshadow/stage".into(),
        region: "us-east-2".into(),
        stage_name: "DB.PUBLIC.STAGE".into(),
        max_file_bytes: 1024 * 1024,
    }
}
fn schema() -> TableSchema {
    TableSchema {
        database: "DB".into(),
        table: "T".into(),
        columns: vec![
            SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "ID".into(),
                type_oid: 1700,
                data_type: SnowflakeType::Number {
                    precision: 38,
                    scale: 10,
                },
                not_null: true,
            },
            SnowflakeColumn {
                attnum: 2,
                source_name: "active".into(),
                name: "ACTIVE".into(),
                type_oid: 16,
                data_type: SnowflakeType::Boolean,
                not_null: false,
            },
            SnowflakeColumn {
                attnum: 3,
                source_name: "payload".into(),
                name: "PAYLOAD".into(),
                type_oid: 17,
                data_type: SnowflakeType::Binary,
                not_null: false,
            },
        ],
        key_indexes: vec![0],
        relation_oid: 42,
    }
}
fn row() -> SnowflakeRow {
    SnowflakeRow {
        values: vec![
            SnowflakeValue::Number("1234567890123456789012345678.1234567890".into()),
            SnowflakeValue::Boolean(true),
            SnowflakeValue::Binary(vec![0, 255]),
        ],
        key: "key".into(),
        source_identity: "source".into(),
        relation_incarnation: u64::MAX,
        load_generation: 7,
        commit_lsn: u64::MAX,
        record_lsn: 8,
        row_ordinal: 1,
        event_id: "event".into(),
        deleted: false,
    }
}

#[test]
fn json_preserves_decimal_and_unsigned_lineage() {
    let value = to_json(&schema(), &row(), "batch").unwrap();
    assert_eq!(value["ID"], "1234567890123456789012345678.1234567890");
    assert_eq!(value["_WS_COMMIT_LSN"], u64::MAX.to_string());
    assert_eq!(value["_WS_RELATION_INCARNATION"], u64::MAX.to_string());
    assert_eq!(value["PAYLOAD"], "00ff");
    assert_eq!(value["ACTIVE"], true);
}

#[test]
fn parquet_roundtrip_is_deterministic_and_has_all_fields() {
    let (first, descriptor) = encode_batch(&config(), "batch-1", &schema(), &[row()]).unwrap();
    let (second, other) = encode_batch(&config(), "batch-1", &schema(), &[row()]).unwrap();
    assert_eq!(first, second);
    assert_eq!(descriptor.key, other.key);
    assert_eq!(descriptor.sha256, hex::encode(Sha256::digest(&first)));
    assert!(descriptor.key.starts_with("walshadow/stage/42/batch-1-"));
    assert_eq!(descriptor.rows, 1);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(first))
        .unwrap()
        .build()
        .unwrap();
    let record = reader.next().unwrap().unwrap();
    let names: Vec<_> = record
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(names.contains(&"_WS_BATCH_ID".into()));
    let id = record
        .column(names.iter().position(|n| n == "ID").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id.value(0), "1234567890123456789012345678.1234567890");
    let batch = record
        .column(names.iter().position(|n| n == "_WS_BATCH_ID").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(batch.value(0), "batch-1");
    let active = record
        .column(names.iter().position(|n| n == "ACTIVE").unwrap())
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(active.value(0));
}

#[test]
fn stage_rejects_oversized_rows_and_unsafe_paths() {
    let mut cfg = config();
    cfg.max_file_bytes = 20;
    assert!(encode_batch(&cfg, "batch", &schema(), &[row()]).is_err());
    let cfg = config();
    assert!(encode_batch(&cfg, "../escape", &schema(), &[row()]).is_err());
}

#[test]
fn row_type_mismatch_fails_closed() {
    let mut invalid = row();
    invalid.values[0] = SnowflakeValue::Text("123".into());
    assert!(to_json(&schema(), &invalid, "batch").is_err());
    invalid.values[0] = SnowflakeValue::Number("123456789012345678901234567890.1234567890".into());
    assert!(to_json(&schema(), &invalid, "batch").is_err());
}
