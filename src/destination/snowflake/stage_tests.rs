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

#[test]
fn ndjson_is_one_object_per_row_and_stops_at_the_streaming_limit() {
    let rows = [row(), row()];
    let body = to_ndjson(&schema(), &rows, "batch", usize::MAX)
        .unwrap()
        .unwrap();
    let lines: Vec<_> = body.split(|b| *b == b'\n').collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[2].is_empty());
    let expected = to_json(&schema(), &row(), "batch").unwrap();
    for line in &lines[..2] {
        assert_eq!(serde_json::from_slice::<Value>(line).unwrap(), expected);
    }
    // At or past the limit the batch stages instead
    assert!(
        to_ndjson(&schema(), &rows, "batch", body.len())
            .unwrap()
            .is_none()
    );
    assert!(
        to_ndjson(&schema(), &rows, "batch", body.len() + 1)
            .unwrap()
            .is_some()
    );
    let mut invalid = row();
    invalid.values[0] = SnowflakeValue::Text("123".into());
    assert!(to_ndjson(&schema(), &[invalid], "batch", usize::MAX).is_err());
}

/// Doubles whose shortest text has 16 or 17 digits, or an exponent no
/// 38-digit decimal can hold
const FLOATS: [f64; 12] = [
    0.0,
    -0.0,
    1.0,
    0.1,
    0.30000000000000004,
    1.7976931348623157e308,
    -1.7976931348623157e308,
    2.2250738585072014e-308,
    5e-324,
    1.2345678901234568e20,
    9007199254740993.0,
    123456.78900000002,
];

fn float_schema() -> TableSchema {
    TableSchema {
        database: "DB".into(),
        table: "F".into(),
        columns: vec![
            SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "ID".into(),
                type_oid: 23,
                data_type: SnowflakeType::Number {
                    precision: 10,
                    scale: 0,
                },
                not_null: true,
            },
            SnowflakeColumn {
                attnum: 2,
                source_name: "reading".into(),
                name: "READING".into(),
                type_oid: 701,
                data_type: SnowflakeType::Float,
                not_null: false,
            },
        ],
        key_indexes: vec![0],
        relation_oid: 43,
    }
}

fn float_rows() -> Vec<SnowflakeRow> {
    FLOATS
        .iter()
        .enumerate()
        .map(|(i, value)| SnowflakeRow {
            values: vec![
                SnowflakeValue::Number(i.to_string()),
                SnowflakeValue::Float(*value),
            ],
            key: format!("key-{i}"),
            event_id: format!("event-{i}"),
            ..row()
        })
        .chain(std::iter::once(SnowflakeRow {
            values: vec![SnowflakeValue::Number("99".into()), SnowflakeValue::Null],
            key: "key-null".into(),
            event_id: "event-null".into(),
            ..row()
        }))
        .collect()
}

/// Streaming and staging must hand Snowflake the same float text. A JSON
/// number would reach the load projection through a double-to-VARCHAR cast
#[test]
fn floats_travel_as_shortest_round_trip_text_on_both_paths() {
    let rows = float_rows();
    let (file, _) = encode_batch(&config(), "batch-f", &float_schema(), &rows).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(file))
        .unwrap()
        .build()
        .unwrap();
    let record = reader.next().unwrap().unwrap();
    let at = record.schema().index_of("READING").unwrap();
    let staged = record
        .column(at)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ndjson = to_ndjson(&float_schema(), &rows, "batch-f", usize::MAX)
        .unwrap()
        .unwrap();
    let streamed: Vec<Value> = ndjson
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(streamed.len(), rows.len());
    for (i, value) in FLOATS.iter().enumerate() {
        let text = streamed[i]["READING"]
            .as_str()
            .unwrap_or_else(|| panic!("{value:e} streamed as a JSON number"));
        assert_eq!(text, staged.value(i), "{value:e}");
        assert_eq!(
            text.parse::<f64>().unwrap().to_bits(),
            value.to_bits(),
            "{text} does not parse back to {value:e}"
        );
    }
    assert_eq!(staged.value(5), "1.7976931348623157e+308");
    assert_eq!(staged.value(8), "5e-324");
    // NULL stays NULL on both paths
    assert!(arrow_array::Array::is_null(staged, FLOATS.len()));
    assert!(streamed[FLOATS.len()]["READING"].is_null());
    let mut nonfinite = rows[0].clone();
    nonfinite.values[1] = SnowflakeValue::Float(f64::NAN);
    assert!(to_json(&float_schema(), &nonfinite, "batch-f").is_err());
}
