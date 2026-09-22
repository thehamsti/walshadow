//! Explicit, ignored Snowflake integration tests. The runtime test needs a
//! provisioned external S3 stage and uses synthetic source descriptors/rows.
use anyhow::{Context, Result, ensure};
use futures::{StreamExt, TryStreamExt};
use uuid::Uuid;
use walshadow::destination::{
    config::DestinationConfig,
    snowflake::{
        http::SnowflakeHttp,
        runtime::SnowflakeRuntime,
        sql::{TableSqlPlan, quote_ident},
        types::{SnowflakeColumn, SnowflakeRow, SnowflakeType, SnowflakeValue, TableSchema},
    },
};
use walshadow::schema::{
    INT8OID, JSONBOID, RelAttr, RelDescriptor, RelName, ReplIdent, TEXTOID, TIMESTAMPTZOID,
};

#[test]
fn example_configuration_parses() {
    DestinationConfig::parse(include_str!("../config/snowflake.toml")).unwrap();
}

#[tokio::test]
#[ignore = "requires WALSHADOW_SNOWFLAKE_LIVE_CONFIG and live Snowflake credentials"]
async fn typed_current_state_version_order_and_receipt() -> Result<()> {
    let path = std::env::var("WALSHADOW_SNOWFLAKE_LIVE_CONFIG")
        .context("WALSHADOW_SNOWFLAKE_LIVE_CONFIG must point to a Snowflake TOML config")?;
    let text = tokio::fs::read_to_string(&path)
        .await
        .context("read live Snowflake config")?;
    let config = DestinationConfig::parse(&text)?
        .snowflake
        .context("config must select Snowflake")?;
    let http = SnowflakeHttp::new(config.http_config()?)?;
    let isolated = format!(
        "WS_LIVE_{}",
        Uuid::new_v4().simple().to_string().to_uppercase()
    );
    let db = quote_ident(&config.database).map_err(anyhow::Error::msg)?;
    let schema_name = quote_ident(&isolated).map_err(anyhow::Error::msg)?;
    let qualified_schema = format!("{db}.{schema_name}");
    http.execute_sql(&format!("CREATE SCHEMA {qualified_schema}"), Uuid::new_v4())
        .await?;
    let result = run_case(&http, &isolated).await;
    let cleanup = http
        .execute_sql(
            &format!("DROP SCHEMA {qualified_schema} CASCADE"),
            Uuid::new_v4(),
        )
        .await;
    cleanup.context("drop isolated live-test schema")?;
    result
}

async fn run_case(http: &SnowflakeHttp, isolated: &str) -> Result<()> {
    let schema = TableSchema {
        database: isolated.to_owned(),
        table: "CURRENT_ROWS".into(),
        relation_oid: 987654321,
        columns: vec![
            SnowflakeColumn {
                attnum: 1,
                source_name: "id".into(),
                name: "ID".into(),
                type_oid: 20,
                data_type: SnowflakeType::Number {
                    precision: 19,
                    scale: 0,
                },
                not_null: true,
            },
            SnowflakeColumn {
                attnum: 2,
                source_name: "value".into(),
                name: "VALUE".into(),
                type_oid: 25,
                data_type: SnowflakeType::Text,
                not_null: false,
            },
        ],
        key_indexes: vec![0],
    };
    let plan = TableSqlPlan::new(&schema, isolated).map_err(anyhow::Error::msg)?;
    for ddl in [
        &plan.create_landing_sql,
        &plan.create_state_sql,
        &plan.create_receipts_sql,
        &plan.view_sql,
    ] {
        http.execute_sql(ddl, Uuid::new_v4()).await?;
    }
    apply(&plan, http, "b10", "first", 10, false).await?;
    assert_value(&plan, http, Some("first")).await?;
    apply(&plan, http, "b20", "new", 20, false).await?;
    assert_value(&plan, http, Some("new")).await?;
    // A delayed older event must not restore the previous value.
    apply(&plan, http, "b15", "old", 15, false).await?;
    assert_value(&plan, http, Some("new")).await?;
    apply(&plan, http, "b30", "new", 30, true).await?;
    assert_value(&plan, http, None).await?;
    Ok(())
}

async fn apply(
    plan: &TableSqlPlan,
    http: &SnowflakeHttp,
    batch: &str,
    value: &str,
    lsn: u64,
    deleted: bool,
) -> Result<()> {
    let value = value.replace('\\', "\\\\").replace('\'', "''");
    let deleted = if deleted { "TRUE" } else { "FALSE" };
    let insert = format!(
        "INSERT INTO {} (\"ID\",\"VALUE\",_WS_KEY,_WS_SOURCE_IDENTITY,_WS_RELATION_INCARNATION,_WS_LOAD_GENERATION,_WS_COMMIT_LSN,_WS_RECORD_LSN,_WS_ROW_ORDINAL,_WS_EVENT_ID,_WS_DELETED,_WS_BATCH_ID) VALUES (1,'{value}','[\"ID:d:1\"]','live-test',1,0,{lsn},{lsn},0,'event-{lsn}',{deleted},'{batch}')",
        plan.landing_table
    );
    http.execute_sql(&insert, Uuid::new_v4()).await?;
    let completeness = http
        .execute_sql(&plan.batch_completeness_sql(batch, 1), Uuid::new_v4())
        .await?;
    ensure!(
        completeness.rows == vec![vec![serde_json::Value::String("1".into())]],
        "live batch completeness failed"
    );
    http.execute_sql_multi(
        &[
            "BEGIN TRANSACTION".into(),
            plan.merge_sql(batch),
            plan.insert_receipt_sql(batch, 1),
            "COMMIT".into(),
        ],
        Uuid::new_v4(),
    )
    .await?;
    let receipt = http
        .execute_sql(&plan.receipt_sql(batch), Uuid::new_v4())
        .await?;
    ensure!(
        receipt.rows == vec![vec![serde_json::Value::String("1".into())]],
        "live apply receipt missing"
    );
    Ok(())
}

async fn assert_value(
    plan: &TableSqlPlan,
    http: &SnowflakeHttp,
    expected: Option<&str>,
) -> Result<()> {
    let result = http
        .execute_sql(
            &format!(
                "SELECT \"VALUE\" FROM {} WHERE \"ID\" = 1",
                plan.public_view
            ),
            Uuid::new_v4(),
        )
        .await?;
    match expected {
        Some(value) => ensure!(
            result.rows == vec![vec![serde_json::Value::String(value.into())]],
            "unexpected current-state value"
        ),
        None => ensure!(result.rows.is_empty(), "deleted row remains visible"),
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires WALSHADOW_SNOWFLAKE_LIVE_CONFIG, Snowflake credentials, and a provisioned S3 stage"]
async fn runtime_stream_copy_restart_snapshot_and_truncate() -> Result<()> {
    let path = std::env::var("WALSHADOW_SNOWFLAKE_LIVE_CONFIG")
        .context("WALSHADOW_SNOWFLAKE_LIVE_CONFIG must point to a Snowflake TOML config")?;
    let text = tokio::fs::read_to_string(&path).await?;
    let mut config = DestinationConfig::parse(&text)?
        .snowflake
        .context("config must select Snowflake")?;
    let suffix = Uuid::new_v4().simple().to_string().to_uppercase();
    let public_schema = format!("WS_LIVE_P_{suffix}");
    let internal_schema = format!("WS_LIVE_I_{suffix}");
    let state_dir = tempfile::tempdir()?;
    config.state.directory = state_dir.path().join("state");
    config.internal_schema = internal_schema.clone();
    config
        .schema_mapping
        .insert("public".into(), public_schema.clone());
    let http = SnowflakeHttp::new(config.http_config()?)?;
    let db = quote_ident(&config.database).map_err(anyhow::Error::msg)?;
    let public = format!(
        "{db}.{}",
        quote_ident(&public_schema).map_err(anyhow::Error::msg)?
    );
    let internal = format!(
        "{db}.{}",
        quote_ident(&internal_schema).map_err(anyhow::Error::msg)?
    );
    http.execute_sql(&format!("CREATE SCHEMA {public}"), Uuid::new_v4())
        .await?;
    let result = run_runtime_case(config, &http, &public_schema).await;
    // Only schemas created by this test are removed. The provisioned stage and
    // checksum-addressed S3 files remain for normal retention management.
    let public_cleanup = http
        .execute_sql(&format!("DROP SCHEMA {public} CASCADE"), Uuid::new_v4())
        .await;
    let internal_cleanup = http
        .execute_sql(
            &format!("DROP SCHEMA IF EXISTS {internal} CASCADE"),
            Uuid::new_v4(),
        )
        .await;
    public_cleanup.context("drop isolated public live-test schema")?;
    internal_cleanup.context("drop isolated internal live-test schema")?;
    result
}

async fn run_runtime_case(
    config: walshadow::destination::config::SnowflakeConfig,
    http: &SnowflakeHttp,
    public_schema: &str,
) -> Result<()> {
    let desc = runtime_descriptor();
    let schema = TableSchema::from_descriptor(&desc, public_schema, "ROWS", &[])
        .map_err(anyhow::Error::msg)?;
    let runtime = SnowflakeRuntime::open(config.clone(), 0x5F11, "live_fixture").await?;
    // Bootstrap prepares independent relations concurrently, including shared
    // schema/receipt DDL. Prove that each resulting generation stays isolated.
    futures::stream::iter(0..8_u32)
        .map(|index| {
            let runtime = &runtime;
            let database = &config.database;
            async move {
                let mut parallel = runtime_descriptor();
                parallel.oid += index + 1;
                parallel.rfn.rel_node = parallel.oid;
                parallel.rel_name = RelName::new("public", &format!("PARALLEL_{index}"));
                runtime.defer_publication(&parallel)?;
                runtime.ensure_table(&parallel).await?;
                let generation = runtime
                    .begin_snapshot_attempt(&parallel, 50, &format!("parallel-{index}"))
                    .await?;
                // Concurrent snapshot batches coalesce into grouped MERGEs;
                // the last relation keeps the explicit-empty path covered.
                let batches = if index == 7 { 0 } else { 5_u64 };
                let schema = TableSchema::from_descriptor(
                    &parallel,
                    public_schema,
                    &parallel.rel_name.name,
                    &[],
                )
                .map_err(anyhow::Error::msg)?;
                let (incarnation, _) = runtime.lineage(&parallel).await?;
                let ids = futures::stream::iter(0..batches)
                    .map(|batch| {
                        let schema = schema.clone();
                        let operation_id = generation.operation_id.clone();
                        let rows = (0..2)
                            .map(|row| {
                                keyed_row(
                                    runtime,
                                    incarnation,
                                    generation.generation_id,
                                    batch * 2 + row,
                                )
                            })
                            .collect();
                        async move { runtime.deliver_snapshot(schema, rows, &operation_id).await }
                    })
                    .buffer_unordered(5)
                    .try_collect::<Vec<_>>()
                    .await?;
                runtime
                    .mark_snapshot_loaded(&parallel, &generation.operation_id, &ids, ids.is_empty())
                    .await?;
                runtime
                    .publish_snapshot(&parallel, &generation.operation_id)
                    .await?;
                let table = format!(
                    "{}.{}.{}",
                    quote_ident(database).map_err(anyhow::Error::msg)?,
                    quote_ident(public_schema).map_err(anyhow::Error::msg)?,
                    quote_ident(&parallel.rel_name.name).map_err(anyhow::Error::msg)?
                );
                let result = http
                    .execute_sql(&format!("SELECT COUNT(*) FROM {table}"), Uuid::new_v4())
                    .await?;
                ensure!(
                    result
                        .rows
                        .first()
                        .and_then(|row| row.first())
                        .and_then(serde_json::Value::as_str)
                        == Some((batches * 2).to_string().as_str()),
                    "parallel generation row count differs from its snapshot"
                );
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(8)
        .try_collect::<Vec<_>>()
        .await?;
    runtime.ensure_table(&desc).await?;
    if let Ok(reader) = std::env::var("WALSHADOW_SNOWFLAKE_TEST_READER_ROLE") {
        http.execute_sql(
            &format!(
                "GRANT SELECT ON VIEW {}.{}.\"ROWS\" TO ROLE {}",
                quote_ident(&config.database).map_err(anyhow::Error::msg)?,
                quote_ident(public_schema).map_err(anyhow::Error::msg)?,
                quote_ident(&reader).map_err(anyhow::Error::msg)?,
            ),
            Uuid::new_v4(),
        )
        .await?;
    }
    let (incarnation, _) = runtime.lineage(&desc).await?;
    runtime
        .deliver(
            schema.clone(),
            vec![runtime_row(&runtime, incarnation, 0, 100, "stream", false)],
        )
        .await?;
    assert_runtime_value(http, &config.database, public_schema, Some("stream")).await?;

    let generation = runtime
        .begin_snapshot_attempt(&desc, 150, "live-snapshot")
        .await?;
    // A later WAL event arrives while the snapshot generation is hidden.
    runtime
        .deliver(
            schema.clone(),
            vec![runtime_row(
                &runtime,
                incarnation,
                0,
                200,
                "later-wal",
                false,
            )],
        )
        .await?;
    let batch = runtime
        .deliver_snapshot(
            schema.clone(),
            vec![runtime_row(
                &runtime,
                incarnation,
                generation.generation_id,
                150,
                "snapshot",
                false,
            )],
            &generation.operation_id,
        )
        .await?;
    runtime
        .mark_snapshot_loaded(&desc, &generation.operation_id, &[batch], false)
        .await?;
    runtime
        .publish_snapshot(&desc, &generation.operation_id)
        .await?;
    assert_runtime_value(http, &config.database, public_schema, Some("later-wal")).await?;

    let active = runtime.active_generation(&schema).await?;
    runtime
        .deliver(
            schema.clone(),
            vec![runtime_row(
                &runtime,
                incarnation,
                active,
                250,
                "deleted",
                true,
            )],
        )
        .await?;
    assert_runtime_value(http, &config.database, public_schema, None).await?;
    runtime
        .deliver(
            schema.clone(),
            vec![runtime_row(
                &runtime,
                incarnation,
                active,
                300,
                "reinserted",
                false,
            )],
        )
        .await?;
    runtime
        .deliver(
            schema.clone(),
            vec![runtime_row(
                &runtime,
                incarnation,
                active,
                275,
                "older",
                false,
            )],
        )
        .await?;
    assert_runtime_value(http, &config.database, public_schema, Some("reinserted")).await?;
    drop(runtime);

    let runtime = SnowflakeRuntime::open(config.clone(), 0x5F11, "live_fixture").await?;
    assert_runtime_value(http, &config.database, public_schema, Some("reinserted")).await?;
    runtime.truncate_at(&desc, 400).await?;
    assert_runtime_value(http, &config.database, public_schema, None).await?;
    // A replayed truncate is idempotent after durable reopen.
    drop(runtime);
    let runtime = SnowflakeRuntime::open(config.clone(), 0x5F11, "live_fixture").await?;
    runtime.truncate_at(&desc, 400).await?;
    assert_runtime_value(http, &config.database, public_schema, None).await?;
    Ok(())
}

fn runtime_descriptor() -> RelDescriptor {
    let attr = |attnum, name: &str, type_oid| RelAttr {
        attnum,
        name: name.into(),
        type_oid,
        typmod: -1,
        not_null: attnum == 1,
        dropped: false,
        type_name: String::new(),
        type_byval: true,
        type_len: 8,
        type_align: 'd',
        type_storage: 'p',
        missing_default: None,
    };
    let mut desc = RelDescriptor {
        rfn: Default::default(),
        oid: 987_654,
        toast_oid: 0,
        namespace_oid: 2200,
        rel_name: RelName::new("public", "ROWS"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes: vec![
            attr(1, "ID", INT8OID),
            attr(2, "VALUE", TEXTOID),
            attr(3, "CREATED_AT", TIMESTAMPTZOID),
            attr(4, "JSON_DATA", JSONBOID),
        ],
    };
    desc.rfn.rel_node = 987_654;
    desc
}

fn runtime_row(
    runtime: &SnowflakeRuntime,
    incarnation: u64,
    generation: u64,
    lsn: u64,
    value: &str,
    deleted: bool,
) -> SnowflakeRow {
    SnowflakeRow {
        values: vec![
            SnowflakeValue::Number("1".into()),
            SnowflakeValue::Text(value.into()),
            SnowflakeValue::TimestampTz("2026-09-21 12:34:56.123456 +00:00".into()),
            SnowflakeValue::Text(r#"{"settings_default_due_time":"23:59:00","label":"café","nested":{"enabled":true}}"#.into()),
        ],
        key: "[\"1:d:1\"]".into(),
        source_identity: runtime.source_identity.clone(),
        relation_incarnation: incarnation,
        load_generation: generation,
        commit_lsn: lsn,
        record_lsn: lsn,
        row_ordinal: 0,
        event_id: format!("live-{lsn}"),
        deleted,
    }
}

fn keyed_row(
    runtime: &SnowflakeRuntime,
    incarnation: u64,
    generation: u64,
    id: u64,
) -> SnowflakeRow {
    let mut row = runtime_row(runtime, incarnation, generation, 50, "snapshot", false);
    row.values[0] = SnowflakeValue::Number(id.to_string());
    row.key = format!("[\"{id}:d:1\"]");
    row.event_id = format!("snapshot-{id}");
    row
}

async fn assert_runtime_value(
    http: &SnowflakeHttp,
    database: &str,
    public_schema: &str,
    expected: Option<&str>,
) -> Result<()> {
    let table = format!(
        "{}.{}.\"ROWS\"",
        quote_ident(database).map_err(anyhow::Error::msg)?,
        quote_ident(public_schema).map_err(anyhow::Error::msg)?,
    );
    let views = http
        .execute_sql(
            &format!(
                "SHOW VIEWS LIKE 'ROWS' IN SCHEMA {}.{}",
                quote_ident(database).map_err(anyhow::Error::msg)?,
                quote_ident(public_schema).map_err(anyhow::Error::msg)?,
            ),
            Uuid::new_v4(),
        )
        .await?;
    let tracking = views
        .row_type
        .as_array()
        .and_then(|columns| {
            columns
                .iter()
                .position(|column| column["name"] == "change_tracking")
        })
        .context("SHOW VIEWS omitted change_tracking")?;
    ensure!(
        views.rows.len() == 1 && views.rows[0][tracking] == "ON",
        "current-state view lost change tracking"
    );
    if let Ok(reader) = std::env::var("WALSHADOW_SNOWFLAKE_TEST_READER_ROLE") {
        let grants = http
            .execute_sql(&format!("SHOW GRANTS ON VIEW {table}"), Uuid::new_v4())
            .await?;
        let columns = grants
            .row_type
            .as_array()
            .context("SHOW GRANTS missing row type")?;
        let privilege = columns
            .iter()
            .position(|column| column["name"] == "privilege")
            .context("missing privilege column")?;
        let grantee = columns
            .iter()
            .position(|column| column["name"] == "grantee_name")
            .context("missing grantee column")?;
        ensure!(
            grants
                .rows
                .iter()
                .any(|row| row[privilege] == "SELECT" && row[grantee] == reader),
            "current-state view lost reader grant"
        );
    }
    let result = http
        .execute_sql(
            &format!("SELECT \"VALUE\" FROM {table} WHERE \"ID\" = 1"),
            Uuid::new_v4(),
        )
        .await?;
    match expected {
        Some(value) => {
            ensure!(
                result.rows == vec![vec![serde_json::Value::String(value.into())]],
                "unexpected runtime current-state value: {:?}",
                result.rows
            );
            let types = http.execute_sql(
                &format!("SELECT IFF(\"CREATED_AT\" = TO_TIMESTAMP_TZ('2026-09-21 12:34:56.123456 +00:00'), 'match', 'mismatch'), PARSE_JSON(\"JSON_DATA\"):settings_default_due_time::TIME::VARCHAR, PARSE_JSON(\"JSON_DATA\"):label::VARCHAR FROM {table} WHERE \"ID\" = 1"),
                Uuid::new_v4(),
            ).await?;
            ensure!(
                types.rows
                    == vec![vec![
                        serde_json::json!("match"),
                        serde_json::json!("23:59:00"),
                        serde_json::json!("café"),
                    ]],
                "timestamp/JSON/time projection mismatch: {:?}",
                types.rows
            );
        }
        None => ensure!(
            result.rows.is_empty(),
            "runtime deleted row remains visible"
        ),
    }
    Ok(())
}
