use super::sql::*;
use super::types::*;
use crate::schema::*;

#[test]
fn identifiers_are_quoted_and_literals_escaped() {
    assert_eq!(quote_ident("a\"b").unwrap(), "\"a\"\"b\"");
    assert_eq!(quote_literal("x'y"), "'x''y'");
    assert!(quote_ident("").is_err());
    assert_eq!(
        quote_qualified_ident("DB.SC.STAGE").unwrap(),
        "\"DB\".\"SC\".\"STAGE\""
    );
    assert!(quote_qualified_ident("DB..STAGE").is_err());
}
#[test]
fn merge_keeps_tombstones_and_records_receipt() {
    let d = RelDescriptor {
        rfn: Default::default(),
        oid: 1,
        toast_oid: 0,
        namespace_oid: 1,
        rel_name: RelName::new("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: INT8OID,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_name: String::new(),
            type_byval: true,
            type_len: 8,
            type_align: 'd',
            type_storage: 'p',
            missing_default: None,
        }],
    };
    let s = TableSchema::from_descriptor(&d, "D", "T", &[]).unwrap();
    let p = TableSqlPlan::new(&s, "INTERNAL").unwrap();
    assert!(!p.landing_table.contains("T_LANDING"));
    assert!(p.landing_table.len() < 255);
    let sql = p.merge_sql("batch'1");
    assert!(sql.contains("WHEN MATCHED"));
    assert!(sql.contains("s._WS_LOAD_GENERATION"));
    assert!(sql.contains("_WS_SOURCE_IDENTITY"));
    assert!(sql.contains("_WS_RELATION_INCARNATION"));
    assert!(sql.contains("WHEN NOT MATCHED"));
    assert!(!sql.contains("DELETE FROM"));
    assert_eq!(sql.matches('(').count(), sql.matches(')').count());
    // Storage generations isolate snapshots; within a generation source WAL
    // order wins even when the snapshot carries a newer load generation tag.
    assert!(!sql.contains("s._WS_LOAD_GENERATION > t._WS_LOAD_GENERATION"));
    assert!(p.view_sql.contains("_WS_DELETED = FALSE"));
    assert!(p.view_sql.contains("CHANGE_TRACKING = TRUE COPY GRANTS"));
    assert!(p.receipt_sql("batch'1").contains("batch''1"));
    let group = p
        .merge_sql_group(&["batch'1".into(), "batch2".into()])
        .unwrap();
    assert!(group.contains("_WS_BATCH_ID IN ('batch''1', 'batch2')"));
    assert!(group.contains("PARTITION BY _WS_KEY"));
    let receipts = p
        .merge_receipts_sql(&[("batch'1".into(), 2), ("batch2".into(), 3)])
        .unwrap();
    assert!(receipts.contains("('batch''1', 2), ('batch2', 3)"));
    assert!(receipts.contains("WHEN NOT MATCHED THEN INSERT"));
    assert!(p.merge_sql_group(&[]).is_err());
}

#[test]
fn copy_modes_have_distinct_error_contracts() {
    let d = RelDescriptor {
        rfn: Default::default(),
        oid: 1,
        toast_oid: 0,
        namespace_oid: 1,
        rel_name: RelName::new("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: INT8OID,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_name: String::new(),
            type_byval: true,
            type_len: 8,
            type_align: 'd',
            type_storage: 'p',
            missing_default: None,
        }],
    };
    let s = TableSchema::from_descriptor(&d, "D", "T", &[]).unwrap();
    let p = TableSqlPlan::new(&s, "INTERNAL").unwrap();
    let streaming = p.create_streaming_pipe_sql(&s, "pipe").unwrap();
    assert!(streaming.contains("DATA_SOURCE(TYPE => 'STREAMING')"));
    assert!(streaming.contains("CREATE PIPE IF NOT EXISTS \"INTERNAL\".\"pipe\""));
    assert!(!streaming.contains("ON_ERROR"));
    let staged = p
        .copy_into_sql(&s, "DB.SC.stage", &["batch/a.parquet".into()])
        .unwrap();
    assert!(staged.contains("ON_ERROR = ABORT_STATEMENT"));
    assert!(staged.contains("FILES = ('batch/a.parquet')"));
    assert!(
        p.copy_into_sql(&s, "DB.SC.stage", &["../escape".into()])
            .is_err()
    );
    assert_eq!(quote_literal("a\\b'c"), "'a\\\\b''c'");
}

#[test]
fn completeness_compares_typed_rows_without_serializing_object_keys() {
    let d = RelDescriptor {
        rfn: Default::default(),
        oid: 1,
        toast_oid: 0,
        namespace_oid: 1,
        rel_name: RelName::new("public", "t"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default {
            pk_attnums: Some(vec![1]),
        },
        attributes: vec![RelAttr {
            attnum: 1,
            name: "id".into(),
            type_oid: INT8OID,
            typmod: -1,
            not_null: true,
            dropped: false,
            type_name: String::new(),
            type_byval: true,
            type_len: 8,
            type_align: 'd',
            type_storage: 'p',
            missing_default: None,
        }],
    };
    let schema = TableSchema::from_descriptor(&d, "D", "T", &[]).unwrap();
    let sql = TableSqlPlan::new(&schema, "INTERNAL")
        .unwrap()
        .batch_completeness_sql("batch'1", 2);
    assert!(sql.contains("SELECT DISTINCT * FROM"));
    assert!(sql.contains("_WS_BATCH_ID = 'batch''1'"));
    assert!(sql.contains("GROUP BY _WS_EVENT_ID"));
    assert!(sql.contains("PAYLOAD_VERSIONS <> 1"));
    assert!(sql.contains("COUNT(*) = 2"));
    assert!(!sql.contains("TO_JSON"));
}
