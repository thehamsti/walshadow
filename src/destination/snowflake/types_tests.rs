use super::types::*;
use crate::decode::heap_decoder::*;
use crate::schema::*;

fn attr(n: i16, name: &str, oid: u32, not_null: bool) -> RelAttr {
    RelAttr {
        attnum: n,
        name: name.into(),
        type_oid: oid,
        typmod: -1,
        not_null,
        dropped: false,
        type_name: String::new(),
        type_byval: true,
        type_len: 4,
        type_align: 'i',
        type_storage: 'p',
        missing_default: None,
    }
}
fn desc() -> RelDescriptor {
    RelDescriptor {
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
        attributes: vec![
            attr(1, "id", INT8OID, true),
            attr(2, "value", TEXTOID, false),
        ],
    }
}
#[test]
fn schema_rejects_missing_or_nullable_key() {
    let mut d = desc();
    d.replident = ReplIdent::Nothing;
    assert!(TableSchema::from_descriptor(&d, "DB", "T", &[]).is_err());
    d = desc();
    d.attributes[0].not_null = false;
    assert!(TableSchema::from_descriptor(&d, "DB", "T", &[]).is_err());
}
#[test]
fn absent_is_distinct_from_null() {
    let s = TableSchema::from_descriptor(&desc(), "DB", "T", &[]).unwrap();
    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: Default::default(),
            xid: 1,
            source_lsn: 2,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int8(7)), None],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 0,
        commit_lsn: 3,
    };
    assert!(SnowflakeRow::from_committed(&s, &tuple, 0, false).is_err());
}
#[test]
fn event_id_changes_with_ordinal() {
    let s = TableSchema::from_descriptor(&desc(), "DB", "T", &[]).unwrap();
    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: Default::default(),
            xid: 1,
            source_lsn: 2,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int8(7)), Some(ColumnValue::Null)],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 0,
        commit_lsn: 3,
    };
    let a = SnowflakeRow::from_committed(&s, &tuple, 0, false).unwrap();
    let b = SnowflakeRow::from_committed(&s, &tuple, 1, false).unwrap();
    assert_ne!(a.event_id, b.event_id);
    assert_eq!(a.values[1], SnowflakeValue::Null);
}

#[test]
fn changed_key_emits_tombstone_then_new_row() {
    let s = TableSchema::from_descriptor(&desc(), "DB", "T", &[]).unwrap();
    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: Default::default(),
            xid: 1,
            source_lsn: 2,
            op: HeapOp::Update,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int8(8)),
                    Some(ColumnValue::Text("new".into())),
                ],
                partial: false,
            }),
            old: Some(DecodedTuple {
                columns: vec![Some(ColumnValue::Int8(7)), None],
                partial: false,
            }),
        },
        commit_ts: 0,
        commit_lsn: 3,
    };
    let rows = SnowflakeRow::update_rows_with_lineage(&s, &tuple, 0, "source", 1, 0).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].deleted);
    assert_eq!(rows[0].values[1], SnowflakeValue::Null);
    assert!(!rows[1].deleted);
    assert_ne!(rows[0].event_id, rows[1].event_id);
}

#[test]
fn timetz_text_preserves_second_offset() {
    let mut d = desc();
    d.attributes[1].type_oid = TIMETZOID;
    let s = TableSchema::from_descriptor(&d, "DB", "T", &[]).unwrap();
    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: Default::default(),
            xid: 1,
            source_lsn: 2,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int8(7)),
                    Some(ColumnValue::TimeTz {
                        micros: 3_600_000_000,
                        tz_seconds: -19_830,
                    }),
                ],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 0,
        commit_lsn: 3,
    };
    let row =
        SnowflakeRow::from_committed_with_lineage(&s, &tuple, 0, false, "source", 1, 0).unwrap();
    assert_eq!(
        row.values[1],
        SnowflakeValue::Text("01:00:00.000000+05:30:30".into())
    );
}

#[test]
fn floating_signed_zero_has_one_replication_key() {
    let mut d = desc();
    d.attributes[0].type_oid = FLOAT8OID;
    let schema = TableSchema::from_descriptor(&d, "PUBLIC", "T", &[]).unwrap();
    let row = |zero| {
        let tuple = CommittedTuple {
            decoded: DecodedHeap {
                rfn: Default::default(),
                xid: 1,
                source_lsn: 2,
                op: HeapOp::Insert,
                new: Some(DecodedTuple {
                    columns: vec![Some(ColumnValue::Float8(zero)), Some(ColumnValue::Null)],
                    partial: false,
                }),
                old: None,
            },
            commit_ts: 0,
            commit_lsn: 3,
        };
        SnowflakeRow::from_committed(&schema, &tuple, 0, false).unwrap()
    };
    assert_eq!(row(0.0).key, row(-0.0).key);
    assert_eq!(row(0.0).event_id, row(-0.0).event_id);
}

#[test]
fn key_encoding_obeys_numeric_and_blank_padded_equality() {
    for (oid, left, right) in [
        (
            NUMERICOID,
            ColumnValue::Numeric(crate::decode::codecs::NumericKind::Finite("1.00".into())),
            ColumnValue::Numeric(crate::decode::codecs::NumericKind::Finite("1.0".into())),
        ),
        (
            BPCHAROID,
            ColumnValue::Text("key ".into()),
            ColumnValue::Text("key".into()),
        ),
    ] {
        let mut d = desc();
        d.attributes[0].type_oid = oid;
        let schema = TableSchema::from_descriptor(&d, "PUBLIC", "T", &[]).unwrap();
        let row = |value| {
            let tuple = CommittedTuple {
                decoded: DecodedHeap {
                    rfn: Default::default(),
                    xid: 1,
                    source_lsn: 2,
                    op: HeapOp::Insert,
                    new: Some(DecodedTuple {
                        columns: vec![Some(value), Some(ColumnValue::Null)],
                        partial: false,
                    }),
                    old: None,
                },
                commit_ts: 0,
                commit_lsn: 3,
            };
            SnowflakeRow::from_committed(&schema, &tuple, 0, false).unwrap()
        };
        assert_eq!(row(left).key, row(right).key);
    }
    let mut d = desc();
    d.attributes[0].type_oid = INTERVALOID;
    assert!(TableSchema::from_descriptor(&d, "PUBLIC", "T", &[]).is_err());
}
