//! Neutral, lossless row vocabulary for Snowflake staging.
use crate::decode::codecs::NumericKind;
use crate::decode::heap_decoder::{ColumnValue, CommittedTuple, HeapOp};
use crate::schema::{
    self, BOOLOID, BPCHAROID, BYTEAOID, CHAROID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, NAMEOID, NUMERICOID, OIDOID, RelDescriptor, TEXTOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, UUIDOID, VARCHAROID, replident_key_attnums,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnowflakeType {
    Boolean,
    Number { precision: u8, scale: u8 },
    Float,
    Binary,
    Text,
    Date,
    Time,
    TimestampNtz,
    TimestampTz,
}
impl SnowflakeType {
    pub fn sql(self) -> String {
        match self {
            Self::Boolean => "BOOLEAN".into(),
            Self::Number { precision, scale } => format!("NUMBER({precision},{scale})"),
            Self::Float => "FLOAT".into(),
            Self::Binary => "BINARY".into(),
            Self::Text => "VARCHAR".into(),
            Self::Date => "DATE".into(),
            Self::Time => "TIME(6)".into(),
            Self::TimestampNtz => "TIMESTAMP_NTZ(6)".into(),
            Self::TimestampTz => "TIMESTAMP_TZ(6)".into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SnowflakeValue {
    Null,
    Boolean(bool),
    Number(String),
    Float(f64),
    Binary(Vec<u8>),
    Text(String),
    Date(String),
    Time(String),
    TimestampNtz(String),
    TimestampTz(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeColumn {
    pub attnum: i16,
    pub source_name: String,
    pub name: String,
    pub type_oid: u32,
    pub data_type: SnowflakeType,
    pub not_null: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Snowflake schema in the session's configured database.
    pub database: String,
    pub table: String,
    pub columns: Vec<SnowflakeColumn>,
    pub key_indexes: Vec<usize>,
    pub relation_oid: u32,
}
impl TableSchema {
    pub fn from_descriptor(
        desc: &RelDescriptor,
        database: &str,
        table: &str,
        mapped_names: &[(i16, String)],
    ) -> Result<Self, String> {
        if database.is_empty() || table.is_empty() {
            return Err("Snowflake database/table name cannot be empty".into());
        }
        let mapped: HashMap<i16, &str> =
            mapped_names.iter().map(|(n, s)| (*n, s.as_str())).collect();
        if mapped.len() != mapped_names.len() {
            return Err("duplicate mapped source attnum".into());
        }
        let mut names = HashSet::new();
        let mut columns = Vec::new();
        for a in &desc.attributes {
            if a.dropped {
                continue;
            }
            let name = mapped.get(&a.attnum).copied().unwrap_or(&a.name);
            if name.is_empty()
                || !names.insert(name.to_ascii_uppercase())
                || name.to_ascii_uppercase().starts_with("_WS_")
            {
                return Err(format!(
                    "invalid, duplicate, or reserved Snowflake column {name:?}"
                ));
            }
            columns.push(SnowflakeColumn {
                attnum: a.attnum,
                source_name: a.name.clone(),
                name: name.into(),
                type_oid: a.type_oid,
                data_type: type_for(a.type_oid, a.typmod),
                not_null: a.not_null,
            });
        }
        let key_attnums = replident_key_attnums(desc);
        if key_attnums.is_empty() {
            return Err(format!(
                "{}.{} requires a stable replica identity or primary key",
                desc.rel_name.namespace, desc.rel_name.name
            ));
        }
        let mut key_indexes = Vec::new();
        let mut key_seen = HashSet::new();
        for attnum in key_attnums {
            if !key_seen.insert(*attnum) {
                return Err("duplicate replica identity attribute".into());
            }
            let idx = columns
                .iter()
                .position(|c| c.attnum == *attnum)
                .ok_or_else(|| {
                    format!("replica identity attribute {attnum} is dropped or missing")
                })?;
            if !columns[idx].not_null {
                return Err(format!(
                    "replica identity column {} must be NOT NULL",
                    columns[idx].source_name
                ));
            }
            if !matches!(
                columns[idx].type_oid,
                BOOLOID
                    | CHAROID
                    | INT2OID
                    | INT4OID
                    | INT8OID
                    | OIDOID
                    | NUMERICOID
                    | FLOAT4OID
                    | FLOAT8OID
                    | BYTEAOID
                    | TEXTOID
                    | VARCHAROID
                    | BPCHAROID
                    | NAMEOID
                    | UUIDOID
                    | DATEOID
                    | TIMEOID
                    | TIMESTAMPOID
                    | TIMESTAMPTZOID
            ) {
                return Err(format!(
                    "replica identity column {} has no supported canonical key encoding",
                    columns[idx].source_name
                ));
            }
            key_indexes.push(idx);
        }
        Ok(Self {
            database: database.into(),
            table: table.into(),
            columns,
            key_indexes,
            relation_oid: desc.oid,
        })
    }
}
pub fn type_for(oid: u32, typmod: i32) -> SnowflakeType {
    use schema::*;
    match oid {
        BOOLOID => SnowflakeType::Boolean,
        CHAROID | INT2OID | INT4OID | INT8OID | OIDOID => SnowflakeType::Number {
            precision: if oid == INT8OID { 19 } else { 10 },
            scale: 0,
        },
        FLOAT4OID | FLOAT8OID => SnowflakeType::Float,
        BYTEAOID => SnowflakeType::Binary,
        DATEOID => SnowflakeType::Date,
        TIMEOID => SnowflakeType::Time,
        TIMESTAMPOID => SnowflakeType::TimestampNtz,
        TIMESTAMPTZOID => SnowflakeType::TimestampTz,
        NUMERICOID
            if typmod >= 4
                && ((typmod - 4) >> 16) > 0
                && ((typmod - 4) >> 16) <= 38
                && ((typmod - 4) & 0xffff) <= ((typmod - 4) >> 16) =>
        {
            SnowflakeType::Number {
                precision: ((typmod - 4) >> 16) as u8,
                scale: ((typmod - 4) & 0xffff) as u8,
            }
        }
        _ => SnowflakeType::Text,
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnowflakeRow {
    pub values: Vec<SnowflakeValue>,
    pub key: String,
    pub source_identity: String,
    pub relation_incarnation: u64,
    pub load_generation: u64,
    pub commit_lsn: u64,
    pub record_lsn: u64,
    pub row_ordinal: u32,
    pub event_id: String,
    pub deleted: bool,
}
impl SnowflakeRow {
    pub fn from_committed(
        schema: &TableSchema,
        tuple: &CommittedTuple,
        ordinal: u32,
        deleted: bool,
    ) -> Result<Self, String> {
        Self::from_committed_with_lineage(schema, tuple, ordinal, deleted, "", 0, 0)
    }
    /// An UPDATE that changes replica identity emits an old-key tombstone first.
    pub fn update_rows_with_lineage(
        schema: &TableSchema,
        tuple: &CommittedTuple,
        ordinal: u32,
        source_identity: &str,
        relation_incarnation: u64,
        load_generation: u64,
    ) -> Result<Vec<Self>, String> {
        if !matches!(tuple.decoded.op, HeapOp::Update | HeapOp::HotUpdate) {
            return Err("update_rows_with_lineage requires UPDATE".into());
        }
        let new = Self::from_committed_with_lineage(
            schema,
            tuple,
            ordinal,
            false,
            source_identity,
            relation_incarnation,
            load_generation,
        )?;
        if tuple.decoded.old.is_none() {
            return Ok(vec![new]);
        }
        let old = Self::from_committed_with_lineage(
            schema,
            tuple,
            ordinal,
            true,
            source_identity,
            relation_incarnation,
            load_generation,
        )?;
        if old.key == new.key {
            Ok(vec![new])
        } else {
            Ok(vec![old, new])
        }
    }
    pub fn from_committed_with_lineage(
        schema: &TableSchema,
        tuple: &CommittedTuple,
        ordinal: u32,
        deleted: bool,
        source_identity: &str,
        relation_incarnation: u64,
        load_generation: u64,
    ) -> Result<Self, String> {
        if tuple.decoded.op == HeapOp::Truncate {
            return Err("TRUNCATE requires a table generation barrier".into());
        }
        let image = if deleted {
            tuple.decoded.old.as_ref().or(tuple.decoded.new.as_ref())
        } else {
            tuple.decoded.new.as_ref()
        }
        .ok_or("WAL tuple image is missing")?;
        let mut values = Vec::with_capacity(schema.columns.len());
        for (idx, c) in schema.columns.iter().enumerate() {
            let value = image
                .columns
                .get((c.attnum - 1) as usize)
                .and_then(Option::as_ref);
            match value {
                Some(v) => values.push(convert(v, c)?),
                None if deleted && !schema.key_indexes.contains(&idx) => {
                    values.push(SnowflakeValue::Null)
                }
                None => return Err(format!("WAL image omits required column {}", c.source_name)),
            }
        }
        let mut key_parts = Vec::new();
        for &i in &schema.key_indexes {
            if values[i] == SnowflakeValue::Null {
                return Err(format!(
                    "key column {} is NULL",
                    schema.columns[i].source_name
                ));
            }
            key_parts.push(format!(
                "{}:{}",
                schema.columns[i].attnum,
                canonical_key(&values[i], schema.columns[i].type_oid)
            ));
        }
        let key = serde_json::to_string(&key_parts).map_err(|e| e.to_string())?;
        let mut digest = Sha256::new();
        for part in [
            source_identity.as_bytes(),
            schema.database.as_bytes(),
            schema.table.as_bytes(),
            &schema.relation_oid.to_be_bytes(),
            &relation_incarnation.to_be_bytes(),
            &load_generation.to_be_bytes(),
            &tuple.commit_lsn.to_be_bytes(),
            &tuple.decoded.source_lsn.to_be_bytes(),
            &ordinal.to_be_bytes(),
            &[u8::from(deleted)],
            key.as_bytes(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
        let event_id = hex::encode(digest.finalize());
        Ok(Self {
            values,
            key,
            source_identity: source_identity.into(),
            relation_incarnation,
            load_generation,
            commit_lsn: tuple.commit_lsn,
            record_lsn: tuple.decoded.source_lsn,
            row_ordinal: ordinal,
            event_id,
            deleted,
        })
    }
}
fn canonical_key(v: &SnowflakeValue, type_oid: u32) -> String {
    if let SnowflakeValue::Text(value) = v {
        if type_oid == NUMERICOID {
            return format!("d:{}", normalize_decimal(value));
        }
        if type_oid == BPCHAROID {
            let value = value.trim_end_matches(' ');
            return format!("s:{}:{value}", value.len());
        }
    }
    match v {
        SnowflakeValue::Null => "n".into(),
        SnowflakeValue::Boolean(x) => format!("b:{x}"),
        SnowflakeValue::Number(x) => format!("d:{}", normalize_decimal(x)),
        SnowflakeValue::Float(x) => format!("f:{:016x}", if *x == 0.0 { 0 } else { x.to_bits() }),
        SnowflakeValue::Binary(x) => format!("x:{}", hex::encode(x)),
        SnowflakeValue::Text(x) => format!("s:{}:{x}", x.len()),
        SnowflakeValue::Date(x)
        | SnowflakeValue::Time(x)
        | SnowflakeValue::TimestampNtz(x)
        | SnowflakeValue::TimestampTz(x) => format!("t:{x}"),
    }
}
pub(crate) fn convert(v: &ColumnValue, c: &SnowflakeColumn) -> Result<SnowflakeValue, String> {
    let err = || {
        format!(
            "column {} (OID {}) cannot be encoded losslessly from {v:?}; resolve with the shadow oracle",
            c.source_name, c.type_oid
        )
    };
    if *v == ColumnValue::Null {
        return Ok(SnowflakeValue::Null);
    }
    match (v, c.data_type) {
        (ColumnValue::Bool(x), SnowflakeType::Boolean) => Ok(SnowflakeValue::Boolean(*x)),
        (ColumnValue::Char(x), SnowflakeType::Number { .. }) => {
            Ok(SnowflakeValue::Number(x.to_string()))
        }
        (ColumnValue::Int2(x), SnowflakeType::Number { .. }) => {
            Ok(SnowflakeValue::Number(x.to_string()))
        }
        (ColumnValue::Int4(x), SnowflakeType::Number { .. }) => {
            Ok(SnowflakeValue::Number(x.to_string()))
        }
        (ColumnValue::Int8(x), SnowflakeType::Number { .. }) => {
            Ok(SnowflakeValue::Number(x.to_string()))
        }
        (ColumnValue::Oid(x), SnowflakeType::Number { .. }) => {
            Ok(SnowflakeValue::Number(x.to_string()))
        }
        (ColumnValue::Float4(x), SnowflakeType::Float) => {
            finite_float(*x as f64).map(SnowflakeValue::Float)
        }
        (ColumnValue::Float8(x), SnowflakeType::Float) => {
            finite_float(*x).map(SnowflakeValue::Float)
        }
        (ColumnValue::Bytea(x), SnowflakeType::Binary) => Ok(SnowflakeValue::Binary(x.clone())),
        (
            ColumnValue::Text(x) | ColumnValue::Name(x) | ColumnValue::Json(x),
            SnowflakeType::Text,
        ) => Ok(SnowflakeValue::Text(x.clone())),
        (ColumnValue::Uuid(x), SnowflakeType::Text) => Ok(SnowflakeValue::Text(format_uuid(x))),
        (
            ColumnValue::Numeric(NumericKind::Finite(x)),
            SnowflakeType::Number { precision, scale },
        ) => {
            validate_decimal(x, precision, scale)?;
            Ok(SnowflakeValue::Number(x.clone()))
        }
        (ColumnValue::Numeric(x), SnowflakeType::Text) => {
            Ok(SnowflakeValue::Text(x.as_text().into()))
        }
        (ColumnValue::Inet(x), SnowflakeType::Text) => {
            Ok(SnowflakeValue::Text(x.to_text().to_string()))
        }
        (ColumnValue::Interval(x), SnowflakeType::Text) => {
            Ok(SnowflakeValue::Text(x.to_text().to_string()))
        }
        (ColumnValue::TimeTz { micros, tz_seconds }, SnowflakeType::Text) => {
            let offset = -i64::from(*tz_seconds);
            let sign = if offset < 0 { '-' } else { '+' };
            let abs = offset.abs();
            if abs > 15 * 3600 + 59 * 60 + 59 {
                return Err(format!(
                    "timetz offset {tz_seconds} outside PostgreSQL range"
                ));
            }
            let zone = if abs % 60 == 0 {
                format!("{sign}{:02}:{:02}", abs / 3600, (abs / 60) % 60)
            } else {
                format!(
                    "{sign}{:02}:{:02}:{:02}",
                    abs / 3600,
                    (abs / 60) % 60,
                    abs % 60
                )
            };
            Ok(SnowflakeValue::Text(format!(
                "{}{}",
                format_time(*micros)?,
                zone
            )))
        }
        (ColumnValue::Date(x), SnowflakeType::Date) => Ok(SnowflakeValue::Date(format_date(*x)?)),
        (ColumnValue::Time(x), SnowflakeType::Time) => Ok(SnowflakeValue::Time(format_time(*x)?)),
        (ColumnValue::Timestamp(x), SnowflakeType::TimestampNtz) => {
            Ok(SnowflakeValue::TimestampNtz(format_timestamp(*x, false)?))
        }
        (ColumnValue::TimestampTz(x), SnowflakeType::TimestampTz) => {
            Ok(SnowflakeValue::TimestampTz(format_timestamp(*x, true)?))
        }
        _ => Err(err()),
    }
}
fn finite_float(x: f64) -> Result<f64, String> {
    if x.is_finite() {
        Ok(x)
    } else {
        Err("Snowflake FLOAT cannot preserve NaN or Infinity".into())
    }
}
fn validate_decimal(x: &str, p: u8, s: u8) -> Result<(), String> {
    let t = x.trim_start_matches('-');
    let mut parts = t.split('.');
    let whole = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !frac.bytes().all(|b| b.is_ascii_digit())
        || (whole.is_empty() && frac.is_empty())
        || frac.len() > s as usize
        || whole.trim_start_matches('0').len() > (p - s) as usize
    {
        return Err(format!("numeric {x:?} exceeds NUMBER({p},{s})"));
    }
    Ok(())
}
fn normalize_decimal(value: &str) -> String {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches('-');
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let fractional = fractional.trim_end_matches('0');
    let sign = if negative && !(whole == "0" && fractional.is_empty()) {
        "-"
    } else {
        ""
    };
    if fractional.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fractional}")
    }
}
fn format_uuid(x: &[u8; 16]) -> String {
    let h = hex::encode(x);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    if m <= 2 {
        y += 1;
    }
    (y, m as u32, d as u32)
}
fn format_date(days: i32) -> Result<String, String> {
    let (y, m, d) = civil_from_days(i64::from(days) + 10957);
    if !(1..=9999).contains(&y) {
        return Err(format!("date year {y} outside Snowflake range"));
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}
fn format_time(us: i64) -> Result<String, String> {
    if !(0..86_400_000_000).contains(&us) {
        return Err(format!("time {us} outside day"));
    }
    let s = us / 1_000_000;
    Ok(format!(
        "{:02}:{:02}:{:02}.{:06}",
        s / 3600,
        (s / 60) % 60,
        s % 60,
        us % 1_000_000
    ))
}
fn format_timestamp(us: i64, tz: bool) -> Result<String, String> {
    let day = us.div_euclid(86_400_000_000);
    let rem = us.rem_euclid(86_400_000_000);
    let day: i32 = day.try_into().map_err(|_| "timestamp day out of range")?;
    Ok(format!(
        "{} {}{}",
        format_date(day)?,
        format_time(rem)?,
        if tz { " +00:00" } else { "" }
    ))
}
