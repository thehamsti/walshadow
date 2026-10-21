//! Bounded selection of fully ingested live batches for one state-table MERGE.

use super::{Payload, SnapshotTarget, TableSchema};
use crate::destination::snowflake::state::DurableBatch;
use anyhow::{Result, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub(super) const MAX_MERGE_BYTES: usize = 64 << 20;

pub(super) fn select(
    first: &DurableBatch,
    candidates: Vec<DurableBatch>,
    schema: &TableSchema,
) -> Result<Vec<DurableBatch>> {
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for batch in candidates {
        if batch.sequence < first.sequence || batch.channel != first.channel {
            continue;
        }
        let payload: Payload = serde_json::from_slice(&batch.payload)?;
        if payload.schema != *schema
            || payload.snapshot.is_some()
            || payload.pending_generation.is_some()
        {
            continue;
        }
        if !selected.is_empty() && bytes.saturating_add(batch.payload.len()) > MAX_MERGE_BYTES {
            break;
        }
        bytes = bytes.saturating_add(batch.payload.len());
        selected.push(batch);
    }
    ensure!(
        selected.iter().any(|batch| batch.id == first.id),
        "verified Snowflake batch missing from its merge candidates"
    );
    Ok(selected)
}

/// Verified batches of `first`'s snapshot generation, `first` always
/// included and leading so the byte budget can never starve it.
pub(super) fn select_snapshot(
    first: &DurableBatch,
    candidates: Vec<DurableBatch>,
    schema: &TableSchema,
    target: &SnapshotTarget,
) -> Result<Vec<DurableBatch>> {
    let mut bytes = first.payload.len();
    let mut selected = vec![first.clone()];
    for batch in candidates {
        if batch.id == first.id || batch.channel != first.channel {
            continue;
        }
        let header: PayloadHeader = serde_json::from_slice(&batch.payload)?;
        let same_generation = header.snapshot.as_ref().is_some_and(|t| {
            t.operation_id == target.operation_id && t.generation_id == target.generation_id
        });
        if header.schema != *schema || !same_generation || header.pending_generation.is_some() {
            continue;
        }
        if bytes.saturating_add(batch.payload.len()) > MAX_MERGE_BYTES {
            break;
        }
        bytes = bytes.saturating_add(batch.payload.len());
        selected.push(batch);
    }
    Ok(selected)
}

/// Routing fields of a durable payload; serde skips the row array.
#[derive(Deserialize)]
struct PayloadHeader {
    schema: TableSchema,
    #[serde(default)]
    snapshot: Option<SnapshotTarget>,
    #[serde(default)]
    pending_generation: Option<u64>,
}

pub(super) fn request_id(state_table: &str, batches: &[DurableBatch]) -> Uuid {
    let mut ids = batches.iter().map(|b| b.id.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    let mut hash = Sha256::new();
    for part in std::iter::once(state_table).chain(ids) {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part.as_bytes());
    }
    let digest = hash.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TableSchema {
        TableSchema {
            database: "PUBLIC".into(),
            table: "T".into(),
            columns: vec![],
            key_indexes: vec![],
            relation_oid: 1,
        }
    }

    fn batch(id: &str, sequence: u64, snapshot: bool) -> DurableBatch {
        let payload = Payload {
            schema: schema(),
            rows: vec![],
            snapshot: snapshot.then(|| super::super::SnapshotTarget {
                operation_id: "snapshot".into(),
                generation_id: 1,
            }),
            pending_generation: None,
        };
        DurableBatch {
            id: id.into(),
            channel: "pipe".into(),
            sequence,
            expected_rows: 1,
            payload: serde_json::to_vec(&payload).unwrap(),
        }
    }

    #[test]
    fn live_group_excludes_snapshot_and_has_stable_request_id() {
        let first = batch("first", 1, false);
        let second = batch("second", 2, false);
        let snapshot = batch("snapshot", 3, true);
        let selected = select(
            &first,
            vec![first.clone(), second.clone(), snapshot],
            &schema(),
        )
        .unwrap();
        assert_eq!(selected.len(), 2);
        assert_eq!(
            request_id("state", &selected),
            request_id("state", &[second, first])
        );
        assert_ne!(
            request_id("state", &selected),
            request_id("other", &selected)
        );
    }
}
