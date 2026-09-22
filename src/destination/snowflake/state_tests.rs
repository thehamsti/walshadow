use super::*;
use bytes::Bytes;
use tempfile::tempdir;

fn identity() -> StateIdentity {
    StateIdentity {
        source_system_id: 42,
        destination_fingerprint: "acct/db/schema".into(),
    }
}
fn batch() -> DurableBatch {
    DurableBatch {
        id: "b-1".into(),
        channel: "wal".into(),
        sequence: 7,
        expected_rows: 2,
        payload: b"immutable payload".to_vec(),
    }
}

#[test]
fn unknown_or_missing_format_version_refuses_restart() {
    for version in [None, Some(STATE_FORMAT_VERSION + 1)] {
        let dir = tempdir().unwrap();
        let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
        if let Some(version) = version {
            state
                .db
                .put(b"format-version", version.to_le_bytes())
                .unwrap();
        } else {
            state.db.delete(b"format-version").unwrap();
        }
        drop(state);
        assert!(
            StateStore::open(dir.path(), identity(), 1024)
                .err()
                .unwrap()
                .to_string()
                .contains("unsupported Snowflake state format")
        );
    }
}

#[test]
fn batch_reopens_and_phase_transitions_are_checked() {
    let dir = tempdir().unwrap();
    {
        let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
        state.enqueue(&batch()).unwrap();
        state.enqueue(&batch()).unwrap();
        assert!(state.mark_applied("b-1").is_err());
        assert_eq!(state.pending().unwrap(), vec![batch()]);
    }
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert_eq!(state.phase("b-1").unwrap(), Some(BatchPhase::Queued));
    state.mark_verified("b-1").unwrap();
    state.mark_verified("b-1").unwrap();
    state.mark_applied("b-1").unwrap();
    assert!(state.pending().unwrap().is_empty());
    assert!(state.mark_verified("b-1").is_err());
}

#[test]
fn verified_candidates_are_channel_ordered_and_payload_bounded() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    for (id, channel, sequence, size) in [
        ("a", "wal", 1, 4),
        ("b", "wal", 2, 5),
        ("c", "wal", 3, 6),
        ("other", "other", 1, 3),
    ] {
        state
            .enqueue(&DurableBatch {
                id: id.into(),
                channel: channel.into(),
                sequence,
                expected_rows: 1,
                payload: vec![1; size],
            })
            .unwrap();
        if id != "c" {
            state.mark_verified(id).unwrap();
        }
    }
    assert_eq!(state.pending_ids().unwrap(), vec!["other", "a", "b", "c"]);
    let (bounded, full) = state
        .verified_candidates_with_fullness("wal", 1, 8)
        .unwrap();
    assert_eq!(
        bounded.iter().map(|b| b.id.as_str()).collect::<Vec<_>>(),
        vec!["a"]
    );
    assert!(full, "a following verified batch fills the merge budget");
    let (bounded, full) = state
        .verified_candidates_with_fullness("wal", 1, 10)
        .unwrap();
    assert_eq!(bounded.len(), 2);
    assert!(!full, "unverified successors do not fill the merge budget");
    assert_eq!(
        state
            .verified_candidates("wal", 1, 8)
            .unwrap()
            .iter()
            .map(|b| b.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert_eq!(
        state
            .verified_candidates("wal", 1, 9)
            .unwrap()
            .iter()
            .map(|b| b.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert_eq!(
        state.verified_candidates("other", 1, 1).unwrap()[0].id,
        "other"
    );
    assert_eq!(state.verified_candidates("wal", 2, 9).unwrap()[0].id, "b");
    state.mark_applied("a").unwrap();
    assert_eq!(state.pending_ids().unwrap(), vec!["other", "b", "c"]);
    assert_eq!(state.verified_candidates("wal", 1, 9).unwrap()[0].id, "b");
}

#[tokio::test]
async fn toast_and_outbound_payloads_share_the_budget() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 512).unwrap();
    let toast = state.toast_store();
    let row = ToastRow {
        toast_relid: 3,
        blkno: 1,
        offnum: 1,
        chunk_id: 7,
        chunk_seq: 0,
        chunk_data: Bytes::from_static(b"data"),
        lsn: 100,
    };
    toast.put(std::slice::from_ref(&row)).await.unwrap();
    // An empty outbox always admits one batch, else nothing could ever
    // release space; a second one waits for the first to apply
    let mut outbound = batch();
    outbound.payload = vec![0; 512];
    state.enqueue(&outbound).unwrap();
    let mut second = batch();
    second.id = "b-2".into();
    second.sequence += 1;
    let err = state.enqueue(&second).unwrap_err();
    assert!(err.is::<BudgetExceeded>(), "{err}");
    let mut large = row.clone();
    large.lsn = 101;
    large.chunk_data = Bytes::from(vec![1; 512]);
    assert!(toast.put(&[large]).await.is_err());
    // A rejected write cannot overwrite the prior as-of value or consume budget.
    assert!(toast.fetch(3, 7, 100, 4).await.is_ok());
    toast.put(&[row]).await.unwrap();
}

#[test]
fn ownership_lock_checksum_and_sequence_fail_closed() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert!(StateStore::open(dir.path(), identity(), 1024).is_err());
    state.enqueue(&batch()).unwrap();
    let mut conflict = batch();
    conflict.id = "b-2".into();
    assert!(state.enqueue(&conflict).is_err());
    drop(state);
    let mut wrong = identity();
    wrong.source_system_id = 43;
    assert!(StateStore::open(dir.path(), wrong, 1024).is_err());
    fs::write(dir.path().join("payloads/b-1.bin"), b"corrupt payload!!!").unwrap();
    assert!(StateStore::open(dir.path(), identity(), 1024).is_err());
}

#[test]
fn toast_clone_keeps_exclusive_owner_lock() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    let toast = state.toast_store();
    drop(state);
    assert!(StateStore::open(dir.path(), identity(), 1024).is_err());
    drop(toast);
    StateStore::open(dir.path(), identity(), 1024).unwrap();
}

#[test]
fn missing_manifest_payload_fails_on_reopen() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    state.enqueue(&batch()).unwrap();
    drop(state);
    fs::remove_file(dir.path().join("payloads/b-1.bin")).unwrap();
    assert!(StateStore::open(dir.path(), identity(), 1024).is_err());
}

#[test]
fn restart_finishes_manifested_temporary_payload_publication() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    state.enqueue(&batch()).unwrap();
    drop(state);
    fs::rename(
        dir.path().join("payloads/b-1.bin"),
        dir.path().join("payloads/b-1.tmp"),
    )
    .unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert_eq!(state.pending().unwrap(), vec![batch()]);
    assert!(dir.path().join("payloads/b-1.bin").exists());
}

#[test]
fn orphan_payloads_without_manifests_are_crashed_enqueues() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    drop(state);
    fs::write(dir.path().join("payloads/attempt.tmp"), b"partial").unwrap();
    StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert!(!dir.path().join("payloads/attempt.tmp").exists());
    // The manifest is the commit point: a final payload without one never
    // reached Snowflake or acknowledged anything
    fs::write(dir.path().join("payloads/unpublished.bin"), b"durable").unwrap();
    StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert!(!dir.path().join("payloads/unpublished.bin").exists());
}

#[test]
fn applied_payload_reclaims_budget_after_durable_phase() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), batch().payload.len() as u64).unwrap();
    state.enqueue(&batch()).unwrap();
    state.mark_verified("b-1").unwrap();
    state.mark_applied("b-1").unwrap();
    let mut next = batch();
    next.id = "b-2".into();
    next.sequence += 1;
    state.enqueue(&next).unwrap();
    assert!(!dir.path().join("payloads/b-1.bin").exists());
    assert_eq!(state.pending().unwrap(), vec![next]);
}

#[test]
fn sequence_reservation_and_metadata_survive_reopen() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert_eq!(state.allocate_sequence("wal").unwrap(), 1);
    assert_eq!(state.allocate_sequence("wal").unwrap(), 2);
    // Only an enqueue persists the head: the allocations above reached
    // nothing durable, so reusing them after a crash is harmless
    let mut enqueued = batch();
    enqueued.channel = "wal".into();
    enqueued.sequence = 2;
    state.enqueue(&enqueued).unwrap();
    state.put_metadata("request-1", b"stable-uuid").unwrap();
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert_eq!(state.allocate_sequence("wal").unwrap(), 3);
    assert_eq!(
        state.get_metadata("request-1").unwrap(),
        Some(b"stable-uuid".to_vec())
    );
    assert!(state.put_metadata("request-1", b"changed").is_err());
    assert!(
        !state
            .compare_exchange_metadata("request-1", Some(b"wrong"), b"next")
            .unwrap()
    );
    assert!(
        state
            .compare_exchange_metadata("request-1", Some(b"stable-uuid"), b"next")
            .unwrap()
    );
    assert_eq!(
        state.get_metadata("request-1").unwrap(),
        Some(b"next".to_vec())
    );
}

#[test]
fn generation_operation_retries_reopen_without_new_id_and_guards_phases() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    let first = state
        .prepare_generation("snapshot-42", 77, 500, "schema-a")
        .unwrap();
    assert_eq!(first.phase, GenerationPhase::Prepared);
    assert_eq!(
        first,
        state
            .prepare_generation("snapshot-42", 77, 500, "schema-a")
            .unwrap()
    );
    assert!(
        state
            .prepare_generation("snapshot-42", 77, 501, "schema-a")
            .is_err()
    );
    assert!(state.mark_generation_published("snapshot-42").is_err());
    state.mark_generation_loaded("snapshot-42").unwrap();
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert_eq!(
        state
            .generation("snapshot-42")
            .unwrap()
            .unwrap()
            .generation_id,
        first.generation_id
    );
    assert_eq!(
        state
            .prepare_generation("snapshot-43", 77, 501, "schema-a")
            .unwrap()
            .generation_id,
        first.generation_id + 1
    );
    state.mark_generation_published("snapshot-42").unwrap();
    state.mark_generation_replayed("snapshot-42").unwrap();
    state.mark_generation_retired("snapshot-42").unwrap();
    assert!(state.mark_generation_loaded("snapshot-42").is_err());
}

#[test]
fn pending_spool_reopens_and_requires_receipt_before_retirement() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4).unwrap();
    let row = PendingRow {
        id: "row-1".into(),
        relation_oid: 77,
        xmin: 10,
        xmax: 11,
        record_lsn: 500,
        payload: b"data".to_vec(),
    };
    state.enqueue_pending(&row).unwrap();
    state.enqueue_pending(&row).unwrap();
    assert!(state.retire_pending(&row).is_err());
    assert!(state.mark_pending_promoted(&row, b"").is_err());
    assert_eq!(
        state.pending_for_xid(77, 10).unwrap()[0].phase,
        PendingPhase::Raw
    );
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 4).unwrap();
    state
        .mark_pending_promoted(&row, b"remote-receipt")
        .unwrap();
    state
        .mark_pending_promoted(&row, b"remote-receipt")
        .unwrap();
    assert!(state.mark_pending_promoted(&row, b"different").is_err());
    assert_eq!(
        state.pending_for_xid(77, 11).unwrap()[0].phase,
        PendingPhase::Promoted
    );
    state.retire_pending(&row).unwrap();
    assert!(
        state.pending_for_xid(77, 10).unwrap()[0]
            .row
            .payload
            .is_empty()
    );
    assert!(state.retire_pending_aborted(&row).is_err());
    drop(state);
    StateStore::open(dir.path(), identity(), 4).unwrap();
}

#[test]
fn pending_corruption_fails_open_and_aborted_row_reclaims_budget() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4).unwrap();
    let row = PendingRow {
        id: "row-2".into(),
        relation_oid: 77,
        xmin: 12,
        xmax: 0,
        record_lsn: 501,
        payload: b"data".to_vec(),
    };
    state.enqueue_pending(&row).unwrap();
    let key = pending_key(&row).unwrap();
    let mut stored: StoredPending =
        serde_json::from_slice(&state.db.get(&key).unwrap().unwrap()).unwrap();
    stored.payload[0] ^= 1;
    state
        .put_sync(&key, serde_json::to_vec(&stored).unwrap())
        .unwrap();
    drop(state);
    assert!(StateStore::open(dir.path(), identity(), 4).is_err());

    let index = DB::open_default(dir.path().join("index")).unwrap();
    stored.payload[0] ^= 1;
    index
        .put(&key, serde_json::to_vec(&stored).unwrap())
        .unwrap();
    drop(index);
    let state = StateStore::open(dir.path(), identity(), 4).unwrap();
    state.retire_pending_aborted(&row).unwrap();
    assert!(state.retire_pending(&row).is_err());
    drop(state);
    StateStore::open(dir.path(), identity(), 4).unwrap();
}

#[tokio::test]
async fn toast_reopens_with_as_of_history_and_rejects_ambiguity() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    let toast = state.toast_store();
    let row = ToastRow {
        toast_relid: 3,
        blkno: 5,
        offnum: 1,
        chunk_id: 10,
        chunk_seq: 0,
        chunk_data: Bytes::from_static(b"abc"),
        lsn: 100,
    };
    toast.put(std::slice::from_ref(&row)).await.unwrap();
    toast.put(std::slice::from_ref(&row)).await.unwrap();
    let mut conflict = row.clone();
    conflict.chunk_data = Bytes::from_static(b"xyz");
    assert!(toast.put(&[conflict]).await.is_err());
    let mut death = row.clone();
    death.chunk_id = 0;
    death.chunk_data = Bytes::new();
    death.lsn = 110;
    toast.put(&[death]).await.unwrap();
    drop(toast);
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    let toast = state.toast_store();
    assert!(
        matches!(toast.fetch(3,10,100,3).await.unwrap(), FetchedValue::Assembled(v) if v == b"abc")
    );
    assert!(matches!(
        toast.fetch(3, 10, 110, 3).await.unwrap(),
        FetchedValue::Missing
    ));
    assert!(toast.truncate_mirror(3).await.is_err());
}

#[tokio::test]
async fn corrupt_toast_value_index_blocks_restart() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    state
        .toast_store()
        .put(&[ToastRow {
            toast_relid: 3,
            blkno: 1,
            offnum: 1,
            chunk_id: 7,
            chunk_seq: 0,
            chunk_data: Bytes::from_static(b"data"),
            lsn: 100,
        }])
        .await
        .unwrap();
    let key = state
        .db
        .prefix_iterator(b"toast-value/")
        .next()
        .unwrap()
        .unwrap()
        .0;
    state.db.delete(key).unwrap();
    drop(state);
    assert!(StateStore::open(dir.path(), identity(), 4096).is_err());
}

#[tokio::test]
async fn orphan_toast_value_index_blocks_restart_even_when_accounted() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    state
        .toast_store()
        .put(&[ToastRow {
            toast_relid: 3,
            blkno: 1,
            offnum: 1,
            chunk_id: 7,
            chunk_seq: 0,
            chunk_data: Bytes::from_static(b"data"),
            lsn: 100,
        }])
        .await
        .unwrap();
    // An index entry with no row behind it, with byte accounting adjusted so
    // only the index/row correspondence can reject it
    let orphan_key = b"toast-value/00000003/00000008/00000000/00000001/0001/0000000000000064";
    let orphan_value = b"toast/00000003/00000009/0001/0000000000000064";
    state.db.put(orphan_key, orphan_value).unwrap();
    let occupied = read_u64(&state.db, b"toast/occupied").unwrap();
    state
        .db
        .put(
            b"toast/occupied",
            (occupied + (orphan_key.len() + orphan_value.len()) as u64).to_le_bytes(),
        )
        .unwrap();
    drop(state);
    let error = StateStore::open(dir.path(), identity(), 4096)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("missing or foreign rows"), "{error}");
}

#[tokio::test]
async fn truncate_preserves_history_and_retirement_preserves_oid_reuse() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    let toast = state.toast_store();
    let row = ToastRow {
        toast_relid: 3,
        blkno: 1,
        offnum: 1,
        chunk_id: 7,
        chunk_seq: 0,
        chunk_data: Bytes::from_static(b"old"),
        lsn: 10,
    };
    toast.put(std::slice::from_ref(&row)).await.unwrap();
    toast.truncate_at(3, 20).await.unwrap();
    assert!(
        matches!(toast.fetch(3,7,19,3).await.unwrap(), FetchedValue::Assembled(v) if v == b"old")
    );
    assert!(matches!(
        toast.fetch(3, 7, 20, 3).await.unwrap(),
        FetchedValue::Missing
    ));
    let mut reused = row;
    reused.lsn = 30;
    reused.chunk_data = Bytes::from_static(b"new");
    toast.put(&[reused]).await.unwrap();
    let before = read_u64(&state.db, b"toast/occupied").unwrap();
    toast.retire_at(3, 25).await.unwrap();
    assert!(read_u64(&state.db, b"toast/occupied").unwrap() < before);
    assert!(matches!(
        toast.fetch(3, 7, 19, 3).await.unwrap(),
        FetchedValue::Missing
    ));
    assert!(
        matches!(toast.fetch(3,7,30,3).await.unwrap(), FetchedValue::Assembled(v) if v == b"new")
    );
    drop(toast);
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    assert!(
        matches!(state.toast_store().fetch(3,7,30,3).await.unwrap(), FetchedValue::Assembled(v) if v == b"new")
    );
}

#[tokio::test]
async fn rewrite_barrier_keeps_pre_barrier_as_of_value() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    let toast = state.toast_store();
    toast
        .put(&[ToastRow {
            toast_relid: 4,
            blkno: 1,
            offnum: 1,
            chunk_id: 9,
            chunk_seq: 0,
            chunk_data: Bytes::from_static(b"abc"),
            lsn: 10,
        }])
        .await
        .unwrap();
    toast.rewrite_barrier(4, 20, 30).await.unwrap();
    assert!(
        matches!(toast.fetch(4, 9, 20, 3).await.unwrap(), FetchedValue::Assembled(v) if v == b"abc")
    );
    assert!(matches!(
        toast.fetch(4, 9, 30, 3).await.unwrap(),
        FetchedValue::Missing
    ));
    drop(toast);
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    assert!(
        matches!(state.toast_store().fetch(4, 9, 20, 3).await.unwrap(), FetchedValue::Assembled(v) if v == b"abc")
    );
}

#[test]
fn applied_manifests_are_collected_after_one_grace_pass() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1024).unwrap();
    state.enqueue(&batch()).unwrap();
    state.mark_verified("b-1").unwrap();
    state.mark_applied("b-1").unwrap();
    // First pass only notes it, so a late idempotent transition still works
    assert_eq!(state.gc_applied().unwrap(), 0);
    state.mark_applied("b-1").unwrap();
    assert_eq!(state.gc_applied().unwrap(), 1);
    assert_eq!(state.phase("b-1").unwrap(), None);
    // The head survives: later allocations never reuse the sequence
    assert!(state.allocate_sequence("wal").unwrap() > batch().sequence);
    drop(state);
    StateStore::open(dir.path(), identity(), 1024).unwrap();
}

#[test]
fn candidate_scan_starts_at_the_oldest_outstanding_sequence() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    for (id, seq) in [("a", 1), ("b", 2), ("c", 3)] {
        state
            .enqueue(&DurableBatch {
                id: id.into(),
                channel: "wal".into(),
                sequence: seq,
                expected_rows: 1,
                payload: id.as_bytes().to_vec(),
            })
            .unwrap();
        state.mark_verified(id).unwrap();
    }
    state.mark_applied("a").unwrap();
    let ids: Vec<_> = state
        .verified_candidates("wal", 0, 1 << 20)
        .unwrap()
        .into_iter()
        .map(|b| b.id)
        .collect();
    assert_eq!(ids, ["b", "c"]);
    state.mark_applied("b").unwrap();
    state.mark_applied("c").unwrap();
    assert!(state.verified_candidates("wal", 0, 1 << 20).unwrap().is_empty());
    // Rebuilt from manifests on reopen
    drop(state);
    let state = StateStore::open(dir.path(), identity(), 4096).unwrap();
    assert!(state.verified_candidates("wal", 0, 1 << 20).unwrap().is_empty());
}

#[tokio::test]
async fn toast_gc_keeps_only_what_reads_above_the_floor_can_see() {
    let dir = tempdir().unwrap();
    let state = StateStore::open(dir.path(), identity(), 1 << 20).unwrap();
    let toast = state.toast_store();
    let chunk = |lsn: u64, chunk_id: u32, data: &'static [u8]| ToastRow {
        toast_relid: 3,
        blkno: 1,
        offnum: 1,
        chunk_id,
        chunk_seq: 0,
        chunk_data: Bytes::from_static(data),
        lsn,
    };
    toast
        .put(&[chunk(100, 7, b"old"), chunk(200, 8, b"mid"), chunk(300, 9, b"new")])
        .await
        .unwrap();
    // Another TID whose newest version at or below the floor is a tombstone
    let mut gone = chunk(100, 5, b"gone");
    gone.blkno = 2;
    let mut tomb = gone.clone();
    tomb.chunk_id = 0;
    tomb.chunk_data = Bytes::new();
    tomb.lsn = 150;
    toast.put(&[gone, tomb]).await.unwrap();

    assert_eq!(toast.gc_below(250).unwrap(), 3, "old, gone and its tombstone");
    // Reads at or above the floor are unchanged
    assert!(matches!(
        toast.fetch(3, 8, 250, 3).await.unwrap(),
        crate::toast::FetchedValue::Assembled(ref v) if v == b"mid"
    ));
    assert!(matches!(
        toast.fetch(3, 9, 300, 3).await.unwrap(),
        crate::toast::FetchedValue::Assembled(ref v) if v == b"new"
    ));
    assert!(!matches!(
        toast.fetch(3, 5, 250, 4).await.unwrap(),
        crate::toast::FetchedValue::Assembled(_)
    ));
    assert_eq!(toast.gc_below(250).unwrap(), 0, "idempotent");
    drop((state, toast));
    // Byte accounting and indexes still verify on reopen
    StateStore::open(dir.path(), identity(), 1 << 20).unwrap();
}
