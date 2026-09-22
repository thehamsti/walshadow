//! Durable outbound batch journal and source-owned TOAST history.
//!
//! A temporary payload is fsynced before its RocksDB manifest, then atomically
//! published. Restart finishes that publication and verifies pending payloads.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

use crate::toast::{ChunkAssembler, ChunkStore, ChunkStoreError, FetchedValue, ToastRow};

const STATE_FORMAT_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateIdentity {
    pub source_system_id: u64,
    pub destination_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableBatch {
    pub id: String,
    pub channel: String,
    pub sequence: u64,
    pub expected_rows: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchPhase {
    Queued,
    Verified,
    Applied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GenerationPhase {
    Prepared,
    Loaded,
    Published,
    Replayed,
    Retired,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerationRecord {
    pub operation_id: String,
    pub relation_oid: u32,
    pub snapshot_lsn: u64,
    pub schema_fingerprint: String,
    pub generation_id: u64,
    pub phase: GenerationPhase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRow {
    pub id: String,
    pub relation_oid: u32,
    pub xmin: u32,
    pub xmax: u32,
    pub record_lsn: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PendingPhase {
    Raw,
    Promoted,
    Retired,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRecord {
    pub row: PendingRow,
    pub phase: PendingPhase,
    pub receipt: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct StoredPending {
    id: String,
    relation_oid: u32,
    xmin: u32,
    xmax: u32,
    record_lsn: u64,
    payload: Vec<u8>,
    payload_len: u64,
    payload_crc32c: u32,
    phase: PendingPhase,
    receipt: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Manifest {
    id: String,
    channel: String,
    sequence: u64,
    expected_rows: u64,
    payload_len: u64,
    payload_crc32c: u32,
    phase: BatchPhase,
}

pub struct StateStore {
    db: Arc<DB>,
    root: PathBuf,
    max_bytes: u64,
    _lock: Arc<File>,
    mutation: Arc<Mutex<()>>,
    /// Next sequence per channel. Allocation is in memory: a sequence only
    /// reaches Snowflake after its enqueue persisted the head at or past it,
    /// so a crash between allocation and enqueue can reuse it harmlessly
    heads: Mutex<HashMap<String, u64>>,
    /// Unapplied sequences per channel, so candidate scans start at the
    /// oldest outstanding batch instead of the channel's first sequence
    outstanding: Mutex<HashMap<String, std::collections::BTreeSet<u64>>>,
    /// Applied manifests seen by the previous [`gc_applied`](Self::gc_applied),
    /// removed on the next call so a late idempotent transition still finds them
    gc_grace: Mutex<HashSet<String>>,
    /// Signalled whenever outbound or pending bytes are released
    space: Arc<tokio::sync::Notify>,
}

/// Outbound payloads cannot fit until applied batches release space
#[derive(Debug, thiserror::Error)]
#[error("Snowflake state byte budget exceeded")]
pub struct BudgetExceeded;

impl StateStore {
    pub fn open(
        path: impl AsRef<Path>,
        identity: StateIdentity,
        max_bytes: u64,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !identity.destination_fingerprint.is_empty(),
            "empty destination fingerprint"
        );
        anyhow::ensure!(max_bytes > 0, "state budget must be positive");
        let root = path.as_ref().to_path_buf();
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&root)?;
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("LOCK.owner"))?;
        // The kernel releases this lock on process exit; never unlink a lock
        // file, which could let another process lock a different inode.
        let rc = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        anyhow::ensure!(
            rc == 0,
            "Snowflake state is already owned by another process"
        );
        fs::create_dir_all(root.join("payloads"))?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, root.join("index"))?);
        let store = Self {
            db,
            root,
            max_bytes,
            _lock: Arc::new(lock),
            mutation: Arc::new(Mutex::new(())),
            heads: Mutex::new(HashMap::new()),
            outstanding: Mutex::new(HashMap::new()),
            gc_grace: Mutex::new(HashSet::new()),
            space: Arc::new(tokio::sync::Notify::new()),
        };
        match store.db.get(b"identity")? {
            Some(prior) => {
                anyhow::ensure!(
                    read_u64(&store.db, b"format-version")? == STATE_FORMAT_VERSION,
                    "unsupported Snowflake state format; restore a compatible binary or reinitialize from the source"
                );
                anyhow::ensure!(
                    prior.as_slice() == serde_json::to_vec(&identity)?.as_slice(),
                    "Snowflake state ownership identity mismatch"
                );
            }
            None => {
                anyhow::ensure!(
                    store.db.iterator(IteratorMode::Start).next().is_none(),
                    "state index lacks ownership identity"
                );
                let mut batch = WriteBatch::default();
                batch.put(b"identity", serde_json::to_vec(&identity)?);
                batch.put(b"format-version", STATE_FORMAT_VERSION.to_le_bytes());
                store.write_sync(batch)?;
            }
        }
        store.verify_and_clean()?;
        sync_dir(&store.root)?;
        if let Some(parent) = store.root.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        Ok(store)
    }

    pub fn enqueue(&self, batch: &DurableBatch) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        validate_id(&batch.id)?;
        anyhow::ensure!(!batch.channel.is_empty(), "empty batch channel");
        let key = manifest_key(&batch.id);
        let path = self.payload_path(&batch.id);
        if let Some(existing) = self.manifest(&batch.id)? {
            anyhow::ensure!(
                existing.channel == batch.channel
                    && existing.sequence == batch.sequence
                    && existing.expected_rows == batch.expected_rows
                    && existing.payload_len == batch.payload.len() as u64
                    && existing.payload_crc32c == crc32c::crc32c(&batch.payload),
                "conflicting batch retry"
            );
            if existing.phase != BatchPhase::Applied || path.exists() {
                anyhow::ensure!(
                    read_payload(&path, &existing)? == batch.payload,
                    "conflicting batch payload retry"
                );
            }
            return Ok(());
        }
        let seq_key = sequence_key(&batch.channel, batch.sequence)?;
        anyhow::ensure!(
            self.db.get(&seq_key)?.is_none(),
            "channel sequence already belongs to a different batch"
        );
        let total = self.occupied()?;
        let pending_total = read_u64(&self.db, b"pending/occupied")?;
        let fits = total
            .checked_add(pending_total)
            .and_then(|n| n.checked_add(read_u64(&self.db, b"toast/occupied").unwrap_or(u64::MAX)))
            .and_then(|n| n.checked_add(batch.payload.len() as u64))
            .is_some_and(|n| n <= self.max_bytes);
        // An empty outbox must still admit one batch, else nothing could
        // ever release space; TOAST is bounded by its own check
        if !fits && (total > 0 || pending_total > 0) {
            return Err(BudgetExceeded.into());
        }
        // The manifest is the commit point: a final payload without one is
        // a crashed enqueue that restart deletes, so no rename is needed
        let result = (|| -> anyhow::Result<()> {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            f.write_all(&batch.payload)?;
            f.sync_all()?;
            sync_dir(&self.root.join("payloads"))?;
            let m = Manifest {
                id: batch.id.clone(),
                channel: batch.channel.clone(),
                sequence: batch.sequence,
                expected_rows: batch.expected_rows,
                payload_len: batch.payload.len() as u64,
                payload_crc32c: crc32c::crc32c(&batch.payload),
                phase: BatchPhase::Queued,
            };
            let mut wb = WriteBatch::default();
            wb.put(&key, serde_json::to_vec(&m)?);
            wb.put(seq_key, batch.id.as_bytes());
            let head_key = sequence_head_key(&batch.channel)?;
            let head = self.sequence_head(&batch.channel)?;
            wb.put(head_key, head.max(batch.sequence).to_le_bytes());
            wb.put(
                b"occupied",
                (total + batch.payload.len() as u64).to_le_bytes(),
            );
            self.write_sync(wb)?;
            Ok(())
        })();
        if result.is_err() && self.manifest(&batch.id)?.is_none() {
            let _ = fs::remove_file(&path);
            return result;
        }
        self.outstanding
            .lock()
            .unwrap()
            .entry(batch.channel.clone())
            .or_default()
            .insert(batch.sequence);
        result
    }

    /// Resolves once space may have been released; callers retry
    /// [`enqueue`](Self::enqueue) after it. Wakes periodically as a backstop
    pub async fn space_released(&self) {
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(1), self.space.notified()).await;
    }

    pub fn pending(&self) -> anyhow::Result<Vec<DurableBatch>> {
        let _guard = self.mutation.lock().unwrap();
        let mut out = Vec::new();
        for m in self.manifests()? {
            if m.phase != BatchPhase::Applied {
                let payload = read_payload(&self.payload_path(&m.id), &m)?;
                out.push(DurableBatch {
                    id: m.id.clone(),
                    channel: m.channel,
                    sequence: m.sequence,
                    expected_rows: m.expected_rows,
                    payload,
                });
            }
        }
        out.sort_by(|a, b| (&a.channel, a.sequence).cmp(&(&b.channel, b.sequence)));
        Ok(out)
    }

    /// Manifest-only recovery inventory; callers load one immutable payload
    /// at a time with `get_batch` so restart memory is independent of backlog.
    pub fn pending_ids(&self) -> anyhow::Result<Vec<String>> {
        let _guard = self.mutation.lock().unwrap();
        let mut rows = self
            .manifests()?
            .into_iter()
            .filter(|m| m.phase != BatchPhase::Applied)
            .map(|m| (m.channel, m.sequence, m.id))
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        Ok(rows.into_iter().map(|(_, _, id)| id).collect())
    }

    pub fn get_batch(&self, id: &str) -> anyhow::Result<Option<DurableBatch>> {
        let _guard = self.mutation.lock().unwrap();
        validate_id(id)?;
        let Some(manifest) = self.manifest(id)? else {
            return Ok(None);
        };
        if manifest.phase == BatchPhase::Applied {
            return Ok(None);
        }
        let payload = read_payload(&self.payload_path(id), &manifest)?;
        Ok(Some(DurableBatch {
            id: manifest.id,
            channel: manifest.channel,
            sequence: manifest.sequence,
            expected_rows: manifest.expected_rows,
            payload,
        }))
    }

    /// Read verified batches in channel sequence order within a bounded
    /// payload budget. The first batch may exceed the budget so it can still
    /// make progress; manifests for other channels are never loaded.
    pub fn verified_candidates(
        &self,
        channel: &str,
        min_sequence: u64,
        max_bytes: usize,
    ) -> anyhow::Result<Vec<DurableBatch>> {
        self.verified_candidates_with_fullness(channel, min_sequence, max_bytes)
            .map(|(batches, _)| batches)
    }

    /// Also report when another verified manifest would exceed the budget,
    /// without loading its payload. The apply timer can then fire immediately.
    pub fn verified_candidates_with_fullness(
        &self,
        channel: &str,
        min_sequence: u64,
        max_bytes: usize,
    ) -> anyhow::Result<(Vec<DurableBatch>, bool)> {
        let _guard = self.mutation.lock().unwrap();
        anyhow::ensure!(
            !channel.is_empty() && max_bytes > 0,
            "invalid verified candidate budget"
        );
        let Some(oldest) = self
            .outstanding
            .lock()
            .unwrap()
            .get(channel)
            .and_then(|seqs| seqs.first().copied())
        else {
            return Ok((Vec::new(), false));
        };
        let min_sequence = min_sequence.max(oldest);
        let mut prefix = sequence_key(channel, 0)?;
        prefix.truncate(prefix.len() - 8);
        let mut out = Vec::new();
        let mut total = 0usize;
        for item in self
            .db
            .prefix_iterator(sequence_key(channel, min_sequence)?)
        {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            anyhow::ensure!(
                key.len() == prefix.len() + 8,
                "corrupt channel sequence key"
            );
            let sequence = u64::from_be_bytes(key[prefix.len()..].try_into()?);
            let id = std::str::from_utf8(&value)?;
            let manifest = self
                .manifest(id)?
                .ok_or_else(|| anyhow::anyhow!("channel sequence lacks a batch manifest"))?;
            anyhow::ensure!(
                manifest.channel == channel && manifest.sequence == sequence,
                "channel sequence and batch manifest disagree"
            );
            if manifest.phase != BatchPhase::Verified {
                continue;
            }
            let size = usize::try_from(manifest.payload_len)?;
            if !out.is_empty() && total.checked_add(size).is_none_or(|n| n > max_bytes) {
                return Ok((out, true));
            }
            let payload = read_payload(&self.payload_path(&manifest.id), &manifest)?;
            total = total
                .checked_add(size)
                .ok_or_else(|| anyhow::anyhow!("candidate payload budget overflow"))?;
            out.push(DurableBatch {
                id: manifest.id,
                channel: manifest.channel,
                sequence,
                expected_rows: manifest.expected_rows,
                payload,
            });
            if total >= max_bytes {
                return Ok((out, true));
            }
        }
        Ok((out, false))
    }

    pub fn phase(&self, id: &str) -> anyhow::Result<Option<BatchPhase>> {
        Ok(self.manifest(id)?.map(|m| m.phase))
    }

    pub fn mark_verified(&self, id: &str) -> anyhow::Result<()> {
        self.transition(id, BatchPhase::Queued, BatchPhase::Verified)
    }
    pub fn mark_applied(&self, id: &str) -> anyhow::Result<()> {
        self.transition(id, BatchPhase::Verified, BatchPhase::Applied)
    }

    /// Reserve a durable, monotonically increasing channel sequence. A crash
    /// may leave a gap; callers must never infer delivery from sequence alone.
    pub fn allocate_sequence(&self, channel: &str) -> anyhow::Result<u64> {
        anyhow::ensure!(!channel.is_empty(), "empty batch channel");
        let mut heads = self.heads.lock().unwrap();
        let head = match heads.get(channel) {
            Some(head) => *head,
            None => self.sequence_head(channel)?,
        };
        let next = head
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("channel sequence overflow"))?;
        heads.insert(channel.to_owned(), next);
        Ok(next)
    }

    pub fn get_metadata(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.db.get(metadata_key(key)?)?)
    }

    /// Metadata is immutable once published, including request IDs used for
    /// retries whose remote outcome is uncertain.
    pub fn put_metadata(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let key = metadata_key(key)?;
        if let Some(old) = self.db.get(&key)? {
            anyhow::ensure!(old == bytes, "conflicting durable metadata");
            return Ok(());
        }
        self.put_sync(key, bytes.to_vec())
    }

    pub fn compare_exchange_metadata(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        next: &[u8],
    ) -> anyhow::Result<bool> {
        let _guard = self.mutation.lock().unwrap();
        let key = metadata_key(key)?;
        let current = self.db.get(&key)?;
        if current.as_deref() != expected {
            return Ok(false);
        }
        self.put_sync(key, next.to_vec())?;
        Ok(true)
    }

    pub fn prepare_generation(
        &self,
        operation_id: &str,
        relation_oid: u32,
        snapshot_lsn: u64,
        schema_fingerprint: &str,
    ) -> anyhow::Result<GenerationRecord> {
        let _guard = self.mutation.lock().unwrap();
        let key = generation_key(operation_id)?;
        anyhow::ensure!(
            !schema_fingerprint.is_empty(),
            "empty generation schema fingerprint"
        );
        if let Some(bytes) = self.db.get(&key)? {
            let old: GenerationRecord = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(
                old.relation_oid == relation_oid
                    && old.snapshot_lsn == snapshot_lsn
                    && old.schema_fingerprint == schema_fingerprint
                    && old.operation_id == operation_id,
                "conflicting generation retry"
            );
            return Ok(old);
        }
        let head = read_u64(&self.db, b"generation/head")?;
        let generation_id = head
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("generation id overflow"))?;
        let record = GenerationRecord {
            operation_id: operation_id.into(),
            relation_oid,
            snapshot_lsn,
            schema_fingerprint: schema_fingerprint.into(),
            generation_id,
            phase: GenerationPhase::Prepared,
        };
        let mut wb = WriteBatch::default();
        wb.put(key, serde_json::to_vec(&record)?);
        wb.put(b"generation/head", generation_id.to_le_bytes());
        self.write_sync(wb)?;
        Ok(record)
    }

    /// Whether any initial-load generation is still being written
    pub fn generation_loading(&self) -> anyhow::Result<bool> {
        for item in self.db.prefix_iterator(b"generation/") {
            let (key, value) = item?;
            if !key.starts_with(b"generation/") {
                break;
            }
            if key.as_ref() == b"generation/head" {
                continue;
            }
            let record: GenerationRecord = serde_json::from_slice(&value)?;
            if record.phase == GenerationPhase::Prepared {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn generation(&self, operation_id: &str) -> anyhow::Result<Option<GenerationRecord>> {
        self.db
            .get(generation_key(operation_id)?)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(Into::into))
            .transpose()
    }

    pub fn mark_generation_loaded(&self, id: &str) -> anyhow::Result<()> {
        self.generation_transition(id, GenerationPhase::Prepared, GenerationPhase::Loaded)
    }
    pub fn mark_generation_published(&self, id: &str) -> anyhow::Result<()> {
        self.generation_transition(id, GenerationPhase::Loaded, GenerationPhase::Published)
    }
    pub fn mark_generation_replayed(&self, id: &str) -> anyhow::Result<()> {
        self.generation_transition(id, GenerationPhase::Published, GenerationPhase::Replayed)
    }
    pub fn mark_generation_retired(&self, id: &str) -> anyhow::Result<()> {
        self.generation_transition(id, GenerationPhase::Replayed, GenerationPhase::Retired)
    }

    pub fn abort_generation(&self, id: &str) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let key = generation_key(id)?;
        let bytes = self
            .db
            .get(&key)?
            .ok_or_else(|| anyhow::anyhow!("unknown generation operation {id}"))?;
        let mut record: GenerationRecord = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            matches!(
                record.phase,
                GenerationPhase::Prepared | GenerationPhase::Loaded | GenerationPhase::Aborted
            ),
            "cannot abort a published snapshot generation"
        );
        if record.phase != GenerationPhase::Aborted {
            record.phase = GenerationPhase::Aborted;
            let mut wb = WriteBatch::default();
            wb.put(key, serde_json::to_vec(&record)?);
            self.write_sync(wb)?;
        }
        Ok(())
    }

    fn generation_transition(
        &self,
        id: &str,
        from: GenerationPhase,
        to: GenerationPhase,
    ) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let key = generation_key(id)?;
        let bytes = self
            .db
            .get(&key)?
            .ok_or_else(|| anyhow::anyhow!("unknown generation operation {id}"))?;
        let mut record: GenerationRecord = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            record.phase == from || record.phase == to,
            "unsafe generation phase transition"
        );
        if record.phase == from {
            record.phase = to;
            self.put_sync(key, serde_json::to_vec(&record)?)?;
        }
        Ok(())
    }

    pub fn enqueue_pending(&self, row: &PendingRow) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let key = pending_key(row)?;
        if let Some(bytes) = self.db.get(&key)? {
            let old: StoredPending = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(pending_matches(&old, row), "conflicting pending row retry");
            return Ok(());
        }
        let occupied = self.occupied()?;
        let pending_occupied = read_u64(&self.db, b"pending/occupied")?;
        let new_bytes = row.payload.len() as u64;
        let fits = occupied
            .checked_add(pending_occupied)
            .and_then(|n| n.checked_add(read_u64(&self.db, b"toast/occupied").unwrap_or(u64::MAX)))
            .and_then(|n| n.checked_add(new_bytes))
            .is_some_and(|n| n <= self.max_bytes);
        if !fits && (occupied > 0 || pending_occupied > 0) {
            return Err(BudgetExceeded.into());
        }
        let stored = StoredPending {
            id: row.id.clone(),
            relation_oid: row.relation_oid,
            xmin: row.xmin,
            xmax: row.xmax,
            record_lsn: row.record_lsn,
            payload: row.payload.clone(),
            payload_len: new_bytes,
            payload_crc32c: crc32c::crc32c(&row.payload),
            phase: PendingPhase::Raw,
            receipt: None,
        };
        let mut wb = WriteBatch::default();
        wb.put(key, serde_json::to_vec(&stored)?);
        wb.put(
            b"pending/occupied",
            (pending_occupied + new_bytes).to_le_bytes(),
        );
        self.write_sync(wb)
    }

    pub fn pending_for_relation(&self, relation_oid: u32) -> anyhow::Result<Vec<PendingRecord>> {
        self.pending_matching(relation_oid, |_| true)
    }

    pub fn pending_relation_oids(&self) -> anyhow::Result<Vec<u32>> {
        let mut relations = HashSet::new();
        for item in self.db.prefix_iterator(b"pending/row/") {
            let (key, value) = item?;
            if !key.starts_with(b"pending/row/") {
                break;
            }
            let stored: StoredPending = serde_json::from_slice(&value)?;
            verify_pending(&stored)?;
            if stored.phase != PendingPhase::Retired {
                relations.insert(stored.relation_oid);
            }
        }
        let mut relations: Vec<_> = relations.into_iter().collect();
        relations.sort_unstable();
        Ok(relations)
    }

    pub fn all_pending(&self) -> anyhow::Result<Vec<PendingRecord>> {
        let mut rows = Vec::new();
        for oid in self.pending_relation_oids()? {
            rows.extend(
                self.pending_for_relation(oid)?
                    .into_iter()
                    .filter(|row| row.phase != PendingPhase::Retired),
            );
        }
        Ok(rows)
    }

    pub fn pending_for_xid(
        &self,
        relation_oid: u32,
        xid: u32,
    ) -> anyhow::Result<Vec<PendingRecord>> {
        self.pending_matching(relation_oid, |stored| {
            stored.xmin == xid || stored.xmax == xid
        })
    }

    fn pending_matching(
        &self,
        relation_oid: u32,
        matches: impl Fn(&StoredPending) -> bool,
    ) -> anyhow::Result<Vec<PendingRecord>> {
        let prefix = format!("pending/row/{relation_oid:08x}/");
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(prefix.as_bytes()) {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }
            let stored: StoredPending = serde_json::from_slice(&value)?;
            verify_pending(&stored)?;
            if matches(&stored) {
                out.push(PendingRecord {
                    row: PendingRow {
                        id: stored.id,
                        relation_oid: stored.relation_oid,
                        xmin: stored.xmin,
                        xmax: stored.xmax,
                        record_lsn: stored.record_lsn,
                        payload: stored.payload,
                    },
                    phase: stored.phase,
                    receipt: stored.receipt,
                });
            }
        }
        Ok(out)
    }

    pub fn mark_pending_promoted(&self, row: &PendingRow, receipt: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(!receipt.is_empty(), "pending promotion receipt required");
        self.pending_transition(
            row,
            PendingPhase::Raw,
            PendingPhase::Promoted,
            Some(receipt),
        )
    }
    pub fn retire_pending(&self, row: &PendingRow) -> anyhow::Result<()> {
        self.pending_transition(row, PendingPhase::Promoted, PendingPhase::Retired, None)
    }
    pub fn retire_pending_aborted(&self, row: &PendingRow) -> anyhow::Result<()> {
        self.pending_transition(row, PendingPhase::Raw, PendingPhase::Retired, None)
    }

    fn pending_transition(
        &self,
        row: &PendingRow,
        from: PendingPhase,
        to: PendingPhase,
        receipt: Option<&[u8]>,
    ) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let key = pending_key(row)?;
        let bytes = self
            .db
            .get(&key)?
            .ok_or_else(|| anyhow::anyhow!("unknown pending row"))?;
        let mut old: StoredPending = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            pending_matches(&old, row),
            "conflicting pending row identity"
        );
        anyhow::ensure!(
            old.phase == from || old.phase == to,
            "unsafe pending phase transition"
        );
        if old.phase == to {
            if let Some(receipt) = receipt {
                anyhow::ensure!(
                    old.receipt.as_deref() == Some(receipt),
                    "conflicting pending receipt"
                );
            }
            if to == PendingPhase::Retired {
                anyhow::ensure!(
                    old.receipt.is_some() == (from == PendingPhase::Promoted),
                    "conflicting pending retirement outcome"
                );
            }
            return Ok(());
        }
        if let Some(receipt) = receipt {
            old.receipt = Some(receipt.to_vec());
        }
        old.phase = to;
        let mut wb = WriteBatch::default();
        if to == PendingPhase::Retired {
            let occupied = read_u64(&self.db, b"pending/occupied")?;
            let remaining = occupied
                .checked_sub(old.payload_len)
                .ok_or_else(|| anyhow::anyhow!("pending byte accounting underflow"))?;
            old.payload.clear();
            wb.put(b"pending/occupied", remaining.to_le_bytes());
        }
        wb.put(key, serde_json::to_vec(&old)?);
        self.write_sync(wb)?;
        if to == PendingPhase::Retired {
            self.space.notify_waiters();
        }
        Ok(())
    }

    pub fn toast_store(&self) -> DurableToastStore {
        DurableToastStore {
            max_bytes: self.max_bytes,
            db: self.db.clone(),
            mutation: self.mutation.clone(),
            _lock: self._lock.clone(),
        }
    }

    fn transition(&self, id: &str, from: BatchPhase, to: BatchPhase) -> anyhow::Result<()> {
        let _guard = self.mutation.lock().unwrap();
        let mut m = self
            .manifest(id)?
            .ok_or_else(|| anyhow::anyhow!("unknown batch {id}"))?;
        anyhow::ensure!(
            m.phase == from || m.phase == to,
            "unsafe batch phase transition"
        );
        let path = self.payload_path(id);
        if m.phase != BatchPhase::Applied || path.exists() {
            read_payload(&path, &m)?;
        }
        if m.phase == from {
            m.phase = to;
            if to == BatchPhase::Applied {
                let occupied = self.occupied()?;
                let remaining = occupied
                    .checked_sub(m.payload_len)
                    .ok_or_else(|| anyhow::anyhow!("batch byte accounting underflow"))?;
                let mut wb = WriteBatch::default();
                wb.put(manifest_key(id), serde_json::to_vec(&m)?);
                wb.put(b"occupied", remaining.to_le_bytes());
                self.write_sync(wb)?;
            } else {
                self.put_sync(manifest_key(id), serde_json::to_vec(&m)?)?;
            }
        }
        if to == BatchPhase::Applied {
            if let Some(seqs) = self.outstanding.lock().unwrap().get_mut(&m.channel) {
                seqs.remove(&m.sequence);
            }
            if path.exists() {
                fs::remove_file(&path)?;
                sync_dir(&self.root.join("payloads"))?;
            }
            self.space.notify_waiters();
        }
        Ok(())
    }

    /// Drop applied manifests and their sequence index entries. A manifest
    /// is removed only on the call after the one that first saw it applied,
    /// so an in-flight idempotent re-transition still resolves it; the
    /// channel head survives, keeping later allocations monotonic
    pub fn gc_applied(&self) -> anyhow::Result<usize> {
        let _guard = self.mutation.lock().unwrap();
        let mut grace = self.gc_grace.lock().unwrap();
        let mut seen = HashSet::new();
        let mut wb = WriteBatch::default();
        let mut removed = 0usize;
        for m in self.manifests()? {
            if m.phase != BatchPhase::Applied {
                continue;
            }
            if grace.contains(&m.id) {
                wb.delete(manifest_key(&m.id));
                wb.delete(sequence_key(&m.channel, m.sequence)?);
                removed += 1;
            } else {
                seen.insert(m.id);
            }
        }
        if removed > 0 {
            self.write_sync(wb)?;
        }
        *grace = seen;
        Ok(removed)
    }

    fn manifest(&self, id: &str) -> anyhow::Result<Option<Manifest>> {
        validate_id(id)?;
        self.db
            .get(manifest_key(id))?
            .map(|b| serde_json::from_slice(&b).map_err(Into::into))
            .transpose()
    }

    fn manifests(&self) -> anyhow::Result<Vec<Manifest>> {
        let mut out = Vec::new();
        for item in self.db.prefix_iterator(b"batch/") {
            let (k, v) = item?;
            if !k.starts_with(b"batch/") {
                break;
            }
            let manifest: Manifest = serde_json::from_slice(&v)?;
            anyhow::ensure!(
                k.as_ref() == manifest_key(&manifest.id).as_slice(),
                "batch manifest key mismatch"
            );
            out.push(manifest);
        }
        Ok(out)
    }

    fn verify_and_clean(&self) -> anyhow::Result<()> {
        let mut toast_bytes = 0u64;
        let mut indexed_rows = 0u64;
        for item in self.db.prefix_iterator(b"toast/") {
            let (key, value) = item?;
            if !key.starts_with(b"toast/") {
                break;
            }
            if key.as_ref() == b"toast/occupied" {
                continue;
            }
            // Identity fields only; the chunk bytes are skipped, not decoded
            let row: StoredToastHeader = serde_json::from_slice(&value)?;
            anyhow::ensure!(
                key.as_ref() == row.key().as_slice(),
                "TOAST history key mismatch"
            );
            toast_bytes = toast_bytes
                .checked_add((key.len() + value.len()) as u64)
                .ok_or_else(|| anyhow::anyhow!("TOAST accounting overflow"))?;
            if row.chunk_id != 0 {
                anyhow::ensure!(
                    self.db.get(row.value_index())?.as_deref() == Some(key.as_ref()),
                    "TOAST history index missing or corrupt"
                );
                indexed_rows += 1;
            }
        }
        // Every row above has its own index entry pointing back at it, and an
        // index key encodes its row's identity, so equal counts leave no
        // orphan or foreign entry without re-reading each row.
        let mut index_entries = 0u64;
        for item in self.db.prefix_iterator(b"toast-value/") {
            let (key, value) = item?;
            if !key.starts_with(b"toast-value/") {
                break;
            }
            index_entries += 1;
            toast_bytes = toast_bytes
                .checked_add((key.len() + value.len()) as u64)
                .ok_or_else(|| anyhow::anyhow!("TOAST index accounting overflow"))?;
        }
        anyhow::ensure!(
            index_entries == indexed_rows,
            "TOAST value index references missing or foreign rows"
        );
        anyhow::ensure!(
            toast_bytes == read_u64(&self.db, b"toast/occupied")?,
            "TOAST byte accounting mismatch"
        );
        let manifests = self.manifests()?;
        let mut names = HashSet::new();
        let mut seqs = HashSet::new();
        for m in &manifests {
            validate_id(&m.id)?;
            anyhow::ensure!(
                names.insert(m.id.clone()) && seqs.insert((m.channel.clone(), m.sequence)),
                "duplicate manifest identity or sequence"
            );
            let path = self.payload_path(&m.id);
            if m.phase != BatchPhase::Applied && !path.exists() {
                let temporary = self.root.join("payloads").join(format!("{}.tmp", m.id));
                read_payload(&temporary, m)?;
                fs::rename(temporary, &path)?;
                sync_dir(&self.root.join("payloads"))?;
            }
            if m.phase != BatchPhase::Applied || path.exists() {
                read_payload(&path, m)?;
            }
        }
        let occupied = manifests
            .iter()
            .filter(|m| m.phase != BatchPhase::Applied)
            .try_fold(0u64, |sum, m| sum.checked_add(m.payload_len))
            .ok_or_else(|| anyhow::anyhow!("batch payload accounting overflow"))?;
        anyhow::ensure!(
            occupied
                .checked_add(read_u64(&self.db, b"pending/occupied")?)
                .and_then(
                    |n| n.checked_add(read_u64(&self.db, b"toast/occupied").unwrap_or(u64::MAX))
                )
                .is_some_and(|n| n <= self.max_bytes),
            "existing Snowflake state exceeds byte budget"
        );
        let mut pending_bytes = 0u64;
        for item in self.db.prefix_iterator(b"pending/row/") {
            let (key, value) = item?;
            if !key.starts_with(b"pending/row/") {
                break;
            }
            let row: StoredPending = serde_json::from_slice(&value)?;
            anyhow::ensure!(
                key.as_ref()
                    == pending_key_parts(row.relation_oid, row.xmin, row.xmax, &row.id)?.as_slice(),
                "pending row key mismatch"
            );
            verify_pending(&row)?;
            if row.phase != PendingPhase::Retired {
                pending_bytes = pending_bytes
                    .checked_add(row.payload_len)
                    .ok_or_else(|| anyhow::anyhow!("pending byte accounting overflow"))?;
            }
        }
        anyhow::ensure!(
            pending_bytes == read_u64(&self.db, b"pending/occupied")?,
            "pending byte accounting mismatch"
        );
        match self.db.get(b"occupied")? {
            Some(bytes) => anyhow::ensure!(
                bytes.as_slice() == occupied.to_le_bytes().as_slice(),
                "batch byte accounting mismatch"
            ),
            None => {
                anyhow::ensure!(manifests.is_empty(), "batch byte accounting missing");
                self.put_sync(b"occupied", 0u64.to_le_bytes().to_vec())?;
            }
        }
        for m in &manifests {
            let key = sequence_key(&m.channel, m.sequence)?;
            anyhow::ensure!(
                self.db.get(key)?.as_deref() == Some(m.id.as_bytes()),
                "batch sequence index missing or corrupt"
            );
            anyhow::ensure!(
                self.sequence_head(&m.channel)? >= m.sequence,
                "channel sequence head behind manifest"
            );
        }
        let generation_head = read_u64(&self.db, b"generation/head")?;
        let mut generation_ids = HashSet::new();
        for item in self.db.prefix_iterator(b"generation/") {
            let (key, value) = item?;
            if !key.starts_with(b"generation/") {
                break;
            }
            if key.as_ref() == b"generation/head" {
                continue;
            }
            let record: GenerationRecord = serde_json::from_slice(&value)?;
            anyhow::ensure!(
                key.as_ref() == generation_key(&record.operation_id)?.as_slice()
                    && record.generation_id > 0
                    && record.generation_id <= generation_head
                    && generation_ids.insert(record.generation_id),
                "generation journal corrupt"
            );
        }
        // A final payload without a manifest is an enqueue that crashed
        // before its commit point; nothing was sent or acknowledged for it
        for entry in fs::read_dir(self.root.join("payloads"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let id = name
                .strip_suffix(".tmp")
                .or_else(|| name.strip_suffix(".bin").filter(|id| !names.contains(*id)));
            if let Some(id) = id
                && validate_id(id).is_ok()
                && entry.file_type()?.is_file()
            {
                fs::remove_file(entry.path())?;
            }
        }
        let mut outstanding = self.outstanding.lock().unwrap();
        for m in manifests.iter().filter(|m| m.phase != BatchPhase::Applied) {
            outstanding
                .entry(m.channel.clone())
                .or_default()
                .insert(m.sequence);
        }
        drop(outstanding);
        for m in &manifests {
            if m.phase == BatchPhase::Applied {
                let path = self.payload_path(&m.id);
                if path.exists() {
                    fs::remove_file(path)?;
                }
            }
        }
        sync_dir(&self.root.join("payloads"))?;
        Ok(())
    }

    fn payload_path(&self, id: &str) -> PathBuf {
        self.root.join("payloads").join(format!("{id}.bin"))
    }
    fn put_sync(&self, key: impl AsRef<[u8]>, value: Vec<u8>) -> anyhow::Result<()> {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.put_opt(key, value, &opts)?;
        Ok(())
    }
    fn write_sync(&self, batch: WriteBatch) -> anyhow::Result<()> {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.write_opt(batch, &opts)?;
        Ok(())
    }
    fn occupied(&self) -> anyhow::Result<u64> {
        let bytes = self
            .db
            .get(b"occupied")?
            .ok_or_else(|| anyhow::anyhow!("batch byte accounting missing"))?;
        anyhow::ensure!(bytes.len() == 8, "batch byte accounting corrupt");
        Ok(u64::from_le_bytes(bytes.as_slice().try_into()?))
    }
    fn sequence_head(&self, channel: &str) -> anyhow::Result<u64> {
        match self.db.get(sequence_head_key(channel)?)? {
            None => Ok(0),
            Some(bytes) => {
                anyhow::ensure!(bytes.len() == 8, "channel sequence head corrupt");
                Ok(u64::from_le_bytes(bytes.as_slice().try_into()?))
            }
        }
    }
}

fn manifest_key(id: &str) -> Vec<u8> {
    format!("batch/{id}").into_bytes()
}
fn sequence_key(channel: &str, sequence: u64) -> anyhow::Result<Vec<u8>> {
    let len = u32::try_from(channel.len())?;
    let mut key = b"sequence/".to_vec();
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(channel.as_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    Ok(key)
}
fn sequence_head_key(channel: &str) -> anyhow::Result<Vec<u8>> {
    let len = u32::try_from(channel.len())?;
    let mut key = b"head/".to_vec();
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(channel.as_bytes());
    Ok(key)
}
fn metadata_key(key: &str) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(
        !key.is_empty()
            && key.len() <= 256
            && key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.')),
        "invalid metadata key"
    );
    Ok(format!("metadata/{key}").into_bytes())
}
fn generation_key(id: &str) -> anyhow::Result<Vec<u8>> {
    validate_id(id)?;
    Ok(format!("generation/{id}").into_bytes())
}
fn pending_key(row: &PendingRow) -> anyhow::Result<Vec<u8>> {
    pending_key_parts(row.relation_oid, row.xmin, row.xmax, &row.id)
}
fn pending_key_parts(relation_oid: u32, xmin: u32, xmax: u32, id: &str) -> anyhow::Result<Vec<u8>> {
    validate_id(id)?;
    Ok(format!("pending/row/{relation_oid:08x}/{xmin:08x}/{xmax:08x}/{id}").into_bytes())
}
fn read_u64(db: &DB, key: &[u8]) -> anyhow::Result<u64> {
    let Some(bytes) = db.get(key)? else {
        return Ok(0);
    };
    anyhow::ensure!(bytes.len() == 8, "durable counter corrupt");
    Ok(u64::from_le_bytes(bytes.as_slice().try_into()?))
}
fn pending_matches(old: &StoredPending, row: &PendingRow) -> bool {
    old.id == row.id
        && old.relation_oid == row.relation_oid
        && old.xmin == row.xmin
        && old.xmax == row.xmax
        && old.record_lsn == row.record_lsn
        && old.payload_len == row.payload.len() as u64
        && old.payload_crc32c == crc32c::crc32c(&row.payload)
        && (old.phase == PendingPhase::Retired || old.payload == row.payload)
}
fn verify_pending(row: &StoredPending) -> anyhow::Result<()> {
    validate_id(&row.id)?;
    if row.phase == PendingPhase::Retired {
        anyhow::ensure!(
            row.payload.is_empty(),
            "retired pending row retains payload"
        );
    } else {
        anyhow::ensure!(
            row.payload.len() as u64 == row.payload_len
                && crc32c::crc32c(&row.payload) == row.payload_crc32c,
            "pending payload corrupt"
        );
    }
    anyhow::ensure!(
        row.phase != PendingPhase::Raw || row.receipt.is_none(),
        "raw pending row has receipt"
    );
    anyhow::ensure!(
        row.phase != PendingPhase::Promoted
            || row.receipt.as_deref().is_some_and(|r| !r.is_empty()),
        "promoted pending row lacks receipt"
    );
    Ok(())
}
fn validate_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "invalid batch id"
    );
    Ok(())
}
fn sync_dir(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}
fn read_payload(path: &Path, m: &Manifest) -> anyhow::Result<Vec<u8>> {
    let mut f = File::open(path)?;
    anyhow::ensure!(
        f.metadata()?.len() == m.payload_len,
        "batch payload size mismatch: {}",
        m.id
    );
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;
    anyhow::ensure!(
        crc32c::crc32c(&data) == m.payload_crc32c,
        "batch payload checksum mismatch: {}",
        m.id
    );
    Ok(data)
}

#[derive(Clone)]
pub struct DurableToastStore {
    max_bytes: u64,
    db: Arc<DB>,
    mutation: Arc<Mutex<()>>,
    _lock: Arc<File>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct StoredToastRow {
    toast_relid: u32,
    blkno: u32,
    offnum: u16,
    chunk_id: u32,
    chunk_seq: u32,
    chunk_data: Vec<u8>,
    lsn: u64,
}
impl From<&ToastRow> for StoredToastRow {
    fn from(r: &ToastRow) -> Self {
        Self {
            toast_relid: r.toast_relid,
            blkno: r.blkno,
            offnum: r.offnum,
            chunk_id: r.chunk_id,
            chunk_seq: r.chunk_seq,
            chunk_data: r.chunk_data.to_vec(),
            lsn: r.lsn,
        }
    }
}
fn toast_prefix(relid: u32) -> Vec<u8> {
    format!("toast/{relid:08x}/").into_bytes()
}
/// A stored TOAST row without its chunk bytes, for integrity scans.
#[derive(Deserialize)]
struct StoredToastHeader {
    toast_relid: u32,
    blkno: u32,
    offnum: u16,
    chunk_id: u32,
    chunk_seq: u32,
    lsn: u64,
}
impl StoredToastHeader {
    fn key(&self) -> Vec<u8> {
        format!(
            "toast/{:08x}/{:08x}/{:04x}/{:016x}",
            self.toast_relid, self.blkno, self.offnum, self.lsn
        )
        .into_bytes()
    }
    fn value_index(&self) -> Vec<u8> {
        format!(
            "toast-value/{:08x}/{:08x}/{:08x}/{:08x}/{:04x}/{:016x}",
            self.toast_relid, self.chunk_id, self.chunk_seq, self.blkno, self.offnum, self.lsn
        )
        .into_bytes()
    }
}
fn toast_key(r: &StoredToastRow) -> Vec<u8> {
    format!(
        "toast/{:08x}/{:08x}/{:04x}/{:016x}",
        r.toast_relid, r.blkno, r.offnum, r.lsn
    )
    .into_bytes()
}
fn toast_error(e: impl std::fmt::Display) -> ChunkStoreError {
    ChunkStoreError::Clickhouse(format!("durable toast: {e}"))
}
impl DurableToastStore {
    fn write_rows(
        &self,
        rows: impl Iterator<Item = StoredToastRow>,
    ) -> Result<(), ChunkStoreError> {
        let mut batch = WriteBatch::default();
        let mut within = HashMap::<Vec<u8>, Vec<u8>>::new();
        let mut added = 0u64;
        for row in rows {
            let key = toast_key(&row);
            let value = serde_json::to_vec(&row).map_err(toast_error)?;
            if let Some(old) = within.get(&key) {
                if old != &value {
                    return Err(toast_error("conflicting rows in one put"));
                }
                continue;
            }
            within.insert(key.clone(), value.clone());
            if let Some(old) = self.db.get(&key).map_err(toast_error)? {
                if old != value {
                    return Err(toast_error("conflicting row at equal TID and LSN"));
                }
                continue;
            }
            added = added
                .checked_add((key.len() + value.len()) as u64)
                .ok_or_else(|| toast_error("TOAST byte accounting overflow"))?;
            if row.chunk_id != 0 {
                let index = format!(
                    "toast-value/{:08x}/{:08x}/{:08x}/{:08x}/{:04x}/{:016x}",
                    row.toast_relid, row.chunk_id, row.chunk_seq, row.blkno, row.offnum, row.lsn
                );
                added = added
                    .checked_add((index.len() + key.len()) as u64)
                    .ok_or_else(|| toast_error("TOAST index byte accounting overflow"))?;
                batch.put(index.as_bytes(), &key);
            }
            batch.put(format!("mirror/{:08x}", row.toast_relid), b"1");
            batch.put(key, value);
        }
        let prior = read_u64(&self.db, b"toast/occupied").map_err(toast_error)?;
        let next = prior
            .checked_add(added)
            .ok_or_else(|| toast_error("TOAST byte accounting overflow"))?;
        // History is bounded on its own: outbound batches drain and wait for
        // space, but a TOAST write that waited on them could deadlock the
        // pipeline that releases it. Garbage collection keeps history small
        if next > self.max_bytes {
            return Err(toast_error(
                "Snowflake TOAST history exceeds the state byte budget",
            ));
        }
        batch.put(b"toast/occupied", next.to_le_bytes());
        self.write(batch)
    }

    /// Remove history no read at or above `floor` can observe. Every later
    /// fetch passes `max_lsn >= floor`, so per TID only the newest version at
    /// or below `floor` stays visible, and none if that version is a
    /// tombstone. Rows below a TRUNCATE at or below `floor` are invisible to
    /// all such reads, as are older TRUNCATE markers. Writes arriving while
    /// this runs are above `floor`, so decisions from a snapshot stay valid.
    /// Returns the rows removed
    pub fn gc_below(&self, floor: u64) -> Result<u64, ChunkStoreError> {
        use std::collections::BTreeMap as Map;
        // relid -> newest TRUNCATE at or below floor, plus markers under it
        let mut truncate_floor = Map::<u32, u64>::new();
        let mut dead_markers = Vec::new();
        for item in self.db.prefix_iterator(b"toast-truncate/") {
            let (key, value) = item.map_err(toast_error)?;
            if !key.starts_with(b"toast-truncate/") {
                break;
            }
            let lsn = u64::from_le_bytes(value.as_ref().try_into().map_err(toast_error)?);
            let relid = std::str::from_utf8(&key[15..23])
                .ok()
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .ok_or_else(|| toast_error("corrupt TOAST truncate key"))?;
            if lsn <= floor
                && let Some(prev) = truncate_floor.insert(relid, lsn)
            {
                dead_markers.push(format!("toast-truncate/{relid:08x}/{prev:016x}"));
            }
        }
        // (row key, value-index key, accounted bytes)
        type Deletion = (Vec<u8>, Option<Vec<u8>>, u64);
        type Version = (Vec<u8>, StoredToastHeader, u64);
        let mut deletions: Vec<Deletion> = Vec::new();
        let mut group: Vec<Version> = Vec::new();
        let flush = |group: &mut Vec<Version>, out: &mut Vec<Deletion>| {
            // Keys sort by lsn within one TID
            let visible = group.iter().rposition(|(_, h, _)| h.lsn <= floor);
            for (i, (key, h, size)) in group.drain(..).enumerate() {
                let truncated = truncate_floor
                    .get(&h.toast_relid)
                    .is_some_and(|&t| h.lsn < t);
                let superseded = visible.is_some_and(|v| i < v);
                let dead_tomb = visible == Some(i) && h.chunk_id == 0;
                if truncated || superseded || dead_tomb {
                    let index = (h.chunk_id != 0).then(|| h.value_index());
                    out.push((key, index, size));
                }
            }
        };
        for item in self.db.prefix_iterator(b"toast/") {
            let (key, value) = item.map_err(toast_error)?;
            if !key.starts_with(b"toast/") {
                break;
            }
            if key.as_ref() == b"toast/occupied" {
                continue;
            }
            let header: StoredToastHeader = serde_json::from_slice(&value).map_err(toast_error)?;
            let tid = (header.toast_relid, header.blkno, header.offnum);
            if group
                .last()
                .is_some_and(|(_, h, _)| (h.toast_relid, h.blkno, h.offnum) != tid)
            {
                flush(&mut group, &mut deletions);
            }
            let mut size = (key.len() + value.len()) as u64;
            if header.chunk_id != 0 {
                size += (header.value_index().len() + key.len()) as u64;
            }
            group.push((key.to_vec(), header, size));
        }
        flush(&mut group, &mut deletions);
        if deletions.is_empty() && dead_markers.is_empty() {
            return Ok(0);
        }
        let removed = deletions.len() as u64;
        // Bounded write batches keep the owner lock hold short
        for chunk in deletions.chunks(4096) {
            let _guard = self.mutation.lock().unwrap();
            let mut batch = WriteBatch::default();
            let mut bytes = 0u64;
            for (key, index, size) in chunk {
                batch.delete(key);
                if let Some(index) = index {
                    batch.delete(index);
                }
                bytes += size;
            }
            let prior = read_u64(&self.db, b"toast/occupied").map_err(toast_error)?;
            let remaining = prior
                .checked_sub(bytes)
                .ok_or_else(|| toast_error("TOAST byte accounting underflow"))?;
            batch.put(b"toast/occupied", remaining.to_le_bytes());
            self.write(batch)?;
        }
        if !dead_markers.is_empty() {
            let _guard = self.mutation.lock().unwrap();
            let mut batch = WriteBatch::default();
            for key in dead_markers {
                batch.delete(key);
            }
            self.write(batch)?;
        }
        Ok(removed)
    }

    fn rows(&self, relid: u32) -> Result<Option<Vec<StoredToastRow>>, ChunkStoreError> {
        let marker = format!("mirror/{relid:08x}");
        if self.db.get(marker).map_err(toast_error)?.is_none() {
            return Ok(None);
        }
        let prefix = toast_prefix(relid);
        let mut rows = Vec::new();
        for item in self.db.prefix_iterator(&prefix) {
            let (k, v) = item.map_err(toast_error)?;
            if !k.starts_with(&prefix) {
                break;
            }
            rows.push(serde_json::from_slice(&v).map_err(toast_error)?);
        }
        Ok(Some(rows))
    }
    fn write(&self, batch: WriteBatch) -> Result<(), ChunkStoreError> {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        self.db.write_opt(batch, &opts).map_err(toast_error)
    }
}

#[async_trait]
impl ChunkStore for DurableToastStore {
    async fn put(&self, rows: &[ToastRow]) -> Result<(), ChunkStoreError> {
        let _guard = self.mutation.lock().unwrap();
        self.write_rows(rows.iter().map(StoredToastRow::from))
    }

    async fn fetch(
        &self,
        relid: u32,
        value_id: u32,
        max_lsn: u64,
        expected_size: usize,
    ) -> Result<FetchedValue, ChunkStoreError> {
        Ok(self
            .fetch_many(relid, &[(value_id, expected_size)], max_lsn)
            .await?
            .remove(0))
    }
    async fn fetch_many(
        &self,
        relid: u32,
        values: &[(u32, usize)],
        max_lsn: u64,
    ) -> Result<Vec<FetchedValue>, ChunkStoreError> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        if self
            .db
            .get(format!("mirror/{relid:08x}"))
            .map_err(toast_error)?
            .is_none()
        {
            return Err(ChunkStoreError::MissingMirror(relid));
        }
        let boundary_prefix = format!("toast-truncate/{relid:08x}/");
        let upper = format!("{boundary_prefix}{max_lsn:016x}");
        let floor = self
            .db
            .iterator(IteratorMode::From(upper.as_bytes(), Direction::Reverse))
            .next()
            .transpose()
            .map_err(toast_error)?
            .filter(|(key, _)| key.starts_with(boundary_prefix.as_bytes()))
            .map(|(_, value)| {
                value
                    .as_ref()
                    .try_into()
                    .map(u64::from_le_bytes)
                    .map_err(toast_error)
            })
            .transpose()?
            .unwrap_or(0);
        // Read only histories for requested values. Validate each candidate
        // against its physical TID as of max_lsn so reuse/deletes cannot revive it.
        let mut latest = HashMap::<(u32, u16), StoredToastRow>::new();
        for (value_id, _) in values {
            let prefix = format!("toast-value/{relid:08x}/{value_id:08x}/").into_bytes();
            for item in self.db.prefix_iterator(&prefix) {
                let (key, row_key) = item.map_err(toast_error)?;
                if !key.starts_with(&prefix) {
                    break;
                }
                let bytes = self
                    .db
                    .get(&row_key)
                    .map_err(toast_error)?
                    .ok_or_else(|| toast_error("TOAST index references missing row"))?;
                let row: StoredToastRow = serde_json::from_slice(&bytes).map_err(toast_error)?;
                if row.lsn > max_lsn || row.lsn < floor {
                    continue;
                }
                let tid_prefix = format!("toast/{relid:08x}/{:08x}/{:04x}/", row.blkno, row.offnum);
                let upper = format!("{tid_prefix}{max_lsn:016x}");
                let mut at = self
                    .db
                    .iterator(IteratorMode::From(upper.as_bytes(), Direction::Reverse));
                if let Some(item) = at.next() {
                    let (key, value) = item.map_err(toast_error)?;
                    if key.starts_with(tid_prefix.as_bytes()) {
                        let current: StoredToastRow =
                            serde_json::from_slice(&value).map_err(toast_error)?;
                        latest.insert((current.blkno, current.offnum), current);
                    }
                }
            }
        }
        let mut newest = HashMap::<u32, BTreeMap<u32, (u64, Vec<u8>)>>::new();
        for row in latest.into_values().filter(|r| r.chunk_id != 0) {
            let slot = newest
                .entry(row.chunk_id)
                .or_default()
                .entry(row.chunk_seq)
                .or_insert((row.lsn, row.chunk_data.clone()));
            if row.lsn == slot.0 && row.chunk_data != slot.1 {
                return Err(toast_error("ambiguous equal-LSN chunk sequence"));
            }
            if row.lsn > slot.0 {
                *slot = (row.lsn, row.chunk_data);
            }
        }
        values
            .iter()
            .map(|(id, size)| {
                let mut asm = ChunkAssembler::new(*size);
                if let Some(chunks) = newest.get(id) {
                    for (&seq, (_, body)) in chunks {
                        asm.push(seq, body).map_err(toast_error)?;
                    }
                }
                Ok(asm.finish())
            })
            .collect()
    }

    async fn truncate_mirror(&self, _relid: u32) -> Result<(), ChunkStoreError> {
        Err(toast_error(
            "truncate lacks a generation/LSN boundary; refusing to erase history",
        ))
    }
    async fn truncate_at(&self, relid: u32, record_lsn: u64) -> Result<(), ChunkStoreError> {
        let _guard = self.mutation.lock().unwrap();
        if record_lsn == 0 {
            return Err(toast_error("truncate record LSN must be positive"));
        }
        let mut batch = WriteBatch::default();
        batch.put(
            format!("toast-truncate/{relid:08x}/{record_lsn:016x}"),
            record_lsn.to_le_bytes(),
        );
        batch.put(format!("mirror/{relid:08x}"), b"1");
        self.write(batch)
    }
    async fn retire_at(&self, relid: u32, commit_lsn: u64) -> Result<(), ChunkStoreError> {
        let _guard = self.mutation.lock().unwrap();
        let Some(rows) = self.rows(relid)? else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        let mut removed = 0u64;
        for row in rows.into_iter().filter(|row| row.lsn < commit_lsn) {
            let key = toast_key(&row);
            let value = serde_json::to_vec(&row).map_err(toast_error)?;
            removed += (key.len() + value.len()) as u64;
            if row.chunk_id != 0 {
                let index = format!(
                    "toast-value/{:08x}/{:08x}/{:08x}/{:08x}/{:04x}/{:016x}",
                    row.toast_relid, row.chunk_id, row.chunk_seq, row.blkno, row.offnum, row.lsn
                );
                removed += (index.len() + key.len()) as u64;
                batch.delete(index);
            }
            batch.delete(key);
        }
        let bytes = read_u64(&self.db, b"toast/occupied").map_err(toast_error)?;
        let remaining = bytes
            .checked_sub(removed)
            .ok_or_else(|| toast_error("TOAST byte accounting underflow"))?;
        batch.put(b"toast/occupied", remaining.to_le_bytes());
        self.write(batch)
    }
    async fn rewrite_barrier(
        &self,
        relid: u32,
        marker_lsn: u64,
        commit_lsn: u64,
    ) -> Result<(), ChunkStoreError> {
        let _guard = self.mutation.lock().unwrap();
        if commit_lsn <= marker_lsn {
            return Err(toast_error("rewrite commit must follow marker"));
        }
        let Some(rows) = self.rows(relid)? else {
            return Ok(());
        };
        let mut below = HashMap::<(u32, u16), StoredToastRow>::new();
        let mut past = HashSet::new();
        for row in rows {
            let tid = (row.blkno, row.offnum);
            if row.lsn > marker_lsn {
                past.insert(tid);
            } else if below.get(&tid).is_none_or(|old| old.lsn < row.lsn) {
                below.insert(tid, row);
            }
        }
        let mut tombstones = Vec::new();
        for (tid, row) in below {
            if row.chunk_id == 0 || past.contains(&tid) {
                continue;
            }
            let tomb = StoredToastRow {
                toast_relid: relid,
                blkno: tid.0,
                offnum: tid.1,
                chunk_id: 0,
                chunk_seq: 0,
                chunk_data: Vec::new(),
                lsn: commit_lsn,
            };
            tombstones.push(tomb);
        }
        self.write_rows(tombstones.into_iter())
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod state_tests;
