//! Append-only audit log of every control-plane mutation
//!
//! JSON lines under `<state-dir>/audit/audit.jsonl` (0600, dir 0700). A
//! mutation that runs writes an `attempt` record, fsynced, before it
//! changes anything, then a `result` record; one refused before it ran
//! writes a single `result`. Each record carries the hash of the one before
//! it (`prev_hash`) and its own (`hash`, SHA-256 over `prev_hash` and the
//! record with `hash` empty), so an edited or dropped record breaks the
//! chain. Files rotate by size; the chain carries across them. Each record
//! is also emitted on the `walshadow::audit` tracing target.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const FILE: &str = "audit.jsonl";
const VERSION: u32 = 1;
const GENESIS: &str = "sha256:0";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    /// `api_key`, `unix` or `external`
    pub kind: String,
    /// API key id; for `external` the source (`sighup`, `sql_registry`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
}

impl Actor {
    pub fn api_key(id: &str, ip: Option<String>) -> Self {
        Self {
            kind: "api_key".into(),
            id: Some(id.into()),
            ip,
            uid: None,
        }
    }

    pub fn unix(uid: Option<u32>) -> Self {
        Self {
            kind: "unix".into(),
            id: None,
            ip: None,
            uid,
        }
    }

    pub fn external(source: &str) -> Self {
        Self {
            kind: "external".into(),
            id: Some(source.into()),
            ip: None,
            uid: None,
        }
    }
}

/// What a caller fills in; [`AuditLog::append`] numbers, stamps and chains it
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Draft {
    pub request_id: String,
    /// `attempt` or `result`
    pub phase: String,
    pub actor: Actor,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default)]
    pub before: Value,
    #[serde(default)]
    pub after: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
    /// `applied`, `rejected`, `rolled_back`, `conflict`, `failed`, `denied`
    /// or `unknown`; empty on an attempt
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub v: u32,
    pub seq: u64,
    /// RFC 3339, UTC
    pub ts: String,
    #[serde(flatten)]
    pub draft: Draft,
    pub prev_hash: String,
    pub hash: String,
}

impl Record {
    fn digest(&self) -> String {
        let mut unhashed = self.clone();
        unhashed.hash = String::new();
        let body = serde_json::to_string(&unhashed).expect("serializable record");
        let mut h = Sha256::new();
        h.update(self.prev_hash.as_bytes());
        h.update(body.as_bytes());
        format!("sha256:{}", hex::encode(h.finalize()))
    }
}

struct Tail {
    seq: u64,
    prev_hash: String,
    size: u64,
}

pub struct AuditLog {
    dir: PathBuf,
    max_bytes: u64,
    keep: usize,
    tail: Mutex<Tail>,
}

/// Records a read returns, and whether their chain holds
#[derive(Debug, Serialize)]
pub struct Page {
    pub records: Vec<Record>,
    pub chain_ok: bool,
}

#[derive(Debug, Default, Clone)]
pub struct Filter {
    pub since_seq: Option<u64>,
    pub tenant: Option<String>,
    pub action: Option<String>,
    pub limit: usize,
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

impl AuditLog {
    /// Open `<state_dir>/audit`, recovering from a crash: a torn last line
    /// is cut off, and an attempt left without a result gets one with
    /// outcome `unknown`
    pub fn open(state_dir: &Path, max_bytes: u64, keep: usize) -> Result<Arc<Self>> {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
        let dir = state_dir.join("audit");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .with_context(|| format!("create {}", dir.display()))?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        let path = dir.join(FILE);
        // Records of the newest file, then the rotated ones for the chain
        let mut records = Vec::new();
        let mut good_len = 0u64;
        if let Ok(f) = std::fs::File::open(&path) {
            let mut reader = std::io::BufReader::new(f);
            let mut line = String::new();
            loop {
                line.clear();
                let n = reader.read_line(&mut line)?;
                if n == 0 {
                    break;
                }
                match serde_json::from_str::<Record>(line.trim_end()) {
                    Ok(r) if line.ends_with('\n') => {
                        good_len += n as u64;
                        records.push(r);
                    }
                    _ => {
                        tracing::warn!(
                            target: "walshadow::audit",
                            file = %path.display(),
                            "audit log ends in a torn record; cutting it off",
                        );
                        break;
                    }
                }
            }
        }
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        f.set_len(good_len)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        f.sync_all()?;
        let last = match records.last() {
            Some(r) => Some(r.clone()),
            None => Self::last_rotated(&dir)?,
        };
        let log = Arc::new(Self {
            dir,
            max_bytes,
            keep: keep.max(1),
            tail: Mutex::new(Tail {
                seq: last.as_ref().map_or(0, |r| r.seq),
                prev_hash: last.map_or_else(|| GENESIS.to_string(), |r| r.hash),
                size: good_len,
            }),
        });
        // An attempt with no result: the process died between them
        let resulted: std::collections::HashSet<&str> = records
            .iter()
            .filter(|r| r.draft.phase == "result")
            .map(|r| r.draft.request_id.as_str())
            .collect();
        let orphans: Vec<Draft> = records
            .iter()
            .filter(|r| {
                r.draft.phase == "attempt" && !resulted.contains(r.draft.request_id.as_str())
            })
            .map(|r| Draft {
                phase: "result".into(),
                outcome: "unknown".into(),
                error: Some("the process stopped before the mutation reported its outcome".into()),
                before: Value::Null,
                after: Value::Null,
                ..r.draft.clone()
            })
            .collect();
        for draft in orphans {
            log.append_blocking(draft)?;
        }
        Ok(log)
    }

    fn last_rotated(dir: &Path) -> Result<Option<Record>> {
        let path = dir.join(format!("{FILE}.1"));
        let Ok(text) = std::fs::read_to_string(&path) else {
            return Ok(None);
        };
        Ok(text
            .lines()
            .rev()
            .find_map(|l| serde_json::from_str::<Record>(l).ok()))
    }

    /// Number, stamp, chain, write and fsync one record. A failure means it
    /// is not durable, and the caller must not mutate
    pub async fn append(self: &Arc<Self>, draft: Draft) -> Result<Record> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.append_blocking(draft))
            .await
            .context("audit write task")?
    }

    fn append_blocking(&self, draft: Draft) -> Result<Record> {
        let mut tail = self.tail.lock().unwrap_or_else(PoisonError::into_inner);
        let mut record = Record {
            v: VERSION,
            seq: tail.seq + 1,
            ts: now_rfc3339(),
            draft,
            prev_hash: tail.prev_hash.clone(),
            hash: String::new(),
        };
        record.hash = record.digest();
        let mut line = serde_json::to_string(&record)?;
        line.push('\n');
        if tail.size > 0 && tail.size + line.len() as u64 > self.max_bytes {
            self.rotate()?;
            tail.size = 0;
        }
        let path = self.dir.join(FILE);
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(&path)
                .with_context(|| format!("open {}", path.display()))?
        };
        f.write_all(line.as_bytes())
            .with_context(|| format!("write {}", path.display()))?;
        f.sync_all()
            .with_context(|| format!("fsync {}", path.display()))?;
        tail.seq = record.seq;
        tail.prev_hash.clone_from(&record.hash);
        tail.size += line.len() as u64;
        drop(tail);
        tracing::info!(
            target: "walshadow::audit",
            seq = record.seq,
            action = %record.draft.action,
            phase = %record.draft.phase,
            outcome = %record.draft.outcome,
            tenant = record.draft.tenant.as_deref().unwrap_or(""),
            actor = %record.draft.actor.id.as_deref().unwrap_or(&record.draft.actor.kind),
            "audit",
        );
        Ok(record)
    }

    /// `audit.jsonl` → `.1` → … → `.<keep>`, the oldest dropped
    fn rotate(&self) -> Result<()> {
        let name = |n: usize| self.dir.join(format!("{FILE}.{n}"));
        let _ = std::fs::remove_file(name(self.keep));
        for n in (1..self.keep).rev() {
            if name(n).exists() {
                std::fs::rename(name(n), name(n + 1))?;
            }
        }
        std::fs::rename(self.dir.join(FILE), name(1))?;
        crate::fs::sync_dir_blocking(&self.dir)?;
        Ok(())
    }

    /// Matching records, oldest first, from every retained file, and
    /// whether the chain over everything retained holds
    pub fn read(&self, filter: &Filter) -> Result<Page> {
        let mut files: Vec<PathBuf> = (1..=self.keep)
            .rev()
            .map(|n| self.dir.join(format!("{FILE}.{n}")))
            .filter(|p| p.exists())
            .collect();
        files.push(self.dir.join(FILE));
        let mut all = Vec::new();
        for path in files {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            for line in text.lines() {
                all.push(
                    serde_json::from_str::<Record>(line)
                        .with_context(|| format!("parse {}", path.display()))?,
                );
            }
        }
        let chain_ok = all
            .windows(2)
            .all(|w| w[1].prev_hash == w[0].hash && w[1].seq == w[0].seq + 1)
            && all.iter().all(|r| r.hash == r.digest());
        let limit = if filter.limit == 0 {
            100
        } else {
            filter.limit.min(1000)
        };
        let records = all
            .into_iter()
            .filter(|r| filter.since_seq.is_none_or(|s| r.seq > s))
            .filter(|r| {
                filter
                    .tenant
                    .as_deref()
                    .is_none_or(|t| r.draft.tenant.as_deref() == Some(t))
            })
            .filter(|r| filter.action.as_deref().is_none_or(|a| r.draft.action == a))
            .take(limit)
            .collect();
        Ok(Page { records, chain_ok })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(request_id: &str, phase: &str, outcome: &str) -> Draft {
        Draft {
            request_id: request_id.into(),
            phase: phase.into(),
            actor: Actor::api_key("ops", Some("10.0.0.5".into())),
            action: "tenant.update".into(),
            tenant: Some("a".into()),
            outcome: outcome.into(),
            ..Draft::default()
        }
    }

    #[tokio::test]
    async fn records_chain_and_survive_reopen() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        let a = log.append(draft("r1", "attempt", "")).await.unwrap();
        let b = log.append(draft("r1", "result", "applied")).await.unwrap();
        assert_eq!((a.seq, b.seq), (1, 2));
        assert_eq!(a.prev_hash, GENESIS);
        assert_eq!(b.prev_hash, a.hash);
        assert!(b.ts.ends_with('Z'), "{}", b.ts);
        let path = dir.path().join("audit").join(FILE);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        drop(log);
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        let c = log.append(draft("r2", "result", "rejected")).await.unwrap();
        assert_eq!(c.seq, 3);
        assert_eq!(c.prev_hash, b.hash);
        let page = log.read(&Filter::default()).unwrap();
        assert!(page.chain_ok);
        assert_eq!(page.records.len(), 3);
        let page = log
            .read(&Filter {
                since_seq: Some(1),
                limit: 1,
                ..Filter::default()
            })
            .unwrap();
        assert_eq!(page.records[0].seq, 2);
    }

    #[tokio::test]
    async fn tampering_breaks_the_chain() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        for i in 0..3 {
            log.append(draft(&format!("r{i}"), "result", "applied"))
                .await
                .unwrap();
        }
        let path = dir.path().join("audit").join(FILE);
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, text.replace("\"tenant\":\"a\"", "\"tenant\":\"b\"")).unwrap();
        assert!(!log.read(&Filter::default()).unwrap().chain_ok);
        // Dropping a record breaks it too
        let lines: Vec<&str> = text.lines().collect();
        std::fs::write(&path, format!("{}\n{}\n", lines[0], lines[2])).unwrap();
        assert!(!log.read(&Filter::default()).unwrap().chain_ok);
    }

    #[tokio::test]
    async fn a_torn_tail_and_an_orphan_attempt_recover() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        log.append(draft("r1", "attempt", "")).await.unwrap();
        drop(log);
        let path = dir.path().join("audit").join(FILE);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(b"{\"v\":1,\"seq\":2,\"ts\"").unwrap();
        drop(f);
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        let page = log.read(&Filter::default()).unwrap();
        assert!(page.chain_ok);
        assert_eq!(page.records.len(), 2);
        let result = &page.records[1].draft;
        assert_eq!(
            (result.phase.as_str(), result.outcome.as_str()),
            ("result", "unknown")
        );
        assert_eq!(result.request_id, "r1");
    }

    #[tokio::test]
    async fn rotation_keeps_the_chain_across_files() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), 600, 2).unwrap();
        for i in 0..12 {
            log.append(draft(&format!("r{i}"), "result", "applied"))
                .await
                .unwrap();
        }
        let audit = dir.path().join("audit");
        assert!(audit.join(format!("{FILE}.1")).exists());
        assert!(!audit.join(format!("{FILE}.3")).exists(), "keeps two");
        let page = log
            .read(&Filter {
                limit: 1000,
                ..Filter::default()
            })
            .unwrap();
        assert!(page.records.len() < 12, "oldest rotated out");
        // The retained records still chain
        assert!(page.chain_ok);
        // Reopening continues from the newest record
        let last = page.records.last().unwrap().clone();
        drop(log);
        let log = AuditLog::open(dir.path(), 600, 2).unwrap();
        let next = log.append(draft("rx", "result", "applied")).await.unwrap();
        assert_eq!(next.prev_hash, last.hash);
    }

    #[tokio::test]
    async fn an_unwritable_log_refuses_the_append() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path(), 1 << 20, 3).unwrap();
        let audit = dir.path().join("audit");
        std::fs::remove_file(audit.join(FILE)).unwrap();
        std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o500)).unwrap();
        let res = log.append(draft("r1", "attempt", "")).await;
        std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(res.is_err());
    }
}
