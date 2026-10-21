//! Where and why the daemon stopped, kept across restarts
//!
//! `<state-dir>/crashes/` holds one JSON record per panic (from a chained
//! panic hook, tokio-task panics included), per fatal exit (`run` returning
//! an error), and per unclean exit: a `running.json` marker written at start
//! and removed on clean exit, still present at the next start with no exit
//! record from that process, means it died without a word (SIGKILL, the
//! OOM killer, power loss). Messages pass [`redact_text`]. The newest
//! [`KEEP`] records or [`KEEP_BYTES`] stay.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::ops::redact::redact_text;

pub const KEEP: usize = 100;
pub const KEEP_BYTES: u64 = 10 << 20;
const BACKTRACE_MAX: usize = 64 << 10;
const MARKER: &str = "running.json";
/// Tells apart records written in one millisecond
static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrashRecord {
    pub v: u32,
    /// File stem, `<kind>-<unix ms>-<pid>-<n>`
    pub id: String,
    /// `panic`, `exit` or `unclean_exit`
    pub kind: String,
    pub ts: String,
    pub pid: u32,
    pub version: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backtrace: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Marker {
    v: u32,
    pid: u32,
    started_at: String,
    version: String,
}

#[derive(Debug)]
pub struct CrashStore {
    dir: PathBuf,
}

fn now() -> (String, u128) {
    let ts = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    (ts, ms)
}

impl CrashStore {
    pub fn open(state_dir: &Path) -> Result<Arc<Self>> {
        use std::os::unix::fs::DirBuilderExt;
        let dir = state_dir.join("crashes");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .with_context(|| format!("create {}", dir.display()))?;
        Ok(Arc::new(Self { dir }))
    }

    fn record(
        &self,
        kind: &str,
        message: &str,
        f: impl FnOnce(&mut CrashRecord),
    ) -> Result<CrashRecord> {
        let (ts, ms) = now();
        let pid = std::process::id();
        let mut rec = CrashRecord {
            v: 1,
            id: format!(
                "{kind}-{ms}-{pid}-{}",
                SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ),
            kind: kind.into(),
            ts,
            pid,
            version: crate::VERSION.into(),
            message: redact_text(message),
            thread: None,
            location: None,
            backtrace: None,
        };
        f(&mut rec);
        self.store(&rec)?;
        Ok(rec)
    }

    /// Synchronous and fsynced: the panic hook and `main` call it with the
    /// process about to go
    fn store(&self, rec: &CrashRecord) -> Result<()> {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.dir.join(format!("{}.json", rec.id));
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .with_context(|| format!("create {}", path.display()))?;
        f.write_all(serde_json::to_string_pretty(rec)?.as_bytes())?;
        f.sync_all()?;
        crate::fs::sync_dir_blocking(&self.dir)?;
        self.prune();
        Ok(())
    }

    /// Record every panic, then run the hook that was installed before
    pub fn install_panic_hook(self: &Arc<Self>) {
        let store = self.clone();
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| info.payload().downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic with a non-string payload".into());
            let location = info
                .location()
                .map(|l| format!("{}:{}", l.file(), l.line()));
            let thread = std::thread::current().name().map(str::to_owned);
            let mut backtrace = std::backtrace::Backtrace::force_capture().to_string();
            if backtrace.len() > BACKTRACE_MAX {
                let mut cut = BACKTRACE_MAX;
                while !backtrace.is_char_boundary(cut) {
                    cut -= 1;
                }
                backtrace.truncate(cut);
            }
            let _ = store.record("panic", &payload, |r| {
                r.location = location;
                r.thread = thread;
                r.backtrace = Some(redact_text(&backtrace));
            });
            previous(info);
        }));
    }

    /// Mark this process running. A marker left by an earlier one with no
    /// exit record from its pid becomes an `unclean_exit` record first
    pub fn mark_running(&self) -> Result<Option<CrashRecord>> {
        let path = self.dir.join(MARKER);
        let mut unclean = None;
        if let Ok(text) = std::fs::read_to_string(&path)
            && let Ok(prior) = serde_json::from_str::<Marker>(&text)
        {
            let reported = self
                .list()
                .iter()
                .any(|r| r.pid == prior.pid && r.kind == "exit" && r.ts >= prior.started_at);
            if !reported {
                unclean = Some(self.record(
                    "unclean_exit",
                    &format!(
                        "walshadow {} (pid {}, started {}) stopped without a clean exit or a \
                         recorded error: SIGKILL, the OOM killer or a host failure",
                        prior.version, prior.pid, prior.started_at
                    ),
                    |r| r.version = prior.version.clone(),
                )?);
            }
        }
        let marker = Marker {
            v: 1,
            pid: std::process::id(),
            started_at: now().0,
            version: crate::VERSION.into(),
        };
        crate::fs::write_private_blocking(
            &self.dir,
            MARKER,
            serde_json::to_string(&marker)?.as_bytes(),
        )?;
        Ok(unclean)
    }

    /// `run` failed: why, before the process exits
    pub fn record_exit(&self, error: &str) -> Result<CrashRecord> {
        self.record("exit", error, |_| {})
    }

    /// Clean shutdown: the next start finds no marker
    pub fn clean_exit(&self) {
        let _ = std::fs::remove_file(self.dir.join(MARKER));
    }

    /// Newest first
    pub fn list(&self) -> Vec<CrashRecord> {
        let mut out: Vec<CrashRecord> = std::fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name() != MARKER)
            .filter_map(|e| std::fs::read_to_string(e.path()).ok())
            .filter_map(|t| serde_json::from_str(&t).ok())
            .collect();
        let key = |r: &CrashRecord| -> (u128, u64) {
            let mut parts = r.id.split('-').skip(1);
            let ms = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
            let n = parts.nth(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            (ms, n)
        };
        out.sort_by_key(|r| std::cmp::Reverse(key(r)));
        out
    }

    pub fn get(&self, id: &str) -> Option<CrashRecord> {
        if id.is_empty()
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return None;
        }
        let text = std::fs::read_to_string(self.dir.join(format!("{id}.json"))).ok()?;
        serde_json::from_str(&text).ok()
    }

    fn prune(&self) {
        let mut files: Vec<(std::time::SystemTime, u64, PathBuf)> = std::fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name() != MARKER)
            .filter_map(|e| {
                let meta = e.metadata().ok()?;
                Some((meta.modified().ok()?, meta.len(), e.path()))
            })
            .collect();
        // Newest first; ids order records written in one millisecond
        files.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.2.cmp(&a.2)));
        let mut bytes = 0;
        for (i, (_, len, path)) in files.into_iter().enumerate() {
            bytes += len;
            if i >= KEEP || bytes > KEEP_BYTES {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exits_and_unclean_exits_are_told_apart() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrashStore::open(dir.path()).unwrap();
        // First start: nothing to report
        assert!(store.mark_running().unwrap().is_none());
        // Started again without a clean exit: the earlier run died silently
        let unclean = store.mark_running().unwrap().expect("unclean exit");
        assert_eq!(unclean.kind, "unclean_exit");
        assert!(unclean.message.contains("without a clean exit"));
        // A recorded error is an exit, not an unclean one
        let exit = store
            .record_exit("connect postgres://repl:hunter2@pg/app: refused")
            .unwrap();
        assert!(!exit.message.contains("hunter2"), "{}", exit.message);
        assert!(store.mark_running().unwrap().is_none());
        store.clean_exit();
        assert!(store.mark_running().unwrap().is_none());
        let listed = store.list();
        assert_eq!(listed.len(), 2);
        assert_eq!(store.get(&exit.id), Some(exit));
        assert!(store.get("../etc/passwd").is_none());
        assert!(store.get("").is_none());
    }

    #[test]
    fn panics_are_recorded_and_the_previous_hook_still_runs() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrashStore::open(dir.path()).unwrap();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = ran.clone();
        std::panic::set_hook(Box::new(move |_| {
            seen.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        store.install_panic_hook();
        let _ = std::thread::Builder::new()
            .name("boom".into())
            .spawn(|| panic!("token=abc123 lost"))
            .unwrap()
            .join();
        let _ = std::panic::take_hook();
        assert!(ran.load(std::sync::atomic::Ordering::SeqCst));
        let panics: Vec<_> = store
            .list()
            .into_iter()
            .filter(|r| r.kind == "panic")
            .collect();
        assert_eq!(panics.len(), 1);
        let p = &panics[0];
        assert!(!p.message.contains("abc123"), "{}", p.message);
        assert_eq!(p.thread.as_deref(), Some("boom"));
        assert!(
            p.location
                .as_deref()
                .is_some_and(|l| l.contains("crash.rs"))
        );
        assert!(p.backtrace.is_some());
    }

    #[test]
    fn retention_keeps_the_newest() {
        let dir = tempfile::tempdir().unwrap();
        let store = CrashStore::open(dir.path()).unwrap();
        for i in 0..(KEEP + 5) {
            store.record_exit(&format!("error {i}")).unwrap();
        }
        assert!(store.list().len() <= KEEP);
    }
}
