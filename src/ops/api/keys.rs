//! API keys: who may call the HTTP API, and with what scope
//!
//! A token is `wsk_<key id>_<64 hex>`: the hex is 32 random bytes. The key
//! file the operator keeps outside the repo (`--api-keys-file`, 0600 or
//! narrower) stores, per key, the SHA-256 of those bytes, never the token:
//!
//! ```toml
//! [[key]]
//! id = "ops-ci"
//! scopes = ["admin"]
//! sha256 = "<64 hex>"
//! expires_at = "2027-01-01T00:00:00Z"   # optional
//! disabled = false                      # optional
//! description = "CI deployer"           # optional
//! ```
//!
//! The secret is uniformly random, so one SHA-256 suffices; a slow hash
//! would only hand callers a CPU lever. Comparison is constant time, an
//! unknown id compares against a dummy. A reload that fails rejects every
//! key until the file is fixed: a botched revocation must not leave a
//! revoked key working.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub const TOKEN_PREFIX: &str = "wsk_";

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Read,
    Admin,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Admin => "admin",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    #[serde(default)]
    key: Vec<KeyEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyEntry {
    id: String,
    scopes: Vec<Scope>,
    sha256: String,
    #[serde(default)]
    expires_at: Option<String>,
    #[serde(default)]
    disabled: bool,
    #[serde(default)]
    #[allow(dead_code)]
    description: Option<String>,
}

#[derive(Clone)]
struct Key {
    scopes: BTreeSet<Scope>,
    hash: [u8; 32],
    expires_at: Option<time::OffsetDateTime>,
    expires_text: Option<String>,
    disabled: bool,
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("scopes", &self.scopes)
            .field("hash", &"***")
            .finish()
    }
}

/// A caller who presented a valid key
#[derive(Clone, Debug, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct Principal {
    pub key_id: String,
    pub scopes: BTreeSet<Scope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl Principal {
    /// `admin` implies `read`
    pub fn allows(&self, scope: Scope) -> bool {
        self.scopes.contains(&Scope::Admin) || self.scopes.contains(&scope)
    }
}

/// Why a request failed authentication; a metric label, never told to the
/// caller
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthFailure {
    Missing,
    Malformed,
    Unknown,
    Mismatch,
    Expired,
    Disabled,
    /// The key file failed to load: every key is refused
    Unavailable,
}

impl AuthFailure {
    pub fn label(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Malformed => "malformed",
            Self::Unknown => "unknown_key",
            Self::Mismatch => "wrong_secret",
            Self::Expired => "expired",
            Self::Disabled => "disabled",
            Self::Unavailable => "keys_unavailable",
        }
    }
}

enum State {
    Loaded(HashMap<String, Key>),
    Failed(String),
}

pub struct KeyStore {
    path: PathBuf,
    state: RwLock<State>,
    mtime: Mutex<Option<SystemTime>>,
}

impl std::fmt::Debug for KeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyStore")
            .field("path", &self.path)
            .finish()
    }
}

fn parse(path: &Path) -> Result<HashMap<String, Key>> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(path).with_context(|| format!("read {}", path.display()))?;
    ensure!(
        meta.permissions().mode() & 0o077 == 0,
        "{} is readable by group or others; chmod 600 it",
        path.display()
    );
    let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let file: KeyFile =
        toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
    let mut keys = HashMap::new();
    for entry in file.key {
        ensure!(
            !entry.id.is_empty()
                && entry.id.len() <= 32
                && entry
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "key id {:?} must be 1-32 of [a-z0-9-]",
            entry.id
        );
        ensure!(!entry.scopes.is_empty(), "key {} has no scopes", entry.id);
        let hash: [u8; 32] = hex::decode(&entry.sha256)
            .ok()
            .and_then(|b| b.try_into().ok())
            .with_context(|| format!("key {}: sha256 must be 64 hex characters", entry.id))?;
        let expires_at = entry
            .expires_at
            .as_deref()
            .map(|t| {
                time::OffsetDateTime::parse(t, &time::format_description::well_known::Rfc3339)
                    .with_context(|| format!("key {}: expires_at is not RFC 3339", entry.id))
            })
            .transpose()?;
        let key = Key {
            scopes: entry.scopes.into_iter().collect(),
            hash,
            expires_at,
            expires_text: entry.expires_at,
            disabled: entry.disabled,
        };
        if keys.insert(entry.id.clone(), key).is_some() {
            bail!("key id {} appears twice", entry.id);
        }
    }
    Ok(keys)
}

impl KeyStore {
    /// Load at startup: an unreadable, loose or invalid file is an error
    pub fn open(path: &Path) -> Result<Arc<Self>> {
        let keys = parse(path)?;
        if keys.is_empty() {
            tracing::warn!(
                target: "walshadow::api",
                file = %path.display(),
                "the API key file holds no keys; every request is refused",
            );
        }
        Ok(Arc::new(Self {
            path: path.to_path_buf(),
            state: RwLock::new(State::Loaded(keys)),
            mtime: Mutex::new(mtime(path)),
        }))
    }

    /// Re-read the file. On failure every key is refused until a reload
    /// succeeds
    pub fn reload(&self) -> Result<usize> {
        *self.mtime.lock().unwrap_or_else(PoisonError::into_inner) = mtime(&self.path);
        match parse(&self.path) {
            Ok(keys) => {
                let n = keys.len();
                *self.state.write().unwrap_or_else(PoisonError::into_inner) = State::Loaded(keys);
                tracing::info!(target: "walshadow::api", keys = n, "API keys reloaded");
                Ok(n)
            }
            Err(e) => {
                let msg = format!("{e:#}");
                tracing::error!(
                    target: "walshadow::api",
                    error = %msg,
                    "API key reload failed; refusing every key until the file is fixed",
                );
                *self.state.write().unwrap_or_else(PoisonError::into_inner) = State::Failed(msg);
                Err(e)
            }
        }
    }

    /// Reload when the file's mtime moved
    pub fn reload_if_changed(&self) {
        let now = mtime(&self.path);
        let changed = *self.mtime.lock().unwrap_or_else(PoisonError::into_inner) != now;
        if changed {
            let _ = self.reload();
        }
    }

    /// Poll the file every `every` and reload on SIGHUP
    pub fn spawn_watcher(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let mut hup =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
            loop {
                let hup_recv = async {
                    match hup.as_mut() {
                        Some(h) => h.recv().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    _ = tokio::time::sleep(every) => this.reload_if_changed(),
                    _ = hup_recv => { let _ = this.reload(); }
                }
            }
        })
    }

    /// `Ok(keys)` while loaded, the load error once a reload failed
    pub fn health(&self) -> std::result::Result<usize, String> {
        match &*self.state.read().unwrap_or_else(PoisonError::into_inner) {
            State::Loaded(keys) => Ok(keys.len()),
            State::Failed(e) => Err(e.clone()),
        }
    }

    /// The `Authorization` header's bearer token, checked
    pub fn authenticate(
        &self,
        header: Option<&str>,
    ) -> std::result::Result<Principal, AuthFailure> {
        let header = header.ok_or(AuthFailure::Missing)?;
        let token = header
            .strip_prefix("Bearer ")
            .ok_or(AuthFailure::Malformed)?
            .trim();
        let (id, secret) = token
            .strip_prefix(TOKEN_PREFIX)
            .and_then(|rest| rest.rsplit_once('_'))
            .ok_or(AuthFailure::Malformed)?;
        let secret: [u8; 32] = hex::decode(secret)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or(AuthFailure::Malformed)?;
        let presented: [u8; 32] = Sha256::digest(secret).into();
        let state = self.state.read().unwrap_or_else(PoisonError::into_inner);
        let keys = match &*state {
            State::Loaded(keys) => keys,
            State::Failed(_) => return Err(AuthFailure::Unavailable),
        };
        let Some(key) = keys.get(id) else {
            // Same work for an unknown id as for a wrong secret
            let _ = presented.ct_eq(&[0u8; 32]);
            return Err(AuthFailure::Unknown);
        };
        if !bool::from(presented.ct_eq(&key.hash)) {
            return Err(AuthFailure::Mismatch);
        }
        if key.disabled {
            return Err(AuthFailure::Disabled);
        }
        if key
            .expires_at
            .is_some_and(|at| at <= time::OffsetDateTime::now_utc())
        {
            return Err(AuthFailure::Expired);
        }
        Ok(Principal {
            key_id: id.to_string(),
            scopes: key.scopes.clone(),
            expires_at: key.expires_text.clone(),
        })
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

/// A new key: the token, shown once, and the `[[key]]` stanza to add to the
/// key file
pub fn generate(id: &str, scopes: &[Scope], expires_at: Option<&str>) -> Result<(String, String)> {
    ensure!(
        !id.is_empty()
            && id.len() <= 32
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "key id {id:?} must be 1-32 of [a-z0-9-]"
    );
    ensure!(!scopes.is_empty(), "a key needs a scope");
    if let Some(t) = expires_at {
        time::OffsetDateTime::parse(t, &time::format_description::well_known::Rfc3339)
            .context("--expires must be RFC 3339, eg 2027-01-01T00:00:00Z")?;
    }
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|e| anyhow::anyhow!("random: {e}"))?;
    let token = format!("{TOKEN_PREFIX}{id}_{}", hex::encode(secret));
    let scopes = scopes
        .iter()
        .map(|s| format!("\"{}\"", s.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stanza = format!(
        "[[key]]\nid = \"{id}\"\nscopes = [{scopes}]\nsha256 = \"{}\"\n",
        hex::encode(Sha256::digest(secret))
    );
    if let Some(t) = expires_at {
        stanza.push_str(&format!("expires_at = \"{t}\"\n"));
    }
    Ok((token, stanza))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The store's debug form names its file, never a key or a hash
    #[test]
    fn debug_shows_the_file_not_the_keys() {
        let dir = tempfile::tempdir().unwrap();
        let (path, tokens) = key_file(dir.path(), &[("ops", &[Scope::Admin], None, false)]);
        let store = KeyStore::open(&path).unwrap();
        let shown = format!("{store:?}");
        assert!(
            shown.contains("KeyStore") && shown.contains("api-keys"),
            "{shown}"
        );
        assert!(
            !shown.contains(&tokens[0]) && !shown.contains("sha256"),
            "{shown}"
        );
        assert_eq!(AuthFailure::Unavailable.label(), "keys_unavailable");
    }

    /// A key file at 0600 holding the stanzas of `keys`, and their tokens
    pub(crate) fn key_file(
        dir: &Path,
        keys: &[(&str, &[Scope], Option<&str>, bool)],
    ) -> (PathBuf, Vec<String>) {
        use std::os::unix::fs::PermissionsExt;
        let mut text = String::new();
        let mut tokens = Vec::new();
        for (id, scopes, expires, disabled) in keys {
            let (token, stanza) = generate(id, scopes, *expires).unwrap();
            text.push_str(&stanza);
            if *disabled {
                text.push_str("disabled = true\n");
            }
            tokens.push(token);
        }
        let path = dir.join("api-keys.toml");
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        (path, tokens)
    }

    #[test]
    fn tokens_authenticate_and_every_failure_is_named() {
        let dir = tempfile::tempdir().unwrap();
        let (path, tokens) = key_file(
            dir.path(),
            &[
                ("ops", &[Scope::Admin], None, false),
                ("dash", &[Scope::Read], Some("2099-01-01T00:00:00Z"), false),
                ("old", &[Scope::Read], Some("2001-01-01T00:00:00Z"), false),
                ("off", &[Scope::Read], None, true),
            ],
        );
        let store = KeyStore::open(&path).unwrap();
        let bearer = |t: &str| format!("Bearer {t}");
        let ops = store.authenticate(Some(&bearer(&tokens[0]))).unwrap();
        assert_eq!(ops.key_id, "ops");
        assert!(ops.allows(Scope::Read) && ops.allows(Scope::Admin));
        let dash = store.authenticate(Some(&bearer(&tokens[1]))).unwrap();
        assert!(dash.allows(Scope::Read) && !dash.allows(Scope::Admin));
        let fail = |h: Option<&str>| store.authenticate(h).unwrap_err();
        assert_eq!(fail(None), AuthFailure::Missing);
        assert_eq!(
            fail(Some(&format!("Basic {}", tokens[0]))),
            AuthFailure::Malformed
        );
        assert_eq!(fail(Some("Bearer wsk_ops_zz")), AuthFailure::Malformed);
        assert_eq!(fail(Some("Bearer nope")), AuthFailure::Malformed);
        let wrong = format!("Bearer wsk_ops_{}", "0".repeat(64));
        assert_eq!(fail(Some(&wrong)), AuthFailure::Mismatch);
        let unknown = format!("Bearer wsk_ghost_{}", "0".repeat(64));
        assert_eq!(fail(Some(&unknown)), AuthFailure::Unknown);
        assert_eq!(fail(Some(&bearer(&tokens[2]))), AuthFailure::Expired);
        assert_eq!(fail(Some(&bearer(&tokens[3]))), AuthFailure::Disabled);
        // Debug output never carries a hash
        if let State::Loaded(keys) = &*store.state.read().unwrap() {
            let shown = format!("{:?}", keys["ops"]);
            assert!(
                shown.contains("***") && !shown.contains("hash: ["),
                "{shown}"
            );
        }
    }

    #[test]
    fn a_loose_or_broken_file_is_refused_and_a_failed_reload_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let (path, tokens) = key_file(dir.path(), &[("ops", &[Scope::Admin], None, false)]);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let err = KeyStore::open(&path).unwrap_err().to_string();
        assert!(err.contains("chmod 600"), "{err}");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let store = KeyStore::open(&path).unwrap();
        let header = format!("Bearer {}", tokens[0]);
        assert!(store.authenticate(Some(&header)).is_ok());
        assert_eq!(store.health(), Ok(1));
        // A botched edit: every key refused, the old one included
        std::fs::write(&path, "[[key]]\nid = 'ops'\n").unwrap();
        assert!(store.reload().is_err());
        assert_eq!(
            store.authenticate(Some(&header)).unwrap_err(),
            AuthFailure::Unavailable
        );
        assert!(store.health().is_err());
        // Fixed: back in service
        let (_, tokens) = key_file(dir.path(), &[("ops", &[Scope::Admin], None, false)]);
        store.reload().unwrap();
        assert!(
            store
                .authenticate(Some(&format!("Bearer {}", tokens[0])))
                .is_ok()
        );
    }

    #[test]
    fn duplicate_ids_and_bad_fields_are_refused() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.toml");
        let h = "0".repeat(64);
        for text in [
            format!(
                "[[key]]\nid = 'a'\nscopes = ['read']\nsha256 = '{h}'\n[[key]]\nid = 'a'\nscopes = ['read']\nsha256 = '{h}'\n"
            ),
            format!("[[key]]\nid = 'A'\nscopes = ['read']\nsha256 = '{h}'\n"),
            format!("[[key]]\nid = 'a'\nscopes = []\nsha256 = '{h}'\n"),
            "[[key]]\nid = 'a'\nscopes = ['read']\nsha256 = 'abc'\n".to_string(),
            format!("[[key]]\nid = 'a'\nscopes = ['root']\nsha256 = '{h}'\n"),
            format!("[[key]]\nid = 'a'\nscopes = ['read']\nsha256 = '{h}'\nexpires_at = 'soon'\n"),
            format!("[[key]]\nid = 'a'\nscopes = ['read']\nsha256 = '{h}'\nextra = 1\n"),
        ] {
            std::fs::write(&path, &text).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(KeyStore::open(&path).is_err(), "{text}");
        }
        assert!(generate("Bad", &[Scope::Read], None).is_err());
        assert!(generate("a", &[], None).is_err());
        assert!(generate("a", &[Scope::Read], Some("tomorrow")).is_err());
    }

    #[tokio::test]
    async fn the_watcher_picks_up_a_rotated_file() {
        let dir = tempfile::tempdir().unwrap();
        let (path, old) = key_file(dir.path(), &[("ops", &[Scope::Admin], None, false)]);
        let store = KeyStore::open(&path).unwrap();
        let watcher = store.spawn_watcher(Duration::from_millis(20));
        // Rotate: a new secret for the same id, with a later mtime
        tokio::time::sleep(Duration::from_millis(20)).await;
        let (_, new) = key_file(dir.path(), &[("ops", &[Scope::Admin], None, false)]);
        let filetime = SystemTime::now() + Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(filetime)
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while store
            .authenticate(Some(&format!("Bearer {}", old[0])))
            .is_ok()
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "rotation not picked up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            store
                .authenticate(Some(&format!("Bearer {}", new[0])))
                .is_ok()
        );
        watcher.abort();
    }
}
