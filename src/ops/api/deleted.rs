//! Two-step soft delete: confirmation tokens and tombstones
//!
//! `delete-intent` issues a single-use token for five minutes, bound to the
//! tenant, its ETag, the key that asked, the revision epoch and generation,
//! and the tenant's incarnation. Any change to the tenant drops its tokens,
//! so one issued for an earlier declaration or attachment cannot delete a
//! later one. A delete writes a tombstone (`<id>-<ms>.toml`, 0600): the
//! whole declaration and who removed it, for `restore`. Expiry removes the
//! tombstone file and nothing else; no directory is ever deleted.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::time::Instant;

use crate::tenants::TenantDecl;

pub const TOKEN_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
struct Token {
    tenant: String,
    etag: String,
    key_id: String,
    epoch: String,
    incarnation: Option<String>,
    expires: Instant,
}

/// What a token is checked against when redeemed
pub struct Redeem<'a> {
    pub tenant: &'a str,
    pub etag: &'a str,
    pub key_id: &'a str,
    pub epoch: &'a str,
    pub incarnation: Option<&'a str>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refusal {
    Unknown,
    Expired,
    Mismatch(&'static str),
}

#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct Tombstone {
    pub tenant: String,
    pub deleted_at: String,
    pub deleted_by: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<String>,
    #[serde(skip)]
    pub file: PathBuf,
    #[serde(skip)]
    pub at_ms: u128,
}

pub struct Deletes {
    dir: PathBuf,
    retention: Duration,
    tokens: Mutex<HashMap<String, Token>>,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
}

impl Deletes {
    pub fn new(dir: PathBuf, retention: Duration) -> Result<Self> {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .with_context(|| format!("create {}", dir.display()))?;
        Ok(Self {
            dir,
            retention,
            tokens: Mutex::new(HashMap::new()),
        })
    }

    fn tokens(&self) -> std::sync::MutexGuard<'_, HashMap<String, Token>> {
        self.tokens.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A fresh token for deleting `tenant` as it stands
    pub fn issue(
        &self,
        tenant: &str,
        etag: &str,
        key_id: &str,
        epoch: &str,
        incarnation: Option<&str>,
    ) -> Result<String> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("random: {e}"))?;
        let token = hex::encode(bytes);
        let now = Instant::now();
        let mut tokens = self.tokens();
        tokens.retain(|_, t| t.expires > now);
        tokens.insert(
            token.clone(),
            Token {
                tenant: tenant.into(),
                etag: etag.into(),
                key_id: key_id.into(),
                epoch: epoch.into(),
                incarnation: incarnation.map(str::to_owned),
                expires: now + TOKEN_TTL,
            },
        );
        Ok(token)
    }

    /// Spend `token` on the delete `r` describes. Spent whatever the
    /// outcome, once it names a live token
    pub fn redeem(&self, token: &str, r: &Redeem<'_>) -> std::result::Result<(), Refusal> {
        let t = self.tokens().remove(token).ok_or(Refusal::Unknown)?;
        if t.expires <= Instant::now() {
            return Err(Refusal::Expired);
        }
        if t.tenant != r.tenant {
            return Err(Refusal::Mismatch("it was issued for another tenant"));
        }
        if t.key_id != r.key_id {
            return Err(Refusal::Mismatch("it was issued to another key"));
        }
        if t.epoch != r.epoch {
            return Err(Refusal::Mismatch("the daemon restarted since"));
        }
        if t.etag != r.etag {
            return Err(Refusal::Mismatch("the tenant changed since"));
        }
        if t.incarnation.as_deref() != r.incarnation {
            return Err(Refusal::Mismatch("the tenant was attached again since"));
        }
        Ok(())
    }

    /// Drop every token for `tenant`: it changed
    pub fn invalidate(&self, tenant: &str) {
        self.tokens().retain(|_, t| t.tenant != tenant);
    }

    /// Drop tokens of every tenant a new revision changed, whoever changed
    /// it
    pub fn spawn_invalidator(self: &Arc<Self>, reloader: Arc<crate::control::Reloader>) {
        let this = self.clone();
        let mut revisions = reloader.subscribe();
        tokio::spawn(async move {
            let mut prev = revisions.borrow_and_update().clone();
            while revisions.changed().await.is_ok() {
                let next = revisions.borrow_and_update().clone();
                let decls = |rev: &crate::control::ConfigRevision| {
                    crate::tenants::TenantsConfig::from_table(&rev.merged)
                        .ok()
                        .flatten()
                        .map(|t| t.decls)
                        .unwrap_or_default()
                };
                let (a, b) = (decls(&prev), decls(&next));
                for d in a.iter().chain(&b) {
                    let before = a.iter().find(|x| x.id == d.id);
                    let after = b.iter().find(|x| x.id == d.id);
                    if before != after {
                        this.invalidate(&d.id);
                    }
                }
                prev = next;
            }
        });
    }

    pub fn write_tombstone(
        &self,
        decl: &TenantDecl,
        deleted_by: &str,
        request_id: &str,
        incarnation: Option<&str>,
    ) -> Result<PathBuf> {
        let at = now_ms();
        let mut root = decl.to_fragment();
        let mut meta = toml::Table::new();
        meta.insert(
            "deleted_at".into(),
            time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default()
                .into(),
        );
        meta.insert("deleted_at_ms".into(), (at as i64).into());
        meta.insert("deleted_by".into(), deleted_by.into());
        meta.insert("request_id".into(), request_id.into());
        if let Some(i) = incarnation {
            meta.insert("incarnation".into(), i.into());
        }
        root.insert("_tombstone".into(), toml::Value::Table(meta));
        let name = format!("{}-{at}.toml", decl.id);
        crate::fs::write_private_blocking(&self.dir, &name, toml::to_string(&root)?.as_bytes())
            .with_context(|| format!("write tombstone {name}"))?;
        Ok(self.dir.join(name))
    }

    pub fn remove_tombstone(&self, path: &Path) {
        if path.parent() == Some(self.dir.as_path()) {
            let _ = std::fs::remove_file(path);
        }
    }

    fn read(&self, path: &Path) -> Option<(Tombstone, TenantDecl)> {
        let text = std::fs::read_to_string(path).ok()?;
        let mut root: toml::Table = text.parse().ok()?;
        let meta = root.remove("_tombstone")?.as_table()?.clone();
        let tenants = root.get("tenant")?.as_table()?;
        let (id, body) = tenants.iter().next()?;
        let decl = TenantDecl::parse(id, body.as_table()?).ok()?;
        let s = |k: &str| meta.get(k).and_then(toml::Value::as_str).map(str::to_owned);
        Some((
            Tombstone {
                tenant: id.clone(),
                deleted_at: s("deleted_at").unwrap_or_default(),
                deleted_by: s("deleted_by").unwrap_or_default(),
                request_id: s("request_id").unwrap_or_default(),
                incarnation: s("incarnation"),
                file: path.to_path_buf(),
                at_ms: meta
                    .get("deleted_at_ms")
                    .and_then(toml::Value::as_integer)
                    .map_or(0, |n| n as u128),
            },
            decl,
        ))
    }

    /// Tombstones within retention, newest first. Expired ones are removed:
    /// the tombstone file only
    pub fn list(&self) -> Vec<(Tombstone, TenantDecl)> {
        let horizon = now_ms().saturating_sub(self.retention.as_millis());
        let mut out: Vec<_> = std::fs::read_dir(&self.dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "toml"))
            .filter_map(|p| self.read(&p))
            .filter(|(t, _)| {
                if t.at_ms < horizon {
                    self.remove_tombstone(&t.file);
                    false
                } else {
                    true
                }
            })
            .collect();
        out.sort_by_key(|(t, _)| std::cmp::Reverse(t.at_ms));
        out
    }

    /// The newest tombstone of `tenant`
    pub fn latest(&self, tenant: &str) -> Option<(Tombstone, TenantDecl)> {
        self.list().into_iter().find(|(t, _)| t.tenant == tenant)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl(id: &str) -> TenantDecl {
        let body: toml::Table = format!("dbname = '{id}_db'\n[ch]\nhost = 'ch'\npassword = 'pw'\n")
            .parse()
            .unwrap();
        TenantDecl::parse(id, &body).unwrap()
    }

    fn redeem<'a>(
        tenant: &'a str,
        etag: &'a str,
        key: &'a str,
        inc: Option<&'a str>,
    ) -> Redeem<'a> {
        Redeem {
            tenant,
            etag,
            key_id: key,
            epoch: "e1",
            incarnation: inc,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tokens_are_single_use_bound_and_expire() {
        let dir = tempfile::tempdir().unwrap();
        let d = Deletes::new(dir.path().join("deleted"), Duration::from_secs(3600)).unwrap();
        let t = d.issue("a", "r-1", "ops", "e1", Some("inc1")).unwrap();
        assert_eq!(t.len(), 64);
        d.redeem(&t, &redeem("a", "r-1", "ops", Some("inc1")))
            .unwrap();
        assert_eq!(
            d.redeem(&t, &redeem("a", "r-1", "ops", Some("inc1"))),
            Err(Refusal::Unknown),
            "single use"
        );
        for (r, why) in [
            (redeem("b", "r-1", "ops", Some("inc1")), "another tenant"),
            (redeem("a", "r-1", "dash", Some("inc1")), "another key"),
            (redeem("a", "r-2", "ops", Some("inc1")), "changed"),
            (redeem("a", "r-1", "ops", Some("inc2")), "attached again"),
            (redeem("a", "r-1", "ops", None), "attached again"),
        ] {
            let t = d.issue("a", "r-1", "ops", "e1", Some("inc1")).unwrap();
            match d.redeem(&t, &r) {
                Err(Refusal::Mismatch(m)) => assert!(m.contains(why), "{m}"),
                other => panic!("{other:?}"),
            }
        }
        let mut other_epoch = redeem("a", "r-1", "ops", Some("inc1"));
        other_epoch.epoch = "e2";
        let t = d.issue("a", "r-1", "ops", "e1", Some("inc1")).unwrap();
        assert!(matches!(
            d.redeem(&t, &other_epoch),
            Err(Refusal::Mismatch(_))
        ));
        let t = d.issue("a", "r-1", "ops", "e1", Some("inc1")).unwrap();
        tokio::time::advance(TOKEN_TTL + Duration::from_secs(1)).await;
        assert_eq!(
            d.redeem(&t, &redeem("a", "r-1", "ops", Some("inc1"))),
            Err(Refusal::Expired)
        );
        // Any change to the tenant drops its tokens
        let t = d.issue("a", "r-1", "ops", "e1", None).unwrap();
        d.invalidate("a");
        assert_eq!(
            d.redeem(&t, &redeem("a", "r-1", "ops", None)),
            Err(Refusal::Unknown)
        );
    }

    #[test]
    fn tombstones_round_trip_privately_and_expire_alone() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let spill = dir.path().join("spill/tenants/a");
        std::fs::create_dir_all(&spill).unwrap();
        let d = Deletes::new(dir.path().join("deleted"), Duration::from_secs(3600)).unwrap();
        let path = d
            .write_tombstone(&decl("a"), "ops", "req-1", Some("inc1"))
            .unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let (t, back) = d.latest("a").unwrap();
        assert_eq!(back, decl("a"));
        assert_eq!(
            (t.deleted_by.as_str(), t.request_id.as_str()),
            ("ops", "req-1")
        );
        assert_eq!(t.incarnation.as_deref(), Some("inc1"));
        assert!(d.latest("b").is_none());
        // Past retention: the tombstone goes, the tenant's directory stays
        let expired = Deletes::new(dir.path().join("deleted"), Duration::ZERO).unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert!(expired.list().is_empty());
        assert!(!path.exists());
        assert!(spill.exists());
        // Only files inside the tombstone directory are ever removed
        let outside = dir.path().join("keep.toml");
        std::fs::write(&outside, "x").unwrap();
        d.remove_tombstone(&outside);
        assert!(outside.exists());
    }
}
