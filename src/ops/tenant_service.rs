//! The one path every tenant mutation takes, control socket and HTTP API
//!
//! A mutation holds the mutation lock, then the reload lock, from reading
//! the authoritative declaration until its revision publishes or rolls back
//! ([`ops::reload`](crate::ops::reload)). In between it checks the caller's
//! precondition, validates the would-be config, classifies the change
//! ([`classify_change`]), enforces acknowledgement of a resync, fences a
//! re-bind on disk ([`RebindFence`]), persists conditionally and publishes
//! with the tenant as the abort target. A refusal by the tenant's own
//! resolver restores the previous declaration, but only if nobody changed
//! it since.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use hmac::{Hmac, Mac};
use toml::{Table, Value};

use crate::control::{PublishError, PublishOpts, ReloadCause, ReloadTx, SharedCtx};
use crate::ops::tenant_ctl::{mirror_registry, tenant_fragment_path};
use crate::tenants::{
    Change, Desired, Phase, RebindFence, Registry, Resource, TenantDecl, TenantState,
    TenantsConfig, classify_change, decls_from_rows, registry_ddl, tenant_dir, validate_id,
};

/// A refusal, shaped for the HTTP API and the control socket alike
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}

impl ServiceError {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn internal(e: impl std::fmt::Display) -> Self {
        Self::new(500, "internal", format!("{e:#}"))
    }
}

impl std::fmt::Display for ServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ServiceError {}

pub type ServiceResult<T> = std::result::Result<T, ServiceError>;

/// Opaque ETags: HMAC-SHA256 under a key only the daemon holds, so a
/// declaration's inline secrets never surface as a guessable hash
pub struct EtagKey([u8; 32]);

impl EtagKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// `<dir>/etag.key`, created 0600 on first use
    pub async fn load_or_create(dir: &Path) -> anyhow::Result<Self> {
        let path = dir.join("etag.key");
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let bytes: [u8; 32] = bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("{} is not 32 bytes", path.display()))?;
                Ok(Self(bytes))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut bytes = [0u8; 32];
                getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("etag key: {e}"))?;
                tokio::fs::create_dir_all(dir).await?;
                crate::fs::write_private(dir, "etag.key", &bytes)
                    .await
                    .with_context(|| format!("write {}", path.display()))?;
                Ok(Self(bytes))
            }
            Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
        }
    }

    pub fn etag(&self, decl: &TenantDecl) -> String {
        let canonical = toml::to_string(&decl.to_fragment()).unwrap_or_default();
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&self.0).expect("any key length");
        mac.update(canonical.as_bytes());
        let tag = hex::encode(mac.finalize().into_bytes());
        format!("r-{}", &tag[..32])
    }
}

/// What the caller expects the tenant to be
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// No check: the control socket, which has no ETags
    Any,
    /// The tenant must not exist
    Absent,
    /// The tenant must exist with this ETag
    Etag(String),
}

#[derive(Debug, Clone)]
pub struct Mutation {
    pub request_id: String,
    /// Who asks, for the audit record
    pub actor: crate::ops::audit::Actor,
    /// Audit action, eg `tenant.update`
    pub action: String,
    /// HTTP method and path, `None` from the control socket
    pub method: Option<String>,
    pub path: Option<String>,
    pub precondition: Precondition,
    pub acknowledge_resync: bool,
    /// The control socket's compatibility: it re-binds identity changes and
    /// attaches as `ctl` always did, without an explicit acknowledgement
    pub legacy_ack: bool,
    pub dry_run: bool,
}

impl Mutation {
    pub fn new(request_id: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            actor: crate::ops::audit::Actor::default(),
            action: "tenant.update".into(),
            method: None,
            path: None,
            precondition: Precondition::Any,
            acknowledge_resync: false,
            legacy_ack: false,
            dry_run: false,
        }
    }
}

/// A mutation that ran, or a dry run that would
#[derive(Debug, Clone)]
pub struct Outcome {
    pub request_id: String,
    pub tenant: String,
    pub change: Change,
    pub before: Option<TenantDecl>,
    pub after: Option<TenantDecl>,
    pub etag: Option<String>,
    pub epoch: String,
    pub generation: u64,
    pub dry_run: bool,
}

/// The authoritative record behind a declaration
enum Stored {
    /// `60-tenant-<id>.toml`, as bytes, `None` when absent
    Fragment(PathBuf, Option<Vec<u8>>),
    /// The registry row `(dbname, spec, state)`, `None` when absent
    Row {
        schema: String,
        row: Option<(String, String, String)>,
    },
}

#[derive(Clone)]
pub struct TenantService {
    ctx: SharedCtx,
    etag_key: Option<Arc<EtagKey>>,
}

impl TenantService {
    pub fn new(ctx: SharedCtx, etag_key: Option<Arc<EtagKey>>) -> Self {
        Self { ctx, etag_key }
    }

    pub fn ctx(&self) -> &SharedCtx {
        &self.ctx
    }

    pub fn etag(&self, decl: &TenantDecl) -> Option<String> {
        self.etag_key.as_ref().map(|k| k.etag(decl))
    }

    /// Apply `edit` to tenant `id`: it maps the current declaration (`None`:
    /// absent) to the next (`None`: remove). Every step runs under the
    /// mutation lock, then the reload lock. Audited: one that runs writes an
    /// attempt before changing anything and a result after; one refused
    /// before it runs writes a single result. Dry runs are not audited
    pub async fn mutate<F>(&self, id: &str, m: &Mutation, edit: F) -> ServiceResult<Outcome>
    where
        F: FnOnce(Option<&TenantDecl>) -> ServiceResult<Option<TenantDecl>>,
    {
        let mut audit = AuditState::default();
        let res = self.mutate_inner(id, m, edit, &mut audit).await;
        if m.dry_run && res.is_ok() {
            tracing::info!(
                target: "walshadow::api",
                request_id = %m.request_id,
                tenant = %id,
                action = %m.action,
                "dry run",
            );
            return res;
        }
        if let Some(log) = self.ctx.reloader.audit() {
            let (outcome, error) = match &res {
                Ok(_) => ("applied", None),
                Err(e) if !audit.attempted => ("rejected", Some(e.to_string())),
                Err(e) if e.code == "apply_refused" => ("rolled_back", Some(e.to_string())),
                Err(e) if e.code == "conflict_after_failed_apply" => {
                    ("conflict", Some(e.to_string()))
                }
                Err(e) => ("failed", Some(e.to_string())),
            };
            let mut draft = self.draft(id, m, "result", &audit);
            draft.outcome = outcome.into();
            draft.error = error;
            if let Ok(out) = &res {
                draft.epoch = Some(out.epoch.clone());
                draft.generation = Some(out.generation);
            }
            if let Err(e) = log.append(draft).await {
                crate::ops::metrics::API_AUDIT_WRITE_FAILURES
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    target: "walshadow::audit",
                    request_id = %m.request_id,
                    error = %format!("{e:#}"),
                    "could not record the outcome of a mutation",
                );
            }
        }
        res
    }

    fn draft(
        &self,
        id: &str,
        m: &Mutation,
        phase: &str,
        audit: &AuditState,
    ) -> crate::ops::audit::Draft {
        crate::ops::audit::Draft {
            request_id: m.request_id.clone(),
            phase: phase.into(),
            actor: m.actor.clone(),
            action: m.action.clone(),
            tenant: Some(id.to_string()),
            method: m.method.clone(),
            path: m.path.clone(),
            before: audit.before.clone(),
            after: audit.after.clone(),
            classification: audit.classification.clone(),
            ..Default::default()
        }
    }

    async fn mutate_inner<F>(
        &self,
        id: &str,
        m: &Mutation,
        edit: F,
        audit: &mut AuditState,
    ) -> ServiceResult<Outcome>
    where
        F: FnOnce(Option<&TenantDecl>) -> ServiceResult<Option<TenantDecl>>,
    {
        validate_id(id).map_err(|e| ServiceError::new(400, "invalid_id", format!("{e:#}")))?;
        let _mutation = self.ctx.mutation_lock.lock().await;
        let mut tx = self.ctx.reloader.begin().await;
        let root =
            crate::ch_emitter::load_effective(&self.ctx.ch_config, self.ctx.cli_base.clone())
                .await
                .map_err(ServiceError::internal)?;
        let tenants = TenantsConfig::from_table(&root)
            .map_err(|e| ServiceError::new(409, "invalid_config", format!("{e:#}")))?
            .ok_or_else(|| {
                ServiceError::new(
                    409,
                    "tenants_not_configured",
                    "no tenants configured: add a [tenants] section to manage tenants",
                )
            })?;
        let declared = tenants.get(id).cloned();
        let (stored, current) = self.read_stored(&tenants, id).await?;
        if declared.is_some() && current.is_none() {
            return Err(ServiceError::new(
                409,
                "managed_outside_api",
                format!(
                    "tenant {id} is declared outside the tenant registry; edit the operator config"
                ),
            ));
        }
        self.check_precondition(m, current.as_ref())?;
        let next = edit(current.as_ref())?;
        if let Some(next) = &next
            && next.id != id
        {
            return Err(ServiceError::new(
                400,
                "invalid_id",
                "the body names another tenant",
            ));
        }
        // The would-be config: this tenant replaced, everything else as is
        let mut preview = root.clone();
        if let Some(Value::Table(all)) = preview.get_mut("tenant") {
            all.remove(id);
        }
        if let Some(next) = &next {
            crate::ch_emitter::merge_tables(&mut preview, next.to_fragment());
        }
        crate::ops::tenant_ctl::validate(&preview)
            .map_err(|e| ServiceError::new(422, "invalid_config", format!("{e:#}")))?;
        let change = match (&current, &next) {
            (_, None) => Change::Live,
            (old, Some(new)) => classify_change(old.as_ref(), new, &root, &preview)
                .map_err(|e| ServiceError::new(422, "invalid_config", format!("{e:#}")))?,
        };
        if let Change::Rejected(why) = &change {
            return Err(ServiceError::new(409, "change_rejected", why.join("; ")));
        }
        let identity = match (&current, &next) {
            (Some(a), Some(b)) => a
                .identity_differs_across(b, &root, &preview)
                .unwrap_or(true),
            _ => false,
        };
        let legacy_ok = m.legacy_ack && (matches!(change, Change::FreshAttach) || identity);
        if change.resyncs() && !m.acknowledge_resync && !legacy_ok {
            return Err(ServiceError::new(
                409,
                "resync_not_acknowledged",
                format!(
                    "this change attaches tenant {id} afresh with fresh initial loads ({}); \
                     resend with acknowledge_resync",
                    rebind_reasons(&change)
                ),
            ));
        }
        let dir = self.ctx.spill_dir.as_deref().map(|s| tenant_dir(s, id));
        let attached = match &dir {
            Some(dir) => TenantState::load(dir)
                .await
                .map_err(ServiceError::internal)?
                .is_some_and(|s| s.phase != Phase::Detached),
            None => false,
        };
        let fence_rebind = matches!(change, Change::Rebind { .. }) && attached;
        if fence_rebind && let Some(dir) = &dir {
            // R3-M1: one re-bind in flight per tenant, so a second never
            // replaces the fence that protects the first
            if let Some(pending) = RebindFence::load(dir)
                .await
                .map_err(ServiceError::internal)?
            {
                return Err(ServiceError::new(
                    409,
                    "rebind_pending",
                    format!(
                        "tenant {id} has re-bind {} pending; retry once it has re-attached",
                        pending.request_id
                    ),
                ));
            }
        }
        if let Change::Rebind {
            fresh_state_dir: Some(state_dir),
            ..
        } = &change
        {
            self.check_fresh_state_dir(id, state_dir).await?;
        }
        let revision = tx.current();
        let mut outcome = Outcome {
            request_id: m.request_id.clone(),
            tenant: id.to_string(),
            change: change.clone(),
            before: current.clone(),
            after: next.clone(),
            etag: next.as_ref().and_then(|d| self.etag(d)),
            epoch: revision.epoch.to_string(),
            generation: revision.generation,
            dry_run: m.dry_run,
        };
        let (before, after) = crate::ops::redact::redact_pair(
            current.as_ref().map(TenantDecl::to_table).as_ref(),
            next.as_ref().map(TenantDecl::to_table).as_ref(),
        );
        audit.before = before;
        audit.after = after;
        audit.classification = Some(change.label().into());
        if m.dry_run || (change == Change::None && current == next) {
            return Ok(outcome);
        }
        // Write-ahead: nothing changes unless the attempt is durable
        if let Some(log) = self.ctx.reloader.audit() {
            log.append(self.draft(id, m, "attempt", audit))
                .await
                .map_err(|e| {
                    ServiceError::new(
                        503,
                        "audit_unavailable",
                        format!("the audit log refused the attempt, so nothing changed: {e:#}"),
                    )
                })?;
            audit.attempted = true;
        }
        let fence = match (&dir, fence_rebind, &next) {
            (Some(dir), true, Some(next)) => {
                let binding = next
                    .binding_digest(&preview)
                    .map_err(ServiceError::internal)?;
                let fence = RebindFence::new(&m.request_id, binding);
                fence.store(dir).await.map_err(ServiceError::internal)?;
                Some(dir.clone())
            }
            _ => None,
        };
        let written = match self.persist(&tx, &root, &stored, id, next.as_ref()).await {
            Ok(written) => written,
            Err(e) => {
                if let Some(dir) = &fence {
                    let _ = RebindFence::remove(dir).await;
                }
                return Err(e);
            }
        };
        let mut force_rebind = BTreeSet::new();
        if matches!(change, Change::Rebind { .. }) {
            force_rebind.insert(id.to_string());
        }
        let opts = PublishOpts {
            force_rebind,
            abort_on: (attached && next.is_some()).then(|| id.to_string()),
        };
        let cause = ReloadCause::Mutation {
            request_id: m.request_id.clone(),
            tenant: Some(id.to_string()),
        };
        match tx.publish(cause.clone(), opts).await {
            Ok(published) => {
                outcome.epoch = published.revision.epoch.to_string();
                outcome.generation = published.revision.generation;
                Ok(outcome)
            }
            Err(refused) => {
                let restored = self.restore(&tx, &root, &stored, &written, id).await;
                if let Some(dir) = &fence {
                    let _ = RebindFence::remove(dir).await;
                }
                if let Err(e) = tx.publish(cause, PublishOpts::default()).await {
                    tracing::warn!(
                        target: "walshadow::control",
                        tenant = %id,
                        error = %e,
                        "republishing after a rolled-back tenant change failed",
                    );
                }
                match restored {
                    Ok(()) => Err(ServiceError::new(
                        422,
                        "apply_refused",
                        format!("rolled back: {}", refusal(&refused)),
                    )),
                    Err(e) => Err(e),
                }
            }
        }
    }

    fn check_precondition(&self, m: &Mutation, current: Option<&TenantDecl>) -> ServiceResult<()> {
        match (&m.precondition, current) {
            (Precondition::Any, _) => Ok(()),
            (Precondition::Absent, Some(d)) => Err(ServiceError::new(
                409,
                "exists",
                format!("tenant {} already exists", d.id),
            )),
            (Precondition::Absent, None) => Ok(()),
            (Precondition::Etag(_), None) => {
                Err(ServiceError::new(404, "not_found", "no such tenant"))
            }
            (Precondition::Etag(want), Some(d)) => {
                let have = self.etag(d).unwrap_or_default();
                if &have == want {
                    Ok(())
                } else {
                    Err(ServiceError::new(
                        412,
                        "precondition_failed",
                        "the tenant changed since that ETag; read it again",
                    ))
                }
            }
        }
    }

    /// A Snowflake state directory a fingerprint change moves to: absent or
    /// empty, and nobody else's, attached or tearing down
    async fn check_fresh_state_dir(&self, id: &str, dir: &Path) -> ServiceResult<()> {
        match tokio::fs::read_dir(dir).await {
            Ok(mut entries) => {
                if entries
                    .next_entry()
                    .await
                    .map_err(ServiceError::internal)?
                    .is_some()
                {
                    return Err(ServiceError::new(
                        409,
                        "change_rejected",
                        format!(
                            "the new Snowflake state directory {} is not empty",
                            dir.display()
                        ),
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(ServiceError::internal(e)),
        }
        let wanted: BTreeSet<Resource> = [Resource::StateDir(dir.to_path_buf())].into();
        if let Some(c) = self
            .ctx
            .reloader
            .reservations()
            .conflicts(id, &wanted)
            .first()
        {
            return Err(ServiceError::new(409, "change_rejected", c.to_string()));
        }
        Ok(())
    }

    async fn read_stored(
        &self,
        tenants: &TenantsConfig,
        id: &str,
    ) -> ServiceResult<(Stored, Option<TenantDecl>)> {
        match &tenants.registry {
            Registry::Config => {
                let path = tenant_fragment_path(&self.ctx.ch_config, id);
                let bytes = match tokio::fs::read(&path).await {
                    Ok(b) => Some(b),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(ServiceError::internal(e)),
                };
                let decl = match &bytes {
                    Some(b) => {
                        let text = std::str::from_utf8(b).map_err(ServiceError::internal)?;
                        let table: Table = text.parse().map_err(ServiceError::internal)?;
                        let body = table
                            .get("tenant")
                            .and_then(|t| t.get(id))
                            .and_then(Value::as_table)
                            .ok_or_else(|| {
                                ServiceError::internal(format!(
                                    "{} holds no [tenant.{id}]",
                                    path.display()
                                ))
                            })?;
                        Some(TenantDecl::parse(id, body).map_err(ServiceError::internal)?)
                    }
                    None => None,
                };
                Ok((Stored::Fragment(path, bytes), decl))
            }
            Registry::Sql { schema, .. } => {
                let row = self.registry_row(schema, id).await?;
                let decl = match &row {
                    Some((dbname, spec, state)) => decls_from_rows(&[(
                        id.to_string(),
                        dbname.clone(),
                        spec.clone(),
                        state.clone(),
                    )])
                    .map_err(ServiceError::internal)?
                    .pop(),
                    None => None,
                };
                Ok((
                    Stored::Row {
                        schema: schema.clone(),
                        row,
                    },
                    decl,
                ))
            }
        }
    }

    async fn registry_client(&self) -> ServiceResult<tokio_postgres::Client> {
        let root =
            crate::ch_emitter::load_effective(&self.ctx.ch_config, self.ctx.cli_base.clone())
                .await
                .map_err(ServiceError::internal)?;
        let client = crate::control::pg_connect(&root, None)
            .await
            .map_err(|e| ServiceError::new(503, "registry_unavailable", format!("{e:#}")))?;
        client
            .batch_execute("SET statement_timeout = '10s'")
            .await
            .map_err(ServiceError::internal)?;
        Ok(client)
    }

    async fn registry_row(
        &self,
        schema: &str,
        id: &str,
    ) -> ServiceResult<Option<(String, String, String)>> {
        let client = self.registry_client().await?;
        client
            .batch_execute(&registry_ddl(schema))
            .await
            .map_err(ServiceError::internal)?;
        let s = crate::pg::quote_ident(schema);
        let row = client
            .query_opt(
                &format!("SELECT dbname, spec, state FROM {s}.tenant WHERE id = $1"),
                &[&id],
            )
            .await
            .map_err(ServiceError::internal)?;
        Ok(row.map(|r| (r.get(0), r.get(1), r.get(2))))
    }

    /// Write `next`, or remove the tenant, only if the record is still what
    /// was read. Returns what now stands, for a conditional restore
    async fn persist(
        &self,
        tx: &ReloadTx<'_>,
        root: &Table,
        stored: &Stored,
        id: &str,
        next: Option<&TenantDecl>,
    ) -> ServiceResult<Written> {
        match stored {
            Stored::Fragment(path, before) => {
                let now = match tokio::fs::read(path).await {
                    Ok(b) => Some(b),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(ServiceError::internal(e)),
                };
                if &now != before {
                    return Err(changed_since());
                }
                let bytes = match next {
                    Some(next) => {
                        let text =
                            toml::to_string(&next.to_fragment()).map_err(ServiceError::internal)?;
                        write_fragment(path, Some(text.as_bytes())).await?;
                        Some(text.into_bytes())
                    }
                    None => {
                        write_fragment(path, None).await?;
                        None
                    }
                };
                Ok(Written::Fragment(bytes))
            }
            Stored::Row { schema, row } => {
                let wanted = next.map(row_of);
                self.swap_row(schema, id, row.as_ref(), wanted.as_ref())
                    .await?;
                mirror_registry(tx, &self.ctx.ch_config, root, schema)
                    .await
                    .map_err(ServiceError::internal)?;
                Ok(Written::Row(wanted))
            }
        }
    }

    /// Put back what was there before, if what stands is still ours
    async fn restore(
        &self,
        tx: &ReloadTx<'_>,
        root: &Table,
        stored: &Stored,
        written: &Written,
        id: &str,
    ) -> ServiceResult<()> {
        match (stored, written) {
            (Stored::Fragment(path, before), Written::Fragment(ours)) => {
                let now = match tokio::fs::read(path).await {
                    Ok(b) => Some(b),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                    Err(e) => return Err(ServiceError::internal(e)),
                };
                if &now != ours {
                    return Err(ServiceError::new(
                        409,
                        "conflict_after_failed_apply",
                        format!(
                            "the tenant's resolver refused the change, and {} changed since; \
                             nothing was restored",
                            path.display()
                        ),
                    ));
                }
                write_fragment(path, before.as_deref()).await
            }
            (Stored::Row { schema, row }, Written::Row(ours)) => {
                self.swap_row(schema, id, ours.as_ref(), row.as_ref())
                    .await
                    .map_err(|e| {
                        if e.code == "precondition_failed" {
                            ServiceError::new(
                                409,
                                "conflict_after_failed_apply",
                                "the tenant's resolver refused the change, and its registry row \
                                 changed since; nothing was restored",
                            )
                        } else {
                            e
                        }
                    })?;
                mirror_registry(tx, &self.ctx.ch_config, root, schema)
                    .await
                    .map_err(ServiceError::internal)?;
                Ok(())
            }
            _ => Err(ServiceError::internal("record kind changed mid-mutation")),
        }
    }

    /// Compare-and-swap the registry row `id` from `from` to `to` in one
    /// transaction: zero rows touched means someone else changed it
    async fn swap_row(
        &self,
        schema: &str,
        id: &str,
        from: Option<&(String, String, String)>,
        to: Option<&(String, String, String)>,
    ) -> ServiceResult<()> {
        let mut client = self.registry_client().await?;
        client
            .batch_execute(&registry_ddl(schema))
            .await
            .map_err(ServiceError::internal)?;
        let s = crate::pg::quote_ident(schema);
        let txn = client.transaction().await.map_err(ServiceError::internal)?;
        let current = txn
            .query_opt(
                &format!("SELECT dbname, spec, state FROM {s}.tenant WHERE id = $1 FOR UPDATE"),
                &[&id],
            )
            .await
            .map_err(ServiceError::internal)?
            .map(|r| {
                (
                    r.get::<_, String>(0),
                    r.get::<_, String>(1),
                    r.get::<_, String>(2),
                )
            });
        if current.as_ref() != from {
            return Err(changed_since());
        }
        let touched = match (from, to) {
            (_, None) => txn
                .execute(&format!("DELETE FROM {s}.tenant WHERE id = $1"), &[&id])
                .await,
            (None, Some((dbname, spec, state))) => txn
                .execute(
                    &format!(
                        "INSERT INTO {s}.tenant (id, dbname, spec, state, updated_at) \
                         VALUES ($1, $2, $3, $4, now()) ON CONFLICT (id) DO NOTHING"
                    ),
                    &[&id, dbname, spec, state],
                )
                .await,
            (Some((d0, s0, st0)), Some((dbname, spec, state))) => txn
                .execute(
                    &format!(
                        "UPDATE {s}.tenant SET dbname = $5, spec = $6, state = $7, updated_at = now() \
                         WHERE id = $1 AND dbname = $2 AND spec = $3 AND state = $4"
                    ),
                    &[&id, d0, s0, st0, dbname, spec, state],
                )
                .await,
        }
        .map_err(|e| ServiceError::new(422, "invalid_config", format!("registry write: {e}")))?;
        if touched != 1 && !(from.is_none() && to.is_none()) {
            return Err(changed_since());
        }
        txn.commit().await.map_err(ServiceError::internal)?;
        Ok(())
    }
}

/// What the audit records of one mutation carry
#[derive(Default)]
struct AuditState {
    attempted: bool,
    before: serde_json::Value,
    after: serde_json::Value,
    classification: Option<String>,
}

enum Written {
    Fragment(Option<Vec<u8>>),
    Row(Option<(String, String, String)>),
}

fn changed_since() -> ServiceError {
    ServiceError::new(
        412,
        "precondition_failed",
        "the tenant changed while this request ran; read it again",
    )
}

fn refusal(e: &PublishError) -> String {
    match e {
        PublishError::TargetRefused { error, .. } => error.clone(),
        PublishError::Invalid(e) => format!("{e:#}"),
    }
}

fn rebind_reasons(change: &Change) -> String {
    match change {
        Change::Rebind { reasons, .. } => reasons.join("; "),
        Change::FreshAttach => "the tenant becomes active".into(),
        _ => String::new(),
    }
}

/// The registry row a declaration stores: `(dbname, spec, state)`
fn row_of(decl: &TenantDecl) -> (String, String, String) {
    let mut fragment = decl.to_fragment();
    let mut spec = fragment
        .get_mut("tenant")
        .and_then(|t| t.get_mut(&decl.id))
        .and_then(Value::as_table_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    spec.remove("dbname");
    spec.remove("state");
    let state = match decl.desired {
        Desired::Active => "active",
        Desired::Detached => "detached",
    };
    (
        decl.dbname.clone(),
        toml::to_string(&spec).unwrap_or_default(),
        state.into(),
    )
}

/// Write or delete a tenant fragment. The file's mode and write path stay
/// what `ctl tenant` always used (tracked separately from T-1021-01)
async fn write_fragment(path: &Path, bytes: Option<&[u8]>) -> ServiceResult<()> {
    match bytes {
        Some(bytes) => {
            if let Some(dir) = path.parent() {
                tokio::fs::create_dir_all(dir)
                    .await
                    .map_err(ServiceError::internal)?;
            }
            tokio::fs::write(path, bytes)
                .await
                .map_err(ServiceError::internal)
        }
        None => match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ServiceError::internal(e)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CliOverrides, ConfigResolver, ResolverBoot};
    use crate::control::Reloader;
    use crate::metrics::MetricsRegistry;

    const BASE: &str = "[source]\nhost = 'pg'\ndbname = 'postgres'\n[tenants]\n";

    /// `extra` lands inside `[tenant.<id>.ch]`
    fn ch_body(id: &str, database: &str, extra: &str) -> String {
        ch_tenant(id, database, "", extra)
    }

    /// `top` lands in `[tenant.<id>]`, `ch` inside `[tenant.<id>.ch]`
    fn ch_tenant(id: &str, database: &str, top: &str, ch: &str) -> String {
        format!(
            "[tenant.{id}]\ndbname = '{id}_db'\n{top}[tenant.{id}.ch]\nhost = 'ch'\ndatabase = '{database}'\n{ch}"
        )
    }

    struct Fixture {
        dir: tempfile::TempDir,
        svc: TenantService,
    }

    impl Fixture {
        async fn new(fragments: &[(&str, String)]) -> Self {
            let dir = tempfile::tempdir().unwrap();
            let config = dir.path().join("ch-config.toml");
            std::fs::write(&config, BASE).unwrap();
            std::fs::create_dir_all(dir.path().join("ch-config.d")).unwrap();
            for (id, body) in fragments {
                std::fs::write(tenant_fragment_path(&config, id), body).unwrap();
            }
            let ctx = SharedCtx {
                fragment: crate::control::fragment_path(&config, "50-api.toml").unwrap(),
                ch_config: config.clone(),
                cli_base: Table::new(),
                metrics: MetricsRegistry::new(),
                reloader: Arc::new(Reloader::new(Some(config), Table::new())),
                mutation_lock: crate::control::mutation_lock(),
                spill_dir: Some(dir.path().join("spill")),
            };
            let svc = TenantService::new(ctx, Some(Arc::new(EtagKey::new([7; 32]))));
            Self { dir, svc }
        }

        fn frag(&self, id: &str) -> String {
            std::fs::read_to_string(tenant_fragment_path(&self.svc.ctx.ch_config, id))
                .unwrap_or_default()
        }

        fn tenant_dir(&self, id: &str) -> PathBuf {
            tenant_dir(&self.dir.path().join("spill"), id)
        }

        /// The tenant as a running daemon would leave it on disk
        async fn attach(&self, id: &str) {
            let mut state = TenantState::new(id, &format!("{id}_db"), 7);
            state.phase = Phase::Active;
            state.store(&self.tenant_dir(id)).await.unwrap();
        }

        async fn current(&self, id: &str) -> TenantDecl {
            let root = self.root().await;
            TenantsConfig::from_table(&root)
                .unwrap()
                .unwrap()
                .get(id)
                .unwrap()
                .clone()
        }

        async fn root(&self) -> Table {
            crate::ch_emitter::load_effective(&self.svc.ctx.ch_config, Table::new())
                .await
                .unwrap()
        }

        /// A live resolver for `id`, registered like an attached tenant's
        async fn register(&self, id: &str) -> Arc<ConfigResolver> {
            let root = self.root().await;
            let decl = self.current(id).await;
            let base =
                crate::ch_emitter::EmitterConfig::from_table(&decl.effective_table(&root)).unwrap();
            let (r, _) = ConfigResolver::new(
                &base,
                CliOverrides::default(),
                Some(self.svc.ctx.ch_config.clone()),
                Table::new(),
                crate::mapping::mapping_handle(Default::default()),
                ResolverBoot::default(),
            );
            r.bind_tenant(id);
            self.svc
                .ctx
                .reloader
                .set_tenant_resolver(id, Some(r.clone()))
                .await;
            r
        }

        fn decl(&self, body: &str, id: &str) -> TenantDecl {
            let t: Table = body.parse().unwrap();
            TenantDecl::parse(id, t["tenant"][id].as_table().unwrap()).unwrap()
        }

        async fn put(&self, id: &str, body: &str, m: &Mutation) -> ServiceResult<Outcome> {
            let next = self.decl(body, id);
            self.svc.mutate(id, m, |_| Ok(Some(next))).await
        }
    }

    fn acked(m: Mutation) -> Mutation {
        Mutation {
            acknowledge_resync: true,
            ..m
        }
    }

    #[tokio::test]
    async fn create_replace_delete_with_etags() {
        let f = Fixture::new(&[]).await;
        let mut create = Mutation::new("req-1");
        create.precondition = Precondition::Absent;
        let out = f.put("b", &ch_body("b", "b", ""), &create).await.unwrap();
        assert_eq!(out.change, Change::Live);
        assert!(out.generation > 0);
        assert!(f.frag("b").contains("database = \"b\""));
        let etag = out.etag.clone().unwrap();
        assert!(etag.starts_with("r-") && etag.len() == 34);
        // Again: it exists
        let err = f
            .put("b", &ch_body("b", "b", ""), &create)
            .await
            .unwrap_err();
        assert_eq!((err.status, err.code), (409, "exists"));

        // Replace under the right ETag, then a stale one
        let mut replace = Mutation::new("req-2");
        replace.precondition = Precondition::Etag(etag.clone());
        let body = ch_body(
            "b",
            "b",
            "[tenant.b.table.public.orders]\nreplicate = true\n",
        );
        let out = f.put("b", &body, &replace).await.unwrap();
        assert_eq!(out.change, Change::Live);
        assert_ne!(out.etag.as_deref(), Some(etag.as_str()));
        let err = f.put("b", &body, &replace).await.unwrap_err();
        assert_eq!((err.status, err.code), (412, "precondition_failed"));

        // Delete, then nothing left to address
        let mut delete = Mutation::new("req-3");
        delete.precondition = Precondition::Etag(out.etag.unwrap());
        f.svc.mutate("b", &delete, |_| Ok(None)).await.unwrap();
        assert!(f.frag("b").is_empty());
        let err = f.svc.mutate("b", &delete, |_| Ok(None)).await.unwrap_err();
        assert_eq!(err.status, 404);
    }

    #[tokio::test]
    async fn etags_change_with_secrets_but_reveal_nothing() {
        let key = EtagKey::new([1; 32]);
        let f = Fixture::new(&[]).await;
        let one = f.decl(&ch_body("a", "a", "password = 'one'\n"), "a");
        let two = f.decl(&ch_body("a", "a", "password = 'two'\n"), "a");
        assert_ne!(key.etag(&one), key.etag(&two));
        // Not a plain hash of the declaration
        use sha2::Digest;
        let plain = hex::encode(sha2::Sha256::digest(
            toml::to_string(&one.to_fragment()).unwrap().as_bytes(),
        ));
        assert!(!plain.starts_with(&key.etag(&one)[2..]));
        // Stable under the same key, different under another
        assert_eq!(key.etag(&one), key.etag(&one));
        assert_ne!(key.etag(&one), EtagKey::new([2; 32]).etag(&one));
    }

    #[tokio::test]
    async fn etag_key_persists_privately() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let a = EtagKey::load_or_create(dir.path()).await.unwrap();
        let b = EtagKey::load_or_create(dir.path()).await.unwrap();
        assert_eq!(a.0, b.0);
        let mode = std::fs::metadata(dir.path().join("etag.key"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::write(dir.path().join("etag.key"), b"short").unwrap();
        assert!(EtagKey::load_or_create(dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn every_route_to_a_fresh_attach_needs_acknowledgement() {
        let f = Fixture::new(&[("a", ch_tenant("a", "a", "state = 'detached'\n", ""))]).await;
        let active = ch_body("a", "a", "");
        // PUT or PATCH alike: detached to active resyncs
        let err = f.put("a", &active, &Mutation::new("r1")).await.unwrap_err();
        assert_eq!(err.code, "resync_not_acknowledged");
        let err = f
            .svc
            .mutate("a", &Mutation::new("r2"), |cur| {
                let mut next = cur.unwrap().clone();
                next.desired = Desired::Active;
                Ok(Some(next))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, "resync_not_acknowledged");
        assert!(f.frag("a").contains("detached"), "nothing persisted");
        // The control socket attaches as ctl always did
        let mut socket = Mutation::new("r3");
        socket.legacy_ack = true;
        let out = f.put("a", &active, &socket).await.unwrap();
        assert_eq!(out.change, Change::FreshAttach);
        assert!(!f.frag("a").contains("detached"));
    }

    #[tokio::test]
    async fn a_newly_recognised_rebind_needs_acknowledgement_even_from_ctl() {
        let f = Fixture::new(&[("a", ch_body("a", "a", ""))]).await;
        f.attach("a").await;
        let moved = ch_body("a", "b", "");
        let mut socket = Mutation::new("r1");
        socket.legacy_ack = true;
        let err = f.put("a", &moved, &socket).await.unwrap_err();
        assert_eq!(err.code, "resync_not_acknowledged");
        assert!(err.message.contains("ClickHouse databases"), "{err}");
        let out = f.put("a", &moved, &acked(socket)).await.unwrap();
        assert!(matches!(out.change, Change::Rebind { .. }));
        let rev = f.svc.ctx.reloader.current();
        assert!(rev.force_rebind.contains("a"));
    }

    /// R3-M1 (Sol plan review r3): one re-bind in flight per tenant. An
    /// accepted A to B re-bind writes its fence; a second, B to C, is
    /// refused while that fence stands and writes none of its own, so a
    /// crash anywhere in the second request boots the tenant afresh
    /// against B and never resumes A's state against it
    #[tokio::test]
    async fn rebind_fence_survives_crash_during_second_rebind() {
        let f = Fixture::new(&[("a", ch_body("a", "a", ""))]).await;
        f.attach("a").await;
        let dir = f.tenant_dir("a");
        let first = f
            .put(
                "a",
                &ch_body("a", "b", ""),
                &acked(Mutation::new("rebind-b")),
            )
            .await
            .unwrap();
        assert!(matches!(first.change, Change::Rebind { .. }));
        let fence = RebindFence::load(&dir)
            .await
            .unwrap()
            .expect("fence written");
        assert_eq!(fence.request_id, "rebind-b");
        assert!(f.frag("a").contains("database = \"b\""));

        // Not yet reconciled: the second re-bind is refused first
        let err = f
            .put(
                "a",
                &ch_body("a", "c", ""),
                &acked(Mutation::new("rebind-c")),
            )
            .await
            .unwrap_err();
        assert_eq!((err.status, err.code), (409, "rebind_pending"));
        assert!(err.message.contains("rebind-b"), "{err}");
        assert_eq!(RebindFence::load(&dir).await.unwrap(), Some(fence.clone()));
        assert!(f.frag("a").contains("database = \"b\""), "B stands");
        // A live edit is still allowed
        f.put(
            "a",
            &ch_body("a", "b", "[tenant.a.table.public.t]\nreplicate = true\n"),
            &Mutation::new("live"),
        )
        .await
        .unwrap();
        assert_eq!(RebindFence::load(&dir).await.unwrap(), Some(fence.clone()));

        // Crash, then boot: the persisted state is still A's
        let state = TenantState::load(&dir).await.unwrap().unwrap();
        let root = f.root().await;
        let decl = f.current("a").await;
        let fence = RebindFence::load(&dir).await.unwrap();
        assert!(matches!(
            crate::tenants::boot_resume(&state, &decl, &root, fence.as_ref()).unwrap(),
            crate::tenants::BootResume::Fresh(why) if why.contains("rebind-b")
        ));
    }

    #[tokio::test]
    async fn refused_changes_persist_nothing() {
        let f = Fixture::new(&[("a", ch_body("a", "a", ""))]).await;
        let before = f.frag("a");
        for (extra, needle) in [
            ("decoder_batch_size = 10\n", "session-wide"),
            ("not_a_setting = 1\n", "unclassified"),
        ] {
            let err = f
                .put("a", &ch_body("a", "a", extra), &acked(Mutation::new("x")))
                .await
                .unwrap_err();
            assert_eq!((err.status, err.code), (409, "change_rejected"), "{extra}");
            assert!(err.message.contains(needle), "{err}");
            assert_eq!(f.frag("a"), before);
        }
        // Invalid as a whole: two tenants on one database
        let f = Fixture::new(&[("a", ch_body("a", "a", "")), ("b", ch_body("b", "b", ""))]).await;
        let err = f
            .put("b", &ch_body("b", "a", ""), &acked(Mutation::new("x")))
            .await
            .unwrap_err();
        assert_eq!(err.code, "invalid_config");
        // A dry run classifies and writes nothing
        let mut dry = Mutation::new("dry");
        dry.dry_run = true;
        let out = f
            .put("a", &ch_body("a", "z", ""), &acked(dry))
            .await
            .unwrap();
        assert!(out.dry_run && matches!(out.change, Change::Rebind { .. }));
        assert!(f.frag("a").contains("database = 'a'"), "unchanged");
    }

    #[tokio::test]
    async fn a_refused_live_change_rolls_back_before_publishing() {
        let f = Fixture::new(&[("a", ch_body("a", "a", ""))]).await;
        f.attach("a").await;
        let r = f.register("a").await;
        let before = f.frag("a");
        let generation = f.svc.ctx.reloader.current().generation;
        r.refuse_reloads(Some(Arc::new(|| {})));
        let err = f
            .put(
                "a",
                &ch_body("a", "a", "[tenant.a.table.public.t]\nreplicate = true\n"),
                &Mutation::new("live"),
            )
            .await
            .unwrap_err();
        assert_eq!((err.status, err.code), (422, "apply_refused"));
        assert_eq!(f.frag("a"), before, "restored");
        // What published after the refusal is the restored config, never
        // the candidate
        let rev = f.svc.ctx.reloader.current();
        assert!(rev.generation > generation);
        assert!(!toml::to_string(&*rev.merged).unwrap().contains("public"));
    }

    #[tokio::test]
    async fn a_refusal_never_overwrites_a_change_made_meanwhile() {
        let f = Fixture::new(&[("a", ch_body("a", "a", ""))]).await;
        f.attach("a").await;
        let r = f.register("a").await;
        let path = tenant_fragment_path(&f.svc.ctx.ch_config, "a");
        let hand = ch_tenant("a", "a", "max_lag_bytes = 5\n", "");
        let edit = hand.clone();
        r.refuse_reloads(Some(Arc::new(move || {
            std::fs::write(&path, &edit).unwrap();
        })));
        let err = f
            .put(
                "a",
                &ch_body("a", "a", "[tenant.a.table.public.t]\nreplicate = true\n"),
                &Mutation::new("live"),
            )
            .await
            .unwrap_err();
        assert_eq!((err.status, err.code), (409, "conflict_after_failed_apply"));
        assert_eq!(f.frag("a"), hand, "the hand edit stands");
    }

    #[tokio::test]
    async fn a_tenant_outside_the_registry_is_not_ours_to_edit() {
        let f = Fixture::new(&[]).await;
        let config = f.svc.ctx.ch_config.clone();
        std::fs::write(&config, format!("{BASE}{}", ch_body("ops", "ops", ""))).unwrap();
        let err = f
            .put(
                "ops",
                &ch_tenant("ops", "ops", "max_lag_bytes = 1\n", ""),
                &Mutation::new("x"),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, "managed_outside_api");
    }

    fn snowflake_body(id: &str, database: &str, state_dir: &Path) -> String {
        format!(
            r#"[tenant.{id}]
dbname = "{id}_db"
[tenant.{id}.destination]
kind = "snowflake"
[tenant.{id}.snowflake]
account_url = "https://acme-test.snowflakecomputing.com"
user = "REPLICATOR"
role = "REPLICATION"
warehouse = "INGEST"
database = "{database}"
[tenant.{id}.snowflake.auth]
method = "oauth"
token_file = "/run/secrets/snowflake"
[tenant.{id}.snowflake.state]
directory = "{}"
max_bytes = 10737418240
[tenant.{id}.snowflake.stage]
bucket = "replication"
prefix = "{id}/"
region = "us-east-1"
name = "{database}.INTERNAL.LOAD_STAGE"
"#,
            state_dir.display()
        )
    }

    #[tokio::test]
    async fn a_snowflake_rebind_takes_a_fresh_state_directory() {
        let state = tempfile::tempdir().unwrap();
        let old_dir = state.path().join("old");
        let f = Fixture::new(&[("a", snowflake_body("a", "A_DB", &old_dir))]).await;
        f.attach("a").await;
        // Same state directory: the reopened store would refuse its identity
        let err = f
            .put(
                "a",
                &snowflake_body("a", "B_DB", &old_dir),
                &acked(Mutation::new("x")),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code, "change_rejected");
        assert!(err.message.contains("state.directory"), "{err}");
        // A fresh directory that already holds files
        let used = state.path().join("used");
        std::fs::create_dir_all(&used).unwrap();
        std::fs::write(used.join("CURRENT"), b"x").unwrap();
        let err = f
            .put(
                "a",
                &snowflake_body("a", "B_DB", &used),
                &acked(Mutation::new("x")),
            )
            .await
            .unwrap_err();
        assert!(err.message.contains("not empty"), "{err}");
        // Fresh and empty: a re-bind
        let out = f
            .put(
                "a",
                &snowflake_body("a", "B_DB", &state.path().join("new")),
                &acked(Mutation::new("x")),
            )
            .await
            .unwrap();
        assert!(matches!(
            out.change,
            Change::Rebind { fresh_state_dir: Some(ref d), .. } if d.ends_with("new")
        ));
        // Live Snowflake settings stay live
        let body =
            snowflake_body("a", "B_DB", &state.path().join("new")).replace("INGEST", "OTHER_WH");
        let out = f.put("a", &body, &Mutation::new("wh")).await;
        // The re-bind's fence is pending, but a live change is no re-bind
        assert_eq!(out.unwrap().change, Change::Live);
    }

    #[tokio::test]
    async fn tenants_mode_keeps_pause_cluster_wide() {
        let with = |stream: &str| -> Table {
            format!("{BASE}{stream}{}", ch_body("a", "a", ""))
                .parse()
                .unwrap()
        };
        assert!(TenantsConfig::from_table(&with("[stream]\npaused = true\n")).is_ok());
        let err = TenantsConfig::from_table(&with("[stream]\nreplicate_all = true\n"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("only `paused` stays cluster-wide"), "{err}");
    }
}
