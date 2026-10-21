//! Daemon side of the HTTP API: flags to config, key minting

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use walshadow::ops::api::keys::Scope;
use walshadow::ops::api::{ApiConfig, Cidr, Rates};

use crate::args::{ApiKeyCommand, Args};

/// Audit log, crash records, tombstones, ETag key
pub(crate) fn state_dir(args: &Args) -> PathBuf {
    args.api_state_dir
        .clone()
        .unwrap_or_else(|| args.spill_dir.join("api"))
}

/// `None` without `--api-bind`
pub(crate) fn config(args: &Args) -> Result<Option<ApiConfig>> {
    let Some(bind) = args.api_bind else {
        return Ok(None);
    };
    let Some(keys_file) = args.api_keys_file.clone() else {
        bail!("--api-bind needs --api-keys-file");
    };
    let mut rates = Rates::default();
    for spec in &args.api_rate_limit {
        rates.set(spec)?;
    }
    let trusted_proxies = args
        .api_trusted_proxies
        .iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| Cidr::parse(s))
        .collect::<Result<Vec<_>>>()?;
    let cfg = ApiConfig {
        bind,
        keys_file,
        tls: args.api_tls_cert.clone().zip(args.api_tls_key.clone()),
        allow_plaintext: args.api_allow_plaintext,
        state_dir: state_dir(args),
        public_livez: args.api_public_livez,
        trusted_proxies,
        rates,
        deleted_retention: Duration::from_secs(args.api_deleted_retention_days * 86_400),
        ready_grace: Duration::from_secs(args.api_ready_grace_secs),
        max_in_flight: 64,
        max_connections: 256,
    };
    cfg.check()?;
    Ok(Some(cfg))
}

/// Start the API over `ctx`, keys watched, tokens invalidated on change
pub(crate) async fn start(
    cfg: ApiConfig,
    ctx: walshadow::control::SharedCtx,
    crashes: Arc<walshadow::ops::crash::CrashStore>,
) -> Result<tokio::task::JoinHandle<()>> {
    let keys = walshadow::ops::api::keys::KeyStore::open(&cfg.keys_file)
        .context("load --api-keys-file")?;
    keys.spawn_watcher(Duration::from_secs(5));
    let reloader = ctx.reloader.clone();
    let state = walshadow::ops::api::ApiState::new(cfg, ctx, keys, Some(crashes)).await?;
    state.deletes.spawn_invalidator(reloader);
    let (_, handle) = walshadow::ops::api::serve(state).await?;
    Ok(handle)
}

/// `api-key new`: the token on stdout, once; the key file stanza on stderr
pub(crate) fn mint(cmd: ApiKeyCommand) -> Result<()> {
    match cmd {
        ApiKeyCommand::New {
            id,
            scopes,
            expires,
        } => {
            let scopes = scopes
                .iter()
                .map(|s| match s.as_str() {
                    "read" => Ok(Scope::Read),
                    "admin" => Ok(Scope::Admin),
                    other => bail!("--scope {other:?}: read or admin"),
                })
                .collect::<Result<Vec<_>>>()?;
            let (token, stanza) =
                walshadow::ops::api::keys::generate(&id, &scopes, expires.as_deref())?;
            eprintln!(
                "# Add to the --api-keys-file (chmod 600). The token below shows once.\n{stanza}"
            );
            println!("{token}");
            Ok(())
        }
    }
}
