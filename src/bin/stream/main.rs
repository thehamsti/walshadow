//! `walshadow-stream` — full WAL capture pipeline.
//!
//! Connects to source PG in replication mode, `IDENTIFY_SYSTEM` then
//! `START_REPLICATION PHYSICAL` (optionally bound to a permanent slot),
//! filters every WAL byte, writes filtered segments shadow PG reads via
//! `restore_command`.
//!
//! ```text
//! walshadow-stream \
//!     --host /tmp/source_sock --port 5432 --user postgres --dbname postgres \
//!     --shadow-socket-dir /tmp/shadow_sock --shadow-port 5433 \
//!     --bootstrap-shadow-data-dir /var/lib/walshadow/shadow \
//!     --walsender-bind 127.0.0.1:5434 \
//!     --out-dir /var/lib/walshadow/filtered \
//!     --spill-dir /var/lib/walshadow/spill \
//!     [--slot walshadow_phys] \
//!     [--start-lsn 0/16B3750] \
//!     [--metrics-bind 127.0.0.1:9484] \
//!     [--retention-bytes 268435456]
//! ```

#[cfg(not(target_os = "linux"))]
compile_error!(
    "walshadow-stream is supported only on Linux; the PostgreSQL bridge extension may still be built separately with `make -C pgext`"
);

// The pipeline allocates rows on the decode thread(s) and frees them on the
// batcher thread; mimalloc's per-thread caches handle that produce-here/
// free-there pattern far better than glibc's shared arena (which serializes on
// its arena lock under that cross-thread churn).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Enable every callsite so coverage runs evaluate tracing field expressions
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn enable_tracing() {
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
}

mod api;
mod archive;
mod args;
mod bootstrap;
mod housekeeping;
mod metrics_publish;
mod runtime_cfg;
mod session;
mod shadow_proc;
mod sinks;
mod source_db;
mod source_recovery;
mod tenant;
mod tracing_setup;

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use walshadow::boundary_hold::AttachSlot;
use walshadow::metrics::MetricsRegistry;

use crate::args::{
    ApiDocsArgs, ApiKeyArgs, Args, InitArgs, cli_base, url_base, validate_transport_args,
};
use crate::runtime_cfg::spawn_shutdown_signals;
use crate::session::run_session;
use crate::tracing_setup::init_tracing;

fn main() -> Result<()> {
    // `ctl` client mode is detected before daemon-arg parsing so it needn't
    // supply the daemon's required args.
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some("ctl") {
        let rest = std::iter::once(format!("{} ctl", argv[0])).chain(argv.into_iter().skip(2));
        let (socket, command) = walshadow::ctl::Cli::parse_from(rest).into_parts()?;
        return runtime(NonZeroUsize::new(1))?.block_on(run_ctl(&socket, command));
    }
    if argv.get(1).map(String::as_str) == Some("api-key") {
        let rest = std::iter::once(format!("{} api-key", argv[0])).chain(argv.into_iter().skip(2));
        return api::mint(ApiKeyArgs::parse_from(rest).command);
    }
    if argv.get(1).map(String::as_str) == Some("api-docs") {
        let rest = std::iter::once(format!("{} api-docs", argv[0])).chain(argv.into_iter().skip(2));
        return api::docs(ApiDocsArgs::parse_from(rest));
    }
    if argv.get(1).map(String::as_str) == Some("init") {
        let rest = std::iter::once(format!("{} init", argv[0])).chain(argv.into_iter().skip(2));
        let opts = InitArgs::parse_from(rest).into_opts();
        return runtime(None)?.block_on(async {
            init_tracing(None);
            walshadow::init::run(opts).await
        });
    }
    let args = Args::parse();
    runtime(args.worker_threads)?.block_on(daemon(args))
}

fn runtime(workers: Option<NonZeroUsize>) -> std::io::Result<tokio::runtime::Runtime> {
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = workers {
        rt.worker_threads(n.get());
    }
    rt.enable_all().build()
}

async fn daemon(args: Args) -> Result<()> {
    walshadow::trace::set_sample_ratio(args.trace_sample_ratio);
    // `--otlp-endpoint` wins; otherwise honor the conventional env var.
    let otlp_endpoint = args
        .otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok());
    let tracer_provider = init_tracing(otlp_endpoint.as_deref());
    // Where and why this process stops, readable after a restart
    let crashes = walshadow::ops::crash::CrashStore::open(&api::state_dir(&args))?;
    crashes.install_panic_hook();
    if let Some(prior) = crashes.mark_running()? {
        tracing::warn!(target: "walshadow", record = %prior.id, "{}", prior.message);
    }
    let result = run(args, crashes.clone()).await;
    match &result {
        Ok(()) => crashes.clean_exit(),
        Err(e) => {
            let _ = crashes.record_exit(&format!("{e:#}"));
        }
    }
    // The batch span processor lives on a background thread, so a bare
    // process exit drops whatever it hasn't flushed. Drain it before we
    // return (best-effort — a failed flush must not mask `run`'s result).
    if let Some(provider) = tracer_provider
        && let Err(e) = provider.shutdown()
    {
        tracing::warn!(target: "walshadow", error = %e, "otlp tracer shutdown");
    }
    result
}

async fn run_ctl(socket: &Path, cmd: walshadow::ctl::Command) -> Result<()> {
    use std::io::{IsTerminal, Read};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = if cmd.reads_stdin && !std::io::stdin().is_terminal() {
        let mut raw = String::new();
        std::io::stdin().read_to_string(&mut raw)?;
        raw.parse().context("parse config body as TOML")?
    } else {
        cmd.body
    };
    let doc = walshadow::control::encode_request(&cmd.verb, body)?;
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect control socket {}", socket.display()))?;
    stream.write_all(doc.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await.ok();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await?;
    let (head, payload) = resp.split_once('\n').unwrap_or((resp.as_str(), ""));
    let Some(trailer) = head.strip_prefix("OK") else {
        eprint!("{resp}");
        std::process::exit(1);
    };
    if !trailer.trim().is_empty() {
        println!("{}", trailer.trim());
    }
    let rendered = walshadow::ctl::render(&cmd.verb, payload);
    if !rendered.trim().is_empty() {
        println!("{}", rendered.trim_end());
    }
    Ok(())
}

/// Process-lifetime entry: bind metrics + control socket + SIGHUP, then stream
/// one session. Every reconfigure (socket / SIGHUP) is a live reload — no
/// restart. SIGINT/SIGTERM break the pump loop and drain gracefully.
async fn run(mut args: Args, crashes: Arc<walshadow::ops::crash::CrashStore>) -> Result<()> {
    use walshadow::control::{Reloader, SharedCtx};

    args.url_base = url_base(&args)?;
    validate_transport_args(&args)?;
    let sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .inspect_err(|e| {
            tracing::warn!(
                target: "walshadow::sighup",
                error = %e,
                "SIGHUP install failed",
            );
        })?;
    let shutdown = spawn_shutdown_signals()?;

    let metrics = MetricsRegistry::new();
    let reloader = Arc::new(Reloader::new(args.ch_config.clone(), cli_base(&args)));
    reloader.set_spill_dir(args.spill_dir.clone());
    // Every control-plane mutation, control socket and HTTP API alike
    reloader.set_audit(
        walshadow::ops::audit::AuditLog::open(&api::state_dir(&args), 64 << 20, 10)
            .context("open the audit log")?,
    );
    let api_config = api::config(&args)?;

    let _metrics_server = if let Some(addr) = args.metrics_bind {
        let (bound, h) = walshadow::metrics::serve(addr, metrics.clone())
            .await
            .context("bind metrics endpoint")?;
        tracing::info!(target: "walshadow::metrics", addr = %bound, "metrics endpoint serving");
        Some(h)
    } else {
        None
    };

    // The control socket and the HTTP API share one context: one mutation
    // lock, one reload pipeline
    let ctx = match args.ch_config.clone() {
        Some(ch_config) => Some(SharedCtx {
            fragment: walshadow::control::fragment_path(&ch_config, &args.control_fragment)?,
            ch_config,
            cli_base: cli_base(&args),
            metrics: metrics.clone(),
            reloader: reloader.clone(),
            mutation_lock: walshadow::control::mutation_lock(),
            spill_dir: Some(args.spill_dir.clone()),
        }),
        None => None,
    };
    let _control_server = if let Some(sock) = args.control_socket.clone() {
        let ctx = ctx
            .clone()
            .context("--control-socket requires --ch-config")?;
        Some(
            walshadow::control::serve(sock, ctx)
                .await
                .context("bind control socket")?,
        )
    } else {
        None
    };
    let _api_server = match api_config {
        Some(cfg) => {
            let ctx = ctx.clone().context("--api-bind requires --ch-config")?;
            Some(
                api::start(cfg, ctx, crashes)
                    .await
                    .context("start the HTTP API")?,
            )
        }
        None => None,
    };

    let mut sighup = sighup;
    loop {
        let attach: AttachSlot = Arc::default();
        let result = run_session(&args, &metrics, &reloader, sighup, &shutdown, &attach).await;
        let Some(attached) = attach.lock().expect("attach slot poisoned").take() else {
            return result;
        };
        metrics
            .update(|snap| snap.session_state = "restarting")
            .await;
        tracing::info!(
            target: "walshadow",
            database = %attached.name,
            oid = attached.oid,
            lsn = format_args!("{:#X}", attached.lsn),
            "restarting the session to follow a database the shadow now holds",
        );
        args.ignore_cursor = false;
        args.start_lsn = None;
        sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .context("reinstall SIGHUP")?;
    }
}
