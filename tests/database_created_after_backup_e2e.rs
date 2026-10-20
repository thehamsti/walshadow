//! A database created after the daemon built its shadow, which therefore
//! never held it, replicates as a tenant once the shadow replays its
//! creation: the tenant attaches live beside one already streaming

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use walshadow::shadow::Shadow;

const LATE_ROWS: i32 = 32;

fn psql_db(source: &Shadow, db: &str, sql: &str) -> Result<()> {
    let out = Command::new("psql")
        .args([
            "-h",
            source.config().socket_dir.to_str().unwrap(),
            "-p",
            &source.config().port.to_string(),
            "-U",
            "postgres",
            "-d",
            db,
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .context("spawn psql")?;
    anyhow::ensure!(
        out.status.success(),
        "psql -d {db} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

/// Tenant `id` replicating `db`'s `public.<table>` into ClickHouse database `id`
fn tenant(id: &str, db: &str, table: &str, ch_port: u16) -> String {
    format!(
        "[tenant.{id}]\n\
         dbname = \"{db}\"\n\
         [tenant.{id}.ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"{id}\"\n\
         compression = \"lz4\"\n\
         [tenant.{id}.stream]\n\
         replicate_all = false\n\
         [tenant.{id}.table.\"public\".\"{table}\"]\n\
         replicate = true\n\
         target_table = \"{table}\"\n"
    )
}

fn write_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[source]\n\
         dbname = \"postgres\"\n\
         \n\
         [tenants]\n\
         stall_timeout_secs = 120\n\
         bridge_workers = 1\n\
         capacity = 8\n\
         \n\
         {}",
        tenant("main", "postgres", "early", ch_port),
    );
    fs::write(path, body).context("write ch-config")?;
    fs::create_dir_all(path.with_extension("d")).context("create ch-config.d")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn database_created_after_boot_replicates_once_replayed() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    psql_db(
        &source,
        "postgres",
        "CREATE TABLE public.early(id int primary key, name text); \
         INSERT INTO public.early VALUES (1, 'before-boot');",
    )
    .expect("load postgres workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    write_config(&ch_config_path, slot.ch_tcp).expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(&source, &ch_config_path, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let pid = child.id();
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(180))
            .context("daemon metrics endpoint never came up")?;
        // A tenant's start scope applies at its database's next commit
        psql_db(
            &source,
            "postgres",
            "INSERT INTO public.early VALUES (2, 'live');",
        )
        .context("live insert into postgres")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM main.early FINAL WHERE _is_deleted = 0",
            "2",
            Duration::from_secs(120),
        )
        .context("postgres's existing and live rows never reached CH")?;

        // The shadow was built before `app` existed
        source
            .apply_schema_dump("CREATE DATABASE app;\n")
            .context("create app after boot")?;
        psql_db(
            &source,
            "app",
            &format!(
                "CREATE TABLE public.late(id int primary key, name text); \
                 INSERT INTO public.late SELECT g, 'after-boot' FROM generate_series(1, {LATE_ROWS}) g;"
            ),
        )
        .context("load app workload")?;
        fs::write(
            ch_config_path
                .with_extension("d")
                .join("60-tenant-app.toml"),
            tenant("app", "app", "late", slot.ch_tcp),
        )
        .context("write app tenant fragment")?;
        let hup = Command::new("kill")
            .args(["-HUP", &pid.to_string()])
            .status()
            .context("kill -HUP")?;
        anyhow::ensure!(hup.success(), "kill -HUP {pid} failed");

        // Live writes give the new tenant commits to apply its start scope
        // at; each lands once the tenant streams, beside the existing rows
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut live = 0;
        loop {
            live += 1;
            psql_db(
                &source,
                "app",
                &format!(
                    "INSERT INTO public.late VALUES ({}, 'live');",
                    900_000 + live
                ),
            )
            .context("live insert into app")?;
            let existing = ch
                .query("SELECT count() FROM app.late FINAL WHERE _is_deleted = 0 AND id < 900000")
                .unwrap_or_default();
            if existing == LATE_ROWS.to_string() {
                break;
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "rows of the database created after boot never reached CH (have {existing:?})"
            );
            std::thread::sleep(Duration::from_secs(2));
        }
        live += 1;
        psql_db(
            &source,
            "app",
            &format!(
                "INSERT INTO public.late VALUES ({}, 'live');",
                900_000 + live
            ),
        )
        .context("live insert into app")?;
        fx::wait_for_ch_value(
            &ch,
            &format!(
                "SELECT count() FROM app.late FINAL WHERE id = {}",
                900_000 + live
            ),
            "1",
            Duration::from_secs(90),
        )
        .context("live CDC into app never reached CH")?;
        psql_db(
            &source,
            "postgres",
            "INSERT INTO public.early VALUES (3, 'live');",
        )
        .context("second live insert into postgres")?;
        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM main.early FINAL WHERE _is_deleted = 0",
            "3",
            Duration::from_secs(90),
        )
        .context("the first tenant stopped streaming after the attach")?;
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
