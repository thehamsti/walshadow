//! Validate measured Snowflake qualification evidence; never creates values.
use anyhow::{Context, Result, ensure};
use std::path::PathBuf;

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path: PathBuf = args
        .next()
        .context("usage: walshadow-snowflake-qualify MEASUREMENTS.json")?
        .into();
    ensure!(
        args.next().is_none(),
        "usage: walshadow-snowflake-qualify MEASUREMENTS.json"
    );
    let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let document: serde_json::Value =
        serde_json::from_slice(&bytes).context("parse measurements JSON")?;
    println!("{}", walshadow_bench::qualification::report(&document)?);
    Ok(())
}
