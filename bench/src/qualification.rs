//! Gate measured, matched Snowflake and ClickHouse runs. This module does not
//! generate measurements or accept omitted evidence as a passing result.
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::collections::HashSet;

pub fn validate(document: &Value) -> Result<Vec<String>> {
    let workload = string(document, "workload_id")?;
    ensure!(!workload.is_empty(), "workload_id must be nonempty");
    let runs = document
        .get("runs")
        .and_then(Value::as_array)
        .context("missing runs array")?;
    ensure!(
        runs.len() >= 3,
        "at least three matched repetitions are required"
    );
    let mut ids = HashSet::new();
    let mut failures = Vec::new();
    let mut input_sha = None;
    for (index, run) in runs.iter().enumerate() {
        let label = format!("run {}", index + 1);
        let id = string(run, "run_id")?;
        ensure!(
            !id.is_empty() && ids.insert(id.to_owned()),
            "duplicate or empty run_id"
        );
        let sha = string(run, "source_input_sha256")?;
        ensure!(
            sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid source_input_sha256 in {label}"
        );
        if let Some(prior) = input_sha {
            ensure!(prior == sha, "source workload digest differs in {label}");
        }
        input_sha = Some(sha);
        let ch = run
            .get("clickhouse")
            .context(format!("missing clickhouse in {label}"))?;
        let sf = run
            .get("snowflake")
            .context(format!("missing snowflake in {label}"))?;
        let ch_rate = positive(ch, "throughput_rows_per_sec")?;
        let sf_rate = positive(sf, "throughput_rows_per_sec")?;
        let ratio = sf_rate / ch_rate;
        if ratio < 0.80 {
            failures.push(format!(
                "{label}: Snowflake throughput {:.1}% of ClickHouse (<80%)",
                ratio * 100.0
            ));
        }
        ensure!(
            integer(ch, "expected_rows")? == integer(sf, "expected_rows")?,
            "matched destinations have different expected row counts in {label}"
        );
        for (name, result) in [("ClickHouse", ch), ("Snowflake", sf)] {
            let expected = integer(result, "expected_rows")?;
            let actual = integer(result, "actual_rows")?;
            let missing = integer(result, "missing_rows")?;
            let resurrected = integer(result, "resurrected_rows")?;
            let mismatched = integer(result, "value_mismatches")?;
            if expected != actual || missing != 0 || resurrected != 0 || mismatched != 0 {
                failures.push(format!(
                    "{label} {name}: current-state values are not exact"
                ));
            }
            for field in ["ingest_usd", "warehouse_usd", "s3_usd", "network_usd"] {
                nonnegative(result, field).with_context(|| format!("{label} {name} cost"))?;
            }
        }
        let p95 = nonnegative(sf, "p95_freshness_ms")?;
        if p95 > 60_000.0 {
            failures.push(format!(
                "{label}: Snowflake p95 freshness {p95:.0}ms (>60000ms)"
            ));
        }
        let window = integer(sf, "backlog_window_secs")?;
        if window < 1_800 {
            failures.push(format!(
                "{label}: Snowflake backlog window {window}s (<1800s)"
            ));
        }
        let start = integer(sf, "backlog_start_rows")?;
        let end = integer(sf, "backlog_end_rows")?;
        if end > start {
            failures.push(format!(
                "{label}: Snowflake backlog grew from {start} to {end} rows"
            ));
        }
    }
    Ok(failures)
}

pub fn report(document: &Value) -> Result<String> {
    let failures = validate(document)?;
    if !failures.is_empty() {
        bail!("Snowflake qualification failed:\n{}", failures.join("\n"));
    }
    let runs = document["runs"].as_array().expect("validated runs");
    let min_ratio = runs
        .iter()
        .map(|run| {
            run["snowflake"]["throughput_rows_per_sec"]
                .as_f64()
                .unwrap()
                / run["clickhouse"]["throughput_rows_per_sec"]
                    .as_f64()
                    .unwrap()
        })
        .fold(f64::INFINITY, f64::min);
    Ok(format!(
        "PASS: {} matched repetitions; minimum Snowflake/ClickHouse throughput {:.1}%; Snowflake p95 <=60s; no net backlog growth over >=30min; exact values; all cost fields present",
        runs.len(),
        min_ratio * 100.0
    ))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing or invalid {key}"))
}
fn integer(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing or invalid {key}"))
}
fn nonnegative(value: &Value, key: &str) -> Result<f64> {
    let number = value
        .get(key)
        .and_then(Value::as_f64)
        .with_context(|| format!("missing or invalid {key}"))?;
    ensure!(number.is_finite() && number >= 0.0, "invalid {key}");
    Ok(number)
}
fn positive(value: &Value, key: &str) -> Result<f64> {
    let number = nonnegative(value, key)?;
    ensure!(number > 0.0, "{key} must be positive");
    Ok(number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn metric(rate: f64) -> Value {
        json!({
            "throughput_rows_per_sec":rate,"p95_freshness_ms":50000,
            "backlog_window_secs":1800,"backlog_start_rows":100,"backlog_end_rows":100,
            "expected_rows":1000,"actual_rows":1000,"missing_rows":0,"resurrected_rows":0,"value_mismatches":0,
            "ingest_usd":1.0,"warehouse_usd":2.0,"s3_usd":0.0,"network_usd":0.1
        })
    }
    fn document() -> Value {
        json!({"workload_id":"same-source-config","runs":(1..=3).map(|i| json!({
            "run_id":format!("run-{i}"),"source_input_sha256":"a".repeat(64),
            "clickhouse":metric(100.0),"snowflake":metric(80.0)
        })).collect::<Vec<_>>()})
    }
    #[test]
    fn three_measured_pairs_at_boundary_pass() {
        assert!(report(&document()).unwrap().starts_with("PASS:"));
    }
    #[test]
    fn every_gate_is_required() {
        let mut doc = document();
        doc["runs"][0]["snowflake"]["throughput_rows_per_sec"] = json!(79.9);
        doc["runs"][1]["snowflake"]["p95_freshness_ms"] = json!(60_001);
        doc["runs"][2]["snowflake"]["backlog_end_rows"] = json!(101);
        doc["runs"][2]["snowflake"]["missing_rows"] = json!(1);
        assert_eq!(validate(&doc).unwrap().len(), 4);
        assert!(report(&doc).is_err());
        doc["runs"][0]["snowflake"]
            .as_object_mut()
            .unwrap()
            .remove("network_usd");
        assert!(validate(&doc).is_err());
    }
    #[test]
    fn unmatched_or_insufficient_runs_fail() {
        let mut doc = document();
        doc["runs"].as_array_mut().unwrap().pop();
        assert!(validate(&doc).is_err());
        let mut doc = document();
        doc["runs"][1]["source_input_sha256"] = json!("b".repeat(64));
        assert!(validate(&doc).is_err());
        let mut doc = document();
        doc["runs"][1]["snowflake"]["expected_rows"] = json!(999);
        assert!(validate(&doc).is_err());
    }
}
