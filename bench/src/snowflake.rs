//! Query the same public current-state view that consumers read.
use crate::Destination;
use anyhow::{Context, Result, ensure};
use std::path::Path;
use uuid::Uuid;
use walshadow::destination::{
    config::DestinationConfig,
    snowflake::{http::SnowflakeHttp, sql::quote_qualified_ident},
};

pub struct SnowflakeDest {
    http: SnowflakeHttp,
    view: String,
}

impl SnowflakeDest {
    pub fn from_config(path: &Path, view: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path).context("read Snowflake benchmark config")?;
        let config = DestinationConfig::parse(&text)?
            .snowflake
            .context("benchmark config must select Snowflake")?;
        Ok(Self {
            http: SnowflakeHttp::new(config.http_config()?)?,
            view: quote_qualified_ident(view).map_err(anyhow::Error::msg)?,
        })
    }

    async fn count(&self, predicate: &str) -> Result<u64> {
        let result = self
            .http
            .execute_sql(
                &format!("SELECT COUNT(*) FROM {}{predicate}", self.view),
                Uuid::new_v4(),
            )
            .await?;
        parse_count(&result.rows)
    }
}

fn parse_count(rows: &[Vec<serde_json::Value>]) -> Result<u64> {
    ensure!(
        rows.len() == 1 && rows[0].len() == 1,
        "Snowflake count must return one cell"
    );
    rows[0][0]
        .as_u64()
        .or_else(|| rows[0][0].as_str().and_then(|v| v.parse().ok()))
        .context("invalid Snowflake count")
}

#[async_trait::async_trait]
impl Destination for SnowflakeDest {
    async fn preflight(&self) -> Result<()> {
        self.count_all().await.map(|_| ())
    }
    // The engine waits for zero rows after source TRUNCATE. Directly modifying
    // internal state would bypass the daemon's generation and replay barriers.
    async fn clear(&self) -> Result<()> {
        Ok(())
    }
    async fn count_id(&self, id: i64) -> Result<u64> {
        self.count(&format!(" WHERE \"id\" = {id}")).await
    }
    async fn count_all(&self) -> Result<u64> {
        self.count("").await
    }
    fn endpoint(&self) -> String {
        format!("Snowflake {}", self.view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn count_parser_rejects_ambiguous_or_lossy_results() {
        assert_eq!(
            parse_count(&[vec![json!("18446744073709551615")]]).unwrap(),
            u64::MAX
        );
        for rows in [
            vec![],
            vec![vec![json!("1.5")]],
            vec![vec![json!(-1)]],
            vec![vec![json!(1)], vec![json!(1)]],
        ] {
            assert!(parse_count(&rows).is_err());
        }
    }
}
