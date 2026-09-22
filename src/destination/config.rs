//! Destination-specific configuration. Credentials are referenced, never embedded.
use super::snowflake::http::{AuthConfig, HttpConfig};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationKind {
    #[default]
    #[serde(rename = "clickhouse")]
    ClickHouse,
    Snowflake,
}

#[derive(Debug, Clone)]
pub struct DestinationConfig {
    pub kind: DestinationKind,
    pub snowflake: Option<SnowflakeConfig>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SnowflakeConfig {
    pub account_url: String,
    pub user: String,
    pub role: String,
    pub warehouse: String,
    pub database: String,
    #[serde(default = "internal_schema")]
    pub internal_schema: String,
    pub auth: Authentication,
    pub state: StateConfig,
    pub stage: StageConfig,
    #[serde(default)]
    pub schema_mapping: BTreeMap<String, String>,
    #[serde(default = "channels")]
    pub channels_per_table: usize,
    #[serde(default = "concurrency")]
    pub max_in_flight: usize,
    #[serde(default = "row_budget")]
    pub batch_rows: usize,
    #[serde(default = "byte_budget")]
    pub batch_bytes: usize,
    #[serde(default = "flush_interval")]
    pub flush_interval_ms: u64,
    #[serde(default = "merge_interval")]
    pub merge_interval_ms: u64,
    /// Relations set up, prepared or published concurrently. These phases
    /// are metadata round trips, not warehouse work, so they scale past
    /// `max_in_flight`.
    #[serde(default = "metadata_concurrency")]
    pub metadata_concurrency: usize,
    /// Tables whose MERGE may run at once; bounds warehouse concurrency
    #[serde(default = "merge_concurrency")]
    pub merge_concurrency: usize,
    /// How long one SQL statement may run before delivery stops. A MERGE or
    /// COPY over a large backlog can take far longer than a status poll
    #[serde(default = "statement_timeout")]
    pub statement_timeout_secs: u64,
    /// When a live batch counts as delivered for the source slot
    #[serde(default)]
    pub ack_after: AckAfter,
}

/// Acknowledging at `outbox` lets the slot advance once a batch is fsynced
/// locally, so a Snowflake outage fills local disk (bounded by
/// `state.max_bytes`) instead of the primary's WAL. The state directory then
/// holds rows no other copy has: back it up. `apply` waits for the verified
/// apply receipt
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckAfter {
    #[default]
    Apply,
    Outbox,
}

#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum Authentication {
    Jwt {
        account: String,
        private_key_file: PathBuf,
        public_key_fingerprint: String,
    },
    Oauth {
        token_file: PathBuf,
    },
}
impl std::fmt::Debug for Authentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Jwt { .. } => "Jwt([redacted])",
            Self::Oauth { .. } => "Oauth([redacted])",
        })
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StateConfig {
    pub directory: PathBuf,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StageConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub name: String,
}

fn internal_schema() -> String {
    "WALSHADOW_INTERNAL".into()
}
fn channels() -> usize {
    8
}
fn concurrency() -> usize {
    16
}
fn row_budget() -> usize {
    50_000
}
fn byte_budget() -> usize {
    8 << 20
}
fn flush_interval() -> u64 {
    250
}
fn merge_interval() -> u64 {
    15_000
}
fn metadata_concurrency() -> usize {
    32
}
fn merge_concurrency() -> usize {
    4
}
fn statement_timeout() -> u64 {
    6 * 3600
}

impl DestinationConfig {
    pub fn parse(text: &str) -> Result<Self> {
        Self::from_table(&toml::from_str(text)?)
    }

    pub fn from_table(root: &toml::Table) -> Result<Self> {
        #[derive(Default, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Selection {
            #[serde(default)]
            kind: DestinationKind,
        }
        let selection: Selection = root
            .get("destination")
            .cloned()
            .map(|v| v.try_into())
            .transpose()
            .context("[destination]")?
            .unwrap_or_default();
        match selection.kind {
            DestinationKind::ClickHouse => {
                ensure!(
                    !root.contains_key("snowflake"),
                    "[snowflake] requires explicit destination.kind = 'snowflake'"
                );
                Ok(Self {
                    kind: selection.kind,
                    snowflake: None,
                })
            }
            DestinationKind::Snowflake => {
                ensure!(
                    !root.contains_key("ch"),
                    "[ch] cannot be combined with the Snowflake destination"
                );
                let config: SnowflakeConfig = root
                    .get("snowflake")
                    .context("Snowflake destination requires [snowflake]")?
                    .clone()
                    .try_into()?;
                config.validate()?;
                Ok(Self {
                    kind: selection.kind,
                    snowflake: Some(config),
                })
            }
        }
    }
}

impl SnowflakeConfig {
    pub fn validate(&self) -> Result<()> {
        let url = url::Url::parse(&self.account_url)?;
        ensure!(
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && (url.path().is_empty() || url.path() == "/"),
            "account_url must be a credential-free HTTPS origin"
        );
        for (label, value) in [
            ("user", &self.user),
            ("role", &self.role),
            ("warehouse", &self.warehouse),
            ("database", &self.database),
            ("internal_schema", &self.internal_schema),
        ] {
            ensure!(
                !value.trim().is_empty() && !value.contains('\0') && value.len() <= 255,
                "invalid Snowflake {label}"
            );
        }
        ensure!(
            !self.state.directory.as_os_str().is_empty() && self.state.max_bytes > 0,
            "Snowflake state directory and positive max_bytes required"
        );
        ensure!(
            self.channels_per_table > 0 && self.channels_per_table <= 2000,
            "channels_per_table must be 1..=2000"
        );
        ensure!(
            (1..=256).contains(&self.metadata_concurrency),
            "metadata_concurrency must be 1..=256"
        );
        ensure!(
            (1..=64).contains(&self.merge_concurrency),
            "merge_concurrency must be 1..=64"
        );
        ensure!(
            self.statement_timeout_secs >= 60,
            "statement_timeout_secs must be at least 60"
        );
        ensure!(
            self.max_in_flight > 0
                && self.batch_rows > 0
                && self.batch_bytes > 0
                && self.flush_interval_ms > 0
                && self.merge_interval_ms > 0,
            "Snowflake budgets and intervals must be positive"
        );
        ensure!(
            !self.stage.bucket.is_empty()
                && !self.stage.region.is_empty()
                && !self.stage.name.is_empty(),
            "S3 stage bucket, region and name required"
        );
        super::snowflake::sql::quote_qualified_ident(&self.stage.name)
            .map_err(anyhow::Error::msg)?;
        ensure!(
            !self.stage.prefix.starts_with('/')
                && !self.stage.prefix.split('/').any(|v| v == ".." || v == ".")
                && !self.stage.prefix.contains(['\0', '\\']),
            "invalid S3 stage prefix"
        );
        ensure!(
            !self.stage.prefix.is_empty(),
            "dedicated S3 stage prefix required"
        );
        for (source, target) in &self.schema_mapping {
            ensure!(
                !source.is_empty()
                    && !target.is_empty()
                    && target != &self.internal_schema
                    && !source.contains('\0')
                    && !target.contains('\0'),
                "invalid schema mapping"
            );
        }
        match &self.auth {
            Authentication::Jwt {
                account,
                private_key_file,
                public_key_fingerprint,
            } => ensure!(
                !account.is_empty()
                    && !private_key_file.as_os_str().is_empty()
                    && public_key_fingerprint.starts_with("SHA256:"),
                "JWT account, key file and SHA256 fingerprint required"
            ),
            Authentication::Oauth { token_file } => ensure!(
                !token_file.as_os_str().is_empty(),
                "OAuth token_file required"
            ),
        }
        Ok(())
    }

    pub fn fingerprint(&self) -> String {
        let mapping = serde_json::to_string(&self.schema_mapping).expect("serializable mapping");
        let fields = [
            &self.account_url,
            &self.database,
            &self.internal_schema,
            &mapping,
            &self.stage.bucket,
            &self.stage.prefix,
            &self.stage.name,
        ];
        let mut hash = Sha256::new();
        for field in fields {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field);
        }
        hex::encode(hash.finalize())
    }

    pub fn http_config(&self) -> Result<HttpConfig> {
        let auth = match &self.auth {
            Authentication::Jwt {
                account,
                private_key_file,
                public_key_fingerprint,
            } => AuthConfig::JwtKeyFile {
                account: account.clone(),
                user: self.user.clone(),
                private_key_path: private_key_file.clone(),
                public_key_fingerprint: public_key_fingerprint.clone(),
            },
            Authentication::Oauth { token_file } => AuthConfig::OAuthTokenFile {
                path: token_file.clone(),
            },
        };
        Ok(HttpConfig {
            account_url: url::Url::parse(&self.account_url)?,
            auth,
            user_agent: format!("walshadow/{}", env!("CARGO_PKG_VERSION")),
            database: Some(self.database.clone()),
            schema: Some(self.internal_schema.clone()),
            warehouse: Some(self.warehouse.clone()),
            role: Some(self.role.clone()),
            statement_timeout: std::time::Duration::from_secs(self.statement_timeout_secs),
        })
    }
}
