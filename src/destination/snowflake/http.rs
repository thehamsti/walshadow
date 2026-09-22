//! Snowflake SQL API and Snowpipe Streaming Named Channel transport.
use anyhow::{Context, Result, bail, ensure};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, Method, StatusCode, Url, redirect::Policy};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const MAX_NDJSON: usize = 4 * 1024 * 1024;
const MAX_ATTEMPTS: usize = 8;
/// Refresh cached credentials this long before they would expire
const CREDENTIAL_LIFETIME: Duration = Duration::from_secs(45 * 60);

#[derive(Clone)]
pub enum AuthConfig {
    JwtKeyFile {
        account: String,
        user: String,
        private_key_path: PathBuf,
        public_key_fingerprint: String,
    },
    OAuthTokenFile {
        path: PathBuf,
    },
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JwtKeyFile { .. } => f.write_str("JwtKeyFile { credentials: [redacted] }"),
            Self::OAuthTokenFile { .. } => {
                f.write_str("OAuthTokenFile { credentials: [redacted] }")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub account_url: Url,
    pub auth: AuthConfig,
    pub user_agent: String,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    /// Longest a statement may stay running before its caller fails
    pub statement_timeout: Duration,
}

#[derive(Clone)]
pub struct SnowflakeHttp {
    client: Client,
    /// Swappable so role, warehouse and credential paths change live
    config: std::sync::Arc<std::sync::RwLock<std::sync::Arc<HttpConfig>>>,
    cache: std::sync::Arc<CredentialCache>,
}

/// Signed JWTs and ingest scoped tokens are valid for about an hour; each
/// request re-signing or re-exchanging them cost extra round trips and a key
/// file read. OAuth token files are still re-read, so rotation stays live
#[derive(Default)]
struct CredentialCache {
    jwt: std::sync::Mutex<Option<(String, std::time::Instant)>>,
    ingest: tokio::sync::Mutex<Option<(String, String, std::time::Instant)>>,
}

#[derive(Clone, Debug)]
pub struct ChannelRef {
    pub database: String,
    pub schema: String,
    pub pipe: String,
    pub channel: String,
}

#[derive(Debug)]
pub struct SqlResult {
    pub statement_handle: String,
    pub rows: Vec<Vec<Value>>,
    pub row_type: Value,
}

#[derive(Debug)]
pub struct OpenedChannel {
    pub continuation_token: String,
    pub status: ChannelStatus,
}

#[derive(Debug)]
pub struct ChannelStatus {
    pub last_committed_offset_token: Option<String>,
    pub rows_inserted: u64,
    pub rows_parsed: u64,
    pub rows_errors: u64,
}

#[derive(Serialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

impl SnowflakeHttp {
    pub fn new(config: HttpConfig) -> Result<Self> {
        validate_endpoint(&config.account_url, false)?;
        ensure!(
            config.user_agent.is_ascii() && !config.user_agent.trim().is_empty(),
            "invalid Snowflake user agent"
        );
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            config: std::sync::Arc::new(std::sync::RwLock::new(std::sync::Arc::new(config))),
            cache: Default::default(),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(config: HttpConfig) -> Result<Self> {
        validate_endpoint(&config.account_url, true)?;
        let client = Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            client,
            config: std::sync::Arc::new(std::sync::RwLock::new(std::sync::Arc::new(config))),
            cache: Default::default(),
        })
    }

    /// Settings snapshot; cheap, and never held across an await
    fn cfg(&self) -> std::sync::Arc<HttpConfig> {
        self.config.read().unwrap().clone()
    }

    /// Swap role, warehouse and credential settings. The endpoint, database
    /// and schema stay: durable request ids and receipts are scoped to them.
    /// Cached credentials are dropped so the next request uses the new ones
    pub fn update_config(&self, next: HttpConfig) -> Result<()> {
        let current = self.cfg();
        ensure!(
            next.account_url == current.account_url
                && next.database == current.database
                && next.schema == current.schema,
            "Snowflake endpoint, database and schema are bound to durable state"
        );
        *self.config.write().unwrap() = std::sync::Arc::new(next);
        *self.cache.jwt.lock().unwrap() = None;
        if let Ok(mut ingest) = self.cache.ingest.try_lock() {
            *ingest = None;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn edit_config(&self, edit: impl FnOnce(&mut HttpConfig)) {
        let mut next = (*self.cfg()).clone();
        edit(&mut next);
        *self.config.write().unwrap() = std::sync::Arc::new(next);
    }

    async fn token(&self) -> Result<(String, &'static str)> {
        let config = self.cfg();
        match &config.auth {
            AuthConfig::OAuthTokenFile { path } => {
                let token = tokio::fs::read_to_string(path)
                    .await
                    .context("read Snowflake OAuth token file")?;
                let token = token.trim();
                ensure!(
                    !token.is_empty() && !token.chars().any(char::is_whitespace),
                    "invalid Snowflake OAuth token file"
                );
                Ok((token.to_owned(), "OAUTH"))
            }
            AuthConfig::JwtKeyFile {
                account,
                user,
                private_key_path,
                public_key_fingerprint,
            } => {
                if let Some((token, at)) = self.cache.jwt.lock().unwrap().as_ref()
                    && at.elapsed() < CREDENTIAL_LIFETIME
                {
                    return Ok((token.clone(), "KEYPAIR_JWT"));
                }
                ensure!(
                    !account.is_empty()
                        && !user.is_empty()
                        && public_key_fingerprint.starts_with("SHA256:"),
                    "invalid Snowflake JWT identity"
                );
                let key = tokio::fs::read(private_key_path)
                    .await
                    .context("read Snowflake private key")?;
                let key =
                    EncodingKey::from_rsa_pem(&key).context("parse Snowflake RSA private key")?;
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let identity = format!("{}.{}", account.to_uppercase(), user.to_uppercase());
                let claims = JwtClaims {
                    iss: format!("{identity}.{public_key_fingerprint}"),
                    sub: identity,
                    iat: now,
                    exp: now + 59 * 60,
                };
                let token = encode(&Header::new(Algorithm::RS256), &claims, &key)?;
                *self.cache.jwt.lock().unwrap() = Some((token.clone(), std::time::Instant::now()));
                Ok((token, "KEYPAIR_JWT"))
            }
        }
    }

    fn url(&self, path: &str) -> Result<Url> {
        Ok(self.cfg().account_url.join(path)?)
    }

    async fn send(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        bearer: Option<&str>,
        token_type: Option<&str>,
    ) -> Result<(StatusCode, Value)> {
        let (status, bytes) = self
            .send_bytes(method, url, body, bearer, token_type)
            .await?;
        let body = serde_json::from_slice(&bytes).context("Snowflake response was not JSON")?;
        Ok((status, body))
    }

    async fn send_bytes(
        &self,
        method: Method,
        url: Url,
        body: Option<Value>,
        bearer: Option<&str>,
        token_type: Option<&str>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let mut req = self
            .client
            .request(method, url)
            .header("Accept", "application/json")
            .header("User-Agent", &self.cfg().user_agent);
        if let Some(token) = bearer {
            req = req.bearer_auth(token);
        }
        if let Some(role) = &self.cfg().role {
            req = req.header("X-Snowflake-Role", role);
        }
        if let Some(kind) = token_type {
            req = req.header("X-Snowflake-Authorization-Token-Type", kind);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req.send().await.context("Snowflake HTTP request")?;
        let status = response.status();
        ensure!(
            !status.is_redirection(),
            "Snowflake redirected authenticated request"
        );
        let bytes = response.bytes().await?;
        if !status.is_success() {
            let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            bail!("Snowflake HTTP {}: {}", status, safe_error(&body));
        }
        Ok((status, bytes.to_vec()))
    }

    async fn sql_get(&self, url: Url, token: &str, kind: &str) -> Result<(StatusCode, Value)> {
        for attempt in 0..MAX_ATTEMPTS {
            match self
                .send(Method::GET, url.clone(), None, Some(token), Some(kind))
                .await
            {
                Ok(result) => return Ok(result),
                Err(error) if attempt + 1 < MAX_ATTEMPTS && retryable(&error) => {
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        bail!("Snowflake SQL GET attempts exhausted")
    }

    /// The UUID must be durable across process restarts and retries of this SQL statement.
    pub async fn execute_sql(&self, sql: &str, request_id: Uuid) -> Result<SqlResult> {
        let (handle, body, token, kind) = self.submit_sql(sql, None, request_id).await?;
        ensure!(
            body.get("statementHandles").is_none(),
            "multi-statement response requires execute_sql_multi"
        );
        self.result_for_handle(&handle, Some(body), &token, kind)
            .await
    }

    /// Executes an explicit multi-statement request in one Snowflake session.
    /// The caller must persist `request_id` with the exact ordered statements.
    pub async fn execute_sql_multi(
        &self,
        statements: &[String],
        request_id: Uuid,
    ) -> Result<Vec<SqlResult>> {
        ensure!(
            statements.len() >= 2 && statements.iter().all(|s| !s.trim().is_empty()),
            "invalid multi-statement SQL"
        );
        let sql = statements.join(";\n");
        let (handle, body, token, kind) = self
            .submit_sql(&sql, Some(statements.len()), request_id)
            .await?;
        let handles = body
            .get("statementHandles")
            .and_then(Value::as_array)
            .context("missing individual SQL statement handles")?;
        ensure!(
            handles.len() == statements.len(),
            "SQL statement handle count mismatch"
        );
        let mut seen = std::collections::HashSet::new();
        let mut individual_handles = Vec::with_capacity(handles.len());
        for value in handles {
            let individual = value
                .as_str()
                .context("invalid individual statement handle")?;
            ensure!(
                Uuid::parse_str(individual).is_ok(),
                "invalid individual statement UUID"
            );
            ensure!(
                individual != handle,
                "invalid duplicate parent statement handle"
            );
            ensure!(
                seen.insert(individual),
                "duplicate individual statement handle"
            );
            individual_handles.push(individual);
        }
        let mut results = Vec::with_capacity(individual_handles.len());
        for individual in individual_handles {
            results.push(
                self.result_for_handle(individual, None, &token, kind)
                    .await?,
            );
        }
        Ok(results)
    }

    async fn submit_sql(
        &self,
        sql: &str,
        count: Option<usize>,
        request_id: Uuid,
    ) -> Result<(String, Value, String, &'static str)> {
        ensure!(!sql.trim().is_empty(), "empty Snowflake SQL");
        let request_id = self.sql_request_id(request_id);
        let (token, kind) = self.token().await?;
        let mut response = None;
        for attempt in 0..MAX_ATTEMPTS {
            let mut url = self.url("/api/v2/statements")?;
            // Every caller supplies a durable UUID; after a restart this may be
            // a resubmission even when this process sees its first attempt.
            url.query_pairs_mut()
                .append_pair("requestId", &request_id.to_string())
                .append_pair("async", "true")
                .append_pair("retry", "true");
            let mut body = json!({"statement":sql,"timeout":0,"database":self.cfg().database,"schema":self.cfg().schema,"warehouse":self.cfg().warehouse,"role":self.cfg().role});
            if let Some(count) = count {
                body["parameters"] = json!({"MULTI_STATEMENT_COUNT":count.to_string()});
            }
            match self
                .send(Method::POST, url, Some(body), Some(&token), Some(kind))
                .await
            {
                Ok((status, value))
                    if status == StatusCode::OK || status == StatusCode::ACCEPTED =>
                {
                    response = Some((status, value));
                    break;
                }
                Ok((status, _)) => bail!("unexpected SQL submission status {status}"),
                Err(e) if attempt + 1 < MAX_ATTEMPTS && retryable(&e) => {
                    tokio::time::sleep(backoff(attempt)).await
                }
                Err(e) => return Err(e),
            }
        }
        let (mut status, mut body) = response.context("SQL submission attempts exhausted")?;
        let handle = required_str(&body, "statementHandle")?.to_owned();
        ensure!(
            Uuid::parse_str(&handle).is_ok(),
            "invalid Snowflake statement handle"
        );
        let deadline = std::time::Instant::now() + self.cfg().statement_timeout;
        let mut token = token;
        let mut attempt = 0;
        while status != StatusCode::OK {
            ensure!(
                status == StatusCode::ACCEPTED,
                "unexpected SQL status {status}"
            );
            ensure!(
                std::time::Instant::now() < deadline,
                "Snowflake SQL still running after {:?}",
                self.cfg().statement_timeout
            );
            tokio::time::sleep(backoff(attempt)).await;
            attempt += 1;
            // A statement can outlive the credential it was submitted with
            token = self.token().await?.0;
            let url = self.url(&format!("/api/v2/statements/{handle}"))?;
            (status, body) = self.sql_get(url, &token, kind).await?;
        }
        Ok((handle, body, token, kind))
    }

    fn sql_request_id(&self, operation: Uuid) -> Uuid {
        use sha2::{Digest, Sha256};
        // The same source operation can be tested against multiple isolated
        // destinations by one Snowflake user. SQL API retries are account-side;
        // their IDs must not reuse a receipt from a different database/schema.
        let context = serde_json::to_vec(&(operation, &self.cfg().database, &self.cfg().schema))
            .expect("serializable SQL request context");
        let digest = Sha256::digest(context);
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&digest[..16]);
        Uuid::from_bytes(bytes)
    }

    async fn result_for_handle(
        &self,
        handle: &str,
        initial: Option<Value>,
        token: &str,
        kind: &str,
    ) -> Result<SqlResult> {
        let body = match initial {
            Some(body) => body,
            None => {
                let deadline = std::time::Instant::now() + self.cfg().statement_timeout;
                let mut attempt = 0;
                loop {
                    let token = self.token().await?.0;
                    let url = self.url(&format!("/api/v2/statements/{handle}"))?;
                    let (status, body) = self.sql_get(url, &token, kind).await?;
                    if status == StatusCode::OK {
                        break body;
                    }
                    ensure!(
                        status == StatusCode::ACCEPTED,
                        "unexpected SQL status {status}"
                    );
                    ensure!(
                        std::time::Instant::now() < deadline,
                        "individual SQL statement still running after {:?}",
                        self.cfg().statement_timeout
                    );
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        };
        ensure!(
            required_str(&body, "statementHandle")? == handle,
            "SQL result handle mismatch"
        );
        let metadata = body
            .get("resultSetMetaData")
            .context("missing SQL result metadata")?;
        let partitions = metadata
            .get("partitionInfo")
            .and_then(Value::as_array)
            .context("missing SQL partition metadata")?;
        ensure!(!partitions.is_empty(), "empty SQL partition metadata");
        let row_type = metadata
            .get("rowType")
            .cloned()
            .context("missing SQL row type")?;
        let mut rows = parse_rows(&body)?;
        for i in 1..partitions.len() {
            let mut url = self.url(&format!("/api/v2/statements/{handle}"))?;
            url.query_pairs_mut()
                .append_pair("partition", &i.to_string());
            let (part_status, part) = self.sql_get(url, token, kind).await?;
            ensure!(part_status == StatusCode::OK, "SQL partition not ready");
            rows.extend(parse_rows(&part)?);
        }
        Ok(SqlResult {
            statement_handle: handle.to_owned(),
            rows,
            row_type,
        })
    }

    pub async fn discover_ingest_host(&self) -> Result<String> {
        let (token, kind) = self.token().await?;
        let (_, body) = self
            .send_bytes(
                Method::GET,
                self.url("/v2/streaming/hostname")?,
                None,
                Some(&token),
                Some(kind),
            )
            .await?;
        let host = streaming_scalar(&body, "hostname")?.replace('_', "-");
        validate_ingest_host(&host, &self.cfg().account_url)?;
        Ok(host)
    }

    async fn scoped_token(&self, host: &str) -> Result<String> {
        validate_ingest_host(host, &self.cfg().account_url)?;
        let (token, kind) = self.token().await?;
        let url = self.url("/oauth/token")?;
        // Ingest authorization uses the role in the scoped token, independently
        // of the SQL API role and the REST context header.
        let scope = match &self.cfg().role {
            Some(role) => format!("{host} session:role:{role}"),
            None => host.to_owned(),
        };
        let mut request = self
            .client
            .post(url)
            .header("User-Agent", &self.cfg().user_agent)
            .header("Accept", "application/json")
            .header("X-Snowflake-Authorization-Token-Type", kind)
            .bearer_auth(token)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("scope", scope.as_str()),
            ]);
        if let Some(role) = &self.cfg().role {
            request = request.header("X-Snowflake-Role", role);
        }
        let response = request.send().await?;
        ensure!(
            !response.status().is_redirection(),
            "Snowflake scoped-token redirect"
        );
        ensure!(
            response.status().is_success(),
            "Snowflake scoped-token exchange failed: {}",
            response.status()
        );
        streaming_scalar(&response.bytes().await?, "token")
    }

    fn ingest_url(&self, host: &str, path: &str) -> Result<Url> {
        let mut url = self.cfg().account_url.clone();
        url.set_host(Some(host))?;
        Ok(url.join(path)?)
    }

    /// Ingest host and scoped token, cached until near expiry
    async fn ingest_credentials(&self) -> Result<(String, String)> {
        let mut cached = self.cache.ingest.lock().await;
        if let Some((host, token, at)) = cached.as_ref()
            && at.elapsed() < CREDENTIAL_LIFETIME
        {
            return Ok((host.clone(), token.clone()));
        }
        let host = self.discover_ingest_host().await?;
        let scoped = self.scoped_token(&host).await?;
        *cached = Some((host.clone(), scoped.clone(), std::time::Instant::now()));
        Ok((host, scoped))
    }

    /// Forget cached ingest credentials after an authorization failure
    async fn invalidate_ingest(&self) {
        *self.cache.ingest.lock().await = None;
    }

    async fn ingest(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut attempt = 0;
        loop {
            let (host, scoped) = self.ingest_credentials().await?;
            match self
                .send(
                    method.clone(),
                    self.ingest_url(&host, path)?,
                    body.clone(),
                    Some(&scoped),
                    None,
                )
                .await
            {
                Ok((status, value)) => {
                    ensure!(
                        status == StatusCode::OK,
                        "unexpected streaming status {status}"
                    );
                    return Ok(value);
                }
                Err(e) if attempt + 1 < MAX_ATTEMPTS && e.to_string().contains("HTTP 401") => {
                    self.invalidate_ingest().await;
                }
                // Channel open and status reads are idempotent
                Err(e) if attempt + 1 < MAX_ATTEMPTS && retryable(&e) => {
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(e) => return Err(e),
            }
            attempt += 1;
        }
    }

    pub async fn open_channel(
        &self,
        channel: &ChannelRef,
        offset_token: Option<&str>,
    ) -> Result<OpenedChannel> {
        self.open_channel_inner(channel, offset_token, true).await
    }

    /// Reopen after a 409 from `open_channel` only when every submitted row is
    /// retained durably and the caller will replay rows after the returned
    /// committed offset. Snowflake discards uncommitted in-flight rows here.
    pub async fn reopen_channel_with_durable_replay(
        &self,
        channel: &ChannelRef,
        offset_token: Option<&str>,
    ) -> Result<OpenedChannel> {
        self.open_channel_inner(channel, offset_token, false).await
    }

    async fn open_channel_inner(
        &self,
        channel: &ChannelRef,
        offset_token: Option<&str>,
        fail_on_uncommitted_rows: bool,
    ) -> Result<OpenedChannel> {
        let path = channel.path()?;
        let body = self
            .ingest(
                Method::PUT,
                &path,
                Some(json!({"offset_token": offset_token, "fail_on_uncommitted_rows": fail_on_uncommitted_rows})),
            )
            .await?;
        let continuation_token = required_str(&body, "next_continuation_token")?.to_owned();
        let status = parse_status(
            body.get("channel_status")
                .context("missing open channel status")?,
        )?;
        Ok(OpenedChannel {
            continuation_token,
            status,
        })
    }

    pub async fn append_rows(
        &self,
        channel: &ChannelRef,
        continuation_token: &str,
        start_offset: &str,
        end_offset: &str,
        rows: &[Value],
        request_id: Uuid,
    ) -> Result<String> {
        ensure!(
            !continuation_token.is_empty()
                && !start_offset.is_empty()
                && !end_offset.is_empty()
                && !rows.is_empty(),
            "invalid streaming append"
        );
        let mut ndjson = Vec::new();
        for row in rows {
            ensure!(row.is_object(), "Snowflake row must be a JSON object");
            serde_json::to_writer(&mut ndjson, row)?;
            ndjson.push(b'\n');
        }
        ensure!(
            ndjson.len() <= MAX_NDJSON,
            "Snowflake streaming payload exceeds 4 MB"
        );
        let (host, scoped) = self.ingest_credentials().await?;
        let mut url = self.ingest_url(
            &host,
            &channel
                .path()?
                .replacen("/v2/streaming/", "/v2/streaming/data/", 1),
        )?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid streaming URL"))?
            .push("rows");
        url.query_pairs_mut()
            .append_pair("continuationToken", continuation_token)
            .append_pair("startOffsetToken", start_offset)
            .append_pair("endOffsetToken", end_offset)
            .append_pair("requestId", &request_id.to_string());
        let mut request = self
            .client
            .post(url)
            .header("User-Agent", &self.cfg().user_agent)
            .header("Accept", "application/json")
            .header("Content-Type", "application/x-ndjson")
            .bearer_auth(scoped)
            .body(ndjson);
        if let Some(role) = &self.cfg().role {
            request = request.header("X-Snowflake-Role", role);
        }
        let response = request.send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            self.invalidate_ingest().await;
        }
        ensure!(
            !response.status().is_redirection(),
            "Snowflake streaming append redirected"
        );
        ensure!(
            response.status() == StatusCode::OK,
            "Snowflake streaming append failed: {}",
            response.status()
        );
        let body: Value = response.json().await?;
        Ok(required_str(&body, "next_continuation_token")?.to_owned())
    }

    pub async fn channel_status(&self, channel: &ChannelRef) -> Result<ChannelStatus> {
        let path = format!("{}:bulk-channel-status", channel.pipe_path()?);
        let body = self
            .ingest(
                Method::POST,
                &path,
                Some(json!({"channel_names":[channel.channel]})),
            )
            .await?;
        let status = body
            .get("channel_statuses")
            .and_then(|v| v.get(&channel.channel))
            .context("missing named channel status")?;
        parse_status(status)
    }
}

impl ChannelRef {
    fn pipe_path(&self) -> Result<String> {
        for value in [&self.database, &self.schema, &self.pipe, &self.channel] {
            ensure!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid Snowflake channel identifier"
            );
        }
        Ok(format!(
            "/v2/streaming/databases/{}/schemas/{}/pipes/{}",
            self.database, self.schema, self.pipe
        ))
    }
    fn path(&self) -> Result<String> {
        Ok(format!("{}/channels/{}", self.pipe_path()?, self.channel))
    }
}

fn parse_status(body: &Value) -> Result<ChannelStatus> {
    ensure!(
        matches!(
            required_str(body, "channel_status_code")?,
            "ACTIVE" | "SUCCESS"
        ),
        "Snowflake channel is not active"
    );
    let errors = body
        .get("rows_error_count")
        .or_else(|| body.get("rows_errors"))
        .and_then(Value::as_u64)
        .context("missing channel error count")?;
    ensure!(errors == 0, "Snowflake channel rejected {errors} rows");
    Ok(ChannelStatus {
        last_committed_offset_token: body
            .get("last_committed_offset_token")
            .and_then(Value::as_str)
            .map(str::to_owned),
        rows_inserted: body
            .get("rows_inserted")
            .and_then(Value::as_u64)
            .context("missing inserted count")?,
        rows_parsed: body
            .get("rows_parsed")
            .and_then(Value::as_u64)
            .context("missing parsed count")?,
        rows_errors: errors,
    })
}

fn parse_rows(body: &Value) -> Result<Vec<Vec<Value>>> {
    body.get("data")
        .and_then(Value::as_array)
        .context("missing SQL data")?
        .iter()
        .map(|row| row.as_array().cloned().context("invalid SQL row"))
        .collect()
}
fn required_str<'a>(body: &'a Value, key: &str) -> Result<&'a str> {
    let value = body
        .get(key)
        .and_then(Value::as_str)
        .context(format!("missing {key}"))?;
    ensure!(!value.is_empty(), "empty {key}");
    Ok(value)
}
fn safe_error(body: &Value) -> String {
    body.get("code")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned()
}
fn retryable(error: &anyhow::Error) -> bool {
    if let Some(e) = error.downcast_ref::<reqwest::Error>() {
        return e.is_timeout() || e.is_connect() || e.is_request();
    }
    let msg = error.to_string();
    ["HTTP 429", "HTTP 500", "HTTP 502", "HTTP 503", "HTTP 504"]
        .iter()
        .any(|code| msg.contains(code))
}
fn backoff(attempt: usize) -> Duration {
    Duration::from_millis((200u64 << attempt.min(6)).min(10_000))
}
fn validate_endpoint(url: &Url, test_http: bool) -> Result<()> {
    ensure!(
        url.scheme() == "https" || (test_http && url.scheme() == "http"),
        "Snowflake endpoint must use HTTPS"
    );
    ensure!(
        url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "invalid Snowflake account endpoint"
    );
    Ok(())
}
fn streaming_scalar(bytes: &[u8], field: &str) -> Result<String> {
    // The deployed discovery/token endpoints can return bare text despite
    // advertising application/json; the reference also documents JSON objects.
    let value = match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::String(value)) => value,
        Ok(value) => required_str(&value, field)?.to_owned(),
        Err(_) => std::str::from_utf8(bytes)
            .context("invalid streaming response encoding")?
            .trim()
            .to_owned(),
    };
    ensure!(
        !value.is_empty() && !value.chars().any(char::is_whitespace),
        "invalid streaming scalar response"
    );
    Ok(value)
}

fn validate_ingest_host(host: &str, account: &Url) -> Result<()> {
    ensure!(
        !host.is_empty()
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-'),
        "invalid Snowflake ingest hostname"
    );
    let account_host = account.host_str().context("missing account hostname")?;
    #[cfg(test)]
    if account.scheme() == "http" && host == account_host {
        return Ok(());
    }
    ensure!(
        account_host.ends_with(".snowflakecomputing.com")
            || account_host.ends_with(".snowflakecomputing.cn"),
        "invalid Snowflake account hostname"
    );
    let suffix = if account_host.ends_with(".snowflakecomputing.cn") {
        ".snowflakecomputing.cn"
    } else {
        ".snowflakecomputing.com"
    };
    let host = host.to_ascii_lowercase();
    ensure!(
        host.ends_with(suffix)
            && host[..host.len() - suffix.len()]
                .split('.')
                .skip(1)
                .any(|label| label == "ingest")
            && host.split('.').all(|label| !label.is_empty()),
        "Snowflake ingest hostname outside ingest domain"
    );
    Ok(())
}

#[cfg(test)]
#[path = "http_tests.rs"]
pub(crate) mod tests;
