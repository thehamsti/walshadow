use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

pub(crate) async fn server(
    responses: Vec<(u16, &'static str)>,
) -> (Url, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0u8; 8192];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..n]);
                let Some(split) = bytes.windows(4).position(|x| x == b"\r\n\r\n") else {
                    continue;
                };
                let header = String::from_utf8_lossy(&bytes[..split]);
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .and_then(|n| n.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if bytes.len() >= split + 4 + length {
                    break;
                }
            }
            requests.push(String::from_utf8_lossy(&bytes).to_string());
            let reason = match status {
                200 => "OK",
                202 => "Accepted",
                302 => "Found",
                503 => "Unavailable",
                _ => "Error",
            };
            let reply = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(reply.as_bytes()).await.unwrap();
        }
        requests
    });
    (url, task)
}

pub(crate) async fn client(url: Url) -> SnowflakeHttp {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep().join("token");
    tokio::fs::write(&path, "test-token\n").await.unwrap();
    SnowflakeHttp::for_test(HttpConfig {
        account_url: url,
        auth: AuthConfig::OAuthTokenFile { path },
        user_agent: "walshadow/test".into(),
        database: Some("DB".into()),
        schema: Some("PUBLIC".into()),
        warehouse: Some("WH".into()),
        role: None,
    })
    .unwrap()
}

#[test]
fn rejects_non_https_and_credential_urls() {
    let config = |url: &str| HttpConfig {
        account_url: Url::parse(url).unwrap(),
        auth: AuthConfig::OAuthTokenFile {
            path: "token".into(),
        },
        user_agent: "walshadow/test".into(),
        database: None,
        schema: None,
        warehouse: None,
        role: None,
    };
    assert!(SnowflakeHttp::new(config("http://example.snowflakecomputing.com/")).is_err());
    assert!(SnowflakeHttp::new(config("https://u:p@example.snowflakecomputing.com/")).is_err());
    assert!(SnowflakeHttp::new(config("https://example.snowflakecomputing.com/path")).is_err());
}

#[tokio::test]
async fn sql_retry_receipts_are_stable_within_a_destination_and_isolated_between_destinations() {
    let url = Url::parse("http://127.0.0.1:1/").unwrap();
    let mut first = client(url.clone()).await;
    let restarted = client(url).await;
    let operation = Uuid::new_v4();
    let receipt = first.sql_request_id(operation);
    assert_eq!(receipt, restarted.sql_request_id(operation));
    first.config.database = Some("OTHER_DB".into());
    assert_ne!(receipt, first.sql_request_id(operation));
    first.config.database = Some("DB".into());
    first.config.schema = Some("OTHER_SCHEMA".into());
    assert_ne!(receipt, first.sql_request_id(operation));
    assert_ne!(receipt, restarted.sql_request_id(Uuid::new_v4()));
}

#[tokio::test]
async fn sql_polls_and_reads_all_partitions_with_context() {
    let handle = "11111111-1111-4111-8111-111111111111";
    let (url, task) = server(vec![
        (202, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111"}"#),
        (200, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111","resultSetMetaData":{"rowType":[],"partitionInfo":[{},{}]},"data":[["1"]]}"#),
        (200, r#"{"data":[["2"]]}"#),
    ]).await;
    let client = client(url).await;
    let id = Uuid::new_v4();
    let result = client.execute_sql("SELECT 1", id).await.unwrap();
    assert_eq!(result.statement_handle, handle);
    assert_eq!(result.rows.len(), 2);
    let requests = task.await.unwrap();
    assert!(requests[0].contains(&format!("requestId={}", client.sql_request_id(id))));
    assert!(requests[0].contains("\"warehouse\":\"WH\""));
    assert!(requests[2].contains("partition=1"));
}

#[tokio::test]
async fn retry_keeps_request_id_and_restart_safe_retry_flag() {
    let (url, task) = server(vec![
        (503, "upstream unavailable"),
        (200, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111","resultSetMetaData":{"rowType":[],"partitionInfo":[{}]},"data":[]}"#),
    ]).await;
    let client = client(url).await;
    let id = Uuid::new_v4();
    client.execute_sql("SELECT 1", id).await.unwrap();
    let requests = task.await.unwrap();
    assert!(
        requests
            .iter()
            .all(|request| request.contains(&format!("requestId={}", client.sql_request_id(id))))
    );
    assert!(requests[0].contains("retry=true"));
    assert!(requests[1].contains("retry=true"));
}

#[tokio::test]
async fn multi_statement_requires_and_checks_each_individual_result() {
    let (url, task) = server(vec![
        (202, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111"}"#),
        (200, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111","statementHandles":["22222222-2222-4222-8222-222222222222","33333333-3333-4333-8333-333333333333"]}"#),
        (200, r#"{"statementHandle":"22222222-2222-4222-8222-222222222222","resultSetMetaData":{"rowType":[],"partitionInfo":[{}]},"data":[]}"#),
        (200, r#"{"statementHandle":"33333333-3333-4333-8333-333333333333","resultSetMetaData":{"rowType":[],"partitionInfo":[{}]},"data":[]}"#),
    ]).await;
    let client = client(url).await;
    let results = client
        .execute_sql_multi(&["BEGIN".into(), "COMMIT".into()], Uuid::new_v4())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    let requests = task.await.unwrap();
    assert!(requests[0].contains("MULTI_STATEMENT_COUNT"));
    assert!(requests[0].contains("BEGIN;\\nCOMMIT"));
    assert!(requests[2].contains("/api/v2/statements/22222222-2222-4222-8222-222222222222"));
    assert!(requests[3].contains("/api/v2/statements/33333333-3333-4333-8333-333333333333"));
}

#[tokio::test]
async fn multi_statement_fails_if_any_individual_statement_fails() {
    let (url, task) = server(vec![
        (200, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111","statementHandles":["22222222-2222-4222-8222-222222222222","33333333-3333-4333-8333-333333333333"]}"#),
        (200, r#"{"statementHandle":"22222222-2222-4222-8222-222222222222","resultSetMetaData":{"rowType":[],"partitionInfo":[{}]},"data":[]}"#),
        (422, r#"{"code":"100183","message":"statement failed"}"#),
    ]).await;
    let client = client(url).await;
    assert!(
        client
            .execute_sql_multi(&["BEGIN".into(), "COMMIT".into()], Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(task.await.unwrap().len(), 3);
}

#[tokio::test]
async fn multi_statement_rejects_duplicate_child_handles() {
    let (url, task) = server(vec![(200, r#"{"statementHandle":"11111111-1111-4111-8111-111111111111","statementHandles":["22222222-2222-4222-8222-222222222222","22222222-2222-4222-8222-222222222222"]}"#)]).await;
    let client = client(url).await;
    assert!(
        client
            .execute_sql_multi(&["BEGIN".into(), "COMMIT".into()], Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn redirect_is_not_followed_with_bearer_token() {
    let (url, task) = server(vec![(302, r#"{"code":"redirect"}"#)]).await;
    let client = client(url).await;
    assert!(
        client
            .execute_sql("SELECT 1", Uuid::new_v4())
            .await
            .is_err()
    );
    assert_eq!(task.await.unwrap().len(), 1);
}

#[tokio::test]
async fn named_channel_open_append_and_status_use_scoped_token() {
    let (url, task) = server(vec![
        (200, "127.0.0.1"),
        (200, "scoped-token"),
        (200, r#"{"next_continuation_token":"cont-1","channel_status":{"channel_status_code":"ACTIVE","rows_inserted":0,"rows_parsed":0,"rows_error_count":0}}"#),
        (200, r#"{"hostname":"127.0.0.1"}"#),
        (200, r#"{"token":"scoped-token"}"#),
        (200, r#"{"next_continuation_token":"cont-2"}"#),
        (200, r#"{"hostname":"127.0.0.1"}"#),
        (200, r#"{"token":"scoped-token"}"#),
        (200, r#"{"channel_statuses":{"CH":{"channel_status_code":"ACTIVE","last_committed_offset_token":"7","rows_inserted":1,"rows_parsed":1,"rows_errors":0}}}"#),
    ]).await;
    let mut client = client(url).await;
    client.config.role = Some("STREAMING_ROLE".into());
    let channel = ChannelRef {
        database: "DB".into(),
        schema: "PUBLIC".into(),
        pipe: "PIPE".into(),
        channel: "CH".into(),
    };
    let opened = client.open_channel(&channel, Some("6")).await.unwrap();
    assert_eq!(opened.continuation_token, "cont-1");
    let next = client
        .append_rows(
            &channel,
            &opened.continuation_token,
            "7",
            "7",
            &[json!({"A":1})],
            Uuid::new_v4(),
        )
        .await
        .unwrap();
    assert_eq!(next, "cont-2");
    assert_eq!(
        client
            .channel_status(&channel)
            .await
            .unwrap()
            .last_committed_offset_token
            .as_deref(),
        Some("7")
    );
    let requests = task.await.unwrap();
    assert!(requests.iter().all(|request| {
        request
            .to_ascii_lowercase()
            .contains("x-snowflake-role: streaming_role")
    }));
    assert!(requests[1].contains("POST /oauth/token"));
    assert!(requests[1].contains("scope=127.0.0.1+session%3Arole%3ASTREAMING_ROLE"));
    assert!(
        requests[1].contains("Authorization: Bearer test-token")
            || requests[1].contains("authorization: Bearer test-token")
    );
    assert!(
        requests[2]
            .contains("PUT /v2/streaming/databases/DB/schemas/PUBLIC/pipes/PIPE/channels/CH")
    );
    assert!(requests[2].contains("scoped-token"));
    assert!(
        requests[5].contains(
            "/v2/streaming/data/databases/DB/schemas/PUBLIC/pipes/PIPE/channels/CH/rows?"
        )
    );
    assert!(requests[5].contains("{\"A\":1}\n"));
    assert!(
        requests[8]
            .contains("/v2/streaming/databases/DB/schemas/PUBLIC/pipes/PIPE:bulk-channel-status")
    );
}

#[tokio::test]
async fn explicit_reopen_with_durable_replay_allows_uncommitted_discard() {
    let (url, task) = server(vec![
        (200, r#"{"hostname":"127.0.0.1"}"#),
        (200, r#"{"token":"scoped-token"}"#),
        (200, r#"{"next_continuation_token":"next","channel_status":{"channel_status_code":"ACTIVE","last_committed_offset_token":"prior","rows_inserted":1,"rows_parsed":1,"rows_error_count":0}}"#),
    ]).await;
    let client = client(url).await;
    let channel = ChannelRef {
        database: "DB".into(),
        schema: "PUBLIC".into(),
        pipe: "PIPE".into(),
        channel: "CH".into(),
    };
    let opened = client
        .reopen_channel_with_durable_replay(&channel, None)
        .await
        .unwrap();
    assert_eq!(
        opened.status.last_committed_offset_token.as_deref(),
        Some("prior")
    );
    let requests = task.await.unwrap();
    assert!(requests[2].contains("\"fail_on_uncommitted_rows\":false"));
}

#[test]
fn rejected_rows_are_never_success() {
    let status = json!({"channel_status_code":"ACTIVE","rows_inserted":4,"rows_parsed":5,"rows_error_count":1,"last_committed_offset_token":"5"});
    assert!(parse_status(&status).is_err());
    let missing = json!({"channel_status_code":"ACTIVE","rows_inserted":4,"rows_parsed":5});
    assert!(parse_status(&missing).is_err());
}

#[test]
fn hostname_cannot_escape_account_domain() {
    let account = Url::parse("https://acct.us-east-2.snowflakecomputing.com/").unwrap();
    assert!(validate_ingest_host("ingest.snowflakecomputing.com.evil.test", &account).is_err());
    assert!(validate_ingest_host("acct.us-east-2.ingest.snowflakecomputing.com", &account).is_ok());
    assert!(validate_ingest_host("AB12345.ingest.xyzabc.snowflakecomputing.com", &account).is_ok());
    assert!(
        validate_ingest_host("acct.ingest.privatelink.snowflakecomputing.com", &account).is_ok()
    );
    assert!(validate_ingest_host("acct.ingest.evil.test", &account).is_err());
    assert!(validate_ingest_host("acct.ingest.snowflakecomputing.cn", &account).is_err());
    assert!(validate_ingest_host("acct.ingest..snowflakecomputing.com", &account).is_err());
}

#[test]
fn deployed_success_status_still_requires_zero_rejected_rows() {
    let mut status = json!({"channel_status_code":"SUCCESS","rows_inserted":1,"rows_parsed":1,"rows_error_count":0});
    assert!(parse_status(&status).is_ok());
    status["rows_error_count"] = json!(1);
    assert!(parse_status(&status).is_err());
    status["rows_error_count"] = json!(0);
    status["channel_status_code"] = json!("FAILED");
    assert!(parse_status(&status).is_err());
}

#[test]
fn streaming_scalar_accepts_deployed_and_documented_encodings() {
    assert_eq!(
        streaming_scalar(b"token-value\n", "token").unwrap(),
        "token-value"
    );
    assert_eq!(
        streaming_scalar(br#""token-value""#, "token").unwrap(),
        "token-value"
    );
    assert_eq!(
        streaming_scalar(br#"{"token":"token-value"}"#, "token").unwrap(),
        "token-value"
    );
    assert!(streaming_scalar(b"{}", "token").is_err());
    assert!(streaming_scalar(b"<html>bad response</html>", "token").is_err());
    assert!(streaming_scalar(b"", "token").is_err());
}
