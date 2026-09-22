use walshadow::destination::config::{DestinationConfig, DestinationKind};

const SNOWFLAKE: &str = r#"
[destination]
kind = "snowflake"
[snowflake]
account_url = "https://acme-test.snowflakecomputing.com"
user = "REPLICATOR"
role = "REPLICATION"
warehouse = "INGEST"
database = "REPLICA"
[snowflake.auth]
method = "oauth"
token_file = "/run/secrets/snowflake"
[snowflake.state]
directory = "/var/lib/walshadow/snowflake"
max_bytes = 10737418240
[snowflake.stage]
bucket = "replication"
prefix = "walshadow/"
region = "us-east-1"
name = "REPLICA.INTERNAL.LOAD_STAGE"
"#;

#[test]
fn legacy_clickhouse_and_no_destination_keep_their_selection() {
    assert_eq!(
        DestinationConfig::parse("[ch]\nhost='localhost'")
            .unwrap()
            .kind,
        DestinationKind::ClickHouse
    );
    assert_eq!(
        DestinationConfig::parse("").unwrap().kind,
        DestinationKind::ClickHouse
    );
}

#[test]
fn snowflake_selection_requires_complete_config() {
    let parsed = DestinationConfig::parse(SNOWFLAKE).unwrap();
    assert_eq!(parsed.kind, DestinationKind::Snowflake);
    let sf = parsed.snowflake.unwrap();
    assert_eq!(sf.channels_per_table, 8);
    assert_eq!(sf.merge_interval_ms, 15_000);
    assert!(DestinationConfig::parse("[destination]\nkind='snowflake'").is_err());
    assert!(
        DestinationConfig::parse(&SNOWFLAKE.replace("max_bytes = 10737418240", "max_bytes = 0"))
            .is_err()
    );
}

#[test]
fn rejects_ambiguous_or_unsafe_destination_settings() {
    for config in [
        format!("{SNOWFLAKE}\n[ch]\nhost='localhost'"),
        SNOWFLAKE.replace("https://", "http://"),
        SNOWFLAKE.replace("https://acme-test", "https://user:secret@acme-test"),
        SNOWFLAKE.replace("kind = \"snowflake\"", "kind = \"snowflak\""),
        SNOWFLAKE.replace("prefix = \"walshadow/\"", "prefix = \"../\""),
    ] {
        assert!(
            DestinationConfig::parse(&config).is_err(),
            "accepted {config}"
        );
    }
}

#[test]
fn rejects_snowflake_section_without_explicit_selection() {
    assert!(
        DestinationConfig::parse(&SNOWFLAKE.replace("[destination]\nkind = \"snowflake\"", ""))
            .is_err()
    );
}
