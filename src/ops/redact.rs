//! Secrets never leave through the HTTP API, its audit log or its crash
//! and error records
//!
//! Config keys that hold a secret become `"***"` at any depth; `*_file`
//! keys hold paths and stay. Free text (error messages, panic payloads)
//! loses `password=…`-style pairs and the password of `scheme://user:pass@`.

use serde_json::{Map, Value};

/// Whether config key `key` holds a secret value
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    if k.ends_with("_file") || k.ends_with("_path") {
        return false;
    }
    [
        "password",
        "secret",
        "token",
        "api_key",
        "passphrase",
        "private_key",
        "access_key",
        "credential",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

pub const MASK: &str = "***";
const CHANGED: &str = "***(changed)";

/// TOML as JSON, a secret at any depth masked
pub fn redact_toml(table: &toml::Table) -> Value {
    redact_json(toml_to_json(&toml::Value::Table(table.clone())))
}

pub fn redact_json(mut value: Value) -> Value {
    fn walk(v: &mut Value) {
        match v {
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    if is_secret_key(k) {
                        *v = Value::String(MASK.into());
                    } else {
                        walk(v);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut value);
    value
}

/// `before` and `after` redacted for an audit record: a secret that changed
/// reads `***(changed)` in `after`, one that did not reads `***`. Never a
/// hash of the secret, which for a weak password is a guessing oracle
pub fn redact_pair(before: Option<&toml::Table>, after: Option<&toml::Table>) -> (Value, Value) {
    let b = before.map(|t| toml_to_json(&toml::Value::Table(t.clone())));
    let a = after.map(|t| toml_to_json(&toml::Value::Table(t.clone())));
    fn walk(after: &mut Value, before: Option<&Value>) {
        if let Value::Array(items) = after {
            for (i, item) in items.iter_mut().enumerate() {
                walk(item, before.and_then(|b| b.get(i)));
            }
        }
        if let Value::Object(map) = after {
            for (k, v) in map.iter_mut() {
                let prior = before.and_then(|b| b.get(k));
                if is_secret_key(k) {
                    let same = prior == Some(&*v);
                    *v = Value::String(if same { MASK } else { CHANGED }.into());
                } else {
                    walk(v, prior);
                }
            }
        }
    }
    let after = a.map(|mut a| {
        walk(&mut a, b.as_ref());
        a
    });
    (
        b.map(redact_json).unwrap_or(Value::Null),
        after.unwrap_or(Value::Null),
    )
}

pub fn toml_to_json(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(s) => Value::String(s.clone()),
        toml::Value::Integer(i) => Value::from(*i),
        toml::Value::Float(f) => {
            serde_json::Number::from_f64(*f).map_or(Value::Null, Value::Number)
        }
        toml::Value::Boolean(b) => Value::Bool(*b),
        toml::Value::Datetime(d) => Value::String(d.to_string()),
        toml::Value::Array(items) => Value::Array(items.iter().map(toml_to_json).collect()),
        toml::Value::Table(t) => Value::Object(
            t.iter()
                .map(|(k, v)| (k.clone(), toml_to_json(v)))
                .collect::<Map<_, _>>(),
        ),
    }
}

/// JSON as TOML: objects become tables. `null` and non-finite numbers have
/// no TOML form and are refused
pub fn json_to_toml(value: &Value) -> Result<toml::Value, String> {
    Ok(match value {
        Value::Null => return Err("null has no TOML form".into()),
        Value::Bool(b) => toml::Value::Boolean(*b),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => toml::Value::Integer(i),
            (None, Some(f)) if f.is_finite() => toml::Value::Float(f),
            _ => return Err(format!("{n} has no TOML form")),
        },
        Value::String(s) => toml::Value::String(s.clone()),
        Value::Array(items) => toml::Value::Array(
            items
                .iter()
                .map(json_to_toml)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Value::Object(map) => toml::Value::Table(
            map.iter()
                .map(|(k, v)| {
                    json_to_toml(v)
                        .map(|v| (k.clone(), v))
                        .map_err(|e| format!("{k}: {e}"))
                })
                .collect::<Result<toml::Table, _>>()?,
        ),
    })
}

/// Free text with credential-shaped substrings scrubbed
pub fn redact_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    // `scheme://user:password@host`
    while let Some(at) = rest.find("://") {
        let (head, tail) = rest.split_at(at + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '/' || c == '"' || c == '\'')
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rfind('@') {
            Some(at) => {
                let userinfo = &authority[..at];
                match userinfo.find(':') {
                    Some(colon) => {
                        out.push_str(&userinfo[..colon]);
                        out.push(':');
                        out.push_str(MASK);
                    }
                    None => out.push_str(userinfo),
                }
                out.push_str(&authority[at..]);
            }
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    scrub_pairs(&out)
}

/// `password=...`, `password: ...`, `token = "..."`: the value masked
fn scrub_pairs(text: &str) -> String {
    let b = text.as_bytes();
    let word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        if !word(b[i]) {
            let c = text[i..].chars().next().expect("in bounds");
            out.push(c);
            i += c.len_utf8();
            continue;
        }
        let start = i;
        while i < b.len() && word(b[i]) {
            i += 1;
        }
        out.push_str(&text[start..i]);
        if !is_secret_key(&text[start..i]) {
            continue;
        }
        let mut j = i;
        while j < b.len() && b[j] == b' ' {
            j += 1;
        }
        if j >= b.len() || !(b[j] == b'=' || b[j] == b':') {
            continue;
        }
        j += 1;
        while j < b.len() && b[j] == b' ' {
            j += 1;
        }
        out.push_str(&text[i..j]);
        let end = match b.get(j) {
            Some(&q @ (b'"' | b'\'')) => b[j + 1..]
                .iter()
                .position(|&c| c == q)
                .map_or(b.len(), |n| j + n + 2),
            _ => b[j..]
                .iter()
                .position(|&c| c.is_ascii_whitespace() || matches!(c, b',' | b';' | b'&'))
                .map_or(b.len(), |n| j + n),
        };
        out.push_str(MASK);
        i = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_mask_at_any_depth_and_paths_stay() {
        let t: toml::Table = r#"
[ch]
host = "ch"
password = "pw"
[ch.instances.mirror]
password = "pw2"
[snowflake.auth]
token_file = "/run/secrets/t"
private_key_file = "/k.p8"
[backup]
secret_key = "sk"
access_key = "ak"
"#
        .parse()
        .unwrap();
        let v = redact_toml(&t);
        let s = v.to_string();
        for secret in ["\"pw\"", "pw2", "\"sk\"", "\"ak\""] {
            assert!(!s.contains(secret), "{secret} in {s}");
        }
        assert_eq!(v["ch"]["host"], "ch");
        assert_eq!(v["ch"]["password"], MASK);
        assert_eq!(v["snowflake"]["auth"]["token_file"], "/run/secrets/t");
        assert_eq!(v["snowflake"]["auth"]["private_key_file"], "/k.p8");
    }

    #[test]
    fn audit_pairs_say_changed_without_the_value() {
        let before: toml::Table = "[ch]\npassword = 'one'\nuser = 'u'\n".parse().unwrap();
        let same: toml::Table = "[ch]\npassword = 'one'\nuser = 'v'\n".parse().unwrap();
        let rotated: toml::Table = "[ch]\npassword = 'two'\nuser = 'u'\n".parse().unwrap();
        let (b, a) = redact_pair(Some(&before), Some(&same));
        assert_eq!(b["ch"]["password"], MASK);
        assert_eq!(a["ch"]["password"], MASK);
        assert_eq!(a["ch"]["user"], "v");
        let (_, a) = redact_pair(Some(&before), Some(&rotated));
        assert_eq!(a["ch"]["password"], CHANGED);
        assert!(!a.to_string().contains("two"));
        let (b, a) = redact_pair(None, Some(&before));
        assert!(b.is_null());
        assert_eq!(a["ch"]["password"], CHANGED);
    }

    #[test]
    fn audit_pairs_redact_secrets_inside_arrays() {
        let before: toml::Table = "items = [{password = 'old', token = 'same'}]"
            .parse()
            .unwrap();
        let after: toml::Table =
            "items = [{password = 'new', token = 'same'}, {password = 'added'}]"
                .parse()
                .unwrap();
        let (b, a) = redact_pair(Some(&before), Some(&after));
        let nested: toml::Table = "credentials = {value = 'hidden'}".parse().unwrap();
        assert_eq!(redact_pair(None, Some(&nested)).1["credentials"], CHANGED);
        assert_eq!(redact_toml(&nested)["credentials"], MASK);

        assert_eq!(b["items"][0]["password"], MASK);
        assert_eq!(a["items"][0]["password"], CHANGED);
        assert_eq!(a["items"][0]["token"], MASK);
        assert_eq!(a["items"][1]["password"], CHANGED);
        assert_eq!(
            redact_json(serde_json::json!([{"password": "pw"}])),
            serde_json::json!([{"password": MASK}])
        );
        let date: toml::Value = toml::from_str("at = 2026-10-21T12:00:00Z").unwrap();
        assert_eq!(toml_to_json(&date)["at"], "2026-10-21T12:00:00Z");
        assert_eq!(
            redact_text("https://user@host/a https://host/b"),
            "https://user@host/a https://host/b"
        );
    }

    #[test]
    fn json_and_toml_round_trip_and_refuse_null() {
        let v: Value = serde_json::json!({"a": {"b": [1, 2.5, "x", true]}});
        let t = json_to_toml(&v).unwrap();
        assert_eq!(toml_to_json(&t), v);
        assert!(json_to_toml(&serde_json::json!({"a": null})).is_err());
    }

    #[test]
    fn free_text_loses_credentials() {
        let s = redact_text(
            "connect postgres://repl:hunter2@pg:5432/app failed; password=hunter2 token: 'abc def' ok",
        );
        assert!(!s.contains("hunter2"), "{s}");
        assert!(!s.contains("abc def"), "{s}");
        assert!(s.contains("postgres://repl:***@pg:5432/app"), "{s}");
        assert!(s.contains("ok"));
        assert_eq!(redact_text("no secrets here"), "no secrets here");
        assert_eq!(redact_text("tokens of praise"), "tokens of praise");
    }
}
