//! The API docs as a web page, rendered from the OpenAPI document
//!
//! One renderer for `/v1/docs` and `walshadow-stream api-docs --format
//! html`, from the same document `/v1/openapi.json` serves, so the page
//! cannot describe anything the document does not. Self-contained: no
//! script, no external asset, every text escaped, and served under
//! [`CONTENT_SECURITY_POLICY`]

use std::fmt::Write as _;

use serde_json::{Map, Value};

/// No script, frame, form, image or remote resource; inline styles only
pub const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; style-src 'unsafe-inline'; \
    img-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

const STYLE: &str = r#"
:root { color-scheme: light dark; --fg: #1b1f24; --muted: #57606a; --line: #d0d7de;
  --bg: #ffffff; --soft: #f6f8fa; --get: #0969da; --post: #1a7f37; --put: #9a6700;
  --patch: #8250df; --delete: #cf222e; }
@media (prefers-color-scheme: dark) { :root { --fg: #e6edf3; --muted: #9da7b3; --line: #30363d;
  --bg: #0d1117; --soft: #161b22; } }
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.5 -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
  color: var(--fg); background: var(--bg); }
main { max-width: 72rem; margin: 0 auto; padding: 1.5rem 1rem 4rem; }
h1 { font-size: 1.8rem; margin: 0 0 .25rem; }
h2 { font-size: 1.35rem; margin: 2.5rem 0 .5rem; padding-bottom: .3rem; border-bottom: 1px solid var(--line); }
h3 { font-size: 1.05rem; margin: 1.25rem 0 .4rem; }
h4 { font-size: .95rem; margin: 1rem 0 .3rem; color: var(--muted); }
code, .path { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: .9em; }
.muted { color: var(--muted); }
nav ul { list-style: none; padding-left: 0; columns: 2 18rem; }
nav li { break-inside: avoid; margin: .15rem 0; }
a { color: var(--get); text-decoration: none; } a:hover { text-decoration: underline; }
section.op { border: 1px solid var(--line); border-radius: 8px; padding: .75rem 1rem 1rem; margin: 1rem 0; }
.op h3 { display: flex; flex-wrap: wrap; gap: .5rem; align-items: baseline; margin-top: 0; }
.m { font-family: ui-monospace, Menlo, monospace; font-weight: 700; font-size: .8rem; color: #fff;
  padding: .1rem .45rem; border-radius: 4px; }
.m-get { background: var(--get); } .m-post { background: var(--post); } .m-put { background: var(--put); }
.m-patch { background: var(--patch); } .m-delete { background: var(--delete); }
.badge { font-size: .75rem; border: 1px solid var(--line); border-radius: 999px; padding: 0 .5rem; color: var(--muted); }
.badge.public { border-color: var(--post); color: var(--post); }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: .25rem 0 .5rem; font-size: .9rem; }
th, td { text-align: left; vertical-align: top; padding: .35rem .5rem; border-top: 1px solid var(--line); }
th { background: var(--soft); font-weight: 600; }
td.name { white-space: nowrap; }
.req { color: var(--delete); font-size: .75rem; margin-left: .25rem; }
p { margin: .35rem 0; }
@media (max-width: 40rem) { body { font-size: 14px; } nav ul { columns: 1; } td.name { white-space: normal; } }
"#;

/// The page for `doc`
pub fn render(doc: &Value) -> String {
    let r = Renderer {
        schemas: doc
            .pointer("/components/schemas")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
    };
    let mut h = String::new();
    let title = str_at(doc, "/info/title");
    let version = str_at(doc, "/info/version");
    let _ = write!(
        h,
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>{} {}</title><style>{STYLE}</style></head><body><main>",
        esc(title),
        esc(version)
    );
    let _ = write!(
        h,
        "<header><h1>{}</h1><p class=\"muted\">Version {} · OpenAPI {} · \
         <a href=\"openapi.json\">openapi.json</a></p><p>{}</p>\
         <p>For a saved page, produce the sibling JSON file with \
         <code>walshadow-stream api-docs --format json &gt; openapi.json</code>. \
         Add <code>--public-docs</code> and <code>--public-livez</code> to match \
         the daemon's exposure flags.</p></header>",
        esc(title),
        esc(version),
        esc(str_at(doc, "/openapi")),
        inline(str_at(doc, "/info/description")),
    );
    r.security(&mut h, doc);
    let tags: Vec<(String, String)> = doc
        .get("tags")
        .and_then(Value::as_array)
        .map(|t| {
            t.iter()
                .map(|t| {
                    (
                        str_at(t, "/name").to_string(),
                        str_at(t, "/description").to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let ops = operations(doc);
    h.push_str("<nav><h2>Endpoints</h2><ul>");
    for (_, method, path, op) in &ops {
        let _ = write!(
            h,
            "<li><a href=\"#op-{}\"><span class=\"m m-{}\">{}</span> <span class=\"path\">{}</span></a> \
             <span class=\"muted\">{}</span></li>",
            esc(str_at(op, "/operationId")),
            esc(method),
            esc(&method.to_ascii_uppercase()),
            esc(path),
            esc(str_at(op, "/summary")),
        );
    }
    h.push_str("</ul></nav>");
    for (tag, description) in &tags {
        let in_tag: Vec<_> = ops.iter().filter(|(t, ..)| t == tag).collect();
        if in_tag.is_empty() {
            continue;
        }
        let _ = write!(
            h,
            "<h2 id=\"tag-{}\">{}</h2><p class=\"muted\">{}</p>",
            esc(tag),
            esc(tag),
            inline(description)
        );
        for (_, method, path, op) in in_tag {
            r.operation(&mut h, method, path, op);
        }
    }
    h.push_str("<h2 id=\"schemas\">Schemas</h2>");
    for (name, schema) in &r.schemas {
        let _ = write!(h, "<h3 id=\"schema-{}\">{}</h3>", esc(name), esc(name));
        if let Some(d) = schema.get("description").and_then(Value::as_str) {
            let _ = write!(h, "<p>{}</p>", inline(d));
        }
        r.schema_body(&mut h, schema);
    }
    h.push_str("</main></body></html>\n");
    h
}

/// `(tag, method, path, operation)`, in path order then method order
fn operations(doc: &Value) -> Vec<(String, String, String, Value)> {
    let mut out = Vec::new();
    if let Some(paths) = doc.get("paths").and_then(Value::as_object) {
        for (path, item) in paths {
            for method in ["get", "post", "put", "patch", "delete"] {
                if let Some(op) = item.get(method) {
                    let tag = op
                        .pointer("/tags/0")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    out.push((tag, method.to_string(), path.clone(), op.clone()));
                }
            }
        }
    }
    out
}

struct Renderer {
    schemas: Map<String, Value>,
}

impl Renderer {
    fn security(&self, h: &mut String, doc: &Value) {
        h.push_str("<h2 id=\"security\">Authentication</h2>");
        if let Some(schemes) = doc
            .pointer("/components/securitySchemes")
            .and_then(Value::as_object)
        {
            for (name, s) in schemes {
                let _ = write!(
                    h,
                    "<p><code>{}</code>: HTTP {} · {}</p>",
                    esc(name),
                    esc(str_at(s, "/scheme")),
                    inline(str_at(s, "/description"))
                );
            }
        }
    }

    fn operation(&self, h: &mut String, method: &str, path: &str, op: &Value) {
        let public = op
            .get("security")
            .and_then(Value::as_array)
            .is_some_and(|s| s.iter().any(|r| r.as_object().is_some_and(Map::is_empty)));
        let _ = write!(
            h,
            "<section class=\"op\" id=\"op-{}\"><h3><span class=\"m m-{}\">{}</span>\
             <span class=\"path\">{}</span> <span class=\"badge\">scope: {}</span>\
             <span class=\"badge\">rate: {}</span>{}</h3><p><strong>{}</strong></p>",
            esc(str_at(op, "/operationId")),
            esc(method),
            esc(&method.to_ascii_uppercase()),
            esc(path),
            esc(str_at(op, "/x-walshadow-scope")),
            esc(str_at(op, "/x-walshadow-rate-class")),
            if public {
                "<span class=\"badge public\">public</span>"
            } else {
                ""
            },
            esc(str_at(op, "/summary")),
        );
        for para in str_at(op, "/description").split("\n\n") {
            if !para.is_empty() {
                let _ = write!(h, "<p>{}</p>", inline(para));
            }
        }
        if let Some(action) = op.get("x-walshadow-audit-action").and_then(Value::as_str) {
            let _ = write!(
                h,
                "<p class=\"muted\">Audited as <code>{}</code></p>",
                esc(action)
            );
        }
        if let Some(params) = op.get("parameters").and_then(Value::as_array)
            && !params.is_empty()
        {
            h.push_str(
                "<h4>Parameters</h4><div class=\"table-wrap\"><table><tr><th>Name</th><th>In</th>\
                 <th>Type</th><th>Description</th></tr>",
            );
            for p in params {
                let required = p.get("required").and_then(Value::as_bool) == Some(true);
                let _ = write!(
                    h,
                    "<tr><td class=\"name\"><code>{}</code>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    esc(str_at(p, "/name")),
                    if required {
                        "<span class=\"req\">required</span>"
                    } else {
                        ""
                    },
                    esc(str_at(p, "/in")),
                    self.type_of(p.get("schema").unwrap_or(&Value::Null)),
                    inline(str_at(p, "/description")),
                );
            }
            h.push_str("</table></div>");
        }
        if let Some(body) = op.get("requestBody") {
            let required = body.get("required").and_then(Value::as_bool) == Some(true);
            let _ = write!(
                h,
                "<h4>Request body{}</h4><p>{}</p>",
                if required { "" } else { " (optional)" },
                inline(str_at(body, "/description"))
            );
            if let Some(content) = body.get("content").and_then(Value::as_object) {
                for (media, c) in content {
                    let schema = c.get("schema").unwrap_or(&Value::Null);
                    let _ = write!(
                        h,
                        "<p><code>{}</code>: {}</p>",
                        esc(media),
                        self.type_of(schema)
                    );
                    self.schema_body(h, self.resolve(schema));
                }
            }
        }
        h.push_str("<h4>Responses</h4>");
        if let Some(responses) = op.get("responses").and_then(Value::as_object) {
            for (status, resp) in responses {
                let _ = write!(
                    h,
                    "<p><strong>{}</strong> {}</p>",
                    esc(status),
                    inline(str_at(resp, "/description"))
                );
                if let Some(content) = resp.get("content").and_then(Value::as_object) {
                    for (media, c) in content {
                        let schema = c.get("schema").unwrap_or(&Value::Null);
                        let _ = write!(
                            h,
                            "<p class=\"muted\"><code>{}</code>: {}</p>",
                            esc(media),
                            self.type_of(schema)
                        );
                        if status.starts_with('2') {
                            self.schema_body(h, self.resolve(schema));
                        }
                    }
                }
                if let Some(headers) = resp.get("headers").and_then(Value::as_object)
                    && !headers.is_empty()
                {
                    let names: Vec<String> = headers
                        .iter()
                        .map(|(n, d)| {
                            format!(
                                "<code>{}</code> ({})",
                                esc(n),
                                inline(str_at(d, "/description"))
                            )
                        })
                        .collect();
                    let _ = write!(h, "<p class=\"muted\">Headers: {}</p>", names.join(", "));
                }
            }
        }
        h.push_str("</section>");
    }

    /// A `$ref` to the schema it names; anything else as is
    fn resolve<'a>(&'a self, s: &'a Value) -> &'a Value {
        s.get("$ref")
            .and_then(Value::as_str)
            .and_then(|r| r.strip_prefix("#/components/schemas/"))
            .and_then(|n| self.schemas.get(n))
            .unwrap_or(s)
    }

    /// An object's field table, a composition's merged fields, or nothing
    /// for a scalar, which [`Self::type_of`] already said
    fn schema_body(&self, h: &mut String, schema: &Value) {
        let mut fields = Vec::new();
        let mut open = false;
        self.fields(schema, &mut fields, &mut open, 0);
        if let Some(variants) = schema
            .get("oneOf")
            .or_else(|| schema.get("anyOf"))
            .and_then(Value::as_array)
        {
            let list: Vec<String> = variants.iter().map(|v| self.type_of(v)).collect();
            let _ = write!(h, "<p>One of: {}</p>", list.join(" · "));
        }
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            let list: Vec<String> = values
                .iter()
                .map(|v| format!("<code>{}</code>", esc(&display(v))))
                .collect();
            let _ = write!(h, "<p>Values: {}</p>", list.join(", "));
        }
        if fields.is_empty() {
            if open {
                h.push_str("<p class=\"muted\">An object; any keys.</p>");
            }
            return;
        }
        h.push_str(
            "<div class=\"table-wrap\"><table><tr><th>Field</th><th>Type</th><th>Description</th></tr>",
        );
        for (name, required, field) in &fields {
            let _ = write!(
                h,
                "<tr><td class=\"name\"><code>{}</code>{}</td><td>{}</td><td>{}</td></tr>",
                esc(name),
                if *required {
                    "<span class=\"req\">required</span>"
                } else {
                    ""
                },
                self.type_of(field),
                self.notes(field),
            );
        }
        h.push_str("</table></div>");
        if open {
            h.push_str("<p class=\"muted\">Other keys are allowed.</p>");
        }
    }

    /// Fields of an object, through `allOf` and references; `depth` cuts a
    /// cycle
    fn fields(
        &self,
        schema: &Value,
        out: &mut Vec<(String, bool, Value)>,
        open: &mut bool,
        depth: usize,
    ) {
        if depth > 8 {
            return;
        }
        let schema = self.resolve(schema);
        if let Some(parts) = schema.get("allOf").and_then(Value::as_array) {
            // The composition decides whether it is open, not its parts
            let mut parts_open = false;
            for p in parts {
                self.fields(p, out, &mut parts_open, depth + 1);
            }
            if schema.get("unevaluatedProperties") == Some(&Value::Bool(true)) {
                *open = true;
            }
            return;
        }
        if schema.get("additionalProperties") == Some(&Value::Bool(true)) {
            *open = true;
        }
        let required: Vec<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|r| r.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        if let Some(props) = schema.get("properties").and_then(Value::as_object) {
            for (name, field) in props {
                if !out.iter().any(|(n, ..)| n == name) {
                    out.push((
                        name.clone(),
                        required.contains(&name.as_str()),
                        field.clone(),
                    ));
                }
            }
        }
    }

    /// Description, values, default and limits of a field
    fn notes(&self, field: &Value) -> String {
        let mut parts = Vec::new();
        if let Some(d) = field.get("description").and_then(Value::as_str) {
            parts.push(inline(d));
        }
        let target = self.resolve(field);
        if let Some(values) = target.get("enum").and_then(Value::as_array) {
            let list: Vec<String> = values
                .iter()
                .map(|v| format!("<code>{}</code>", esc(&display(v))))
                .collect();
            parts.push(format!("Values: {}", list.join(", ")));
        }
        if let Some(d) = field.get("default") {
            parts.push(format!("Default: <code>{}</code>", esc(&display(d))));
        }
        for (key, label) in [
            ("minimum", "min"),
            ("maximum", "max"),
            ("minLength", "min length"),
            ("maxLength", "max length"),
            ("pattern", "pattern"),
            ("format", "format"),
        ] {
            if let Some(v) = field.get(key) {
                parts.push(format!("{label} <code>{}</code>", esc(&display(v))));
            }
        }
        parts.join(". ")
    }

    /// A short type: `string`, `array of Job`, `Destination or null`, …,
    /// with links to named schemas
    fn type_of(&self, s: &Value) -> String {
        if let Some(r) = s.get("$ref").and_then(Value::as_str) {
            let name = r.trim_start_matches("#/components/schemas/");
            return format!("<a href=\"#schema-{}\">{}</a>", esc(name), esc(name));
        }
        for key in ["oneOf", "anyOf"] {
            if let Some(variants) = s.get(key).and_then(Value::as_array) {
                let list: Vec<String> = variants.iter().map(|v| self.type_of(v)).collect();
                return list.join(" or ");
            }
        }
        if let Some(parts) = s.get("allOf").and_then(Value::as_array) {
            let list: Vec<String> = parts.iter().map(|v| self.type_of(v)).collect();
            return format!("all of {}", list.join(", "));
        }
        let types: Vec<&str> = match s.get("type") {
            Some(Value::String(t)) => vec![t.as_str()],
            Some(Value::Array(ts)) => ts.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        let mut named: Vec<String> = types
            .iter()
            .filter(|t| **t != "null")
            .map(|t| match *t {
                "array" => format!(
                    "array of {}",
                    s.get("items")
                        .map_or_else(|| "any".into(), |i| self.type_of(i))
                ),
                "object" => match s.get("additionalProperties") {
                    Some(v) if v.is_object() => format!("map of string to {}", self.type_of(v)),
                    _ => "object".into(),
                },
                other => match s.get("format").and_then(Value::as_str) {
                    Some(f) => format!("{} ({})", esc(other), esc(f)),
                    None => esc(other),
                },
            })
            .collect();
        if named.is_empty() {
            if types.contains(&"null") {
                return "null".into();
            }
            named.push(if s.get("properties").is_some() {
                "object".into()
            } else {
                "any".into()
            });
        }
        let mut out = named.join(" or ");
        if types.contains(&"null") {
            out.push_str(" or null");
        }
        out
    }
}

fn str_at<'a>(v: &'a Value, pointer: &str) -> &'a str {
    v.pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn display(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// HTML-escape
pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Escaped text with `` `code` `` spans
fn inline(s: &str) -> String {
    let mut out = String::new();
    for (i, part) in s.split('`').enumerate() {
        if i % 2 == 1 {
            let _ = write!(out, "<code>{}</code>", esc(part));
        } else {
            out.push_str(&esc(part));
        }
    }
    out
}
