//! The last WARN and ERROR events, for `GET /v1/errors`
//!
//! A `tracing` layer beside [`LogEventLayer`](crate::log_events::LogEventLayer)
//! keeps the newest [`CAPACITY`] events: time, level, target, message and
//! the fields an operator filters on (`tenant`, `database`, `error`,
//! `reason`), each value cut to [`FIELD_MAX`] bytes and scrubbed of
//! credentials. Counts by level live in the log event counters.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{LazyLock, Mutex, PoisonError};

use serde::Serialize;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

pub const CAPACITY: usize = 1000;
pub const FIELD_MAX: usize = 2048;
const FIELDS: [&str; 4] = ["tenant", "database", "error", "reason"];

#[derive(Clone, Debug, Serialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ErrorEvent {
    /// Monotonic within the process
    pub seq: u64,
    pub ts: String,
    pub level: &'static str,
    pub target: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

struct Ring {
    seq: u64,
    events: VecDeque<ErrorEvent>,
}

static RING: LazyLock<Mutex<Ring>> = LazyLock::new(|| {
    Mutex::new(Ring {
        seq: 0,
        events: VecDeque::with_capacity(CAPACITY),
    })
});

#[derive(Default)]
struct Collect {
    message: String,
    fields: [Option<String>; 4],
}

fn cut(mut s: String) -> String {
    if s.len() > FIELD_MAX {
        let mut at = FIELD_MAX;
        while !s.is_char_boundary(at) {
            at -= 1;
        }
        s.truncate(at);
        s.push('…');
    }
    crate::ops::redact::redact_text(&s)
}

impl Visit for Collect {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let mut s = String::new();
        let _ = write!(s, "{value:?}");
        self.set(field, s);
    }
}

impl Collect {
    fn set(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = value;
        } else if let Some(i) = FIELDS.iter().position(|f| *f == field.name()) {
            self.fields[i] = Some(value);
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ErrorRingLayer;

impl<S: Subscriber> Layer<S> for ErrorRingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let level = *meta.level();
        if level > Level::WARN {
            return;
        }
        let mut c = Collect::default();
        event.record(&mut c);
        let [tenant, database, error, reason] = c.fields.map(|f| f.map(cut));
        let ts = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        let mut ring = RING.lock().unwrap_or_else(PoisonError::into_inner);
        ring.seq += 1;
        let ev = ErrorEvent {
            seq: ring.seq,
            ts,
            level: meta.level().as_str(),
            target: meta.target().to_string(),
            message: cut(c.message),
            tenant,
            database,
            error,
            reason,
        };
        if ring.events.len() >= CAPACITY {
            ring.events.pop_front();
        }
        ring.events.push_back(ev);
    }
}

#[derive(Debug, Default, Clone)]
pub struct Filter {
    /// `error` keeps only ERROR; anything else keeps WARN and ERROR
    pub level: Option<String>,
    pub tenant: Option<String>,
    pub target: Option<String>,
    pub since_seq: Option<u64>,
    pub limit: usize,
}

/// Matching events, newest first
pub fn recent(filter: &Filter) -> Vec<ErrorEvent> {
    let ring = RING.lock().unwrap_or_else(PoisonError::into_inner);
    let limit = if filter.limit == 0 {
        100
    } else {
        filter.limit.min(CAPACITY)
    };
    ring.events
        .iter()
        .rev()
        .filter(|e| filter.level.as_deref() != Some("error") || e.level == "ERROR")
        .filter(|e| {
            filter
                .tenant
                .as_deref()
                .is_none_or(|t| e.tenant.as_deref() == Some(t))
        })
        .filter(|e| {
            filter
                .target
                .as_deref()
                .is_none_or(|t| e.target.starts_with(t))
        })
        .filter(|e| filter.since_seq.is_none_or(|s| e.seq > s))
        .take(limit)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::prelude::*;

    use super::*;

    #[test]
    fn keeps_warnings_and_errors_with_their_fields() {
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(ErrorRingLayer),
            || {
                tracing::info!(target: "walshadow::ring_test", "not kept");
                tracing::warn!(target: "walshadow::ring_test", tenant = "acme", reason = %"lag", "tenant evicted");
                tracing::error!(
                    target: "walshadow::ring_test",
                    error = %"connect postgres://u:secret@h/db",
                    "pipeline failed"
                );
            },
        );
        let ours = |f: Filter| -> Vec<ErrorEvent> {
            recent(&f)
                .into_iter()
                .filter(|e| e.target == "walshadow::ring_test")
                .collect()
        };
        let all = ours(Filter::default());
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].level, "ERROR", "newest first");
        assert!(!all[0].error.as_deref().unwrap().contains("secret"));
        assert_eq!(all[1].tenant.as_deref(), Some("acme"));
        assert_eq!(all[1].reason.as_deref(), Some("lag"));
        assert_eq!(all[1].message, "tenant evicted");
        let errors = ours(Filter {
            level: Some("error".into()),
            ..Filter::default()
        });
        assert_eq!(errors.len(), 1);
        let acme = ours(Filter {
            tenant: Some("acme".into()),
            ..Filter::default()
        });
        assert_eq!(acme.len(), 1);
        let later = ours(Filter {
            since_seq: Some(all[0].seq),
            ..Filter::default()
        });
        assert!(later.is_empty());
    }

    #[test]
    fn long_values_are_cut() {
        let s = cut("x".repeat(FIELD_MAX + 10));
        assert!(s.len() <= FIELD_MAX + '…'.len_utf8());
    }
    #[test]
    fn overflow_keeps_only_the_newest_and_truncates_utf8() {
        tracing::subscriber::with_default(
            tracing_subscriber::registry().with(ErrorRingLayer),
            || {
                for i in 0..CAPACITY + 3 {
                    tracing::warn!(target: "walshadow::overflow_test", tenant = "overflow", "event {i}");
                }
            },
        );
        let events = recent(&Filter {
            tenant: Some("overflow".into()),
            limit: CAPACITY,
            ..Filter::default()
        });
        assert_eq!(events.len(), CAPACITY);
        assert_eq!(
            events.first().unwrap().message,
            format!("event {}", CAPACITY + 2)
        );
        assert_eq!(events.last().unwrap().message, "event 3");
        assert_eq!(
            cut("界".repeat(FIELD_MAX)),
            format!("{}…", "界".repeat(FIELD_MAX / 3))
        );
    }
}
