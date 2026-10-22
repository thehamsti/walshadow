//! Health checks for `/v1/health` and `/v1/health/ready`
//!
//! Liveness, observation freshness, source receipt, shadow replay,
//! destination acknowledgement and reconciliation stay apart, and idle,
//! paused, blocked and failing stay distinct: an idle source with nothing
//! to deliver is healthy, a destination that stopped acknowledging while
//! work waits is not. A stale snapshot makes every check `unknown`.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;

use crate::metrics::{MetricsRegistry, MetricsSnapshot};

pub const SNAPSHOT_STALE: Duration = Duration::from_secs(5);
pub const ACK_STALL: Duration = Duration::from_secs(300);
pub const RECONCILE_LAG: Duration = Duration::from_secs(60);
pub const SHADOW_LAG_BYTES: u64 = 1 << 30;

#[derive(Default)]
struct Seen {
    ack: Option<(u64, Instant)>,
    reconcile_behind_since: Option<Instant>,
}

/// What changed when, sampled every few seconds
#[derive(Default)]
pub struct Tracker {
    seen: Mutex<Seen>,
}

impl Tracker {
    pub fn spawn_sampler(self: &Arc<Self>, metrics: MetricsRegistry) {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                let snap = metrics.snapshot().await;
                this.observe(&snap, None);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /// Note the acknowledgement and, given the published generation,
    /// whether reconciliation trails it
    pub fn observe(&self, snap: &MetricsSnapshot, published: Option<u64>) {
        let now = Instant::now();
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        let ack = snap.emitter_ack_lsn.get();
        if seen.ack.is_none_or(|(a, _)| a != ack) {
            seen.ack = Some((ack, now));
        }
        if let Some(published) = published {
            if published > snap.reconciled_generation {
                seen.reconcile_behind_since.get_or_insert(now);
            } else {
                seen.reconcile_behind_since = None;
            }
        }
    }

    fn ack_unchanged_for(&self) -> Option<Duration> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ack
            .map(|(_, at)| at.elapsed())
    }

    fn reconcile_behind_for(&self) -> Option<Duration> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reconcile_behind_since
            .map(|at| at.elapsed())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Paused,
    Degraded,
    Blocked,
    Failing,
    Unknown,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct Check {
    pub name: &'static str,
    pub status: Status,
    pub detail: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct Report {
    pub status: Status,
    pub ready: bool,
    pub checks: Vec<Check>,
}

/// Inputs beyond the snapshot
pub struct Inputs<'a> {
    pub snap: &'a MetricsSnapshot,
    pub age: Option<Duration>,
    pub paused: bool,
    pub published_generation: u64,
    pub keys: std::result::Result<usize, String>,
    pub ready_grace: Duration,
}

fn check(name: &'static str, status: Status, detail: impl Into<String>) -> Check {
    Check {
        name,
        status,
        detail: detail.into(),
    }
}

pub fn evaluate(tracker: &Tracker, i: &Inputs<'_>) -> Report {
    tracker.observe(i.snap, Some(i.published_generation));
    let s = i.snap;
    let mut checks = Vec::new();
    let stale = i.age.is_none_or(|a| a > SNAPSHOT_STALE);
    checks.push(match i.age {
        None => check(
            "snapshot",
            Status::Unknown,
            "the session has not published yet",
        ),
        Some(a) if stale => check(
            "snapshot",
            Status::Unknown,
            format!("last published {}s ago", a.as_secs()),
        ),
        Some(a) => check(
            "snapshot",
            Status::Ok,
            format!("published {}ms ago", a.as_millis()),
        ),
    });
    checks.push(match i.keys.as_ref() {
        Ok(n) => check("api_keys", Status::Ok, format!("{n} keys loaded")),
        Err(e) => check(
            "api_keys",
            Status::Failing,
            format!("key file failed to load: {e}"),
        ),
    });
    let mut observed = Vec::new();
    let uptime = Duration::from_secs(s.uptime_seconds);
    observed.push(match s.session_state {
        "streaming" => check("session", Status::Ok, "streaming"),
        state if uptime > i.ready_grace => check(
            "session",
            Status::Failing,
            format!(
                "{} for over {}s",
                if state.is_empty() { "starting" } else { state },
                i.ready_grace.as_secs()
            ),
        ),
        state => check(
            "session",
            Status::Degraded,
            if state.is_empty() { "starting" } else { state }.to_string(),
        ),
    });
    observed.push(if s.source_endpoint_swap_blocked_on.is_empty() {
        check("source", Status::Ok, "")
    } else {
        check(
            "source",
            Status::Blocked,
            format!(
                "endpoint swap blocked on {}",
                s.source_endpoint_swap_blocked_on
            ),
        )
    });
    observed.push(if s.shadow_apply_lag_bytes > SHADOW_LAG_BYTES {
        check(
            "shadow_replay",
            Status::Degraded,
            format!("{} bytes behind the source", s.shadow_apply_lag_bytes),
        )
    } else {
        check(
            "shadow_replay",
            Status::Ok,
            format!("{} bytes behind", s.shadow_apply_lag_bytes),
        )
    });
    let backlog = s.decoder_commit_lsn.get() > s.emitter_ack_lsn.get();
    observed.push(match tracker.ack_unchanged_for() {
        Some(d) if backlog && d > ACK_STALL => check(
            "destination",
            Status::Failing,
            format!("no acknowledgement for {}s while work waits", d.as_secs()),
        ),
        _ if backlog => check("destination", Status::Ok, "delivering"),
        _ => check("destination", Status::Ok, "idle, nothing to deliver"),
    });
    observed.push(if s.crossing_blocked_on.is_empty() {
        check("crossing", Status::Ok, "")
    } else {
        check(
            "crossing",
            Status::Blocked,
            format!("timeline crossing blocked on {}", s.crossing_blocked_on),
        )
    });
    observed.push(if i.paused {
        check(
            "paused",
            Status::Paused,
            "source consumption paused by an operator",
        )
    } else {
        check("paused", Status::Ok, "")
    });
    observed.push(if s.stuck_teardowns.is_empty() && s.tearing_down.is_empty() {
        check("tenants", Status::Ok, format!("{} tenants", s.tenants.len()))
    } else if !s.stuck_teardowns.is_empty() {
        check(
            "tenants",
            Status::Degraded,
            format!(
                "teardown left a writer running for {}; their resources stay reserved until restart",
                s.stuck_teardowns.join(", ")
            ),
        )
    } else {
        check("tenants", Status::Ok, format!("tearing down {}", s.tearing_down.join(", ")))
    });
    observed.push(match tracker.reconcile_behind_for() {
        Some(d) if d > RECONCILE_LAG => check(
            "reconcile",
            Status::Degraded,
            format!(
                "generation {} published, {} reconciled, for {}s",
                i.published_generation,
                s.reconciled_generation,
                d.as_secs()
            ),
        ),
        _ => check(
            "reconcile",
            Status::Ok,
            format!("generation {}", s.reconciled_generation),
        ),
    });
    if stale {
        for c in &mut observed {
            c.status = Status::Unknown;
            c.detail = format!("stale; last known: {}", c.detail);
        }
    }
    checks.extend(observed);
    let status = checks.iter().map(|c| c.status).max().unwrap_or(Status::Ok);
    let ready = !checks
        .iter()
        .any(|c| matches!(c.status, Status::Failing | Status::Unknown))
        && s.session_state == "streaming";
    Report {
        status,
        ready,
        checks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs<'a>(snap: &'a MetricsSnapshot, age: Option<Duration>) -> Inputs<'a> {
        Inputs {
            snap,
            age,
            paused: false,
            published_generation: snap.reconciled_generation,
            keys: Ok(1),
            ready_grace: Duration::from_secs(600),
        }
    }

    fn streaming() -> MetricsSnapshot {
        MetricsSnapshot {
            session_state: "streaming",
            ..MetricsSnapshot::default()
        }
    }

    fn status(r: &Report, name: &str) -> Status {
        r.checks.iter().find(|c| c.name == name).unwrap().status
    }

    #[tokio::test(start_paused = true)]
    async fn idle_is_healthy_and_a_stalled_destination_with_work_is_not() {
        let t = Tracker::default();
        let snap = streaming();
        let r = evaluate(&t, &inputs(&snap, Some(Duration::from_millis(100))));
        assert!(r.ready);
        assert_eq!(r.status, Status::Ok);
        // Work waits, the ack does not move
        let mut busy = streaming();
        busy.decoder_commit_lsn = 200.into();
        busy.emitter_ack_lsn = 100.into();
        let r = evaluate(&t, &inputs(&busy, Some(Duration::from_millis(100))));
        assert_eq!(status(&r, "destination"), Status::Ok);
        tokio::time::advance(ACK_STALL + Duration::from_secs(1)).await;
        let r = evaluate(&t, &inputs(&busy, Some(Duration::from_millis(100))));
        assert_eq!(status(&r, "destination"), Status::Failing);
        assert!(!r.ready);
        // Acknowledgement resumes
        busy.emitter_ack_lsn = 150.into();
        let r = evaluate(&t, &inputs(&busy, Some(Duration::from_millis(100))));
        assert_eq!(status(&r, "destination"), Status::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_snapshots_and_failed_keys_are_not_ready() {
        let t = Tracker::default();
        let snap = streaming();
        let r = evaluate(&t, &inputs(&snap, None));
        assert!(!r.ready);
        assert_eq!(status(&r, "snapshot"), Status::Unknown);
        let r = evaluate(&t, &inputs(&snap, Some(Duration::from_secs(30))));
        assert!(!r.ready);
        assert!(
            r.checks
                .iter()
                .filter(|c| c.name != "api_keys")
                .all(|c| c.status == Status::Unknown)
        );
        let mut i = inputs(&snap, Some(Duration::from_millis(1)));
        i.keys = Err("bad".into());
        let r = evaluate(&t, &i);
        assert_eq!(status(&r, "api_keys"), Status::Failing);
        assert!(!r.ready);
    }

    #[tokio::test(start_paused = true)]
    async fn paused_blocked_and_degraded_stay_ready() {
        let t = Tracker::default();
        let mut snap = streaming();
        snap.crossing_blocked_on = "slot_missing";
        snap.source_endpoint_swap_blocked_on = "slot_missing";
        snap.shadow_apply_lag_bytes = SHADOW_LAG_BYTES + 1;
        snap.stuck_teardowns = vec!["a".into()];
        let mut i = inputs(&snap, Some(Duration::from_millis(1)));
        i.paused = true;
        let r = evaluate(&t, &i);
        assert!(r.ready);
        assert_eq!(status(&r, "paused"), Status::Paused);
        assert_eq!(status(&r, "crossing"), Status::Blocked);
        assert_eq!(status(&r, "source"), Status::Blocked);
        assert_eq!(status(&r, "shadow_replay"), Status::Degraded);
        assert_eq!(status(&r, "tenants"), Status::Degraded);
        assert_eq!(r.status, Status::Blocked);
    }

    #[tokio::test(start_paused = true)]
    async fn a_session_not_streaming_fails_after_the_grace() {
        let t = Tracker::default();
        let snap = MetricsSnapshot::default();
        let r = evaluate(&t, &inputs(&snap, Some(Duration::from_millis(1))));
        assert_eq!(status(&r, "session"), Status::Degraded);
        assert!(!r.ready);
        let late = MetricsSnapshot {
            uptime_seconds: 3600,
            ..MetricsSnapshot::default()
        };
        let r = evaluate(&t, &inputs(&late, Some(Duration::from_millis(1))));
        assert_eq!(status(&r, "session"), Status::Failing);
    }

    #[tokio::test(start_paused = true)]
    async fn reconciliation_trailing_publication_degrades() {
        let t = Tracker::default();
        let snap = MetricsSnapshot {
            reconciled_generation: 3,
            ..streaming()
        };
        let mut i = inputs(&snap, Some(Duration::from_millis(1)));
        i.published_generation = 5;
        assert_eq!(status(&evaluate(&t, &i), "reconcile"), Status::Ok);
        tokio::time::advance(RECONCILE_LAG + Duration::from_secs(1)).await;
        assert_eq!(status(&evaluate(&t, &i), "reconcile"), Status::Degraded);
        i.published_generation = 3;
        assert_eq!(status(&evaluate(&t, &i), "reconcile"), Status::Ok);
    }
}
