//! The API's own metrics, rendered with the daemon's on `/metrics`

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::{LazyLock, Mutex, PoisonError};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;

use crate::ops::metrics::{counter, counter_pairs, counter_series};

type Requests = BTreeMap<(&'static str, String, String), u64>;

static REQUESTS: LazyLock<Mutex<Requests>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));
static AUTH_FAILURES: LazyLock<Mutex<BTreeMap<&'static str, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static RATE_LIMITED: LazyLock<Mutex<BTreeMap<&'static str, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub fn request(route: &'static str, method: &str, status: u16) {
    *REQUESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry((route, method.to_string(), status.to_string()))
        .or_default() += 1;
}

pub fn auth_failure(reason: &'static str) {
    *AUTH_FAILURES
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(reason)
        .or_default() += 1;
}

pub fn rate_limited(class: &'static str) {
    *RATE_LIMITED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(class)
        .or_default() += 1;
}

#[derive(Debug)]
pub struct ApiCollector;

impl Collector for ApiCollector {
    fn encode(&self, mut enc: DescriptorEncoder) -> fmt::Result {
        let requests = REQUESTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        counter_pairs(
            &mut enc,
            "walshadow_api_requests",
            "HTTP API requests by route, method and status.",
            ["route", "method", "status"],
            requests
                .iter()
                .map(|((r, m, s), n)| ([*r, m.as_str(), s.as_str()], *n)),
        )?;
        let failures = AUTH_FAILURES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        counter_series(
            &mut enc,
            "walshadow_api_auth_failures",
            "HTTP API requests refused authentication, by reason.",
            "reason",
            failures.iter().map(|(r, n)| (*r, *n)),
        )?;
        let limited = RATE_LIMITED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        counter_series(
            &mut enc,
            "walshadow_api_rate_limited",
            "HTTP API requests refused by a rate limit, by class.",
            "class",
            limited.iter().map(|(c, n)| (*c, *n)),
        )?;
        counter(
            &mut enc,
            "walshadow_api_audit_write_failures",
            "Audit records the daemon could not write after a mutation ran.",
            crate::ops::metrics::API_AUDIT_WRITE_FAILURES.load(Ordering::Relaxed),
        )
    }
}
