//! Token buckets per client IP and per API key
//!
//! Each class keeps its own buckets: a cheap pre-auth flood guard and an
//! auth-failure budget per IP, then read, expensive-read and mutation
//! budgets per key. A bucket map holds at most `cap` keys; past that it
//! drops the buckets that have refilled to full, then the least recently
//! used. Time is `tokio::time::Instant`, so tests pause it.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use tokio::time::Instant;

/// `burst` tokens, refilled at `per_sec`
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rate {
    pub per_sec: f64,
    pub burst: f64,
}

impl Rate {
    pub const fn new(per_sec: f64, burst: f64) -> Self {
        Self { per_sec, burst }
    }

    /// `<n>/<s|m|h>:<burst>`, eg `20/s:40` or `10/m:10`
    pub fn parse(spec: &str) -> Result<Self> {
        let (rate, burst) = spec
            .split_once(':')
            .with_context(|| format!("rate {spec:?}: expected <n>/<s|m|h>:<burst>"))?;
        let (n, per) = rate
            .split_once('/')
            .with_context(|| format!("rate {spec:?}: expected <n>/<s|m|h>"))?;
        let n: f64 = n.trim().parse().with_context(|| format!("rate {spec:?}"))?;
        let secs = match per.trim() {
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            other => bail!("rate {spec:?}: period {other:?} is not s, m or h"),
        };
        let burst: f64 = burst
            .trim()
            .parse()
            .with_context(|| format!("rate {spec:?}"))?;
        if !(n > 0.0 && burst >= 1.0 && n.is_finite() && burst.is_finite()) {
            bail!("rate {spec:?}: rate must be positive and burst at least 1");
        }
        Ok(Self::new(n / secs, burst))
    }
}

#[derive(Clone, Copy)]
struct Bucket {
    tokens: f64,
    last: Instant,
}

pub struct Limiter<K> {
    rate: Rate,
    cap: usize,
    buckets: Mutex<HashMap<K, Bucket>>,
}

impl<K: Hash + Eq + Clone> Limiter<K> {
    pub fn new(rate: Rate, cap: usize) -> Self {
        Self {
            rate,
            cap: cap.max(1),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    fn refilled(&self, b: Bucket, now: Instant) -> Bucket {
        let elapsed = now.saturating_duration_since(b.last).as_secs_f64();
        Bucket {
            tokens: (b.tokens + elapsed * self.rate.per_sec).min(self.rate.burst),
            last: now,
        }
    }

    fn wait_for(&self, b: Bucket) -> Duration {
        Duration::from_secs_f64(((1.0 - b.tokens) / self.rate.per_sec).max(0.0))
    }

    /// Take a token, or say how long until one is free
    pub fn check(&self, key: &K) -> std::result::Result<(), Duration> {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        if !buckets.contains_key(key) && buckets.len() >= self.cap {
            self.evict(&mut buckets, now);
        }
        let b = buckets.get(key).copied().unwrap_or(Bucket {
            tokens: self.rate.burst,
            last: now,
        });
        let mut b = self.refilled(b, now);
        let out = if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            Ok(())
        } else {
            Err(self.wait_for(b))
        };
        buckets.insert(key.clone(), b);
        out
    }

    /// How long until `key` has a token, without taking one: `None` when
    /// one is free now
    pub fn exhausted(&self, key: &K) -> Option<Duration> {
        let now = Instant::now();
        let buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let b = self.refilled(*buckets.get(key)?, now);
        (b.tokens < 1.0).then(|| self.wait_for(b))
    }

    fn evict(&self, buckets: &mut HashMap<K, Bucket>, now: Instant) {
        buckets.retain(|_, b| self.refilled(*b, now).tokens < self.rate.burst);
        if buckets.len() >= self.cap
            && let Some(oldest) = buckets
                .iter()
                .min_by_key(|(_, b)| b.last)
                .map(|(k, _)| k.clone())
        {
            buckets.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.buckets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rates_parse() {
        assert_eq!(Rate::parse("20/s:40").unwrap(), Rate::new(20.0, 40.0));
        assert_eq!(
            Rate::parse("10/m:10").unwrap(),
            Rate::new(10.0 / 60.0, 10.0)
        );
        assert_eq!(Rate::parse("36/h:1").unwrap(), Rate::new(0.01, 1.0));
        for bad in ["20/s", "x/s:1", "1/d:1", "0/s:1", "1/s:0", "1/s:x", "1s:1"] {
            assert!(Rate::parse(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn burst_then_refill_per_key() {
        let l = Limiter::new(Rate::new(2.0, 3.0), 100);
        for _ in 0..3 {
            l.check(&"a").unwrap();
        }
        let wait = l.check(&"a").unwrap_err();
        assert!(
            wait > Duration::ZERO && wait <= Duration::from_millis(500),
            "{wait:?}"
        );
        assert!(l.exhausted(&"a").is_some());
        // Another key is untouched
        l.check(&"b").unwrap();
        assert!(l.exhausted(&"b").is_none());
        assert!(l.exhausted(&"unseen").is_none());
        tokio::time::advance(Duration::from_millis(500)).await;
        l.check(&"a").unwrap();
        assert!(l.check(&"a").is_err());
        // Refill never exceeds the burst
        tokio::time::advance(Duration::from_secs(60)).await;
        for _ in 0..3 {
            l.check(&"a").unwrap();
        }
        assert!(l.check(&"a").is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn the_map_stays_bounded_under_churn() {
        let l = Limiter::new(Rate::new(1.0, 1.0), 1000);
        for ip in 0..20_000u32 {
            let _ = l.check(&ip);
        }
        assert!(l.len() <= 1000, "{}", l.len());
        assert!(!l.is_empty());
        // Full buckets go first: after a refill, churn evicts them
        tokio::time::advance(Duration::from_secs(5)).await;
        let _ = l.check(&999_999);
        assert!(l.len() <= 1000);
    }
}
