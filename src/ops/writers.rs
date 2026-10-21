//! Tasks that write a destination or durable state, owned so a teardown
//! can stop every one of them before anything else takes over what they
//! write
//!
//! A [`WriterSet`] spawns async writers into a `JoinSet` behind a
//! cancellation token, and counts every live writer, `spawn_blocking`
//! descendants included: a blocking closure cannot be aborted, but it holds
//! a [`WriterToken`] until it returns. [`WriterSet::abort_and_join`] cancels,
//! aborts and then waits for the count to reach zero, so `true` means no
//! writer of the set can touch anything any more.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Live {
    count: AtomicUsize,
    zero: Notify,
}

/// One live writer; dropped when the writer's future or closure ends
pub struct WriterToken {
    live: Arc<Live>,
}

impl Clone for WriterToken {
    fn clone(&self) -> Self {
        self.live.count.fetch_add(1, Ordering::AcqRel);
        Self {
            live: self.live.clone(),
        }
    }
}

impl Drop for WriterToken {
    fn drop(&mut self) {
        if self.live.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.live.zero.notify_waiters();
        }
    }
}

#[derive(Default)]
pub struct WriterSet {
    tasks: Mutex<JoinSet<()>>,
    cancel: CancellationToken,
    live: Arc<Live>,
    closed: AtomicBool,
}

impl std::fmt::Debug for WriterSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriterSet")
            .field("live", &self.live_count())
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish()
    }
}

impl WriterSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Count `self` as holding one more live writer until the token drops.
    /// Hand one to a nested spawn or blocking closure the set cannot reach
    pub fn token(&self) -> WriterToken {
        self.live.count.fetch_add(1, Ordering::AcqRel);
        WriterToken {
            live: self.live.clone(),
        }
    }

    /// Cancelled once a teardown starts; long-running writers may select on
    /// it to stop at a clean point
    pub fn cancelled(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Spawn an async writer. After [`abort_and_join`](Self::abort_and_join)
    /// started, nothing more spawns
    pub fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        let token = self.token();
        let cancel = self.cancel.clone();
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        // Reap finished writers so a long-lived set stays small
        while tasks.try_join_next().is_some() {}
        tasks.spawn(async move {
            let _token = token;
            tokio::select! {
                biased;
                () = cancel.cancelled() => {}
                () = fut => {}
            }
        });
    }

    /// `spawn_blocking` counted as a live writer until the closure returns,
    /// however its caller ends
    pub fn spawn_blocking<F, R>(&self, f: F) -> tokio::task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let token = self.token();
        tokio::task::spawn_blocking(move || {
            let _token = token;
            f()
        })
    }

    /// Writers still running, blocking descendants included
    pub fn live_count(&self) -> usize {
        self.live.count.load(Ordering::Acquire)
    }

    /// Cancel, abort every async writer, then wait up to `wait` for every
    /// writer to end. `true` once none is left; `false` when one, typically
    /// a blocking call, outlived the wait. Idempotent
    pub async fn abort_and_join(&self, wait: Duration) -> bool {
        self.closed.store(true, Ordering::Release);
        self.cancel.cancel();
        let mut tasks =
            std::mem::take(&mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner));
        tasks.abort_all();
        let joined = async {
            while tasks.join_next().await.is_some() {}
            loop {
                let zero = self.live.zero.notified();
                tokio::pin!(zero);
                zero.as_mut().enable();
                if self.live_count() == 0 {
                    return;
                }
                zero.await;
            }
        };
        tokio::time::timeout(wait, joined).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn abort_joins_async_writers_blocked_on_anything() {
        let set = WriterSet::new();
        let gate = Arc::new(Notify::new());
        for _ in 0..3 {
            let gate = gate.clone();
            set.spawn(async move { gate.notified().await });
        }
        assert_eq!(set.live_count(), 3);
        assert!(set.abort_and_join(Duration::from_secs(5)).await);
        assert_eq!(set.live_count(), 0);
        // Closed: later spawns are refused
        set.spawn(async {});
        assert_eq!(set.live_count(), 0);
    }

    #[tokio::test]
    async fn a_blocking_descendant_holds_the_join_until_it_returns() {
        let set = WriterSet::new();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let blocking = set.spawn_blocking(move || {
            let _ = held.recv();
        });
        // Its caller is gone, the closure is not
        drop(blocking);
        assert!(!set.abort_and_join(Duration::from_millis(100)).await);
        assert_eq!(set.live_count(), 1);
        release.send(()).unwrap();
        assert!(set.abort_and_join(Duration::from_secs(5)).await);
        assert_eq!(set.live_count(), 0);
    }

    #[tokio::test]
    async fn a_token_handed_to_a_nested_spawn_counts() {
        let set = WriterSet::new();
        let token = set.token();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // A nested spawn the set never sees
        tokio::spawn(async move {
            let _token = token;
            let _ = rx.await;
        });
        assert!(!set.abort_and_join(Duration::from_millis(50)).await);
        tx.send(()).unwrap();
        assert!(set.abort_and_join(Duration::from_secs(5)).await);
    }

    #[tokio::test]
    async fn finished_writers_are_reaped() {
        let set = WriterSet::new();
        for _ in 0..10 {
            set.spawn(async {});
        }
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(set.live_count(), 0);
        set.spawn(async {});
        assert!(set.tasks.lock().unwrap().len() <= 1);
        assert!(set.abort_and_join(Duration::from_secs(1)).await);
    }
}
