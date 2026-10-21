//! Lock ranks for the control plane: one total order, checked in debug builds
//!
//! Every control-plane lock has a rank, and a task may only acquire a lock
//! ranked strictly above every lock it already holds. The checker keys held
//! ranks by tokio task id, so a guard held across `.await` stays attributed
//! to its task whichever worker thread resumes it. Outside a task (a bare
//! `block_on`) nothing is tracked. Release builds compile it away.
//!
//! | Rank | Lock |
//! |---|---|
//! | [`MUTATION`] | one control-plane mutation at a time (API and socket) |
//! | [`RELOAD`] | `Reloader`: config writes, resolver application, publication |
//! | [`RESOLVER`] | a `ConfigResolver`'s merge inputs |
//! | R31 | Snowflake HTTP settings and credential caches: std leaf locks |
//! | [`MAPPING_WRITE`] | the routing map's write side |
//!
//! R31 locks never span `.await` or acquire another ranked lock. Other
//! locks below these are leaves: std locks never held across `.await` that
//! acquire nothing (Snowflake HTTP settings, a resolver's activation layer,
//! the audit writer, rate-limit buckets).

use std::ops::{Deref, DerefMut};

/// Serialises control-plane mutations, API and control socket alike
pub const MUTATION: u8 = 10;
/// Held from a config write until its revision publishes or rolls back
pub const RELOAD: u8 = 20;
/// A resolver's merge inputs
pub const RESOLVER: u8 = 30;
/// Routing map write, taken under [`RESOLVER`] or alone
pub const MAPPING_WRITE: u8 = 32;

#[cfg(debug_assertions)]
mod held {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex, PoisonError};

    type Held = HashMap<tokio::task::Id, Vec<(u8, &'static str)>>;

    static HELD: LazyLock<Mutex<Held>> = LazyLock::new(|| Mutex::new(HashMap::new()));

    pub fn enter(rank: u8, name: &'static str) -> Option<tokio::task::Id> {
        let id = tokio::task::try_id()?;
        let mut held = HELD.lock().unwrap_or_else(PoisonError::into_inner);
        let stack = held.entry(id).or_default();
        if let Some(&(top, top_name)) = stack.iter().max_by_key(|(r, _)| *r)
            && top >= rank
        {
            let msg = format!(
                "lock rank inversion: acquiring {name} ({rank}) while holding {top_name} ({top})"
            );
            drop(held);
            panic!("{msg}");
        }
        stack.push((rank, name));
        Some(id)
    }

    pub fn exit(id: tokio::task::Id, rank: u8) {
        let mut held = HELD.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(stack) = held.get_mut(&id) {
            if let Some(at) = stack.iter().rposition(|(r, _)| *r == rank) {
                stack.remove(at);
            }
            if stack.is_empty() {
                held.remove(&id);
            }
        }
    }
}

/// Proof a ranked lock is held, released on drop
#[must_use]
pub struct RankToken {
    #[cfg(debug_assertions)]
    held: Option<(tokio::task::Id, u8)>,
}

impl RankToken {
    /// Record `rank` as held by the current task, panicking (debug builds)
    /// if the task already holds an equal or higher rank. Take it before
    /// awaiting the lock, so an inversion fails instead of deadlocking
    pub fn enter(rank: u8, name: &'static str) -> Self {
        #[cfg(debug_assertions)]
        {
            Self {
                held: held::enter(rank, name).map(|id| (id, rank)),
            }
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = (rank, name);
            Self {}
        }
    }
}

impl Drop for RankToken {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if let Some((id, rank)) = self.held.take() {
            held::exit(id, rank);
        }
    }
}

/// A tokio mutex with a rank
pub struct RankedMutex<T> {
    rank: u8,
    name: &'static str,
    inner: tokio::sync::Mutex<T>,
}

impl<T> RankedMutex<T> {
    pub fn new(rank: u8, name: &'static str, value: T) -> Self {
        Self {
            rank,
            name,
            inner: tokio::sync::Mutex::new(value),
        }
    }

    pub async fn lock(&self) -> RankedGuard<'_, T> {
        let token = RankToken::enter(self.rank, self.name);
        let guard = self.inner.lock().await;
        RankedGuard {
            guard,
            _token: token,
        }
    }
}

impl<T: Default> Default for RankedMutex<T> {
    fn default() -> Self {
        Self::new(u8::MAX, "unranked", T::default())
    }
}

/// Guard of a [`RankedMutex`]; drops the lock before the rank
pub struct RankedGuard<'a, T> {
    guard: tokio::sync::MutexGuard<'a, T>,
    _token: RankToken,
}

impl<T> Deref for RankedGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<T> DerefMut for RankedGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ascending_ranks_nest() {
        let outer = RankedMutex::new(MUTATION, "outer", ());
        let inner = RankedMutex::new(RELOAD, "inner", ());
        let _a = outer.lock().await;
        let _b = inner.lock().await;
        let _c = RankToken::enter(RESOLVER, "resolver");
        let _d = RankToken::enter(MAPPING_WRITE, "mapping");
    }

    #[tokio::test]
    async fn released_ranks_can_be_taken_again() {
        let reload = RankedMutex::new(RELOAD, "reload", ());
        let mutation = RankedMutex::new(MUTATION, "mutation", ());
        drop(reload.lock().await);
        // Nothing held any more, so a lower rank is fine
        let _m = mutation.lock().await;
        let _r = reload.lock().await;
    }

    /// Spawned so the panic stays inside its own task
    async fn inverts(first: u8, second: u8) -> String {
        let err = tokio::spawn(async move {
            let _a = RankToken::enter(first, "first");
            let _b = RankToken::enter(second, "second");
        })
        .await
        .unwrap_err();
        let payload = err.into_panic();
        payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn inversions_panic() {
        // R30 under R32: a mapping writer reaching back into a resolver
        let msg = inverts(MAPPING_WRITE, RESOLVER).await;
        assert!(msg.contains("lock rank inversion"), "{msg}");
        // R20 under R30: a resolver reaching for the reload lock
        let msg = inverts(RESOLVER, RELOAD).await;
        assert!(msg.contains("acquiring second (20)"), "{msg}");
        // Equal ranks never nest
        let msg = inverts(RELOAD, RELOAD).await;
        assert!(msg.contains("lock rank inversion"), "{msg}");
    }

    #[test]
    fn outside_a_task_nothing_is_tracked() {
        let _a = RankToken::enter(MAPPING_WRITE, "a");
        let _b = RankToken::enter(MUTATION, "b");
    }
}
