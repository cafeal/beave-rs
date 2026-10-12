//! Message locks of received messages, renewed while they are processed.
use super::management::{LOCK_LOST, Management, ManagementError};
use crate::shutdown::CancellationToken;
use fe2o3_amqp::types::primitives::Uuid;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::time::Instant;
use tracing::warn;

/// Lock tokens one renew-lock request carries at most.
const BATCH: usize = 100;
/// Shortest and longest lock duration Service Bus allows an entity.
const MIN_LOCK: Duration = Duration::from_secs(5);
const MAX_LOCK: Duration = Duration::from_secs(5 * 60);

struct Lock {
    token: Uuid,
    /// When the lock ends unless renewed.
    expires: Instant,
    /// The lock duration observed when the message was received.
    duration: Duration,
    revoked: CancellationToken,
}

/// The locks of one receive link's messages that have been neither settled
/// nor released, by delivery ID.
#[derive(Default)]
pub(super) struct Locks {
    entries: Mutex<HashMap<u64, Lock>>,
    next: Mutex<u64>,
}

/// How long from `now` a lock that ends at `until`, in milliseconds since the
/// Unix epoch, still holds. Service Bus reports the time by its own clock, so
/// the result assumes the clocks agree.
fn remaining(until: i64, now: SystemTime) -> Duration {
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    Duration::from_millis(u64::try_from(i128::from(until) - now as i128).unwrap_or(0))
}

impl Locks {
    /// Starts tracking a lock that Service Bus holds until `locked_until`.
    /// Returns `None` for a lock that already ended, whose message Service Bus
    /// will deliver again. Without `locked_until`, the shortest lock duration
    /// is assumed.
    pub(super) fn add(
        &self,
        token: Uuid,
        locked_until: Option<i64>,
        now: (Instant, SystemTime),
    ) -> Option<(u64, CancellationToken)> {
        let left = locked_until.map_or(MIN_LOCK, |until| remaining(until, now.1));
        if left.is_zero() {
            return None;
        }
        let id = {
            let mut next = self.next.lock().unwrap();
            *next += 1;
            *next
        };
        let revoked = CancellationToken::new();
        self.entries.lock().unwrap().insert(
            id,
            Lock {
                token,
                expires: now.0 + left,
                duration: left.clamp(MIN_LOCK, MAX_LOCK),
                revoked: revoked.clone(),
            },
        );
        Some((id, revoked))
    }

    /// Stops tracking a lock without revoking it. Returns whether it still held.
    pub(super) fn remove(&self, id: u64) -> bool {
        self.entries.lock().unwrap().remove(&id).is_some()
    }

    /// Stops tracking a lock and revokes its delivery.
    pub(super) fn revoke(&self, id: u64) {
        if let Some(lock) = self.entries.lock().unwrap().remove(&id) {
            lock.revoked.cancel();
        }
    }

    /// Revokes every delivery, when their receive link ended.
    pub(super) fn revoke_all(&self) {
        for (_, lock) in self.entries.lock().unwrap().drain() {
            lock.revoked.cancel();
        }
    }

    /// Revokes the locks that ended at `now`, and returns those due for a
    /// renewal: a third of their duration has passed.
    fn due(&self, now: Instant) -> Vec<(u64, Uuid)> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, lock| {
            let expired = lock.expires <= now;
            if expired {
                lock.revoked.cancel();
            }
            !expired
        });
        entries
            .iter()
            .filter(|(_, lock)| {
                lock.expires.saturating_duration_since(now) <= lock.duration * 2 / 3
            })
            .map(|(id, lock)| (*id, lock.token.clone()))
            .collect()
    }

    /// Extends a lock to `expires`. A renewal grants the entity's whole lock
    /// duration, which a prefetched message may not have had left when it was
    /// received.
    fn renewed(&self, id: u64, expires: Instant, now: Instant) {
        if let Some(lock) = self.entries.lock().unwrap().get_mut(&id) {
            lock.expires = lock.expires.max(expires);
            lock.duration = lock
                .duration
                .max(expires.saturating_duration_since(now))
                .clamp(MIN_LOCK, MAX_LOCK);
        }
    }

    /// How often the keeper looks for locks due for a renewal: often enough
    /// for the shortest lock duration.
    fn interval() -> Duration {
        MIN_LOCK / 5
    }
}

/// Renews due locks until `stop` is cancelled.
pub(super) async fn keep(mut management: Management, locks: Arc<Locks>, stop: CancellationToken) {
    let mut ticks = tokio::time::interval(Locks::interval());
    loop {
        tokio::select! {
            _ = stop.cancelled() => {
                management.close().await;
                return;
            }
            _ = ticks.tick() => {
                let due = locks.due(Instant::now());
                for chunk in due.chunks(BATCH) {
                    renew(&mut management, &locks, chunk).await;
                }
            }
        }
    }
}

/// Renews a chunk of locks. When the request fails, for example because one
/// lock was lost, each lock is renewed alone so the others still hold.
async fn renew(management: &mut Management, locks: &Locks, chunk: &[(u64, Uuid)]) {
    let tokens: Vec<Uuid> = chunk.iter().map(|(_, token)| token.clone()).collect();
    match management.renew_locks(&tokens).await {
        Ok(expirations) => {
            for ((id, _), expiration) in chunk.iter().zip(expirations) {
                renewed(locks, *id, expiration.milliseconds());
            }
        }
        Err(_) if chunk.len() > 1 => {
            for (id, token) in chunk {
                match management.renew_locks(std::slice::from_ref(token)).await {
                    Ok(expirations) => renewed(locks, *id, expirations[0].milliseconds()),
                    Err(error) => failed(locks, *id, error),
                }
            }
        }
        Err(error) => failed(locks, chunk[0].0, error),
    }
}

fn renewed(locks: &Locks, id: u64, until: i64) {
    let now = (Instant::now(), SystemTime::now());
    locks.renewed(id, now.0 + remaining(until, now.1), now.0);
}

/// A lock Service Bus reports lost is revoked at once; after other failures it
/// is revoked when it ends without a later renewal.
fn failed(locks: &Locks, id: u64, error: ManagementError) {
    match error {
        ManagementError::Status {
            code: LOCK_LOST, ..
        } => locks.revoke(id),
        error => warn!(%error, "renewing a Service Bus message lock failed"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(now: SystemTime, offset: Duration) -> i64 {
        (now + offset)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    #[test]
    fn locks_are_due_after_a_third_of_their_duration() {
        let locks = Locks::default();
        let start = (Instant::now(), SystemTime::now());
        let (id, revoked) = locks
            .add(
                Uuid::from([1; 16]),
                Some(at(start.1, Duration::from_secs(30))),
                start,
            )
            .unwrap();
        assert!(locks.due(start.0 + Duration::from_secs(9)).is_empty());
        assert_eq!(locks.due(start.0 + Duration::from_secs(11)).len(), 1);
        locks.renewed(
            id,
            start.0 + Duration::from_secs(41),
            start.0 + Duration::from_secs(11),
        );
        assert!(locks.due(start.0 + Duration::from_secs(20)).is_empty());
        assert!(!revoked.is_cancelled());
    }

    #[test]
    fn ended_locks_are_revoked_or_refused() {
        let locks = Locks::default();
        let start = (Instant::now(), SystemTime::now());
        let (id, revoked) = locks
            .add(
                Uuid::from([1; 16]),
                Some(at(start.1, Duration::from_secs(30))),
                start,
            )
            .unwrap();
        assert!(locks.due(start.0 + Duration::from_secs(30)).is_empty());
        assert!(revoked.is_cancelled());
        assert!(!locks.remove(id));
        let past = at(start.1, Duration::ZERO) - 1;
        assert!(locks.add(Uuid::from([2; 16]), Some(past), start).is_none());
    }

    #[test]
    fn locks_without_an_expiry_assume_the_shortest_duration() {
        let locks = Locks::default();
        let start = (Instant::now(), SystemTime::now());
        locks.add(Uuid::from([1; 16]), None, start).unwrap();
        assert!(locks.due(start.0 + Duration::from_millis(1600)).is_empty());
        assert_eq!(locks.due(start.0 + Duration::from_millis(1700)).len(), 1);
    }

    #[test]
    fn removed_and_revoked_locks_end() {
        let locks = Locks::default();
        let start = (Instant::now(), SystemTime::now());
        let until = Some(at(start.1, Duration::from_secs(60)));
        let (first, first_revoked) = locks.add(Uuid::from([1; 16]), until, start).unwrap();
        let (second, second_revoked) = locks.add(Uuid::from([2; 16]), until, start).unwrap();
        let (_, third_revoked) = locks.add(Uuid::from([3; 16]), until, start).unwrap();
        assert!(locks.remove(first));
        assert!(!first_revoked.is_cancelled());
        locks.revoke(second);
        assert!(second_revoked.is_cancelled());
        locks.revoke_all();
        assert!(third_revoked.is_cancelled());
        assert!(locks.due(start.0).is_empty());
    }
}
