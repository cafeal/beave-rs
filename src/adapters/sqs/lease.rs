//! Visibility leases of received messages, extended while they are processed.
use super::config::MAX_VISIBILITY;
use crate::shutdown::CancellationToken;
use aws_sdk_sqs::{Client, types::ChangeMessageVisibilityBatchRequestEntry};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tracing::warn;

/// Entries `ChangeMessageVisibilityBatch` accepts per call.
const BATCH: usize = 10;

struct Lease {
    receipt_handle: Arc<str>,
    /// When the message becomes visible again unless extended.
    expires: Instant,
    received: Instant,
    revoked: CancellationToken,
}

/// The received messages of one source that have been neither deleted nor
/// released, by lease ID.
pub(super) struct Leases {
    entries: Mutex<HashMap<u64, Lease>>,
    next: Mutex<u64>,
    visibility: Duration,
}

impl Leases {
    pub(super) fn new(visibility: Duration) -> Self {
        Self {
            entries: Mutex::default(),
            next: Mutex::new(0),
            visibility,
        }
    }

    /// Starts the lease of a message received at `received`.
    pub(super) fn add(&self, receipt_handle: &str, received: Instant) -> (u64, CancellationToken) {
        let id = {
            let mut next = self.next.lock().unwrap();
            *next += 1;
            *next
        };
        let revoked = CancellationToken::new();
        self.entries.lock().unwrap().insert(
            id,
            Lease {
                receipt_handle: receipt_handle.into(),
                expires: received + self.visibility,
                received,
                revoked: revoked.clone(),
            },
        );
        (id, revoked)
    }

    /// Ends a lease without revoking it, returning its receipt handle, or
    /// `None` when it already ended.
    pub(super) fn remove(&self, id: u64) -> Option<Arc<str>> {
        Some(self.entries.lock().unwrap().remove(&id)?.receipt_handle)
    }

    /// Ends a lease and revokes its delivery.
    pub(super) fn revoke(&self, id: u64) {
        if let Some(lease) = self.entries.lock().unwrap().remove(&id) {
            lease.revoked.cancel();
        }
    }

    /// Ends every lease, returning their receipt handles.
    pub(super) fn drain(&self) -> Vec<Arc<str>> {
        let mut entries = self.entries.lock().unwrap();
        entries
            .drain()
            .map(|(_, lease)| lease.receipt_handle)
            .collect()
    }

    /// Revokes the leases that expired at `now`, and returns those due for an
    /// extension: a third of their visibility has passed. Each comes with the
    /// visibility timeout to request, which never reaches past SQS's limit.
    fn due(&self, now: Instant) -> Vec<(u64, Arc<str>, Duration)> {
        let mut entries = self.entries.lock().unwrap();
        entries.retain(|_, lease| {
            let expired = lease.expires <= now;
            if expired {
                lease.revoked.cancel();
            }
            !expired
        });
        entries
            .iter()
            .filter(|(_, lease)| {
                lease.expires.saturating_duration_since(now) <= self.visibility * 2 / 3
            })
            .filter_map(|(id, lease)| {
                let remaining = MAX_VISIBILITY.saturating_sub(now.duration_since(lease.received));
                let timeout = self.visibility.min(remaining);
                (timeout >= Duration::from_secs(1))
                    .then(|| (*id, lease.receipt_handle.clone(), timeout))
            })
            .collect()
    }

    fn extended(&self, id: u64, expires: Instant) {
        if let Some(lease) = self.entries.lock().unwrap().get_mut(&id) {
            lease.expires = lease.expires.max(expires);
        }
    }

    /// How often the keeper looks for leases due for an extension.
    fn interval(&self) -> Duration {
        (self.visibility / 3).clamp(Duration::from_millis(500), Duration::from_secs(60))
    }
}

/// Extends due leases and releases abandoned messages until `stop` is cancelled.
pub(super) async fn keep(
    client: Client,
    queue_url: String,
    leases: Arc<Leases>,
    mut released: mpsc::UnboundedReceiver<Arc<str>>,
    stop: CancellationToken,
) {
    let mut ticks = tokio::time::interval(leases.interval());
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = ticks.tick() => {
                let due = leases.due(Instant::now());
                for chunk in due.chunks(BATCH) {
                    extend(&client, &queue_url, &leases, chunk).await;
                }
            }
            Some(handle) = released.recv() => {
                let mut handles = vec![handle];
                while handles.len() < BATCH {
                    match released.try_recv() {
                        Ok(handle) => handles.push(handle),
                        Err(_) => break,
                    }
                }
                release(&client, &queue_url, &handles).await;
            }
        }
    }
}

async fn extend(
    client: &Client,
    queue_url: &str,
    leases: &Leases,
    chunk: &[(u64, Arc<str>, Duration)],
) {
    let requested = Instant::now();
    let entries = chunk.iter().filter_map(|(id, handle, timeout)| {
        ChangeMessageVisibilityBatchRequestEntry::builder()
            .id(id.to_string())
            .receipt_handle(handle.as_ref())
            .visibility_timeout(timeout.as_secs() as i32)
            .build()
            .ok()
    });
    let result = client
        .change_message_visibility_batch()
        .queue_url(queue_url)
        .set_entries(Some(entries.collect()))
        .send()
        .await;
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            // Leases that expire before a later attempt succeeds are revoked.
            warn!(error = %aws_sdk_sqs::error::DisplayErrorContext(&error), "extending SQS visibility failed");
            return;
        }
    };
    let timeouts: HashMap<String, Duration> = chunk
        .iter()
        .map(|(id, _, timeout)| (id.to_string(), *timeout))
        .collect();
    for entry in output.successful() {
        if let (Ok(id), Some(timeout)) = (entry.id().parse(), timeouts.get(entry.id())) {
            leases.extended(id, requested + *timeout);
        }
    }
    for entry in output.failed() {
        // An invalid receipt handle means the message was deleted or received again.
        if let Ok(id) = entry.id().parse() {
            leases.revoke(id);
        }
    }
}

/// Makes abandoned messages visible again at once, so another receive can take them.
pub(super) async fn release(client: &Client, queue_url: &str, handles: &[Arc<str>]) {
    for chunk in handles.chunks(BATCH) {
        let entries = chunk.iter().enumerate().filter_map(|(index, handle)| {
            ChangeMessageVisibilityBatchRequestEntry::builder()
                .id(index.to_string())
                .receipt_handle(handle.as_ref())
                .visibility_timeout(0)
                .build()
                .ok()
        });
        let result = client
            .change_message_visibility_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries.collect()))
            .send()
            .await;
        if let Err(error) = result {
            warn!(error = %aws_sdk_sqs::error::DisplayErrorContext(&error), "releasing SQS messages failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_are_due_after_a_third_of_their_visibility() {
        let leases = Leases::new(Duration::from_secs(30));
        let start = Instant::now();
        let (id, revoked) = leases.add("handle", start);
        assert!(leases.due(start + Duration::from_secs(9)).is_empty());
        let due = leases.due(start + Duration::from_secs(10));
        assert_eq!(due.len(), 1);
        assert_eq!((due[0].0, due[0].2), (id, Duration::from_secs(30)));
        leases.extended(id, start + Duration::from_secs(40));
        assert!(leases.due(start + Duration::from_secs(19)).is_empty());
        assert!(!revoked.is_cancelled());
    }

    #[test]
    fn expired_leases_are_revoked() {
        let leases = Leases::new(Duration::from_secs(30));
        let start = Instant::now();
        let (id, revoked) = leases.add("handle", start);
        assert!(leases.due(start + Duration::from_secs(30)).is_empty());
        assert!(revoked.is_cancelled());
        assert_eq!(leases.remove(id), None);
    }

    #[test]
    fn extensions_stop_at_the_twelve_hour_limit() {
        let leases = Leases::new(Duration::from_secs(600));
        let start = Instant::now();
        let (id, _) = leases.add("handle", start);
        let late = start + MAX_VISIBILITY - Duration::from_secs(120);
        leases.extended(id, late + Duration::from_secs(300));
        let due = leases.due(late);
        assert_eq!(due[0].2, Duration::from_secs(120));
        leases.extended(id, start + MAX_VISIBILITY);
        assert!(
            leases
                .due(start + MAX_VISIBILITY - Duration::from_millis(500))
                .is_empty()
        );
    }

    #[test]
    fn removed_and_revoked_leases_end() {
        let leases = Leases::new(Duration::from_secs(30));
        let start = Instant::now();
        let (first, first_revoked) = leases.add("first", start);
        let (second, second_revoked) = leases.add("second", start);
        assert_eq!(leases.remove(first).as_deref(), Some("first"));
        assert!(!first_revoked.is_cancelled());
        leases.revoke(second);
        assert!(second_revoked.is_cancelled());
        assert!(leases.drain().is_empty());
        let _ = leases.add("third", start);
        assert_eq!(leases.drain().len(), 1);
    }
}
