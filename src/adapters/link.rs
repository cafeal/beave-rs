//! Process-local connection between subscriptions that defers the upstream ACK.
//!
//! [`link`] connects the sink of one subscription to the source of the next, so a
//! pipeline can split I/O-bound and CPU-bound stages into subscriptions with their
//! own concurrency, retries, and error policies, for example an async handler that
//! fetches data followed by a [`blocking`](crate::blocking) handler that scores it.
//!
//! ```
//! use beavers::{App, InMemorySink, IterSource, Result, blocking, link};
//!
//! async fn fetch(id: u32) -> Result<String> {
//!     Ok(format!("document {id}"))
//! }
//!
//! fn score(document: String) -> Result<usize> {
//!     Ok(document.len())
//! }
//!
//! # #[tokio::main] async fn main() -> anyhow::Result<()> {
//! let (to_score, fetched) = link(16);
//! let scores = InMemorySink::default();
//! App::new()
//!     .subscribe("fetch", IterSource::new([1, 2]), to_score, fetch)
//!     .subscribe("score", fetched, scores.clone(), blocking(score))
//!     .run()
//!     .await?;
//! assert_eq!(scores.values(), [10, 10]);
//! # Ok(()) }
//! ```
use crate::{
    message::Delivery,
    shutdown::CancellationToken,
    sink::Sink,
    source::{Receive, ReceiveError, Source},
};
use std::sync::{Mutex, PoisonError};
use tokio::sync::{mpsc, oneshot};

/// A value handed from an upstream publication to the downstream subscription.
struct Linked<T> {
    value: T,
    done: oneshot::Sender<()>,
    abandoned: CancellationToken,
}

/// Cancels the downstream delivery when the upstream publication is dropped
/// before the downstream subscription completes it.
struct AbandonOnDrop(Option<CancellationToken>);

impl AbandonOnDrop {
    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        if let Some(token) = self.0.take() {
            token.cancel();
        }
    }
}

/// Connect two subscriptions so the upstream delivery is acknowledged only after the
/// downstream subscription finishes the linked value. Panics if capacity is zero.
pub fn link<T>(capacity: usize) -> (LinkSink<T>, LinkSource<T>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        LinkSink {
            sender: Mutex::new(Some(sender)),
        },
        LinkSource { receiver },
    )
}

/// Upstream end of a [`link`]. Publication succeeds once the downstream subscription
/// has acknowledged the value: its outputs were published, or its error policy
/// dead-lettered or discarded it.
///
/// The sink is not cloneable, so each link has exactly one upstream subscription.
pub struct LinkSink<T> {
    sender: Mutex<Option<mpsc::Sender<Linked<T>>>>,
}

impl<T: Clone + Send + Sync + 'static> Sink<T> for LinkSink<T> {
    type Prepared = T;

    fn prepare(&self, value: T) -> anyhow::Result<T> {
        Ok(value)
    }

    async fn publish(&self, output: &T) -> anyhow::Result<()> {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| anyhow::anyhow!("link sink is closed"))?;
        // Reserve first: cancellation while waiting for capacity never enqueues a value.
        let permit = sender
            .reserve()
            .await
            .map_err(|_| anyhow::anyhow!("link downstream subscription has stopped"))?;
        let (done, completed) = oneshot::channel();
        let abandoned = CancellationToken::new();
        let guard = AbandonOnDrop(Some(abandoned.clone()));
        permit.send(Linked {
            value: output.clone(),
            done,
            abandoned,
        });
        let result = completed.await;
        guard.disarm();
        result.map_err(|_| {
            anyhow::anyhow!("link downstream subscription stopped before acknowledging the value")
        })
    }

    /// Ends the downstream source after the values already sent have been received.
    async fn close(&self) -> anyhow::Result<()> {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Ok(())
    }
}

/// Downstream end of a [`link`].
///
/// Application shutdown does not stop this source: it keeps receiving until the
/// upstream sink is closed or dropped, so values the upstream subscription is still
/// draining can complete. Each delivery is revoked when the upstream publication is
/// abandoned, for example after an upstream revocation or drain timeout.
pub struct LinkSource<T> {
    receiver: mpsc::Receiver<Linked<T>>,
}

impl<T: Clone + Send + Sync + 'static> Source for LinkSource<T> {
    type Message = Delivery<T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        loop {
            let Some(linked) = self.receiver.recv().await else {
                return Ok(Receive::End);
            };
            // The upstream publication was abandoned while the value was buffered.
            if linked.abandoned.is_cancelled() {
                continue;
            }
            let Linked {
                value,
                done,
                abandoned,
            } = linked;
            let delivery = Delivery::new(value, move || async move {
                // An upstream that is no longer waiting has abandoned its delivery.
                let _ = done.send(());
                Ok(())
            });
            return Ok(Receive::Message(delivery.with_revocation(abandoned)));
        }
    }

    fn stops_on_shutdown(&self) -> bool {
        false
    }

    /// Reject new values; buffered values remain available until the source is dropped.
    async fn close(&mut self) -> anyhow::Result<()> {
        self.receiver.close();
        Ok(())
    }
}
