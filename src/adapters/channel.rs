//! Bounded, typed, process-local connection between subscriptions.
//!
//! [`channel`] connects the sink of one subscription to the source of the next, so a
//! pipeline can split I/O-bound and CPU-bound stages into subscriptions with their
//! own concurrency, retries, and error policies, for example an async handler that
//! fetches data followed by a [`blocking`](crate::blocking) handler that scores it.
//!
//! ```
//! use beavers::{App, InMemorySink, IterSource, Result, blocking, channel};
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
//! let (to_score, fetched) = channel(16);
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
struct Queued<T> {
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

/// Connect subscriptions so each upstream delivery is acknowledged only after the
/// downstream subscription finishes the value. Panics if capacity is zero.
///
/// Use [`ChannelSource::bounded`] or [`ChannelSink::bounded`] when application code
/// sends to or receives from a subscription instead.
pub fn channel<T>(capacity: usize) -> (ChannelSink<T>, ChannelSource<T>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        ChannelSink::with_route(Route::Subscription(sender)),
        ChannelSource {
            receiver,
            stops_on_shutdown: false,
        },
    )
}

/// Enqueues a value for a downstream subscription and, when `wait` is set, waits
/// until that subscription acknowledges it.
async fn hand_off<T>(sender: &mpsc::Sender<Queued<T>>, value: T, wait: bool) -> anyhow::Result<()> {
    // Reserve first: cancellation while waiting for capacity never enqueues a value.
    let permit = sender
        .reserve()
        .await
        .map_err(|_| anyhow::anyhow!("channel downstream subscription has stopped"))?;
    let (done, completed) = oneshot::channel();
    let abandoned = CancellationToken::new();
    if !wait {
        permit.send(Queued {
            value,
            done,
            abandoned,
        });
        return Ok(());
    }
    let guard = AbandonOnDrop(Some(abandoned.clone()));
    permit.send(Queued {
        value,
        done,
        abandoned,
    });
    let result = completed.await;
    guard.disarm();
    result.map_err(|_| {
        anyhow::anyhow!("channel downstream subscription stopped before acknowledging the value")
    })
}

/// Where a [`ChannelSink`] delivers its values.
enum Route<T> {
    /// A downstream subscription whose ACK completes the publication.
    Subscription(mpsc::Sender<Queued<T>>),
    /// Application code; enqueueing completes the publication.
    Application(mpsc::Sender<T>),
}

impl<T> Clone for Route<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Subscription(sender) => Self::Subscription(sender.clone()),
            Self::Application(sender) => Self::Application(sender.clone()),
        }
    }
}

/// Upstream end of a channel, used as a subscription's sink.
///
/// Created by [`channel`], publication succeeds once the downstream subscription has
/// acknowledged the value: its outputs were published, or its error policy
/// dead-lettered or discarded it. Created by [`ChannelSink::bounded`], publication
/// succeeds once the value is enqueued for a [`ChannelReceiver`]; a value still
/// buffered there is lost if the process stops.
///
/// Each clone is an independent sender with its own close state, so several
/// subscriptions can feed one channel. The receiving end ends once every clone has
/// been closed or dropped.
pub struct ChannelSink<T> {
    route: Mutex<Option<Route<T>>>,
}

impl<T> ChannelSink<T> {
    /// A sink whose values application code receives from the returned
    /// [`ChannelReceiver`]. Panics if capacity is zero.
    pub fn bounded(capacity: usize) -> (Self, ChannelReceiver<T>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self::with_route(Route::Application(sender)),
            ChannelReceiver { receiver },
        )
    }

    fn with_route(route: Route<T>) -> Self {
        Self {
            route: Mutex::new(Some(route)),
        }
    }

    fn route(&self) -> Option<Route<T>> {
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl<T> Clone for ChannelSink<T> {
    fn clone(&self) -> Self {
        Self {
            route: Mutex::new(self.route()),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Sink<T> for ChannelSink<T> {
    type Prepared = T;

    fn prepare(&self, value: T) -> anyhow::Result<T> {
        Ok(value)
    }

    async fn publish(&self, output: &T) -> anyhow::Result<()> {
        match self.route() {
            None => anyhow::bail!("channel sink is closed"),
            Some(Route::Subscription(sender)) => hand_off(&sender, output.clone(), true).await,
            Some(Route::Application(sender)) => {
                // Reserve first: cancellation while waiting for capacity never enqueues a value.
                let permit = sender
                    .reserve()
                    .await
                    .map_err(|_| anyhow::anyhow!("channel receiver is closed"))?;
                permit.send(output.clone());
                Ok(())
            }
        }
    }

    /// Rejects later publications from this clone. The receiving end ends after every
    /// clone is closed or dropped and the values already sent have been received.
    async fn close(&self) -> anyhow::Result<()> {
        self.route
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Ok(())
    }
}

/// Application-side receiving end of [`ChannelSink::bounded`].
pub struct ChannelReceiver<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T> ChannelReceiver<T> {
    /// The next value, or `None` once every sink clone is closed or dropped and the
    /// buffer is empty. Cancellation-safe.
    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    /// Reject new values; buffered values remain available to `recv`.
    pub fn close(&mut self) {
        self.receiver.close();
    }
}

/// Application-side sending end of [`ChannelSource::bounded`]. Clones feed the same
/// source, which ends once every clone is dropped.
pub struct ChannelSender<T> {
    sender: mpsc::Sender<Queued<T>>,
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> ChannelSender<T> {
    /// Enqueue a value, waiting for capacity. Returns once the value is buffered.
    /// Cancellation while waiting for capacity never enqueues it.
    pub async fn send(&self, value: T) -> anyhow::Result<()> {
        hand_off(&self.sender, value, false).await
    }

    /// Enqueue a value and wait until the subscription acknowledges it: its outputs
    /// were published, or its error policy dead-lettered or discarded it. Dropping the
    /// returned future after the value was enqueued abandons the delivery.
    pub async fn send_and_wait(&self, value: T) -> anyhow::Result<()> {
        hand_off(&self.sender, value, true).await
    }
}

/// Downstream end of a channel, used as a subscription's source.
///
/// Created by [`channel`], application shutdown does not stop this source: it keeps
/// receiving until every upstream sink clone is closed or dropped, so values the
/// upstream subscription is still draining can complete. Each delivery is revoked
/// when the upstream publication is abandoned, for example after an upstream
/// revocation or drain timeout.
///
/// Created by [`ChannelSource::bounded`], it is fed by a [`ChannelSender`] and stops
/// receiving on application shutdown like other sources.
pub struct ChannelSource<T> {
    receiver: mpsc::Receiver<Queued<T>>,
    stops_on_shutdown: bool,
}

impl<T> ChannelSource<T> {
    /// A source that application code feeds through the returned [`ChannelSender`].
    /// Panics if capacity is zero.
    pub fn bounded(capacity: usize) -> (ChannelSender<T>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            ChannelSender { sender },
            Self {
                receiver,
                stops_on_shutdown: true,
            },
        )
    }
}

impl<T: Clone + Send + Sync + 'static> Source for ChannelSource<T> {
    type Message = Delivery<T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        loop {
            let Some(queued) = self.receiver.recv().await else {
                return Ok(Receive::End);
            };
            // The upstream publication was abandoned while the value was buffered.
            if queued.abandoned.is_cancelled() {
                continue;
            }
            let Queued {
                value,
                done,
                abandoned,
            } = queued;
            let delivery = Delivery::new(value, move || async move {
                // A sender that is not waiting has either abandoned the value or
                // completed at enqueue.
                let _ = done.send(());
                Ok(())
            });
            return Ok(Receive::Message(delivery.with_revocation(abandoned)));
        }
    }

    fn stops_on_shutdown(&self) -> bool {
        self.stops_on_shutdown
    }

    /// Reject new values; buffered values remain available until the source is dropped.
    async fn close(&mut self) -> anyhow::Result<()> {
        self.receiver.close();
        Ok(())
    }
}
