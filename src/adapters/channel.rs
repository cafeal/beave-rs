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
    message::{Delivery, OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    sink::{Completion, Sink},
    source::{Receive, ReceiveError, Source},
};
use serde::{Serialize, Serializer};
use std::{
    any::Any,
    fmt::{self, Debug},
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::{mpsc, oneshot};

/// A value handed from an upstream publication to the downstream subscription.
struct Queued<T> {
    value: T,
    done: oneshot::Sender<()>,
    abandoned: CancellationToken,
    /// Trace context of the code that sent the value.
    propagation: Vec<(String, String)>,
    origin: Origin,
}

/// What a channel delivery keeps of the upstream delivery that produced it.
#[derive(Clone, Default)]
struct Origin {
    ordering_key: Option<OrderingKey>,
    raw: ChannelRaw,
}

/// The raw form of a channel delivery: the undecoded upstream delivery whose
/// handler produced the value, such as a `KafkaRecord<Vec<u8>>`.
///
/// Dead letters of a downstream subscription carry it, so they keep the
/// original payload and broker metadata. Through several channels it stays the
/// delivery of the first subscription. It is empty for values sent by
/// application code through a [`ChannelSender`] or [`Sink::publish`].
///
/// It serializes as the upstream raw form, or as `None` when empty. Use
/// [`downcast_ref`](Self::downcast_ref) to read it as its concrete type.
#[derive(Clone, Default)]
pub struct ChannelRaw(Option<Arc<dyn Raw>>);

/// A type-erased [`SourceMessage::Raw`].
trait Raw: erased_serde::Serialize + Any + Debug + Send + Sync {}

impl<R: Serialize + Any + Debug + Send + Sync> Raw for R {}

erased_serde::serialize_trait_object!(Raw);

impl ChannelRaw {
    fn of<R: Serialize + Debug + Send + Sync + 'static>(raw: R) -> Self {
        match (&raw as &dyn Any).downcast_ref::<Self>() {
            // A delivery of another channel already carries the original form.
            Some(raw) => raw.clone(),
            None => Self(Some(Arc::new(raw))),
        }
    }

    /// The upstream raw form, if there is one and it is an `R`.
    pub fn downcast_ref<R: 'static>(&self) -> Option<&R> {
        let raw: &dyn Any = self.0.as_deref()?;
        raw.downcast_ref()
    }

    /// Whether the value was sent without an upstream delivery.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl Debug for ChannelRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(raw) => raw.fmt(f),
            None => f.write_str("None"),
        }
    }
}

impl Serialize for ChannelRaw {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Some(raw) => raw.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

/// A value prepared for a [`ChannelSink`], with the ordering key and raw form
/// of the delivery that produced it.
///
/// Application code calling [`Sink::publish`] directly can convert a value into
/// one without an upstream delivery, as [`Sink::prepare`] does.
#[derive(Clone)]
pub struct ChannelOutput<T> {
    value: T,
    origin: Origin,
}

impl<T> From<T> for ChannelOutput<T> {
    fn from(value: T) -> Self {
        Self {
            value,
            origin: Origin::default(),
        }
    }
}

/// The sender's trace context, so the receiving subscription continues its trace.
fn propagation() -> Vec<(String, String)> {
    #[cfg(feature = "opentelemetry")]
    return crate::telemetry::current_fields();
    #[cfg(not(feature = "opentelemetry"))]
    Vec::new()
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
        ChannelSink::new(sender),
        ChannelSource {
            receiver,
            stops_on_shutdown: false,
        },
    )
}

/// Enqueues a value, waiting for capacity, and returns the completion that resolves
/// when the receiving end takes responsibility for it. Dropping the completion before
/// then abandons the value. Cancellation while waiting for capacity never enqueues it.
async fn enqueue<T>(
    sender: &mpsc::Sender<Queued<T>>,
    value: T,
    origin: Origin,
) -> anyhow::Result<Completion> {
    let permit = sender
        .reserve()
        .await
        .map_err(|_| anyhow::anyhow!("channel receiving end has stopped"))?;
    let (done, completed) = oneshot::channel();
    let abandoned = CancellationToken::new();
    let guard = AbandonOnDrop(Some(abandoned.clone()));
    permit.send(Queued {
        value,
        done,
        abandoned,
        propagation: propagation(),
        origin,
    });
    Ok(Completion::pending(async move {
        let result = completed.await;
        guard.disarm();
        result.map_err(|_| {
            anyhow::anyhow!(
                "channel receiving end stopped before taking responsibility for the value"
            )
        })
    }))
}

/// Upstream end of a channel, used as a subscription's sink.
///
/// Each value keeps the ordering key and [raw form](ChannelRaw) of the delivery
/// whose handler produced it, and the downstream delivery carries both.
///
/// A subscription publishing here frees its job slot once the value is enqueued and
/// acknowledges its input once the receiving end has taken responsibility for the
/// value. Created by [`channel`], that is when the downstream subscription
/// acknowledges it: its outputs were published, or its error policy dead-lettered or
/// discarded it. Created by [`ChannelSink::bounded`], that is when application code
/// takes it from the [`ChannelReceiver`]. Until then the upstream delivery stays
/// unacknowledged. Calling [`Sink::publish`] directly waits for both steps.
///
/// Each clone is an independent sender with its own close state, so several
/// subscriptions can feed one channel. The receiving end ends once every clone has
/// been closed or dropped.
pub struct ChannelSink<T> {
    sender: Mutex<Option<mpsc::Sender<Queued<T>>>>,
}

impl<T> ChannelSink<T> {
    /// A sink whose values application code receives from the returned
    /// [`ChannelReceiver`]. Panics if capacity is zero.
    pub fn bounded(capacity: usize) -> (Self, ChannelReceiver<T>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (Self::new(sender), ChannelReceiver { receiver })
    }

    fn new(sender: mpsc::Sender<Queued<T>>) -> Self {
        Self {
            sender: Mutex::new(Some(sender)),
        }
    }

    fn sender(&self) -> Option<mpsc::Sender<Queued<T>>> {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl<T> Clone for ChannelSink<T> {
    fn clone(&self) -> Self {
        Self {
            sender: Mutex::new(self.sender()),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Sink<T> for ChannelSink<T> {
    type Prepared = ChannelOutput<T>;

    /// A value without an upstream delivery: no ordering key and an empty raw form.
    fn prepare(&self, value: T) -> anyhow::Result<ChannelOutput<T>> {
        Ok(value.into())
    }

    fn prepare_from<M: SourceMessage>(
        &self,
        value: T,
        delivery: &M,
    ) -> anyhow::Result<ChannelOutput<T>> {
        Ok(ChannelOutput {
            value,
            origin: Origin {
                ordering_key: delivery.ordering_key(),
                raw: ChannelRaw::of(delivery.raw()),
            },
        })
    }

    async fn publish(&self, output: &ChannelOutput<T>) -> anyhow::Result<()> {
        self.submit(output).await?.wait().await
    }

    /// Returns once the value is enqueued. The completion resolves when the
    /// receiving end takes responsibility for it.
    async fn submit(&self, output: &ChannelOutput<T>) -> anyhow::Result<Completion> {
        let sender = self
            .sender()
            .ok_or_else(|| anyhow::anyhow!("channel sink is closed"))?;
        enqueue(&sender, output.value.clone(), output.origin.clone()).await
    }

    /// Rejects later publications from this clone. The receiving end ends after every
    /// clone is closed or dropped and the values already sent have been received.
    async fn close(&self) -> anyhow::Result<()> {
        self.sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        Ok(())
    }
}

/// Application-side receiving end of [`ChannelSink::bounded`].
pub struct ChannelReceiver<T> {
    receiver: mpsc::Receiver<Queued<T>>,
}

impl<T> ChannelReceiver<T> {
    /// The next value, or `None` once every sink clone is closed or dropped and the
    /// buffer is empty. Taking a value completes its publication, so the upstream
    /// delivery can be acknowledged. Cancellation-safe.
    pub async fn recv(&mut self) -> Option<T> {
        loop {
            let queued = self.receiver.recv().await?;
            // The upstream publication was abandoned while the value was buffered.
            if queued.abandoned.is_cancelled() {
                continue;
            }
            let _ = queued.done.send(());
            return Some(queued.value);
        }
    }

    /// Reject new values; buffered values remain available to `recv`. Values still
    /// buffered when the receiver is dropped fail their publications.
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
        let permit = self
            .sender
            .reserve()
            .await
            .map_err(|_| anyhow::anyhow!("channel receiving end has stopped"))?;
        // Nobody waits for completion, so the value can never be abandoned.
        let (done, _) = oneshot::channel();
        permit.send(Queued {
            value,
            done,
            abandoned: CancellationToken::new(),
            propagation: propagation(),
            origin: Origin::default(),
        });
        Ok(())
    }

    /// Enqueue a value and wait until the subscription acknowledges it: its outputs
    /// were published, or its error policy dead-lettered or discarded it. Dropping the
    /// returned future after the value was enqueued abandons the delivery.
    pub async fn send_and_wait(&self, value: T) -> anyhow::Result<()> {
        enqueue(&self.sender, value, Origin::default())
            .await?
            .wait()
            .await
    }
}

/// Downstream end of a channel, used as a subscription's source.
///
/// Each delivery carries the ordering key of the upstream delivery that produced
/// its value, so [`ProcessingOrder::PerKey`](crate::ProcessingOrder::PerKey)
/// keeps the upstream order, and its [raw form](ChannelRaw) is that upstream
/// delivery.
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
    type Message = Delivery<T, ChannelRaw>;

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
                propagation,
                origin,
            } = queued;
            let mut delivery = Delivery::new(value, move || async move {
                // A sender that is not waiting has either abandoned the value or
                // completed at enqueue.
                let _ = done.send(());
                Ok(())
            })
            .with_revocation(abandoned)
            .with_propagation_fields(propagation);
            if let Some(key) = origin.ordering_key {
                delivery = delivery.with_ordering_key(key);
            }
            return Ok(Receive::Message(delivery.with_raw(origin.raw)));
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
