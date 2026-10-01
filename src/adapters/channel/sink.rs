//! The upstream end of a channel and its application-side receiver.
use super::{
    queue::{Origin, Queued, enqueue},
    record::{ChannelOutput, ChannelRaw},
};
use crate::{
    message::SourceMessage,
    sink::{Completion, Sink},
};
use std::sync::{Mutex, PoisonError};
use tokio::sync::mpsc;

/// Upstream end of a channel, used as a subscription's sink.
///
/// Each value keeps the ordering key and [raw form](ChannelRaw) of the delivery
/// whose handler produced it, and the downstream delivery carries both.
///
/// A subscription publishing here frees its job slot once the value is enqueued and
/// acknowledges its input once the receiving end has taken responsibility for the
/// value. Created by [`channel`](crate::channel), that is when the downstream subscription
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

    pub(super) fn new(sender: mpsc::Sender<Queued<T>>) -> Self {
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
