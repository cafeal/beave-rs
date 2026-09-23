//! Bounded, typed, process-local message transport.
use crate::{Delivery, Receive, ReceiveError, Sink, Source};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};

/// A source backed by a bounded Tokio channel. ACK is a local no-op.
pub struct ChannelSource<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T> ChannelSource<T> {
    pub fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self { receiver }
    }

    /// Panics if capacity is zero, matching `tokio::sync::mpsc::channel`.
    pub fn bounded(capacity: usize) -> (mpsc::Sender<T>, Self) {
        let (sender, receiver) = mpsc::channel(capacity);
        (sender, Self::new(receiver))
    }
}

impl<T: Clone + Send + Sync + 'static> Source for ChannelSource<T> {
    type Message = Delivery<T>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        Ok(match self.receiver.recv().await {
            Some(value) => Receive::Message(Delivery::untracked(value)),
            None => Receive::End,
        })
    }

    /// Reject new sends; buffered values remain available until the source is dropped.
    async fn close(&mut self) -> anyhow::Result<()> {
        self.receiver.close();
        Ok(())
    }
}

struct Shared<T> {
    sender: Mutex<Option<mpsc::Sender<T>>>,
    closed: watch::Sender<bool>,
}

/// Publication acknowledges enqueueing, not downstream processing or persistence.
/// Clones share one close state; closing any clone closes all of them.
pub struct ChannelSink<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for ChannelSink<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> ChannelSink<T> {
    pub fn new(sender: mpsc::Sender<T>) -> Self {
        let (closed, _) = watch::channel(false);
        Self {
            shared: Arc::new(Shared {
                sender: Mutex::new(Some(sender)),
                closed,
            }),
        }
    }

    /// Panics if capacity is zero, matching `tokio::sync::mpsc::channel`.
    pub fn bounded(capacity: usize) -> (Self, mpsc::Receiver<T>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (Self::new(sender), receiver)
    }
}

impl<T: Clone + Send + Sync + 'static> Sink<T> for ChannelSink<T> {
    type Prepared = T;

    fn prepare(&self, value: T) -> anyhow::Result<T> {
        Ok(value)
    }

    async fn publish(&self, output: &T) -> anyhow::Result<()> {
        let mut closed = self.shared.closed.subscribe();
        let sender = self
            .shared
            .sender
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("channel sink is closed"))?;
        // Reserve first: cancellation while waiting never enqueues a value.
        let permit = tokio::select! {
            biased;
            _ = closed.changed() => anyhow::bail!("channel sink is closed"),
            permit = sender.reserve() => permit.map_err(|_| anyhow::anyhow!("channel receiver is closed"))?,
        };
        let value = output.clone();
        // Serialize the enqueue with close so no publication succeeds after close.
        let guard = self.shared.sender.lock().unwrap();
        anyhow::ensure!(guard.is_some(), "channel sink is closed");
        permit.send(value);
        Ok(())
    }

    async fn close(&self) -> anyhow::Result<()> {
        self.shared.sender.lock().unwrap().take();
        self.shared.closed.send_replace(true);
        Ok(())
    }
}

/// Connect subscriptions with a bounded typed channel, without a codec.
/// Panics if capacity is zero.
pub fn channel<T>(capacity: usize) -> (ChannelSink<T>, ChannelSource<T>) {
    let (sink, receiver) = ChannelSink::bounded(capacity);
    (sink, ChannelSource::new(receiver))
}
