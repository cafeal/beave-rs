//! The downstream end of a channel and its application-side sender.
use super::{
    queue::{Origin, Queued, enqueue, propagation},
    record::ChannelRaw,
};
use crate::{
    message::Delivery,
    shutdown::CancellationToken,
    source::{Receive, ReceiveError, Source},
};
use tokio::sync::{mpsc, oneshot};

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
/// Created by [`channel`](crate::channel), application shutdown does not stop this source: it keeps
/// receiving until every upstream sink clone is closed or dropped, so values the
/// upstream subscription is still draining can complete. Each delivery is revoked
/// when the upstream publication is abandoned, for example after an upstream
/// revocation or drain timeout.
///
/// Created by [`ChannelSource::bounded`], it is fed by a [`ChannelSender`] and stops
/// receiving on application shutdown like other sources.
pub struct ChannelSource<T> {
    pub(super) receiver: mpsc::Receiver<Queued<T>>,
    pub(super) stops_on_shutdown: bool,
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
