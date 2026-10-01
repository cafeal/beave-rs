//! Values in flight between the two ends of a channel and their completion.
use super::{record::ChannelRaw, sink::ChannelSink, source::ChannelSource};
use crate::{message::OrderingKey, shutdown::CancellationToken, sink::Completion};
use tokio::sync::{mpsc, oneshot};

/// A value handed from an upstream publication to the downstream subscription.
pub(super) struct Queued<T> {
    pub(super) value: T,
    pub(super) done: oneshot::Sender<()>,
    pub(super) abandoned: CancellationToken,
    /// Trace context of the code that sent the value.
    pub(super) propagation: Vec<(String, String)>,
    pub(super) origin: Origin,
}

/// What a channel delivery keeps of the upstream delivery that produced it.
#[derive(Clone, Default)]
pub(super) struct Origin {
    pub(super) ordering_key: Option<OrderingKey>,
    pub(super) raw: ChannelRaw,
}

/// The sender's trace context, so the receiving subscription continues its trace.
pub(super) fn propagation() -> Vec<(String, String)> {
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
pub(super) async fn enqueue<T>(
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
