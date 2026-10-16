//! Parallel decoding of received deliveries, finished in receive order.
use super::{
    instruments::{Instruments, Stage},
    scheduler::Admission,
};
use crate::{error::BoxError, message::SourceMessage};
use std::{collections::VecDeque, time::Instant};
use tokio::task::JoinHandle;
use tracing::{Span, info_span};

/// A delivery with the outcome of decoding it, ready to be distributed.
pub(super) struct Decoded<M: SourceMessage> {
    pub(super) delivery: M,
    pub(super) value: Result<M::Item, BoxError>,
    /// The delivery's `message` span, which also covers its decoding.
    pub(super) span: Span,
}

/// Deliveries being decoded, oldest first. Each decodes in a task of its own;
/// [`next`](Self::next) returns them in the order they were started, so a
/// delivery that decodes quickly waits for earlier ones.
pub(super) struct Decoding<M: SourceMessage> {
    pending: VecDeque<(Admission, JoinHandle<Decoded<M>>)>,
}

impl<M: SourceMessage> Decoding<M> {
    pub(super) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    /// Starts decoding `delivery` on the blocking thread pool, so a codec
    /// that takes long does not hold up receiving or running handlers.
    pub(super) fn start(
        &mut self,
        admission: Admission,
        delivery: M,
        span: Span,
        instruments: &Instruments,
    ) {
        let instruments = instruments.clone();
        let task = tokio::task::spawn_blocking(move || {
            let value = span.in_scope(|| {
                let started = Instant::now();
                let value = info_span!("decode").in_scope(|| delivery.decode());
                instruments.record(Stage::Decode, started);
                value
            });
            Decoded {
                delivery,
                value,
                span,
            }
        });
        self.pending.push_back((admission, task));
    }

    /// The oldest delivery once it has decoded, or `None` when nothing is
    /// decoding. Cancellation-safe: a cancelled call leaves the delivery in
    /// place. An error means the decoding task panicked.
    pub(super) async fn next(&mut self) -> Option<Result<(Admission, Decoded<M>), BoxError>> {
        let (_, task) = self.pending.front_mut()?;
        let result = task.await;
        let (admission, _) = self.pending.pop_front()?;
        Some(
            result
                .map(|decoded| (admission, decoded))
                .map_err(BoxError::from),
        )
    }

    /// Abandons every delivery still decoding, unacknowledged, and returns
    /// their admissions.
    pub(super) fn discard(&mut self) -> Vec<Admission> {
        self.pending
            .drain(..)
            .map(|(admission, task)| {
                task.abort();
                admission
            })
            .collect()
    }
}
