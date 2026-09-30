//! The final stage of a delivery: prepare outputs, publish them, and acknowledge.
use super::{
    instruments::{Instruments, Stage},
    processing::retry_publish,
};
use crate::{
    message::SourceMessage, retry::RetryPolicy, sink, sink::Sink, transaction::TransactionalSink,
};
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Instant};
use tracing::{Instrument, info_span};

pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a delivery's processing ended at the completion stage.
pub(super) enum Completion<M> {
    Done,
    /// Every output was submitted but some have not completed yet; the delivery is
    /// returned unacknowledged, to be acknowledged once they succeed.
    Pending(M, Vec<sink::Completion>),
    /// Preparing an output failed before anything was published; the
    /// delivery is returned unacknowledged for failure routing.
    Encode(M, anyhow::Error),
}

/// Completes deliveries of one subscription with the sink's output.
pub(super) trait Complete<M, O>: Send + Sync + 'static {
    /// Prepares every output, then publishes them and acknowledges `delivery`.
    /// With no outputs, only acknowledges `delivery`.
    fn complete<'a>(
        &'a self,
        delivery: M,
        outputs: Vec<O>,
        policy: &'a RetryPolicy,
        instruments: &'a Instruments,
    ) -> BoxFuture<'a, anyhow::Result<Completion<M>>>;
    fn close(&self) -> BoxFuture<'_, anyhow::Result<()>>;
}

/// Publishes outputs sequentially, then acknowledges the delivery separately.
pub(super) struct Publish<K>(pub(super) Arc<K>);

impl<M, O, K> Complete<M, O> for Publish<K>
where
    M: SourceMessage,
    O: Send + 'static,
    K: Sink<O>,
{
    fn complete<'a>(
        &'a self,
        delivery: M,
        outputs: Vec<O>,
        policy: &'a RetryPolicy,
        instruments: &'a Instruments,
    ) -> BoxFuture<'a, anyhow::Result<Completion<M>>> {
        Box::pin(async move {
            // Preparing all outputs first means an encode failure never follows a partial publish.
            let outputs = match prepare(&*self.0, outputs, instruments) {
                Ok(outputs) => outputs,
                Err(error) => return Ok(Completion::Encode(delivery, error)),
            };
            let mut completions = Vec::with_capacity(outputs.len());
            if !outputs.is_empty() {
                let started = Instant::now();
                let failures = &instruments.publish_failures;
                async {
                    for output in &outputs {
                        completions
                            .push(retry_publish(policy, failures, || self.0.submit(output)).await?);
                    }
                    anyhow::Ok(())
                }
                .instrument(info_span!("publish", outputs = outputs.len()))
                .await?;
                instruments.record(Stage::Publish, started);
            }
            if !completions.iter().all(sink::Completion::is_done) {
                return Ok(Completion::Pending(delivery, completions));
            }
            acknowledge(delivery, instruments).await?;
            Ok(Completion::Done)
        })
    }

    fn close(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(self.0.close())
    }
}

/// Publishes outputs and acknowledges the delivery in one sink transaction.
pub(super) struct Transact<K, O> {
    pub(super) sink: Arc<K>,
    pub(super) marker: PhantomData<fn(O)>,
}

impl<M, O, K> Complete<M, O> for Transact<K, O>
where
    M: SourceMessage + Sync,
    O: Send + 'static,
    K: TransactionalSink<M, O>,
{
    fn complete<'a>(
        &'a self,
        delivery: M,
        outputs: Vec<O>,
        policy: &'a RetryPolicy,
        instruments: &'a Instruments,
    ) -> BoxFuture<'a, anyhow::Result<Completion<M>>> {
        Box::pin(async move {
            let outputs = match prepare(&*self.sink, outputs, instruments) {
                Ok(outputs) => outputs,
                Err(error) => return Ok(Completion::Encode(delivery, error)),
            };
            let started = Instant::now();
            // Each retry commits a new transaction with the same prepared outputs.
            retry_publish(policy, &instruments.publish_failures, || {
                self.sink.commit(&delivery, &outputs)
            })
            .instrument(info_span!("commit", outputs = outputs.len()))
            .await?;
            instruments.record(Stage::Commit, started);
            instruments.acknowledged.increment(1);
            Ok(Completion::Done)
        })
    }

    fn close(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(self.sink.close())
    }
}

fn prepare<O, K: Sink<O>>(
    sink: &K,
    outputs: Vec<O>,
    instruments: &Instruments,
) -> anyhow::Result<Vec<K::Prepared>> {
    // A delivery without outputs, such as a discarded one, records no encode stage.
    if outputs.is_empty() {
        return Ok(Vec::new());
    }
    let started = Instant::now();
    let prepared = info_span!("encode").in_scope(|| {
        outputs
            .into_iter()
            .map(|output| sink.prepare(output))
            .collect()
    });
    instruments.record(Stage::Encode, started);
    prepared
}

pub(super) async fn acknowledge<M: SourceMessage>(
    delivery: M,
    instruments: &Instruments,
) -> anyhow::Result<()> {
    let started = Instant::now();
    delivery.ack().instrument(info_span!("ack")).await?;
    instruments.record(Stage::Ack, started);
    instruments.acknowledged.increment(1);
    Ok(())
}
