//! The final stage of a delivery: prepare outputs, publish them, and acknowledge.
use super::{
    config::SubscriptionConfig,
    instruments::{Instruments, Stage},
    processing::retry_publish,
    transaction::Batcher,
};
use crate::{
    error::{BoxError, Context},
    message::SourceMessage,
    retry::RetryPolicy,
    sink::{self, PublishRejected, Sink},
    transaction::TransactionalSink,
};
use metrics::Counter;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};
use tracing::{Instrument, info_span};

pub(super) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a delivery's processing ended at the completion stage.
pub(super) enum Completion<M> {
    Done,
    /// Every output was submitted but some have not completed yet; the delivery is
    /// returned unacknowledged, to be acknowledged once they succeed.
    Pending(M, Vec<sink::Completion>),
    /// The delivery joined a transaction that has not committed yet; the
    /// completion resolves when that transaction acknowledges it.
    Committing(sink::Completion),
    /// Preparing an output failed before anything was published; the
    /// delivery is returned unacknowledged for failure routing.
    Encode(M, BoxError),
    /// The sink rejected an output after every output submitted before it
    /// completed; the delivery is returned unacknowledged for failure routing.
    Rejected(M, BoxError),
}

/// Completes deliveries of one subscription with the sink's output.
pub(super) trait Complete<M, O>: Send + Sync + 'static {
    /// Starts background work before the subscription receives its first
    /// delivery.
    fn start(&self, _config: &SubscriptionConfig, _instruments: &Instruments) {}
    /// Prepares every output, then publishes them and acknowledges `delivery`.
    /// With no outputs, only acknowledges `delivery`.
    fn complete<'a>(
        &'a self,
        delivery: M,
        outputs: Vec<O>,
        policy: &'a RetryPolicy,
        instruments: &'a Instruments,
    ) -> BoxFuture<'a, Result<Completion<M>, BoxError>>;
    /// Checks before the first delivery is processed that deliveries of this
    /// source can be completed, and returns the delivery.
    fn verify<'a>(
        &'a self,
        delivery: M,
        policy: &'a RetryPolicy,
    ) -> BoxFuture<'a, Result<M, BoxError>>;
    fn close(&self) -> BoxFuture<'_, Result<(), BoxError>>;
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
    ) -> BoxFuture<'a, Result<Completion<M>, BoxError>> {
        Box::pin(async move {
            // Preparing all outputs first means an encode failure never follows a partial publish.
            let outputs = match prepare(&*self.0, outputs, &delivery, instruments) {
                Ok(outputs) => outputs,
                Err(error) => return Ok(Completion::Encode(delivery, error)),
            };
            let mut completions = Vec::with_capacity(outputs.len());
            if !outputs.is_empty() {
                let started = Instant::now();
                let failures = &instruments.publish_failures;
                let rejected = async {
                    for output in &outputs {
                        match retry_publish(policy, failures, Some(&instruments.health), || {
                            self.0.submit(output)
                        })
                        .await
                        {
                            Ok(completion) => completions.push(completion),
                            Err(error) if PublishRejected::is(error.as_ref()) => {
                                return Ok(Some(error));
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok::<_, BoxError>(None)
                }
                .instrument(info_span!("publish", outputs = outputs.len()))
                .await?;
                instruments.record(Stage::Publish, started);
                if let Some(error) = rejected {
                    // Routing acknowledges the delivery, so earlier outputs must
                    // reach their acknowledgement boundary first.
                    for completion in completions {
                        completion
                            .wait()
                            .await
                            .context("output completion failed")?;
                    }
                    return Ok(Completion::Rejected(delivery, error));
                }
            }
            if !completions.iter().all(sink::Completion::is_done) {
                return Ok(Completion::Pending(delivery, completions));
            }
            acknowledge(delivery, instruments).await?;
            Ok(Completion::Done)
        })
    }

    fn verify<'a>(&'a self, delivery: M, _: &'a RetryPolicy) -> BoxFuture<'a, Result<M, BoxError>> {
        Box::pin(async move { Ok(delivery) })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(self.0.close())
    }
}

/// Publishes outputs and acknowledges deliveries in batched sink transactions.
pub(super) struct Transact<K: Sink<O>, M, O> {
    sink: Arc<K>,
    batcher: Mutex<Option<Batcher<M, K::Prepared>>>,
}

impl<K: Sink<O>, M, O> Transact<K, M, O> {
    pub(super) fn new(sink: Arc<K>) -> Self {
        Self {
            sink,
            batcher: Mutex::new(None),
        }
    }
}

impl<M, O, K> Complete<M, O> for Transact<K, M, O>
where
    M: SourceMessage + Sync,
    O: Send + 'static,
    K: TransactionalSink<M, O>,
{
    fn start(&self, config: &SubscriptionConfig, instruments: &Instruments) {
        let batcher = Batcher::spawn(
            self.sink.clone(),
            config.transaction_batch,
            config.publish_retry.clone(),
            instruments.clone(),
        );
        *self.batcher.lock().unwrap() = Some(batcher);
    }

    fn complete<'a>(
        &'a self,
        delivery: M,
        outputs: Vec<O>,
        _: &'a RetryPolicy,
        instruments: &'a Instruments,
    ) -> BoxFuture<'a, Result<Completion<M>, BoxError>> {
        Box::pin(async move {
            let outputs = match prepare(&*self.sink, outputs, &delivery, instruments) {
                Ok(outputs) => outputs,
                Err(error) => return Ok(Completion::Encode(delivery, error)),
            };
            let enlister = self
                .batcher
                .lock()
                .unwrap()
                .as_ref()
                .map(Batcher::enlister)
                .context("transaction batching has not started")?;
            let committed = enlister.enlist(delivery, outputs).await?;
            Ok(Completion::Committing(committed))
        })
    }

    fn verify<'a>(
        &'a self,
        delivery: M,
        policy: &'a RetryPolicy,
    ) -> BoxFuture<'a, Result<M, BoxError>> {
        Box::pin(async move {
            retry_publish(policy, &Counter::noop(), None, || {
                self.sink.verify_source(&delivery)
            })
            .await
            .context("the source cannot join the sink's transactions")?;
            Ok(delivery)
        })
    }

    fn close(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            let batcher = self.batcher.lock().unwrap().take();
            let batched = match batcher {
                Some(batcher) => batcher.close().await,
                None => Ok(()),
            };
            batched.and(self.sink.close().await)
        })
    }
}

fn prepare<M: SourceMessage, O, K: Sink<O>>(
    sink: &K,
    outputs: Vec<O>,
    delivery: &M,
    instruments: &Instruments,
) -> Result<Vec<K::Prepared>, BoxError> {
    // A delivery without outputs, such as a discarded one, records no encode stage.
    if outputs.is_empty() {
        return Ok(Vec::new());
    }
    let started = Instant::now();
    let prepared = info_span!("encode").in_scope(|| {
        outputs
            .into_iter()
            .map(|output| sink.prepare_from(output, delivery))
            .collect()
    });
    instruments.record(Stage::Encode, started);
    prepared
}

pub(super) async fn acknowledge<M: SourceMessage>(
    delivery: M,
    instruments: &Instruments,
) -> Result<(), BoxError> {
    let started = Instant::now();
    delivery.ack().instrument(info_span!("ack")).await?;
    instruments.record(Stage::Ack, started);
    instruments.acknowledged.increment(1);
    Ok(())
}
