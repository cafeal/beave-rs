//! Receive scheduling, bounded concurrency, revocation, draining, and resource cleanup.
use super::{
    builder::Subscription,
    instruments::Instruments,
    processing::{PendingAck, Pipeline, process},
    scheduler::Scheduler,
};
use crate::{
    message::{OrderingKey, SourceMessage},
    shutdown::CancellationToken,
    sink::Sink,
    source::{Receive, ReceiveError, Source},
};
use std::{future::Future, result::Result as StdResult, sync::Arc};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, sleep_until, timeout},
};
use tracing::{Instrument, debug, error, info, info_span, warn};

/// A finished job's ordering key and the acknowledgement still waiting for its outputs.
type Jobs = JoinSet<anyhow::Result<(Option<OrderingKey>, Option<PendingAck>)>>;
/// Acknowledgements waiting for submitted outputs to complete, outside job slots and
/// `max_in_flight`; sinks bound them by applying backpressure in `submit`.
type Acks = JoinSet<anyhow::Result<()>>;

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    pub(crate) async fn run(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let span = info_span!("subscription", subscription = %self.name);
        self.execute(shutdown).instrument(span).await
    }

    async fn execute(mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        info!("subscription started");
        let instruments =
            Instruments::new(&self.name, &self.config.error_policy, self.dlq.is_some());
        let output = self.output;
        let worker = Worker {
            pipeline: Arc::new(Pipeline {
                name: self.name.clone(),
                handler: self.handler,
                middleware: self.middleware,
                output: output.clone(),
                dlq: self.dlq,
                handler_retry: self.config.handler_retry.clone(),
                publish_retry: self.config.publish_retry.clone(),
                dead_letter_retry: self.config.dead_letter_retry.clone(),
                error_policy: self.config.error_policy.clone(),
                instruments,
            }),
        };
        let concurrency = self.config.concurrency;
        let max_in_flight = self.config.max_in_flight;
        let mut scheduler = Scheduler::new(self.config.ordering);
        let mut jobs = JoinSet::new();
        let mut acks: Acks = JoinSet::new();
        let mut failure = None;
        let mut failures = 0;
        let mut next_receive = Instant::now();
        let mut ended = false;
        let mut verified = false;
        let in_flight = worker.pipeline.instruments.in_flight.clone();
        // A source fed by an upstream subscription ends when that upstream closes it.
        let stop = if self.source.stops_on_shutdown() {
            shutdown.clone()
        } else {
            CancellationToken::new()
        };
        loop {
            in_flight.set((scheduler.outstanding() + acks.len()) as f64);
            worker.start_ready(&mut jobs, &mut scheduler, concurrency);
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                result = jobs.join_next(), if !jobs.is_empty() => {
                    match flatten(result.unwrap()) {
                        Ok((key, pending)) => {
                            scheduler.complete(key);
                            if let Some(pending) = pending { acks.spawn(pending); }
                        }
                        Err(error) => { failure = Some(error); shutdown.cancel(); break; }
                    }
                }
                result = acks.join_next(), if !acks.is_empty() => {
                    if let Err(error) = flatten(result.unwrap()) { failure = Some(error); shutdown.cancel(); break; }
                }
                received = async { sleep_until(next_receive).await; self.source.receive().await },
                    if jobs.len() < concurrency && scheduler.outstanding() < max_in_flight => {
                    match received {
                        Ok(Receive::End) => { ended = true; break; }
                        Ok(Receive::Message(delivery)) => {
                            worker.pipeline.instruments.received.increment(1);
                            failures = 0;
                            next_receive = Instant::now();
                            let delivery = if verified {
                                delivery
                            } else {
                                // The first delivery waits for the check; shutdown abandons it unacknowledged.
                                let verify = output.verify(delivery, &self.config.publish_retry);
                                match tokio::select! { biased; _ = stop.cancelled() => None, result = verify => Some(result) } {
                                    None => break,
                                    Some(Ok(delivery)) => { verified = true; delivery }
                                    Some(Err(error)) => { failure = Some(error); shutdown.cancel(); break; }
                                }
                            };
                            scheduler.push(delivery);
                        }
                        Err(ReceiveError::Retry(error)) => {
                            worker.pipeline.instruments.receive_errors.increment(1);
                            failures += 1;
                            if failures >= self.config.receive_retry.max_attempts { failure = Some(error.context("receive retry exhausted")); shutdown.cancel(); break; }
                            let delay = self.config.receive_retry.delay(failures);
                            warn!(attempt = failures, ?delay, error = format!("{error:#}"), "retrying receive");
                            next_receive = Instant::now() + delay;
                        }
                        Err(ReceiveError::Fatal(error)) => {
                            worker.pipeline.instruments.receive_errors.increment(1); failure = Some(error.context("fatal receive error")); shutdown.cancel(); break; }
                    }
                }
            }
        }
        // After End, received deliveries still run. On shutdown or failure,
        // unstarted deliveries are dropped unacknowledged.
        if !ended {
            scheduler.discard_pending();
        }
        let drained = timeout(self.config.drain_timeout, async {
            loop {
                if stop.is_cancelled() || failure.is_some() {
                    scheduler.discard_pending();
                }
                in_flight.set((scheduler.outstanding() + acks.len()) as f64);
                worker.start_ready(&mut jobs, &mut scheduler, concurrency);
                let result = tokio::select! {
                    Some(result) = jobs.join_next() => flatten(result).map(|(key, pending)| {
                        scheduler.complete(key);
                        if let Some(pending) = pending {
                            acks.spawn(pending);
                        }
                    }),
                    Some(result) = acks.join_next() => flatten(result),
                    else => break,
                };
                if let Err(error) = result {
                    failure.get_or_insert(error);
                    shutdown.cancel();
                }
            }
        })
        .await;
        if drained.is_err() {
            jobs.abort_all();
            acks.abort_all();
            while jobs.join_next().await.is_some() {}
            while acks.join_next().await.is_some() {}
            failure.get_or_insert_with(|| {
                anyhow::anyhow!("drain timeout; unfinished deliveries were not acknowledged")
            });
            shutdown.cancel();
        }
        // Cleanup also has a deadline, even when the transport is broken.
        let cleanup = timeout(self.config.drain_timeout, async {
            let (source_result, sink_result, dlq_result) =
                tokio::join!(self.source.close(), output.close(), async {
                    match &self.close_dlq {
                        Some(close) => close().await,
                        None => Ok(()),
                    }
                });
            source_result.and(sink_result).and(dlq_result)
        })
        .await;
        match cleanup {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                failure.get_or_insert(e);
            }
            Err(_) => {
                failure.get_or_insert_with(|| anyhow::anyhow!("close timeout"));
            }
        }
        in_flight.set(0.0);
        match failure {
            Some(failure) => {
                shutdown.cancel();
                error!(error = format!("{failure:#}"), "subscription failed");
                Err(failure.context(self.name))
            }
            None => {
                info!("subscription stopped");
                Ok(())
            }
        }
    }
}

/// Everything a job needs to process one delivery independently of the scheduler.
struct Worker<M: SourceMessage, O> {
    pipeline: Arc<Pipeline<M, O>>,
}

impl<M: SourceMessage, O: Send + Sync + 'static> Worker<M, O> {
    fn start_ready(&self, jobs: &mut Jobs, scheduler: &mut Scheduler<M>, concurrency: usize) {
        while jobs.len() < concurrency {
            let Some((key, delivery)) = scheduler.next_ready() else {
                return;
            };
            let revocation = delivery.revocation();
            let span = info_span!("message", "otel.kind" = "consumer");
            #[cfg(feature = "opentelemetry")]
            crate::telemetry::set_remote_parent(&span, &delivery.propagation_fields());
            let pipeline = self.pipeline.clone();
            let processing = process(delivery, pipeline.clone());
            let span_for_ack = span.clone();
            let job = async move {
                let pending = abandon_on_revocation(processing, revocation.clone(), &pipeline)
                    .await?
                    .flatten();
                // The acknowledgement after completion can still be revoked.
                let pending = pending.map(|pending| -> PendingAck {
                    Box::pin(
                        async move {
                            abandon_on_revocation(pending, revocation, &pipeline)
                                .await
                                .map(|_| ())
                        }
                        .instrument(span_for_ack),
                    )
                });
                Ok((key, pending))
            };
            jobs.spawn(job.instrument(span));
        }
    }
}

/// Runs `work` unless the delivery is revoked first. A revoked delivery belongs to
/// another consumer now; its outcome, including an ACK rejected because of the
/// revocation, is not a failure. Returns `None` when the work was abandoned.
async fn abandon_on_revocation<T, M: SourceMessage, O>(
    work: impl Future<Output = anyhow::Result<T>>,
    revocation: Option<CancellationToken>,
    pipeline: &Pipeline<M, O>,
) -> anyhow::Result<Option<T>> {
    let Some(revoked) = revocation else {
        return work.await.map(Some);
    };
    tokio::select! {
        biased;
        _ = revoked.cancelled() => {}
        result = work => match result {
            Err(_) if revoked.is_cancelled() => {}
            result => return result.map(Some),
        },
    }
    pipeline.instruments.revoked.increment(1);
    debug!("abandoned revoked delivery");
    Ok(None)
}

fn flatten<T>(result: StdResult<anyhow::Result<T>, JoinError>) -> anyhow::Result<T> {
    result?
}
