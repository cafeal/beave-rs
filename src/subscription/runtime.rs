//! Receive scheduling, bounded concurrency, revocation, draining, and resource cleanup.
use super::{
    builder::Subscription,
    instruments::{Instruments, report_probe},
    processing::{PendingAck, Pipeline, process},
    scheduler::{FlowChange, Scheduler, Ticket},
};
use crate::{
    error::{BoxContext, BoxError, Error, chain},
    health::{SubscriptionStatus, Tracker},
    message::SourceMessage,
    shutdown::CancellationToken,
    sink::Sink,
    source::{Receive, ReceiveError, Source},
};
use std::{future::Future, result::Result as StdResult, sync::Arc};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, sleep_until, timeout},
};
use tracing::{Instrument, Span, debug, error, field::Empty, info, info_span, warn};

/// A finished job's scheduler ticket and the acknowledgement still waiting for its outputs.
type Jobs = JoinSet<Result<(Ticket, Option<PendingAck>), BoxError>>;
/// Acknowledgements waiting for submitted outputs to complete, outside job slots and
/// `max_in_flight`; sinks bound them by applying backpressure in `submit`.
type Acks = JoinSet<Result<(), BoxError>>;

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    pub(crate) async fn run(
        self,
        shutdown: CancellationToken,
        health: Arc<Tracker>,
    ) -> Result<(), Error> {
        let span = info_span!("subscription", subscription = %self.name);
        let result = self
            .execute(shutdown, health.clone())
            .instrument(span)
            .await;
        health.set_status(match result {
            Ok(()) => SubscriptionStatus::Stopped,
            Err(_) => SubscriptionStatus::Failed,
        });
        result
    }

    async fn execute(
        mut self,
        shutdown: CancellationToken,
        health: Arc<Tracker>,
    ) -> Result<(), Error> {
        info!("subscription started");
        let probe = self.source.probe();
        health.start(probe.clone(), self.config.stall_timeout);
        let probing = probe.map(|probe| tokio::spawn(report_probe(self.name.clone(), probe)));
        let instruments = Instruments::new(
            &self.name,
            &self.config.error_policy,
            self.dlq.is_some(),
            health.clone(),
        );
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
        output.start(&self.config, &worker.pipeline.instruments);
        let concurrency = self.config.concurrency;
        let max_in_flight = self.config.max_in_flight;
        let mut scheduler = Scheduler::new(self.config.ordering, self.config.max_in_flight_per_key);
        let mut jobs = JoinSet::new();
        let mut acks: Acks = JoinSet::new();
        let mut failure = None;
        let mut failures = 0;
        let mut next_receive = Instant::now();
        let mut ended = false;
        let mut verified = false;
        let instruments = worker.pipeline.instruments.clone();
        // A source fed by an upstream subscription ends when that upstream closes it.
        let stop = if self.source.stops_on_shutdown() {
            shutdown.clone()
        } else {
            CancellationToken::new()
        };
        loop {
            instruments.set_in_flight(scheduler.outstanding() + acks.len());
            worker.start_ready(&mut jobs, &mut scheduler, concurrency);
            for change in scheduler.take_flow_changes() {
                match change {
                    FlowChange::Pause(key) => {
                        if !self.source.pause(&key) {
                            debug!("source cannot pause single ordering keys");
                            scheduler.disable_flow_control();
                            break;
                        }
                        debug!(
                            scope = key.scope(),
                            index = key.index(),
                            "paused ordering key"
                        );
                    }
                    FlowChange::Resume(key) => {
                        self.source.resume(&key);
                        debug!(
                            scope = key.scope(),
                            index = key.index(),
                            "resumed ordering key"
                        );
                    }
                }
            }
            tokio::select! {
                biased;
                _ = stop.cancelled() => break,
                result = jobs.join_next(), if !jobs.is_empty() => {
                    match flatten(result.unwrap()) {
                        Ok((ticket, pending)) => {
                            scheduler.complete(ticket);
                            if let Some(pending) = pending { acks.spawn(pending); }
                        }
                        Err(error) => { failure = Some(error); shutdown.cancel(); break; }
                    }
                }
                result = acks.join_next(), if !acks.is_empty() => {
                    if let Err(error) = flatten(result.unwrap()) { failure = Some(error); shutdown.cancel(); break; }
                }
                received = async {
                    sleep_until(next_receive).await;
                    health.receive_resumed();
                    self.source.receive().await
                },
                    if jobs.len() < concurrency && scheduler.outstanding() < max_in_flight => {
                    match received {
                        Ok(Receive::End) => { ended = true; break; }
                        Ok(Receive::Message(delivery)) => {
                            instruments.received.increment(1);
                            health.progressed();
                            failures = 0;
                            health.receive_succeeded();
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
                            health.receive_failed(failures);
                            if failures >= self.config.receive_retry.max_attempts { failure = Some(error.context("receive retry exhausted")); shutdown.cancel(); break; }
                            let delay = self.config.receive_retry.delay(failures);
                            warn!(attempt = failures, ?delay, error = chain(error.as_ref()), "retrying receive");
                            next_receive = Instant::now() + delay;
                        }
                        Err(ReceiveError::Fatal(error)) => {
                            worker.pipeline.instruments.receive_errors.increment(1); failure = Some(error.context("fatal receive error")); shutdown.cancel(); break; }
                    }
                }
            }
        }
        health.set_status(SubscriptionStatus::Stopping);
        // After End, received deliveries still run. On shutdown or failure,
        // unstarted deliveries are dropped unacknowledged.
        if !ended {
            self.source.stop_receiving();
            scheduler.discard_pending();
        }
        let drained = timeout(self.config.drain_timeout, async {
            loop {
                if stop.is_cancelled() || failure.is_some() {
                    scheduler.discard_pending();
                }
                instruments.set_in_flight(scheduler.outstanding() + acks.len());
                worker.start_ready(&mut jobs, &mut scheduler, concurrency);
                let result = tokio::select! {
                    Some(result) = jobs.join_next() => flatten(result).map(|(ticket, pending)| {
                        scheduler.complete(ticket);
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
                Error::timeout("drain timeout; unfinished deliveries were not acknowledged").into()
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
                failure.get_or_insert_with(|| Error::timeout("close timeout").into());
            }
        }
        instruments.set_in_flight(0);
        if let Some(probing) = probing {
            probing.abort();
        }
        match failure {
            Some(failure) => {
                shutdown.cancel();
                error!(error = chain(failure.as_ref()), "subscription failed");
                Err(Error::Subscription {
                    name: self.name,
                    source: failure,
                })
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
            let Some((ticket, delivery)) = scheduler.next_ready() else {
                return;
            };
            // A delivery the source is handing off is abandoned unless it has started.
            if delivery
                .handoff()
                .is_some_and(|handoff| handoff.is_cancelled())
            {
                drop(delivery);
                scheduler.complete(ticket);
                self.pipeline.instruments.revoked.increment(1);
                debug!("abandoned delivery handed off before it started");
                continue;
            }
            let revocation = delivery.revocation();
            let span = message_span(&delivery);
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
                Ok((ticket, pending))
            };
            jobs.spawn(job.instrument(span));
        }
    }
}

/// Runs `work` unless the delivery is revoked first. A revoked delivery belongs to
/// another consumer now; its outcome, including an ACK rejected because of the
/// revocation, is not a failure. Returns `None` when the work was abandoned.
async fn abandon_on_revocation<T, M: SourceMessage, O>(
    work: impl Future<Output = Result<T, BoxError>>,
    revocation: Option<CancellationToken>,
    pipeline: &Pipeline<M, O>,
) -> Result<Option<T>, BoxError> {
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
    pipeline.instruments.health.progressed();
    debug!("abandoned revoked delivery");
    Ok(None)
}

/// The span of one delivery, with the messaging attributes its source reports.
fn message_span(delivery: &impl SourceMessage) -> Span {
    let span = info_span!(
        "message",
        "otel.kind" = "consumer",
        "otel.name" = Empty,
        "messaging.system" = Empty,
        "messaging.operation.type" = Empty,
        "messaging.operation.name" = Empty,
        "messaging.destination.name" = Empty,
        "messaging.destination.partition.id" = Empty,
        "messaging.destination.subscription.name" = Empty,
        "messaging.consumer.group.name" = Empty,
        "messaging.message.id" = Empty,
        "messaging.kafka.offset" = Empty,
    );
    if span.is_disabled() {
        return span;
    }
    let Some(attributes) = delivery.messaging_attributes() else {
        return span;
    };
    let name = match attributes.destination {
        Some(destination) => format!("process {destination}"),
        None => "process".to_owned(),
    };
    span.record("otel.name", name);
    span.record("messaging.system", attributes.system);
    span.record("messaging.operation.type", "process");
    span.record("messaging.operation.name", "process");
    if let Some(destination) = attributes.destination {
        span.record("messaging.destination.name", destination);
    }
    if let Some(partition) = attributes.partition {
        span.record("messaging.destination.partition.id", partition.to_string());
    }
    if let Some(subscription) = attributes.subscription {
        span.record("messaging.destination.subscription.name", subscription);
    }
    if let Some(group) = attributes.consumer_group {
        span.record("messaging.consumer.group.name", group);
    }
    if let Some(id) = &attributes.message_id {
        span.record("messaging.message.id", id.as_ref());
    }
    if let Some(offset) = attributes.kafka_offset {
        span.record("messaging.kafka.offset", offset);
    }
    span
}

fn flatten<T>(result: StdResult<Result<T, BoxError>, JoinError>) -> Result<T, BoxError> {
    result?
}
