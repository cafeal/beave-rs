//! Receive scheduling, bounded concurrency, revocation, draining, and resource cleanup.
use super::{
    builder::{BoxHandler, DeadLetter, Mapper, Subscription},
    processing::process,
    scheduler::Scheduler,
};
use crate::{
    message::{OrderingKey, SourceMessage},
    retry::RetryPolicy,
    shutdown::CancellationToken,
    sink::Sink,
    source::{Receive, ReceiveError, Source},
};
use std::{result::Result as StdResult, sync::Arc};
use tokio::{
    task::{JoinError, JoinSet},
    time::{Instant, sleep_until, timeout},
};

type Jobs = JoinSet<anyhow::Result<Option<OrderingKey>>>;

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let sink = Arc::new(self.sink);
        let worker = Worker {
            handler: self.handler.clone(),
            sink: sink.clone(),
            dlq: self.dlq.clone(),
            middleware: self.middleware.clone(),
            handler_retry: self.config.handler_retry.clone(),
            publish_retry: self.config.publish_retry.clone(),
        };
        let concurrency = self.config.concurrency;
        let max_in_flight = self.config.max_in_flight;
        let mut scheduler = Scheduler::new(self.config.ordering);
        let mut jobs = JoinSet::new();
        let mut failure = None;
        let mut failures = 0;
        let mut next_receive = Instant::now();
        let mut ended = false;
        loop {
            worker.start_ready(&mut jobs, &mut scheduler, concurrency);
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                result = jobs.join_next(), if !jobs.is_empty() => {
                    match flatten(result.unwrap()) {
                        Ok(key) => scheduler.complete(key),
                        Err(error) => { failure = Some(error); shutdown.cancel(); break; }
                    }
                }
                received = async { sleep_until(next_receive).await; self.source.receive().await },
                    if jobs.len() < concurrency && scheduler.outstanding() < max_in_flight => {
                    match received {
                        Ok(Receive::End) => { ended = true; break; }
                        Ok(Receive::Message(delivery)) => {
                            failures = 0;
                            next_receive = Instant::now();
                            scheduler.push(delivery);
                        }
                        Err(ReceiveError::Retry(error)) => {
                            failures += 1;
                            if failures >= self.config.receive_retry.max_attempts { failure = Some(error.context("receive retry exhausted")); shutdown.cancel(); break; }
                            next_receive = Instant::now() + self.config.receive_retry.delay(failures);
                        }
                        Err(ReceiveError::Fatal(error)) => { failure = Some(error.context("fatal receive error")); shutdown.cancel(); break; }
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
                if shutdown.is_cancelled() {
                    scheduler.discard_pending();
                }
                worker.start_ready(&mut jobs, &mut scheduler, concurrency);
                let Some(result) = jobs.join_next().await else {
                    break;
                };
                match flatten(result) {
                    Ok(key) => scheduler.complete(key),
                    Err(error) => {
                        failure.get_or_insert(error);
                        shutdown.cancel();
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
            failure.get_or_insert_with(|| {
                anyhow::anyhow!("drain timeout; unfinished deliveries were not acknowledged")
            });
            shutdown.cancel();
        }
        // Cleanup also has a deadline, even when the transport is broken.
        let cleanup = timeout(self.config.drain_timeout, async {
            let (source_result, sink_result, dlq_result) =
                tokio::join!(self.source.close(), sink.close(), async {
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
        match failure {
            Some(error) => {
                shutdown.cancel();
                Err(error.context(self.config.name))
            }
            None => Ok(()),
        }
    }
}

/// Everything a job needs to process one delivery independently of the scheduler.
struct Worker<I, O, K> {
    handler: BoxHandler<I, O>,
    sink: Arc<K>,
    dlq: Option<DeadLetter<I>>,
    middleware: Vec<Mapper<I, O>>,
    handler_retry: RetryPolicy,
    publish_retry: RetryPolicy,
}

impl<I: Clone + Send + Sync + 'static, O: Send + Sync + 'static, K: Sink<O>> Worker<I, O, K> {
    fn start_ready<M: SourceMessage<Item = I>>(
        &self,
        jobs: &mut Jobs,
        scheduler: &mut Scheduler<M>,
        concurrency: usize,
    ) {
        while jobs.len() < concurrency {
            let Some((key, delivery)) = scheduler.next_ready() else {
                return;
            };
            let revocation = delivery.revocation();
            let processing = process(
                delivery,
                self.handler.clone(),
                self.sink.clone(),
                self.dlq.clone(),
                self.handler_retry.clone(),
                self.publish_retry.clone(),
                self.middleware.clone(),
            );
            jobs.spawn(async move {
                let Some(revoked) = revocation else {
                    return processing.await.map(|()| key);
                };
                // A revoked delivery belongs to another consumer now. Its outcome,
                // including an ACK rejected because of the revocation, is not a failure.
                tokio::select! {
                    biased;
                    _ = revoked.cancelled() => Ok(key),
                    result = processing => match result {
                        Err(_) if revoked.is_cancelled() => Ok(key),
                        result => result.map(|()| key),
                    },
                }
            });
        }
    }
}

fn flatten<T>(result: StdResult<anyhow::Result<T>, JoinError>) -> anyhow::Result<T> {
    result?
}
