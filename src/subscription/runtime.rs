//! Receive scheduling, bounded concurrency, draining, and resource cleanup.
use super::{Subscription, processing::process};
use crate::{
    shutdown::CancellationToken,
    sink::Sink,
    source::{Receive, ReceiveError, Source},
};
use std::sync::Arc;
use tokio::{
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> anyhow::Result<()> {
        let sink = Arc::new(self.sink);
        let mut jobs = JoinSet::new();
        let mut failure = None;
        let mut failures = 0;
        let mut next_receive = Instant::now();
        let limit = self.config.concurrency.min(self.config.max_in_flight);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                result = jobs.join_next(), if !jobs.is_empty() => {
                    if let Err(error) = flatten(result.unwrap()) { failure = Some(error); shutdown.cancel(); break; }
                }
                received = async { sleep_until(next_receive).await; self.source.receive().await }, if jobs.len() < limit => {
                    match received {
                        Ok(Receive::End) => break,
                        Ok(Receive::Message(delivery)) => {
                            failures = 0;
                            next_receive = Instant::now();
                            let handler = self.handler.clone(); let sink = sink.clone(); let dlq = self.dlq.clone();
                            let middleware = self.middleware.clone();
                            let hp = self.config.handler_retry.clone(); let pp = self.config.publish_retry.clone();
                            jobs.spawn(async move { process(delivery, handler, sink, dlq, hp, pp, middleware).await });
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
        let drained = timeout(self.config.drain_timeout, async {
            while let Some(result) = jobs.join_next().await {
                if let Err(error) = flatten(result) {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                    shutdown.cancel();
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
fn flatten(
    result: std::result::Result<anyhow::Result<()>, tokio::task::JoinError>,
) -> anyhow::Result<()> {
    result?
}
