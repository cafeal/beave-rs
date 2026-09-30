//! One delivery: handler retries, output mapping, publishing, DLQ, then ACK.
use super::builder::{BoxHandler, DeadLetter, Mapper};
use crate::{handler::HandlerError, message::SourceMessage, retry::RetryPolicy, sink::Sink};
use std::{future::Future, sync::Arc};

pub(super) async fn process<M: SourceMessage, O: Send + Sync + 'static, K: Sink<O>>(
    delivery: M,
    handler: BoxHandler<M::Item, O>,
    sink: Arc<K>,
    dlq: Option<DeadLetter<M::Item>>,
    hp: RetryPolicy,
    pp: RetryPolicy,
    middleware: Vec<Mapper<M::Item, O>>,
) -> anyhow::Result<()> {
    let input = delivery
        .decode()
        .map_err(|error| error.context("decode failed"))?;
    // The first middleware that intercepts the input replaces the handler for this delivery.
    let intercepted = middleware
        .iter()
        .find_map(|middleware| middleware.intercept(&input).transpose());
    let outputs = match intercepted {
        Some(Ok(values)) => values.values(),
        Some(Err(HandlerError::Reject(error))) => {
            let error = error.context("rejected input interception");
            return dead_letter(delivery, input, dlq, pp, error).await;
        }
        Some(Err(HandlerError::Retry(error) | HandlerError::Fatal(error))) => {
            return Err(error.context("input interception failed"));
        }
        None => {
            let mut attempts = 0;
            loop {
                attempts += 1;
                match handler(input.clone()).await {
                    Ok(values) => break values.values(),
                    Err(HandlerError::Retry(error)) => {
                        if attempts >= hp.max_attempts {
                            return Err(error.context("handler retry exhausted"));
                        }
                        tokio::time::sleep(hp.delay(attempts)).await;
                    }
                    Err(HandlerError::Reject(error)) => {
                        let error = error.context("rejected input");
                        return dead_letter(delivery, input, dlq, pp, error).await;
                    }
                    Err(HandlerError::Fatal(error)) => {
                        return Err(error.context("fatal handler error"));
                    }
                }
            }
        }
    };
    // Resolve every output before publishing any; mapping errors never rerun the handler.
    let mut mapped = Vec::with_capacity(outputs.len());
    for output in outputs {
        match middleware
            .iter()
            .try_fold(output, |output, map| map.map(&input, output))
        {
            Ok(output) => mapped.push(output),
            Err(HandlerError::Reject(error)) => {
                let error = error.context("rejected metadata mapping");
                return dead_letter(delivery, input, dlq, pp, error).await;
            }
            Err(HandlerError::Retry(error) | HandlerError::Fatal(error)) => {
                return Err(error.context("metadata mapping failed"));
            }
        }
    }
    let outputs = mapped
        .into_iter()
        .map(|output| sink.prepare(output))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|error| error.context("prepare output failed"))?;
    for output in outputs {
        retry_publish(&pp, || sink.publish(&output)).await?;
    }
    delivery.ack().await
}
/// Publishes the original input to the DLQ before ACK. Without a DLQ, the
/// rejection stops processing and the delivery remains unacknowledged.
async fn dead_letter<M: SourceMessage>(
    delivery: M,
    input: M::Item,
    dlq: Option<DeadLetter<M::Item>>,
    policy: RetryPolicy,
    error: anyhow::Error,
) -> anyhow::Result<()> {
    let dlq = dlq.ok_or_else(|| error.context("no DLQ configured"))?;
    dlq(input, policy).await?;
    delivery.ack().await
}

pub(super) async fn retry_publish<F, Fut>(
    policy: &RetryPolicy,
    mut publish: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let mut attempt = 1;
    loop {
        match publish().await {
            Ok(()) => return Ok(()),
            Err(error) if attempt >= policy.max_attempts => {
                return Err(error.context("publish retry exhausted"));
            }
            Err(_) => {
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
        }
    }
}
