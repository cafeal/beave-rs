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
    let mut attempts = 0;
    let outputs = loop {
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
                let dlq = dlq
                    .as_ref()
                    .ok_or_else(|| error.context("rejected input without DLQ"))?;
                dlq(input.clone(), pp.clone()).await?;
                return delivery.ack().await;
            }
            Err(HandlerError::Fatal(error)) => return Err(error.context("fatal handler error")),
        }
    };
    // Resolve every output before publishing any; mapping errors never rerun the handler.
    let outputs = outputs
        .into_iter()
        .map(|output| {
            middleware.iter().try_fold(output, |output, map| {
                map(&input, output).map_err(|error| match error {
                    HandlerError::Retry(e) | HandlerError::Reject(e) | HandlerError::Fatal(e) => {
                        e.context("metadata mapping failed")
                    }
                })
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let outputs = outputs
        .into_iter()
        .map(|output| sink.prepare(output))
        .collect::<anyhow::Result<Vec<_>>>()
        .map_err(|error| error.context("prepare output failed"))?;
    for output in outputs {
        retry_publish(&pp, || sink.publish(&output)).await?;
    }
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
