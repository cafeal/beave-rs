//! One delivery: decode, handler retries, output mapping, publishing, failure routing, then ACK.
use super::builder::{BoxHandler, DeadLetterRoute, Mapper};
use crate::{
    dead_letter::DeadLetter,
    error_policy::{ErrorPolicy, FailureAction, FailureKind},
    handler::HandlerError,
    message::SourceMessage,
    retry::RetryPolicy,
    sink::Sink,
};
use std::{future::Future, sync::Arc};

/// Everything a job needs, shared by all jobs of one subscription run.
pub(super) struct Pipeline<I, R, O, K> {
    pub(super) name: String,
    pub(super) handler: BoxHandler<I, O>,
    pub(super) middleware: Vec<Mapper<I, O>>,
    pub(super) sink: Arc<K>,
    pub(super) dlq: Option<DeadLetterRoute<I, R>>,
    pub(super) handler_retry: RetryPolicy,
    pub(super) publish_retry: RetryPolicy,
    pub(super) dead_letter_retry: RetryPolicy,
    pub(super) error_policy: ErrorPolicy,
}

/// A routable failure; `input` is absent only when decoding failed.
struct Failure<I> {
    kind: FailureKind,
    error: anyhow::Error,
    attempts: usize,
    input: Option<I>,
}

pub(super) async fn process<M, O, K>(
    delivery: M,
    pipeline: Arc<Pipeline<M::Item, M::Raw, O, K>>,
) -> anyhow::Result<()>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
    K: Sink<O>,
{
    let outcome = match delivery.decode() {
        Ok(input) => handle(input, &pipeline).await?,
        Err(error) => Some(Failure {
            kind: FailureKind::Decode,
            error,
            attempts: 0,
            input: None,
        }),
    };
    match outcome {
        None => delivery.ack().await,
        Some(failure) => route(delivery, &pipeline, failure).await,
    }
}

/// Returns `Ok(None)` when every output was published and the delivery may be acknowledged.
async fn handle<I, R, O, K>(
    input: I,
    pipeline: &Pipeline<I, R, O, K>,
) -> anyhow::Result<Option<Failure<I>>>
where
    I: Clone,
    O: Send + Sync + 'static,
    K: Sink<O>,
{
    let policy = &pipeline.handler_retry;
    let mut attempts = 0;
    let outputs = loop {
        attempts += 1;
        let (kind, error) = match (pipeline.handler)(input.clone()).await {
            Ok(values) => break values.values(),
            Err(HandlerError::Retry(error)) if attempts >= policy.max_attempts => {
                (FailureKind::RetryExhausted, error)
            }
            Err(HandlerError::Retry(_)) => {
                tokio::time::sleep(policy.delay(attempts)).await;
                continue;
            }
            Err(HandlerError::Reject(error)) => (FailureKind::Rejected, error),
            Err(HandlerError::Fatal(error)) => return Err(error.context("fatal handler error")),
        };
        return Ok(Some(Failure {
            kind,
            error,
            attempts,
            input: Some(input),
        }));
    };
    // Resolve every output before publishing any; mapping errors never rerun the handler.
    let outputs = outputs
        .into_iter()
        .map(|output| {
            pipeline.middleware.iter().try_fold(output, |output, map| {
                map(&input, output).map_err(|error| match error {
                    HandlerError::Retry(e) | HandlerError::Reject(e) | HandlerError::Fatal(e) => {
                        e.context("metadata mapping failed")
                    }
                })
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    // Preparing all outputs first means an encode failure never follows a partial publish.
    let outputs = match outputs
        .into_iter()
        .map(|output| pipeline.sink.prepare(output))
        .collect::<anyhow::Result<Vec<_>>>()
    {
        Ok(outputs) => outputs,
        Err(error) => {
            return Ok(Some(Failure {
                kind: FailureKind::Encode,
                error,
                attempts,
                input: Some(input),
            }));
        }
    };
    for output in outputs {
        retry_publish(&pipeline.publish_retry, || pipeline.sink.publish(&output)).await?;
    }
    Ok(None)
}

async fn route<M, O, K>(
    delivery: M,
    pipeline: &Pipeline<M::Item, M::Raw, O, K>,
    failure: Failure<M::Item>,
) -> anyhow::Result<()>
where
    M: SourceMessage,
{
    let Failure {
        kind,
        error,
        attempts,
        input,
    } = failure;
    match pipeline.error_policy.action(kind) {
        FailureAction::Stop => Err(error.context(kind)),
        FailureAction::Discard => delivery.ack().await,
        FailureAction::DeadLetter => {
            let Some(dlq) = &pipeline.dlq else {
                return Err(error
                    .context(kind)
                    .context("no dead-letter sink configured"));
            };
            let dead_letter = DeadLetter::new(
                pipeline.name.clone(),
                kind,
                &error,
                attempts,
                input,
                delivery.raw(),
            );
            dlq(dead_letter, pipeline.dead_letter_retry.clone())
                .await
                .map_err(|dlq_error| dlq_error.context(format!("{kind}: {error:#}")))?;
            delivery.ack().await
        }
    }
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
