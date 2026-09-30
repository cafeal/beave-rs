//! One delivery: decode, middleware, handler retries, publishing, failure routing, then ACK.
use super::builder::{BoxHandler, DeadLetterRoute, Mapper};
use crate::{
    dead_letter::DeadLetter,
    error_policy::{ErrorPolicy, FailureAction, FailureKind},
    handler::HandlerError,
    message::SourceMessage,
    middleware::Flow,
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
    I: Clone + 'static,
    O: Send + Sync + 'static,
    K: Sink<O>,
{
    // Pre-handler hooks transform the handler input or intercept the delivery;
    // `input` stays as decoded for post-handler hooks and dead letters.
    let mut flow = Flow::Continue(input.clone());
    for middleware in &pipeline.middleware {
        let Flow::Continue(value) = flow else { break };
        flow = match middleware.pre_handler(value) {
            Ok(next) => next,
            Err(HandlerError::Reject(error)) => {
                return Ok(Some(Failure {
                    kind: FailureKind::Rejected,
                    error: error.context("rejected before the handler"),
                    attempts: 0,
                    input: Some(input),
                }));
            }
            Err(HandlerError::Retry(error) | HandlerError::Fatal(error)) => {
                return Err(error.context("pre-handler middleware failed"));
            }
        };
    }
    let handler_input = match flow {
        Flow::Continue(value) => value,
        Flow::Intercept(values) => return finish(values.values(), input, 0, pipeline).await,
    };
    let policy = &pipeline.handler_retry;
    let mut attempts = 0;
    let outputs = loop {
        attempts += 1;
        let (kind, error) = match (pipeline.handler)(handler_input.clone()).await {
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
    finish(outputs, input, attempts, pipeline).await
}

/// Runs post-handler hooks, then prepares and publishes every output.
async fn finish<I, R, O, K>(
    outputs: Vec<O>,
    input: I,
    attempts: usize,
    pipeline: &Pipeline<I, R, O, K>,
) -> anyhow::Result<Option<Failure<I>>>
where
    I: 'static,
    O: Send + Sync + 'static,
    K: Sink<O>,
{
    // Resolve every output before publishing any; middleware errors never rerun the handler.
    let mut mapped = Vec::with_capacity(outputs.len());
    for output in outputs {
        match pipeline
            .middleware
            .iter()
            .try_fold(output, |output, middleware| {
                middleware.post_handler(&input, output)
            }) {
            Ok(output) => mapped.push(output),
            Err(HandlerError::Reject(error)) => {
                return Ok(Some(Failure {
                    kind: FailureKind::Rejected,
                    error: error.context("rejected after the handler"),
                    attempts,
                    input: Some(input),
                }));
            }
            Err(HandlerError::Retry(error) | HandlerError::Fatal(error)) => {
                return Err(error.context("post-handler middleware failed"));
            }
        }
    }
    let outputs = mapped;
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
