//! One delivery: decode, middleware, handler retries, publishing, failure routing, then ACK.
use super::{
    builder::{BoxHandler, DeadLetterRoute, Mapper},
    completion::{BoxFuture, Complete, Completion, acknowledge as ack_delivery},
    instruments::{Instruments, Stage},
};
use crate::{
    dead_letter::DeadLetter,
    error_policy::{ErrorPolicy, FailureAction, FailureKind},
    handler::HandlerError,
    health::Tracker,
    message::SourceMessage,
    middleware::Flow,
    retry::RetryPolicy,
};
use metrics::Counter;
use std::{future::Future, sync::Arc, time::Instant};
use tracing::{Instrument, debug, info_span, warn};

/// Everything a job needs, shared by all jobs of one subscription run.
pub(super) struct Pipeline<M: SourceMessage, O> {
    pub(super) name: String,
    pub(super) handler: BoxHandler<M::Item, O>,
    pub(super) middleware: Vec<Mapper<M::Item, O>>,
    pub(super) output: Arc<dyn Complete<M, O>>,
    pub(super) dlq: Option<DeadLetterRoute<M::Item, M::Raw>>,
    pub(super) handler_retry: RetryPolicy,
    pub(super) publish_retry: RetryPolicy,
    pub(super) dead_letter_retry: RetryPolicy,
    pub(super) error_policy: ErrorPolicy,
    pub(super) instruments: Instruments,
}

/// Waits for submitted outputs to complete, then acknowledges the delivery.
pub(super) type PendingAck = BoxFuture<'static, anyhow::Result<()>>;

/// A routable failure; `input` is absent only when decoding failed.
struct Failure<I> {
    kind: FailureKind,
    error: anyhow::Error,
    attempts: usize,
    input: Option<I>,
}

/// Outputs to complete, or a failure to route; errors stop the subscription.
type Handled<I, O> = anyhow::Result<Result<Outputs<I, O>, Failure<I>>>;

/// Outputs ready for the completion stage, with what failure routing needs.
struct Outputs<I, O> {
    values: Vec<O>,
    input: I,
    attempts: usize,
}

/// Processes one delivery. Returns the acknowledgement still waiting for submitted
/// outputs to complete, if any; otherwise the delivery was already settled.
pub(super) async fn process<M, O>(
    delivery: M,
    pipeline: Arc<Pipeline<M, O>>,
) -> anyhow::Result<Option<PendingAck>>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    let started = Instant::now();
    let decoded = info_span!("decode").in_scope(|| delivery.decode());
    pipeline.instruments.record(Stage::Decode, started);
    let failure = match decoded {
        Ok(input) => match handle(input, &pipeline).await? {
            Ok(Outputs {
                values,
                input,
                attempts,
            }) => match pipeline
                .output
                .complete(
                    delivery,
                    values,
                    &pipeline.publish_retry,
                    &pipeline.instruments,
                )
                .await?
            {
                Completion::Done => return Ok(None),
                Completion::Pending(delivery, completions) => {
                    return Ok(Some(await_completions(delivery, completions, pipeline)));
                }
                Completion::Encode(delivery, error) => {
                    let failure = Failure {
                        kind: FailureKind::Encode,
                        error,
                        attempts,
                        input: Some(input),
                    };
                    return route(delivery, &pipeline, failure).await.map(|()| None);
                }
            },
            Err(failure) => failure,
        },
        Err(error) => Failure {
            kind: FailureKind::Decode,
            error,
            attempts: 0,
            input: None,
        },
    };
    route(delivery, &pipeline, failure).await.map(|()| None)
}

/// The job ends before this runs and frees its slot; the acknowledgement follows
/// completion of every submitted output.
fn await_completions<M, O>(
    delivery: M,
    completions: Vec<crate::sink::Completion>,
    pipeline: Arc<Pipeline<M, O>>,
) -> PendingAck
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    Box::pin(async move {
        let started = Instant::now();
        async {
            for completion in completions {
                completion.wait().await?;
            }
            anyhow::Ok(())
        }
        .instrument(info_span!("complete"))
        .await
        .map_err(|error| error.context("output completion failed"))?;
        pipeline.instruments.record(Stage::Complete, started);
        ack_delivery(delivery, &pipeline.instruments).await
    })
}

/// Acknowledges a delivery without output through the completion stage, so a
/// transactional subscription commits it in a transaction as well.
async fn acknowledge<M, O>(delivery: M, pipeline: &Pipeline<M, O>) -> anyhow::Result<()>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    match pipeline
        .output
        .complete(
            delivery,
            Vec::new(),
            &pipeline.publish_retry,
            &pipeline.instruments,
        )
        .await?
    {
        Completion::Done => Ok(()),
        // Without outputs nothing is submitted, so nothing can be pending.
        Completion::Pending(delivery, _) => ack_delivery(delivery, &pipeline.instruments).await,
        Completion::Encode(_, error) => Err(error),
    }
}

async fn handle<M, O>(input: M::Item, pipeline: &Pipeline<M, O>) -> Handled<M::Item, O>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    // Pre-handler hooks transform the handler input or intercept the delivery;
    // `input` stays as decoded for post-handler hooks and dead letters.
    let mut flow = Flow::Continue(input.clone());
    for middleware in &pipeline.middleware {
        let Flow::Continue(value) = flow else { break };
        flow = match middleware.pre_handler(value).await {
            Ok(next) => next,
            Err(HandlerError::Reject(error)) => {
                return Ok(Err(Failure {
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
        let started = Instant::now();
        let result = (pipeline.handler)(handler_input.clone())
            .instrument(info_span!("handler", attempt = attempts))
            .await;
        pipeline.instruments.record(Stage::Handler, started);
        let (kind, error) = match result {
            Ok(values) => break values.values(),
            Err(HandlerError::Retry(error)) if attempts >= policy.max_attempts => {
                (FailureKind::RetryExhausted, error)
            }
            Err(HandlerError::Retry(error)) => {
                pipeline.instruments.handler_retries.increment(1);
                debug!(
                    attempt = attempts,
                    error = format!("{error:#}"),
                    "retrying handler"
                );
                tokio::time::sleep(policy.delay(attempts)).await;
                continue;
            }
            Err(HandlerError::Reject(error)) => (FailureKind::Rejected, error),
            Err(HandlerError::Fatal(error)) => return Err(error.context("fatal handler error")),
        };
        return Ok(Err(Failure {
            kind,
            error,
            attempts,
            input: Some(input),
        }));
    };
    finish(outputs, input, attempts, pipeline).await
}

/// Runs post-handler hooks over every output.
async fn finish<M, O>(
    outputs: Vec<O>,
    input: M::Item,
    attempts: usize,
    pipeline: &Pipeline<M, O>,
) -> Handled<M::Item, O>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    // Resolve every output before publishing any; middleware errors never rerun the handler.
    let mut mapped = Vec::with_capacity(outputs.len());
    for output in outputs {
        match post_handlers(output, &input, pipeline).await {
            Ok(output) => mapped.push(output),
            Err(HandlerError::Reject(error)) => {
                return Ok(Err(Failure {
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
    Ok(Ok(Outputs {
        values: mapped,
        input,
        attempts,
    }))
}

/// Passes one output through every middleware's post-handler hook in registration order.
async fn post_handlers<M, O>(
    mut output: O,
    input: &M::Item,
    pipeline: &Pipeline<M, O>,
) -> crate::handler::Result<O>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    for middleware in &pipeline.middleware {
        output = middleware.post_handler(input, output).await?;
    }
    Ok(output)
}

async fn route<M, O>(
    delivery: M,
    pipeline: &Pipeline<M, O>,
    failure: Failure<M::Item>,
) -> anyhow::Result<()>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
{
    let Failure {
        kind,
        error,
        attempts,
        input,
    } = failure;
    let instruments = &pipeline.instruments;
    instruments.failure(kind).increment(1);
    match pipeline.error_policy.action(kind) {
        FailureAction::Stop => Err(error.context(kind)),
        FailureAction::Discard => {
            warn!(
                failure = %kind,
                attempts,
                error = format!("{error:#}"),
                "discarding delivery"
            );
            acknowledge(delivery, pipeline).await
        }
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
            let started = Instant::now();
            dlq(
                dead_letter,
                pipeline.dead_letter_retry.clone(),
                instruments.dead_letter_publish_failures.clone(),
            )
            .instrument(info_span!("dead_letter"))
            .await
            .map_err(|dlq_error| dlq_error.context(format!("{kind}: {error:#}")))?;
            instruments.record(Stage::DeadLetter, started);
            warn!(
                failure = %kind,
                attempts,
                error = format!("{error:#}"),
                "dead-lettered delivery"
            );
            acknowledge(delivery, pipeline).await
        }
    }
}

/// Publishes with retries. With `health`, the delivery counts as retrying its
/// publication from the first failure until it succeeds or gives up.
pub(super) async fn retry_publish<T, F, Fut>(
    policy: &RetryPolicy,
    failures: &Counter,
    health: Option<&Tracker>,
    mut publish: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut retrying = None;
    let mut attempt = 1;
    loop {
        let result = publish().await;
        if result.is_err() {
            failures.increment(1);
        }
        match result {
            Ok(value) => return Ok(value),
            Err(error) if attempt >= policy.max_attempts => {
                return Err(error.context("publish retry exhausted"));
            }
            Err(error) => {
                debug!(attempt, error = format!("{error:#}"), "retrying publish");
                if retrying.is_none() {
                    retrying = health.map(Tracker::publish_retry);
                }
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
        }
    }
}
