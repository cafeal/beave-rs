//! One delivery: decode, middleware, handler retries, publishing, failure routing, then ACK.
use super::{
    builder::{BoxFuture, BoxHandler, DeadLetterRoute, Mapper},
    instruments::{Instruments, Stage},
};
use crate::{
    dead_letter::DeadLetter,
    error_policy::{ErrorPolicy, FailureAction, FailureKind},
    handler::HandlerError,
    message::SourceMessage,
    middleware::Flow,
    retry::RetryPolicy,
    sink::{Completion, Sink},
};
use metrics::Counter;
use std::{future::Future, sync::Arc, time::Instant};
use tracing::{Instrument, debug, info_span, warn};

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
    pub(super) instruments: Instruments,
}

/// Waits for submitted outputs to complete, then acknowledges the delivery.
pub(super) type PendingAck = BoxFuture<anyhow::Result<()>>;

/// Result of running the handler for one input.
enum Handled<I> {
    /// Every output was submitted; the delivery may be acknowledged once they complete.
    Submitted(Vec<Completion>),
    Failed(Failure<I>),
}

/// A routable failure; `input` is absent only when decoding failed.
struct Failure<I> {
    kind: FailureKind,
    error: anyhow::Error,
    attempts: usize,
    input: Option<I>,
}

/// Processes one delivery. Returns the acknowledgement still waiting for submitted
/// outputs to complete, if any; otherwise the delivery was already settled.
pub(super) async fn process<M, O, K>(
    delivery: M,
    pipeline: Arc<Pipeline<M::Item, M::Raw, O, K>>,
) -> anyhow::Result<Option<PendingAck>>
where
    M: SourceMessage,
    O: Send + Sync + 'static,
    K: Sink<O>,
{
    let started = Instant::now();
    let decoded = info_span!("decode").in_scope(|| delivery.decode());
    pipeline.instruments.record(Stage::Decode, started);
    let outcome = match decoded {
        Ok(input) => handle(input, &pipeline).await?,
        Err(error) => Handled::Failed(Failure {
            kind: FailureKind::Decode,
            error,
            attempts: 0,
            input: None,
        }),
    };
    let completions = match outcome {
        Handled::Submitted(completions) => completions,
        Handled::Failed(failure) => {
            return route(delivery, &pipeline, failure).await.map(|()| None);
        }
    };
    if completions.iter().all(Completion::is_done) {
        return acknowledge(delivery, &pipeline.instruments)
            .await
            .map(|()| None);
    }
    // The job ends here and frees its slot; the acknowledgement follows completion.
    Ok(Some(Box::pin(async move {
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
        acknowledge(delivery, &pipeline.instruments).await
    })))
}

async fn acknowledge<M: SourceMessage>(
    delivery: M,
    instruments: &Instruments,
) -> anyhow::Result<()> {
    let started = Instant::now();
    delivery.ack().instrument(info_span!("ack")).await?;
    instruments.record(Stage::Ack, started);
    instruments.acknowledged.increment(1);
    Ok(())
}

async fn handle<I, R, O, K>(input: I, pipeline: &Pipeline<I, R, O, K>) -> anyhow::Result<Handled<I>>
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
                return Ok(Handled::Failed(Failure {
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
        return Ok(Handled::Failed(Failure {
            kind,
            error,
            attempts,
            input: Some(input),
        }));
    };
    finish(outputs, input, attempts, pipeline).await
}

/// Runs post-handler hooks, then prepares and submits every output.
async fn finish<I, R, O, K>(
    outputs: Vec<O>,
    input: I,
    attempts: usize,
    pipeline: &Pipeline<I, R, O, K>,
) -> anyhow::Result<Handled<I>>
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
                return Ok(Handled::Failed(Failure {
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
    // Preparing all outputs first means an encode failure never follows a partial publish.
    let started = Instant::now();
    let prepared = info_span!("encode").in_scope(|| {
        mapped
            .into_iter()
            .map(|output| pipeline.sink.prepare(output))
            .collect::<anyhow::Result<Vec<_>>>()
    });
    pipeline.instruments.record(Stage::Encode, started);
    let outputs = match prepared {
        Ok(outputs) => outputs,
        Err(error) => {
            return Ok(Handled::Failed(Failure {
                kind: FailureKind::Encode,
                error,
                attempts,
                input: Some(input),
            }));
        }
    };
    let started = Instant::now();
    let failures = &pipeline.instruments.publish_failures;
    let completions = async {
        let mut completions = Vec::with_capacity(outputs.len());
        for output in &outputs {
            completions.push(
                retry_publish(&pipeline.publish_retry, failures, || {
                    pipeline.sink.submit(output)
                })
                .await?,
            );
        }
        anyhow::Ok(completions)
    }
    .instrument(info_span!("publish", outputs = outputs.len()))
    .await?;
    pipeline.instruments.record(Stage::Publish, started);
    Ok(Handled::Submitted(completions))
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
            acknowledge(delivery, instruments).await
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
            acknowledge(delivery, instruments).await
        }
    }
}

pub(super) async fn retry_publish<T, F, Fut>(
    policy: &RetryPolicy,
    failures: &Counter,
    mut publish: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
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
                tokio::time::sleep(policy.delay(attempt)).await;
                attempt += 1;
            }
        }
    }
}
