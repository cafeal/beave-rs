//! Subscription type and builder API.
use super::{
    completion::{Complete, Publish, Transact},
    config::{ProcessingOrder, SubscriptionConfig, TransactionBatch},
    handler::{IntoHandler, Many, One},
    hooks::DynMiddleware,
    processing,
};
use crate::{
    dead_letter::DeadLetter,
    error::{BoxContext, BoxError, Error, ensure},
    error_policy::ErrorPolicy,
    handler::{Emit, Handler},
    middleware::Middleware,
    retry::RetryPolicy,
    sink::Sink,
    source::{Source, SourceItem, SourceRaw},
    transaction::TransactionalSink,
};
use metrics::Counter;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub(super) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(super) type BoxHandler<I, O> =
    Arc<dyn Fn(I) -> BoxFuture<crate::handler::Result<Emit<O>>> + Send + Sync>;
pub(super) type Mapper<I, O> = Arc<dyn DynMiddleware<I, O>>;
pub(super) type DeadLetterRoute<I, R> = Arc<
    dyn Fn(DeadLetter<I, R>, RetryPolicy, Counter) -> BoxFuture<Result<(), BoxError>> + Send + Sync,
>;
type CloseDeadLetter = Arc<dyn Fn() -> BoxFuture<Result<(), BoxError>> + Send + Sync>;

/// One source, handler, and sink processed together, with its middleware, dead-letter sink,
/// and [`SubscriptionConfig`].
///
/// Register it on an [`App`](crate::App) with [`App::subscription`](crate::App::subscription).
pub struct Subscription<S: Source, K, O> {
    pub(super) name: String,
    pub(super) source: S,
    pub(super) sink: Arc<K>,
    pub(super) output: Arc<dyn Complete<S::Message, O>>,
    pub(super) transactional: bool,
    pub(super) handler: BoxHandler<SourceItem<S>, O>,
    pub(super) dlq: Option<DeadLetterRoute<SourceItem<S>, SourceRaw<S>>>,
    pub(super) middleware: Vec<Mapper<SourceItem<S>, O>>,
    pub(super) close_dlq: Option<CloseDeadLetter>,
    pub(super) config: SubscriptionConfig,
}

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    /// Registers a handler. `name` identifies the subscription in errors, dead letters,
    /// spans, and metric labels; it must be non-empty and unique within an [`App`](crate::App).
    ///
    /// The handler takes the received record or its value and returns the sink's
    /// output type or a plain value; see [`IntoHandler`] for the accepted shapes.
    pub fn new<H, In, Out>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        H: IntoHandler<S, K, (In, Out, One), Output = O>,
    {
        handler.into_subscription(name.into(), source, sink)
    }

    /// Explicitly opts into 0/1/N output: the handler returns [`Emit`]. A plain `Vec`
    /// remains one payload. Each emitted value of a [`ByValue`](super::ByValue) output
    /// inherits metadata from the same input record.
    pub fn new_emitting<H, In, Out>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        H: IntoHandler<S, K, (In, Out, Many), Output = O>,
    {
        handler.into_subscription(name.into(), source, sink)
    }

    pub(super) fn with_handler<H>(name: String, source: S, sink: K, handler: H) -> Self
    where
        H: Handler<SourceItem<S>, Output = Emit<O>>,
    {
        let handler = Arc::new(handler);
        let sink = Arc::new(sink);
        Self {
            name,
            source,
            output: Arc::new(Publish(sink.clone())),
            sink,
            transactional: false,
            handler: Arc::new(move |value| {
                let handler = handler.clone();
                Box::pin(async move { handler.handle(value).await })
            }),
            dlq: None,
            close_dlq: None,
            middleware: vec![],
            config: SubscriptionConfig::default(),
        }
    }

    /// Maximum number of deliveries processed at the same time. Defaults to 1.
    pub fn concurrency(mut self, value: usize) -> Self {
        self.config.concurrency = value;
        self
    }

    /// Maximum number of received but unfinished deliveries. Defaults to 64.
    pub fn max_in_flight(mut self, value: usize) -> Self {
        self.config.max_in_flight = value;
        self
    }

    /// How deliveries that share an ordering key are scheduled. Defaults to
    /// [`ProcessingOrder::PerKey`].
    pub fn ordering(mut self, order: ProcessingOrder) -> Self {
        self.config.ordering = order;
        self
    }

    /// Retry policy for handler attempts after [`HandlerError::Retry`](crate::HandlerError::Retry).
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.config.handler_retry = policy;
        self
    }

    /// Retry policy for failed receives.
    pub fn receive_retry(mut self, policy: RetryPolicy) -> Self {
        self.config.receive_retry = policy;
        self
    }

    /// Retry policy for output publication.
    pub fn publish_retry(mut self, policy: RetryPolicy) -> Self {
        self.config.publish_retry = policy;
        self
    }

    /// Bound on draining outstanding deliveries when the subscription stops. Defaults to 30 s.
    pub fn drain_timeout(mut self, duration: Duration) -> Self {
        self.config.drain_timeout = duration;
        self
    }

    /// Dead-letter sink for failures the [`ErrorPolicy`] routes there. Each dead letter is
    /// published before the original delivery is acknowledged.
    pub fn dlq<D: Sink<DeadLetter<SourceItem<S>, SourceRaw<S>>>>(self, sink: D) -> Self {
        self.dlq_with(sink, Ok)
    }

    /// Like [`dlq`](Self::dlq), converting each dead letter into the sink's output type first,
    /// for example to forward the original payload with failure details as broker metadata.
    /// Conversion and preparation run once, before dead-letter publish retries.
    pub fn dlq_with<D, T, F>(mut self, sink: D, convert: F) -> Self
    where
        D: Sink<T>,
        F: Fn(DeadLetter<SourceItem<S>, SourceRaw<S>>) -> Result<T, BoxError>
            + Send
            + Sync
            + 'static,
    {
        let sink = Arc::new(sink);
        let convert = Arc::new(convert);
        let close_sink = sink.clone();
        self.close_dlq = Some(Arc::new(move || {
            let sink = close_sink.clone();
            Box::pin(async move { sink.close().await })
        }));
        self.dlq = Some(Arc::new(move |dead_letter, policy, failures| {
            let sink = sink.clone();
            let convert = convert.clone();
            Box::pin(async move {
                let prepared = convert(dead_letter)
                    .and_then(|output| sink.prepare(output))
                    .map_err(|error| error.context("prepare dead letter failed"))?;
                processing::retry_publish(&policy, &failures, None, || sink.publish(&prepared))
                    .await
                    .map_err(|error| error.context("dead-letter publish failed"))
            })
        }));
        self
    }

    /// Retry policy for dead-letter publication, independent of the output publish policy.
    pub fn dlq_retry(mut self, policy: RetryPolicy) -> Self {
        self.config.dead_letter_retry = policy;
        self
    }

    /// Outcome of decode, rejection, handler retry exhaustion, and encode failures.
    pub fn error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.config.error_policy = policy;
        self
    }

    /// Registers middleware whose hooks run around the handler in registration order.
    pub fn middleware<M: Middleware<SourceItem<S>, O>>(mut self, middleware: M) -> Self {
        self.middleware.push(Arc::new(middleware));
        self
    }

    /// Batching of a [transactional](Self::transactional) subscription's
    /// commits.
    pub fn transaction_batch(mut self, batch: TransactionBatch) -> Self {
        self.config.transaction_batch = batch;
        self
    }

    /// Replace the whole configuration, including settings made by earlier builder calls.
    pub fn config(mut self, config: SubscriptionConfig) -> Self {
        self.config = config;
        self
    }

    /// The subscription name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Publishes deliveries' outputs and acknowledges the deliveries in sink
    /// transactions, for exactly-once processing between a source and a sink
    /// whose transactions include the source's acknowledgement. Each
    /// transaction commits a batch of deliveries bounded by
    /// [`transaction_batch`](Self::transaction_batch).
    ///
    /// Only compiles when the sink implements [`TransactionalSink`] for the
    /// source's message type:
    ///
    /// ```compile_fail
    /// use beavers::{InMemorySink, IterSource, Subscription};
    ///
    /// let source = IterSource::new([1]);
    /// Subscription::new("numbers", source, InMemorySink::default(), |n: i32| async move {
    ///     Ok(n)
    /// })
    /// .transactional();
    /// ```
    ///
    /// A delivery acknowledged without output, because it was discarded or
    /// dead-lettered, joins a batch without outputs. Dead letters are published
    /// by the dead-letter sink outside the transaction.
    /// A transactional subscription requires [`ProcessingOrder::PerKey`], so
    /// deliveries of one ordering scope join batches in receive order.
    pub fn transactional(mut self) -> Self
    where
        S::Message: Sync,
        K: TransactionalSink<S::Message, O>,
    {
        self.output = Arc::new(Transact::new(self.sink.clone()));
        self.transactional = true;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        ensure!(
            !self.name.trim().is_empty(),
            Error::config,
            "subscription name must not be empty"
        );
        self.config.validate()?;
        ensure!(
            !self.transactional || self.config.ordering == ProcessingOrder::PerKey,
            Error::config,
            "transactional subscriptions require ProcessingOrder::PerKey"
        );
        self.config.error_policy.validate(self.dlq.is_some())
    }
}
