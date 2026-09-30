//! Subscription type and builder API.
use super::{
    config::{ProcessingOrder, SubscriptionConfig},
    processing,
};
use crate::{
    dead_letter::DeadLetter,
    error_policy::ErrorPolicy,
    forward::{SamePlatform, ValueRecord},
    handler::{Emit, Handler},
    middleware::Middleware,
    retry::RetryPolicy,
    sink::Sink,
    source::{Source, SourceItem, SourceRaw},
};
use metrics::Counter;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub(super) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(super) type BoxHandler<I, O> =
    Arc<dyn Fn(I) -> BoxFuture<crate::handler::Result<Emit<O>>> + Send + Sync>;
pub(super) type Mapper<I, O> = Arc<dyn Middleware<I, O>>;
pub(super) type DeadLetterRoute<I, R> = Arc<
    dyn Fn(DeadLetter<I, R>, RetryPolicy, Counter) -> BoxFuture<anyhow::Result<()>> + Send + Sync,
>;

pub struct Subscription<S: Source, K, O> {
    pub(super) name: String,
    pub(super) source: S,
    pub(super) sink: K,
    pub(super) handler: BoxHandler<SourceItem<S>, O>,
    pub(super) dlq: Option<DeadLetterRoute<SourceItem<S>, SourceRaw<S>>>,
    pub(super) middleware: Vec<Mapper<SourceItem<S>, O>>,
    pub(super) close_dlq: Option<Arc<dyn Fn() -> BoxFuture<anyhow::Result<()>> + Send + Sync>>,
    pub(super) config: SubscriptionConfig,
}

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    /// Registers a handler. `name` identifies the subscription in errors, dead letters,
    /// spans, and metric labels; it must be non-empty and unique within an [`App`](crate::App).
    pub fn new<H>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        H: Handler<SourceItem<S>, Output = O>,
    {
        let handler = Arc::new(handler);
        Self::new_emitting(name, source, sink, move |input| {
            let handler = handler.clone();
            async move { handler.handle(input).await.map(Emit::One) }
        })
    }

    /// Explicitly opts into 0/1/N output; a plain Vec remains one payload.
    pub fn new_emitting<H>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        H: Handler<SourceItem<S>, Output = Emit<O>>,
    {
        let handler = Arc::new(handler);
        Self {
            name: name.into(),
            source,
            sink,
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

    /// Registers a value-only handler between a source and sink of the same platform.
    ///
    /// The handler receives the record value and returns the output value. The
    /// platform's publish record is built from that value, and the platform's
    /// default metadata inheritance runs before any other middleware.
    pub fn forward<H, U>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        SourceItem<S>: SamePlatform<U, Publish = O>,
        H: Handler<<SourceItem<S> as ValueRecord>::Value, Output = U>,
        U: Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        Self::forward_emitting(name, source, sink, move |value| {
            let handler = handler.clone();
            async move { handler.handle(value).await.map(Emit::One) }
        })
    }

    /// Value-only counterpart of `new_emitting`; each emitted value inherits
    /// metadata from the same input record.
    pub fn forward_emitting<H, U>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        SourceItem<S>: SamePlatform<U, Publish = O>,
        H: Handler<<SourceItem<S> as ValueRecord>::Value, Output = Emit<U>>,
        U: Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        Self::new_emitting(name, source, sink, move |record: SourceItem<S>| {
            let handler = handler.clone();
            async move {
                let values = handler.handle(record.value()?).await?.values();
                Ok(Emit::Many(
                    values
                        .into_iter()
                        .map(<SourceItem<S> as SamePlatform<U>>::publish)
                        .collect(),
                ))
            }
        })
        .middleware(<SourceItem<S> as SamePlatform<U>>::Inherit::default())
    }

    pub fn concurrency(mut self, value: usize) -> Self {
        self.config.concurrency = value;
        self
    }

    pub fn max_in_flight(mut self, value: usize) -> Self {
        self.config.max_in_flight = value;
        self
    }

    pub fn ordering(mut self, order: ProcessingOrder) -> Self {
        self.config.ordering = order;
        self
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.config.handler_retry = policy;
        self
    }

    pub fn receive_retry(mut self, policy: RetryPolicy) -> Self {
        self.config.receive_retry = policy;
        self
    }

    pub fn publish_retry(mut self, policy: RetryPolicy) -> Self {
        self.config.publish_retry = policy;
        self
    }

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
        F: Fn(DeadLetter<SourceItem<S>, SourceRaw<S>>) -> anyhow::Result<T> + Send + Sync + 'static,
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
                processing::retry_publish(&policy, &failures, || sink.publish(&prepared))
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

    pub fn config(mut self, config: SubscriptionConfig) -> Self {
        self.config = config;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.name.trim().is_empty(),
            "subscription name must not be empty"
        );
        self.config.validate()?;
        self.config.error_policy.validate(self.dlq.is_some())
    }
}
