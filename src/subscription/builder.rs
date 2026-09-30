//! Subscription type and builder API.
use super::{
    config::{ProcessingOrder, SubscriptionConfig},
    processing,
};
use crate::{
    forward::{SamePlatform, ValueRecord},
    handler::{Emit, Handler, Result},
    middleware::Middleware,
    retry::RetryPolicy,
    sink::Sink,
    source::{Source, SourceItem},
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub(super) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(super) type BoxHandler<I, O> = Arc<dyn Fn(I) -> BoxFuture<Result<Emit<O>>> + Send + Sync>;
pub(super) type Mapper<I, O> = Arc<dyn Middleware<I, O>>;
pub(super) type DeadLetter<I> =
    Arc<dyn Fn(I, RetryPolicy) -> BoxFuture<anyhow::Result<()>> + Send + Sync>;

pub struct Subscription<S: Source, K, O> {
    pub(super) source: S,
    pub(super) sink: K,
    pub(super) handler: BoxHandler<SourceItem<S>, O>,
    pub(super) dlq: Option<DeadLetter<SourceItem<S>>>,
    pub(super) middleware: Vec<Mapper<SourceItem<S>, O>>,
    pub(super) close_dlq: Option<Arc<dyn Fn() -> BoxFuture<anyhow::Result<()>> + Send + Sync>>,
    pub(super) config: SubscriptionConfig,
}

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static> Subscription<S, K, O> {
    pub fn new<H>(source: S, sink: K, handler: H) -> Self
    where
        H: Handler<SourceItem<S>, Output = O>,
    {
        let handler = Arc::new(handler);
        Self::new_emitting(source, sink, move |input| {
            let handler = handler.clone();
            async move { handler.handle(input).await.map(Emit::One) }
        })
    }

    /// Explicitly opts into 0/1/N output; a plain Vec remains one payload.
    pub fn new_emitting<H>(source: S, sink: K, handler: H) -> Self
    where
        H: Handler<SourceItem<S>, Output = Emit<O>>,
    {
        let handler = Arc::new(handler);
        Self {
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
    pub fn forward<H, U>(source: S, sink: K, handler: H) -> Self
    where
        SourceItem<S>: SamePlatform<U, Publish = O>,
        H: Handler<<SourceItem<S> as ValueRecord>::Value, Output = U>,
        U: Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        Self::forward_emitting(source, sink, move |value| {
            let handler = handler.clone();
            async move { handler.handle(value).await.map(Emit::One) }
        })
    }

    /// Value-only counterpart of `new_emitting`; each emitted value inherits
    /// metadata from the same input record.
    pub fn forward_emitting<H, U>(source: S, sink: K, handler: H) -> Self
    where
        SourceItem<S>: SamePlatform<U, Publish = O>,
        H: Handler<<SourceItem<S> as ValueRecord>::Value, Output = Emit<U>>,
        U: Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        Self::new_emitting(source, sink, move |record: SourceItem<S>| {
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

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = name.into();
        self
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

    /// Rejected original typed input is published before ACK. Without a DLQ, Reject stops safely.
    pub fn dlq<D: Sink<SourceItem<S>>>(mut self, sink: D) -> Self {
        let sink = Arc::new(sink);
        let close_sink = sink.clone();
        self.close_dlq = Some(Arc::new(move || {
            let sink = close_sink.clone();
            Box::pin(async move { sink.close().await })
        }));
        self.dlq = Some(Arc::new(move |value, policy| {
            let sink = sink.clone();
            Box::pin(async move {
                let prepared = sink.prepare(value)?;
                processing::retry_publish(&policy, || sink.publish(&prepared)).await
            })
        }));
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

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.config.validate()
    }
}
