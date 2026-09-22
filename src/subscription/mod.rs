//! Subscription construction and configuration.
//! Execution and per-delivery processing remain private implementation details.
mod config;
mod processing;
pub use config::SubscriptionConfig;
mod runtime;

use crate::{
    handler::{Emit, Handler, Result},
    retry::RetryPolicy,
    sink::Sink,
    source::{Source, SourceItem},
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type BoxHandler<I, O> = Arc<dyn Fn(I) -> BoxFuture<Result<Emit<O>>> + Send + Sync>;
type Mapper<I, O> = Arc<dyn Fn(&I, O) -> Result<O> + Send + Sync>;
type DeadLetter<I> = Arc<dyn Fn(I, RetryPolicy) -> BoxFuture<anyhow::Result<()>> + Send + Sync>;

pub struct Subscription<S: Source, K, O> {
    source: S,
    sink: K,
    handler: BoxHandler<SourceItem<S>, O>,
    dlq: Option<DeadLetter<SourceItem<S>>>,
    middleware: Vec<Mapper<SourceItem<S>, O>>,
    close_dlq: Option<Arc<dyn Fn() -> BoxFuture<anyhow::Result<()>> + Send + Sync>>,
    config: SubscriptionConfig,
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
            handler: Arc::new(move |v| {
                let handler = handler.clone();
                Box::pin(async move { handler.handle(v).await })
            }),
            dlq: None,
            close_dlq: None,
            middleware: vec![],
            config: SubscriptionConfig::default(),
        }
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
    /// Typed post-handler output mapping; executed once per emitted value, before publish retry.
    /// Broker-specific metadata policies can build on this hook.
    pub fn middleware<M>(mut self, map: M) -> Self
    where
        M: Fn(&SourceItem<S>, O) -> Result<O> + Send + Sync + 'static,
    {
        self.middleware.push(Arc::new(map));
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
