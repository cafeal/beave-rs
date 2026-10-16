//! Subscription type and builder API.
use super::{
    completion::{Complete, Publish, Transact},
    config::{SubscriptionConfig, TransactionBatch},
    handler::{IntoHandler, Many, One},
    hooks::DynMiddleware,
    processing,
    scheduler::Distributor,
};
use crate::{
    dead_letter::DeadLetter,
    distribution::{Distribution, SourceOrder},
    error::{BoxContext, BoxError, Error, ensure},
    error_policy::ErrorPolicy,
    handler::Emit,
    health::Tracker,
    middleware::Middleware,
    retry::RetryPolicy,
    sink::Sink,
    source::{Source, SourceItem, SourceRaw},
    state::{Distributable, Keying, StateContext, Unkeyed},
    transaction::TransactionalSink,
};
use metrics::Counter;
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc, time::Duration};

pub(super) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(super) type BoxHandler<I, O> = Arc<
    dyn Fn(I, Option<Arc<StateContext>>) -> BoxFuture<crate::handler::Result<Outcome<O>>>
        + Send
        + Sync,
>;
/// Applies a stateful handler's state change; runs after the outputs are
/// published and before the delivery is acknowledged.
pub(super) type Commit = Box<dyn FnOnce() -> BoxFuture<Result<(), BoxError>> + Send>;

/// What one handler attempt produced.
pub(super) struct Outcome<O> {
    pub(super) outputs: Emit<O>,
    /// The state change to commit, for a stateful handler that ran.
    pub(super) commit: Option<Commit>,
}

impl<O> Outcome<O> {
    pub(super) fn stateless(outputs: Emit<O>) -> Self {
        Self {
            outputs,
            commit: None,
        }
    }
}
pub(super) type Mapper<I, O> = Arc<dyn DynMiddleware<I, O>>;
pub(super) type DeadLetterRoute<I, R> = Arc<
    dyn Fn(DeadLetter<I, R>, RetryPolicy, Counter, Arc<Tracker>) -> BoxFuture<Result<(), BoxError>>
        + Send
        + Sync,
>;
type CloseDeadLetter = Arc<dyn Fn() -> BoxFuture<Result<(), BoxError>> + Send + Sync>;

/// One source, handler, and sink processed together, with its middleware, dead-letter sink,
/// and processing settings such as concurrency, retries, and the [`ErrorPolicy`].
///
/// Register it on an [`App`](crate::App) with [`App::subscription`](crate::App::subscription).
///
/// `Ks` is [`Keyed`](crate::state::Keyed) when a stateful handler keeps keyed
/// state, which fixes the subscription's distribution to the state key, and
/// [`Unkeyed`] otherwise.
pub struct Subscription<S: Source, K, O, Ks: Keying = Unkeyed> {
    pub(super) name: String,
    pub(super) source: S,
    pub(super) sink: Arc<K>,
    pub(super) output: Arc<dyn Complete<S::Message, O>>,
    pub(super) transactional: bool,
    pub(super) stateful: bool,
    pub(super) handler: BoxHandler<SourceItem<S>, O>,
    pub(super) dlq: Option<DeadLetterRoute<SourceItem<S>, SourceRaw<S>>>,
    pub(super) middleware: Vec<Mapper<SourceItem<S>, O>>,
    pub(super) close_dlq: Option<CloseDeadLetter>,
    pub(super) distributor: Distributor<S::Message>,
    pub(super) config: SubscriptionConfig,
    pub(super) keying: PhantomData<Ks>,
}

impl<S: Source, K: Sink<O>, O: Send + Sync + 'static, Ks: Keying> Subscription<S, K, O, Ks> {
    /// Registers a handler. `name` identifies the subscription in errors, dead letters,
    /// spans, and metric labels; it must be non-empty and unique within an [`App`](crate::App).
    ///
    /// The handler takes the received record or its value and returns the sink's
    /// output type or a plain value; see [`IntoHandler`] for the accepted shapes.
    pub fn new<H, In, Out, St>(name: impl Into<String>, source: S, sink: K, handler: H) -> Self
    where
        H: IntoHandler<S, K, (In, Out, One, St), Output = O, Keying = Ks>,
    {
        handler.into_subscription(name.into(), source, sink)
    }

    /// Explicitly opts into 0/1/N output: the handler returns [`Emit`]. A plain `Vec`
    /// remains one payload. Each emitted value of a [`ByValue`](super::ByValue) output
    /// inherits metadata from the same input record.
    pub fn new_emitting<H, In, Out, St>(
        name: impl Into<String>,
        source: S,
        sink: K,
        handler: H,
    ) -> Self
    where
        H: IntoHandler<S, K, (In, Out, Many, St), Output = O, Keying = Ks>,
    {
        handler.into_subscription(name.into(), source, sink)
    }

    pub(super) fn with_handler(
        name: String,
        source: S,
        sink: K,
        stateful: bool,
        handler: BoxHandler<SourceItem<S>, O>,
    ) -> Self {
        let sink = Arc::new(sink);
        Self {
            name,
            source,
            output: Arc::new(Publish(sink.clone())),
            sink,
            transactional: false,
            stateful,
            handler,
            dlq: None,
            close_dlq: None,
            middleware: vec![],
            distributor: Distributor::new(SourceOrder),
            config: SubscriptionConfig::default(),
            keying: PhantomData,
        }
    }

    /// Distributes by `distributor`, which a keyed stateful handler sets to its
    /// state key.
    pub(super) fn distribute_by(mut self, distributor: Option<Distributor<S::Message>>) -> Self {
        if let Some(distributor) = distributor {
            self.distributor = distributor;
        }
        self
    }

    /// Maximum number of deliveries processed at the same time. Defaults to 1.
    /// Received deliveries decode in parallel up to the same number.
    pub fn concurrency(mut self, value: usize) -> Self {
        self.config.concurrency = value;
        self
    }

    /// Maximum number of received but unfinished deliveries, including
    /// deliveries still decoding or waiting for their
    /// [distribution](Self::distribution) key. Defaults to 64.
    pub fn max_in_flight(mut self, value: usize) -> Self {
        self.config.max_in_flight = value;
        self
    }

    /// Maximum number of received but unfinished deliveries with one ordering
    /// key, such as one Kafka partition. Unlimited by default.
    ///
    /// When a key reaches the limit, the subscription asks the source to
    /// [pause](crate::Source::pause) that key and resumes it once its
    /// unfinished deliveries drop to half the limit, so one slow key does not
    /// fill [`max_in_flight`](Self::max_in_flight) and stop the other keys.
    /// Sources that cannot pause single keys ignore it. Deliveries the source
    /// had already fetched still arrive, so a key can exceed the limit by
    /// what the source buffers.
    pub fn max_in_flight_per_key(mut self, value: usize) -> Self {
        self.config.max_in_flight_per_key = Some(value);
        self
    }

    /// Which deliveries are processed one at a time, in the order they were
    /// received; the others run in parallel up to
    /// [`concurrency`](Self::concurrency). Defaults to [`SourceOrder`], the
    /// order in which the source delivers, such as one Kafka partition.
    ///
    /// Takes any [`Distribution`] of the source's messages, such as
    /// [`Parallel`](crate::Parallel), an adapter's own distribution, or a
    /// closure that returns a key from the decoded value.
    ///
    /// A subscription whose [stateful](crate::state::stateful) handler keeps
    /// [keyed](crate::state::Keyed) state distributes by the
    /// [state key](crate::state::KeyedStateStore::key) instead, so that
    /// deliveries sharing state are processed in the order they were
    /// received, and does not compile with it:
    ///
    /// ```compile_fail
    /// use beavers::{InMemorySink, IterSource, Parallel, Subscription};
    /// use beavers::state::{InMemoryState, State, stateful};
    ///
    /// async fn total(value: i32, mut total: State<i32>) -> beavers::Result<i32> {
    ///     *total += value;
    ///     Ok(*total)
    /// }
    ///
    /// Subscription::new(
    ///     "totals",
    ///     IterSource::new([1, 2]),
    ///     InMemorySink::<i32>::default(),
    ///     stateful(InMemoryState::<i32>::new().key(|_: &i32| "all"), total),
    /// )
    /// .distribution(Parallel);
    /// ```
    pub fn distribution<D: Distribution<S::Message>>(mut self, distribution: D) -> Self
    where
        Ks: Distributable,
    {
        self.distributor = Distributor::new(distribution);
        self
    }

    /// Retry policy for handler attempts after [`HandlerError::Retry`](crate::HandlerError::Retry).
    pub fn handler_retry(mut self, policy: RetryPolicy) -> Self {
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

    /// Makes the application not live when deliveries are in flight but no
    /// handler attempt, publish attempt, or acknowledgement has finished for
    /// `duration`. Disabled by default; choose a duration longer than the
    /// slowest expected handler attempt.
    pub fn stall_timeout(mut self, duration: Duration) -> Self {
        self.config.stall_timeout = Some(duration);
        self
    }

    /// Dead-letter sink for failures the [`ErrorPolicy`] routes there. Each dead letter is
    /// published before the original delivery is acknowledged.
    pub fn dead_letter<D: Sink<DeadLetter<SourceItem<S>, SourceRaw<S>>>>(self, sink: D) -> Self {
        self.dead_letter_with(sink, Ok)
    }

    /// Like [`dead_letter`](Self::dead_letter), converting each dead letter into the sink's
    /// output type first, for example to forward the original payload with failure details as
    /// broker metadata. Conversion and preparation run once, before dead-letter publish retries.
    pub fn dead_letter_with<D, T, F>(mut self, sink: D, convert: F) -> Self
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
        self.dlq = Some(Arc::new(move |dead_letter, policy, failures, health| {
            let sink = sink.clone();
            let convert = convert.clone();
            Box::pin(async move {
                let prepared = convert(dead_letter)
                    .and_then(|output| sink.prepare(output))
                    .map_err(|error| error.context("prepare dead letter failed"))?;
                // Dead-letter retries do not affect readiness.
                processing::retry_publish(&policy, &failures, Some(&health), false, || {
                    sink.publish(&prepared)
                })
                .await
                .map_err(|error| error.context("dead-letter publish failed"))
            })
        }));
        self
    }

    /// Retry policy for dead-letter publication, independent of the output publish policy.
    pub fn dead_letter_retry(mut self, policy: RetryPolicy) -> Self {
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
    /// A transactional subscription requires a [`Distribution`] that
    /// [keeps the source order](Distribution::keeps_source_order), such as the
    /// default [`SourceOrder`], so deliveries of one ordering scope join
    /// batches in receive order. A
    /// [stateful](crate::state::stateful) handler cannot run in a
    /// transactional subscription, because its state change could not commit
    /// together with the transaction.
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
            !(self.transactional && self.stateful),
            Error::config,
            "a transactional subscription cannot run a stateful handler"
        );
        ensure!(
            !self.transactional || self.distributor.keeps_source_order(),
            Error::config,
            "transactional subscriptions require a distribution that keeps the source order"
        );
        self.config.error_policy.validate(self.dlq.is_some())
    }
}
