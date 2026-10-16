//! Handler shapes accepted by subscription constructors.
use super::{
    builder::{Commit, Outcome, Subscription},
    scheduler::Distributor,
};
use crate::{
    handler::{Emit, Handler, HandlerError, Result},
    record::{SamePlatform, ValueRecord},
    sink::Sink,
    source::{Source, SourceItem},
    state::{Begin, KeyOf, Keying, StateContext, StateStore, Stateful, StatefulHandler, Unkeyed},
};
use std::sync::Arc;
use tracing::debug;

/// Handler shape marker for the received record, or for an output that is
/// already the sink's type.
///
/// As the input marker, the handler takes [`SourceItem`] unchanged, such as a
/// `KafkaRecord<T>`, or the plain item of a source without records, such as
/// [`IterSource`](crate::IterSource). As the output marker, the handler
/// returns the type the sink publishes, such as a `KafkaPublish<T>`.
#[derive(Debug)]
pub struct ByRecord;

/// Handler shape marker for a plain value.
///
/// As the input marker, the handler takes the record's
/// [`ValueRecord::Value`]; a record without a representable value, such as a
/// Kafka null value, is rejected before the handler runs. As the output
/// marker, the handler returns a plain value that the source platform wraps
/// with [`SamePlatform::publish`], and the platform's
/// [`SamePlatform::Inherit`] middleware runs before any other middleware.
#[derive(Debug)]
pub struct ByValue;

/// Output cardinality of [`Subscription::new`]: each handler result is one output.
#[derive(Debug)]
pub struct One;

/// Output cardinality of [`Subscription::new_emitting`]: the handler returns
/// [`Emit`] with zero, one, or many outputs.
#[derive(Debug)]
pub struct Many;

/// State marker for a [`Handler`] taking only its input.
#[derive(Debug)]
pub struct Stateless;

/// State marker for a [`Stateful`] handler, which takes its input and the
/// state its store keeps for it.
#[derive(Debug)]
pub struct WithState;

/// Turns a handler result into the outputs of one delivery.
pub trait Cardinality<R>: 'static {
    /// One output value.
    type Item: Send + Sync + 'static;
    /// The outputs carried by `result`.
    fn emit(result: R) -> Emit<Self::Item>;
}

impl<R: Send + Sync + 'static> Cardinality<R> for One {
    type Item = R;
    fn emit(result: R) -> Emit<R> {
        Emit::One(result)
    }
}

impl<T: Send + Sync + 'static> Cardinality<Emit<T>> for Many {
    type Item = T;
    fn emit(result: Emit<T>) -> Emit<T> {
        result
    }
}

/// A handler that a subscription between source `S` and sink `K` can run.
///
/// Implemented for every [`Handler`], and every [`Stateful`] handler, whose
/// input and output fit one of the shapes selected by the marker
/// `M = (In, Out, C, St)`. `In` and `Out` are [`ByRecord`] or [`ByValue`] and
/// are inferred from the handler's signature; `C` is [`One`] or [`Many`] and
/// is fixed by the constructor; `St` is [`Stateless`] or [`WithState`]. For a
/// Kafka source and sink:
///
/// | Handler | `In` | `Out` |
/// | --- | --- | --- |
/// | `async fn(String) -> Result<String>` | `ByValue` | `ByValue` |
/// | `async fn(KafkaRecord<String>) -> Result<String>` | `ByRecord` | `ByValue` |
/// | `async fn(String) -> Result<KafkaPublish<String>>` | `ByValue` | `ByRecord` |
/// | `async fn(KafkaRecord<String>) -> Result<KafkaPublish<String>>` | `ByRecord` | `ByRecord` |
///
/// A stateful handler takes its state as a second parameter, as in
/// `async fn(String, State<u64>) -> Result<String>`, and the input and output
/// shapes are chosen the same way. Its store's key must suit the source: the
/// default message key needs a source whose messages implement
/// [`KeyedMessage`](crate::KeyedMessage).
///
/// A handler whose shape the source or sink does not support fails to
/// compile. Closures need their parameter type annotated, because the shape is
/// chosen from it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot handle the deliveries of `{S}` for `{K}`",
    label = "this handler's input or output does not match the source or sink",
    note = "a handler takes the source's record or its value, and returns the sink's publish type or a plain value",
    note = "a stateful handler keeps its state per message key by default, which sources without message keys lack; choose a state key with `.key(|input| ..)`"
)]
pub trait IntoHandler<S: Source, K, M>: Send + Sync + 'static {
    /// Output type the sink publishes.
    type Output: Send + Sync + 'static;
    /// [`Keyed`](crate::state::Keyed) for a stateful handler whose store keeps keyed state,
    /// otherwise [`Unkeyed`].
    type Keying: Keying;
    /// Build a subscription that runs this handler.
    #[doc(hidden)]
    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output, Self::Keying>;
}

impl<S, K, H, C> IntoHandler<S, K, (ByRecord, ByRecord, C, Stateless)> for H
where
    S: Source,
    H: Handler<SourceItem<S>>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;
    type Keying = Unkeyed;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, C::Item, Unkeyed> {
        let handler = Arc::new(self);
        Subscription::with_handler(
            name,
            source,
            sink,
            false,
            Arc::new(move |input, _| {
                let handler = handler.clone();
                Box::pin(async move {
                    let result = handler.handle(input).await?;
                    Ok(Outcome::stateless(C::emit(result)))
                })
            }),
        )
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByValue, ByRecord, C, Stateless)> for H
where
    S: Source,
    SourceItem<S>: ValueRecord,
    H: Handler<<SourceItem<S> as ValueRecord>::Value>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;
    type Keying = Unkeyed;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, C::Item, Unkeyed> {
        let handler = Arc::new(self);
        Subscription::with_handler(
            name,
            source,
            sink,
            false,
            Arc::new(move |record: SourceItem<S>, _| {
                let handler = handler.clone();
                Box::pin(async move {
                    let result = handler.handle(record.value()?).await?;
                    Ok(Outcome::stateless(C::emit(result)))
                })
            }),
        )
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByRecord, ByValue, C, Stateless)> for H
where
    S: Source,
    H: Handler<SourceItem<S>>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;
    type Keying = Unkeyed;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output, Unkeyed> {
        let handler = Arc::new(self);
        Subscription::with_handler(
            name,
            source,
            sink,
            false,
            Arc::new(move |input, _| {
                let handler = handler.clone();
                Box::pin(async move {
                    let result = handler.handle(input).await?;
                    Ok(Outcome::stateless(publish::<S, _>(C::emit(result))))
                })
            }),
        )
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByValue, ByValue, C, Stateless)> for H
where
    S: Source,
    H: Handler<<SourceItem<S> as ValueRecord>::Value>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;
    type Keying = Unkeyed;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output, Unkeyed> {
        let handler = Arc::new(self);
        Subscription::with_handler(
            name,
            source,
            sink,
            false,
            Arc::new(move |record: SourceItem<S>, _| {
                let handler = handler.clone();
                Box::pin(async move {
                    let result = handler.handle(record.value()?).await?;
                    Ok(Outcome::stateless(publish::<S, _>(C::emit(result))))
                })
            }),
        )
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}

impl<S, K, St, H, C> IntoHandler<S, K, (ByRecord, ByRecord, C, WithState)> for Stateful<St, H>
where
    S: Source,
    St: StateStore<SourceItem<S>, S::Message>,
    St::Keying: KeyOf<St, SourceItem<S>, S::Message>,
    H: StatefulHandler<SourceItem<S>, St::State>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;
    type Keying = St::Keying;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, C::Item, St::Keying> {
        let stateful = Arc::new(self);
        let distributor = by_record_key::<S, _, _>(&stateful, &name);
        Subscription::with_handler(
            name,
            source,
            sink,
            true,
            Arc::new(move |input, context| {
                Box::pin(run_stateful(stateful.clone(), input, context, C::emit))
            }),
        )
        .distribute_by(distributor)
    }
}

impl<S, K, St, H, C> IntoHandler<S, K, (ByValue, ByRecord, C, WithState)> for Stateful<St, H>
where
    S: Source,
    SourceItem<S>: ValueRecord,
    <SourceItem<S> as ValueRecord>::Value: Sync,
    St: StateStore<<SourceItem<S> as ValueRecord>::Value, S::Message>,
    St::Keying: KeyOf<St, <SourceItem<S> as ValueRecord>::Value, S::Message>,
    H: StatefulHandler<<SourceItem<S> as ValueRecord>::Value, St::State>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;
    type Keying = St::Keying;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, C::Item, St::Keying> {
        let stateful = Arc::new(self);
        let distributor = by_value_key::<S, _, _>(&stateful, &name);
        Subscription::with_handler(
            name,
            source,
            sink,
            true,
            Arc::new(move |record: SourceItem<S>, context| {
                let stateful = stateful.clone();
                Box::pin(
                    async move { run_stateful(stateful, record.value()?, context, C::emit).await },
                )
            }),
        )
        .distribute_by(distributor)
    }
}

impl<S, K, St, H, C> IntoHandler<S, K, (ByRecord, ByValue, C, WithState)> for Stateful<St, H>
where
    S: Source,
    St: StateStore<SourceItem<S>, S::Message>,
    St::Keying: KeyOf<St, SourceItem<S>, S::Message>,
    H: StatefulHandler<SourceItem<S>, St::State>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;
    type Keying = St::Keying;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output, St::Keying> {
        let stateful = Arc::new(self);
        let distributor = by_record_key::<S, _, _>(&stateful, &name);
        Subscription::with_handler(
            name,
            source,
            sink,
            true,
            Arc::new(move |input, context| {
                Box::pin(run_stateful(stateful.clone(), input, context, |result| {
                    publish::<S, _>(C::emit(result))
                }))
            }),
        )
        .distribute_by(distributor)
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}

impl<S, K, St, H, C> IntoHandler<S, K, (ByValue, ByValue, C, WithState)> for Stateful<St, H>
where
    S: Source,
    <SourceItem<S> as ValueRecord>::Value: Sync,
    St: StateStore<<SourceItem<S> as ValueRecord>::Value, S::Message>,
    St::Keying: KeyOf<St, <SourceItem<S> as ValueRecord>::Value, S::Message>,
    H: StatefulHandler<<SourceItem<S> as ValueRecord>::Value, St::State>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;
    type Keying = St::Keying;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output, St::Keying> {
        let stateful = Arc::new(self);
        let distributor = by_value_key::<S, _, _>(&stateful, &name);
        Subscription::with_handler(
            name,
            source,
            sink,
            true,
            Arc::new(move |record: SourceItem<S>, context| {
                let stateful = stateful.clone();
                Box::pin(async move {
                    run_stateful(stateful, record.value()?, context, |result| {
                        publish::<S, _>(C::emit(result))
                    })
                    .await
                })
            }),
        )
        .distribute_by(distributor)
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}

/// Distributes deliveries by the state key of the record a handler takes,
/// for a keyed store.
fn by_record_key<S, St, H>(
    stateful: &Arc<Stateful<St, H>>,
    name: &str,
) -> Option<Distributor<S::Message>>
where
    S: Source,
    St: StateStore<SourceItem<S>, S::Message>,
    St::Keying: KeyOf<St, SourceItem<S>, S::Message>,
    H: Send + Sync + 'static,
{
    if !<St::Keying as KeyOf<St, SourceItem<S>, S::Message>>::KEYED {
        return None;
    }
    let stateful = stateful.clone();
    let name = name.to_owned();
    Some(Distributor::by_key(move |message, record| {
        let context = StateContext::of(&name, message);
        St::Keying::key(&stateful.store, record, &context)
    }))
}

/// Distributes deliveries by the state key of the value a handler takes,
/// for a keyed store.
fn by_value_key<S, St, H>(
    stateful: &Arc<Stateful<St, H>>,
    name: &str,
) -> Option<Distributor<S::Message>>
where
    S: Source,
    SourceItem<S>: ValueRecord,
    St: StateStore<<SourceItem<S> as ValueRecord>::Value, S::Message>,
    St::Keying: KeyOf<St, <SourceItem<S> as ValueRecord>::Value, S::Message>,
    H: Send + Sync + 'static,
{
    if !<St::Keying as KeyOf<St, _, S::Message>>::KEYED {
        return None;
    }
    let stateful = stateful.clone();
    let name = name.to_owned();
    Some(Distributor::by_key(
        move |message, record: &SourceItem<S>| {
            let context = StateContext::of(&name, message);
            let value = record.value().ok()?;
            St::Keying::key(&stateful.store, &value, &context)
        },
    ))
}

/// Wraps each value output in the source platform's publish record.
fn publish<S, U>(outputs: Emit<U>) -> Emit<<SourceItem<S> as SamePlatform<U>>::Publish>
where
    S: Source,
    SourceItem<S>: SamePlatform<U>,
{
    outputs.map(<SourceItem<S> as SamePlatform<U>>::publish)
}

/// Runs one attempt of a stateful handler: begins the state change, runs the
/// handler with the state, and returns the outputs with the change to commit.
async fn run_stateful<I, M, St, H, O>(
    stateful: Arc<Stateful<St, H>>,
    input: I,
    context: Option<Arc<StateContext>>,
    emit: impl FnOnce(H::Output) -> Emit<O>,
) -> Result<Outcome<O>>
where
    I: Sync,
    St: StateStore<I, M>,
    H: StatefulHandler<I, St::State>,
{
    let context = context
        .ok_or_else(|| HandlerError::Fatal("a stateful handler ran without its context".into()))?;
    let (state, txn) = match stateful.store.begin(&input, &context).await? {
        Begin::Ready(state, txn) => (state, txn),
        Begin::Applied => {
            debug!("state already holds this delivery; skipping the handler");
            return Ok(Outcome::stateless(Emit::None));
        }
    };
    let result = stateful.handler.handle(input, state).await?;
    let commit: Commit =
        Box::new(move || Box::pin(async move { stateful.store.commit(txn).await }));
    Ok(Outcome {
        outputs: emit(result),
        commit: Some(commit),
    })
}
