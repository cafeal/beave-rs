//! Handler shapes accepted by subscription constructors.
use super::builder::Subscription;
use crate::{
    forward::{SamePlatform, ValueRecord},
    handler::{Emit, Handler},
    sink::Sink,
    source::{Source, SourceItem},
};
use std::sync::Arc;

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
/// Implemented for every [`Handler`] whose input and output fit one of the
/// shapes selected by the marker `M = (In, Out, C)`. `In` and `Out` are
/// [`ByRecord`] or [`ByValue`] and are inferred from the handler's signature;
/// `C` is [`One`] or [`Many`] and is fixed by the constructor. For a Kafka
/// source and sink:
///
/// | Handler | `In` | `Out` |
/// | --- | --- | --- |
/// | `async fn(String) -> Result<String>` | `ByValue` | `ByValue` |
/// | `async fn(KafkaRecord<String>) -> Result<String>` | `ByRecord` | `ByValue` |
/// | `async fn(String) -> Result<KafkaPublish<String>>` | `ByValue` | `ByRecord` |
/// | `async fn(KafkaRecord<String>) -> Result<KafkaPublish<String>>` | `ByRecord` | `ByRecord` |
///
/// A handler whose shape the source or sink does not support fails to
/// compile. Closures need their parameter type annotated, because the shape is
/// chosen from it.
pub trait IntoHandler<S: Source, K, M>: Send + Sync + 'static {
    /// Output type the sink publishes.
    type Output: Send + Sync + 'static;
    /// Build a subscription that runs this handler.
    #[doc(hidden)]
    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output>;
}

impl<S, K, H, C> IntoHandler<S, K, (ByRecord, ByRecord, C)> for H
where
    S: Source,
    H: Handler<SourceItem<S>>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;

    fn into_subscription(self, name: String, source: S, sink: K) -> Subscription<S, K, C::Item> {
        let handler = Arc::new(self);
        Subscription::with_handler(name, source, sink, move |input| {
            let handler = handler.clone();
            async move { handler.handle(input).await.map(C::emit) }
        })
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByValue, ByRecord, C)> for H
where
    S: Source,
    SourceItem<S>: ValueRecord,
    H: Handler<<SourceItem<S> as ValueRecord>::Value>,
    C: Cardinality<H::Output>,
    K: Sink<C::Item>,
{
    type Output = C::Item;

    fn into_subscription(self, name: String, source: S, sink: K) -> Subscription<S, K, C::Item> {
        let handler = Arc::new(self);
        Subscription::with_handler(name, source, sink, move |record: SourceItem<S>| {
            let handler = handler.clone();
            async move { handler.handle(record.value()?).await.map(C::emit) }
        })
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByRecord, ByValue, C)> for H
where
    S: Source,
    H: Handler<SourceItem<S>>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output> {
        let handler = Arc::new(self);
        Subscription::with_handler(name, source, sink, move |input| {
            let handler = handler.clone();
            async move {
                let result = handler.handle(input).await?;
                Ok(C::emit(result).map(<SourceItem<S> as SamePlatform<C::Item>>::publish))
            }
        })
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}

impl<S, K, H, C> IntoHandler<S, K, (ByValue, ByValue, C)> for H
where
    S: Source,
    H: Handler<<SourceItem<S> as ValueRecord>::Value>,
    C: Cardinality<H::Output>,
    SourceItem<S>: SamePlatform<C::Item>,
    K: Sink<<SourceItem<S> as SamePlatform<C::Item>>::Publish>,
{
    type Output = <SourceItem<S> as SamePlatform<C::Item>>::Publish;

    fn into_subscription(
        self,
        name: String,
        source: S,
        sink: K,
    ) -> Subscription<S, K, Self::Output> {
        let handler = Arc::new(self);
        Subscription::with_handler(name, source, sink, move |record: SourceItem<S>| {
            let handler = handler.clone();
            async move {
                let result = handler.handle(record.value()?).await?;
                Ok(C::emit(result).map(<SourceItem<S> as SamePlatform<C::Item>>::publish))
            }
        })
        .middleware(<SourceItem<S> as SamePlatform<C::Item>>::Inherit::default())
    }
}
