//! Value-only handlers for sources and sinks that share a platform.
use crate::{handler::Result, middleware::Middleware};

/// A received record that exposes its payload to value-only handlers.
///
/// `value` returns `HandlerError::Reject` when the record has no payload that
/// can be represented as `Value`, such as a Kafka null value. A value-only
/// handler is not invoked for such a record.
pub trait ValueRecord: Clone + Send + Sync + 'static {
    /// Payload type passed to value-only handlers.
    type Value: Send + 'static;
    /// The record's payload.
    fn value(&self) -> Result<Self::Value>;
}

/// A received record whose platform also defines the publish type and its
/// default metadata inheritance.
///
/// Implemented by an adapter's record type for that adapter's publish type.
/// A [`ByValue`](crate::ByValue) handler output is built with `publish`, and
/// `Inherit::default()` is registered as the first middleware, so the metadata
/// policy is chosen by the platform rather than by the handler.
pub trait SamePlatform<U>: ValueRecord {
    /// The platform's publish type.
    type Publish: Send + Sync + 'static;
    /// Middleware that copies the platform's default metadata from the record to the output.
    type Inherit: Middleware<Self, Self::Publish> + Default;
    /// Build a publish carrying `value` and no metadata of its own.
    fn publish(value: U) -> Self::Publish;
}
