//! Value-only handlers for sources and sinks that share a platform.
use crate::{handler::Result, middleware::Middleware};

/// A received record that exposes its payload to value-only handlers.
///
/// `value` returns `HandlerError::Reject` when the record has no payload that
/// can be represented as `Value`, such as a Kafka null value. A value-only
/// handler is not invoked for such a record.
pub trait ValueRecord: Clone + Send + Sync + 'static {
    type Value: Send + 'static;
    fn value(&self) -> Result<Self::Value>;
}

/// A received record whose platform also defines the publish type and its
/// default metadata inheritance.
///
/// Implemented by an adapter's record type for that adapter's publish type.
/// `Subscription::forward` builds each output with `publish` and registers
/// `Inherit::default()` as the first middleware, so the metadata policy is
/// chosen by the platform rather than by the handler.
pub trait SamePlatform<U>: ValueRecord {
    type Publish: Send + Sync + 'static;
    type Inherit: Middleware<Self, Self::Publish> + Default;
    fn publish(value: U) -> Self::Publish;
}
