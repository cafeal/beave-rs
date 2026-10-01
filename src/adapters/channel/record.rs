//! The raw form and prepared output of channel deliveries.
use super::queue::Origin;
use serde::{Serialize, Serializer};
use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

/// The raw form of a channel delivery: the undecoded upstream delivery whose
/// handler produced the value, such as a `KafkaRecord<Vec<u8>>`.
///
/// Dead letters of a downstream subscription carry it, so they keep the
/// original payload and broker metadata. Through several channels it stays the
/// delivery of the first subscription. It is empty for values sent by
/// application code through a [`ChannelSender`](crate::ChannelSender) or [`Sink::publish`](crate::Sink::publish).
///
/// It serializes as the upstream raw form, or as `None` when empty. Use
/// [`downcast_ref`](Self::downcast_ref) to read it as its concrete type.
#[derive(Clone, Default)]
pub struct ChannelRaw(Option<Arc<dyn Raw>>);

/// A type-erased [`SourceMessage::Raw`].
trait Raw: erased_serde::Serialize + Any + Debug + Send + Sync {}

impl<R: Serialize + Any + Debug + Send + Sync> Raw for R {}

erased_serde::serialize_trait_object!(Raw);

impl ChannelRaw {
    pub(super) fn of<R: Serialize + Debug + Send + Sync + 'static>(raw: R) -> Self {
        match (&raw as &dyn Any).downcast_ref::<Self>() {
            // A delivery of another channel already carries the original form.
            Some(raw) => raw.clone(),
            None => Self(Some(Arc::new(raw))),
        }
    }

    /// The upstream raw form, if there is one and it is an `R`.
    pub fn downcast_ref<R: 'static>(&self) -> Option<&R> {
        let raw: &dyn Any = self.0.as_deref()?;
        raw.downcast_ref()
    }

    /// Whether the value was sent without an upstream delivery.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl Debug for ChannelRaw {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(raw) => raw.fmt(f),
            None => f.write_str("None"),
        }
    }
}

impl Serialize for ChannelRaw {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Some(raw) => raw.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

/// A value prepared for a [`ChannelSink`](crate::ChannelSink), with the ordering key and raw form
/// of the delivery that produced it.
///
/// Application code calling [`Sink::publish`](crate::Sink::publish) directly can convert a value into
/// one without an upstream delivery, as [`Sink::prepare`](crate::Sink::prepare) does.
#[derive(Clone)]
pub struct ChannelOutput<T> {
    pub(super) value: T,
    pub(super) origin: Origin,
}

impl<T> From<T> for ChannelOutput<T> {
    fn from(value: T) -> Self {
        Self {
            value,
            origin: Origin::default(),
        }
    }
}
