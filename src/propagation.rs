//! Text-map fields that carry trace context, such as W3C `traceparent`, across brokers.
//!
//! A source exposes the fields it received through
//! [`SourceMessage::propagation_fields`](crate::message::SourceMessage::propagation_fields),
//! and an output type accepts fields through [`PropagationCarrier`]. Each adapter maps
//! fields to its own metadata model, such as Kafka headers or Pulsar properties.

/// An output record that can carry text-map propagation fields.
///
/// Setting a field replaces any value the record already carries under the
/// same name, so injected trace context supersedes context copied from the
/// input by inheritance middleware.
pub trait PropagationCarrier {
    /// Set the field `name` to `value`.
    fn set_propagation_field(&mut self, name: &str, value: String);
}
