#![cfg(feature = "testing")]

#[cfg(feature = "kafka")]
#[path = "dead_letter/kafka.rs"]
mod kafka;
#[cfg(feature = "pubsub")]
#[path = "dead_letter/pubsub.rs"]
mod pubsub;
#[cfg(feature = "pulsar")]
#[path = "dead_letter/pulsar.rs"]
mod pulsar;
#[cfg(feature = "rabbitmq")]
#[path = "dead_letter/rabbitmq.rs"]
mod rabbitmq;
#[cfg(feature = "servicebus")]
#[path = "dead_letter/servicebus.rs"]
mod servicebus;
#[cfg(feature = "sqs")]
#[path = "dead_letter/sqs.rs"]
mod sqs;
