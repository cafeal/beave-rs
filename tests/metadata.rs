#[cfg(feature = "kafka")]
#[path = "metadata/kafka.rs"]
mod kafka;
#[path = "metadata/mapping.rs"]
mod mapping;
#[cfg(feature = "pubsub")]
#[path = "metadata/pubsub.rs"]
mod pubsub;
#[cfg(feature = "pulsar")]
#[path = "metadata/pulsar.rs"]
mod pulsar;
#[cfg(feature = "rabbitmq")]
#[path = "metadata/rabbitmq.rs"]
mod rabbitmq;
#[cfg(feature = "servicebus")]
#[path = "metadata/servicebus.rs"]
mod servicebus;
#[cfg(feature = "sqs")]
#[path = "metadata/sqs.rs"]
mod sqs;
