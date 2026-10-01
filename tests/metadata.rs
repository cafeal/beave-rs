#[cfg(feature = "kafka")]
#[path = "metadata/kafka.rs"]
mod kafka;
#[path = "metadata/mapping.rs"]
mod mapping;
#[cfg(feature = "pulsar")]
#[path = "metadata/pulsar.rs"]
mod pulsar;
#[cfg(feature = "rabbitmq")]
#[path = "metadata/rabbitmq.rs"]
mod rabbitmq;
