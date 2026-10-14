#[cfg(all(feature = "kafka", feature = "testing"))]
#[path = "metadata/kafka.rs"]
mod kafka;
#[path = "metadata/mapping.rs"]
mod mapping;
#[cfg(all(feature = "pubsub", feature = "testing"))]
#[path = "metadata/pubsub.rs"]
mod pubsub;
#[cfg(all(feature = "pulsar", feature = "testing"))]
#[path = "metadata/pulsar.rs"]
mod pulsar;
#[cfg(all(feature = "rabbitmq", feature = "testing"))]
#[path = "metadata/rabbitmq.rs"]
mod rabbitmq;
#[cfg(all(feature = "servicebus", feature = "testing"))]
#[path = "metadata/servicebus.rs"]
mod servicebus;
#[cfg(all(feature = "sqs", feature = "testing"))]
#[path = "metadata/sqs.rs"]
mod sqs;
