#![cfg(feature = "testing")]

#[path = "testing/deliveries.rs"]
mod deliveries;
#[cfg(feature = "kafka")]
#[path = "testing/kafka.rs"]
mod kafka;
#[cfg(feature = "pulsar")]
#[path = "testing/pulsar.rs"]
mod pulsar;
