#![cfg(feature = "testing")]

#[cfg(feature = "kafka")]
#[path = "dead_letter/kafka.rs"]
mod kafka;
#[cfg(feature = "pulsar")]
#[path = "dead_letter/pulsar.rs"]
mod pulsar;
