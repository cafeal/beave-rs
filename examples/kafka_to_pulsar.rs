//! Bridges Kafka orders to Pulsar with an explicit metadata mapping.
//!
//! ```sh
//! cargo run --example kafka_orders --features kafka -- produce 10
//! cargo run --example kafka_to_pulsar --features kafka,pulsar
//! ```
//!
//! Consumes the Kafka `orders` topic and republishes each order to
//! `persistent://public/default/orders-from-kafka` until interrupted with
//! Ctrl-C. The Kafka key becomes the Pulsar key, and Kafka headers with UTF-8
//! values become Pulsar properties; beavers never maps metadata between
//! platforms implicitly. Set `KAFKA_BROKERS` and `PULSAR_URL` to use other
//! brokers.
use beavers::{
    App, Error, Json, MapMetadata, Result, Subscription, Tombstones,
    adapters::{
        kafka::{KafkaRecord, KafkaSource, KafkaSourceConfig},
        pulsar::{PulsarPublish, PulsarSink, PulsarSinkConfig},
    },
};
use serde::{Deserialize, Serialize};
use std::{env, result::Result as StdResult};

const KAFKA_TOPIC: &str = "orders";
const PULSAR_TOPIC: &str = "persistent://public/default/orders-from-kafka";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: u64,
}

async fn republish(record: KafkaRecord<Order>) -> Result<PulsarPublish<Order>> {
    let metadata = record.metadata();
    println!(
        "bridging {}[{}]@{}",
        metadata.topic, metadata.partition, metadata.offset
    );
    let order = record
        .value
        .ok_or("tombstones are skipped before the handler")?;
    Ok(PulsarPublish::new(order))
}

fn map_metadata(
    input: &KafkaRecord<Order>,
    mut output: PulsarPublish<Order>,
) -> Result<PulsarPublish<Order>> {
    output.key = input.key.clone();
    for (name, value) in &input.headers {
        if let Some(value) = value
            .as_deref()
            .and_then(|value| str::from_utf8(value).ok())
        {
            output.properties.insert(name.clone(), value.to_owned());
        }
    }
    output
        .properties
        .insert("kafka-topic".into(), input.metadata().topic.clone());
    Ok(output)
}

#[tokio::main]
async fn main() -> StdResult<(), Error> {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let service_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());

    let mut source = KafkaSourceConfig::new(&brokers, "beavers-bridge", [KAFKA_TOPIC]);
    source
        .properties
        .insert("auto.offset.reset".into(), "earliest".into());
    println!("bridging Kafka {KAFKA_TOPIC} to Pulsar {PULSAR_TOPIC}; press Ctrl-C to stop");
    App::new()
        .subscription(
            Subscription::new(
                "kafka-to-pulsar",
                KafkaSource::<Json, Order>::new(source),
                PulsarSink::<Json, Order>::new(PulsarSinkConfig::new(&service_url, PULSAR_TOPIC)),
                republish,
            )
            .middleware(Tombstones::skip())
            .middleware(MapMetadata::new(map_metadata)),
        )
        .run()
        .await
}
