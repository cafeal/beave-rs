//! Kafka to HTTP delivery for the local development brokers in `compose.yaml`.
//!
//! ```sh
//! cargo run --example kafka_to_http --features kafka,http -- receive
//! cargo run --example kafka_orders --features kafka -- produce 10
//! cargo run --example kafka_to_http --features kafka,http -- forward
//! ```
//!
//! `receive` runs an HTTP server on `127.0.0.1:8090` that prints each order it
//! is sent. `forward` consumes the `orders` topic and sends each order to that
//! server with an idempotency key built from the record's position, committing
//! the offset only after the server answered with a success status. Set
//! `KAFKA_BROKERS` or `HTTP_SINK_URL` to use other addresses.
use beavers::{
    App, BoxError, InMemorySink, Json, Result, Subscription,
    adapters::{
        http::{HttpPublish, HttpRecord, HttpSink, HttpSinkConfig, HttpSource, HttpSourceConfig},
        kafka::{KafkaRecord, KafkaSource, KafkaSourceConfig},
    },
};
use serde::{Deserialize, Serialize};
use std::{env, result::Result as StdResult};

const ORDERS: &str = "orders";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: u64,
}

async fn to_request(record: KafkaRecord<Order>) -> Result<HttpPublish<Order>> {
    let metadata = record.metadata().clone();
    let order = record.value.ok_or("tombstones are not forwarded")?;
    let key = format!(
        "{}-{}-{}",
        metadata.topic, metadata.partition, metadata.offset
    );
    Ok(HttpPublish::new(order).with_header("idempotency-key", key))
}

async fn print(record: HttpRecord<Order>) -> Result<()> {
    let key = record
        .header("idempotency-key")
        .map(String::from_utf8_lossy)
        .unwrap_or_default();
    println!("received {:?} with idempotency key {key}", record.body);
    Ok(())
}

#[tokio::main]
async fn main() -> StdResult<(), BoxError> {
    match env::args().nth(1).as_deref() {
        Some("receive") => {
            let source =
                HttpSource::<Json, Order>::new(HttpSourceConfig::new("127.0.0.1:8090".parse()?))?;
            println!("listening on {}; press Ctrl-C to stop", source.local_addr());
            App::new()
                .subscribe(
                    "receive-orders",
                    source,
                    InMemorySink::<()>::default(),
                    print,
                )
                .run()
                .await?;
            Ok(())
        }
        Some("forward") | None => {
            let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
            let url =
                env::var("HTTP_SINK_URL").unwrap_or_else(|_| "http://127.0.0.1:8090/orders".into());
            let mut source = KafkaSourceConfig::new(&brokers, "beavers-examples-http", [ORDERS]);
            source
                .properties
                .insert("auto.offset.reset".into(), "earliest".into());
            let mut sink = HttpSinkConfig::new(&url);
            sink.headers
                .push(("content-type".into(), "application/json".into()));
            println!("consuming {ORDERS} and sending to {url}; press Ctrl-C to stop");
            App::new()
                .subscription(
                    Subscription::new(
                        "forward-orders",
                        KafkaSource::<Json, Order>::new(source),
                        HttpSink::<Json, Order>::new(sink)?,
                        to_request,
                    )
                    .concurrency(3),
                )
                .run()
                .await?;
            Ok(())
        }
        Some(other) => Err(format!("unknown command {other:?}; use `receive` or `forward`").into()),
    }
}
