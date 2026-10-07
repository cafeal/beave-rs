//! Kafka order pipeline for the local development brokers in `compose.yaml`.
//!
//! ```sh
//! cargo run --example kafka_orders --features kafka -- produce 10
//! cargo run --example kafka_orders --features kafka -- process
//! ```
//!
//! `produce` publishes JSON orders keyed by customer to the `orders` topic.
//! `process` consumes `orders` and publishes one event per order to
//! `order-events` until interrupted with Ctrl-C. Set `KAFKA_BROKERS` to use a
//! broker other than `localhost:9092`.
use beavers::{
    App, IterSource, Json, Result, Subscription,
    adapters::kafka::{KafkaPublish, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig},
};
use serde::{Deserialize, Serialize};
use std::env;

const ORDERS: &str = "orders";
const EVENTS: &str = "order-events";

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    amount_cents: u64,
}

#[derive(Debug, Serialize)]
struct OrderEvent {
    order_id: u64,
    customer: String,
    amount: String,
}

async fn keyed_by_customer(order: Order) -> Result<KafkaPublish<Order>> {
    let mut output = KafkaPublish::new(order);
    output.key = output
        .value
        .as_ref()
        .map(|order| order.customer.clone().into_bytes());
    Ok(output)
}

async fn to_event(order: Order) -> Result<OrderEvent> {
    println!("processing {order:?}");
    Ok(OrderEvent {
        order_id: order.id,
        amount: format!(
            "{}.{:02}",
            order.amount_cents / 100,
            order.amount_cents % 100
        ),
        customer: order.customer,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("produce") => {
            let count = args
                .next()
                .map(|count| count.parse())
                .transpose()?
                .unwrap_or(10);
            let orders = (1..=count).map(|id| Order {
                id,
                customer: format!("customer-{}", id % 3),
                amount_cents: id * 1250,
            });
            App::new()
                .subscribe(
                    "produce-orders",
                    IterSource::new(orders),
                    KafkaSink::<Json, Order>::new(KafkaSinkConfig::new(&brokers, ORDERS)),
                    keyed_by_customer,
                )
                .run()
                .await?;
            println!("published {count} orders to {ORDERS}");
            Ok(())
        }
        Some("process") | None => {
            let mut source = KafkaSourceConfig::new(&brokers, "beavers-examples", [ORDERS]);
            source
                .properties
                .insert("auto.offset.reset".into(), "earliest".into());
            println!("consuming {ORDERS} and publishing to {EVENTS}; press Ctrl-C to stop");
            App::new()
                .subscription(
                    Subscription::new(
                        "process-orders",
                        KafkaSource::<Json, Order>::new(source),
                        KafkaSink::<Json, OrderEvent>::new(KafkaSinkConfig::new(&brokers, EVENTS)),
                        to_event,
                    )
                    .concurrency(3),
                )
                .run()
                .await?;
            Ok(())
        }
        Some(other) => {
            anyhow::bail!("unknown command {other:?}; use `produce [count]` or `process`")
        }
    }
}
