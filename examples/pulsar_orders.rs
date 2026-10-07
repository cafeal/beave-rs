//! Pulsar order pipeline for the local development brokers in `compose.yaml`.
//!
//! ```sh
//! cargo run --example pulsar_orders --features pulsar -- produce 10
//! cargo run --example pulsar_orders --features pulsar -- process
//! ```
//!
//! `produce` publishes JSON orders keyed by customer to
//! `persistent://public/default/orders`. `process` consumes them with a
//! `Key_Shared` subscription and publishes one event per order to
//! `persistent://public/default/order-events` until interrupted with Ctrl-C.
//! Set `PULSAR_URL` to use a broker other than `pulsar://localhost:6650`.
//!
//! A new Pulsar subscription starts at the latest message. The local
//! environment creates the `beavers-examples` subscription at the earliest
//! position, so orders produced before the first `process` run are consumed.
use beavers::{
    App, IterSource, Json, Result, Subscription,
    adapters::pulsar::{
        PulsarPublish, PulsarSink, PulsarSinkConfig, PulsarSource, PulsarSourceConfig,
        PulsarSubscriptionType,
    },
};
use serde::{Deserialize, Serialize};
use std::env;

const ORDERS: &str = "persistent://public/default/orders";
const EVENTS: &str = "persistent://public/default/order-events";

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

async fn keyed_by_customer(order: Order) -> Result<PulsarPublish<Order>> {
    let key = order.customer.clone().into_bytes();
    let mut output = PulsarPublish::new(order);
    output.key = Some(key);
    output
        .properties
        .insert("source".into(), "pulsar_orders".into());
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
    let service_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
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
                    PulsarSink::<Json, Order>::new(PulsarSinkConfig::new(&service_url, ORDERS)),
                    keyed_by_customer,
                )
                .run()
                .await?;
            println!("published {count} orders to {ORDERS}");
            Ok(())
        }
        Some("process") | None => {
            let mut source = PulsarSourceConfig::new(&service_url, ORDERS, "beavers-examples");
            source.subscription_type = PulsarSubscriptionType::KeyShared;
            println!("consuming {ORDERS} and publishing to {EVENTS}; press Ctrl-C to stop");
            App::new()
                .subscription(
                    Subscription::new(
                        "process-orders",
                        PulsarSource::<Json, Order>::new(source),
                        PulsarSink::<Json, OrderEvent>::new(PulsarSinkConfig::new(
                            &service_url,
                            EVENTS,
                        )),
                        to_event,
                    )
                    .concurrency(3),
                )
                .run()
                .await
        }
        Some(other) => {
            anyhow::bail!("unknown command {other:?}; use `produce [count]` or `process`")
        }
    }
}
