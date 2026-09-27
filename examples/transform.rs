use beavers::{App, IterSource, Json, Result, StdinSource, StdoutSink};
use serde::{Deserialize, Serialize};
use std::env;
#[derive(Clone, Deserialize)]
struct Order {
    id: u64,
}
#[derive(Serialize)]
struct Event {
    order_id: u64,
}
async fn handler(order: Order) -> Result<Event> {
    Ok(Event { order_id: order.id })
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if env::args().any(|arg| arg == "--stdin") {
        App::new()
            .subscribe(
                StdinSource::<Json, _>::new(),
                StdoutSink::<Json>::new(),
                handler,
            )
            .run()
            .await
    } else {
        App::new()
            .subscribe(
                IterSource::new([Order { id: 1 }, Order { id: 2 }]),
                StdoutSink::<Json>::new(),
                handler,
            )
            .run()
            .await
    }
}
