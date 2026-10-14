# JSON codec

`Json<T>` converts between JSON payloads and a type `T` implementing Serde's
traits. It holds no state, implements `Default` for every `T`, and is always
available; it needs no Cargo feature. Decode into `serde_json::Value` when the
payload has no fixed schema; see [JSON without a schema](#json-without-a-schema).

`Json` uses `serde_json::from_slice` for decoding and `serde_json::to_vec` for
encoding.

| Operation | Payload requirement | Result |
|---|---|---|
| Decode | `serde::de::DeserializeOwned` | Owned typed value |
| Encode | `serde::Serialize` | Compact JSON bytes |

Json does not itself append a newline or split a stream into messages. Input
framing belongs to the source; output framing belongs to the sink. In particular,
StdinSource reads one line at a time and StdoutSink appends a newline after encoding.

## Using JSON with adapters

Select the codec through the adapter's type parameter. The handler determines
the input and output payload types:

```rust
use beavers::{App, Json, Result, StdinSource, StdoutSink};
use serde::{Deserialize, Serialize};

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
    App::new()
        .subscribe(
            "orders",
            StdinSource::<Json<_>>::new(),
            StdoutSink::<Json<_>>::new(),
            handler,
        )
        .run()
        .await?;
    Ok(())
}
```

The `_` in `Json<_>` is the payload type, inferred here from the handler as
`Order` for the source and `Event` for the sink. Input values also need the runtime's `Clone + Send + Sync + 'static`
bounds; those are not additional requirements of the JSON format.

The [transform example](https://github.com/cafeal/beave-rs/blob/main/examples/transform.rs) provides this pipeline. Feed it
one order object per line:

```sh
printf '%s\n' '{"id":10}' '{"id":20}' | cargo run --example transform -- --stdin
```

IterSource and InMemorySink already exchange typed values, so they require no
serialization codec.

## JSON without a schema

When payloads have no fixed shape, decode into `serde_json::Value` and read the
fields the handler needs. Any valid JSON decodes; only malformed JSON is a
decode failure. Import `Value` so the codec type stays short:

```rust,ignore
use beavers::{App, Json, Result, StdoutSink, adapters::kafka::Kafka};
use serde_json::Value;

async fn audit(event: Value) -> Result<Value> {
    let kind = event["type"].as_str().unwrap_or("unknown");
    Ok(serde_json::json!({ "type": kind }))
}

let kafka = Kafka::new("localhost:9092");
App::new()
    .subscribe(
        "audit",
        kafka.source::<Json<Value>>("audit", ["events"]),
        StdoutSink::<Json<Value>>::new(),
        audit,
    )
    .run()
    .await?;
```

A `Json<Value>` sink accepts any `Value`, so a handler can also build output
with `serde_json::json!` without declaring a struct.
