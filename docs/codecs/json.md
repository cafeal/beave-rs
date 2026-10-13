# JSON codec

`Json<T>` converts between JSON payloads and a type `T` implementing Serde's
traits. It holds no state, implements `Default` for every `T`, and is always
available; it needs no Cargo feature. Decode into `serde_json::Value` when the payload has
no fixed schema.

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

Select the codec through the adapter's type parameters. The handler determines
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

The second StdinSource type argument is the decoded input type, inferred here as
Order. Input values also need the runtime's `Clone + Send + Sync + 'static`
bounds; those are not additional requirements of the JSON format.

The [transform example](../../examples/transform.rs) provides this pipeline. Feed it
one order object per line:

```sh
printf '%s\n' '{"id":10}' '{"id":20}' | cargo run --example transform -- --stdin
```

IterSource and InMemorySink already exchange typed values, so they require no
serialization codec.
