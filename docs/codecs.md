# Codecs

Codecs convert between serialized bytes and typed values. They do not receive
messages, publish output, route metadata, or acknowledge deliveries. Those
responsibilities belong to [adapters](adapters.md) and the
[subscription runtime](runtime.md).

## Contracts

The framework defines separate decoding and encoding traits:

```rust
pub trait Decoder<T>: Send + Sync + 'static {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<T>;
}

pub trait Encoder<T>: Send + Sync + 'static {
    fn encode(&self, value: &T) -> anyhow::Result<Vec<u8>>;
}
```

A codec can implement either or both. These are synchronous transformations;
implementations should not perform blocking network or filesystem operations.
Expensive serialization still occupies the executing thread; there is currently
no dedicated codec execution pool.

The traits do not require Serde. Each codec chooses its own representation and
payload bounds. `Decoder`, `Encoder`, `Json`, `RawBytes`, and `Utf8` are available
through `beavers::codec` and re-exported at the crate root. `Protobuf` and `Avro`
are available when their corresponding Cargo features are enabled.

## JSON

`Json` uses `serde_json::from_slice` for decoding and `serde_json::to_vec` for
encoding.

| Operation | Payload requirement | Result |
|---|---|---|
| Decode | `serde::de::DeserializeOwned` | Owned typed value |
| Encode | `serde::Serialize` | Compact JSON bytes |

Json does not itself append a newline or split a stream into messages. Input
framing belongs to the source; output framing belongs to the sink. In particular,
StdinSource reads one line at a time and StdoutSink appends a newline after encoding.

### Using JSON with adapters

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
            StdinSource::<Json, _>::new(),
            StdoutSink::<Json>::new(),
            handler,
        )
        .run()
        .await
}
```

The second StdinSource type argument is the decoded input type, inferred here as
Order. Input values also need the runtime's `Clone + Send + Sync + 'static`
bounds; those are not additional requirements of the JSON format.

The [transform example](../examples/transform.rs) provides this pipeline. Feed it
one order object per line:

```sh
printf '%s\n' '{"id":10}' '{"id":20}' | cargo run --example transform -- --stdin
```

IterSource and InMemorySink already exchange typed values, so they require no
serialization codec.

## Lifecycle and failures

For the current stdin-to-stdout pipeline:

```text
receive raw line
    → SourceMessage::decode (Decoder)
    → handler
    → output middleware
    → Sink::prepare (Encoder + line framing)
    → Sink::publish
    → ACK
```

Decode runs once per received message before handler attempts. Handler retries
reuse cloned decoded input. The runtime prepares all emitted outputs before
publishing any; publish retries reuse the same prepared representation and do
not rerun encoding.

Codec errors return through `anyhow::Result`. Currently, a decode or preparation
failure stops processing without ACK; neither is automatically retried or sent
to the DLQ. More detailed classification is future design work. An invalid JSON
line is a decode failure, whereas a failed stdin read is a receive failure.

## Implementing a codec

Implement the relevant trait for the payload type. For example, a UTF-8 string
codec needs no Serde dependency:

```rust
use beavers::{Decoder, Encoder};

#[derive(Default)]
struct Utf8;

impl Decoder<String> for Utf8 {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<String> {
        Ok(std::str::from_utf8(bytes)?.to_owned())
    }
}

impl Encoder<String> for Utf8 {
    fn encode(&self, value: &String) -> anyhow::Result<Vec<u8>> {
        Ok(value.as_bytes().to_vec())
    }
}
```

`Utf8` is an exported built-in codec; see [Raw bytes and UTF-8](codecs/raw-utf8.md)
for its exact behavior and `RawBytes` for arbitrary payloads. The local stdin and
stdout adapters default-construct their codec, so using a custom codec with them
requires `Default`. The codec traits themselves do not require it. Kafka and
Pulsar adapters also provide `with_codec(config, codec)` for existing configured
codec instances.

Choose framing-compatible codecs for line-based adapters. A binary encoding can
contain newline bytes, and an arbitrary string may contain embedded newlines;
neither automatically forms a safe one-record-per-line protocol. The UTF-8
example preserves all bytes, including any newline supplied by the source.

## Available formats and broker integration

`RawBytes` preserves arbitrary `Vec<u8>` payloads and `Utf8` converts valid UTF-8
between bytes and `String`. See [Raw bytes and UTF-8](codecs/raw-utf8.md).

The optional `protobuf` feature provides `Protobuf` for raw `prost::Message`
payloads. The optional `avro` feature provides schema-bound `Avro` values. Both
formats leave framing to the adapter. See [Protobuf](codecs/protobuf.md) and
[Avro](codecs/avro.md).

Kafka applies its selected codec to a record value; keys and headers remain raw
bytes in `KafkaRecord<T>`, and a value can be null. Pulsar applies its selected
codec to a non-null byte payload while exposing key and properties as metadata.
Neither adapter implicitly carries received metadata into outgoing records. Typed
metadata mapping and automatic inheritance remain future work; see the
[serialization plan](plan.md#serialization-and-codecs).
