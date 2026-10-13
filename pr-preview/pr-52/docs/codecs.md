# Codecs

Codecs convert between serialized bytes and typed values. They do not receive
messages, publish output, route metadata, or acknowledge deliveries. Those
responsibilities belong to [adapters](adapters.md) and the
[subscription runtime](runtime.md).

## Contracts

A codec names the typed value it converts, and implements decoding, encoding,
or both:

```rust
pub trait Codec: Send + Sync + 'static {
    type Value;
}

pub trait Decoder: Codec {
    fn decode(&self, bytes: &[u8]) -> Result<Self::Value, BoxError>;
}

pub trait Encoder: Codec {
    fn encode(&self, value: &Self::Value) -> Result<Vec<u8>, BoxError>;
}
```

Sources and sinks take their payload type from the codec, so the codec is their
only type parameter. A format that serializes many types carries the type as
its parameter, as in `Json<Order>`; a codec bound to one type, such as `Utf8`
for `String`, has none:

```rust,ignore
let orders: KafkaSource<Json<Order>> = KafkaSource::new(source_config);
let invoices: KafkaSink<Avro<Invoice>> = KafkaSink::new(sink_config);
let lines: KafkaSink<Utf8> = KafkaSink::new(lines_config);
```

When a handler parameter fixes the type, the codec parameter can be left to
inference, as in `StdinSource::<Json<_>>::new()`. These are synchronous transformations;
implementations should not perform blocking network or filesystem operations.
Expensive serialization still occupies the executing thread; there is currently
no dedicated codec execution pool.

The traits do not require Serde. Each codec chooses its own representation and
payload bounds. `Codec`, `Decoder`, `Encoder`, `Json`, `RawBytes`, and `Utf8` are available
through `beavers::codec` and re-exported at the crate root. `Protobuf`, `Avro`,
`MessagePack`, `Cbor`, and the `Base64`, `Gzip`, and `Zstd` wrappers are
available when their corresponding Cargo features are enabled.

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

Codec errors return as `BoxError`, so an implementation can propagate any error
type with `?`. Neither decode nor preparation failures are retried, because the
same bytes or value would fail again. By
default both stop processing without ACK; the subscription's
[error policy](runtime/errors.md#error-policy) can instead dead-letter or discard them
as `FailureKind::Decode` and `FailureKind::Encode`. An invalid JSON line is a
decode failure, whereas a failed stdin read is a receive failure.

## Implementing a codec

Implement `Codec` to name the value type, then the relevant direction. For
example, a UTF-8 string codec needs no Serde dependency:

```rust
use beavers::{BoxError, Codec, Decoder, Encoder};

#[derive(Default)]
struct Utf8;

impl Codec for Utf8 {
    type Value = String;
}

impl Decoder for Utf8 {
    fn decode(&self, bytes: &[u8]) -> Result<String, BoxError> {
        Ok(std::str::from_utf8(bytes)?.to_owned())
    }
}

impl Encoder for Utf8 {
    fn encode(&self, value: &String) -> Result<Vec<u8>, BoxError> {
        Ok(value.as_bytes().to_vec())
    }
}
```

A codec for a format that serializes many types takes the type as a parameter
and holds it in `PhantomData<fn() -> T>`, as `Json<T>` does, so the codec stays
`Send`, `Sync`, and `Copy` whatever `T` is.

`Utf8` is an exported built-in codec; see [Utf8](codecs/utf8.md)
for its exact behavior and [`RawBytes`](codecs/raw-bytes.md) for arbitrary payloads. The local stdin and
stdout adapters default-construct their codec, so using a custom codec with them
requires `Default`. The codec traits themselves do not require it. Kafka and
Pulsar adapters also provide `with_codec(config, codec)` for existing configured
codec instances.

Choose framing-compatible codecs for line-based adapters. A binary encoding can
contain newline bytes, and an arbitrary string may contain embedded newlines;
neither automatically forms a safe one-record-per-line protocol. The UTF-8
example preserves all bytes, including any newline supplied by the source.

## Available formats and broker integration

`Json` converts between JSON and any Serde type. See [JSON](codecs/json.md).

`RawBytes` preserves arbitrary `Vec<u8>` payloads and `Utf8` converts valid UTF-8
between bytes and `String`. See [RawBytes](codecs/raw-bytes.md) and
[Utf8](codecs/utf8.md).

The optional `protobuf` feature provides `Protobuf` for raw `prost::Message`
payloads. The optional `avro` feature provides schema-bound `Avro` values. Both
formats leave framing to the adapter. See [Protobuf](codecs/protobuf.md) and
[Avro](codecs/avro.md).

The optional `msgpack` and `cbor` features provide `MessagePack` and `Cbor`,
compact binary formats for the same Serde types as `Json`. See
[MessagePack](codecs/msgpack.md) and [CBOR](codecs/cbor.md).

The optional `base64`, `gzip`, and `zstd` features provide `Base64<C>`,
`Gzip<C>`, and `Zstd<C>`, which wrap another codec to carry its payload as text
or compress it. See [codec wrappers](codecs/wrappers.md).

Kafka applies its selected codec to a record value; keys and headers remain raw
bytes in `KafkaRecord<T>`, and a value can be null. Pulsar applies its selected
codec to a non-null payload while exposing key and properties as metadata; by
default an empty payload is a null value. A null value never reaches the codec.
Neither adapter implicitly carries received metadata into outgoing records.
Metadata mapping is explicit output middleware; see the
[runtime guide](runtime/middleware.md).
