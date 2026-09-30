# Pulsar adapter

The optional `pulsar` feature provides typed Apache Pulsar source and sink
adapters backed by `pulsar` 6.9.

```toml
beavers = { version = "0.1", features = ["pulsar"] }
```

Configure a source with a service URL, topic, and subscription. `validate`
checks the values and optional static authentication without contacting a
broker. The source opens its client and consumer on the first `receive`.

```rust
use beavers::{Source, Utf8};
use beavers::adapters::pulsar::{PulsarSource, PulsarSourceConfig};

let mut source = PulsarSource::<Utf8, String>::new(PulsarSourceConfig::new(
    "pulsar://localhost:6650",
    "persistent://public/default/orders",
    "orders-workers",
));
```

`PulsarAuthentication::token` supplies a JWT token, while the general
`PulsarAuthentication` form accepts a Pulsar authentication method name and
credential bytes. Leave `authentication` unset for an unauthenticated broker.

The codec applies to the payload. `PulsarMessage::decode` returns a
`PulsarRecord<T>` with top-level `value`, `key`, `properties`, and `event_time`
fields. These application fields map directly to `PulsarPublish<T>`. Its
`metadata` contains read-only delivery facts: topic, message ID, and publish
time.

A null value is a tombstone. Pulsar topic compaction deletes a key when it sees
a message with that key and an empty payload, and producers can also mark a
value as null explicitly. `empty_payload_is_tombstone`, enabled by default on
both `PulsarSourceConfig` and `PulsarSinkConfig`, follows the compaction rule:

| Setting | Source | Sink |
|---|---|---|
| `true` (default) | An empty payload or a null-marked message becomes `value: None` and is not passed to the codec | `value: None` publishes an empty payload; a value that encodes to an empty payload is refused |
| `false` | Only a null-marked message becomes `value: None`; an empty payload is decoded by the codec | `value: None` is refused; empty encoded values are published |

Disable the setting only for topics that carry meaningful empty values and are
not compacted. The `pulsar` 6.9 client cannot publish the null marker, and it
drops the marker for messages inside a producer batch, which then arrive as
empty payloads. With the default setting both cases still round-trip as
tombstones; with it disabled, a batched null value reaches the codec as empty
input. `PulsarPublish::tombstone(key)` builds a tombstone, and
`Tombstones::propagate()` supports Pulsar-to-Pulsar forwarding. See
[tombstones](../runtime.md#tombstones).

The source owns a dedicated consumer task. `receive` only waits on a bounded
delivery channel, so dropping a pending receive future does not consume a
delivery. The task also owns acknowledgements and can process an ACK while the
consumer is waiting for the next message or while delivery buffering is full.
Dropping a `PulsarMessage` leaves it unacknowledged. Call `ack` only after the
handler has completed successfully. An individual ACK is sent with the
message's ID; the adapter does not use cumulative acknowledgements. Broker
stream errors are reported as `ReceiveError::Retry`, while connection,
configuration, and malformed message metadata errors are `ReceiveError::Fatal`.
Call `close` during shutdown to close the consumer task cleanly.

Configure a sink separately. `prepare` accepts a `PulsarPublish<T>`, encodes
the typed value once, and returns a cloneable `PulsarPrepared` payload. Set the
key, ordering key, event time, and user properties before preparation:

```rust
use beavers::{Sink, Utf8};
use beavers::adapters::pulsar::{PulsarPublish, PulsarSink, PulsarSinkConfig};

let sink = PulsarSink::<Utf8, String>::new(PulsarSinkConfig::new(
    "pulsar://localhost:6650",
    "persistent://public/default/processed-orders",
));
let mut output = PulsarPublish::new("processed".to_owned());
output.key = Some(b"customer-42".to_vec());
output.properties.insert("kind".into(), "order".into());
let prepared = sink.prepare(output)?;
sink.publish(&prepared).await?;
# Ok::<(), anyhow::Error>(())
```

The sink connects lazily on the first `publish`. Each publish clones the
prepared bytes and metadata into a fresh producer message, then waits for
Pulsar's broker receipt. Retrying a prepared value does not rerun the codec.
`close` closes the producer after in-flight publication has released it.

## Metadata inheritance

Register `PulsarInherit` on a Pulsar-to-Pulsar subscription to forward the
received key, properties, and event time:

```rust,ignore
use beavers::adapters::pulsar::PulsarInherit;

Subscription::new(pulsar_source, pulsar_sink, handler)
    .middleware(PulsarInherit::new())
```

Explicit output fields take precedence. The key and event time are inherited
only when the output leaves them `None`, and a received property is added only
when the output does not already set that name. `without_key()`,
`without_properties()`, and `without_event_time()` disable the corresponding
field. The source topic, message ID, and publish time are never inherited, and
no ordering key is derived from the input.

`Subscription::forward` applies `PulsarInherit::new()` automatically for a
value-only handler between a Pulsar source and sink. See the
[runtime guide](../runtime.md#same-platform-forwarding).

## Delivery guarantees

The adapter does not claim transactions or exactly-once processing. A source
ACK and a sink publication are separate broker operations, so a process failure
between them can produce a duplicate on redelivery.
