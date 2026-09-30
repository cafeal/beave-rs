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
time. Pulsar payloads are byte vectors, so an empty payload remains an empty
byte vector rather than a nullable value. Pulsar does not have Kafka-style
tombstones.

The raw form of a `PulsarMessage` is a `PulsarRecord<Vec<u8>>` with the
undecoded payload bytes. Dead letters carry it, so the original payload, key,
properties, event time, and delivery metadata survive even when decoding fails.

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

Each delivery's ordering key follows the order Pulsar guarantees for the
configured subscription type:

| Subscription type | Ordering key |
|---|---|
| `Exclusive`, `Failover` | Topic partition |
| `Key_Shared` | Ordering key, or the message key when no ordering key is set, within the topic partition |
| `Shared` | None |

Under the default `ProcessingOrder::PerKey`, deliveries with the same ordering
key are processed one at a time in receive order. `Key_Shared` messages without
a key and all `Shared` deliveries run in parallel up to the subscription's
`concurrency`. The Pulsar client does not report consumer failover or key-range
reassignment, so Pulsar deliveries have no revocation token. When the broker
moves a subscription to another consumer, unacknowledged messages are
redelivered there and can be processed twice.

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

The adapter does not claim transactions or exactly-once processing. A source
ACK and a sink publication are separate broker operations, so a process failure
between them can produce a duplicate on redelivery.
