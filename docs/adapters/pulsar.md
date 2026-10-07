# Pulsar adapter

The optional `pulsar` feature provides typed Apache Pulsar source and sink
adapters backed by the `magnetar-driver` 1.7 client.

```toml
beavers = { version = "0.1", features = ["pulsar"] }
```

## Configuration

Configure a source with a service URL, topic, and subscription. `validate`
checks the values and optional static authentication without contacting a
broker. The source opens its client and consumer on the first `receive`. A
partitioned topic is consumed through one consumer per partition, all in the
same subscription; the partition list is read when the source connects.

```rust
use beavers::{Source, Utf8};
use beavers::adapters::pulsar::{PulsarSource, PulsarSourceConfig};

let mut source = PulsarSource::<Utf8, String>::new(PulsarSourceConfig::new(
    "pulsar://localhost:6650",
    "persistent://public/default/orders",
    "orders-workers",
));
```

`subscription_type` selects a `PulsarSubscriptionType` and defaults to
`Shared`. `buffer_size` is the number of messages each partition's consumer
prefetches. A new subscription starts at the latest message.

`PulsarAuthentication::token` supplies a JWT token, while the general
`PulsarAuthentication` form accepts a Pulsar authentication method name and
credential bytes. Leave `authentication` unset for an unauthenticated broker.

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
# Ok::<(), beavers::BoxError>(())
```

## Records

The codec applies to the payload. `PulsarMessage::decode` returns a
`PulsarRecord<T>` with top-level `value`, `key`, `properties`, and `event_time`
fields. These application fields map directly to `PulsarPublish<T>`. Its
`metadata` contains read-only delivery facts: topic, message ID, and publish
time. For a partitioned topic, the metadata topic is the partition's topic,
such as `persistent://public/default/orders-partition-2`. The source reads the
key, properties, event time, and null marker of a message inside a producer
batch from that message's own metadata.

The raw form of a `PulsarMessage` is a `PulsarRecord<Vec<u8>>` with the
undecoded payload bytes, or `value: None` for a tombstone. Dead letters carry
it, so the original payload, key, properties, event time, and delivery metadata
survive even when decoding fails.

## Acknowledgements and ordering

`receive` waits for the next message of any partition, starting with the
partition after the one that delivered last so that a busy partition cannot
starve the others. A message leaves its consumer's queue only when a receive
completes, so dropping a pending receive future does not consume a delivery.
Dropping a `PulsarMessage` leaves it unacknowledged. Call `ack` only after the
handler has completed successfully. An individual ACK is sent with the
message's ID and waits for the broker's response; the adapter does not use
cumulative acknowledgements. Broker receive errors are reported as
`ReceiveError::Retry`, while connection, configuration, and malformed message
metadata errors are `ReceiveError::Fatal`. The client reconnects after the
broker connection drops and reattaches its consumers.

A topic unload, as the broker's load balancer performs routinely, or a broker
restart closes the consumers. An acknowledgement in flight at that moment
fails, and one sent before the consumer has reattached times out after the
client's 30-second operation timeout. `ack` therefore retries a failed
acknowledgement under `PulsarSourceConfig::ack_retry`, five attempts with
backoff from 100 ms to 2 s by default, and an attempt on the reattached
consumer succeeds. Only an exhausted retry fails the acknowledgement and stops
the subscription. After reattaching, the broker redelivers every
unacknowledged message, including messages the consumer had already
prefetched and still returns, so deliveries received around an unload can be
processed twice and out of their original order.

Call `close` during shutdown to close the consumers. A delivery acknowledged
after its source closed fails with an error, and the client closes once the
last delivery of the source is dropped.

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

## Publication

The sink connects lazily on the first `publish`, creating one producer per
partition of a partitioned topic. A keyed message is routed like the Java
client's default: the Java `String.hashCode` of its base64 key, modulo the
partition count, so the same key always reaches the same partition. Keyless
messages rotate across partitions. Keys are always sent in Pulsar's base64
representation because they are arbitrary bytes. Each submission clones the
prepared bytes and metadata into a fresh producer message. Retrying a prepared
value does not rerun the codec.

`PulsarSink::submit` returns once the partition's producer has queued the
message, with a completion that resolves on Pulsar's broker receipt. The
runtime frees the job's concurrency slot at submission and acknowledges the
input when the receipt arrives. `publish` queues the message and waits for the
receipt. A producer sends queued messages in order and replays unacknowledged
ones in order after a reconnect, so outputs of one partition keep their
submission order unless the broker rejects a send, as described below.

`PulsarSinkConfig::max_pending` bounds the messages queued by `submit` whose
receipt has not arrived; the default is 1000. `submit` waits while that many
are outstanding. A completion dropped before its receipt, as when the input is
abandoned, frees its slot; the message stays queued and may still be written,
but it is not sent again after a rejection. Publish retries apply to queueing
only.

The client replays sends that lose their connection. A send the broker answers
with an error is not replayed by the client, and a broker that is shutting down
or unloading the topic, as during a restart or a routine load-balancer unload,
rejects the sends in flight with a persistence error. The sink therefore sends
a rejected message again under `PulsarSinkConfig::send_retry`, ten sends with
backoff from 100 ms to 5 s by default, measured from the previous send. A task
per producer observes the receipts in submission order, so the messages of one
rejected run are sent again in their original order and reach the topic's next
owner. Like the Java client, the sink does not send a message again after a
`NotAllowedError` or `TopicTerminatedError` rejection. A send error that is
not sent again, or a message rejected on every attempt, fails the completion,
which stops the subscription without acknowledging the input.

A message sent again is written after later messages of its partition that the
client replayed or the broker accepted in the meantime, so a rejection can
reorder the outputs of one partition. A message the broker wrote despite
answering with an error is written twice.

`close` stops new submissions, waits for every outstanding receipt, then closes
the producers.

## Metadata inheritance

Register `PulsarInherit` on a Pulsar-to-Pulsar subscription to forward the
received key, properties, and event time:

```rust,ignore
use beavers::adapters::pulsar::PulsarInherit;

Subscription::new("orders", pulsar_source, pulsar_sink, handler)
    .middleware(PulsarInherit::new())
```

Explicit output fields take precedence. The key and event time are inherited
only when the output leaves them `None`, and a received property is added only
when the output does not already set that name. Dead-letter properties starting
with `beavers-dlq-` are never inherited. `without_key()`,
`without_properties()`, and `without_event_time()` disable the corresponding
field. The source topic, message ID, and publish time are never inherited, and
no ordering key is derived from the input.

A handler that returns a plain value between a Pulsar source and sink gets
`PulsarInherit::new()` automatically. See the
[runtime guide](../runtime.md#handler-shapes).

Inherited properties include trace-context properties such as `traceparent`.
Register `TraceContext` after `PulsarInherit` to replace them with the
processing span's context; see
[trace-context propagation](../runtime.md#trace-context-propagation).

## Tombstones

A null value is a tombstone. Pulsar topic compaction deletes a key when it sees
a message with that key and an empty payload, and producers can also mark a
value as null explicitly. `PulsarSourceConfig::empty_payload_is_tombstone`,
enabled by default, follows the compaction rule: an empty payload or a
null-marked message becomes `PulsarRecord { value: None, .. }` and is not passed
to the codec. When disabled, only a null-marked message becomes `None`, and an
empty payload is decoded by the codec. Disable it only for topics that carry
meaningful empty values and are not compacted.

The sink publishes `PulsarPublish { value: None, .. }` as an empty payload with
the null marker set, so readers see a null value with or without
`empty_payload_is_tombstone`. `PulsarPublish::tombstone(key)` builds one, and
`Tombstones::propagate()` supports Pulsar-to-Pulsar forwarding. The sink publishes non-null values as
encoded, even when the encoding is empty; readers that treat empty payloads as
tombstones, including topic compaction, will read such a value as a deletion.
See [tombstones](../runtime.md#tombstones).

## Dead letters

`PulsarPublish::from_dead_letter` forwards a dead letter to a Pulsar topic with
its original message and failure properties, and
`PulsarDeadLetter::from_record` reads them back; see
[forwarding dead letters](../runtime.md#forwarding-dead-letters-to-a-broker-topic).

## Delivery guarantees

A source ACK and a sink publication are separate broker operations, so a
process failure between them can produce a duplicate on redelivery. The
adapter provides no Pulsar transactions: `magnetar-driver` 1.7 mishandles
transactional acknowledgements of batched messages and transactions after a
broker restart, so Pulsar pipelines are at-least-once.
