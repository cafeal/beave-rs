# Pulsar adapter

The optional `pulsar` feature provides typed Apache Pulsar source and sink
adapters backed by the `magnetar-driver` 1.7 client.

```toml
beavers = { version = "0.1", features = ["pulsar"] }
```

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

The codec applies to the payload. `PulsarMessage::decode` returns a
`PulsarRecord<T>` with top-level `value`, `key`, `properties`, and `event_time`
fields. These application fields map directly to `PulsarPublish<T>`. Its
`metadata` contains read-only delivery facts: topic, message ID, and publish
time. For a partitioned topic, the metadata topic is the partition's topic,
such as `persistent://public/default/orders-partition-2`.

A null value is a tombstone. Pulsar topic compaction deletes a key when it sees
a message with that key and an empty payload, and producers can also mark a
value as null explicitly. `PulsarSourceConfig::empty_payload_is_tombstone`,
enabled by default, follows the compaction rule: an empty payload or a
null-marked message becomes `PulsarRecord { value: None, .. }` and is not passed
to the codec. When disabled, only a null-marked message becomes `None`, and an
empty payload is decoded by the codec. Disable it only for topics that carry
meaningful empty values and are not compacted.

The source reads the key, properties, event time, and null marker of a message
inside a producer batch from that message's own metadata. The sink publishes
`PulsarPublish { value: None, .. }` as an empty payload with the null marker
set, so readers see a null value with or without
`empty_payload_is_tombstone`. `PulsarPublish::tombstone(key)` builds one, and `Tombstones::propagate()`
supports Pulsar-to-Pulsar forwarding. The sink publishes non-null values as
encoded, even when the encoding is empty; readers that treat empty payloads as
tombstones, including topic compaction, will read such a value as a deletion.
See [tombstones](../runtime.md#tombstones).

The raw form of a `PulsarMessage` is a `PulsarRecord<Vec<u8>>` with the
undecoded payload bytes, or `value: None` for a tombstone. Dead letters carry it, so the original payload, key,
properties, event time, and delivery metadata survive even when decoding fails.

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

The sink connects lazily on the first `publish`, creating one producer per
partition of a partitioned topic. A keyed message is routed like the Java
client's default: the Java `String.hashCode` of its base64 key, modulo the
partition count, so the same key always reaches the same partition. Keyless
messages rotate across partitions. Keys are always sent in Pulsar's base64
representation because they are arbitrary bytes. Each submission clones the
prepared bytes and metadata into a fresh producer message. Retrying a prepared
value does not rerun the codec.

## Publication

`PulsarSink::submit` returns once the partition's producer has queued the
message, with a completion that resolves on Pulsar's broker receipt. The
runtime frees the job's concurrency slot at submission and acknowledges the
input when the receipt arrives. `publish` queues the message and waits for the
receipt. A producer sends queued messages in order and replays unacknowledged
ones in order after a reconnect, so outputs of one partition keep their
submission order.

`PulsarSinkConfig::max_pending` bounds the messages queued by `submit` whose
receipt has not arrived; the default is 1000. `submit` waits while that many
are outstanding. A completion dropped before its receipt, as when the input is
abandoned, frees its slot; the message stays queued and may still be written.
A send error on the receipt fails the completion, which stops the subscription
without acknowledging the input. Publish retries apply to queueing only.
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
when the output does not already set that name. `without_key()`,
`without_properties()`, and `without_event_time()` disable the corresponding
field. The source topic, message ID, and publish time are never inherited, and
no ordering key is derived from the input.

`Subscription::forward` applies `PulsarInherit::new()` automatically for a
value-only handler between a Pulsar source and sink. See the
[runtime guide](../runtime.md#same-platform-forwarding).

Inherited properties include trace-context properties such as `traceparent`.
Register `TraceContext` after `PulsarInherit` to replace them with the
processing span's context; see
[trace-context propagation](../runtime.md#trace-context-propagation).

## Transactions

`PulsarSink::transactional()` converts a sink into a
`PulsarTransactionalSink`, which publishes in Pulsar transactions. The broker
must run with `transactionCoordinatorEnabled=true`, and the cluster's
transaction coordinator must be initialized.

```rust,ignore
use beavers::adapters::pulsar::{PulsarSink, PulsarSinkConfig};

let sink = PulsarSink::<Json, Order>::new(PulsarSinkConfig::new(
    "pulsar://localhost:6650",
    "persistent://public/default/processed-orders",
))
.transactional();

Subscription::forward("orders", pulsar_source, sink, handler).transactional()
```

With `Subscription::transactional()` and a `PulsarSource`, each delivery is one
transaction:

1. The sink opens a transaction with the configured `transaction_timeout`.
2. It registers every output partition, publishes the outputs within the
   transaction, and waits for their receipts.
3. It registers the delivery's topic partition and subscription, and
   acknowledges the delivery within the transaction.
4. It commits the transaction.

Consumers see the outputs only after the commit, and the acknowledgement takes
effect at the same time. When any step fails, the sink aborts the transaction
and the delivery stays unacknowledged, so the runtime retries it with the same
prepared outputs. The transaction coordinator also aborts a transaction that
remains open longer than `transaction_timeout`. Each transaction runs in its
own task, so a cancelled commit still finishes or aborts.

Pulsar allows many open transactions per producer, so deliveries of different
ordering scopes commit concurrently. Pulsar has no transactional producer ID
and no producer fencing: a second instance with the same subscription is simply
another consumer of it. Used as a plain `Sink`, a `PulsarTransactionalSink`
publishes each output in a transaction of its own.

`TransactionalSink` is implemented only for a `PulsarSource` delivery and a
`PulsarTransactionalSink`, so pairing a Pulsar transactional sink with another
source, or a Pulsar source with another platform's transactional sink, does not
compile.

## Delivery guarantees

Without transactions, a source ACK and a sink publication are separate broker
operations, so a process failure between them can produce a duplicate on
redelivery. A transactional Pulsar-to-Pulsar subscription publishes outputs and
acknowledges the delivery atomically.
