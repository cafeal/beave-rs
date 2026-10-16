# Kafka adapter

The optional `kafka` feature provides typed Kafka source and sink adapters
backed by `rdkafka`.

```toml
beavers = { version = "0.1", features = ["kafka"] }
```

## Configuration

A `Kafka` handle holds the bootstrap brokers and the librdkafka settings that
every client built from it uses. `property` sets a librdkafka property on every
consumer and producer of the handle, `producer_property` on its producers only,
and `sasl_plain` configures SASL/PLAIN authentication, over TLS unless
`security.protocol` is set otherwise. `handle.source::<C>(group_id, topics)`
builds a source and `handle.sink::<C>(topic)` a sink that publishes every
output to `topic`; `source_with_codec` and `sink_with_codec` take a codec value
as their first argument instead of using the codec's default. Optional settings
are builder methods on the source and sink, such as `property` on a source for
consumer-only settings like `auto.offset.reset`, or `max_pending` on a sink.

```rust
use beavers::{Utf8, adapters::kafka::{Kafka, KafkaPublish}};

let kafka = Kafka::new("localhost:9092").property("client.id", "orders-service");
let source = kafka
    .source::<Utf8>("orders-workers", ["orders"])
    .property("auto.offset.reset", "earliest");
let sink = kafka.sink::<Utf8>("processed-orders").max_pending(500);

let output = KafkaPublish::new("created".to_owned());
```

Every non-transactional sink built from one handle publishes through one
shared producer, created on the first publication of any of them. Each source
has a consumer of its own, because a consumer belongs to one consumer group,
and each [transactional sink](#transactions) has a producer of its own. Each
builder method on the handle returns a handle with a producer of its own, so
configure the handle before building sources and sinks from it.

The handle, the source, and the sink each have `validate`, which checks the
values without contacting a broker. Sources and sinks validate their settings
when first used, then create their Kafka clients lazily, so an application can
construct its pipeline before a broker is reachable. The handle's `Debug`
output shows property names only, because their values can hold credentials.

## Records

`KafkaRecord<T>` is the decoded input, including immutable delivery metadata;
`KafkaPublish<T>` is the user-controlled output accepted by the sink.
The value is decoded and encoded by the selected codec. `key` and header values
remain bytes. A Kafka null payload becomes `KafkaRecord { value: None, .. }`,
which supports tombstones without inventing a sentinel value. Records decoded
from a source carry `KafkaMetadata` with topic, partition, offset, and
timestamp. `KafkaPublish` has no source metadata, so input metadata is never
inherited implicitly. The sink always publishes to its
configured topic and lets Kafka choose a partition from the explicit key. It
does not copy a source partition, offset, timestamp, or topic into output.

The raw form of a `KafkaMessage` is a `KafkaRecord<Vec<u8>>` with the undecoded
value bytes. Dead letters carry it, so the original value, key, headers, and
delivery metadata survive even when decoding fails. `KafkaRecord` implements
`Serialize` when its value does.

## Acknowledgements and ordering

The source disables Kafka auto-commit and auto-offset-store. A successful ACK
records a completed delivery and commits up to the first unfinished delivery
of that topic partition, or past the last received delivery when all are
finished. Because Kafka delivers a partition in offset order, offsets missing
between received deliveries, such as compacted records or transaction markers,
do not hold the commit back. A completion after an earlier in-flight offset
cannot advance the commit. Broker commit failures leave completed local
progress in place, so a later acknowledgement can retry the same position.

Each delivery's ordering key is its topic partition. Under the default
[distribution](../runtime.md#decoding-and-distribution), `SourceOrder`, a
subscription processes one record at a time per partition, in offset order,
while different partitions run in parallel up to the subscription's
`concurrency`. `KafkaDistribution` names the choices for Kafka records:

| `KafkaDistribution` | Processed one at a time |
|---|---|
| `Partition` (default) | The records of one topic partition, as `SourceOrder` |
| `MessageKey` | The records of one record key within a topic partition; records with a null key one at a time per partition |

```rust,ignore
use beavers::adapters::kafka::KafkaDistribution;

Subscription::new("orders", source, sink, handler)
    .concurrency(16)
    .distribution(KafkaDistribution::MessageKey);
```

A producer's default partitioner sends the records of one key to one
partition, so `MessageKey` keeps each key in offset order while different keys
of a busy partition run in parallel. Under `MessageKey`, `Parallel`, or a
distribution by a value of the payload, records of one partition can complete
out of order; the commit still never skips an unfinished record.

Each partition assignment has its own generation and revocation token. When
Kafka revokes a partition, or the adapter detects that its assignment was lost,
the token is cancelled. The runtime abandons that partition's running and
queued deliveries without ACK or subscription failure; handlers are not
notified. Acknowledgements from an old generation are rejected, so they never
commit into a newer assignment. Records of a partition that is not currently
assigned, such as records fetched before a revoke, are skipped by `receive`.
A publication that completed before the revoke is not undone, and the new owner
reprocesses the record from the last committed offset.

A source with a [graceful handoff](#graceful-handoff) lets deliveries that
already started finish before the revoke completes instead.

`Source::pause` pauses the partition an ordering key names, so a subscription's
`max_in_flight_per_key` stops one busy partition without stopping the others,
and `Source::resume` resumes it. A paused partition stays paused when it is
revoked and assigned to the source again, until the runtime resumes it. A record
the source handed to `receive` before the pause is still delivered.

The consumer is polled on a thread of its own, which hands one record at a time
to `receive` and runs the consumer's rebalance callbacks. It polls only as fast
as the subscription receives, so a subscription that stops receiving for longer
than `max.poll.interval.ms` makes the consumer leave its group. Dropping a
pending source receive does not consume a record. Dropping a received
`KafkaMessage` does not acknowledge it.

## Publication

`prepare` encodes the nullable value once; publication retries reuse that
prepared record.

`KafkaSink::submit` returns once the producer has queued the record, with a
completion that resolves on the record's delivery report. The runtime frees the
job's concurrency slot at submission and acknowledges the input when the report
arrives, so a subscription keeps several records in the producer while each
input is still acknowledged only after Kafka has its output. `publish` queues
the record and waits for the report.

`max_pending` bounds the records queued by `submit` on one sink whose report
has not arrived; the default is 1000. The limit is per sink and shared by its
clones, not by the other sinks of the handle. `submit` waits while that many are
outstanding, and also while librdkafka's own queue
(`queue.buffering.max.messages`, `queue.buffering.max.kbytes`) is full. A
completion dropped before its report, as when the input's partition is revoked,
frees its slot; the record stays queued and may still be written.

librdkafka retries a failed produce request internally until
`delivery.timeout.ms`. A delivery report that still carries an error fails the
completion, which stops the subscription without acknowledging the input. The
input is then redelivered from the last committed offset. Publish retries apply
to queueing only, such as a record rejected before it reaches the queue.

Records queued for one partition keep their order when the producer does not
reorder retried batches. Set `enable.idempotence=true` so that a retry cannot
move a record behind a later record with the same key.
`close` stops new submissions on that sink and flushes the shared producer for
at most `close_timeout`, 30 seconds by default. The flush also waits for
records the handle's other sinks have queued, and the producer stays open for
them. Because the producer is shared, a cluster that stays unreachable past
`delivery.timeout.ms` fails the queued records of every sink of the handle, and
each subscription with a failed record stops.

## Metadata inheritance

Register `KafkaInherit` on a Kafka-to-Kafka subscription to forward the received
key and headers:

```rust,ignore
use beavers::adapters::kafka::KafkaInherit;

Subscription::new("orders", kafka_source, kafka_sink, handler)
    .middleware(KafkaInherit::new())
```

Explicit output fields take precedence. The received key is used only when the
output key is `None`; because an unset key and an intentionally absent key are
both `None`, use `KafkaInherit::new().without_key()` to publish keyless records.
Received headers are placed before the output's own headers, except headers
whose name the output already sets and dead-letter headers starting with
`beavers-dlq-`. `without_headers()` disables header
inheritance. The source topic, partition, offset, and timestamp are never
inherited: the sink chooses the topic, Kafka chooses the partition and
timestamp, and the source offset is only used for the source's own commits.

A handler that returns a plain value between a Kafka source and sink gets
`KafkaInherit::new()` automatically. See the
[runtime guide](../runtime/handlers.md#handler-shapes).

Inherited headers include trace-context headers such as `traceparent`. Register
`TraceContext` after `KafkaInherit` to replace them with the processing span's
context; see [trace-context propagation](../runtime/observability.md#trace-context-propagation).

## Tombstones

A Kafka producer sends a null value as a tombstone, typically to delete a key in
a compacted topic. Use the platform-neutral `Tombstones` middleware to reject,
skip, or propagate them before the handler runs; see
[tombstones](../runtime/tombstones.md). Propagation publishes
`KafkaPublish::tombstone` with the received key and rejects a tombstone without
a key.

## Dead letters

`KafkaPublish::from_dead_letter` forwards a dead letter to a Kafka topic with
its original record and failure headers, and `KafkaDeadLetter::from_record`
reads them back; see
[forwarding dead letters](../runtime/errors.md#forwarding-dead-letters-to-a-broker-topic).

## Health and tracing

The source reports its connection state to
[health checks](../runtime/health.md#connection-state) from librdkafka
statistics, enabling `statistics.interval.ms` at 5000 ms unless the handle or
source sets it. Consumer lag comes from the broker; see the
[monitoring guide](../monitoring-and-alerting/README.md#backlog).
Each delivery's `message` span carries the topic, partition, offset, and group
ID as [messaging attributes](../runtime/observability.md#messaging-attributes).

## Delivery guarantees

With a `KafkaSink`, producer publication and source offset commits are separate
operations, so a failure or rebalance between them can produce duplicates.
Because publication is pipelined, a crash can leave records whose reports had
not arrived; their inputs are uncommitted and are reprocessed. Use
a [transactional subscription](#transactions) to make them atomic. Without a
[graceful handoff](#graceful-handoff), a rebalance abandons in-flight work at
once.

## Transactions

`KafkaSink::transactional(transactional_id)` converts a sink into a
`KafkaTransactionalSink`, which publishes records in Kafka producer
transactions. Registered with `Subscription::transactional()` behind a
`KafkaSource`, each [batch](../runtime/transactions.md#batches) of deliveries is completed
by one transaction that contains the outputs of every delivery in it and, for
each partition, the consumer offset after the batch's last delivery of that
partition:

```text
begin transaction → produce outputs → send consumer offsets → commit transaction
```

```rust
use beavers::{Subscription, Utf8, adapters::kafka::Kafka};

let kafka = Kafka::new("localhost:9092");
let source = kafka.source::<Utf8>("orders-workers", ["orders"]);
let sink = kafka
    .sink::<Utf8>("processed-orders")
    .transactional("orders-workers-1");
let subscription = Subscription::new("orders", source, sink, |order: String| async move {
    Ok(order.to_uppercase())
})
.transactional();
```

`KafkaTransactionalSink` is a separate type, so only a transactional sink can be
registered with `Subscription::transactional()`.

Consumers that read the output with `isolation.level=read_committed`, the
librdkafka default, see a batch's outputs only once its offsets are committed
with them. A commit failure aborts the transaction, so neither the outputs nor
the offsets take effect, and the runtime retries the batch with the same
prepared outputs. Deliveries that complete without output, such as discarded
or dead-lettered ones, contribute their offset without records. Dead letters
are published outside the transaction.

The transactional ID becomes the producer's `transactional.id`. Give each
running instance of the application its own ID and keep it across restarts of
that instance: a new producer with the same ID fences the previous one and
aborts its unfinished transaction. The adapter sets the ID itself, so the
handle's `validate` rejects `transactional.id` set with `property` or
`producer_property`. `KafkaTransactionalSink::transaction_timeout` bounds each
blocking transaction call: initialization, sending offsets, commit, and abort;
the default is 60 seconds. Its `close_timeout` defaults to the sink's. The
transactional sink publishes through a producer of its own, created with the
handle's properties and producer properties, and its transactions are
initialized on first use.

The source and sink must use the same Kafka cluster, because the offsets are
committed through the group coordinator of the sink's cluster. Before the
first delivery is processed, the sink compares the cluster ID reported by the
source's consumer with the one reported by its brokers, read with a separate
non-transactional client, and a mismatch stops the subscription. A source and
sink built from different handles, or with different broker lists for the same
cluster, are accepted. Each cluster ID request
waits up to `transaction_timeout`.

A producer runs one transaction at a time, so transactions from every clone of
the sink are serialized, while handlers still run in parallel up to the
subscription's `concurrency`. Batching amortizes that serialization: one
transaction's commit latency covers up to `TransactionBatch::max_deliveries`
deliveries from every partition. Each transaction runs in its own task: an
abandoned batch does not leave a transaction half-finished or release the
producer while it is in use. A commit that fails with a
retriable error is retried within the same transaction. After an error that
cannot be aborted, or a fatal one such as fencing, the producer is discarded and
the next attempt initializes a new one with the same transactional ID.

Offsets are sent with the consumer's current group metadata, so the group
coordinator rejects them from a consumer that has left the group generation.
The offset check and commit run while the source holds its revoke callback
back: a revoke waits for a commit in progress, and a transaction that contains
a delivery of a revoked or lost assignment is aborted without committing. The
runtime then retries the batch without the revoked deliveries. A batch holds
the deliveries of one source; a batch whose deliveries come from different
consumers is rejected. Offsets are committed only
through transactions; the source's own commits are never used in a
transactional subscription.

Used as a plain `Sink`, `KafkaTransactionalSink` commits each publication in a
transaction of its own without consumer offsets. That makes each record visible
to `read_committed` consumers atomically, but the subscription remains
at-least-once.

## Graceful handoff

`KafkaSource::graceful_handoff(timeout)` makes a revoke wait for the revoked
partitions' deliveries before the partitions go to their next owner. It is off
by default.

```rust
use beavers::{Json, adapters::kafka::Kafka};
use std::time::Duration;

let kafka = Kafka::new("localhost:9092");
let orders = kafka
    .source::<Json<String>>("billing", ["orders"])
    .graceful_handoff(Duration::from_secs(10));
```

When Kafka revokes partitions, the source cancels each delivery's handoff token
and stops accepting records of those partitions. The runtime abandons
deliveries that have not started, while started ones continue and are
acknowledged as usual. The revoke waits until every delivery of the partitions
was acknowledged or dropped, or until `timeout` passes, then commits the
acknowledged offsets and cancels the revocation tokens, abandoning whatever is
still running. The next owner starts after the acknowledged records instead of
reprocessing them.

The rebalance callback runs on the consumer's polling thread, so the consumer
receives no records of any partition while it waits. Kafka also waits for the
revoke before it completes the rebalance, for at most `max.poll.interval.ms`, so
`validate` requires `timeout` to be shorter than that property (5 minutes by
default). A lost assignment is not handed off, because its offsets can no
longer be committed. In a [transactional subscription](#transactions), the
revoke waits for the transactions that contain the started deliveries, and the
source commits no offsets of its own.

