# Kafka adapter

The optional `kafka` feature provides typed Kafka source and sink adapters
backed by `rdkafka`.

```toml
beavers = { version = "0.1", features = ["kafka"] }
```

## Configuration

`KafkaSourceConfig` takes the bootstrap brokers, consumer group, and topics;
`KafkaSinkConfig` takes the brokers and the topic every output is published to.
Both accept additional librdkafka settings in `properties`. `validate` checks
the values without contacting a broker. Both adapters validate their
configuration when first used, then create their Kafka clients lazily, so an
application can construct its pipeline before a broker is reachable.

```rust
use beavers::{Utf8, adapters::kafka::{KafkaPublish, KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig}};

let source = KafkaSource::<Utf8, String>::new(KafkaSourceConfig::new(
    "localhost:9092",
    "orders-workers",
    ["orders"],
));
let sink = KafkaSink::<Utf8, String>::new(KafkaSinkConfig::new(
    "localhost:9092",
    "processed-orders",
));

let output = KafkaPublish::new("created".to_owned());
```

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
`ProcessingOrder::PerKey`, a subscription processes one record at a time per
partition, in offset order, while different partitions run in parallel up to
the subscription's `concurrency`. With `ProcessingOrder::Unordered`, records of
one partition can complete out of order; the commit still never skips an
unfinished record.

Each partition assignment has its own generation and revocation token. When
Kafka revokes a partition, or the adapter detects that its assignment was lost,
the token is cancelled. The runtime abandons that partition's running and
queued deliveries without ACK or subscription failure; handlers are not
notified. Acknowledgements from an old generation are rejected, so they never
commit into a newer assignment. Records of a partition that is not currently
assigned, such as records fetched before a revoke, are skipped by `receive`.
A publication that completed before the revoke is not undone, and the new owner
reprocesses the record from the last committed offset.

`StreamConsumer::recv` is cancellation-safe in rdkafka 0.39. Dropping a pending
source receive does not consume a record. Dropping a received `KafkaMessage`
does not acknowledge it.

## Publication

`prepare` encodes the nullable value once; publication retries reuse that
prepared record.

`KafkaSink::submit` returns once the producer has queued the record, with a
completion that resolves on the record's delivery report. The runtime frees the
job's concurrency slot at submission and acknowledges the input when the report
arrives, so a subscription keeps several records in the producer while each
input is still acknowledged only after Kafka has its output. `publish` queues
the record and waits for the report.

`KafkaSinkConfig::max_pending` bounds the records queued by `submit` whose
report has not arrived; the default is 1000. `submit` waits while that many are
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
`close` stops new submissions and flushes the producer for at most
`close_timeout`.

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

`Subscription::forward` applies `KafkaInherit::new()` automatically for a
value-only handler between a Kafka source and sink. See the
[runtime guide](../runtime.md#same-platform-forwarding).

Inherited headers include trace-context headers such as `traceparent`. Register
`TraceContext` after `KafkaInherit` to replace them with the processing span's
context; see [trace-context propagation](../runtime.md#trace-context-propagation).

## Tombstones

A Kafka producer sends a null value as a tombstone, typically to delete a key in
a compacted topic. Use the platform-neutral `Tombstones` middleware to reject,
skip, or propagate them before the handler runs; see
[tombstones](../runtime.md#tombstones). Propagation publishes
`KafkaPublish::tombstone` with the received key and rejects a tombstone without
a key.

## Dead letters

`KafkaPublish::from_dead_letter` forwards a dead letter to a Kafka topic with
its original record and failure headers, and `KafkaDeadLetter::from_record`
reads them back; see
[forwarding dead letters](../runtime.md#forwarding-dead-letters-to-a-broker-topic).

## Delivery guarantees

With a `KafkaSink`, producer publication and source offset commits are separate
operations, so a failure or rebalance between them can produce duplicates.
Because publication is pipelined, a crash can leave records whose reports had
not arrived; their inputs are uncommitted and are reprocessed. Use
a [transactional subscription](#transactions) to make them atomic. The adapter
does not delay a rebalance to let in-flight work finish.

## Transactions

`KafkaSink::transactional(transactional_id)` converts a sink into a
`KafkaTransactionalSink`, which publishes records in Kafka producer
transactions. Registered with `Subscription::transactional()` behind a
`KafkaSource`, each [batch](../runtime.md#batches) of deliveries is completed
by one transaction that contains the outputs of every delivery in it and, for
each partition, the consumer offset after the batch's last delivery of that
partition:

```text
begin transaction → produce outputs → send consumer offsets → commit transaction
```

```rust
use beavers::{
    Subscription, Utf8,
    adapters::kafka::{KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig},
};

let source = KafkaSource::<Utf8, String>::new(KafkaSourceConfig::new(
    "localhost:9092",
    "orders-workers",
    ["orders"],
));
let sink = KafkaSink::<Utf8, String>::new(KafkaSinkConfig::new(
    "localhost:9092",
    "processed-orders",
))
.transactional("orders-workers-1");
let subscription = Subscription::forward("orders", source, sink, |order: String| async move {
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
aborts its unfinished transaction. The adapter sets the ID itself, so
`KafkaSinkConfig` rejects `transactional.id` in `properties`.
`KafkaSinkConfig::transaction_timeout` bounds each blocking transaction call:
initialization, sending offsets, commit, and abort. The producer is created and
its transactions are initialized on first use.

The source and sink must use the same Kafka cluster, because the offsets are
committed through the group coordinator of the sink's cluster. Before the
first delivery is processed, the sink compares the cluster ID reported by the
source's consumer with the one reported by its brokers, read with a separate
non-transactional client, and a mismatch stops the subscription. Different
`brokers` lists for the same cluster are accepted. Each cluster ID request
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
