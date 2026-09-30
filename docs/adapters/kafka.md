# Kafka adapter

The optional `kafka` feature provides typed Kafka source and sink adapters
backed by `rdkafka`. Both adapters validate their configuration when first used,
then create their Kafka clients lazily. This lets applications construct their
pipeline before a broker is reachable.

```toml
beavers = { version = "0.1", features = ["kafka"] }
```

`KafkaRecord<T>` is the decoded input, including immutable delivery metadata;
`KafkaPublish<T>` is the user-controlled output accepted by the sink:

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

The value is decoded and encoded by the selected codec. `key` and header values
remain bytes. A Kafka null payload becomes `KafkaRecord { value: None, .. }`,
which supports tombstones without inventing a sentinel value. Records decoded
from a source carry `KafkaMetadata` with topic, partition, offset, and
timestamp. `KafkaPublish` has no source metadata, so source location is never
implicitly copied into producer routing.

The raw form of a `KafkaMessage` is a `KafkaRecord<Vec<u8>>` with the undecoded
value bytes. Dead letters carry it, so the original value, key, headers, and
delivery metadata survive even when decoding fails. `KafkaRecord` implements
`Serialize` when its value does.

Input metadata is never inherited implicitly. The sink always publishes to its
configured topic and lets Kafka choose a partition from the explicit key. It
does not copy a source partition, offset, timestamp, or topic into output.
`prepare` encodes the nullable value, key, and headers once; publication retries
reuse that prepared value and wait for Kafka's producer delivery report.

## Metadata inheritance

Register `KafkaInherit` on a Kafka-to-Kafka subscription to forward the received
key and headers:

```rust,ignore
use beavers::adapters::kafka::KafkaInherit;

Subscription::new(kafka_source, kafka_sink, handler)
    .middleware(KafkaInherit::new())
```

Explicit output fields take precedence. The received key is used only when the
output key is `None`; because an unset key and an intentionally absent key are
both `None`, use `KafkaInherit::new().without_key()` to publish keyless records.
Received headers are placed before the output's own headers, except headers
whose name the output already sets. `without_headers()` disables header
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

## Acknowledgements and ordering

The source disables Kafka auto-commit and auto-offset-store. A successful ACK
records a completed delivery and commits only the contiguous completed prefix
for that topic partition. A completion after an earlier in-flight or unseen
offset cannot advance the commit. Broker commit failures leave completed local
progress in place, so a later acknowledgement can retry the same prefix.

Each delivery's ordering key is its topic partition. Under the default
`ProcessingOrder::PerKey`, a subscription processes one record at a time per
partition, in offset order, while different partitions run in parallel up to
the subscription's `concurrency`. With `ProcessingOrder::Unordered`, records of
one partition can complete out of order; the contiguous commit rule still
prevents a commit from skipping unfinished records.

Each partition assignment has its own generation and revocation token. When
Kafka revokes a partition, or the adapter detects that its assignment was lost,
the token is cancelled. The runtime abandons that partition's running and
queued deliveries without ACK or subscription failure; handlers are not
notified. Acknowledgements from an old generation are rejected, so they never
commit into a newer assignment. Records of a partition that is not currently
assigned, such as records fetched before a revoke, are skipped by `receive`.
A publication that completed before the revoke is not undone, and the new owner
reprocesses the record from the last committed offset.

The adapter does not claim exactly-once processing: producer publication and
source offset commits are separate operations, so a failure or rebalance
between them can produce duplicates. The adapter does not delay a rebalance to
let in-flight work finish.

`StreamConsumer::recv` is cancellation-safe in rdkafka 0.39. Dropping a pending
source receive does not consume a record. Dropping a received `KafkaMessage`
does not acknowledge it.
