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

Input metadata is never inherited by a sink. The sink always publishes to its
configured topic and lets Kafka choose a partition from the explicit key. It
does not copy a source partition, offset, timestamp, or topic into output.
`prepare` encodes the nullable value, key, and headers once; publication retries
reuse that prepared value and wait for Kafka's producer delivery report.

The source disables Kafka auto-commit and auto-offset-store. A successful ACK
records a completed delivery and commits only the contiguous completed prefix
for that topic partition. A completion after an earlier in-flight or unseen
offset cannot advance the commit. Broker commit failures leave completed local
progress in place, so a later acknowledgement can retry the same prefix.

Each partition assignment has its own generation. A revoke invalidates only the
affected partition's outstanding deliveries; acknowledgements from an old
generation fail safely. The adapter does not claim exactly-once processing:
producer publication and source offset commits are separate operations, so a
failure between them can produce duplicates. Kafka preserves its normal
per-partition log order, but concurrent handler completion may be out of order;
the adapter only guarantees that its commits do not skip unfinished deliveries.

`StreamConsumer::recv` is cancellation-safe in rdkafka 0.39. Dropping a pending
source receive does not consume a record. Dropping a received `KafkaMessage`
does not acknowledge it.
