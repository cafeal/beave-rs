# Documentation

| Document | Purpose |
|---|---|
| [Architecture](architecture.md) | Module layout, trait contracts, and extension boundaries |
| [Adapters](adapters.md) | Local, channel, Kafka, and Pulsar adapters, usage, and extension contracts |
| [Codecs](codecs.md) | Serialization contracts and JSON, raw bytes, UTF-8, Protobuf, and Avro codecs |
| [Runtime](runtime.md) | Implemented APIs, processing behavior, configuration, and limitations |
| [Design plan](plan.md) | Product goals, agreed design direction, future work, and open questions |

The runtime guide describes what works today. The design plan includes future
APIs and capabilities; its examples are conceptual unless explicitly identified
as implemented.

Adapter references: [Channel](adapters/channel.md), [Kafka](adapters/kafka.md),
and [Pulsar](adapters/pulsar.md).

Codec references: [Raw bytes and UTF-8](codecs/raw-utf8.md),
[Protobuf](codecs/protobuf.md), and [Avro](codecs/avro.md).

Keep repository documentation, code comments, and examples in English. Keep the
root README focused on the project overview and quick start. Put detailed user
and contributor explanations here, and document public API contracts alongside
their Rust definitions. When behavior changes, update the relevant guide and
keep planned capabilities distinct from implemented ones.
