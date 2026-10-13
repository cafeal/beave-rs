# Guides

## Using beave-rs

| Guide | Purpose |
|---|---|
| [Architecture](architecture.md) | How sources, handlers, and sinks fit together, their trait contracts, and errors |
| [Runtime guide](runtime.md) | Registration, configuration, processing behavior, shutdown, and limitations, with topic guides for handlers, middleware, errors, tombstones, transactions, observability, and health checks |
| [Adapters](adapters.md) | Local, channel, Kafka, Pulsar, RabbitMQ, SQS, Pub/Sub, Azure Service Bus, and HTTP adapters, usage, and extension contracts |
| [Codecs](codecs.md) | Serialization contracts; JSON, raw bytes, UTF-8, Protobuf, Avro, MessagePack, and CBOR codecs; Base64 and compression wrappers |
| [Testing](testing.md) | Testing handlers, middleware, and error policies with fabricated broker records |
| [Handling dead letters](dead-letter-handling.md) | Finding the cause of dead letters, and reprocessing, parking, or dropping them on log and queue brokers |
| [Monitoring and alerting](monitoring-and-alerting/README.md) | Signals to alert on, alert rules, and setups for Kubernetes, AWS, Google Cloud, and Azure |

Runtime topics: [Handlers](runtime/handlers.md),
[Middleware](runtime/middleware.md), [Errors and retries](runtime/errors.md),
[Tombstones](runtime/tombstones.md), [Transactions](runtime/transactions.md),
[Observability](runtime/observability.md), and [Health checks](runtime/health.md).

Adapter references: [Local adapters](adapters/local.md),
[Channel](adapters/channel.md), [Kafka](adapters/kafka.md),
[Pulsar](adapters/pulsar.md), [RabbitMQ](adapters/rabbitmq.md),
[SQS](adapters/sqs.md), [Pub/Sub](adapters/pubsub.md),
[Azure Service Bus](adapters/servicebus.md), and [HTTP](adapters/http.md).

Codec references: [JSON](codecs/json.md),
[Raw bytes and UTF-8](codecs/raw-utf8.md),
[Protobuf](codecs/protobuf.md), [Avro](codecs/avro.md),
[MessagePack and CBOR](codecs/msgpack-cbor.md), and
[Base64 and compression wrappers](codecs/wrappers.md).

## Contributing

| Guide | Purpose |
|---|---|
| [Local development brokers](development.md) | Docker Compose Kafka, Pulsar, RabbitMQ, ElasticMQ, Pub/Sub emulator, and Service Bus emulator brokers, web consoles, broker examples, and live tests |
| [Design plan](plan.md) | Product goals, agreed design direction, future work, and open questions |
| [Versioning and releases](releasing.md) | Version numbers, the compatibility contract, the minimum supported Rust version, and the website |
| [Code guidelines](../CODE_GUIDELINES.md) | Module layout and implementation standards |

The runtime guide describes what works today. The design plan includes future
APIs and capabilities; its examples are conceptual unless explicitly identified
as implemented.

Keep repository documentation, code comments, and examples in English. Keep the
root README focused on the project overview and quick start. Put detailed user
and contributor explanations here, and document public API contracts alongside
their Rust definitions. When behavior changes, update the relevant guide and
keep planned capabilities distinct from implemented ones.
