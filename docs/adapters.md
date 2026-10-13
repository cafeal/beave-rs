# Adapters

Adapters connect the framework's [Source and Sink contracts](architecture.md#trait-boundaries)
to concrete inputs and outputs. Local adapters and the channel adapter are
available by default. Kafka, Pulsar, RabbitMQ, SQS, Pub/Sub, Azure Service Bus, and HTTP adapters are optional features and are
available through `beavers::adapters`; the default local adapters and channel
types are also re-exported at the crate root.

| Adapter | Role | Typical use |
|---|---|---|
| `IterSource<T>` | Typed input | Fixtures, examples, finite jobs |
| `StdinSource<C>` | Line-delimited input | Files, pipes, interactive input |
| `InMemorySink<T>` | Typed output collection | Assertions and local inspection |
| `StdoutSink<C>` | Line-delimited output | JSON output, files, pipes |
| `ChannelSource<T>` / `ChannelSink<T>` | Chained subscriptions and application ends | Pipeline stages whose upstream ACK waits for the downstream stage; in-process workers fed and read by application code |
| `KafkaSource<C>` / `KafkaSink<C>` | Kafka records and publishes | Durable broker pipelines (`kafka` feature) |
| `KafkaTransactionalSink<C>` | Kafka publishes in producer transactions | Exactly-once Kafka-to-Kafka pipelines (`kafka` feature) |
| `PulsarSource<C>` / `PulsarSink<C>` | Pulsar records and publishes | Durable broker pipelines (`pulsar` feature) |
| `RabbitMqSource<C>` / `RabbitMqSink<C>` | RabbitMQ queue deliveries and exchange publishes | Work queues and routed messaging (`rabbitmq` feature) |
| `SqsSource<C>` / `SqsSink<C>` | Amazon SQS queue messages and sends | Managed work queues on AWS (`sqs` feature) |
| `PubSubSource<C>` / `PubSubSink<C>` | Google Cloud Pub/Sub subscription messages and topic publishes | Managed messaging on Google Cloud (`pubsub` feature) |
| `ServiceBusSource<C>` / `ServiceBusSink<C>` | Azure Service Bus queue and subscription messages and sends | Managed work queues and topics on Azure (`servicebus` feature) |
| `HttpSource<C>` | HTTP `POST` requests answered on ACK | Webhooks and push ingestion (`http` feature) |
| `HttpSink<C>` | HTTP requests that succeed on a `2xx` response | Delivering events to HTTP services (`http` feature) |

NATS JetStream is [planned](plan.md#future-adapters). Local and
channel adapters do not provide durable delivery guarantees.

Enable a broker adapter in the application manifest:

```toml
[dependencies]
beavers = { version = "0.1", features = ["kafka"] }
# or: features = ["pulsar"], ["rabbitmq"], ["sqs"], ["pubsub"], ["servicebus"], or ["http"]
```

The broker-specific guides document configuration, metadata, ACK behavior, and
delivery boundaries: [Local adapters](adapters/local.md),
[Channel](adapters/channel.md), [Kafka](adapters/kafka.md),
[Pulsar](adapters/pulsar.md), [RabbitMQ](adapters/rabbitmq.md),
[SQS](adapters/sqs.md), [Pub/Sub](adapters/pubsub.md),
[Azure Service Bus](adapters/servicebus.md), and [HTTP](adapters/http.md).

## Implementing another adapter

Implement `Source` with an associated `Message: SourceMessage` for input, or
`Sink<T>` with an associated prepared representation for output. Adapters own
transport-specific settings and resource cleanup. Codecs own serialization;
subscription runtime owns retries, concurrency, and ACK sequencing.

Keep these contracts explicit:

- A canceled receive must not silently lose a delivery.
- Decoding and preparing output must not publish or ACK.
- Dropping a received message must not ACK it.
- Successful publication must reach the sink's documented confirmation boundary.
- A sink whose destination refuses an output permanently, where sending it
  again cannot succeed, marks the error with `PublishRejected::wrap` so the
  error policy routes the delivery instead of retrying it.
- Broker-specific commit order and assignment validity belong to the adapter.
- `SourceMessage::Raw` implements `Debug` and `Serialize`, so dead letters can
  be published as they are and channels can keep it for the next stage.
- A source with ordered delivery scopes, such as partitions, returns an
  `OrderingKey` from `SourceMessage::ordering_key`.
- A source whose deliveries can move to another consumer returns a revocation
  `CancellationToken` from `SourceMessage::revocation` and cancels it when the
  delivery is no longer owned.
- A source that accepts input from clients, such as a server, stops accepting
  it in `Source::stop_receiving`, which the runtime calls when it stops
  receiving on shutdown or failure, before draining.

`Delivery::with_ordering_key` and `Delivery::with_revocation` provide both for
custom sources built on `Delivery<T>`, and `Delivery::with_raw` sets the raw
form their dead letters carry.

See [architecture](architecture.md) for ownership boundaries and the
[runtime guide](runtime.md) for retry, failure, and shutdown behavior.

## Broker adapters

Kafka, Pulsar, RabbitMQ, SQS, Pub/Sub, and Azure Service Bus keep their
platform-specific message models explicit below `beavers::adapters::kafka`,
`beavers::adapters::pulsar`, `beavers::adapters::rabbitmq`,
`beavers::adapters::sqs`, `beavers::adapters::pubsub`, and
`beavers::adapters::servicebus`. Each decodes source
messages into a broker-specific `*Record<T>` containing delivery metadata and
accept a separate `*Publish<T>` type for user-controlled output. Prepared
payloads hold encoded bytes for retries. See the dedicated guides for each
broker's configuration and delivery behavior.

Broker adapter constructors use `C::default()` for a codec. Their
`with_codec(config, codec)` constructors accept an existing codec instance,
which is required for configured codecs such as `Avro`. `StdinSource` and
`StdoutSink` take a codec the same way with `with_codec(codec)`.
