# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows the versioning policy in
[versioning and releases](docs/releasing.md).

## [Unreleased]

### Added

- Setter methods on `RetryPolicy` and `ErrorPolicy`, such as
  `RetryPolicy::default().max_attempts(5)`.
- `Acknowledgements`, returned by `IterSource::acknowledgements`, with
  `count()`.
- `payload()` on every adapter's prepared output, and `key()`, `headers()`, or
  `properties()` on the Kafka and Pulsar ones.

### Changed

- `FailureKind`, `FailureAction`, `ProcessingOrder`, `Jitter`,
  `SubscriptionStatus`, `HttpMethod`, `RabbitMqValue`, `ServiceBusValue`, and
  `SqsAttributeValue` are `#[non_exhaustive]`, as are `RetryPolicy`,
  `ErrorPolicy`, `TransactionBatch`, `HealthReport`, `SubscriptionReport`, and
  every adapter's `*Metadata` struct. Build the structs from `Default` or their
  constructors; the `testing` record functions build adapter metadata.
- `Subscription::retry` is renamed to `handler_retry`, and `dlq`, `dlq_with`,
  and `dlq_retry` to `dead_letter`, `dead_letter_with`, and
  `dead_letter_retry`.
- The `forward` module is renamed to `record`.
- The fields of `KafkaPrepared` and `PulsarPrepared` are private, and the
  `body()` and `data()` accessors of the other prepared outputs are renamed to
  `payload()`.
- `PulsarMetadata::publish_time`, `SqsMetadata::sent_timestamp`, and
  `SqsMetadata::first_receive_timestamp` are `i64` milliseconds, like the other
  adapters' timestamps.

### Removed

- `SubscriptionConfig` and `Subscription::config`; the `Subscription` builder
  methods set every setting.
- The `metadata()` methods of the adapter records, which duplicated their
  public `metadata` field.
- The implementations of the Pulsar client's `AuthProvider` for
  `PulsarAuthentication` and of `From<PulsarSubscriptionType>` for its
  subscription type, so that client is no longer part of the public API.

## [0.0.2] - 2026-10-14

A preview for trying the API. `0.1.0` will be the first release intended for
use.

### Added

- A Google Cloud Pub/Sub source and sink behind the `pubsub` feature.
- An Azure Service Bus source and sink behind the `servicebus` feature, with
  peek-lock receipt and lock renewal.
- MessagePack and CBOR codecs behind the `msgpack` and `cbor` features, and
  Base64, gzip, and zstd wrappers behind the `base64`, `gzip`, and `zstd`
  features.
- `Avro<T>::default()` for types that implement `AvroSchema`, taking the
  schema from the type.
- Guides published on the project website, including monitoring and alerting
  for Kubernetes and cloud providers, dead-letter handling, and a reference
  page for each adapter and codec.

### Changed

- **Breaking:** Each adapter has a connection handle, such as `Kafka::new`,
  `Pulsar`, `RabbitMq`, `Sqs`, `PubSub`, `ServiceBus`, and `HttpClient`, that
  builds its sources and sinks and shares one connection among them. Required
  values are arguments and optional settings are builder methods. Build sources
  and sinks from a handle instead of from configuration structs.
- **Breaking:** Sources and sinks take one type parameter, the codec, which
  names the payload type: write `KafkaSink<Json<Invoice>>` instead of
  `KafkaSink<Json, Invoice>`. `Json`, `Avro`, `Protobuf`, `MessagePack`, and
  `Cbor` are generic over their value type, and the new `Codec` trait, which
  `Decoder` and `Encoder` extend, names it as `Value`.
- **Breaking:** `App::subscribe` and the `Subscription` constructors accept a
  handler whose input is either the received record or its value, and whose
  output is either a publish record or a plain value. A plain output value is
  wrapped in the source platform's publish record with default metadata
  inheritance.
- **Breaking:** The crate reports errors as `beavers::Error`, and
  application-implemented traits such as codecs, sources, and sinks return
  `BoxError`. `anyhow` is no longer part of the public API or a dependency.
- The README starts with a Kafka quick start.

### Removed

- **Breaking:** Adapter configuration structs and the constructors that took
  them; use the connection handles.
- **Breaking:** `Subscription::forward` and `Subscription::forward_emitting`;
  pass a value handler to `App::subscribe` or `Subscription::new` instead.

## [0.0.1] - 2026-10-02

Published to reserve the crate name on crates.io; not intended for use.

### Added

- Typed `Source → Handler → Sink` subscriptions with at-least-once
  delivery: bounded concurrency, per-key ordering, independent receive,
  handler, publish, and dead-letter retry policies, and graceful shutdown.
- Error classification with `HandlerError` and `Classify`, routing through
  `ErrorPolicy`, and dead-letter sinks.
- Typed middleware, value-only forwarding between sources and sinks of one
  platform, tombstone handling, and synchronous handlers on a bounded pool.
- Local adapters and an in-process channel that chains subscriptions with
  end-to-end acknowledgement.
- Kafka, Apache Pulsar, RabbitMQ, Amazon SQS, and HTTP adapters behind Cargo
  features, including Kafka-to-Kafka transactions.
- JSON, raw byte, and UTF-8 codecs, with Avro and Protobuf behind features.
- `tracing` spans and `metrics` instruments for every delivery stage,
  trace-context propagation with the `opentelemetry` feature, liveness and
  readiness endpoints with the `health` feature, and test utilities with the
  `testing` feature.
