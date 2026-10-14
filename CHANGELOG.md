# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows the versioning policy in
[versioning and releases](docs/releasing.md).

## [Unreleased]

## [0.0.2] - 2026-10-14

An early preview for trying the API. `0.1.0` will be the first release
intended for use.

### Added

- Typed `Source → Handler → Sink` subscriptions with at-least-once
  delivery: bounded concurrency, per-key ordering, independent receive,
  handler, publish, and dead-letter retry policies, and graceful shutdown.
- Error classification with `HandlerError` and `Classify`, routing through
  `ErrorPolicy`, and dead-letter sinks.
- `beavers::Error` for errors the crate reports, such as from `App::run` and
  configuration validation, and `BoxError` for errors returned by application
  components such as codecs, sources, and sinks. The crate does not expose or
  depend on `anyhow`.
- Typed middleware, value-only forwarding between sources and sinks of one
  platform, tombstone handling, and synchronous handlers on a bounded pool.
- Local adapters and an in-process channel that chains subscriptions with
  end-to-end acknowledgement.
- Kafka, Apache Pulsar, RabbitMQ, Amazon SQS, and HTTP adapters behind Cargo
  features, including Kafka-to-Kafka transactions.
- Google Cloud Pub/Sub source and sink adapters behind the `pubsub` feature.
- An Azure Service Bus source and sink behind the `servicebus` feature, with
  peek-lock receipt and lock renewal.
- Connection handles, such as `Kafka::new`, from which an adapter's sources and
  sinks are built and share one connection.
- JSON, raw byte, and UTF-8 codecs, with Avro, Protobuf, MessagePack, and CBOR
  behind features, and Base64, gzip, and zstd wrappers. A source or sink names
  its payload type through its codec, as in `KafkaSink<Json<Invoice>>`.
- `tracing` spans and `metrics` instruments for every delivery stage,
  trace-context propagation with the `opentelemetry` feature, liveness and
  readiness endpoints with the `health` feature, and test utilities with the
  `testing` feature.
