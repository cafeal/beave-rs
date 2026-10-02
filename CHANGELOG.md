# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows the versioning policy in
[versioning and releases](docs/releasing.md).

## [Unreleased]

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
- Kafka, Apache Pulsar, RabbitMQ, and HTTP adapters behind Cargo features,
  including Kafka-to-Kafka transactions.
- JSON, raw byte, and UTF-8 codecs, with Avro and Protobuf behind features.
- `tracing` spans and `metrics` instruments for every delivery stage,
  trace-context propagation with the `opentelemetry` feature, liveness and
  readiness endpoints with the `health` feature, and test utilities with the
  `testing` feature.
