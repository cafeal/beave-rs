# beave.rs — Design plan

This document contains planned work and unresolved design decisions. Current
behavior belongs in the [architecture](architecture.md), [runtime](runtime.md),
[adapter](adapters.md), and [codec](codecs.md) documentation. When planned work
is implemented, remove it from this document and document the resulting
contract in the appropriate guide.

## Direction

Beavers is a lightweight Rust message-processing framework for Kafka, Pulsar,
NATS JetStream, SQS, and other transports:

```text
Source → Subscription → Handler → Sink
```

Application code should primarily contain typed `Input → Output` handlers. The
framework should own connectivity, serialization, acknowledgements, retries,
concurrency, shutdown, and observability while describing delivery guarantees
according to the actual source and sink capabilities.

The project does not aim to provide stateful stream processing, windows, joins,
watermarks, state stores, distributed scheduling, SQL, or general DAG execution.

## Handler execution model

Add an explicit wrapper for synchronous handlers instead of overlapping blanket
implementations that attempt to infer whether a function is synchronous:

```rust,ignore
app.subscribe(source, sink, async_handler);
app.subscribe(source, sink, blocking(sync_handler));
```

The wrapper should implement the existing handler contract and submit work to a
dedicated, bounded worker pool. The design must define:

- pool ownership at the application or subscription level;
- worker and queue limits;
- startup and shutdown behavior;
- panic handling;
- cancellation of queued jobs;
- treatment of results produced after the waiting future is cancelled;
- shutdown deadlines for work already running.

Cancelling an async waiter cannot forcibly stop synchronous code. Unfinished
work must never be treated as successful or acknowledged. Async handlers can
also block an executor thread; whether handler futures need executor isolation
remains a separate decision.

## Metadata mapping and inheritance

Add typed subscription middleware for explicit metadata mapping. It should
support same-platform inheritance and cross-platform conversions without a
universal metadata structure.

```rust,ignore
Subscription::new(kafka_source, sqs_sink, handler)
    .middleware(MapMetadata::new(map_kafka_to_sqs))
```

The middleware design must establish:

- compile-time versus startup validation of mapping compatibility;
- precedence between explicit publish fields and inherited fields;
- behavior for plain handler outputs and `Emit::Many`;
- mapping error classification;
- preparation before publication so retries reuse the mapped output;
- a clear policy when cross-platform value-only forwarding would discard
  metadata.

For Kafka-to-Kafka forwarding, the expected default is to inherit keys and
headers, let the sink choose the partition and timestamp, and never inherit the
source offset. Typed Kafka keys remain a possible future API refinement.

## Serialization and codecs

Potential codec work includes:

- MessagePack support;
- Schema Registry integration for Avro;
- writer-schema and reader-schema resolution;
- configured-codec injection for local stdin and stdout adapters;
- clearer decode and encode error classification;
- evaluation of typed key codecs where a broker supports typed keys.

Schema Registry support must keep wire framing separate from raw Avro datum
encoding and define schema lookup caching, compatibility failures, and behavior
when the registry is unavailable.

## Delivery semantics and transactions

Kafka-to-Kafka transactions are the first exactly-once target:

```text
consume → handler → begin transaction → produce output
    → send consumed offsets to transaction → commit transaction
```

Exactly-once support must be represented as a capability of a compatible
source/sink pair. Unsupported combinations should fail at compile time where
practical or during startup otherwise. Pulsar transaction support may be
considered after the Kafka model is established.

Multiple sinks within one subscription remain deferred because partial publish
success makes retry and acknowledgement behavior ambiguous. Any future design
must define atomicity or explicit partial-failure semantics.

## Error policy and dead letters

Refine the error model around:

- final handler retry exhaustion;
- decode and encode failures;
- rejection when no dead-letter sink is configured;
- dead-letter publication retries and exhaustion;
- structured error context without forcing raw broker messages into ordinary
  handlers;
- jitter for retry backoff.

An error policy must continue to preserve the publish-before-ACK rule and must
never classify ambiguous completion as success.

## Concurrency and ordering

Add partition-aware scheduling for brokers with partition ordering. Kafka should
process sequentially within a partition by default while allowing parallel work
across partitions. An explicit unordered mode may allow multiple in-flight
messages from one partition while offset commits continue to advance only over
contiguous completed deliveries.

The design must coordinate runtime backpressure with adapter pause/resume
capabilities and avoid unbounded consumption when a handler or sink is slow.

## Kafka rebalance behavior

Define the cancellation strategy for work in flight when Kafka revokes a
partition. The adapter and runtime must coordinate ownership changes, handler
cancellation, publication already in progress, and safe commits without leaking
rebalance details into ordinary handler APIs.

## Observability

Add tracing spans around subscriptions and individual message stages:

```text
subscription
  └── message
       ├── decode
       ├── handler
       ├── encode
       ├── publish
       └── ack
```

Candidate metrics include received, processed, rejected, retried, publish
failures, dead-letter outcomes, duration per stage, and in-flight work. Plan
OpenTelemetry integration for standard monitoring backends. Trace-context
propagation needs explicit mappings for Kafka headers, Pulsar properties, NATS
headers, and SQS attributes.

## Future adapters

Candidate adapters are:

- NATS JetStream source and sink;
- AWS SQS source and sink;
- local file input and output if concrete debugging use cases justify their
  framing and durability semantics.

Each broker adapter must define its native record and publish types, ACK model,
redelivery behavior, ordering scope, cancellation behavior, and connection
lifecycle before implementation.

## Implementation order

| Priority | Scope |
|---|---|
| 1 | Metadata middleware and same-platform inheritance |
| 2 | Partition-aware scheduling and Kafka rebalance cancellation |
| 3 | Error-policy and dead-letter refinements |
| 4 | Observability and trace-context propagation |
| 5 | Kafka transactions and exactly-once processing |
| 6 | Blocking-handler worker pool |
| 7 | NATS JetStream and AWS SQS adapters |
| 8 | Schema Registry and additional codecs |

The order may change when a concrete application requires a later capability.
When work begins, update this document with any newly resolved decisions. When
the work is complete, remove its planning details and place the stable behavior
in the relevant durable documentation.

## Open questions

1. Further refinement of source, message, and sink generics, lifetimes, and
   error types.
2. Handler ergonomics for implicit versus explicit `Emit` registration.
3. Interaction between classified handler errors and ordinary Rust errors
   propagated with `?`.
4. Compile-time versus startup validation of broker capabilities.
5. Typed metadata middleware composition and mapping error classification.
6. Shutdown deadlines and cancellation policy.
7. Adapter and codec crate boundaries as optional dependencies grow.
8. Kafka null values in handlers that request a plain value.
9. Isolation of handler futures from communication and control execution.

## Core philosophy

Application code should process events. Beavers should manage the surrounding
flow without promising capabilities that the underlying platform cannot
provide.
