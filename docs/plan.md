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

## Cross-platform metadata mapping

Typed output middleware and same-platform inheritance are described in the
[runtime guide](runtime.md#middleware). Cross-platform mappings are
currently application-written `MapMetadata` functions. Remaining decisions:

- whether adapters should provide reusable conversions between platforms, such
  as Kafka headers to Pulsar properties or SQS message attributes, without a
  universal metadata structure;
- a clear policy when cross-platform value-only forwarding would discard
  metadata, including whether such a subscription should warn or fail at
  startup;
- typed Kafka keys as a possible refinement of the byte-oriented key field.

## Serialization and codecs

Potential codec work includes:

- MessagePack support;
- Schema Registry integration for Avro;
- writer-schema and reader-schema resolution;
- configured-codec injection for local stdin and stdout adapters;
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

## Concurrency and ordering

Per-key scheduling bounds consumption with `max_in_flight`, but one busy
partition can fill that bound and stop receiving for every partition. Coordinate
runtime backpressure with adapter pause/resume capabilities, such as pausing a
Kafka partition whose queue reaches a per-key limit, so other partitions keep
flowing. The design must define the per-key limit, resume timing, and the pause
state across rebalances.

## Kafka rebalance behavior

Revoked partitions currently abandon in-flight work immediately. Evaluate an
optional graceful handoff that delays revoke completion for a bounded time so
work in flight can finish and commit, reducing duplicates for the next owner.

Partition scheduling and revocation are covered by unit and runtime tests but
have not been verified against a live broker. Verify with Kafka:

- `receive` skips records of partitions the adapter does not consider assigned.
  This assumes rdkafka always runs `post_rebalance` with the assignment before
  it returns the first record of a newly assigned partition. If that does not
  hold, the source silently skips every record of that partition.
- Eager and cooperative (`partition.assignment.strategy=cooperative-sticky`)
  rebalances both cancel the revoked partitions' tokens and reassign cleanly.
- After `assignment_lost`, all tokens are cancelled and the next assignment
  resumes processing.
- A revoked delivery's in-flight commit does not affect the next assignment.
- Commits advance past offset gaps on a compacted topic and on a topic written
  by transactional producers (`isolation.level=read_committed`, including
  aborted transactions). This assumes the consumer returns a partition's
  records in strictly increasing offset order within an assignment.

Verify with Pulsar that Failover and Key_Shared deliveries carry the partition
index and ordering key expected by the adapter.

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
failures, error-policy outcomes per `FailureKind` (dead-lettered and discarded
deliveries), duration per stage, and in-flight work. Discarded deliveries must
always be counted and logged, because `FailureAction::Discard` otherwise leaves
no trace. Plan OpenTelemetry integration for standard monitoring backends.
Trace-context propagation needs explicit mappings for Kafka headers, Pulsar
properties, NATS headers, and SQS attributes.

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
| 1 | Adapter pause/resume backpressure and graceful rebalance handoff |
| 2 | Cross-platform metadata mapping policy |
| 3 | Observability and trace-context propagation |
| 4 | Kafka transactions and exactly-once processing |
| 5 | Blocking-handler worker pool |
| 6 | NATS JetStream and AWS SQS adapters |
| 7 | Schema Registry and additional codecs |

The order may change when a concrete application requires a later capability.
When work begins, update this document with any newly resolved decisions. When
the work is complete, remove its planning details and place the stable behavior
in the relevant durable documentation.

## Open questions

1. Further refinement of source, message, and sink generics, lifetimes, and
   error types.
2. Handler ergonomics for implicit versus explicit `Emit` registration.
3. Compile-time versus startup validation of broker capabilities.
4. Shutdown deadlines and cancellation policy.
5. Adapter and codec crate boundaries as optional dependencies grow.
6. Isolation of handler futures from communication and control execution.

## Core philosophy

Application code should process events. Beavers should manage the surrounding
flow without promising capabilities that the underlying platform cannot
provide.
