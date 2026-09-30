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

Kafka-to-Kafka and Pulsar-to-Pulsar transactions are described in the
[runtime guide](runtime.md#transactions) and the
[Kafka](adapters/kafka.md#transactions) and
[Pulsar](adapters/pulsar.md#transactions) guides. Remaining work:

- Batch several deliveries into one Kafka transaction. Each delivery currently
  commits its own transaction and one producer serializes them, so throughput
  is bounded by commit latency. A batch must close on a size or time limit,
  commit each partition's highest contiguous offset, and abort and retry every
  delivery in it together.
- A commit that times out after its retriable retries has an unknown outcome.
  The transaction is then aborted or its producer replaced, and the retry can
  duplicate outputs if the timed-out commit had in fact completed.
- Pulsar transactions have the same per-delivery cost: each one opens,
  registers partitions and a subscription, and ends with coordinator round
  trips. Batching deliveries into one Pulsar transaction needs the same size or
  time limits and joint abort and retry.
- A Pulsar commit whose response is lost has an unknown outcome as well. The
  sink reports it as a failure, and the retried delivery can duplicate outputs
  if the commit had completed.

The ignored `transactional_pipeline_commits_outputs_with_offsets` test passes
against a single-node Kafka 3.9 broker. Failure and rebalance paths have not
been verified against a live broker. Verify with Kafka:

- Outputs of an aborted transaction stay invisible to `read_committed`
  consumers, and the retried delivery commits once.
- A revoke waits for a commit in progress, and a delivery of the revoked
  partition is aborted instead of committed. Blocking the rebalance callback
  on a commit must not stall the consumer beyond `max.poll.interval.ms`.
- Restarting an instance with the same transactional ID fences the old
  producer, and replacing a producer after a fatal error recovers.
- Offsets sent with the group metadata of a consumer that has rejoined the
  group are accepted only for partitions it still owns, including under
  cooperative rebalancing.

The ignored Pulsar transaction tests pass against a Pulsar 4.0 standalone
broker with `transactionCoordinatorEnabled=true`: outputs routed to a
three-partition topic commit with the acknowledgements of a two-partition
input, and a transaction whose acknowledgement fails is aborted, its output
stays invisible, and the redelivered message commits. Verify with Pulsar:

- Acknowledging a message from a producer batch within a transaction, with and
  without `acknowledgmentAtBatchIndexLevelEnabled`. The adapter acknowledges
  each batch index individually.
- A broker restart or topic unload during a transaction, and a transaction
  coordinator that is unavailable when the sink opens a transaction.
- A transaction that outlives `transaction_timeout` is aborted by the
  coordinator, and the retried delivery commits once.

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

## Pulsar client

The Pulsar adapter uses `magnetar-driver`, which implements Pulsar
transactions; the `pulsar` crate it replaced has no transaction API. The client
was first released in 2026, so its reconnect behavior needs verification under
broker restarts and topic unloads. Other client limitations:

- Acknowledging through a client after `close` never completes, so a source
  keeps its client open until the last of its deliveries is dropped.
- The source and sink read the partition count when they connect. Partitions
  added to a topic later are neither consumed nor published to until the
  application restarts.

## Observability

Spans, events, metrics, and trace-context propagation are described in the
[runtime guide](runtime.md#observability). Remaining decisions:

- OpenTelemetry messaging semantic-convention attributes, such as destination,
  partition, and offset, which need an adapter-provided description of each
  delivery;
- a producer span per published output instead of injecting the `message`
  span's context into every output;
- whether an application that uses OpenTelemetry metrics should get a built-in
  bridge from the `metrics` facade.

Trace-context propagation has not been verified against live brokers or an
OpenTelemetry collector.

## Future adapters

Candidate adapters are:

- NATS JetStream source and sink;
- AWS SQS source and sink;
- local file input and output if concrete debugging use cases justify their
  framing and durability semantics.

Each broker adapter must define its native record and publish types, ACK model,
redelivery behavior, ordering scope, cancellation behavior, connection
lifecycle, and mapping of trace-context propagation fields (NATS headers, SQS
message attributes) before implementation.

## Implementation order

| Priority | Scope |
|---|---|
| 1 | Adapter pause/resume backpressure and graceful rebalance handoff |
| 2 | Cross-platform metadata mapping policy |
| 3 | Observability refinements |
| 4 | Kafka transaction batching and Pulsar transactions |
| 5 | NATS JetStream and AWS SQS adapters |
| 6 | Schema Registry and additional codecs |

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
6. Isolation of async handler futures, which can block an executor thread, from
   communication and control execution.

## Core philosophy

Application code should process events. Beavers should manage the surrounding
flow without promising capabilities that the underlying platform cannot
provide.
