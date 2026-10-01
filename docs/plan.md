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

## Middleware

The asynchronous `pre_handler` and `post_handler` hooks are described in the
[runtime guide](runtime.md#middleware). Remaining decisions:

- whether a middleware `Retry` error should be retried with a policy of its
  own, as handler errors are, instead of stopping the subscription without ACK
  now that hooks can perform I/O;
- whether `MapMetadata` should gain an asynchronous counterpart once closures
  returning `Send` futures that borrow the input can be expressed on stable
  Rust.

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

Kafka-to-Kafka transactions are described in the
[runtime guide](runtime.md#transactions) and the
[Kafka guide](adapters/kafka.md#transactions), including the
[batching](runtime.md#batches) of deliveries into one transaction. Remaining
work:

- A commit that times out after its retriable retries has an unknown outcome.
  The transaction is then aborted or its producer replaced, and the retry can
  duplicate outputs if the timed-out commit had in fact completed. Batching
  widens this to every delivery of the batch.
- A rejected batch commit (`PublishRejected`) cannot be attributed to one
  delivery, so it stops the subscription instead of being routed by the error
  policy. Committing the batch's deliveries one by one would identify the
  rejected output, but its failure routing must finish before later batches
  commit past it.
- The Kafka sink commits a batch of one source only; a batch with deliveries of
  several consumers fails. Several subscriptions sharing one transactional sink
  each form their own batches and serialize on its producer.

The ignored `transactional_pipeline_commits_outputs_with_offsets` test passes
against a single-node Kafka 3.9 broker with the default `TransactionBatch`.
Failure and rebalance paths of batched commits have not been verified against
a live broker. Verify with Kafka:

- A batch with deliveries of several partitions commits the offset after each
  partition's last delivery, and the committed offsets match the outputs
  visible to `read_committed` consumers.
- Throughput and commit latency for the default `TransactionBatch` and for
  larger batches, and a batch whose outputs approach `transaction.timeout.ms`.
- A revoke during a batch's commit aborts it, the retry without the revoked
  partition's deliveries commits, and the revoked records are reprocessed by the
  next owner.
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
- A sink on a different cluster is rejected by the cluster ID check. The
  development environment runs one Kafka cluster, so no live test covers the
  mismatch.

Pulsar transactions were removed. `magnetar-driver` 1.7 reuses the consumer's
session-wide batch bitset for transactional acknowledgements of batched
messages, so the broker rejects all but the first message of a batch, does not
repeat its transaction coordinator handshake after a broker restart, and fails
every retry of a redelivered delivery whose original transaction committed.
Reintroducing them needs a client that fixes these defects.

Multiple sinks within one subscription remain deferred because partial publish
success makes retry and acknowledgement behavior ambiguous. Any future design
must define atomicity or explicit partial-failure semantics.

## Failure defaults

`ErrorPolicy::default()` stops the subscription without ACK on a decode or
encode failure, and on a handler rejection or exhausted retry when no
dead-letter sink is configured. For a durable broker source, one poison record
then stops the application on every restart until it is handled. For a source
fed by untrusted clients, such as the HTTP source, one bad request would stop
the server; the HTTP source avoids this for decode failures by decoding before
a request becomes a delivery, but handler rejections without a dead-letter sink
still stop it. Remaining decisions:

- whether the defaults stay uniform or each source declares its own, for
  example through a capability trait, so a broker source keeps `Stop` while a
  request-driven source defaults to discarding or rejecting the input;
- whether a subscription without a dead-letter sink should warn or fail at
  startup when its policy can stop on a single input;
- the HTTP server's default limits: request and header-read timeouts, the
  number of concurrent connections, and how many requests may wait for the
  subscription before new ones are refused with `503`.

## Dead-letter handling

Forwarding dead letters with failure headers, reprocessing them with a
subscription on the dead-letter topic, and example alert rules are described
in the [runtime guide](runtime.md#forwarding-dead-letters-to-a-broker-topic).
The library deliberately sends no notifications and has no redrive command.
Remaining decisions:

- a source option that ends once it reaches the end of its topics as of
  startup, such as each Kafka partition's high watermark or the last Pulsar
  message ID, so a one-off reprocessing run exits by itself instead of being
  stopped once its lag reaches zero;
- delayed reprocessing without a sleeping handler, such as Pulsar's delayed
  delivery on the dead-letter producer or pausing a Kafka partition until its
  next record is due, if the `max.poll.interval.ms` limit becomes a problem.

Dead-letter headers and properties have not been verified against live
brokers.

## Concurrency and ordering

Per-key scheduling bounds consumption with `max_in_flight`, but one busy
partition can fill that bound and stop receiving for every partition. Coordinate
runtime backpressure with adapter pause/resume capabilities, such as pausing a
Kafka partition whose queue reaches a per-key limit, so other partitions keep
flowing. The design must define the per-key limit, resume timing, and the pause
state across rebalances.

## Chained subscriptions

`channel` chains subscriptions, as described in the
[channel adapter guide](adapters/channel.md). Remaining decisions:

- fan-out to several downstream subscriptions, which needs a completion rule
  for one upstream value observed by several stages;
- startup validation that both ends of a channel are registered in the same
  `App`, since an unregistered upstream leaves the downstream subscription
  waiting for `Receive::End` during shutdown.

Upstream revocation and redelivery through a channel are covered by local tests
only. Verify with Kafka that a partition revocation during a downstream stage
abandons the downstream work and that the next owner reprocesses it.

## Pipelined broker publication

Kafka and Pulsar sinks accept a record when the producer queues it and complete
it on the delivery report or broker receipt, as described in the
[Kafka](adapters/kafka.md#publication) and
[Pulsar](adapters/pulsar.md#publication) guides. Remaining decisions:

- A failed Kafka delivery report, or a Pulsar receipt that still fails after
  the sink's `send_retry`, stops the subscription, because the failure usually
  means the producer cannot recover. The runtime could instead publish the
  prepared output again under `publish_retry`, at the cost of reordering it
  behind later outputs.
- `max_pending` bounds each sink separately. Several subscriptions sharing one
  sink share its bound, and a subscription cannot reserve part of it.

Only the offline acceptance bound of the Kafka sink is covered by default tests.
The ignored live tests check that completions resolve on delivery and that
`max_pending` holds. Verify with the brokers:

- Kafka with `enable.idempotence=false` and retried produce requests: outputs of
  one key can reorder; confirm that `enable.idempotence=true` keeps them in
  submission order.
- A partition revocation while completions are pending abandons their
  acknowledgements, and the new owner reprocesses those records.
- Shutdown with pending completions drains them before the sink closes, within
  the subscription's shutdown timeout.

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

The Pulsar adapter uses `magnetar-driver`. Its consumers and producers
reattach after topic unloads and broker restarts, while the `pulsar` crate
adapter it replaced stalled its consumer after a topic unload and ended the
pipeline on a broker restart. Other client limitations:

- Acknowledging through a client after `close` never completes, so a source
  keeps its client open until the last of its deliveries is dropped.
- The source and sink read the partition count when they connect. Partitions
  added to a topic later are neither consumed nor published to until the
  application restarts.
- A send the broker rejects is not replayed by the client, unlike the Java
  client, which reconnects and replays every pending send in order. The sink
  sends rejected messages again itself, after later messages the client has
  already replayed, so a broker restart or topic unload can reorder outputs of
  one partition. Keeping the order needs a client that replays rejected sends.

## Observability

Spans, events, metrics, and trace-context propagation are described in the
[runtime guide](runtime.md#observability). Remaining decisions:

- OpenTelemetry messaging semantic-convention attributes, such as destination,
  partition, and offset, which need an adapter-provided description of each
  delivery;
- a producer span per published output instead of injecting the `message`
  span's context into every output.

Health checks report runtime-observed state, as described in the
[runtime guide](runtime.md#health-checks). Remaining decisions:

- consumer lag in the health report, which needs an adapter-provided
  measurement such as the distance from a Kafka partition's committed offset to
  its high watermark or a Pulsar subscription's backlog, and a policy for
  whether lag above a threshold makes the application unready;
- source connection state in readiness, since the Kafka and Pulsar clients
  reconnect internally and a pending receive cannot tell an idle broker from an
  unreachable one;
- liveness that detects a stalled subscription, such as a handler or ACK that
  never completes, without restarting instances that are merely idle.

Trace-context propagation and the
[Prometheus export path](runtime.md#exporting-metrics-to-opentelemetry) have not
been verified against live brokers or an OpenTelemetry Collector.

## Testing utilities

The `testing` feature is described in the [testing guide](testing.md).
Remaining decisions:

- fabricated HTTP requests, whose source decodes the body before a request
  becomes a delivery and answers `400` instead of reporting a decode failure;
- Key_Shared and Shared ordering scopes for fabricated Pulsar messages, which
  depend on the subscription type rather than on the record;
- scripted receive errors and revocations, and a sink that fails publication
  or completion on demand, for testing retry and revocation paths.

## Future adapters

Candidate adapters are:

- NATS JetStream source and sink;
- AWS SQS source and sink;
- local file input and output if concrete debugging use cases justify their
  framing and durability semantics.

The HTTP source answers each request with a status only. Remaining decisions:

- a request-reply mode that returns handler output in the response body, which
  needs a sink bound to the originating request;
- TLS and HTTP/2 in the adapter rather than at a reverse proxy;
- routing paths of one listener to different subscriptions;
- idempotency keys that let a retried request be recognized as a duplicate.

Its server limits are listed under [failure defaults](#failure-defaults).

The HTTP sink sends one request per output and classifies responses with a
fixed rule. Remaining decisions:

- configurable classification of retryable and rejected statuses, and honoring
  `Retry-After` on `429` and `503` instead of the `publish_retry` delay;
- a path or query chosen per output, for destinations that address resources in
  the URL;
- custom root certificates, client certificates, and HTTP/2;
- batching several outputs into one request for destinations with bulk APIs.

The [RabbitMQ adapter](adapters/rabbitmq.md) consumes one queue and publishes
to one exchange. Remaining decisions:

- sending a rejected message again on a new channel, as the Pulsar sink does,
  instead of stopping the subscription when a channel is lost before its
  publisher confirmation arrives;
- whether an error policy should be able to reject a delivery
  (`basic.reject` without requeue) so RabbitMQ's dead-letter exchange receives
  it, instead of publishing dead letters through a sink;
- several queues per source, consumer priorities, and RabbitMQ streams;
- configured TLS client certificates beyond what `amqps://` URIs provide.

The ignored RabbitMQ live tests cover publication with confirmations, ACK,
redelivery after close, revocation after a connection is closed by the broker,
unroutable messages, and a forwarding pipeline against a single RabbitMQ 4.1
node. Verify with
RabbitMQ:

- a broker restart and a network partition, where heartbeats rather than a
  closed socket detect the loss;
- publisher confirmations of persistent messages on quorum queues under load,
  and `max_pending` against RabbitMQ's flow control;
- a single-active-consumer queue with `ordered` sources on several instances,
  including the handover when the active consumer stops.

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
| 4 | Live verification of batched Kafka transactions |
| 5 | AWS SQS and NATS JetStream adapters |
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
