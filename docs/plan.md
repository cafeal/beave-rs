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

Kafka-to-Kafka and Pulsar-to-Pulsar transactions are described in the
[runtime guide](runtime.md#transactions) and the
[Kafka](adapters/kafka.md#transactions) and
[Pulsar](adapters/pulsar.md#transactions) guides, including the
[batching](runtime.md#batches) of deliveries into one transaction. Remaining
work:

- A commit that times out after its retriable retries has an unknown outcome.
  The transaction is then aborted or its producer replaced, and the retry can
  duplicate outputs if the timed-out commit had in fact completed. Batching
  widens this to every delivery of the batch.
- A subscription's batches commit one at a time. Pulsar allows concurrent
  transactions, so batches of different ordering scopes could commit in
  parallel; that needs a rule for a failed batch whose scopes also appear in a
  later batch that already committed.
- The Kafka sink commits a batch of one source only; a batch with deliveries of
  several consumers fails. Several subscriptions sharing one transactional sink
  each form their own batches and serialize on its producer.
- A Pulsar commit whose response is lost has an unknown outcome as well. The
  sink reports it as a failure, and the retried delivery can duplicate outputs
  if the commit had completed.
- A Pulsar transactional sink accepts only a source with the same service URL,
  because the protocol reports no cluster identity. Comparing an identity read
  from the brokers, such as the cluster name from the admin API, would also
  accept different URLs of one cluster.

The ignored `transactional_pipeline_commits_outputs_with_offsets` test passed
against a single-node Kafka 3.9 broker with one transaction per delivery.
Batched commits, and failure and rebalance paths, have not been verified
against a live broker. Verify with Kafka:

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

The ignored Pulsar transaction tests passed against a Pulsar 4.0 standalone
broker with `transactionCoordinatorEnabled=true`, with one transaction per
delivery: outputs routed to a three-partition topic commit with the
acknowledgements of a two-partition input, and a transaction whose
acknowledgement fails is aborted, its output stays invisible, and the
redelivered message commits. Batched commits have not been verified against a
live broker. Verify with Pulsar:

- A batch acknowledges deliveries of several input partitions within one
  transaction, registering each partition's subscription once, and an aborted
  batch redelivers all of them.
- Throughput and coordinator latency for the default `TransactionBatch` and for
  larger batches.

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
- whether a channel delivery should carry the upstream ordering key, so the
  downstream stage can schedule `PerKey` independently of the upstream job
  that waits for it;
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

- A failed delivery report or receipt stops the subscription, because both
  clients already retry internally and the failure usually means the producer
  cannot recover. The runtime could instead publish the prepared output again
  under `publish_retry`, at the cost of reordering it behind later outputs.
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
- A Pulsar broker restart or topic unload with messages awaiting receipts: the
  producer replays them, the completions resolve, and the order holds.
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
  span's context into every output.

Trace-context propagation and the
[Prometheus export path](runtime.md#exporting-metrics-to-opentelemetry) have not
been verified against live brokers or an OpenTelemetry Collector.

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

## HTTP sink

The [HTTP sink](adapters/http.md#sink) sends each output as a request and
retries every failed attempt under `publish_retry`. Remaining decisions:

- A permanent failure such as `400` or `422` is retried like a transient one,
  and an exhausted retry stops the subscription. Routing such an output to the
  dead-letter sink needs publish failures that the error policy can classify,
  which no sink offers yet.
- `Retry-After` on `429` and `503` is ignored; the retry policy's backoff
  applies.
- Each publication waits for its response. Pipelining requests through
  `Sink::submit` would raise throughput per ordering scope, but requests on
  separate connections can reach the endpoint out of order.
- Mutual TLS, custom root certificates, and HTTP/2 are not configurable.
- A default `content-type` derived from the codec.

The sink is tested against a local HTTP/1.1 endpoint only. HTTPS was checked by
hand against one public endpoint through a proxy; certificate failures and
connection reuse after an endpoint restart are untested.

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
| 4 | Live verification of batched Kafka and Pulsar transactions |
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
