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
