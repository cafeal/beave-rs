# beave-rs — Design plan

This document contains planned work and unresolved design decisions. Current
behavior belongs in the [architecture](architecture.md), [runtime](runtime.md),
[adapter](adapters.md), and [codec](codecs.md) documentation. When planned work
is implemented, remove it from this document and document the resulting
contract in the appropriate guide.

## Direction

beave-rs is a lightweight Rust message-processing framework for Kafka, Pulsar,
NATS JetStream, SQS, Azure Service Bus, and other transports:

```text
Source → Handler → Sink
```

A `Subscription` connects one source, one handler, and one sink.

Application code should primarily contain typed `Input → Output` handlers. The
framework should own connectivity, serialization, acknowledgements, retries,
concurrency, shutdown, and observability while describing delivery guarantees
according to the actual source and sink capabilities.

Handlers can keep simple per-key [state](runtime/state.md) committed with
each delivery. The project does not aim to provide stateful stream processing
such as windows, joins, watermarks, changelog-backed local state stores,
distributed scheduling, SQL, or general DAG execution.

## Middleware

The asynchronous `pre_handler` and `post_handler` hooks are described in the
[runtime guide](runtime/middleware.md). Remaining decisions:

- whether a middleware `Retry` error should be retried with a policy of its
  own, as handler errors are, instead of stopping the subscription without ACK
  now that hooks can perform I/O;
- whether `MapMetadata` should gain an asynchronous counterpart once closures
  returning `Send` futures that borrow the input can be expressed on stable
  Rust.

## Serialization and codecs

Potential codec work includes:

- Schema Registry integration for Avro;
- writer-schema and reader-schema resolution;
- evaluation of typed key codecs where a broker supports typed keys;
- recording a wrapper's content encoding, such as gzip, in message metadata so
  one subscription can accept both wrapped and unwrapped payloads. `Decoder`
  receives payload bytes only, so this needs metadata in the decoding contract.

Schema Registry support must keep wire framing separate from raw Avro datum
encoding and define schema lookup caching, compatibility failures, and behavior
when the registry is unavailable.

## Delivery semantics and transactions

Kafka-to-Kafka transactions are described in the
[runtime guide](runtime/transactions.md) and the
[Kafka guide](adapters/kafka.md#transactions), including the
[batching](runtime/transactions.md#batches) of deliveries into one transaction. Remaining
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

Forwarding dead letters with failure headers is described in the
[runtime guide](runtime/errors.md#forwarding-dead-letters-to-a-broker-topic), and
examining, reprocessing, parking, and dropping them in the
[dead-letter handling guide](dead-letter-handling.md).
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

## Per-key backpressure

`max_in_flight_per_key` pauses and resumes keys through `Source::pause`, as
described in the [runtime guide](runtime.md#backpressure). Only
the Kafka source can pause a key. Remaining decisions:

- pausing a Pulsar partition under Failover or Exclusive subscriptions, which
  needs `magnetar-driver` to stop requesting messages for one partition's
  consumer; Key_Shared keys are message keys, which cannot be paused;
- whether a default per-key limit, such as a share of `max_in_flight`, should
  apply when a source can pause.

## State

Stateful handlers and `InMemoryState` are described in the
[state guide](runtime/state.md). Planned stores, each in its own change:

- a Redis store, offering `State<T>` encoded with a codec and a handle that
  reads keys and queues writes for one `MULTI`/`EXEC` at commit, with `WATCH`
  on the keys read so that a concurrent writer is detected;
- a Postgres store, offering `State<T>` in a table it manages and a handle to a
  `sqlx` transaction the handler runs its own SQL in, with each partition's
  applied position written in the same transaction;
- a Kafka compacted-topic store offering `State<T>`, which reads a partition's
  state when the partition is assigned and needs the state key to be the record
  key, since a compacted topic cannot detect a concurrent writer.

Remaining decisions:

- how an unkeyed store that records applied positions, such as the planned
  Postgres store, handles a distribution that completes one partition's
  deliveries out of order, where a later position committed first would make
  an earlier delivery look applied;
- what the runtime does when a durable store detects a concurrent change at
  commit. The outputs were already published from the stale state, so rerunning
  the handler publishes a second set, and stopping the subscription restarts
  the process;
- how assignment and revocation reach a store that loads a whole partition,
  since the runtime sees revocation only through each delivery's token;
- positions for Pulsar, whose message IDs are ordered within a partition but
  can be redelivered out of order under Shared subscriptions and after
  negative acknowledgement;
- retrying a failed commit under a policy instead of stopping the
  subscription;
- removing idle keys and bounding the size of `InMemoryState`;
- stateful blocking handlers.

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

## Shared connection follow-ups

Sources and sinks share their adapter handle's connection; the adapter
documents describe what each handle shares. Remaining work:

- Verify against live brokers that the shared Service Bus connection authorizes
  entities added after it opened, renews their tokens, and reopens for every
  link after it is lost, and that the RabbitMQ handle reopens a lost
  connection for every channel.
- Decide whether HTTP sources should share one listener and route by path.
- Decide whether to add Entra ID authentication to the Service Bus handle.

## Kafka rebalance behavior

The [graceful handoff](adapters/kafka.md#graceful-handoff) and partition pause
are covered by unit, runtime, and ignored live tests. The live tests pause and
resume one partition, and hand off a partition with an eager rebalance while
its delivery is acknowledged, against single-node Kafka 3.9 and 4.1 brokers.
Verify with Kafka:

- A synchronous commit from an acknowledgement task completes while the
  polling thread waits in the revoke callback, under load and with several
  partitions.
- A handoff under `partition.assignment.strategy=cooperative-sticky` revokes
  only the moved partitions, and the other partitions resume receiving once the
  callback returns.
- A handoff that reaches its timeout abandons the remaining deliveries, and the
  next owner reprocesses them from the committed offset.
- A transactional subscription with a graceful handoff commits the started
  deliveries' batches before the revoke completes.
- Pausing and resuming under sustained traffic neither skips nor duplicates
  records, including records librdkafka had fetched ahead before the pause, and
  a paused partition that is revoked and assigned again stays paused.

Partition scheduling and revocation are covered by unit and runtime tests but
have not been verified against a live broker. Verify with Kafka:

- The source skips records of partitions the adapter does not consider assigned.
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

Spans, events, metrics, messaging attributes, and trace-context propagation
are described in the [runtime guide](runtime/observability.md). Remaining
decisions:

- a producer span per published output instead of injecting the `message`
  span's context into every output.

Health checks, source connection state, and stall detection are described in
the [runtime guide](runtime/health.md). Consumer lag is left to broker metrics.
Remaining decision:

- connection state for the RabbitMQ, SQS, Pub/Sub, and Service Bus sources.

The Pulsar probe's `is_connected` has not been verified across a broker
restart, and neither probe across a multi-broker cluster in which one broker
fails.

Trace-context propagation and the
[Prometheus export path](runtime/observability.md#exporting-metrics-to-opentelemetry) have not
been verified against live brokers or an OpenTelemetry Collector.

The [monitoring and alerting guide](monitoring-and-alerting/README.md) has not
been verified against live cloud accounts. In particular, check the CloudWatch
dimension sets produced by the `awsemf` exporter, the `job` label that the
Azure Monitor `PodMonitor` assigns, and the `customMetrics` values that the
`azuremonitor` exporter writes after `cumulativetodelta`.

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

The [SQS adapter](adapters/sqs.md) consumes and sends to one queue with one
request per message. Remaining decisions:

- batching acknowledgements with `DeleteMessageBatch` and publications with
  `SendMessageBatch`, which needs completions that resolve per entry;
- the extended client's S3 payloads beyond 256 KiB;
- whether an error policy should leave a failed delivery visible again so the
  queue's redrive policy moves it, instead of publishing dead letters through a
  sink;
- releasing an abandoned message after a configurable delay rather than at
  once, so a message that keeps failing does not loop through the queue.

The ignored SQS live tests cover sending with attributes, deletion on ACK,
visibility extension, release on drop and close, FIFO group ordering, and a
forwarding pipeline against ElasticMQ. Verify with Amazon SQS:

- visibility extension near the 12-hour limit, throttling of
  `ChangeMessageVisibilityBatch` with many held messages, and receipt handles
  rejected after a failed extension;
- FIFO queues with high-throughput mode, and message group IDs on standard
  queues (fair queues);
- a redrive policy's `maxReceiveCount` against releases on shutdown;
- the default credential chain with web identity and container credentials.

The [Pub/Sub adapter](adapters/pubsub.md) consumes one subscription with the
official client's streaming pull and publishes to one topic. Remaining
decisions:

- whether an error policy should nack a failed delivery so the subscription's
  dead-letter policy moves it, instead of publishing dead letters through a
  sink;
- revoking a delivery when the client fails to extend its lease, which the
  client does not report, rather than only at `max_lease`;
- push subscriptions, which deliver through HTTP and would build on the HTTP
  source.

The ignored Pub/Sub live tests cover publication with attributes, ACK, lease
extension, revocation at the maximum lease, redelivery after a drop and close,
ordering keys, and a forwarding pipeline against the emulator. Verify with
Google Cloud Pub/Sub:

- exactly-once delivery subscriptions, including confirmed acknowledgements
  and `LeaseExpired` revocations;
- ordering keys through the global endpoint, and publication resumed after an
  ordering key failed;
- a dead-letter policy's `maxDeliveryAttempts` against the nacks of shutdowns
  and dropped deliveries, and the reported `delivery_attempt`;
- Application Default Credentials on GKE workload identity and Cloud Run, and
  flow control with `max_outstanding_messages` across several instances.

The [Azure Service Bus adapter](adapters/servicebus.md) receives from one queue
or subscription in peek-lock mode and sends to one queue or topic over AMQP.
Remaining decisions:

- session-enabled entities, which need a session receiver per session and an
  ordering key per session ID;
- Microsoft Entra ID credentials, through `azure_identity` or a token callback;
- sending batches with one transfer, and settling several messages at once;
- confirming completions, which Service Bus settles without an answer, so a
  completion lost with the connection is only seen as a redelivery;
- whether an error policy should abandon a failed delivery until the entity's
  `MaxDeliveryCount` moves it to the dead-letter subqueue, instead of
  publishing dead letters through a sink.

The ignored Service Bus live tests cover sending with application properties,
completion, lock renewal, abandonment on drop, release of prefetched messages
on close, the dead-letter subqueue, topic subscriptions, scheduled messages,
and a forwarding pipeline against the Service Bus emulator. The emulator serves
plain AMQP only and cannot create topic subscriptions through its management
API. Verify with an Azure namespace:

- AMQP over TLS on port 5671 with the system root certificates, and the
  `sb://` token audiences of claims-based security;
- `com.microsoft:renew-lock` and lock-lost errors on a premium namespace, and
  renewal of many held locks against throttling;
- clock skew between the host and Service Bus, since the lock expiry Service
  Bus reports is compared with the local clock;
- partitioned entities with a partition key, and duplicate detection with the
  message IDs that dead letters clear;
- maximum message sizes of standard and premium namespaces.

Each broker adapter must define its native record and publish types, ACK model,
redelivery behavior, ordering scope, cancellation behavior, connection
lifecycle, and mapping of trace-context propagation fields (NATS headers)
before implementation.

## Release preparation

The crate is published as `beavers`; versioning and the compatibility contract
are described in [versioning and releases](releasing.md). Remaining work before
the first crates.io release:

- Decide a consistent set of `with_*` builder methods on the `*Publish` types,
  which differ by adapter: `KafkaPublish` and `PulsarPublish` have none, while
  `SqsPublish` has only `with_message_group_id`.
- After the first release, add `cargo-semver-checks` to CI and switch the
  release workflow from the API token to crates.io trusted publishing.
- Create the project icon and character, and place them in the landing page
  (`site/index.html`) and the README.
- Publish `0.1.0` as the first release intended for use.

## Implementation order

| Priority | Scope |
|---|---|
| 1 | Observability refinements |
| 2 | Live verification of batched Kafka transactions, shared connections, and Kafka rebalance handoff |
| 3 | NATS JetStream adapter |
| 4 | Schema Registry and additional codecs |

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

Application code should process events. beave-rs should manage the surrounding
flow without promising capabilities that the underlying platform cannot
provide.
