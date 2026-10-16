# Runtime guide

This document describes how the current implementation registers
subscriptions and processes each delivery. Future broker behavior and proposed
APIs are documented in the [design plan](plan.md).

Topic guides cover the rest of the runtime:

| Guide | Covers |
|---|---|
| [Handlers](runtime/handlers.md) | Handler signatures, emitting outputs, and blocking handlers |
| [Middleware](runtime/middleware.md) | Hooks around the handler and metadata inheritance |
| [Errors and retries](runtime/errors.md) | Retry policies, the error policy, and dead letters |
| [Tombstones](runtime/tombstones.md) | Handling null-value records |
| [State](runtime/state.md) | Per-key state kept across deliveries and committed before acknowledgement |
| [Transactions](runtime/transactions.md) | Batched transactional publish and acknowledgement |
| [Observability](runtime/observability.md) | Spans, events, metrics, and trace-context propagation |
| [Health checks](runtime/health.md) | Liveness and readiness reports and endpoints |

## Registration and configuration

Use `App::subscribe(name, source, sink, handler)` for defaults, or register a configured
`Subscription`. The following sketch assumes application-specific components and
retry policies have already been constructed:

```rust,ignore
App::new()
    .subscription(
        Subscription::new("orders", source, sink, handler)
            .concurrency(16)
            .max_in_flight(64)
            .max_in_flight_per_key(16)
            .distribution(SourceOrder)
            .handler_retry(handler_retry)
            .receive_retry(receive_retry)
            .publish_retry(publish_retry)
            .dead_letter_retry(dead_letter_retry)
            .drain_timeout(Duration::from_secs(30))
            .middleware(MapMetadata::new(|_input, output| Ok(output)))
            .dead_letter(dead_letter_sink)
            .error_policy(ErrorPolicy::dead_letter_all()),
    )
    .run()
    .await?;
```

Every subscription has a required name, passed as the first argument of
`Subscription::new`, `Subscription::new_emitting`, and `App::subscribe`. The name appears in subscription errors, dead letters, spans,
and metric labels, so it must be non-empty and unique within an `App`; an empty
or duplicate name fails validation before any subscription starts.

Registration constructs the application. Processing starts in `run()`.

| Setting | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Maximum concurrent message jobs, and maximum deliveries decoding at once |
| `max_in_flight` | 64 | Maximum received but unfinished deliveries, including decoding and queued ones |
| `max_in_flight_per_key` | none | Unfinished deliveries of one ordering key at which the source pauses that key |
| `distribution` | `SourceOrder` | Which deliveries are processed one at a time; see [distribution](#decoding-and-distribution) |
| Retry attempts | 3 | Includes the first attempt |
| Initial retry delay | 100 ms | Exponential backoff starting delay |
| Maximum retry delay | 5 s | Backoff cap |
| Retry jitter | `Jitter::None` | Randomization of each backoff delay |
| Error policy | `ErrorPolicy::default()` | Dead-letter handler failures and rejected publications; stop on decode and encode failures |
| Drain timeout | 30 s | Bound on draining; cleanup has a separate timeout of the same duration |
| Stall timeout | Disabled | Liveness fails while deliveries are in flight without progress for this long; see [stall detection](runtime/health.md#stall-detection) |

Receive, handler, output publish, and dead-letter publish retries have
independent policies. Invalid zero concurrency, in-flight limits, or retry
attempt counts, and a zero stall timeout fail validation before subscriptions start, as does an error
policy that dead-letters decode or encode failures without a dead-letter sink.

Each `RetryPolicy` doubles its delay after every failed attempt, starting at
`initial_delay` and capped at `max_delay`. Set them with the methods of the same
name, such as `RetryPolicy::default().max_attempts(5)`. `jitter` randomizes the capped
delay: `Jitter::Full` waits a uniformly random duration up to it, and
`Jitter::Equal` waits half of it plus a random share of the other half. Jitter
spreads retries from many consumers that failed at the same moment.

## Decoding and distribution

Each subscription receives deliveries from its source one at a time. The
runtime then:

1. decodes each delivery on the blocking thread pool, up to `concurrency`
   deliveries at once, so a slow codec holds up neither receiving nor
   handlers;
2. passes the decoded deliveries to the subscription's `Distribution` in the
   order they were received, even when a later delivery finished decoding
   first;
3. queues each delivery behind earlier deliveries with the same distribution
   key, and runs deliveries with different keys, or without a key, in parallel
   up to `concurrency`.

A delivery whose key is busy waits in a queue and starts when the earlier
delivery finishes. Outputs of deliveries with different keys have no ordering
relationship with each other. A delivery that fails to decode has no key: the
runtime routes it by the [error policy](runtime/errors.md) as soon as a job
slot is free, even while earlier deliveries from the same source scope still
run.

A source can give each delivery an `OrderingKey`, the scope in which the source
delivers in order. The Kafka source uses the topic partition. The Pulsar source
uses the topic partition or message key, depending on its subscription type.
The RabbitMQ source uses its queue when configured as ordered. The SQS source
uses the message group of a FIFO queue. The Pub/Sub source uses the ordering
key of a message published with one. The Azure Service Bus source and local
adapters provide no key.

The distribution decides the key:

| Distribution | Key |
|---|---|
| `SourceOrder` (default) | The delivery's `OrderingKey`, so each source scope is processed in the order the source delivers it |
| `Parallel` | None: every delivery runs independently |
| An adapter's distribution: [`KafkaDistribution`](adapters/kafka.md#acknowledgements-and-ordering), [`PulsarDistribution`](adapters/pulsar.md#acknowledgements-and-ordering), [`ServiceBusDistribution`](adapters/servicebus.md#acknowledgements-and-ordering) | A choice the adapter offers, such as the record key within its partition or the Service Bus session ID |
| A closure `Fn(&Item) -> K` | A value of the decoded input, such as a customer ID |
| Your own `Distribution` implementation | Anything the delivery or its decoded input carries |

```rust,ignore
Subscription::new("orders", source, sink, handler)
    .concurrency(16)
    .distribution(|order: &KafkaRecord<Order>| order.value.as_ref().map(|o| o.customer));
```

A subscription whose [stateful handler](runtime/state.md) keeps keyed state,
such as `InMemoryState`, distributes by its state key instead and takes no
distribution of its own.

Under `Parallel`, or any distribution finer than the source scope, deliveries
from one partition can run and complete out of order. Sources remain
responsible for safe acknowledgement; Kafka still commits only the contiguous
completed prefix of each partition. A [transactional](runtime/transactions.md)
subscription requires a distribution that keeps the source order, such as
`SourceOrder` or `KafkaDistribution::Partition`.

A key taken from the payload orders deliveries within one process only. The
broker does not read the payload, so with several consumers the deliveries of
one payload key can reach different processes. Distribute by a key the broker
also routes by, such as the Kafka record key, when one process must see all of
a key's deliveries, as [in-memory state](runtime/state.md) does.

## Backpressure

The runtime receives only while a job slot is free, fewer than `concurrency`
deliveries are decoding, and fewer than `max_in_flight` deliveries are
unfinished. Decoding and queued deliveries count toward `max_in_flight`, so
consumption stays bounded when a handler or sink is slow. Deliveries without a
key never queue: each one starts as soon as it is distributed.

Without a per-key limit, a busy partition can fill `max_in_flight` and stop
receiving for all of them. `max_in_flight_per_key` bounds the unfinished
deliveries of each source `OrderingKey`, whatever the distribution. When a key reaches the limit,
the runtime calls `Source::pause` for it, and calls `Source::resume` once the
key's unfinished deliveries drop to half the limit, so a key near the limit
does not pause and resume on every delivery. Other keys keep flowing while one
is paused. Deliveries the source fetched before the pause still arrive, so a
key can exceed the limit by what the source buffers. The Kafka source pauses
the partition. Sources that cannot pause single keys, which is every other
adapter, return `false` from `pause`; the runtime then stops counting and only
`max_in_flight` applies.

## Revocation

A source can also give each delivery a revocation `CancellationToken`. The
source cancels it when it stops owning the delivery, for example when Kafka
revokes the partition. The runtime then drops the delivery's job at its next
await point, whether it is decoding, in the handler, retrying, publishing, or
acknowledging. The delivery is not acknowledged and its outcome is not a
subscription failure, including an ACK that the source rejected because of the
revocation. Queued deliveries of the same assignment are dropped the same way.

Handlers do not observe revocation. Cancellation is cooperative, so work that
never yields continues until it does. An output publication that was already
accepted by the sink is not retracted; the new owner reprocesses the delivery
and can produce a duplicate.

A source can let started work finish before it gives a delivery up by also
returning a handoff `CancellationToken` from `SourceMessage::handoff`. When the
source cancels it, the runtime drops the delivery without ACK if it has not
started and lets it continue otherwise; the source cancels the revocation token
when the handoff ends. The Kafka source does this during a
[graceful handoff](adapters/kafka.md#graceful-handoff).

## Adapters

See the [local adapters](adapters/local.md) for IterSource, StdinSource,
InMemorySink, and StdoutSink, and the [Channel adapter](adapters/channel.md)
that chains subscriptions, including examples, EOF behavior, ACK guarantees,
and I/O limits. These components do not provide durable
redelivery after process exit. Optional broker adapters provide
broker-specific sources and sinks built from a connection handle, such as
`Kafka` or `RabbitMq`; their documentation covers configuration and
acknowledgement semantics.

## Per-message lifecycle

```text
receive → decode → pre_handler → handler → post_handler → prepare all outputs → submit → complete → ACK
```

Use `Subscription::new_emitting` to register a handler returning
`Result<Emit<T>>`. `Emit::None` emits nothing, `One` emits one value, and `Many`
emits several. A regular `Vec<T>` remains a single payload. With `Many`, outputs
are published sequentially and the input is acknowledged only after all succeed.
With no outputs, successful processing can proceed directly to ACK.

Decode and preparation failures follow the [error policy](runtime/errors.md#error-policy).

`Sink::submit` hands an output to the sink and returns a `Completion` once the
sink has accepted it. Sinks that keep the default complete at submission. A sink whose
acceptance precedes its acknowledgement boundary, such as a
[channel](adapters/channel.md) drained by another subscription, a
[Kafka sink](adapters/kafka.md#publication) waiting for a delivery report, or a
[Pulsar sink](adapters/pulsar.md#publication) waiting for a broker receipt, returns a
pending completion: the job ends and frees its concurrency slot, and the
delivery is acknowledged after every completion succeeds. Deliveries waiting
for completion do not count toward `max_in_flight`; the sink bounds them by
making `submit` wait, as a channel does for capacity. They are abandoned
without ACK when revoked and are drained on shutdown. A failed completion stops the
subscription without acknowledging the delivery. Publish retries apply to
submission only.

## End of input and shutdown

`Receive::End` explicitly means no more messages will arrive. A temporarily empty
source waits instead of reporting End. After End, a subscription drains work and
closes resources; failures during draining or cleanup still produce an error.

One subscription ending normally does not stop the others. `App::run()` returns
success after all subscriptions finish. A failure requests shutdown across the
application, which ultimately returns an error.

SIGINT / SIGTERM or `App::run_until(CancellationToken)` stops new receives and
starts draining running jobs. Deliveries still decoding or queued behind a key are
dropped without ACK. After `Receive::End`, queued deliveries still run. A source whose
`stops_on_shutdown` returns `false`, such as a `ChannelSource` created by `channel`,
keeps receiving until its upstream subscriptions close it; see
[channel shutdown](adapters/channel.md#shutdown). When a subscription stops
receiving before its source ended, the runtime calls `Source::stop_receiving`
before draining, so a source such as the [HTTP source](adapters/http.md#closing)
refuses new input instead of queuing it. A drain
timeout cancels unfinished tasks, then cleanup runs with its own deadline. In-progress publish or ACK can have an uncertain result if
interrupted; a durable broker may redeliver and cause duplicates.

Async handlers and middleware hooks share the Tokio executor with runtime work. They must not block
worker threads; use [blocking handlers](runtime/handlers.md#blocking-handlers) for synchronous work.
Cancellation and timeouts are cooperative and cannot forcibly interrupt
arbitrary synchronous code.

## Implementation limits

Kafka, Pulsar, RabbitMQ, SQS, Pub/Sub, and Azure Service Bus adapters, broker
record types, Protobuf, and Avro
codecs are implemented. Kafka commits up to the first unfinished offset per partition,
schedules work per partition or per record key, and abandons revoked work. Kafka-to-Kafka
subscriptions can publish and commit offsets in batched Kafka transactions;
other pairs are at-least-once. Pulsar uses individual
acknowledgements, schedules work by its subscription type's ordering scope, and
provides no transactions or exactly-once processing. RabbitMQ uses individual
acknowledgements and revokes the deliveries of a lost channel. SQS deletes each
acknowledged message, extends the visibility of held messages, and revokes a
delivery whose visibility could not be kept. Pub/Sub acknowledges through the
client's streaming pull, extends the leases of held messages, and revokes a
delivery held past its maximum lease. Azure Service Bus completes each
acknowledged message, renews the locks of held messages, and revokes a delivery
whose lock was lost. NATS JetStream and adapter pause/resume backpressure are not implemented. Async handler
futures run on the shared Tokio executor without isolation. Metadata inheritance
is limited to same-platform middleware;
cross-platform mappings are application-written `MapMetadata` functions.

Inputs currently require `Clone + Send + Sync`.
See [architecture](architecture.md) for extension contracts and the
[roadmap](plan.md#implementation-order) for the intended sequence.
