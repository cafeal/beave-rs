# Runtime guide

This document describes the current implementation. Future broker behavior and
proposed APIs are documented in the [design plan](plan.md).

## Registration and configuration

Use `App::subscribe(source, sink, handler)` for defaults, or register a configured
`Subscription`. The following sketch assumes application-specific components and
retry policies have already been constructed:

```rust,ignore
App::new()
    .subscription(
        Subscription::new(source, sink, handler)
            .name("orders")
            .concurrency(16)
            .max_in_flight(64)
            .retry(handler_retry)
            .receive_retry(receive_retry)
            .publish_retry(publish_retry)
            .drain_timeout(Duration::from_secs(30))
            .middleware(MapMetadata::new(|_input, output| Ok(output)))
            .dlq(dead_letter_sink),
    )
    .run()
    .await?;
```

Registration constructs the application. Processing starts in `run()`.
`SubscriptionConfig` can also be passed through `.config(...)`.

| Setting | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Maximum concurrent message jobs |
| `max_in_flight` | 64 | Additional bound on outstanding jobs |
| Retry attempts | 3 | Includes the first attempt |
| Initial retry delay | 100 ms | Exponential backoff starting delay |
| Maximum retry delay | 5 s | Backoff cap |
| Drain timeout | 30 s | Bound on draining; cleanup has a separate timeout of the same duration |

The effective job limit is the smaller of concurrency and max_in_flight. The
scheduler has no prefetch queue. Receive, handler, and publish retries have
independent policies; DLQ publication currently uses the publish retry policy.
Jitter is not implemented. Invalid zero concurrency, in-flight limits, or retry
attempt counts fail validation before subscriptions start.

Concurrent jobs do not guarantee output ordering. Use concurrency 1 for sequential
processing. Partition-aware scheduling is not implemented.

## Adapters

See the [adapter guide](adapters.md) for IterSource, StdinSource, InMemorySink,
StdoutSink, and the bounded Channel adapter, including examples, EOF behavior,
ACK guarantees, and I/O limits. These components do not provide durable
redelivery after process exit. Optional Kafka and Pulsar adapters provide
broker-specific source and sink implementations; their documentation covers
configuration and acknowledgement semantics.

## Per-message lifecycle

```text
receive → decode → intercept or handler → map outputs → prepare all outputs → publish → ACK
```

Use `Subscription::new_emitting` to register a handler returning
`Result<Emit<T>>`. `Emit::None` emits nothing, `One` emits one value, and `Many`
emits several. A regular `Vec<T>` remains a single payload. With `Many`, outputs
are published sequentially and the input is acknowledged only after all succeed.
With no outputs, successful processing can proceed directly to ACK.

Decode and preparation failures stop without ACK.

## Middleware

`Subscription::middleware` registers a `Middleware<Input, Output>`. Its `map`
hook maps each emitted value using the decoded input that produced it, and its
`intercept` hook can decide a delivery's outputs before the handler runs. Both
hooks default to doing nothing. The source item and
sink output types are checked at compile time, so a middleware written for one
broker's record and publish types cannot be registered on a subscription whose
source or sink uses different types. Wrap a function or closure in
`MapMetadata::new` for an application-specific mapping, including conversions
between different platforms:

```rust,ignore
Subscription::new(kafka_source, pulsar_sink, handler)
    .middleware(MapMetadata::new(|input: &KafkaRecord<Order>, mut output: PulsarPublish<Order>| {
        output.key = input.key.clone();
        Ok(output)
    }))
```

Middleware runs in registration order, once for every value in `Emit::One` or
`Emit::Many`, and each invocation receives the same original input. All outputs
are mapped and prepared before the first publication, so publish retries reuse
the mapped output and never rerun a middleware. Handler outputs carry explicit
publish fields; an inheriting middleware only fills fields that the output
leaves unset.

`intercept` runs once per delivery, before the handler, in registration order.
The first middleware that returns `Some(emit)` replaces the handler for that
delivery: `Emit::None` acknowledges it without output, and other values become
its outputs, which still pass through every middleware's `map`. Handler retries
never rerun `intercept`.

A mapping or interception error never reruns the handler. `Reject` publishes the original input
to the DLQ and then acknowledges it, publishing none of the delivery's outputs;
without a DLQ, it stops without ACK. `Retry` and `Fatal` stop processing without
ACK.

Adapters provide same-platform inheritance middleware: `KafkaInherit` for Kafka
to Kafka and `PulsarInherit` for Pulsar to Pulsar. They copy user-controlled
metadata and never copy delivery facts such as offsets, partitions, message IDs,
or broker timestamps. See the [Kafka](adapters/kafka.md#metadata-inheritance)
and [Pulsar](adapters/pulsar.md#metadata-inheritance) guides. A subscription
registered with `Subscription::new` carries no received metadata into outputs
unless middleware maps it.

## Same-platform forwarding

When the source and sink use the same platform's record and publish types,
`Subscription::forward` accepts a handler that works only with values:

```rust,ignore
Subscription::forward(kafka_source, kafka_sink, |order: Order| async move {
    Ok(enrich(order))
})
```

The handler receives the record value and returns the output value. The runtime
builds the platform's publish record from each output value and registers that
platform's default inheritance as the first middleware, so outputs keep the
input metadata without the handler handling it. `forward_emitting` is the
`Emit` counterpart; every emitted value inherits from the same input. Further
`.middleware(...)` registrations run after the default inheritance.

The pairing is checked at compile time through the `ValueRecord` and
`SamePlatform` traits, which an adapter implements for its record type. Kafka
uses `KafkaInherit::new()` and Pulsar uses `PulsarInherit::new()`. A record
whose value cannot be represented as a plain value, such as a Kafka null value,
is rejected without invoking the handler: it goes to the DLQ when one is
configured and otherwise stops without ACK. Register `Tombstones` to choose
another policy, or use `Subscription::new` with a record handler to customize
inheritance.

## Tombstones

A tombstone is a received record whose value is null. Producers send them to
delete a key in a compacted topic, and change-data-capture tools emit them
after deleted rows; append-only event streams normally never contain them.
Adapters mark such records through `TombstoneRecord`: Kafka and Pulsar records
with `value: None`.

Register `Tombstones` to decide their handling before the handler runs:

```rust,ignore
Subscription::forward(kafka_source, kafka_sink, handler)
    .middleware(Tombstones::propagate())
```

| Policy | Behavior for a tombstone |
|---|---|
| `Tombstones::reject()` | Route the record to the DLQ, or stop without ACK when none is configured |
| `Tombstones::skip()` | Acknowledge without output |
| `Tombstones::propagate()` | Publish a tombstone for the same key; reject a tombstone the sink cannot express |

Records with a value always reach the handler. `propagate()` requires the output
type to implement `TombstonePublish` for the input, so it compiles only for
sinks that can publish a tombstone; `KafkaPublish` implements it for Kafka input
and requires a key. Propagated tombstones still pass through every middleware's
`map`, so inheritance adds metadata. Propagating is appropriate only when the
output shares the input key space; a handler that re-keys its output should
handle tombstones itself.

Without `Tombstones`, a value-only `forward` handler rejects tombstones, while a
record handler registered with `Subscription::new` receives them.

See the [codec guide](codecs.md#lifecycle-and-failures) for decoding and encoding boundaries.

## Errors and retries

`beavers::Result<T>` uses `HandlerError`. Converting ordinary errors through `?`
classifies them as Fatal; retry must be requested explicitly.

| Failure | Current behavior |
|---|---|
| Handler `Retry` | Retry the handler with cloned input; stop without ACK on exhaustion |
| Handler `Reject` | Prepare and publish the original typed input to the configured DLQ, then ACK |
| Reject without DLQ | Stop without ACK |
| Handler `Fatal` | Stop without ACK |
| Receive `Retry` | Back off and retry receive; reset the failure count after receiving a message |
| Receive `Fatal` or exhausted retry | Stop receiving, drain outstanding work, return an error |
| Value-only input unavailable (Kafka null) | Treated as handler `Reject` without invoking the handler |
| Interception `Reject` | Publish the original typed input to the configured DLQ, then ACK; do not run the handler |
| Interception `Retry` or `Fatal` | Stop without ACK |
| Mapping `Reject` | Publish the original typed input to the configured DLQ, then ACK; publish no outputs |
| Mapping `Retry` or `Fatal` | Stop without ACK; never rerun the handler or mapping |
| Publish failure | Retry the prepared output; never rerun handler, mapping, or encoding |
| Exhausted publish or DLQ retry | Stop without ACK; do not route infrastructure failures to DLQ |
| ACK failure | Return an error; do not claim successful completion |

DLQ preparation runs once before its publish retries. The current DLQ carries
typed input; a raw-input envelope with failure context is not implemented.

## End of input and shutdown

`Receive::End` explicitly means no more messages will arrive. A temporarily empty
source waits instead of reporting End. After End, a subscription drains work and
closes resources; failures during draining or cleanup still produce an error.

One subscription ending normally does not stop the others. `App::run()` returns
success after all subscriptions finish. A failure requests shutdown across the
application, which ultimately returns an error.

SIGINT / SIGTERM or `App::run_until(CancellationToken)` stops new receives and
starts draining. A drain timeout cancels unfinished tasks, then cleanup runs with
its own deadline. In-progress publish or ACK can have an uncertain result if
interrupted; a durable broker may redeliver and cause duplicates.

Current handlers share the Tokio executor with runtime work. They must not block
worker threads. Cancellation and timeouts are cooperative and cannot forcibly
interrupt arbitrary synchronous code. Dedicated blocking-handler execution is a
[future addition](plan.md#handler-execution-model).

## Implementation limits

Kafka and Pulsar adapters, broker record types, Protobuf, and Avro codecs are
implemented. Kafka maintains contiguous commits for completed offsets and
handles assignment generations, but the runtime does not schedule work by
partition and does not provide Kafka transactions or exactly-once processing.
Pulsar uses individual acknowledgements and likewise provides no transactions or
exactly-once processing. NATS JetStream, SQS, partition-aware scheduling, and
tracing / metrics integration are not implemented. Metadata inheritance is
limited to same-platform middleware; cross-platform mappings are
application-written `MapMetadata` functions.

Inputs currently require `Clone + Send + Sync`. Stdin and stdout construct
`Default` codecs internally and do not yet accept configured codec instances.
Detailed decode/encode error classification and raw-input DLQ envelopes remain
open design work. See
[architecture](architecture.md) for extension contracts and the [roadmap](plan.md#implementation-order)
for the intended sequence.
