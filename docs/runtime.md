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
            .ordering(ProcessingOrder::PerKey)
            .retry(handler_retry)
            .receive_retry(receive_retry)
            .publish_retry(publish_retry)
            .dlq_retry(dead_letter_retry)
            .drain_timeout(Duration::from_secs(30))
            .middleware(|_input, output| Ok(output))
            .dlq(dead_letter_sink)
            .error_policy(ErrorPolicy::dead_letter_all()),
    )
    .run()
    .await?;
```

Registration constructs the application. Processing starts in `run()`.
`SubscriptionConfig` can also be passed through `.config(...)`.

| Setting | Default | Meaning |
|---|---|---|
| `concurrency` | 1 | Maximum concurrent message jobs |
| `max_in_flight` | 64 | Maximum received but unfinished deliveries, including queued ones |
| `ordering` | `PerKey` | Sequential processing within a source ordering key |
| Retry attempts | 3 | Includes the first attempt |
| Initial retry delay | 100 ms | Exponential backoff starting delay |
| Maximum retry delay | 5 s | Backoff cap |
| Retry jitter | `Jitter::None` | Randomization of each backoff delay |
| Error policy | `ErrorPolicy::default()` | Dead-letter handler failures; stop on decode and encode failures |
| Drain timeout | 30 s | Bound on draining; cleanup has a separate timeout of the same duration |

Receive, handler, output publish, and dead-letter publish retries have
independent policies. Invalid zero concurrency, in-flight limits, or retry
attempt counts fail validation before subscriptions start, as does an error
policy that dead-letters decode or encode failures without a dead-letter sink.

Each `RetryPolicy` doubles its delay after every failed attempt, starting at
`initial_delay` and capped at `max_delay`. `jitter` randomizes the capped
delay: `Jitter::Full` waits a uniformly random duration up to it, and
`Jitter::Equal` waits half of it plus a random share of the other half. Jitter
spreads retries from many consumers that failed at the same moment.

## Ordering and backpressure

A source can give each delivery an `OrderingKey`, the scope in which the source
delivers in order. The Kafka source uses the topic partition. The Pulsar source
uses the topic partition or message key, depending on its subscription type.
Local adapters provide no key.

With the default `ProcessingOrder::PerKey`, the runtime runs at most one
delivery per key at a time, in receive order. A delivery whose key is busy waits
in a queue behind it and starts when the earlier delivery finishes. Deliveries
with other keys, and deliveries without a key, run in parallel up to
`concurrency`. Their outputs have no ordering relationship with each other.

`ProcessingOrder::Unordered` ignores keys, so deliveries from one partition can
run and complete out of order. Sources remain responsible for safe
acknowledgement; Kafka still commits only the contiguous completed prefix of
each partition.

The runtime receives only while a job slot is free and fewer than
`max_in_flight` deliveries are unfinished. Queued deliveries count toward
`max_in_flight`, so consumption stays bounded when a handler or sink is slow.
Deliveries without a key never queue: each one starts as soon as it is
received. A busy key can fill `max_in_flight` and stop receiving for all keys;
adapter pause and resume integration is [planned](plan.md#concurrency-and-ordering).

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

## Adapters

See the [adapter guide](adapters.md) for IterSource, StdinSource, InMemorySink,
StdoutSink, and the bounded Channel adapter, including examples, EOF behavior,
ACK guarantees, and I/O limits. These components do not provide durable
redelivery after process exit. Optional Kafka and Pulsar adapters provide
broker-specific source and sink implementations; their documentation covers
configuration and acknowledgement semantics.

## Per-message lifecycle

```text
receive → decode → handler → map outputs → prepare all outputs → publish → ACK
```

Use `Subscription::new_emitting` to register a handler returning
`Result<Emit<T>>`. `Emit::None` emits nothing, `One` emits one value, and `Many`
emits several. A regular `Vec<T>` remains a single payload. With `Many`, outputs
are published sequentially and the input is acknowledged only after all succeed.
With no outputs, successful processing can proceed directly to ACK.

The current middleware hook is `Fn(&Input, Output) -> Result<Output>`. It runs for
each emitted value before preparation. Mapping errors stop processing regardless
of their handler error classification. Decode and preparation failures follow the
[error policy](#error-policy). Broker-specific metadata inheritance is not
implemented.

See the [codec guide](codecs.md#lifecycle-and-failures) for decoding and encoding boundaries.

## Errors and retries

`beavers::Result<T>` uses `HandlerError`. Ordinary errors propagated with `?`
become `Reject`: the handler is not retried, and the input is dead-lettered by
default. The runtime cannot tell a transient failure from a deterministic one,
so the handler requests other outcomes at the call site with the `Classify`
extension trait. `.reject()?` states the default explicitly, `.retry()?` reruns
the handler under its retry policy and dead-letters the input once that is
exhausted; `.fatal()?` stops the subscription:

```rust,ignore
use beavers::{Classify, Result};

async fn handle(order: Order) -> Result<Output> {
    validate(&order).reject()?; // invalid input: dead-letter now (same as `?`)
    let user = db.find_user(order.user_id).await.retry()?; // transient: retry, then dead-letter
    Ok(build_output(order, user))
}
```

### Error policy

`ErrorPolicy` decides what happens to a delivery after one of four routable
failures, identified by `FailureKind`:

| `FailureKind` | Cause | Default action |
|---|---|---|
| `Decode` | `SourceMessage::decode` failed | `Stop` |
| `Rejected` | The handler returned `Reject`, including errors propagated with `?` | `DeadLetter` |
| `RetryExhausted` | The handler returned `Retry` on its final permitted attempt | `DeadLetter` |
| `Encode` | `Sink::prepare` failed for an emitted output | `Stop` |

Each failure maps to one `FailureAction`:

- `Stop` returns an error and leaves the delivery unacknowledged, so a durable
  broker can redeliver it after restart.
- `DeadLetter` publishes a `DeadLetter` envelope to the dead-letter sink and
  then acknowledges the delivery.
- `Discard` acknowledges the delivery without publishing anything. It is an
  explicit choice to lose that delivery.

Rejections and exhausted handler retries without a configured dead-letter sink
stop without ACK: the handler could not process the input and nothing can
receive it. `DeadLetter` for decode or encode failures requires a dead-letter
sink and fails validation otherwise. `ErrorPolicy::dead_letter_all()` routes
every kind to the dead-letter sink.

Encoding happens for all emitted outputs before any publication, so an `Encode`
failure never follows a partial publish; dead-lettering the input after it does
not duplicate outputs.

### Dead letters

`Subscription::dlq(sink)` accepts a `Sink<DeadLetter<Input, Raw>>`, where `Raw`
is the source message's undecoded form (`SourceRaw<S>`).
`Subscription::dlq_with(sink, convert)` converts each envelope into the sink's
own output type first, for example to forward the original payload to a broker
topic with failure details as headers. Handlers never see dead letters.

| Field | Meaning |
|---|---|
| `subscription` | Name of the failing subscription |
| `failure` | The `FailureKind` |
| `error` | Error message including its context chain |
| `attempts` | Handler attempts made; zero for decode failures |
| `input` | Decoded handler input; `None` after a decode failure |
| `raw` | The delivery as received, with its broker metadata |

`SourceMessage::Raw` defines `raw` per source, so broker metadata keeps its
native shape instead of passing through a universal structure:

| Source | `Raw` |
|---|---|
| Kafka | `KafkaRecord<Vec<u8>>`: value bytes, key, headers, and delivery metadata |
| Pulsar | `PulsarRecord<Vec<u8>>`: payload bytes, key, properties, event time, and delivery metadata |
| Stdin | `Vec<u8>`: the received line |
| `Delivery` (`IterSource`, `Channel`) | `()`: input is already typed |

`DeadLetter` implements `Serialize` when its input and raw form do, so a JSON
sink can publish it directly; this holds for Kafka and stdin sources. Pulsar
message IDs are not serializable, so Pulsar dead letters go through `dlq_with`.

Conversion and preparation run once. Publication then retries under the
`dlq_retry` policy. A conversion, preparation, or exhausted publication failure
stops without ACK; dead-letter failures are never routed again.

### Other failures

| Failure | Behavior |
|---|---|
| Handler `Retry` | Retry the handler with cloned input, then apply `RetryExhausted` routing |
| Handler `Fatal` | Stop without ACK |
| Output mapping failure | Stop without ACK, regardless of the returned classification |
| Receive `Retry` | Back off and retry receive; reset the failure count after receiving a message |
| Receive `Fatal` or exhausted retry | Stop receiving, drain outstanding work, return an error |
| Publish failure | Retry the prepared output; never rerun handler, mapping, or encoding |
| Exhausted publish | Stop without ACK; infrastructure failures are never dead-lettered |
| ACK failure | Return an error; do not claim successful completion |

Publication, dead-letter publication, and ACK results are never assumed. An
interrupted or failed attempt is not treated as success.

## End of input and shutdown

`Receive::End` explicitly means no more messages will arrive. A temporarily empty
source waits instead of reporting End. After End, a subscription drains work and
closes resources; failures during draining or cleanup still produce an error.

One subscription ending normally does not stop the others. `App::run()` returns
success after all subscriptions finish. A failure requests shutdown across the
application, which ultimately returns an error.

SIGINT / SIGTERM or `App::run_until(CancellationToken)` stops new receives and
starts draining running jobs. Deliveries still queued behind an ordering key are
dropped without ACK. After `Receive::End`, queued deliveries still run. A drain
timeout cancels unfinished tasks, then cleanup runs with its own deadline. In-progress publish or ACK can have an uncertain result if
interrupted; a durable broker may redeliver and cause duplicates.

Current handlers share the Tokio executor with runtime work. They must not block
worker threads. Cancellation and timeouts are cooperative and cannot forcibly
interrupt arbitrary synchronous code. Dedicated blocking-handler execution is a
[future addition](plan.md#handler-execution-model).

## Implementation limits

Kafka and Pulsar adapters, broker record types, Protobuf, and Avro codecs are
implemented. Kafka maintains contiguous commits for completed offsets, schedules
work per partition, and abandons revoked work, but does not provide Kafka
transactions or exactly-once processing. Pulsar uses individual
acknowledgements, schedules work by its subscription type's ordering scope, and
likewise provides no transactions or exactly-once processing. NATS JetStream, SQS, automatic metadata inheritance, adapter
pause/resume backpressure, and tracing / metrics integration are not
implemented. The middleware is an output transformation hook, not yet a
validated cross-broker metadata mapping API.

Inputs currently require `Clone + Send + Sync`. Stdin and stdout construct
`Default` codecs internally and do not yet accept configured codec instances.
See [architecture](architecture.md) for extension contracts and the
[roadmap](plan.md#implementation-order) for the intended sequence.
