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
            .middleware(|_input, output| Ok(output))
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
receive → decode → handler → map outputs → prepare all outputs → publish → ACK
```

Use `Subscription::new_emitting` to register a handler returning
`Result<Emit<T>>`. `Emit::None` emits nothing, `One` emits one value, and `Many`
emits several. A regular `Vec<T>` remains a single payload. With `Many`, outputs
are published sequentially and the input is acknowledged only after all succeed.
With no outputs, successful processing can proceed directly to ACK.

The current middleware hook is `Fn(&Input, Output) -> Result<Output>`. It runs for
each emitted value before preparation. Mapping errors stop processing regardless
of their handler error classification. Decode and preparation failures also stop
without ACK. Broker-specific metadata inheritance is not implemented.

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
exactly-once processing. NATS JetStream, SQS, automatic metadata inheritance,
partition-aware scheduling, and tracing / metrics integration are not
implemented. The middleware is an output transformation hook, not yet a
validated cross-broker metadata mapping API.

Inputs currently require `Clone + Send + Sync`. Stdin and stdout construct
`Default` codecs internally and do not yet accept configured codec instances.
Detailed decode/encode error classification and raw-input DLQ envelopes remain
open design work. See
[architecture](architecture.md) for extension contracts and the [roadmap](plan.md#implementation-order)
for the intended sequence.
