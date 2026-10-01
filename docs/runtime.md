# Runtime guide

This document describes the current implementation. Future broker behavior and
proposed APIs are documented in the [design plan](plan.md).

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
            .ordering(ProcessingOrder::PerKey)
            .retry(handler_retry)
            .receive_retry(receive_retry)
            .publish_retry(publish_retry)
            .dlq_retry(dead_letter_retry)
            .drain_timeout(Duration::from_secs(30))
            .middleware(MapMetadata::new(|_input, output| Ok(output)))
            .dlq(dead_letter_sink)
            .error_policy(ErrorPolicy::dead_letter_all()),
    )
    .run()
    .await?;
```

Every subscription has a required name, passed as the first argument of
`Subscription::new`, `new_emitting`, `forward`, `forward_emitting`, and
`App::subscribe`. The name appears in subscription errors, dead letters, spans,
and metric labels, so it must be non-empty and unique within an `App`; an empty
or duplicate name fails validation before any subscription starts.

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
StdoutSink, and the Channel adapter that chains subscriptions, including examples, EOF behavior,
ACK guarantees, and I/O limits. These components do not provide durable
redelivery after process exit. Optional Kafka and Pulsar adapters provide
broker-specific source and sink implementations; their documentation covers
configuration and acknowledgement semantics.

## Per-message lifecycle

```text
receive → decode → pre_handler → handler → post_handler → prepare all outputs → submit → complete → ACK
```

Use `Subscription::new_emitting` to register a handler returning
`Result<Emit<T>>`. `Emit::None` emits nothing, `One` emits one value, and `Many`
emits several. A regular `Vec<T>` remains a single payload. With `Many`, outputs
are published sequentially and the input is acknowledged only after all succeed.
With no outputs, successful processing can proceed directly to ACK.

Decode and preparation failures follow the [error policy](#error-policy).

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

## Transactions

`Subscription::transactional()` replaces the final publish and ACK steps with
one sink transaction per delivery:

```text
first delivery: verify_source(delivery)
… → prepare all outputs → commit(delivery, outputs)
```

```rust,ignore
Subscription::forward("orders", kafka_source, kafka_transactional_sink, handler)
    .transactional()
```

The method only compiles when the sink implements
`TransactionalSink<Source::Message, Output>`, which an adapter provides for the
source messages whose acknowledgement can join its transactions. The
Kafka adapter implements it for a `KafkaSource` and a `KafkaTransactionalSink`,
and the Pulsar adapter for a `PulsarSource` and a `PulsarTransactionalSink`;
see the [Kafka](adapters/kafka.md#transactions) and
[Pulsar](adapters/pulsar.md#transactions) guides. The runtime never calls
the delivery's own ACK in a transactional subscription.

A transaction is not split into acceptance and completion: the commit is both
the publication and the acknowledgement, so the job holds its concurrency slot
until the commit finishes. Kafka and Pulsar transactional sinks keep the default
`submit`, which also applies when one is used as a plain sink.

`commit` publishes every output and acknowledges the delivery atomically. A
failed commit leaves neither in effect and is retried with the same prepared
outputs under the subscription's `publish_retry` policy; an exhausted retry
stops the subscription without acknowledgement. A delivery that completes
without output, including one that emitted nothing, was discarded, or was
dead-lettered, is committed in a transaction without outputs. Dead letters are
published by the dead-letter sink before that transaction and are not part of
it, so they remain at-least-once.

Types cannot tell whether a source and a sink of the same platform connect to
the same cluster, and a transaction can only acknowledge a delivery of its own
cluster. The runtime therefore passes the first delivery of a transactional
subscription to `TransactionalSink::verify_source` before processing it. The
check is retried under the `publish_retry` policy, and a failure stops the
subscription before any handler runs or anything is published or
acknowledged. Shutdown during the check leaves the delivery unacknowledged.

A transactional subscription requires `ProcessingOrder::PerKey`. Deliveries of
one ordering scope then commit one at a time in receive order, so every commit
acknowledges exactly the delivery whose outputs it contains. Other orderings
fail validation before the application starts.

## Middleware

`Subscription::middleware` registers a `Middleware<Input, Output>` with two
asynchronous hooks, both of which default to doing nothing:

```rust,ignore
pub trait Middleware<I, O> {
    fn pre_handler(&self, input: I) -> impl Future<Output = Result<Flow<I, O>>> + Send;
    fn post_handler(&self, input: &I, output: O) -> impl Future<Output = Result<O>> + Send;
}

pub enum Flow<I, O> {
    Continue(I),
    Intercept(Emit<O>),
}
```

Implement the hooks with `async fn`. A hook can await external lookups, such
as a cache, a schema registry, or a metadata service, before the handler runs
or before the outputs are prepared:

```rust,ignore
struct Customers(CustomerClient);

impl Middleware<KafkaRecord<Order>, KafkaPublish<Order>> for Customers {
    async fn pre_handler(
        &self,
        mut input: KafkaRecord<Order>,
    ) -> beavers::Result<Flow<KafkaRecord<Order>, KafkaPublish<Order>>> {
        if let Some(order) = input.value.as_mut() {
            order.customer = self.0.fetch(order.customer_id).await.map_err(HandlerError::Retry)?;
        }
        Ok(Flow::Continue(input))
    }
}
```

The runtime awaits each hook before starting the next, so the hooks of one
delivery never run concurrently. Other deliveries keep processing while a hook
waits, within the subscription's concurrency and ordering limits. The input and
output types must be `Send`, and the input type also `Sync`, because the hook
futures run on the multi-threaded Tokio executor. Like handlers, hooks must
not block executor threads, and a hook future is dropped at its current await
point when the delivery is revoked or a drain timeout cancels it; the delivery
is then not acknowledged.

`pre_handler` runs once per delivery, before the handler, in registration
order. Each middleware receives the input returned by the previous one.
`Flow::Continue(input)` passes a possibly transformed input on, and the handler
receives the final one. `Flow::Intercept(emit)` skips the remaining
`pre_handler` hooks and the handler: `Emit::None` acknowledges the delivery
without output, and other values become its outputs. Handler retries reuse the
transformed input and never rerun `pre_handler`.

`post_handler` runs in registration order for every output, including outputs
from an intercepting middleware, and every invocation receives the input as
decoded from the source, before any `pre_handler` transformation. All outputs
pass through `post_handler` and are prepared before the first publication, so
publish retries never rerun a middleware. Handler outputs carry explicit
publish fields; an inheriting middleware only fills fields that the output
leaves unset.

The source item and sink output types are checked at compile time, so a
middleware written for one broker's record and publish types cannot be
registered on a subscription whose source or sink uses different types. Wrap a
synchronous function or closure in `MapMetadata::new` to use it as a
`post_handler`,
including for conversions between different platforms:

```rust,ignore
Subscription::new("orders", kafka_source, pulsar_sink, handler)
    .middleware(MapMetadata::new(|input: &KafkaRecord<Order>, mut output: PulsarPublish<Order>| {
        output.key = input.key.clone();
        Ok(output)
    }))
```

A middleware error never reruns the handler or the middleware. `Reject` is a
`Rejected` failure routed by the [error policy](#error-policy) with the input as
decoded from the source, and none of the delivery's outputs are published.
`Retry` and `Fatal` stop processing without ACK.

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
Subscription::forward("orders", kafka_source, kafka_sink, |order: Order| async move {
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
is rejected without invoking the handler and routed by the error policy as a
`Rejected` failure. Register `Tombstones` to choose
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
Subscription::forward("orders", kafka_source, kafka_sink, handler)
    .middleware(Tombstones::propagate())
```

| Policy | Behavior for a tombstone |
|---|---|
| `Tombstones::reject()` | Route the record as a `Rejected` failure through the error policy |
| `Tombstones::skip()` | Acknowledge without output |
| `Tombstones::propagate()` | Publish a tombstone for the same key; reject a tombstone the sink cannot express |

Records with a value always reach the handler. `propagate()` requires the output
type to implement `TombstonePublish` for the input, so it compiles only for
sinks that can publish a tombstone; `KafkaPublish` and `PulsarPublish` implement
it for input from the same platform and require a key. Propagated tombstones
still pass through every middleware's `post_handler`, so inheritance adds
metadata. Propagating is appropriate only when the output shares the input key
space; a handler that re-keys its output should handle tombstones itself.

Without `Tombstones`, a value-only `forward` handler rejects tombstones, while a
record handler registered with `Subscription::new` receives them.

See the [codec guide](codecs.md#lifecycle-and-failures) for decoding and encoding boundaries.

## Blocking handlers

Wrap a synchronous function with `blocking` to register it wherever an async
handler is accepted, including `new_emitting` and `forward`:

```rust,ignore
fn score(order: Order) -> beavers::Result<Score> {
    Ok(model.predict(&order))
}

App::new().subscribe("scores", source, sink, blocking(score));
```

Each call runs on a thread of a `BlockingPool`, a fixed set of worker threads
with a bounded job queue, so the handler can block without stalling receiving,
publishing, or ACK on the Tokio executor.

| Topic | Behavior |
|---|---|
| Ownership | `blocking(f)` gives the handler its own pool; `pool.blocking(f)` shares a cloned `BlockingPool` between handlers and subscriptions |
| Limits | `BlockingPool::default()` has one worker per available CPU and a queue of the same size; `BlockingPool::new(workers)` and `with_queue_capacity(workers, capacity)` reject zero |
| Startup | Worker threads start on the first job, not at registration; a thread that fails to start is a `Fatal` handler error |
| Backpressure | A job waits asynchronously for queue space when every worker is busy and the queue is full |
| Panics | A panic becomes `HandlerError::Fatal`: the delivery is not acknowledged and the subscription stops; the worker thread keeps serving other jobs |
| Cancelled waiters | When revocation, shutdown, or a drain timeout drops the waiting future, a queued job is skipped and a running job's result is discarded; the delivery is never acknowledged |
| Running work | A synchronous call cannot be interrupted. After a drain timeout it runs to completion on its worker thread, which does not delay subscription shutdown |
| Pool shutdown | Workers exit after their current job once every pool clone and every handler using it is dropped, which happens when their subscriptions finish |

To give I/O-bound and CPU-bound steps of one pipeline their own concurrency,
retries, and error policies, split them into subscriptions chained with the
[channel adapter](adapters/channel.md): an async handler upstream and a blocking
handler downstream, with the upstream delivery acknowledged after both finish.

Handler retries submit a new job for every attempt. Queue capacity counts jobs
waiting for a thread, not running ones; subscription `concurrency` still bounds
the jobs each subscription submits.

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
| `Rejected` | The handler or a middleware returned `Reject`, including errors propagated with `?` | `DeadLetter` |
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
| Receive `Retry` | Back off and retry receive; reset the failure count after receiving a message |
| Receive `Fatal` or exhausted retry | Stop receiving, drain outstanding work, return an error |
| Value-only input unavailable (tombstone) | `Rejected` routing without invoking the handler |
| Middleware `Reject` | `Rejected` routing with the decoded input; publish no outputs |
| Middleware `Retry` or `Fatal` | Stop without ACK; never rerun the handler or middleware |
| Publish failure | Retry the prepared output; never rerun handler, middleware, or encoding |
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
worker threads; use [blocking handlers](#blocking-handlers) for synchronous work.
Cancellation and timeouts are cooperative and cannot forcibly interrupt
arbitrary synchronous code.

## Observability

The runtime reports through the [`tracing`](https://docs.rs/tracing) and
[`metrics`](https://docs.rs/metrics) facades. Both do nothing until the
application installs a subscriber or recorder, such as `tracing-subscriber`
or a Prometheus exporter. Metric handles are registered when a subscription
starts, so install the recorder before `App::run`.

### Spans

```text
subscription                 subscription = <name>
  └── message                one per delivery
       ├── decode
       ├── handler           one per attempt, with `attempt`
       ├── encode            `Sink::prepare` for every output
       ├── publish           every output, including publish retries
       ├── dead_letter       conversion and dead-letter publication
       ├── ack
       └── commit            transactional subscriptions: every commit attempt
```

A [transactional subscription](#transactions) records `commit` in place of
`publish` and `ack`.

`pre_handler` and `post_handler` middleware run in the `message` span.

### Events

| Event | Level | Fields |
|---|---|---|
| `subscription started` / `subscription stopped` | INFO | |
| `subscription failed` | ERROR | `error` |
| `discarding delivery` | WARN | `failure`, `attempts`, `error` |
| `dead-lettered delivery` | WARN | `failure`, `attempts`, `error` |
| `retrying receive` | WARN | `attempt`, `delay`, `error` |
| `retrying handler` / `retrying publish` | DEBUG | `attempt`, `error` |
| `abandoned revoked delivery` | DEBUG | |

A discarded delivery is always logged and counted, because
`FailureAction::Discard` leaves no other trace.

### Metrics

Every metric carries a `subscription` label with the subscription name.

| Metric | Type | Additional labels | Meaning |
|---|---|---|---|
| `beavers_deliveries_received_total` | counter | | Deliveries received from the source |
| `beavers_deliveries_acknowledged_total` | counter | | Successful ACKs or transaction commits, including after discard and dead-lettering |
| `beavers_delivery_failures_total` | counter | `failure`, `action` | Routable failures by `FailureKind` and the action taken |
| `beavers_handler_retries_total` | counter | | Handler attempts that returned `Retry` and were retried |
| `beavers_publish_failures_total` | counter | `sink` (`output`, `dead_letter`) | Failed publish or transaction commit attempts, including retried ones |
| `beavers_receive_errors_total` | counter | | Failed receive attempts |
| `beavers_deliveries_revoked_total` | counter | | Deliveries abandoned after revocation |
| `beavers_deliveries_in_flight` | gauge | | Received deliveries that have not finished |
| `beavers_stage_duration_seconds` | histogram | `stage` | Duration of `decode`, `handler`, `encode`, `publish`, `complete`, `dead_letter`, `ack`, and `commit` |

`failure` is `decode`, `rejected`, `retry_exhausted`, or `encode`. `action` is
`stop`, `dead_letter`, or `discard`; a `DeadLetter` action without a
dead-letter sink is reported as `stop`, which is what it does. A failure is
counted when it is routed, before the action runs. `handler` durations are
per attempt and exclude retry backoff; `publish` durations cover all outputs
of a delivery, including backoff.

### Exporting metrics to OpenTelemetry

beavers does not bridge the `metrics` facade to OpenTelemetry metrics. An
application that sends metrics through an OpenTelemetry Collector exposes them
in the Prometheus format with
[`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus)
and lets the Collector scrape them.

Install the exporter before `App::run`:

```rust,ignore
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};

PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9000))
    .set_buckets_for_metric(
        Matcher::Full("beavers_stage_duration_seconds".to_owned()),
        &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    )?
    .install()?;

app.run().await?;
```

The listener serves `/metrics` over HTTP. Without configured buckets, the
exporter writes histograms as Prometheus summaries, which arrive as
OpenTelemetry summaries that many backends do not support; configure buckets for
`beavers_stage_duration_seconds` and any histograms the application records
itself.

Scrape the endpoint with the Collector's Prometheus receiver, which is part of
the `otelcol-contrib` distribution, and forward the metrics to any exporter:

```yaml
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: beavers
          scrape_interval: 15s
          static_configs:
            - targets: ["my-app:9000"]

exporters:
  otlp:
    endpoint: my-backend:4317

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [otlp]
```

Counters arrive as cumulative OpenTelemetry sums, the in-flight gauge as a
gauge, and bucketed histograms as histograms. Labels become data-point
attributes, and the receiver maps the scrape's `job` and `instance` to the
`service.name` and `service.instance.id` resource attributes.

### Trace-context propagation

`SourceMessage::propagation_fields` exposes text-map fields received with a
delivery, and `PropagationCarrier` lets an output record accept them. Each
adapter maps the fields to its own metadata:

| Adapter | Received fields | Output fields |
|---|---|---|
| Kafka | Headers with UTF-8 values | Headers; every header of the same name is replaced |
| Pulsar | Properties | Properties |
| Local adapters and `Delivery` | None | Not supported |

With the `opentelemetry` feature, the runtime extracts each `message` span's
remote parent from those fields through the global text-map propagator, and
the `TraceContext` middleware injects the `message` span's context into every
output:

```rust,ignore
use beavers::TraceContext;

opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
// Install a tracing subscriber with a tracing-opentelemetry layer.

Subscription::forward("orders", kafka_source, kafka_sink, handler)
    .middleware(TraceContext::new())
```

Inheritance middleware such as `KafkaInherit` copies every header, including
`traceparent`, unchanged. Without `TraceContext`, an output therefore carries
the received context, and the processing step is invisible to the trace. With
`TraceContext` registered after the inheritance middleware, the injected
context replaces the inherited one, so downstream consumers continue the trace
as children of the processing step. Middleware runs in registration order, and
`Subscription::forward` registers the inheritance middleware first.

## Health checks

`App::health()` returns a cloneable `Health` handle. Obtain it before
`App::run`; it reports every subscription registered with the application.
`Health::report()` returns a `HealthReport` for the current moment:

| Field | Meaning |
|---|---|
| `live` | `false` once a subscription has failed, which stops the application |
| `ready` | The application is running, shutdown has not started, and every subscription is ready |
| `shutting_down` | Shutdown was requested by a signal, the `run_until` token, or a failure |
| `subscriptions` | One `SubscriptionReport` per subscription, in registration order |

Each `SubscriptionReport` carries the subscription `name`, its `status`
(`Pending` before the application runs, then `Running`, `Stopping` while it
drains, and finally `Stopped` or `Failed`), `receive_failures`, the
consecutive failed receives since the last successful one,
`receive_backoff`, whether the runtime is waiting for a receive retry delay,
and `publish_retries`, the deliveries whose output publication or transaction
commit failed and is waiting to be retried. A subscription is ready while it is
`Running` with no receive backoff and no publish retry. A subscription whose
source ended and that stopped without an error stays ready, so a finite source
next to a long-running one does not make the application unready.

Readiness reflects only what the runtime observes. A retried receive that waits
for input counts as ready again, because a source cannot tell an idle broker
from an unreachable one while its receive is pending. Dead-letter publish
retries do not affect readiness. Liveness reports that the runtime answers and
that no subscription failed; it does not detect a handler that never returns.

With the `health` feature, `HealthServer` answers probes over HTTP/1.1 while
the application runs:

```rust,ignore
let server = HealthServer::bind("0.0.0.0:8080".parse()?)?;
App::new()
    .subscription(subscription)
    .health_server(server)
    .run()
    .await?;
```

`GET /livez` answers `200 OK` when the report is live and `GET /readyz` when it
is ready; otherwise each answers `503 Service Unavailable`. Both return the
report as a JSON body. Other paths answer `404 Not Found` and other methods
`405 Method Not Allowed`. `HealthServer::bind` binds the listener immediately,
so address errors surface before the application runs and `local_addr`
reports the port chosen for port 0. The server starts with the application and
stops after every subscription has finished. Readiness therefore turns `503`
as soon as shutdown starts, which removes an instance serving an
[HTTP source](adapters/http.md) from a Kubernetes Service while it drains,
and liveness keeps answering until draining completes. Use the readiness probe
for traffic routing and the liveness probe for restarts:

```yaml
livenessProbe:
  httpGet: { path: /livez, port: 8080 }
readinessProbe:
  httpGet: { path: /readyz, port: 8080 }
```

The health server does not export metrics; [metrics](#metrics) go through the
`metrics` facade and the recorder the application installs.

## Implementation limits

Kafka and Pulsar adapters, broker record types, Protobuf, and Avro codecs are
implemented. Kafka commits up to the first unfinished offset per partition,
schedules work per partition, and abandons revoked work. Kafka-to-Kafka
subscriptions can publish and commit offsets in one Kafka transaction per
delivery; other pairs are at-least-once. Pulsar uses individual
acknowledgements, schedules work by its subscription type's ordering scope, and
provides no transactions or exactly-once processing. NATS JetStream,
SQS and adapter pause/resume backpressure are not implemented. Async handler
futures run on the shared Tokio executor without isolation. Metadata inheritance
is limited to same-platform middleware;
cross-platform mappings are application-written `MapMetadata` functions.

Inputs currently require `Clone + Send + Sync`. Stdin and stdout construct
`Default` codecs internally and do not yet accept configured codec instances.
See [architecture](architecture.md) for extension contracts and the
[roadmap](plan.md#implementation-order) for the intended sequence.
