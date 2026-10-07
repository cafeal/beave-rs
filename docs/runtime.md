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
`Subscription::new`, `Subscription::new_emitting`, and `App::subscribe`. The name appears in subscription errors, dead letters, spans,
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
| Error policy | `ErrorPolicy::default()` | Dead-letter handler failures and rejected publications; stop on decode and encode failures |
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
The RabbitMQ source uses its queue when configured as ordered. The SQS source
uses the message group of a FIFO queue. Local adapters provide no key.

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
sink transactions that each commit a batch of deliveries:

```text
first delivery: verify_source(delivery)
… → prepare all outputs → enlist in a batch → commit(batch)
```

```rust,ignore
Subscription::new("orders", kafka_source, kafka_transactional_sink, handler)
    .transactional()
    .transaction_batch(TransactionBatch::new(100, Duration::from_millis(10)))
```

The method only compiles when the sink implements
`TransactionalSink<Source::Message, Output>`, which an adapter provides for the
source messages whose acknowledgement can join its transactions. The
Kafka adapter implements it for a `KafkaSource` and a `KafkaTransactionalSink`;
see the [Kafka guide](adapters/kafka.md#transactions). The runtime never calls
the delivery's own ACK in a transactional subscription.

### Batches

A delivery whose outputs are prepared joins the open batch, and its job ends
and frees its concurrency slot and ordering key, as a
[pending completion](#per-message-lifecycle) does. The delivery counts as
acknowledged when its batch commits. A batch closes when it holds
`max_deliveries` deliveries or `max_linger` after its first delivery joined,
whichever comes first:

| `TransactionBatch` field | Default | Meaning |
|---|---|---|
| `max_deliveries` | 100 | Maximum deliveries committed in one transaction |
| `max_linger` | 10 ms | Maximum wait of a batch's first delivery for more deliveries |

Set it with `Subscription::transaction_batch` or
`SubscriptionConfig::transaction_batch`; a zero `max_deliveries` fails
validation. `TransactionBatch::single()` commits every delivery in its own
transaction without waiting. Other subscriptions ignore the setting.

Batches of one subscription commit one at a time, in a task of their own, so a
commit finishes or aborts even when its jobs are abandoned. Deliveries that
complete while a batch commits form the next batch, which commits as soon as
the previous one ends if it is already full or its linger has passed. At most
one further batch of deliveries waits for a commit: when it is full, the next
delivery's job waits to join, which bounds deliveries waiting for a commit
outside `max_in_flight`. At the end of input and on shutdown, the last batch
commits after its linger, within the drain timeout.

`commit` publishes every output of the batch and acknowledges every delivery
in it atomically. A failed commit leaves none in effect, and the whole batch is
retried with the same prepared outputs under the subscription's
`publish_retry` policy. Before each attempt, deliveries that were revoked or
abandoned leave the batch; an attempt that fails while a delivery of the batch
is revoked is retried without it and does not count against the policy. An
exhausted retry stops the subscription without acknowledging any delivery of
the batch, and later batches fail without committing, because a later
delivery's acknowledgement could cover a failed delivery of the same ordering
scope. A commit error marked with `PublishRejected` is not retried and fails the
batch the same way: it cannot be attributed to one delivery, so the error policy
does not route it.

A delivery that completes without output, including one that emitted nothing,
was discarded, or was dead-lettered, joins a batch without outputs. Dead
letters are published by the dead-letter sink before the delivery joins and are
not part of the transaction, so they remain at-least-once.

### Source verification

Types cannot tell whether a source and a sink of the same platform connect to
the same cluster, and a transaction can only acknowledge a delivery of its own
cluster. The runtime therefore passes the first delivery of a transactional
subscription to `TransactionalSink::verify_source` before processing it. The
check is retried under the `publish_retry` policy, and a failure stops the
subscription before any handler runs or anything is published or
acknowledged. Shutdown during the check leaves the delivery unacknowledged.

### Ordering

A transactional subscription requires `ProcessingOrder::PerKey`. Deliveries of
one ordering scope then join batches in receive order, and a batch lists them
in that order, so the acknowledgement of a scope's last delivery in a batch
covers every earlier delivery of that scope. Other orderings fail validation
before the application starts.

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
to Kafka, `PulsarInherit` for Pulsar to Pulsar, `RabbitMqInherit` for
RabbitMQ to RabbitMQ, and `SqsInherit` for SQS to SQS. They copy user-controlled metadata and never copy delivery
facts such as offsets, partitions, message IDs, routing keys, or broker
timestamps. See the [Kafka](adapters/kafka.md#metadata-inheritance),
[Pulsar](adapters/pulsar.md#metadata-inheritance),
[RabbitMQ](adapters/rabbitmq.md#metadata-inheritance), and
[SQS](adapters/sqs.md#metadata-inheritance) guides. A handler that
returns the sink's publish record carries no received metadata into outputs
unless middleware maps it; a handler that returns a plain value inherits the
platform's defaults, as described in [handler shapes](#handler-shapes).

## Handler shapes

`App::subscribe`, `Subscription::new`, and `Subscription::new_emitting` accept
a handler that takes either the received record or its value, and returns
either the sink's output type or a plain value. The shape is chosen at compile
time from the handler's signature:

```rust,ignore
// Value in, value out: the output inherits the input's key and headers.
async fn enrich(order: Order) -> Result<Order> { .. }
// Record in, value out: read the metadata, keep the default inheritance.
async fn route(record: KafkaRecord<Order>) -> Result<Order> { .. }
// Value in, publish record out: the handler sets every output field.
async fn rekey(order: Order) -> Result<KafkaPublish<Order>> { .. }
// Record in, publish record out: the handler controls everything.
async fn custom(record: KafkaRecord<Order>) -> Result<KafkaPublish<Order>> { .. }

App::new()
    .subscribe("enrich", kafka_source, kafka_sink, enrich)
```

| Input | Handler parameter | Requirement |
|---|---|---|
| `ByRecord` | `SourceItem<S>`, such as `KafkaRecord<T>` or an `IterSource` item | none |
| `ByValue` | the record's value, `T` | the record implements `ValueRecord` |

| Output | Handler result | Published as |
|---|---|---|
| `ByRecord` | the sink's type, such as `KafkaPublish<T>` | returned |
| `ByValue` | a plain value `U` | the source platform's publish record built from `U`, after the platform's default inheritance |

A value output requires the source record to implement `SamePlatform<U>` and
the sink to accept that platform's publish type, so a Kafka value cannot be
sent to a Pulsar sink without a record handler and an explicit mapping. The
runtime registers the platform's default inheritance as the first middleware
for a value output, so outputs keep the input metadata without the handler
handling it; further `.middleware(...)` registrations run after it. Every value
emitted through `new_emitting` inherits from the same input. A publish record
output carries no received metadata unless middleware maps it.

Kafka uses `KafkaInherit::new()`, Pulsar uses `PulsarInherit::new()`,
RabbitMQ uses `RabbitMqInherit::new()`, and SQS uses `SqsInherit::new()`. A
record whose value cannot be represented as a plain value, such as a Kafka null
value, is rejected without invoking a value handler and routed by the error
policy as a `Rejected` failure. Register `Tombstones` to choose another policy,
or take the record to handle it.

The shape is inferred from types, so a closure's parameter must be annotated,
and a sink whose item type is generic, such as `InMemorySink::default()`, may
need its item type spelled out when a value and a publish record would both
fit it. Sources whose item is not a platform record, such as `IterSource<T>`
or `ChannelSource<T>`, accept only record-shaped handlers taking `T`.

## Tombstones

A tombstone is a received record whose value is null. Producers send them to
delete a key in a compacted topic, and change-data-capture tools emit them
after deleted rows; append-only event streams normally never contain them.
Adapters mark such records through `TombstoneRecord`: Kafka and Pulsar records
with `value: None`.

Register `Tombstones` to decide their handling before the handler runs:

```rust,ignore
Subscription::new("orders", kafka_source, kafka_sink, handler)
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

`ErrorPolicy` decides what happens to a delivery after one of five routable
failures, identified by `FailureKind`:

| `FailureKind` | Cause | Default action |
|---|---|---|
| `Decode` | `SourceMessage::decode` failed | `Stop` |
| `Rejected` | The handler or a middleware returned `Reject`, including errors propagated with `?` | `DeadLetter` |
| `RetryExhausted` | The handler returned `Retry` on its final permitted attempt | `DeadLetter` |
| `Encode` | `Sink::prepare` failed for an emitted output | `Stop` |
| `PublishRejected` | The sink's destination refused an output permanently | `DeadLetter` |

Each failure maps to one `FailureAction`:

- `Stop` returns an error and leaves the delivery unacknowledged, so a durable
  broker can redeliver it after restart.
- `DeadLetter` publishes a `DeadLetter` envelope to the dead-letter sink and
  then acknowledges the delivery.
- `Discard` acknowledges the delivery without publishing anything. It is an
  explicit choice to lose that delivery.

Rejections, exhausted handler retries, and rejected publications without a
configured dead-letter sink stop without ACK: the input could not be processed
and nothing can receive it. `DeadLetter` for decode or encode failures requires a dead-letter
sink and fails validation otherwise. `ErrorPolicy::dead_letter_all()` routes
every kind to the dead-letter sink.

Encoding happens for all emitted outputs before any publication, so an `Encode`
failure never follows a partial publish; dead-lettering the input after it does
not duplicate outputs.

A sink reports a permanent refusal, such as an HTTP `4xx` response, by marking
its publish, submit, or commit error with `PublishRejected::wrap`. The runtime
does not retry a rejected output and submits no later outputs of the delivery.
Outputs submitted before it stay published, and their completions are awaited
before the delivery is routed, so a `DeadLetter` or `Discard` action
acknowledges it only after they reached the sink's acknowledgement boundary.
Errors without the marker are retried under `publish_retry`, and an exhausted
retry stops the subscription. A failed completion is never routed. A
[transactional subscription](#batches) cannot route a rejected commit either.

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
| RabbitMQ | `RabbitMqRecord<Vec<u8>>`: body bytes, headers, properties, and delivery metadata |
| SQS | `SqsRecord<Vec<u8>>`: body bytes, message attributes, and message metadata |
| Stdin | `Vec<u8>`: the received line |
| `Delivery` (`IterSource`) | `()`: input is already typed |
| Channel | `ChannelRaw`: the raw form of the upstream delivery that produced the value |

`SourceMessage::Raw` implements `Serialize`, so `DeadLetter` implements it
whenever its input does and a JSON sink can publish it directly.

Conversion and preparation run once. Publication then retries under the
`dlq_retry` policy. A conversion, preparation, or exhausted publication failure
stops without ACK; dead-letter failures are never routed again.

### Forwarding dead letters to a broker topic

`KafkaPublish::from_dead_letter`, `PulsarPublish::from_dead_letter`,
`RabbitMqPublish::from_dead_letter`, and `SqsPublish::from_dead_letter` convert a dead letter into a record for a
dead-letter topic or queue of the same platform. The record keeps the original
key, value bytes, and headers or properties (and the Pulsar event time), so the
payload can be inspected with ordinary broker tools and decoded by the same
codec as the original topic. Failure details are added as text headers (Kafka),
properties (Pulsar), or string headers (RabbitMQ). SQS accepts at most 10
message attributes, so the SQS adapter writes the same names and values as one
JSON object into the `beavers-dlq-details` string attribute:

| Name | Value |
|---|---|
| `beavers-dlq-subscription` | Subscription that dead-lettered the payload |
| `beavers-dlq-failure` | `FailureKind` name, such as `rejected` |
| `beavers-dlq-error` | Error message including its context chain |
| `beavers-dlq-attempts` | Handler attempts made |
| `beavers-dlq-count` | Times the payload has been dead-lettered, including this one |
| `beavers-dlq-origin-topic` | Kafka or Pulsar topic the payload was first received from |
| `beavers-dlq-origin-partition`, `-offset`, `-timestamp` | Kafka location and record timestamp of the first receipt |
| `beavers-dlq-origin-message-id`, `-publish-time` | Pulsar message ID (`ledger:entry:partition:batch`) and publish time of the first receipt |
| `beavers-dlq-origin-queue`, `-exchange`, `-routing-key` | RabbitMQ queue of the first receipt, and the exchange and routing key it was published with |
| `beavers-dlq-origin-queue-url`, `-message-id` | SQS queue URL and message ID of the first receipt |

The sink must publish raw bytes:

```rust,ignore
let dead_letters = KafkaSink::<RawBytes, Vec<u8>>::new(KafkaSinkConfig::new(brokers, "orders-dlq"));

Subscription::new("orders", source, sink, handle)
    .dlq_with(dead_letters, |dead_letter| Ok(KafkaPublish::from_dead_letter(dead_letter)));
```

When the dead-lettered record was itself received from a dead-letter topic, the
origin headers are kept and the count is incremented, while the other details
describe the latest failure. Previous dead-letter headers that are malformed
are replaced as if the payload had never been dead-lettered.
`KafkaDeadLetter::from_record`, `PulsarDeadLetter::from_record`,
`RabbitMqDeadLetter::from_record`, and `SqsDeadLetter::from_record` read the details back from a received record
and return `None` for a record without them. The inheritance middleware of each
adapter never copies names starting with
`DEAD_LETTER_HEADER_PREFIX` into outputs.

The dead-letter record's own timestamp or publish time is when it was
dead-lettered. A subscription downstream of a [channel](adapters/channel.md)
receives a `ChannelRaw`; `DeadLetter::try_map_raw` reads it as the upstream
record before conversion.

### Reprocessing dead letters

beavers has no dedicated redrive command. A dead-letter topic is an ordinary
topic, so dead letters are reprocessed by a subscription whose source reads the
dead-letter topic and whose handler is the original one, or a wrapper around
it that inspects `KafkaDeadLetter::from_record` first:

```rust,ignore
let source = KafkaSource::<Json, Order>::new(KafkaSourceConfig::new(
    brokers,
    "orders-redrive",
    ["orders-dlq"],
));

Subscription::new("orders-redrive", source, sink, |record: KafkaRecord<Order>| async move {
    let dead_letter = KafkaDeadLetter::from_record(&record).reject()?;
    if dead_letter.is_some_and(|dead| dead.details.count >= 3) {
        return Err(HandlerError::Fatal(anyhow::anyhow!("dead-lettered too often")));
    }
    handle(record).await
})
.dlq_with(dead_letters, |dead_letter| Ok(KafkaPublish::from_dead_letter(dead_letter)));
```

Reading the dead-letter topic, rather than publishing dead letters back to the
original topic, keeps other consumers of the original topic from receiving
them again. Two operating styles are common:

- **After a fix.** An operator runs the subscription once the cause is fixed
  and stops it once its consumer lag reaches zero.
- **Continuously, for transient failures.** The subscription runs alongside
  the original one and waits before reprocessing: the handler sleeps until the
  record's timestamp plus a delay. Later records of a partition are newer, so
  waiting at the head of the partition delays them by no more than the same
  delay. Payloads that keep failing return to the dead-letter topic, so the
  handler must stop them after a limit on `details.count`, for example by
  failing or by routing them to a final topic that nothing reads automatically.

Reprocessed payloads arrive after newer payloads with the same key, so a
handler that overwrites state by key can replace newer state with older state.
A Kafka handler that sleeps longer than `max.poll.interval.ms` (5 minutes by
default) while `max_in_flight` deliveries are waiting makes the consumer leave
its group; raise that property for longer delays. Shutdown does not wait for a
sleeping handler beyond `drain_timeout`; its delivery stays unacknowledged and
is received again.

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
| Publish rejected by the destination | `PublishRejected` routing without retry |
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
       └── ack
  └── commit                 transactional subscriptions: every attempt to commit
                             a batch, with `deliveries` and `outputs`
```

A [transactional subscription](#transactions) records no `publish` or `ack`
span. Its `commit` spans belong to the subscription, not to a delivery,
because one transaction commits several deliveries.

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
| `beavers_transaction_deliveries` | histogram | | Deliveries in each committed transaction |

`failure` is `decode`, `rejected`, `retry_exhausted`, `encode`, or
`publish_rejected`. `action` is
`stop`, `dead_letter`, or `discard`; a `DeadLetter` action without a
dead-letter sink is reported as `stop`, which is what it does. A failure is
counted when it is routed, before the action runs. `handler` durations are
per attempt and exclude retry backoff; `publish` durations cover all outputs
of a delivery, including backoff. `commit` durations are per successful
transaction and cover the whole batch.

### Alerting

beavers sends no notifications itself. Alert on the metrics with a monitoring
system and let it route alerts, since per-message notifications flood a channel
when many deliveries fail at once. The
[monitoring and alerting guide](monitoring-and-alerting/README.md) lists the
signals worth alerting on, PromQL alert rules, and configurations for
Kubernetes, AWS, Google Cloud, and Azure. A failed subscription also makes the
[liveness probe](#health-checks) fail.

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
| RabbitMQ | String headers | String headers |
| SQS | String message attributes | String message attributes |
| Channel | The sender's trace context, captured when a value is enqueued | Captured automatically; no carrier needed |
| `Delivery` | Fields set with `Delivery::with_propagation_fields` | Not supported |
| Other local adapters | None | Not supported |

With the `opentelemetry` feature, the runtime extracts each `message` span's
remote parent from those fields through the global text-map propagator, and
the `TraceContext` middleware injects the `message` span's context into every
output:

A [channel](adapters/channel.md) needs no middleware. With the
`opentelemetry` feature, `ChannelSink` and `ChannelSender` capture the current
span's context when they enqueue a value, so the receiving subscription's
`message` span continues the trace as a child of the sending stage.

```rust,ignore
use beavers::TraceContext;

opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
// Install a tracing subscriber with a tracing-opentelemetry layer.

Subscription::new("orders", kafka_source, kafka_sink, handler)
    .middleware(TraceContext::new())
```

Inheritance middleware such as `KafkaInherit` copies every header, including
`traceparent`, unchanged. Without `TraceContext`, an output therefore carries
the received context, and the processing step is invisible to the trace. With
`TraceContext` registered after the inheritance middleware, the injected
context replaces the inherited one, so downstream consumers continue the trace
as children of the processing step. Middleware runs in registration order, and
a handler returning a plain value registers the inheritance middleware first.

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

Kafka, Pulsar, RabbitMQ, and SQS adapters, broker record types, Protobuf, and Avro
codecs are implemented. Kafka commits up to the first unfinished offset per partition,
schedules work per partition, and abandons revoked work. Kafka-to-Kafka
subscriptions can publish and commit offsets in batched Kafka transactions;
other pairs are at-least-once. Pulsar uses individual
acknowledgements, schedules work by its subscription type's ordering scope, and
provides no transactions or exactly-once processing. RabbitMQ uses individual
acknowledgements and revokes the deliveries of a lost channel. SQS deletes each
acknowledged message, extends the visibility of held messages, and revokes a
delivery whose visibility could not be kept. NATS JetStream and adapter pause/resume backpressure are not implemented. Async handler
futures run on the shared Tokio executor without isolation. Metadata inheritance
is limited to same-platform middleware;
cross-platform mappings are application-written `MapMetadata` functions.

Inputs currently require `Clone + Send + Sync`.
See [architecture](architecture.md) for extension contracts and the
[roadmap](plan.md#implementation-order) for the intended sequence.
