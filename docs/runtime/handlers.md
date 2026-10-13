# Handlers

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
RabbitMQ uses `RabbitMqInherit::new()`, SQS uses `SqsInherit::new()`,
Pub/Sub uses `PubSubInherit::new()`, and Azure Service Bus uses
`ServiceBusInherit::new()`. A
record whose value cannot be represented as a plain value, such as a Kafka null
value, is rejected without invoking a value handler and routed by the error
policy as a `Rejected` failure. Register `Tombstones` to choose another policy,
or take the record to handle it.

The shape is inferred from types, so a closure's parameter must be annotated,
and a sink whose item type is generic, such as `InMemorySink::default()`, may
need its item type spelled out when a value and a publish record would both
fit it. Sources whose item is not a platform record, such as `IterSource<T>`
or `ChannelSource<T>`, accept only record-shaped handlers taking `T`.

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
[channel adapter](../adapters/channel.md): an async handler upstream and a blocking
handler downstream, with the upstream delivery acknowledged after both finish.

Handler retries submit a new job for every attempt. Queue capacity counts jobs
waiting for a thread, not running ones; subscription `concurrency` still bounds
the jobs each subscription submits.
