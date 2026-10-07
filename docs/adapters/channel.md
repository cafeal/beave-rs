# Channel adapter

The channel adapter chains subscriptions inside one application while keeping the
upstream delivery unacknowledged until the downstream subscription finishes the
value. Each stage keeps its own handler, concurrency, retries, error policy, and
dead-letter sink, so a pipeline can separate I/O-bound work in async handlers
from CPU-bound work in [blocking handlers](../runtime.md#blocking-handlers):

```rust,ignore
use beavers::{App, BlockingPool, Subscription, blocking, channel};

let (to_score, fetched) = channel(16);
App::new()
    // I/O-bound: many concurrent async calls on the Tokio executor.
    .subscription(Subscription::new("fetch", kafka_source, to_score, fetch).concurrency(64))
    // CPU-bound: synchronous calls on a pool sized for the machine.
    .subscription(
        Subscription::new("score", fetched, kafka_sink, pool.blocking(score)).concurrency(8),
    )
    .run()
    .await?;
```

`channel(capacity)` returns a `ChannelSink<T>` for the upstream subscription and
a `ChannelSource<T>` for the downstream one. Values are typed and are not
encoded. A capacity of zero panics, following `tokio::sync::mpsc::channel`.
Channels can be chained to build pipelines of more than two stages.

Application code can also call `Sink::publish` on a `ChannelSink` created by
`channel` directly, with a `ChannelOutput::from(value)`. The call returns once
the downstream subscription has finished the value. To use subscriptions as an in-process worker framework
instead, use the [application ends](#application-ends).

## Delivery and completion

A subscription publishing to a `ChannelSink` submits each output: the job ends
and frees its concurrency slot as soon as the value is enqueued, while the
input stays unacknowledged. The upstream subscription acknowledges its delivery
only after the downstream subscription has acknowledged the value: all of its
outputs were published, or its error policy dead-lettered or discarded the
value. A process failure at any point before the final stage completes leaves
the original broker delivery unacknowledged and it is redelivered. Delivery
remains at-least-once: a value the downstream stage already published can be
published again after redelivery.

When the downstream subscription stops without acknowledging a value, for
example after a fatal handler error, the upstream completion fails and the
upstream subscription stops without acknowledging the input.

| Limit | Bounds |
|---|---|
| Upstream `concurrency` | Upstream handler jobs running at once |
| Channel capacity | Finished values waiting for the downstream subscription; a job waits for space before it ends |
| Upstream `max_in_flight` | Upstream deliveries received and not yet enqueued |
| Downstream `concurrency` or `BlockingPool` | Work the downstream stage runs in parallel |

Upstream work therefore runs ahead of the downstream stage by up to the
channel capacity, independently of its own concurrency and `max_in_flight`.
Upstream deliveries whose values were enqueued stay unacknowledged but do not
count toward `max_in_flight`; they are bounded by the channel capacity plus the
deliveries the downstream subscription is processing. Those also widen the
range a durable source redelivers after a crash.

Outputs of one upstream delivery, including each value of `Emit::Many`, are
enqueued in order, and the delivery is acknowledged after all of them complete.
With `ProcessingOrder::PerKey`, the next delivery of a key starts once the
previous one has enqueued its outputs, so values of one key enter the channel in
order.

Each channel delivery carries the ordering key of the upstream delivery that
produced it, such as a Kafka topic partition. A downstream subscription with
`ProcessingOrder::PerKey` therefore processes the values of one upstream key in
order, while values of other keys run concurrently. Values sent by application
code have no ordering key.

## Metadata and tracing

A downstream handler receives only the typed value. Broker metadata of the
original delivery, such as Kafka keys and headers, reaches the handler only when
the value contains it: an upstream handler can return a `KafkaRecord<U>`
built from its input, and the downstream stage can then return a plain value into a Kafka sink to
inherit that metadata.

The channel also keeps the upstream delivery's raw form, captured with
`Sink::prepare_from` when the upstream subscription prepares the value. The
downstream delivery's raw form is a `ChannelRaw` holding it, so downstream dead
letters carry the value as their input and the original record, such as a
`KafkaRecord<Vec<u8>>` with its payload bytes, key, headers, and offset, as
their raw delivery. Through several channels, `ChannelRaw` stays the delivery
of the first subscription. It serializes as that record, so a JSON dead-letter
sink publishes it unchanged, and `ChannelRaw::downcast_ref` reads it as its
concrete type in `dlq_with`, and `DeadLetter::try_map_raw` replaces it so
the dead letter can be [forwarded to a broker topic](../runtime.md#forwarding-dead-letters-to-a-broker-topic):

```rust,ignore
type Raw = KafkaRecord<Vec<u8>>;

Subscription::new("score", fetched, kafka_sink, score)
    .dlq_with(dead_letter_sink, |dead_letter: DeadLetter<Document, ChannelRaw>| {
        let dead_letter = dead_letter.try_map_raw(|raw| {
            raw.downcast_ref::<Raw>().cloned().context("no Kafka record")
        })?;
        Ok(KafkaPublish::from_dead_letter(dead_letter))
    });
```

Values sent by application code have an empty `ChannelRaw`, which serializes
as `None`.

With the `opentelemetry` feature, the channel carries the sender's trace
context with each value, and the downstream `message` span continues that
trace. See [trace-context propagation](../runtime.md#trace-context-propagation).

## Revocation

When the upstream subscription stops waiting before the downstream stage
completes the value, the channel delivery is revoked: the downstream runtime abandons it without
ACK and without a failure, and a value still buffered in the channel is skipped.
This happens when the upstream delivery is revoked, for example by a Kafka
partition revocation, or when the upstream drain timeout cancels its wait. The
upstream delivery is redelivered to its new owner.

## Shutdown

Application shutdown does not stop a `ChannelSource` subscription from receiving.
The upstream subscription stops receiving and drains its running jobs and the
acknowledgements waiting for the downstream stage. After the upstream subscription finishes it
closes its `ChannelSink`, and the downstream subscription receives
`Receive::End` once every sink clone is closed or dropped and the buffered
values have been received, then drains and stops. In-flight values therefore complete end to end within the upstream
drain timeout.

A downstream subscription ends only when every `ChannelSink` clone is closed or
dropped, so register the upstream subscriptions in the same `App` and drop any
sink held by application code during shutdown. A subscription failure stops
the downstream subscription as usual.

## Fan-in and closing

Clone a `ChannelSink` to feed one downstream subscription from several
upstream subscriptions. Each clone waits only for the values it published. Each
clone also has its own close state: closing one rejects later publications
from that clone without affecting the others, and the downstream source ends
after all of them are closed or dropped. Closing either end more than once is
safe. Closing the source rejects later publications from every clone.

## Application ends

`ChannelSource::bounded(capacity)` returns a `ChannelSender<T>` and a source, and
`ChannelSink::bounded(capacity)` returns a sink and a `ChannelReceiver<T>`. They
let application code send values to a subscription and receive its outputs,
without an external broker:

```rust
use beavers::{App, CancellationToken, ChannelSink, ChannelSource};

#[tokio::main]
async fn main() -> Result<(), beavers::BoxError> {
    let (sender, input) = ChannelSource::bounded(16);
    let (output, mut results) = ChannelSink::bounded(16);
    let worker = tokio::spawn(
        App::new()
            .subscribe("double", input, output, |n: i32| async move { Ok(n * 2) })
            .run_until(CancellationToken::new()),
    );

    sender.send(21).await?;
    assert_eq!(results.recv().await, Some(42));
    drop(sender);
    worker.await??;
    Ok(())
}
```

| End | Completes when | Shutdown |
|---|---|---|
| `ChannelSender::send` | The value is enqueued | The source stops receiving; buffered values are dropped |
| `ChannelSender::send_and_wait` | The subscription acknowledges the value | A value the subscription did not finish returns an error |
| `ChannelSink::bounded` publication | `ChannelReceiver::recv` takes the value | The upstream subscription drains while it waits for `recv` |

A subscription publishing to a `ChannelSink::bounded` sink acknowledges its
input only after application code takes the output with `recv`, so a process
failure before then leaves the input unacknowledged. As with a downstream
subscription, upstream work runs ahead of `recv` by up to the channel capacity. Dropping the receiver fails the completions of values
still waiting for it, which stops the upstream subscription.
`ChannelReceiver::recv` returns `None` after every sink clone is closed or
dropped and the buffer is empty.

A source created by `ChannelSource::bounded` stops receiving on application
shutdown like other sources, and ends once every `ChannelSender` clone is
dropped. All waits for capacity can be cancelled without enqueueing the value.
