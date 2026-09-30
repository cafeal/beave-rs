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
`channel` directly. The call returns once the downstream subscription has
finished the value. To use subscriptions as an in-process worker framework
instead, use the [application ends](#application-ends).

## Delivery and completion

A publication to `ChannelSink` succeeds only after the downstream subscription has
acknowledged the value: all of its outputs were published, or its error
policy dead-lettered or discarded the value. The upstream subscription
acknowledges its own delivery after that, so a process failure at any point
before the final stage completes leaves the original broker delivery
unacknowledged and it is redelivered. Delivery remains at-least-once: a value
the downstream stage already published can be published again after
redelivery.

When the downstream subscription stops without acknowledging a value, for
example after a fatal handler error, the upstream publication fails and follows
the upstream publish retry policy.

An upstream job holds its concurrency slot and its `max_in_flight` entry until
the downstream stage finishes, so the upstream limits bound the work in flight
across the whole chain. Size the upstream `concurrency` for the number of values
that should be in flight end to end, and the downstream `concurrency` or
`BlockingPool` for the work that stage can run in parallel. The channel capacity
bounds values waiting to be received by the downstream subscription.

Outputs of one upstream delivery, including each value of `Emit::Many`, are
sent one at a time. Because an upstream key's delivery does not finish before
the downstream stage does, `ProcessingOrder::PerKey` upstream also keeps the
downstream processing of one key in order. Linked deliveries carry no ordering
key of their own.

## Revocation

When the upstream publication is dropped before the downstream stage completes
it, the channel delivery is revoked: the downstream runtime abandons it without
ACK and without a failure, and a value still buffered in the channel is skipped.
This happens when the upstream delivery is revoked, for example by a Kafka
partition revocation, or when the upstream drain timeout cancels its job. The
upstream delivery is redelivered to its new owner.

## Shutdown

Application shutdown does not stop a `ChannelSource` subscription from receiving.
The upstream subscription stops receiving and drains its running jobs, which
wait for the downstream stage. After the upstream subscription finishes it
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
async fn main() -> anyhow::Result<()> {
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
failure before then leaves the input unacknowledged. Each waiting job holds an
upstream concurrency slot, so upstream work runs ahead of `recv` by at most its
`concurrency`. Dropping the receiver fails publications still waiting for it.
`ChannelReceiver::recv` returns `None` after every sink clone is closed or
dropped and the buffer is empty.

A source created by `ChannelSource::bounded` stops receiving on application
shutdown like other sources, and ends once every `ChannelSender` clone is
dropped. All waits for capacity can be cancelled without enqueueing the value.
