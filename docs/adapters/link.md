# Link adapter

The link adapter chains subscriptions inside one application while keeping the
upstream delivery unacknowledged until the downstream subscription finishes the
value. Each stage keeps its own handler, concurrency, retries, error policy, and
dead-letter sink, so a pipeline can separate I/O-bound work in async handlers
from CPU-bound work in [blocking handlers](../runtime.md#blocking-handlers):

```rust,ignore
use beavers::{App, BlockingPool, Subscription, blocking, link};

let (to_score, fetched) = link(16);
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

`link(capacity)` returns a `LinkSink<T>` for the upstream subscription and a
`LinkSource<T>` for the downstream one. Values are typed and are not encoded. A
capacity of zero panics, following `tokio::sync::mpsc::channel`. Links can be
chained to build pipelines of more than two stages.

## Delivery and completion

A publication to `LinkSink` succeeds only after the downstream subscription has
acknowledged the linked value: all of its outputs were published, or its error
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
`BlockingPool` for the work that stage can run in parallel. The link capacity
bounds values waiting to be received by the downstream subscription.

Outputs of one upstream delivery, including each value of `Emit::Many`, are
linked one at a time. Because an upstream key's delivery does not finish before
the downstream stage does, `ProcessingOrder::PerKey` upstream also keeps the
downstream processing of one key in order. Linked deliveries carry no ordering
key of their own.

## Revocation

When the upstream publication is dropped before the downstream stage completes
it, the linked delivery is revoked: the downstream runtime abandons it without
ACK and without a failure, and a value still buffered in the link is skipped.
This happens when the upstream delivery is revoked, for example by a Kafka
partition revocation, or when the upstream drain timeout cancels its job. The
upstream delivery is redelivered to its new owner.

## Shutdown

Application shutdown does not stop a `LinkSource` subscription from receiving.
The upstream subscription stops receiving and drains its running jobs, which
wait for the downstream stage. After the upstream subscription finishes it
closes its `LinkSink`, and the downstream subscription receives
`Receive::End` once the buffered values have been received, then drains and
stops. In-flight values therefore complete end to end within the upstream
drain timeout.

A downstream subscription ends only when its `LinkSink` is closed or dropped,
so register the upstream subscription in the same `App`. A subscription
failure stops the downstream subscription as usual.

`LinkSink` is not cloneable: each link connects exactly one upstream
subscription to one downstream subscription. Closing either end more than once
is safe. Closing the source rejects later publications.
