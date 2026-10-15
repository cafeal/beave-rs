# State

A stateful handler keeps a value across deliveries, such as a running total
per customer, the IDs already seen for deduplication, or the last price per
symbol. It takes its state as a second parameter and is registered with
`stateful(store, handler)`:

```rust,ignore
use beavers::state::{InMemoryState, State, stateful};

#[derive(Clone, Default)]
struct Count {
    orders: u64,
}

// Alert on a customer's tenth order.
async fn count(order: Order, mut count: State<Count>) -> Result<Emit<Alert>> {
    count.orders += 1;
    Ok(if count.orders == 10 {
        Emit::One(Alert::new(order.customer_id))
    } else {
        Emit::None
    })
}

let counts = InMemoryState::<Count>::new();
App::new().subscription(Subscription::new_emitting(
    "tenth-order",
    orders,
    alerts,
    stateful(counts, count),
));
```

`stateful` registers wherever a handler does, with `App::subscribe`,
`Subscription::new`, and `Subscription::new_emitting`. The input and output
shapes are chosen as for other [handlers](handlers.md): the first parameter is
the record or its value, and the result is a plain value or the sink's
publish type.

## Lifecycle of a delivery

For each handler attempt the runtime asks the store to begin a state change,
runs the handler with the state, and keeps the change until the delivery
completes:

1. The store loads the state for the delivery's key.
2. The handler runs with the state and changes it.
3. The outputs are published, and pending completions finish.
4. The state change is committed.
5. The delivery is acknowledged.

| Event | State change |
|---|---|
| The handler fails and is retried | Discarded; the next attempt starts from the stored state |
| The delivery is dead-lettered or discarded, including encode and publish rejections | Not applied |
| Publishing fails and the subscription stops | Not applied; the redelivery starts from the stored state |
| The commit fails | The subscription stops without acknowledging the delivery |
| The process stops after the commit but before the acknowledgement | Applied; see [redeliveries](#redeliveries) |

A handler that only reads its state commits nothing. A
[transactional](transactions.md) subscription cannot run a stateful handler,
because the state change could not commit inside the sink's transaction;
registration fails validation.

## Keys

State is kept per key. By default the key is the delivery's message key:

| Source | Message key |
|---|---|
| Kafka | Record key |
| Pulsar | Message key |
| SQS | Message group ID |
| Pub/Sub | Ordering key |
| Service Bus | Session ID |

A single delivery without one, such as a Kafka record with a null key or an
SQS message sent without a group ID, is rejected and routed by the error
policy. Choose another key from the handler's input with `key`:

```rust,ignore
let counts = InMemoryState::<Count>::new().key(|order: &Order| order.customer_id.to_string());
```

A key closure receives the handler's input, so it takes the value for a value
handler and the record for a record handler. Sources without message keys,
such as RabbitMQ, HTTP, stdin, channels, and `IterSource`, need a key
closure: with the default key, the subscription fails to compile, with a note
to choose a key with `.key(..)`. A source's messages have message keys when
they implement `KeyedMessage`.

In `InMemoryState`, deliveries with the same key take turns: a delivery waits
to begin until the previous delivery with that key has committed or abandoned
its change, so each sees the last committed state. With the Kafka source, deliveries with the
same record key arrive in one partition and are processed in offset order.
Deliveries with the same state key in different ordering scopes, or under
`ProcessingOrder::Unordered`, take turns in whichever order they begin.

## Redeliveries

Delivery is at least once, so a delivery whose change was committed can arrive
again when the process stops after the commit and before the acknowledgement,
or when a rebalance moves a partition before its offsets are committed.

The Kafka source reports each delivery's position, its topic partition and
offset. `InMemoryState` records the position of the delivery whose commit last
changed a key, and a later delivery of the same partition at or before that
position is acknowledged without running the handler; a store returns
`Begin::Applied` for it. Its outputs were published before
the commit, so skipping them loses nothing. Other sources report no position:
a redelivered delivery runs the handler again and applies its change a second
time.

## `State<T>`

`State<T>` holds the value stored under the delivery's key. A key without a
stored value starts as `T::default()`, and `is_new` tells whether a value was
stored before this delivery. The handler changes the value through
`DerefMut`, and `clear` removes the stored value when the delivery commits.

The handler owns the `State` and drops it when it returns; the runtime then
takes back the value. A handler that keeps the `State` after returning, for
example by moving it into a spawned task, fails the commit.

## Stores

A store implements `StateStore<I, M>`, where `I` is the handler's input and
`M` the source's message type, so a store can require `M: KeyedMessage`:

| Method | Called |
|---|---|
| `begin(&input, &context)` | Before each handler attempt. Returns the state passed to the handler and a transaction kept by the runtime, or `Begin::Applied` for a redelivery the store already holds |
| `commit(txn)` | After the outputs are published, before the acknowledgement. Dropping the transaction abandons the change |

`begin` returns a `HandlerError`, so a store classifies its failures as it
would in a handler: `Retry` reruns the attempt, `Reject` routes the delivery,
and `Fatal` stops the subscription. `StateContext` carries the subscription
name, the delivery's message key and position, and the revocation token of
its partition.

The store's `State` type is what the handler receives. It is `State<T>` for
one value per key, and a store can instead hand the handler a client of its
backend so the handler uses the backend's own features, with the store
committing the backend's transaction at the commit step.

### `InMemoryState`

`InMemoryState<T>` keeps one value per key in the memory of the process, for
state that may be lost. The values are lost when the process stops. When a
partition is revoked, the values its deliveries committed are discarded on
their next use, because another consumer may change them meanwhile, so the
key starts again from `T::default()`. Every key used keeps an entry for the
life of the process. Clones of an `InMemoryState` share its values, and `get`
returns the value stored under a key, for example to inspect it in a test.

Durable stores are [planned](../plan.md#state).

## Calling other services

A handler that only calls another service, such as a gRPC or HTTP API, needs no
store: the call has nothing to commit with the acknowledgement. Implement
`Handler` on a struct that holds the client, or capture the client in a
closure, and register it as any handler:

```rust,ignore
struct Enrich {
    users: UserServiceClient<Channel>,
}

impl Handler<Order> for Enrich {
    type Output = EnrichedOrder;

    async fn handle(&self, order: Order) -> Result<EnrichedOrder> {
        let user = self
            .users
            .clone()
            .get_user(GetUser { id: order.user_id })
            .await
            .retry()?
            .into_inner();
        Ok(EnrichedOrder { order, user })
    }
}

App::new().subscribe("enrich", orders, enriched, Enrich { users });
```

A store is for work whose result must be committed after the outputs are
published and before the acknowledgement, or recorded with the delivery's
position.
