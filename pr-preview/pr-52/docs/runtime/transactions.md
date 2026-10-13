# Transactions

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
see the [Kafka guide](../adapters/kafka.md#transactions). The runtime never calls
the delivery's own ACK in a transactional subscription.

## Batches

A delivery whose outputs are prepared joins the open batch, and its job ends
and frees its concurrency slot and ordering key, as a
[pending completion](../runtime.md#per-message-lifecycle) does. The delivery counts as
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

## Source verification

Types cannot tell whether a source and a sink of the same platform connect to
the same cluster, and a transaction can only acknowledge a delivery of its own
cluster. The runtime therefore passes the first delivery of a transactional
subscription to `TransactionalSink::verify_source` before processing it. The
check is retried under the `publish_retry` policy, and a failure stops the
subscription before any handler runs or anything is published or
acknowledged. Shutdown during the check leaves the delivery unacknowledged.

## Ordering

A transactional subscription requires `ProcessingOrder::PerKey`. Deliveries of
one ordering scope then join batches in receive order, and a batch lists them
in that order, so the acknowledgement of a scope's last delivery in a batch
covers every earlier delivery of that scope. Other orderings fail validation
before the application starts.
