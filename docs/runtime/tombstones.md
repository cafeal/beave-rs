# Tombstones

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

See the [codec guide](../codecs.md#lifecycle-and-failures) for decoding and encoding boundaries.
