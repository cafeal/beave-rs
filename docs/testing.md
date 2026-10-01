# Testing

The `testing` feature provides a source of fabricated records so handlers,
middleware, and error policies can be tested without a broker. Enable it for
tests only:

```toml
[dev-dependencies]
beavers = { version = "0.1", features = ["testing", "kafka"] }
```

The Kafka and Pulsar record types additionally need the `kafka` or `pulsar`
feature, which an application testing those handlers already enables.

## What each part covers

| Need | Use |
|---|---|
| Feed Kafka or Pulsar records with chosen topic, partition, offset, key, headers, or properties | `KafkaTestSource<C, T>`, `PulsarTestSource<C, T>`, with `kafka_record` and `pulsar_record` |
| Feed bare payloads through a codec | `TestSource<C, T, Vec<u8>>` |
| Check which records were acknowledged or left unacknowledged | `TestSource::deliveries()` |
| Capture published outputs | `InMemorySink<T>` for the subscription's output type |
| Capture dead letters | `InMemorySink<DeadLetter<I, R>>` as the dead-letter sink |

`IterSource` remains the simplest input for handlers that take plain values.
It yields already typed `Delivery<T>` values, so decoding cannot fail, dead
letters carry `()` as their raw form, and deliveries have no ordering key or
trace-context fields.

## TestSource

A `TestSource<C, T, R>` holds a finite list of records of type `R` with their
payload left undecoded, such as `KafkaRecord<Vec<u8>>`. Each record becomes one
delivery, and the source ends after the last one. The delivery matches the
adapter the record type belongs to:

- the handler input is the record decoded with codec `C`, such as
  `KafkaRecord<T>`, so a payload the codec rejects is a decode failure;
- the raw form carried by dead letters is the record itself;
- the ordering key and trace-context fields are those the adapter reports.

| Record type | Handler input | Ordering key | Trace-context fields |
|---|---|---|---|
| `Vec<u8>` | `T` | None | None |
| `KafkaRecord<Vec<u8>>` | `KafkaRecord<T>` | Topic and partition | Headers with UTF-8 values |
| `PulsarRecord<Vec<u8>>` | `PulsarRecord<T>` | Topic and partition index | Properties |

A null Kafka or Pulsar value stays `None` after decoding, as with the broker
sources. The Pulsar ordering key is the partition scope of Exclusive and
Failover subscriptions; the Key_Shared and Shared scopes of a real
`PulsarSource` are not reproduced.

`new(records)` uses a default-constructed codec; `with_codec(records, codec)`
accepts a configured one. `kafka_record(topic, partition, offset, value)` and
`pulsar_record(topic, entry_id, value)` build records with empty optional
fields. Set the public fields for keys, headers, properties, timestamps, a
partition topic's index, or a null value.

Implement `TestRecord` to fabricate records of another source.

## Delivery states

`deliveries()` returns a shared view of each record's `DeliveryState`:

| State | Meaning |
|---|---|
| `Pending` | The subscription has not received the record |
| `Received` | Received but not acknowledged: in flight, abandoned, or left unacknowledged when the subscription stopped |
| `Acknowledged` | Its outputs were published, or the error policy dead-lettered or discarded it |

`acknowledged()` lists records in acknowledgement order and `unacknowledged()`
lists received but unacknowledged records in source order. The source only
observes acknowledgements, so whether an acknowledged record was processed,
dead-lettered, or discarded is read from the output and dead-letter sinks.

## Example

```rust
use beavers::{
    App, DeadLetter, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json,
    Subscription,
    adapters::kafka::{KafkaInherit, KafkaPublish, KafkaRecord},
    testing::{KafkaTestSource, kafka_record},
};

async fn price(record: KafkaRecord<u32>) -> beavers::Result<KafkaPublish<u32>> {
    let cents = record.value.unwrap_or_default();
    if cents == 0 {
        return Err(HandlerError::Reject(anyhow::anyhow!("free order")));
    }
    Ok(KafkaPublish::new(cents * 110 / 100))
}

#[tokio::test]
async fn prices_orders() {
    let mut order = kafka_record("orders", 3, 42, "100");
    order.key = Some(b"customer-7".to_vec());
    order.headers = vec![("tenant".into(), Some(b"acme".to_vec()))];
    let free = kafka_record("orders", 3, 43, "0");
    let malformed = kafka_record("orders", 3, 44, "not json");

    let source = KafkaTestSource::<Json, u32>::new([order, free, malformed]);
    let deliveries = source.deliveries();
    let sink = InMemorySink::default();
    let dlq = InMemorySink::<DeadLetter<KafkaRecord<u32>, KafkaRecord<Vec<u8>>>>::default();

    App::new()
        .subscription(
            Subscription::new("price", source, sink.clone(), price)
                .middleware(KafkaInherit::new())
                .dlq(dlq.clone())
                .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();

    assert_eq!(
        sink.values(),
        [KafkaPublish {
            key: Some(b"customer-7".to_vec()),
            value: Some(110),
            headers: vec![("tenant".into(), Some(b"acme".to_vec()))],
        }]
    );
    let failures: Vec<_> = dlq
        .values()
        .into_iter()
        .map(|dead| (dead.failure, dead.raw.metadata.offset))
        .collect();
    assert_eq!(failures, [(FailureKind::Rejected, 43), (FailureKind::Decode, 44)]);
    assert!(deliveries.all_acknowledged());
}
```

With the default error policy, the decode failure would instead stop the
subscription with an error, leaving the malformed record in
`deliveries.unacknowledged()`.
