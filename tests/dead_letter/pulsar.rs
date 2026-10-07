use beavers::{
    App, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result, Subscription,
    adapters::pulsar::{
        PulsarDeadLetter, PulsarInherit, PulsarMessageId, PulsarMetadata, PulsarPublish,
        PulsarRecord,
    },
    testing::{PulsarTestSource, pulsar_record},
};
use std::collections::HashMap;

fn order(entry_id: u64, value: &str) -> PulsarRecord<Vec<u8>> {
    let mut record = pulsar_record(
        "persistent://public/default/orders-partition-2",
        entry_id,
        value,
    );
    record.key = Some(b"customer-7".to_vec());
    record.event_time = Some(900);
    record.metadata.message_id.partition = 2;
    record.metadata.publish_time = 1_000;
    record
        .properties
        .insert("traceparent".to_owned(), "00-trace".to_owned());
    record
}

/// The message a consumer of the dead-letter topic receives for `publish`.
fn received(publish: PulsarPublish<Vec<u8>>, entry_id: u64) -> PulsarRecord<Vec<u8>> {
    PulsarRecord {
        value: publish.value,
        key: publish.key,
        properties: publish.properties,
        event_time: publish.event_time,
        metadata: PulsarMetadata {
            topic: "persistent://public/default/orders-dlq".into(),
            message_id: PulsarMessageId {
                ledger_id: 9,
                entry_id,
                partition: -1,
                batch_index: -1,
            },
            publish_time: 5_000,
        },
    }
}

async fn reject_odd(record: PulsarRecord<u32>) -> Result<u32> {
    match record.value {
        Some(value) if value % 2 == 0 => Ok(value),
        _ => Err(HandlerError::Reject(anyhow::anyhow!("odd value"))),
    }
}

/// Runs `reject_odd` over `records` and returns the dead-letter messages.
async fn dead_letter(
    name: &str,
    records: Vec<PulsarRecord<Vec<u8>>>,
) -> Vec<PulsarPublish<Vec<u8>>> {
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                PulsarTestSource::<Json, u32>::new(records),
                InMemorySink::<u32>::default(),
                reject_odd,
            )
            .dlq_with(dlq.clone(), |dead| {
                Ok(PulsarPublish::from_dead_letter(dead))
            })
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_message_and_add_failure_properties() {
    let published = dead_letter("orders", vec![order(7, "3")]).await;

    let expected: HashMap<String, String> = [
        ("traceparent", "00-trace"),
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        (
            "beavers-dlq-origin-topic",
            "persistent://public/default/orders-partition-2",
        ),
        ("beavers-dlq-origin-message-id", "0:7:2:-1"),
        ("beavers-dlq-origin-publish-time", "1000"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect();
    assert_eq!(
        published,
        [PulsarPublish {
            value: Some(b"3".to_vec()),
            properties: expected,
            key: Some(b"customer-7".to_vec()),
            ordering_key: None,
            event_time: Some(900),
        }]
    );
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order(7, "3")]).await;
    let second = dead_letter("orders-redrive", vec![received(first[0].clone(), 0)]).await;

    let parsed = PulsarDeadLetter::from_record(&received(second[0].clone(), 1))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.failure, FailureKind::Rejected);
    assert_eq!(parsed.details.count, 2);
    assert_eq!(parsed.origin, order(7, "3").metadata);
}

#[test]
fn malformed_dead_letter_properties_are_errors() {
    assert_eq!(PulsarDeadLetter::from_record(&order(7, "3")).unwrap(), None);

    let mut record = order(7, "3");
    for (name, value) in [
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-topic", "orders"),
        ("beavers-dlq-origin-message-id", "0:7"),
        ("beavers-dlq-origin-publish-time", "1000"),
    ] {
        record.properties.insert(name.to_owned(), value.to_owned());
    }
    assert!(PulsarDeadLetter::from_record(&record).is_err());
}

#[tokio::test]
async fn inheritance_skips_dead_letter_properties() {
    let dead = dead_letter("orders", vec![order(7, "3")]).await;
    let sink = InMemorySink::<PulsarPublish<u32>>::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                PulsarTestSource::<Json, u32>::new([received(dead[0].clone(), 0)]),
                sink.clone(),
                |record: PulsarRecord<u32>| async move {
                    Ok(PulsarPublish::new(record.value.unwrap_or_default() + 1))
                },
            )
            .middleware(PulsarInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let outputs = sink.values();
    assert_eq!(
        outputs[0].properties,
        HashMap::from([("traceparent".to_owned(), "00-trace".to_owned())])
    );
}
