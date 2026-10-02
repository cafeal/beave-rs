use beavers::{
    App, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result, Subscription,
    adapters::sqs::{
        SqsAttributeValue, SqsAttributes, SqsDeadLetter, SqsInherit, SqsMetadata, SqsOrigin,
        SqsPublish, SqsRecord,
    },
    testing::{SqsTestSource, sqs_record},
};
use std::collections::BTreeMap;

const QUEUE: &str = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo";

fn order(message_id: &str, value: &str) -> SqsRecord<Vec<u8>> {
    let mut record = sqs_record(QUEUE, message_id, value);
    record.metadata.message_group_id = Some("customer-7".into());
    record
        .attributes
        .insert("traceparent".into(), SqsAttributeValue::from("00-trace"));
    record
        .attributes
        .insert("count".into(), SqsAttributeValue::Number("2".into()));
    record
}

/// The message a consumer of the dead-letter queue receives for `publish`.
fn received(publish: SqsPublish<Vec<u8>>, message_id: &str) -> SqsRecord<Vec<u8>> {
    SqsRecord {
        value: publish.value,
        attributes: publish.attributes,
        metadata: SqsMetadata {
            message_group_id: publish.message_group_id,
            ..sqs_record("https://sqs/123/orders-dlq.fifo", message_id, "").metadata
        },
    }
}

async fn reject_odd(record: SqsRecord<u32>) -> Result<u32> {
    if record.value.is_multiple_of(2) {
        Ok(record.value)
    } else {
        Err(HandlerError::Reject(anyhow::anyhow!("odd value")))
    }
}

/// Runs `reject_odd` over `records` and returns the dead-letter messages.
async fn dead_letter(name: &str, records: Vec<SqsRecord<Vec<u8>>>) -> Vec<SqsPublish<Vec<u8>>> {
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                SqsTestSource::<Json, u32>::new(records),
                InMemorySink::default(),
                reject_odd,
            )
            .dlq_with(dlq.clone(), |dead| Ok(SqsPublish::from_dead_letter(dead)))
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_message_and_add_one_details_attribute() {
    let published = dead_letter("orders", vec![order("m-7", "3")]).await;
    assert_eq!(published.len(), 1);
    let publish = &published[0];
    assert_eq!(publish.value, b"3");
    assert_eq!(publish.message_group_id.as_deref(), Some("customer-7"));
    assert_eq!(publish.attributes.len(), 3);
    assert_eq!(
        publish.attributes["traceparent"],
        SqsAttributeValue::from("00-trace")
    );
    assert_eq!(
        publish.attributes["count"],
        SqsAttributeValue::Number("2".into())
    );
    let details: BTreeMap<String, String> =
        serde_json::from_str(publish.attributes["beavers-dlq-details"].as_str().unwrap()).unwrap();
    let expected: BTreeMap<String, String> = [
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-queue-url", QUEUE),
        ("beavers-dlq-origin-message-id", "m-7"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect();
    assert_eq!(details, expected);
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order("m-7", "3")]).await;
    let second = dead_letter("orders-redrive", vec![received(first[0].clone(), "d-1")]).await;

    let parsed = SqsDeadLetter::from_record(&received(second[0].clone(), "d-2"))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.failure, FailureKind::Rejected);
    assert_eq!(parsed.details.count, 2);
    let SqsOrigin {
        queue_url,
        message_id,
        ..
    } = parsed.origin;
    assert_eq!((queue_url.as_str(), message_id.as_str()), (QUEUE, "m-7"));
}

#[test]
fn malformed_details_are_errors() {
    assert_eq!(
        SqsDeadLetter::from_record(&order("m-7", "3")).unwrap(),
        None
    );
    let mut record = order("m-7", "3");
    for malformed in [
        SqsAttributeValue::Number("1".into()),
        SqsAttributeValue::from("not json"),
        SqsAttributeValue::from(r#"{"beavers-dlq-failure":"rejected"}"#),
    ] {
        record
            .attributes
            .insert("beavers-dlq-details".into(), malformed);
        assert!(SqsDeadLetter::from_record(&record).is_err());
    }
}

#[tokio::test]
async fn inheritance_skips_the_details_attribute() {
    let dead = dead_letter("orders", vec![order("m-7", "3")]).await;
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                SqsTestSource::<Json, u32>::new([received(dead[0].clone(), "d-1")]),
                sink.clone(),
                |record: SqsRecord<u32>| async move { Ok(SqsPublish::new(record.value + 1)) },
            )
            .middleware(SqsInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let outputs: Vec<SqsPublish<u32>> = sink.values();
    assert_eq!(
        outputs[0].attributes,
        SqsAttributes::from([
            ("count".to_owned(), SqsAttributeValue::Number("2".into())),
            (
                "traceparent".to_owned(),
                SqsAttributeValue::from("00-trace")
            ),
        ])
    );
}
