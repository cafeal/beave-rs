use beavers::{
    App, BoxError, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result,
    Subscription,
    adapters::servicebus::{
        ServiceBusApplicationProperties, ServiceBusDeadLetter, ServiceBusInherit, ServiceBusOrigin,
        ServiceBusPublish, ServiceBusRecord, ServiceBusValue,
    },
    testing::{ServiceBusTestSource, servicebus_record},
};
use std::{collections::BTreeMap, time::Duration};

const ENTITY: &str = "orders/subscriptions/billing";

fn order(sequence_number: i64, value: &str) -> ServiceBusRecord<Vec<u8>> {
    let mut record = servicebus_record(ENTITY, sequence_number, value);
    record.properties.message_id = Some("m-7".into());
    record.properties.subject = Some("order".into());
    record.properties.time_to_live = Some(Duration::from_secs(60));
    record
        .application_properties
        .insert("traceparent".into(), ServiceBusValue::from("00-trace"));
    record
        .application_properties
        .insert("count".into(), ServiceBusValue::I32(2));
    record
}

/// The message a consumer of the dead-letter entity receives for `publish`.
fn received(
    publish: ServiceBusPublish<Vec<u8>>,
    sequence_number: i64,
) -> ServiceBusRecord<Vec<u8>> {
    ServiceBusRecord {
        value: publish.value,
        application_properties: publish.application_properties,
        properties: publish.properties,
        metadata: servicebus_record("orders-dlq", sequence_number, "").metadata,
    }
}

async fn reject_odd(record: ServiceBusRecord<u32>) -> Result<u32> {
    if record.value.is_multiple_of(2) {
        Ok(record.value)
    } else {
        Err(HandlerError::Reject(BoxError::from("odd value")))
    }
}

/// Runs `reject_odd` over `records` and returns the dead-letter messages.
async fn dead_letter(
    name: &str,
    records: Vec<ServiceBusRecord<Vec<u8>>>,
) -> Vec<ServiceBusPublish<Vec<u8>>> {
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                ServiceBusTestSource::<Json, u32>::new(records),
                InMemorySink::<u32>::default(),
                reject_odd,
            )
            .dlq_with(dlq.clone(), |dead| {
                Ok(ServiceBusPublish::from_dead_letter(dead))
            })
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_message_and_add_detail_properties() {
    let published = dead_letter("orders", vec![order(41, "3")]).await;
    assert_eq!(published.len(), 1);
    let publish = &published[0];
    assert_eq!(publish.value, b"3");
    assert_eq!(publish.properties.subject.as_deref(), Some("order"));
    assert_eq!(publish.properties.message_id, None);
    assert_eq!(publish.properties.time_to_live, None);
    assert_eq!(publish.scheduled_enqueue_time, None);
    assert_eq!(
        publish.application_properties["traceparent"],
        ServiceBusValue::from("00-trace")
    );
    assert_eq!(
        publish.application_properties["count"],
        ServiceBusValue::I32(2)
    );
    let details: BTreeMap<&str, &str> = publish
        .application_properties
        .iter()
        .filter(|(name, _)| name.starts_with("beavers-dlq-"))
        .map(|(name, value)| (name.as_str(), value.as_str().unwrap()))
        .collect();
    let expected = BTreeMap::from([
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-entity-path", ENTITY),
        ("beavers-dlq-origin-sequence-number", "41"),
    ]);
    assert_eq!(details, expected);
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order(41, "3")]).await;
    let second = dead_letter("orders-redrive", vec![received(first[0].clone(), 1)]).await;

    let parsed = ServiceBusDeadLetter::from_record(&received(second[0].clone(), 2))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.failure, FailureKind::Rejected);
    assert_eq!(parsed.details.count, 2);
    let ServiceBusOrigin {
        entity_path,
        sequence_number,
        ..
    } = parsed.origin;
    assert_eq!((entity_path.as_str(), sequence_number), (ENTITY, 41));
}

#[test]
fn malformed_details_are_errors() {
    assert_eq!(
        ServiceBusDeadLetter::from_record(&order(41, "3")).unwrap(),
        None
    );
    let mut record = order(41, "3");
    record
        .application_properties
        .insert("beavers-dlq-failure".into(), ServiceBusValue::I32(1));
    assert!(ServiceBusDeadLetter::from_record(&record).is_err());
}

#[tokio::test]
async fn inheritance_skips_the_detail_properties() {
    let dead = dead_letter("orders", vec![order(41, "3")]).await;
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                ServiceBusTestSource::<Json, u32>::new([received(dead[0].clone(), 1)]),
                sink.clone(),
                |record: ServiceBusRecord<u32>| async move {
                    Ok(ServiceBusPublish::new(record.value + 1))
                },
            )
            .middleware(ServiceBusInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let outputs: Vec<ServiceBusPublish<u32>> = sink.values();
    assert_eq!(
        outputs[0].application_properties,
        ServiceBusApplicationProperties::from([
            ("count".to_owned(), ServiceBusValue::I32(2)),
            ("traceparent".to_owned(), ServiceBusValue::from("00-trace")),
        ])
    );
}
