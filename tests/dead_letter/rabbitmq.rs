use beavers::{
    App, BoxError, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result,
    Subscription,
    adapters::rabbitmq::{
        RabbitMqDeadLetter, RabbitMqHeaders, RabbitMqInherit, RabbitMqMetadata, RabbitMqOrigin,
        RabbitMqPublish, RabbitMqRecord, RabbitMqValue,
    },
    testing::{RabbitMqTestSource, rabbitmq_record},
};

fn order(delivery_tag: u64, value: &str) -> RabbitMqRecord<Vec<u8>> {
    let mut record = rabbitmq_record("orders", delivery_tag, value);
    record.metadata.exchange = "shop".into();
    record.metadata.routing_key = "order.created".into();
    record.properties.message_id = Some("m-1".into());
    record
        .headers
        .insert("traceparent".into(), RabbitMqValue::from("00-trace"));
    record
        .headers
        .insert("retries".into(), RabbitMqValue::I32(2));
    record
}

/// The message a consumer of the dead-letter queue receives for `publish`.
fn received(publish: RabbitMqPublish<Vec<u8>>, delivery_tag: u64) -> RabbitMqRecord<Vec<u8>> {
    RabbitMqRecord {
        value: publish.value,
        headers: publish.headers,
        properties: publish.properties,
        metadata: RabbitMqMetadata {
            queue: "orders-dlq".into(),
            exchange: String::new(),
            routing_key: "orders-dlq".into(),
            redelivered: false,
            persistent: true,
            delivery_tag,
        },
    }
}

async fn reject_odd(record: RabbitMqRecord<u32>) -> Result<u32> {
    if record.value.is_multiple_of(2) {
        Ok(record.value)
    } else {
        Err(HandlerError::Reject(BoxError::from("odd value")))
    }
}

/// Runs `reject_odd` over `records` and returns the dead-letter messages.
async fn dead_letter(
    name: &str,
    records: Vec<RabbitMqRecord<Vec<u8>>>,
) -> Vec<RabbitMqPublish<Vec<u8>>> {
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                RabbitMqTestSource::<Json, u32>::new(records),
                InMemorySink::<u32>::default(),
                reject_odd,
            )
            .dlq_with(dlq.clone(), |dead| {
                Ok(RabbitMqPublish::from_dead_letter(dead))
            })
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_message_and_add_failure_headers() {
    let published = dead_letter("orders", vec![order(7, "3")]).await;

    let mut expected: RabbitMqHeaders = [
        ("traceparent", "00-trace"),
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-queue", "orders"),
        ("beavers-dlq-origin-exchange", "shop"),
        ("beavers-dlq-origin-routing-key", "order.created"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), RabbitMqValue::from(value)))
    .collect();
    expected.insert("retries".into(), RabbitMqValue::I32(2));
    assert_eq!(
        published,
        [RabbitMqPublish {
            value: b"3".to_vec(),
            routing_key: None,
            headers: expected,
            properties: order(7, "3").properties,
        }]
    );
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order(7, "3")]).await;
    let second = dead_letter("orders-redrive", vec![received(first[0].clone(), 1)]).await;

    let parsed = RabbitMqDeadLetter::from_record(&received(second[0].clone(), 2))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.failure, FailureKind::Rejected);
    assert_eq!(parsed.details.count, 2);
    assert_eq!(
        parsed.origin,
        RabbitMqDeadLetter::from_record(&received(first[0].clone(), 1))
            .unwrap()
            .unwrap()
            .origin
    );
    let RabbitMqOrigin {
        queue,
        exchange,
        routing_key,
        ..
    } = parsed.origin;
    assert_eq!((queue.as_str(), exchange.as_str()), ("orders", "shop"));
    assert_eq!(routing_key, "order.created");
}

#[test]
fn malformed_dead_letter_headers_are_errors() {
    assert_eq!(
        RabbitMqDeadLetter::from_record(&order(7, "3")).unwrap(),
        None
    );

    let mut record = order(7, "3");
    for (name, value) in [
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-queue", "orders"),
        ("beavers-dlq-origin-exchange", "shop"),
    ] {
        record
            .headers
            .insert(name.to_owned(), RabbitMqValue::from(value));
    }
    assert!(RabbitMqDeadLetter::from_record(&record).is_err());
    record.headers.insert(
        "beavers-dlq-origin-routing-key".into(),
        RabbitMqValue::from("order.created"),
    );
    assert!(RabbitMqDeadLetter::from_record(&record).unwrap().is_some());
    record
        .headers
        .insert("beavers-dlq-count".into(), RabbitMqValue::I64(1));
    assert!(RabbitMqDeadLetter::from_record(&record).is_err());
}

#[tokio::test]
async fn inheritance_skips_dead_letter_headers() {
    let dead = dead_letter("orders", vec![order(7, "3")]).await;
    let sink = InMemorySink::<RabbitMqPublish<u32>>::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                RabbitMqTestSource::<Json, u32>::new([received(dead[0].clone(), 1)]),
                sink.clone(),
                |record: RabbitMqRecord<u32>| async move {
                    Ok(RabbitMqPublish::new(record.value + 1))
                },
            )
            .middleware(RabbitMqInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let outputs = sink.values();
    assert_eq!(
        outputs[0].headers,
        RabbitMqHeaders::from([
            ("retries".to_owned(), RabbitMqValue::I32(2)),
            ("traceparent".to_owned(), RabbitMqValue::from("00-trace")),
        ])
    );
}
