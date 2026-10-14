use beavers::{
    App, BoxError, ErrorPolicy, FailureKind, HandlerError, InMemorySink, Json, Result,
    Subscription,
    adapters::pubsub::{
        PubSubAttributes, PubSubDeadLetter, PubSubInherit, PubSubOrigin, PubSubPublish,
        PubSubRecord,
    },
    testing::{PubSubTestSource, pubsub_record},
};

const SUBSCRIPTION: &str = "projects/p/subscriptions/orders";

fn order(message_id: &str, value: &str) -> PubSubRecord<Vec<u8>> {
    let mut record = pubsub_record(SUBSCRIPTION, message_id, value);
    record.ordering_key = Some("customer-7".into());
    record
        .attributes
        .insert("traceparent".into(), "00-trace".into());
    record
}

/// The message a consumer of the dead-letter topic receives for `publish`.
fn received(publish: PubSubPublish<Vec<u8>>, message_id: &str) -> PubSubRecord<Vec<u8>> {
    PubSubRecord {
        value: publish.value,
        attributes: publish.attributes,
        ordering_key: publish.ordering_key,
        ..pubsub_record("projects/p/subscriptions/orders-dlq", message_id, "")
    }
}

async fn reject_odd(record: PubSubRecord<u32>) -> Result<u32> {
    if record.value.is_multiple_of(2) {
        Ok(record.value)
    } else {
        Err(HandlerError::Reject(BoxError::from("odd value")))
    }
}

/// Runs `handler` over `records` and returns the dead-letter messages.
async fn dead_letter<F, Fut>(
    name: &str,
    records: Vec<PubSubRecord<Vec<u8>>>,
    handler: F,
) -> Vec<PubSubPublish<Vec<u8>>>
where
    F: Fn(PubSubRecord<u32>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<u32>> + Send + 'static,
{
    let dlq = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                name,
                PubSubTestSource::<Json<u32>>::new(records),
                InMemorySink::<u32>::default(),
                handler,
            )
            .dead_letter_with(dlq.clone(), |dead| {
                Ok(PubSubPublish::from_dead_letter(dead))
            })
            .error_policy(ErrorPolicy::dead_letter_all()),
        )
        .run()
        .await
        .unwrap();
    dlq.values()
}

#[tokio::test]
async fn dead_letters_keep_the_original_message_and_add_detail_attributes() {
    let published = dead_letter("orders", vec![order("m-7", "3")], reject_odd).await;
    assert_eq!(published.len(), 1);
    let publish = &published[0];
    assert_eq!(publish.value, b"3");
    assert_eq!(publish.ordering_key.as_deref(), Some("customer-7"));
    let expected: PubSubAttributes = [
        ("traceparent", "00-trace"),
        ("beavers-dlq-subscription", "orders"),
        ("beavers-dlq-failure", "rejected"),
        ("beavers-dlq-error", "odd value"),
        ("beavers-dlq-attempts", "1"),
        ("beavers-dlq-count", "1"),
        ("beavers-dlq-origin-subscription", SUBSCRIPTION),
        ("beavers-dlq-origin-message-id", "m-7"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_owned(), value.to_owned()))
    .collect();
    assert_eq!(publish.attributes, expected);
}

#[tokio::test]
async fn long_errors_are_cut_to_the_attribute_limit() {
    let published = dead_letter("orders", vec![order("m-7", "3")], |_| async {
        Err(HandlerError::Reject(BoxError::from("é".repeat(600))))
    })
    .await;
    let error = &published[0].attributes["beavers-dlq-error"];
    assert_eq!(error.len(), 1024);
    assert!(error.chars().all(|c| c == 'é'));
}

#[tokio::test]
async fn dead_lettering_a_dead_letter_keeps_its_origin_and_counts_again() {
    let first = dead_letter("orders", vec![order("m-7", "3")], reject_odd).await;
    let second = dead_letter(
        "orders-redrive",
        vec![received(first[0].clone(), "d-1")],
        reject_odd,
    )
    .await;

    let parsed = PubSubDeadLetter::from_record(&received(second[0].clone(), "d-2"))
        .unwrap()
        .unwrap();
    assert_eq!(parsed.details.subscription, "orders-redrive");
    assert_eq!(parsed.details.failure, FailureKind::Rejected);
    assert_eq!(parsed.details.count, 2);
    let PubSubOrigin {
        subscription,
        message_id,
        ..
    } = parsed.origin;
    assert_eq!(
        (subscription.as_str(), message_id.as_str()),
        (SUBSCRIPTION, "m-7")
    );
}

#[test]
fn malformed_details_are_errors() {
    assert_eq!(
        PubSubDeadLetter::from_record(&order("m-7", "3")).unwrap(),
        None
    );
    let mut record = order("m-7", "3");
    record
        .attributes
        .insert("beavers-dlq-failure".into(), "rejected".into());
    assert!(PubSubDeadLetter::from_record(&record).is_err());
}

#[tokio::test]
async fn inheritance_skips_dead_letter_attributes() {
    let dead = dead_letter("orders", vec![order("m-7", "3")], reject_odd).await;
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "orders-redrive",
                PubSubTestSource::<Json<u32>>::new([received(dead[0].clone(), "d-1")]),
                sink.clone(),
                |record: PubSubRecord<u32>| async move { Ok(PubSubPublish::new(record.value + 1)) },
            )
            .middleware(PubSubInherit::new()),
        )
        .run()
        .await
        .unwrap();
    let outputs: Vec<PubSubPublish<u32>> = sink.values();
    assert_eq!(
        outputs[0].attributes,
        PubSubAttributes::from([("traceparent".to_owned(), "00-trace".to_owned())])
    );
}
