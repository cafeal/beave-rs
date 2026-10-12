use beavers::{
    App, BoxError, HandlerError, InMemorySink, Json, Receive, Source, SourceMessage, Subscription,
    testing::{DeliveryState, TestSource},
};

fn payloads(values: &[&str]) -> Vec<Vec<u8>> {
    values
        .iter()
        .map(|value| value.as_bytes().to_vec())
        .collect()
}

#[tokio::test]
async fn records_are_pending_until_received_and_acknowledged_on_ack() {
    let mut source = TestSource::<Json, u32, _>::new(payloads(&["1", "2"]));
    let deliveries = source.deliveries();
    assert_eq!(
        deliveries.states(),
        [DeliveryState::Pending, DeliveryState::Pending]
    );

    let Receive::Message(first) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    assert_eq!(first.decode().unwrap(), 1);
    assert_eq!(first.raw(), b"1");
    assert_eq!(first.ordering_key(), None);
    assert_eq!(
        deliveries.states(),
        [DeliveryState::Received, DeliveryState::Pending]
    );
    assert_eq!(deliveries.unacknowledged(), [b"1".to_vec()]);

    first.ack().await.unwrap();
    assert_eq!(
        deliveries.states(),
        [DeliveryState::Acknowledged, DeliveryState::Pending]
    );
    assert_eq!(deliveries.acknowledged(), [b"1".to_vec()]);
    assert!(!deliveries.all_acknowledged());

    let Receive::Message(second) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    drop(second);
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
    assert_eq!(deliveries.unacknowledged(), [b"2".to_vec()]);
}

#[tokio::test]
async fn a_stopped_subscription_leaves_its_failed_record_unacknowledged() {
    let source = TestSource::<Json, u32, _>::new(payloads(&["1", "2", "3"]));
    let deliveries = source.deliveries();
    let sink = InMemorySink::default();
    let result = App::new()
        .subscription(Subscription::new(
            "a_stopped_subscription_leaves_its_failed_record_unacknowledged",
            source,
            sink.clone(),
            |value: u32| async move {
                if value == 2 {
                    return Err(HandlerError::Fatal(BoxError::from("broken")));
                }
                Ok(value)
            },
        ))
        .run()
        .await;
    assert!(result.is_err());
    assert_eq!(sink.values(), [1]);
    assert_eq!(deliveries.acknowledged(), [b"1".to_vec()]);
    assert_eq!(deliveries.states()[1], DeliveryState::Received);
    assert!(!deliveries.all_acknowledged());
}
