use beavers::{
    Json, OrderingKey, Receive, Source, SourceMessage,
    testing::{PulsarTestSource, pulsar_record},
};

#[tokio::test]
async fn deliveries_follow_the_pulsar_adapter() {
    let mut record = pulsar_record("persistent://public/default/orders-partition-2", 5, "7");
    record.metadata.message_id.partition = 2;
    record.key = Some(b"customer-7".to_vec());
    record
        .properties
        .insert("traceparent".to_owned(), "00-trace".to_owned());
    let mut source = PulsarTestSource::<Json, u32>::new([record.clone()]);

    let Receive::Message(message) = source.receive().await.unwrap() else {
        panic!("expected a delivery");
    };
    let decoded = message.decode().unwrap();
    assert_eq!(decoded.value, Some(7));
    assert_eq!(decoded.key, record.key);
    assert_eq!(decoded.metadata, record.metadata);
    assert_eq!(message.raw(), record);
    assert_eq!(
        message.ordering_key(),
        Some(OrderingKey::new(
            "persistent://public/default/orders-partition-2",
            2
        ))
    );
    assert_eq!(message.propagation_fields(), [("traceparent", "00-trace")]);
    assert!(matches!(source.receive().await.unwrap(), Receive::End));
}
