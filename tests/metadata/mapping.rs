use beavers::{App, InMemorySink, IterSource, MapMetadata, Subscription};

#[tokio::test]
async fn middleware_runs_in_registration_order_with_the_original_input() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(IterSource::new([2]), sink.clone(), |n: i32| async move {
                Ok(n * 10)
            })
            .middleware(MapMetadata::new(|input: &i32, output: i32| {
                Ok(output + input)
            }))
            .middleware(MapMetadata::new(|input: &i32, output: i32| {
                Ok(output * input)
            })),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![44]);
}
