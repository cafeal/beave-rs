use beavers::{App, Emit, InMemorySink, IterSource, MapMetadata, Middleware, Subscription};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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

struct Negatives;

impl Middleware<i32, i32> for Negatives {
    fn intercept(&self, input: &i32) -> beavers::Result<Option<Emit<i32>>> {
        Ok((*input < 0).then_some(Emit::One(0)))
    }
}

#[tokio::test]
async fn intercepted_input_skips_the_handler_but_still_maps_outputs() {
    let sink = InMemorySink::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    App::new()
        .subscription(
            Subscription::new(IterSource::new([-1, 2]), sink.clone(), move |n: i32| {
                counter.fetch_add(1, Ordering::SeqCst);
                async move { Ok(n * 10) }
            })
            .middleware(Negatives)
            .middleware(MapMetadata::new(|_: &i32, output: i32| Ok(output + 1))),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 21]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
