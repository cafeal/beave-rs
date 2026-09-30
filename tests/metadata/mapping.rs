use beavers::{App, Emit, Flow, InMemorySink, IterSource, MapMetadata, Middleware, Subscription};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Mutex;

#[tokio::test]
async fn middleware_runs_in_registration_order_with_the_original_input() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "middleware_runs_in_registration_order_with_the_original_input",
                IterSource::new([2]),
                sink.clone(),
                |n: i32| async move { Ok(n * 10) },
            )
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
    async fn pre_handler(&self, input: i32) -> beavers::Result<Flow<i32, i32>> {
        Ok(if input < 0 {
            Flow::Intercept(Emit::One(0))
        } else {
            Flow::Continue(input)
        })
    }
}

struct Double;

impl Middleware<i32, i32> for Double {
    async fn pre_handler(&self, input: i32) -> beavers::Result<Flow<i32, i32>> {
        Ok(Flow::Continue(input * 2))
    }
}

#[tokio::test]
async fn intercepted_input_skips_the_handler_but_still_runs_post_handlers() {
    let sink = InMemorySink::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    App::new()
        .subscription(
            Subscription::new(
                "intercepted_input_skips_the_handler_but_still_runs_post_handlers",
                IterSource::new([-1, 2]),
                sink.clone(),
                move |n: i32| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(n * 10) }
                },
            )
            .middleware(Negatives)
            .middleware(MapMetadata::new(|_: &i32, output: i32| Ok(output + 1))),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1, 21]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pre_handlers_transform_handler_input_while_post_handlers_see_the_decoded_input() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(
            Subscription::new(
                "pre_handlers_transform_handler_input_while_post_handlers_see_the_decoded_input",
                IterSource::new([3]),
                sink.clone(),
                |n: i32| async move { Ok(n) },
            )
            .middleware(Double)
            .middleware(Double)
            .middleware(MapMetadata::new(|input: &i32, output: i32| {
                Ok(output * 100 + input)
            })),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![1203]);
}

/// Looks up values in a store that is only reachable asynchronously.
struct Lookup {
    offsets: Arc<Mutex<HashMap<i32, i32>>>,
}

impl Middleware<i32, i32> for Lookup {
    async fn pre_handler(&self, input: i32) -> beavers::Result<Flow<i32, i32>> {
        tokio::task::yield_now().await;
        let offset = self.offsets.lock().await.get(&input).copied();
        Ok(match offset {
            Some(offset) => Flow::Continue(input + offset),
            None => Flow::Intercept(Emit::None),
        })
    }

    async fn post_handler(&self, input: &i32, output: i32) -> beavers::Result<i32> {
        tokio::task::yield_now().await;
        self.offsets.lock().await.insert(*input, output);
        Ok(output)
    }
}

#[tokio::test]
async fn async_hooks_await_before_the_handler_and_before_publishing() {
    let sink = InMemorySink::default();
    let offsets = Arc::new(Mutex::new(HashMap::from([(1, 100), (2, 200)])));
    App::new()
        .subscription(
            Subscription::new(
                "async_hooks_await_before_the_handler_and_before_publishing",
                IterSource::new([1, 3, 2]),
                sink.clone(),
                |n: i32| async move { Ok(n * 2) },
            )
            .middleware(Lookup {
                offsets: offsets.clone(),
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), vec![202, 404]);
    assert_eq!(*offsets.lock().await, HashMap::from([(1, 202), (2, 404)]));
}
