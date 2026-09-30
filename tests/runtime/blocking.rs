use beavers::{
    App, BlockingPool, CancellationToken, Emit, InMemorySink, IterSource, Subscription, blocking,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

#[tokio::test]
async fn blocking_handlers_run_on_pool_threads() {
    let source = IterSource::new([1, 2, 3]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::default();
    let threads = Arc::new(Mutex::new(Vec::new()));
    let seen = threads.clone();
    App::new()
        .subscribe(
            source,
            sink.clone(),
            blocking(move |n: i32| {
                seen.lock()
                    .unwrap()
                    .push(thread::current().name().map(str::to_owned));
                Ok(n * 10)
            }),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [10, 20, 30]);
    assert_eq!(acks.load(Ordering::SeqCst), 3);
    for name in threads.lock().unwrap().iter() {
        assert!(name.as_deref().unwrap().starts_with("beavers-blocking-"));
    }
}

#[tokio::test]
async fn blocking_handlers_can_emit_many() {
    let sink = InMemorySink::default();
    App::new()
        .subscription(Subscription::new_emitting(
            IterSource::new([2]),
            sink.clone(),
            blocking(|n: i32| Ok(Emit::Many(vec![n; n as usize]))),
        ))
        .run()
        .await
        .unwrap();
    assert_eq!(sink.values(), [2, 2]);
}

#[tokio::test]
async fn shared_pool_bounds_parallel_calls() {
    let pool = BlockingPool::new(1).unwrap();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler = |running: Arc<AtomicUsize>, peak: Arc<AtomicUsize>| {
        pool.blocking(move |n: i32| {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(5));
            running.fetch_sub(1, Ordering::SeqCst);
            Ok(n)
        })
    };
    App::new()
        .subscription(
            Subscription::new(
                IterSource::new(0..4),
                InMemorySink::default(),
                handler(running.clone(), peak.clone()),
            )
            .name("first")
            .concurrency(4),
        )
        .subscription(
            Subscription::new(
                IterSource::new(0..4),
                InMemorySink::default(),
                handler(running.clone(), peak.clone()),
            )
            .name("second")
            .concurrency(4),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn panicking_blocking_handler_stops_without_ack() {
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let result = App::new()
        .subscribe(
            source,
            InMemorySink::<i32>::default(),
            blocking(|_: i32| -> beavers::Result<i32> { panic!("broken handler") }),
        )
        .run()
        .await;
    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("broken handler"), "{error}");
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn drain_timeout_abandons_running_blocking_work_without_ack() {
    let token = CancellationToken::new();
    let stop = token.clone();
    let source = IterSource::new([1]);
    let acks = source.acknowledgements();
    let sink = InMemorySink::default();
    let finished = Arc::new(AtomicUsize::new(0));
    let done = finished.clone();
    let app = App::new().subscription(
        Subscription::new(
            source,
            sink.clone(),
            blocking(move |n: i32| {
                stop.cancel();
                thread::sleep(Duration::from_millis(100));
                done.fetch_add(1, Ordering::SeqCst);
                Ok(n)
            }),
        )
        .drain_timeout(Duration::from_millis(10)),
    );
    assert!(app.run_until(token).await.is_err());
    // The synchronous call keeps running after the waiter is dropped; its result is discarded.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
    assert!(sink.values().is_empty());
}
