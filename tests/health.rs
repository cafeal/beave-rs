use beavers::{
    App, CancellationToken, Delivery, Health, Receive, ReceiveError, RetryPolicy, Sink, Source,
    Subscription, health::SubscriptionStatus,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc},
    task::JoinHandle,
    time::{sleep, timeout},
};

/// Receives what the test sends: a value, or `None` for a retryable failure.
/// The source ends when the sender is dropped.
struct Scripted(mpsc::UnboundedReceiver<Option<i32>>);

impl Source for Scripted {
    type Message = Delivery<i32>;

    async fn receive(&mut self) -> Result<Receive<Self::Message>, ReceiveError> {
        match self.0.recv().await {
            Some(Some(value)) => Ok(Receive::Message(Delivery::untracked(value))),
            Some(None) => Err(ReceiveError::Retry(anyhow::anyhow!("broker unavailable"))),
            None => Ok(Receive::End),
        }
    }
}

/// Fails every publication while `down` is set.
#[derive(Clone, Default)]
struct Switch {
    down: Arc<AtomicBool>,
}

impl Sink<i32> for Switch {
    type Prepared = i32;

    fn prepare(&self, value: i32) -> anyhow::Result<i32> {
        Ok(value)
    }

    async fn publish(&self, _: &i32) -> anyhow::Result<()> {
        anyhow::ensure!(!self.down.load(Ordering::SeqCst), "sink unavailable");
        Ok(())
    }
}

fn patient(delay: Duration) -> RetryPolicy {
    RetryPolicy {
        max_attempts: 1000,
        initial_delay: delay,
        max_delay: delay,
        ..RetryPolicy::default()
    }
}

async fn eventually(health: &Health, condition: impl Fn(&Health) -> bool) {
    timeout(Duration::from_secs(5), async {
        while !condition(health) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("health never matched: {:?}", health.report()));
}

fn scripted(
    name: &str,
    sink: Switch,
) -> (
    Subscription<Scripted, Switch, i32>,
    mpsc::UnboundedSender<Option<i32>>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let subscription = Subscription::new(name, Scripted(receiver), sink, |value: i32| async move {
        Ok(value)
    });
    (subscription, sender)
}

fn run(app: App, shutdown: &CancellationToken) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(app.run_until(shutdown.clone()))
}

#[tokio::test]
async fn receive_backoff_makes_the_application_unready_until_the_retry() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app =
        App::new().subscription(subscription.receive_retry(patient(Duration::from_millis(200))));
    let health = app.health();
    assert!(!health.is_ready(), "not ready before the application runs");
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);
    eventually(&health, Health::is_ready).await;

    input.send(None).unwrap();
    eventually(&health, |health| !health.is_ready()).await;
    let report = health.report();
    assert!(report.live);
    assert_eq!(report.subscriptions[0].receive_failures, 1);
    assert!(report.subscriptions[0].receive_backoff);

    // Receiving again after the delay is ready, though no receive succeeded yet.
    eventually(&health, Health::is_ready).await;
    assert_eq!(health.report().subscriptions[0].receive_failures, 1);
    input.send(Some(1)).unwrap();
    eventually(&health, |health| {
        health.report().subscriptions[0].receive_failures == 0
    })
    .await;

    shutdown.cancel();
    running.await.unwrap().unwrap();
    let report = health.report();
    assert!(report.shutting_down && !report.ready && report.live);
    assert_eq!(report.subscriptions[0].status, SubscriptionStatus::Stopped);
}

#[tokio::test]
async fn publish_retry_makes_the_application_unready_until_it_succeeds() {
    let sink = Switch::default();
    sink.down.store(true, Ordering::SeqCst);
    let (subscription, input) = scripted("orders", sink.clone());
    let app =
        App::new().subscription(subscription.publish_retry(patient(Duration::from_millis(10))));
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    input.send(Some(1)).unwrap();
    eventually(&health, |health| {
        health.report().subscriptions[0].publish_retries == 1
    })
    .await;
    assert!(!health.is_ready());

    sink.down.store(false, Ordering::SeqCst);
    eventually(&health, Health::is_ready).await;
    assert_eq!(health.report().subscriptions[0].publish_retries, 0);

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn ended_source_keeps_the_application_ready() {
    let (seed, seed_input) = scripted("seed", Switch::default());
    let (orders, _orders_input) = scripted("orders", Switch::default());
    let app = App::new().subscription(seed).subscription(orders);
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    drop(seed_input);
    eventually(&health, |health| {
        health.report().subscriptions[0].status == SubscriptionStatus::Stopped
    })
    .await;
    assert!(health.is_ready());

    shutdown.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_subscription_is_not_live() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app = App::new().subscription(fail_once(subscription));
    let health = app.health();
    let running = run(app, &CancellationToken::new());

    input.send(None).unwrap();
    assert!(running.await.unwrap().is_err());
    let report = health.report();
    assert!(!report.live && !report.ready && report.shutting_down);
    assert_eq!(report.subscriptions[0].status, SubscriptionStatus::Failed);
}

fn fail_once(
    subscription: Subscription<Scripted, Switch, i32>,
) -> Subscription<Scripted, Switch, i32> {
    subscription.receive_retry(RetryPolicy {
        max_attempts: 1,
        ..RetryPolicy::default()
    })
}

#[tokio::test(start_paused = true)]
async fn exit_delay_holds_a_failure_before_returning() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app = App::new()
        .subscription(fail_once(subscription))
        .exit_delay(Duration::from_secs(30));
    let health = app.health();
    let started = tokio::time::Instant::now();
    let running = run(app, &CancellationToken::new());

    input.send(None).unwrap();
    eventually(&health, |health| !health.is_live()).await;
    assert!(!running.is_finished());
    assert!(running.await.unwrap().is_err());
    assert!(started.elapsed() >= Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn exit_delay_does_not_hold_a_clean_stop() {
    let (subscription, input) = scripted("orders", Switch::default());
    let app = App::new()
        .subscription(subscription)
        .exit_delay(Duration::from_secs(30));
    let started = tokio::time::Instant::now();
    let running = run(app, &CancellationToken::new());

    drop(input);
    running.await.unwrap().unwrap();
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[tokio::test]
async fn draining_subscription_is_unready_but_live() {
    let release = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    let (sender, receiver) = mpsc::unbounded_channel();
    let subscription = Subscription::new("orders", Scripted(receiver), Switch::default(), {
        let release = release.clone();
        let started = started.clone();
        move |value: i32| {
            let release = release.clone();
            let started = started.clone();
            async move {
                started.notify_one();
                release.notified().await;
                Ok(value)
            }
        }
    });
    let app = App::new().subscription(subscription);
    let health = app.health();
    let shutdown = CancellationToken::new();
    let running = run(app, &shutdown);

    sender.send(Some(1)).unwrap();
    started.notified().await;
    shutdown.cancel();
    eventually(&health, |health| {
        health.report().subscriptions[0].status == SubscriptionStatus::Stopping
    })
    .await;
    let report = health.report();
    assert!(report.live && !report.ready);

    release.notify_one();
    running.await.unwrap().unwrap();
}

#[cfg(feature = "health")]
mod server {
    use super::*;
    use beavers::health::HealthServer;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// Sends one request over its own connection and returns the status and body.
    async fn request(addr: SocketAddr, method: &str, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let head =
            format!("{method} {path} HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response.split(' ').nth(1).unwrap().parse().unwrap();
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_default();
        (status, body)
    }

    fn server() -> HealthServer {
        HealthServer::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap()
    }

    #[tokio::test]
    async fn probes_answer_with_the_report() {
        let server = server();
        let addr = server.local_addr();
        assert_ne!(addr.port(), 0);
        let (subscription, _input) = scripted("orders", Switch::default());
        let app = App::new().subscription(subscription).health_server(server);
        let health = app.health();
        let shutdown = CancellationToken::new();
        let running = run(app, &shutdown);
        eventually(&health, Health::is_ready).await;

        let (status, body) = request(addr, "GET", "/readyz").await;
        assert_eq!(status, 200);
        let report: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(report["ready"], true);
        assert_eq!(report["subscriptions"][0]["name"], "orders");
        assert_eq!(report["subscriptions"][0]["status"], "running");
        assert_eq!(request(addr, "GET", "/livez").await.0, 200);
        assert_eq!(request(addr, "GET", "/metrics").await.0, 404);
        assert_eq!(request(addr, "POST", "/readyz").await.0, 405);

        shutdown.cancel();
        running.await.unwrap().unwrap();
        assert!(
            TcpStream::connect(addr).await.is_err(),
            "the server stops with the application"
        );
    }

    #[tokio::test]
    async fn liveness_fails_during_the_exit_delay() {
        let server = server();
        let addr = server.local_addr();
        let (subscription, input) = scripted("orders", Switch::default());
        let app = App::new()
            .subscription(fail_once(subscription))
            .health_server(server)
            .exit_delay(Duration::from_secs(60));
        let health = app.health();
        let running = run(app, &CancellationToken::new());

        input.send(None).unwrap();
        eventually(&health, |health| !health.is_live()).await;
        let (status, body) = request(addr, "GET", "/livez").await;
        assert_eq!(status, 503);
        assert!(body.contains(r#""status":"failed""#), "{body}");
        assert!(!running.is_finished(), "the delay holds the failure");
        running.abort();
    }

    #[tokio::test]
    async fn readiness_fails_while_draining_and_liveness_holds() {
        let server = server();
        let addr = server.local_addr();
        let release = Arc::new(Notify::new());
        let started = Arc::new(Notify::new());
        let (sender, receiver) = mpsc::unbounded_channel();
        let subscription = Subscription::new("orders", Scripted(receiver), Switch::default(), {
            let release = release.clone();
            let started = started.clone();
            move |value: i32| {
                let release = release.clone();
                let started = started.clone();
                async move {
                    started.notify_one();
                    release.notified().await;
                    Ok(value)
                }
            }
        });
        let shutdown = CancellationToken::new();
        let running = run(
            App::new().subscription(subscription).health_server(server),
            &shutdown,
        );

        sender.send(Some(1)).unwrap();
        started.notified().await;
        assert_eq!(request(addr, "GET", "/readyz").await.0, 200);
        shutdown.cancel();
        let (status, body) = request(addr, "GET", "/readyz").await;
        assert_eq!(status, 503);
        assert!(body.contains(r#""shutting_down":true"#), "{body}");
        assert_eq!(request(addr, "GET", "/livez").await.0, 200);

        release.notify_one();
        running.await.unwrap().unwrap();
    }
}
