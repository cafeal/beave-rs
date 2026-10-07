//! Application registration, subscription supervision, and shutdown coordination.
#[cfg(feature = "health")]
use crate::health::HealthServer;
use crate::{
    health::Health,
    shutdown::{CancellationToken, termination_signal},
    sink::Sink,
    source::Source,
    subscription::{IntoHandler, One, Subscription},
};
use std::{collections::HashSet, future::Future, pin::Pin};
use tokio::task::JoinSet;

type Runner = Box<
    dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send,
>;
/// A set of subscriptions that run, fail, and shut down together.
///
/// ```no_run
/// use beavers::{App, IterSource, Json, Result, StdoutSink};
///
/// async fn double(value: u64) -> Result<u64> {
///     Ok(value * 2)
/// }
///
/// # async fn run() -> anyhow::Result<()> {
/// App::new()
///     .subscribe("double", IterSource::new([1, 2, 3]), StdoutSink::<Json>::new(), double)
///     .run()
///     .await
/// # }
/// ```
#[derive(Default)]
pub struct App {
    subscriptions: Vec<Runner>,
    names: HashSet<String>,
    validation: Vec<String>,
    health: Health,
    #[cfg(feature = "health")]
    health_server: Option<HealthServer>,
}
impl App {
    /// An application without subscriptions.
    pub fn new() -> Self {
        Self::default()
    }
    /// Add a subscription with the default [`SubscriptionConfig`](crate::SubscriptionConfig).
    ///
    /// Use [`subscription`](Self::subscription) to register a configured [`Subscription`].
    ///
    /// The handler takes the received record or its value and returns the sink's
    /// output type or a plain value; see [`IntoHandler`] for the accepted shapes.
    pub fn subscribe<S, K, H, In, Out>(
        self,
        name: impl Into<String>,
        source: S,
        sink: K,
        handler: H,
    ) -> Self
    where
        S: Source,
        H: IntoHandler<S, K, (In, Out, One)>,
        K: Sink<H::Output>,
    {
        self.subscription(Subscription::new(name, source, sink, handler))
    }
    /// Add a configured subscription.
    ///
    /// Subscription names must be unique. Invalid configuration is reported when the
    /// application starts running.
    pub fn subscription<S, K, O>(mut self, subscription: Subscription<S, K, O>) -> Self
    where
        S: Source,
        K: Sink<O>,
        O: Send + Sync + 'static,
    {
        if let Err(error) = subscription.validate() {
            self.validation.push(error.to_string());
        }
        // Names label logs, spans, and metrics, so two subscriptions must not share one.
        if !self.names.insert(subscription.name().to_owned()) {
            self.validation.push(format!(
                "duplicate subscription name {:?}",
                subscription.name()
            ));
        }
        let tracker = self.health.register(subscription.name());
        self.subscriptions
            .push(Box::new(|token| Box::pin(subscription.run(token, tracker))));
        self
    }
    /// A handle reporting the liveness and readiness of the subscriptions
    /// registered so far and of those registered later.
    pub fn health(&self) -> Health {
        self.health.clone()
    }
    /// Serves liveness and readiness probes while the application runs.
    #[cfg(feature = "health")]
    pub fn health_server(mut self, server: HealthServer) -> Self {
        self.health_server = Some(server);
        self
    }
    /// Run every subscription until all of them finish or `shutdown` is cancelled.
    ///
    /// A subscription failure cancels `shutdown`, so the others drain and stop; the first
    /// failure is returned after every subscription has finished.
    pub async fn run_until(self, shutdown: CancellationToken) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.validation.is_empty(),
            "invalid subscription configuration: {}",
            self.validation.join("; ")
        );
        self.health.start(&shutdown);
        #[cfg(feature = "health")]
        let health_server = self.health_server.map(|server| {
            let stop = CancellationToken::new();
            let task = tokio::spawn(server.serve(self.health.clone(), stop.clone()));
            (stop, task)
        });
        let mut subscriptions = JoinSet::new();
        for run in self.subscriptions {
            subscriptions.spawn(run(shutdown.clone()));
        }
        let mut failure = None;
        while let Some(result) = subscriptions.join_next().await {
            if let Err(error) = result.unwrap_or_else(|error| Err(error.into())) {
                shutdown.cancel();
                failure.get_or_insert(error);
            }
        }
        #[cfg(feature = "health")]
        if let Some((stop, task)) = health_server {
            stop.cancel();
            if let Err(error) = task.await.unwrap_or_else(|error| Err(error.into())) {
                failure.get_or_insert(error.context("health server failed"));
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
    /// Run every subscription until all of them finish, one fails, or the process receives
    /// SIGINT or SIGTERM.
    pub async fn run(self) -> anyhow::Result<()> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            signal = termination_signal() => { token.cancel(); let result = run.await; signal?; result }
        }
    }
}
