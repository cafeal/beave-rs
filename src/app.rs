//! Application registration, subscription supervision, and shutdown coordination.
#[cfg(feature = "health")]
use crate::health::HealthServer;
use crate::{
    handler::Handler,
    health::Health,
    shutdown::{CancellationToken, termination_signal},
    sink::Sink,
    source::{Source, SourceItem},
    subscription::Subscription,
};
use std::{collections::HashSet, future::Future, pin::Pin};
use tokio::task::JoinSet;

type Runner = Box<
    dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
        + Send,
>;
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
    pub fn new() -> Self {
        Self::default()
    }
    pub fn subscribe<S, K, H, O>(
        self,
        name: impl Into<String>,
        source: S,
        sink: K,
        handler: H,
    ) -> Self
    where
        S: Source,
        K: Sink<O>,
        H: Handler<SourceItem<S>, Output = O>,
        O: Send + Sync + 'static,
    {
        self.subscription(Subscription::new(name, source, sink, handler))
    }
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
