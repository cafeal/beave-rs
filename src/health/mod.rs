//! Liveness and readiness of an application's subscriptions, readable through
//! [`Health`] and, with the `health` feature, served over HTTP by
//! `HealthServer`.

#[cfg(feature = "health")]
mod server;
mod state;

#[cfg(feature = "health")]
pub use server::HealthServer;
pub(crate) use state::Tracker;
pub use state::{Health, HealthReport, SubscriptionReport, SubscriptionStatus};
