//! Bounded, typed, process-local connection between subscriptions.
//!
//! [`channel`] connects the sink of one subscription to the source of the next, so a
//! pipeline can split I/O-bound and CPU-bound stages into subscriptions with their
//! own concurrency, retries, and error policies, for example an async handler that
//! fetches data followed by a [`blocking`](crate::blocking) handler that scores it.
//!
//! ```
//! use beavers::{App, InMemorySink, IterSource, Result, blocking, channel};
//!
//! async fn fetch(id: u32) -> Result<String> {
//!     Ok(format!("document {id}"))
//! }
//!
//! fn score(document: String) -> Result<usize> {
//!     Ok(document.len())
//! }
//!
//! # #[tokio::main] async fn main() -> anyhow::Result<()> {
//! let (to_score, fetched) = channel(16);
//! let scores = InMemorySink::default();
//! App::new()
//!     .subscribe("fetch", IterSource::new([1, 2]), to_score, fetch)
//!     .subscribe("score", fetched, scores.clone(), blocking(score))
//!     .run()
//!     .await?;
//! assert_eq!(scores.values(), [10, 10]);
//! # Ok(()) }
//! ```

mod queue;
mod record;
mod sink;
mod source;

pub use queue::channel;
pub use record::{ChannelOutput, ChannelRaw};
pub use sink::{ChannelReceiver, ChannelSink};
pub use source::{ChannelSender, ChannelSource};
