//! State kept across deliveries and committed with each delivery's acknowledgement.
mod handler;
mod key;
mod memory;
mod store;
mod value;

pub use handler::{Stateful, StatefulHandler, stateful};
pub use key::{MessageKey, StateKey};
pub use memory::{InMemoryState, InMemoryTxn};
pub use store::{Begin, StateContext, StateStore};
pub use value::State;
