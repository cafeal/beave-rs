//! Ready-made handlers that run code not written in Rust.
//!
//! Each handler takes a JSON value and emits zero, one, or many JSON values, so it is
//! registered with [`Subscription::new_emitting`](crate::Subscription::new_emitting)
//! between a source and a sink using the `Json<serde_json::Value>` codec. The `lua`
//! and `python` modules are enabled by Cargo features of the same names.
#[cfg(feature = "lua")]
mod lua;
#[cfg(feature = "python")]
mod python;

#[cfg(feature = "lua")]
pub use lua::Lua;
#[cfg(feature = "python")]
pub use python::Python;
