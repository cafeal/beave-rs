use crate::blocking::{Blocking, BlockingPool};
use crate::error::Error;
use crate::handler::{Emit, Handler, HandlerError, Result};
use mlua::{Function, LuaOptions, LuaSerdeExt, MultiValue, StdLib, chunk::ChunkMode};
use serde_json::Value;
use std::{error::Error as StdError, fmt, future::Future, result::Result as StdResult};

type Call = Box<dyn Fn(Value) -> Result<Emit<Value>> + Send + Sync>;

/// A handler running a Lua 5.4 chunk for each input (`lua` feature).
///
/// The chunk receives the decoded input as the local `input`, converted from JSON to
/// Lua tables, and returns the outputs: no value or `nil` emits nothing, one value
/// emits one output, and several values emit one output each. JSON `null` is
/// `beavers.null`.
///
/// A Lua error rejects the input, and calling `beavers.retry(message)` retries it
/// under the subscription's retry policy. The chunk runs with the `table`, `string`,
/// `utf8`, `math`, and `coroutine` libraries only, on a dedicated worker thread, one
/// input at a time. Globals persist between inputs.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), beavers::Error> {
/// use beavers::{App, InMemorySink, IterSource, Subscription, handlers::Lua};
/// use serde_json::{Value, json};
///
/// let paid = Lua::new(
///     r#"
///     if input.status ~= "paid" then return nil end
///     return { order_id = input.id, amount = input.total }
///     "#,
/// )?;
/// let sink = InMemorySink::<Value>::default();
/// let orders = IterSource::new([json!({"id": 1, "status": "paid", "total": 30})]);
///
/// App::new()
///     .subscription(Subscription::new_emitting("paid", orders, sink.clone(), paid))
///     .run()
///     .await?;
/// assert_eq!(sink.values(), [json!({"order_id": 1, "amount": 30})]);
/// # Ok(())
/// # }
/// ```
pub struct Lua {
    call: Blocking<Call>,
}

impl Lua {
    /// Compile `code` into a handler. Syntax errors are reported here.
    pub fn new(code: &str) -> StdResult<Self, Error> {
        let script = Script::load(code)?;
        let call: Call = Box::new(move |input| script.call(input));
        Ok(Self {
            call: BlockingPool::new(1)?.blocking(call),
        })
    }
}

impl Handler<Value> for Lua {
    type Output = Emit<Value>;

    fn handle(&self, input: Value) -> impl Future<Output = Result<Emit<Value>>> + Send {
        self.call.handle(input)
    }
}

struct Script {
    lua: mlua::Lua,
    chunk: Function,
}

impl Script {
    fn load(code: &str) -> StdResult<Self, Error> {
        let libraries =
            StdLib::TABLE | StdLib::STRING | StdLib::UTF8 | StdLib::MATH | StdLib::COROUTINE;
        let lua = mlua::Lua::new_with(libraries, LuaOptions::new())
            .map_err(|error| Error::wrap("failed to create the Lua state", error))?;
        install_module(&lua)
            .map_err(|error| Error::wrap("failed to create the Lua state", error))?;
        // The prefix stays on the first line so error messages keep the user's line numbers.
        let chunk = lua
            .load(format!("local input = ...; {code}"))
            .set_name("=handler")
            .set_mode(ChunkMode::Text)
            .into_function()
            .map_err(|error| Error::config(format!("invalid Lua handler: {error}")))?;
        Ok(Self { lua, chunk })
    }

    fn call(&self, input: Value) -> Result<Emit<Value>> {
        let input = self
            .lua
            .to_value(&input)
            .map_err(|error| HandlerError::Reject(error.into()))?;
        let outputs: MultiValue = self.chunk.call(input).map_err(classify)?;
        let mut values = outputs
            .into_iter()
            .filter(|value| !value.is_nil())
            .map(|value| self.lua.from_value::<Value>(value))
            .collect::<StdResult<Vec<_>, _>>()
            .map_err(|error| {
                HandlerError::Reject(format!("Lua handler output is not JSON: {error}").into())
            })?;
        Ok(match values.len() {
            0 => Emit::None,
            1 => Emit::One(values.remove(0)),
            _ => Emit::Many(values),
        })
    }
}

fn install_module(lua: &mlua::Lua) -> mlua::Result<()> {
    let module = lua.create_table()?;
    module.set("null", lua.null())?;
    module.set(
        "retry",
        lua.create_function(|_, message: String| {
            Err::<(), _>(mlua::Error::external(RetryRequested(message)))
        })?,
    )?;
    lua.globals().set("beavers", module)
}

fn classify(error: mlua::Error) -> HandlerError {
    match retry_message(&error) {
        Some(message) => HandlerError::Retry(message.into()),
        None => HandlerError::Reject(error.into()),
    }
}

fn retry_message(error: &mlua::Error) -> Option<String> {
    match error {
        mlua::Error::CallbackError { cause, .. } | mlua::Error::WithContext { cause, .. } => {
            retry_message(cause)
        }
        mlua::Error::ExternalError(error) => error
            .downcast_ref::<RetryRequested>()
            .map(|retry| retry.0.clone()),
        _ => None,
    }
}

#[derive(Debug)]
struct RetryRequested(String);

impl fmt::Display for RetryRequested {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl StdError for RetryRequested {}
