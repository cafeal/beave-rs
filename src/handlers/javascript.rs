use crate::blocking::{Blocking, BlockingPool};
use crate::error::Error;
use crate::handler::{Emit, Handler, HandlerError, Result};
use rquickjs::{
    CatchResultExt, CaughtError, Coerced, Context, Function, Object, Runtime, context::EvalOptions,
};
use serde_json::Value;
use std::{future::Future, result::Result as StdResult};

type Call = Box<dyn Fn(Value) -> Result<Emit<Value>> + Send + Sync>;

/// The file name of [`PRELUDE`], whose frames are left out of error stacks.
const PRELUDE_FILE: &str = "beavers";

/// The hidden global holding the wrapped `handle` function.
const CALL: &str = "__beavers_call";

/// Wraps the user's `handle` so that conversions, generators, and `beavers.Retry`
/// are handled in JavaScript and Rust only exchanges JSON text.
const PRELUDE: &str = r#"
(() => {
  class Retry extends Error {
    constructor(message) {
      super(message);
      this.name = "Retry";
    }
  }
  globalThis.beavers = Object.freeze({ Retry });
  const isGenerator = (value) => Object.prototype.toString.call(value) === "[object Generator]";
  const encode = (value) => {
    const json = JSON.stringify(value);
    if (json === undefined) throw new TypeError("handler output is not JSON");
    return json;
  };
  const wrap = (handle) => (input) => {
    try {
      const output = handle(JSON.parse(input));
      if (output === undefined || output === null) return { outputs: [] };
      if (typeof output.then === "function") throw new TypeError("handle must not return a promise");
      if (isGenerator(output)) return { outputs: Array.from(output, encode) };
      return { outputs: [encode(output)] };
    } catch (error) {
      if (error instanceof Retry) return { retry: String(error.message) };
      throw error;
    }
  };
  return (handle) => Object.defineProperty(globalThis, "__beavers_call", { value: wrap(handle) });
})()
"#;

/// A handler running a JavaScript `handle(input)` function for each input
/// (`javascript` feature).
///
/// The code runs once when the handler is created and must define a global
/// `handle` function. Each input is passed to it as a value produced by
/// `JSON.parse`. Returning `undefined` or `null` emits nothing, a generator emits
/// one output per yielded value, and any other value, including an array, is one
/// output encoded with `JSON.stringify`.
///
/// A thrown error rejects the input, and `throw new beavers.Retry(message)` retries
/// it under the subscription's retry policy. The code runs in the QuickJS engine
/// without file, network, or module access, on a dedicated worker thread, one input
/// at a time. Globals persist between inputs.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), beavers::Error> {
/// use beavers::{App, InMemorySink, IterSource, Subscription, handlers::JavaScript};
/// use serde_json::{Value, json};
///
/// let paid = JavaScript::new(
///     r#"
///     function handle(input) {
///       if (input.status !== "paid") return;
///       return { order_id: input.id, amount: input.total };
///     }
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
pub struct JavaScript {
    call: Blocking<Call>,
}

impl JavaScript {
    /// Run `code` and look up its `handle` function. Syntax errors, exceptions
    /// thrown by the code, and a missing `handle` are reported here.
    pub fn new(code: &str) -> StdResult<Self, Error> {
        let script = Script::load(code)?;
        let call: Call = Box::new(move |input| script.call(input));
        Ok(Self {
            call: BlockingPool::new(1)?.blocking(call),
        })
    }
}

impl Handler<Value> for JavaScript {
    type Output = Emit<Value>;

    fn handle(&self, input: Value) -> impl Future<Output = Result<Emit<Value>>> + Send {
        self.call.handle(input)
    }
}

struct Script {
    // Fields drop in order, so the context goes before its runtime.
    context: Context,
    _runtime: Runtime,
}

impl Script {
    fn load(code: &str) -> StdResult<Self, Error> {
        let engine = |error| Error::wrap("failed to create the JavaScript engine", error);
        let runtime = Runtime::new().map_err(engine)?;
        let context = Context::full(&runtime).map_err(engine)?;
        context.with(|ctx| -> StdResult<_, Error> {
            let mut prelude = EvalOptions::default();
            prelude.filename = Some(PRELUDE_FILE.to_owned());
            let wrap: Function = ctx
                .eval_with_options(PRELUDE, prelude)
                .catch(&ctx)
                .map_err(|error| invalid(&error))?;
            let mut options = EvalOptions::default();
            options.strict = false;
            options.filename = Some("handler".to_owned());
            ctx.eval_with_options::<(), _>(code, options)
                .catch(&ctx)
                .map_err(|error| invalid(&error))?;
            // Evaluating the name also finds `const` and `let` declarations, which are
            // not properties of the global object.
            let handle: Option<Function> = ctx
                .eval(r#"typeof handle === "function" ? handle : undefined"#)
                .catch(&ctx)
                .map_err(|error| invalid(&error))?;
            let handle = handle.ok_or_else(|| {
                Error::config("invalid JavaScript handler: no `handle` function is defined")
            })?;
            wrap.call::<_, ()>((handle,))
                .catch(&ctx)
                .map_err(|error| invalid(&error))
        })?;
        Ok(Self {
            context,
            _runtime: runtime,
        })
    }

    fn call(&self, input: Value) -> Result<Emit<Value>> {
        let input = input.to_string();
        let outcome = self.context.with(|ctx| {
            let call: Function = ctx
                .globals()
                .get(CALL)
                .map_err(|error| HandlerError::Reject(error.into()))?;
            let result: Object = call
                .call((input,))
                .catch(&ctx)
                .map_err(|error| HandlerError::Reject(describe(&error).into()))?;
            if let Some(message) = result
                .get::<_, Option<String>>("retry")
                .map_err(|error| HandlerError::Reject(error.into()))?
            {
                return Err(HandlerError::Retry(message.into()));
            }
            result
                .get::<_, Vec<String>>("outputs")
                .map_err(|error| HandlerError::Reject(error.into()))
        })?;
        let mut values = outcome
            .iter()
            .map(|json| serde_json::from_str(json))
            .collect::<StdResult<Vec<Value>, _>>()
            .map_err(|error| HandlerError::Reject(error.into()))?;
        Ok(match values.len() {
            0 => Emit::None,
            1 => Emit::One(values.remove(0)),
            _ => Emit::Many(values),
        })
    }
}

fn invalid(error: &CaughtError<'_>) -> Error {
    Error::config(format!("invalid JavaScript handler: {}", describe(error)))
}

/// Format a thrown value as `Name: message` followed by the stack frames of the
/// handler code.
fn describe(error: &CaughtError<'_>) -> String {
    match error {
        CaughtError::Exception(exception) => {
            let name = exception
                .get::<_, Coerced<String>>("name")
                .map_or_else(|_| "Error".to_owned(), |name| name.0);
            let mut text = match exception.message() {
                Some(message) if !message.is_empty() => format!("{name}: {message}"),
                _ => name,
            };
            let prelude = format!("({PRELUDE_FILE}:");
            for frame in exception.stack().iter().flat_map(|stack| stack.lines()) {
                if !frame.trim().is_empty() && !frame.contains(&prelude) {
                    text.push('\n');
                    text.push_str(frame);
                }
            }
            text
        }
        CaughtError::Value(value) => value.get::<Coerced<String>>().map_or_else(
            |_| "a non-string value was thrown".to_owned(),
            |text| text.0,
        ),
        CaughtError::Error(error) => error.to_string(),
    }
}
