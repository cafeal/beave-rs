use crate::blocking::{Blocking, BlockingPool};
use crate::error::Error;
use crate::handler::{Emit, Handler, HandlerError, Result};
use pyo3::{
    prelude::*,
    types::{PyDict, PyModule},
};
use serde_json::Value;
use std::{ffi::CString, future::Future, result::Result as StdResult};

type Call = Box<dyn Fn(Value) -> Result<Emit<Value>> + Send + Sync>;

/// A handler calling a Python function for each input (`python` feature).
///
/// The code defines `handle(input)`, which receives the decoded input converted with
/// `json.loads` and returns the output: `None` emits nothing, a generator emits one
/// output per yielded value, and any other value emits one output. Outputs are
/// converted with `json.dumps`.
///
/// An exception rejects the input, and raising `beavers.Retry` retries it under the
/// subscription's retry policy. The module `beavers` is predefined in the code's
/// globals. Module globals persist between inputs.
///
/// The code runs in the CPython interpreter embedded in the process, which must be
/// able to load `libpython` at run time. Calls run on a [`BlockingPool::default`]
/// pool and hold the interpreter's global lock while Python code runs, so they overlap
/// only while one waits for I/O.
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), beavers::Error> {
/// use beavers::{App, InMemorySink, IterSource, Subscription, handlers::Python};
/// use serde_json::{Value, json};
///
/// let paid = Python::new(
///     r#"
/// def handle(input):
///     if input["status"] != "paid":
///         return None
///     return {"order_id": input["id"], "amount": input["total"]}
/// "#,
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
pub struct Python {
    call: Blocking<Call>,
}

impl Python {
    /// Run `code` once to define `handle`. Syntax errors, exceptions raised while the
    /// code runs, and a missing `handle` function are reported here.
    pub fn new(code: &str) -> StdResult<Self, Error> {
        let code = CString::new(code)
            .map_err(|_| Error::config("Python handler code contains a NUL byte"))?;
        let script = pyo3::Python::attach(|py| Script::load(py, &code))?;
        let call: Call = Box::new(move |input| script.call(input));
        Ok(Self {
            call: BlockingPool::default().blocking(call),
        })
    }
}

impl Handler<Value> for Python {
    type Output = Emit<Value>;

    fn handle(&self, input: Value) -> impl Future<Output = Result<Emit<Value>>> + Send {
        self.call.handle(input)
    }
}

struct Script {
    handle: Py<PyAny>,
    retry: Py<PyAny>,
    generator: Py<PyAny>,
    loads: Py<PyAny>,
    dumps: Py<PyAny>,
}

impl Script {
    fn load(py: pyo3::Python<'_>, code: &CString) -> StdResult<Self, Error> {
        let setup = |error: PyErr| Error::wrap("failed to prepare the Python interpreter", error);
        let module = beavers_module(py).map_err(setup)?;
        let json = py.import("json").map_err(setup)?;
        let generator = py
            .import("types")
            .and_then(|types| types.getattr("GeneratorType"))
            .map_err(setup)?;
        let globals = PyDict::new(py);
        globals
            .set_item("__name__", "__handler__")
            .and_then(|()| globals.set_item("beavers", &module))
            .map_err(setup)?;
        py.run(code, Some(&globals), None).map_err(|error| {
            Error::config(format!("invalid Python handler: {}", describe(py, &error)))
        })?;
        let handle = globals
            .get_item("handle")
            .map_err(setup)?
            .filter(|handle| handle.is_callable())
            .ok_or_else(|| {
                Error::config("Python handler code must define a function `handle(input)`")
            })?;
        Ok(Self {
            handle: handle.unbind(),
            retry: module.getattr("Retry").map_err(setup)?.unbind(),
            generator: generator.unbind(),
            loads: json.getattr("loads").map_err(setup)?.unbind(),
            dumps: json.getattr("dumps").map_err(setup)?.unbind(),
        })
    }

    fn call(&self, input: Value) -> Result<Emit<Value>> {
        let input =
            serde_json::to_string(&input).map_err(|error| HandlerError::Reject(error.into()))?;
        pyo3::Python::attach(|py| {
            let fail = |error: PyErr| self.classify(py, &error);
            let input = self.loads.call1(py, (input,)).map_err(fail)?;
            let result = self.handle.bind(py).call1((input,)).map_err(fail)?;
            if result.is_none() {
                return Ok(Emit::None);
            }
            if !result.is_instance(self.generator.bind(py)).map_err(fail)? {
                return self.to_json(py, &result).map(Emit::One);
            }
            let mut values = Vec::new();
            for value in result.try_iter().map_err(fail)? {
                values.push(self.to_json(py, &value.map_err(fail)?)?);
            }
            Ok(Emit::Many(values))
        })
    }

    fn to_json(&self, py: pyo3::Python<'_>, value: &Bound<'_, PyAny>) -> Result<Value> {
        let reject = |message: String| {
            HandlerError::Reject(format!("Python handler output is not JSON: {message}").into())
        };
        let text: String = self
            .dumps
            .call1(py, (value,))
            .and_then(|text| text.extract(py))
            .map_err(|error| reject(describe(py, &error)))?;
        serde_json::from_str(&text).map_err(|error| reject(error.to_string()))
    }

    fn classify(&self, py: pyo3::Python<'_>, error: &PyErr) -> HandlerError {
        if error.is_instance(py, self.retry.bind(py)) {
            return HandlerError::Retry(error.value(py).to_string().into());
        }
        HandlerError::Reject(describe(py, error).into())
    }
}

/// The predefined `beavers` module, shared by every handler of the interpreter so that
/// `import beavers` and the predefined global name the same `Retry` class.
fn beavers_module<'py>(py: pyo3::Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let modules = py.import("sys")?.getattr("modules")?;
    if let Ok(module) = modules.get_item("beavers") {
        return Ok(module);
    }
    let module = PyModule::new(py, "beavers")?;
    py.run(
        c"class Retry(Exception):\n    \"\"\"Retry the input under the subscription's retry policy.\"\"\"\n",
        Some(&module.dict()),
        None,
    )?;
    modules.set_item("beavers", &module)?;
    Ok(module.into_any())
}

/// The exception with its traceback, when there is one.
fn describe(py: pyo3::Python<'_>, error: &PyErr) -> String {
    match error
        .traceback(py)
        .and_then(|traceback| traceback.format().ok())
    {
        Some(traceback) => format!("{error}\n{traceback}"),
        None => error.to_string(),
    }
}
