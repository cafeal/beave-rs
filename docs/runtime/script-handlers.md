# JavaScript and Python handlers

The `javascript` and `python` features add handlers in `beavers::handlers` that
run code written in JavaScript or Python instead of Rust. Each takes the decoded input as
a JSON value and emits zero, one, or many JSON values, so it is registered with
`Subscription::new_emitting` between a source and a sink that use the
`Json<serde_json::Value>` codec:

```rust,ignore
use beavers::{App, Json, Subscription, adapters::kafka::Kafka, handlers::Python};
use serde_json::Value;

let paid = Python::new(r#"
def handle(input):
    if input["status"] != "paid":
        return None
    return {"order_id": input["id"], "amount": input["total"]}
"#)?;

let kafka = Kafka::new("localhost:9092");
App::new()
    .subscription(Subscription::new_emitting(
        "paid-orders",
        kafka.source::<Json<Value>>("paid-orders", ["orders"]),
        kafka.sink::<Json<Value>>("paid-orders"),
        paid,
    ))
    .run()
    .await?;
```

The handler takes the record's value, so outputs inherit the input's key and
headers as described in [handler shapes](handlers.md#handler-shapes). The code
cannot read or set broker metadata.

## JavaScript

`JavaScript::new(code)` runs the code once in the embedded QuickJS engine and
looks up its `handle(input)` function. Syntax errors, exceptions thrown while
the code runs, and a missing `handle` are reported as `Error::Config`.

```js
function handle(input) {
  if (input.status !== "paid") return;
  return { order_id: input.id, amount: input.total };
}
```

| Topic | Behavior |
|---|---|
| Input | The value passed through `JSON.parse` |
| Outputs | `undefined` or `null` emits nothing; a generator function (`function*`) emits one output per yielded value; any other value, including an array, is one output |
| Output conversion | `JSON.stringify`; a value it cannot encode, such as a function, rejects the input |
| Errors | A thrown value rejects the input; `throw new beavers.Retry(message)` retries it under the handler retry policy |
| `beavers` object | A predefined global holding `Retry` |
| Language | The ECMAScript standard as implemented by QuickJS-ng; there is no `require`, `import`, `fetch`, `console`, or other file, network, or process access |
| Execution | One engine per handler on its own worker thread, one input at a time; globals persist between inputs and retries |

`handle` must return its result directly: an `async` function or another value
with a `then` method rejects the input. QuickJS is compiled into the crate, so
the `javascript` feature needs a C compiler at build time and nothing at run
time.

## Python

`Python::new(code)` runs the code once in the embedded CPython interpreter and
looks up its `handle(input)` function. Syntax errors, exceptions raised while
the code runs, and a missing `handle` are reported as `Error::Config`.

```python
def handle(input):
    for item in input["items"]:
        yield {"order_id": input["id"], "item": item}
```

| Topic | Behavior |
|---|---|
| Input | The value passed through `json.loads` |
| Outputs | `None` emits nothing; a generator emits one output per yielded value; any other value, including a list, is one output |
| Output conversion | `json.dumps`; a value it cannot encode rejects the input |
| Errors | An exception rejects the input; `raise beavers.Retry(message)` retries it under the handler retry policy |
| `beavers` module | Predefined in the code's globals; `import beavers` returns the same module |
| Modules | The standard library and any package installed for the interpreter can be imported |
| Execution | Calls run on a `BlockingPool::default()` pool; module globals persist between inputs and are shared by concurrent calls |

The interpreter's global lock lets one call run Python code at a time, so
concurrent calls overlap only while one waits for I/O. Every Python handler of
a process shares one interpreter.

The `python` feature links against `libpython`. Building needs a Python 3
interpreter that PyO3 can find, by default `python3` on `PATH` or the one named
by `PYO3_PYTHON`, and running needs the same version's shared library. Neither
handler can interrupt code that does not return: an endless loop occupies its
worker thread until the process exits.
