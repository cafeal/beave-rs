# Lua and Python handlers

The `lua` and `python` features add handlers in `beavers::handlers` that run
code written in Lua or Python instead of Rust. Each takes the decoded input as
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

## Lua

`Lua::new(code)` compiles a Lua 5.4 chunk and reports syntax errors as
`Error::Config`. The chunk runs once per input:

```lua
if input.status ~= "paid" then return nil end
return { order_id = input.id, amount = input.total }
```

| Topic | Behavior |
|---|---|
| Input | The local `input`, converted from JSON: objects and arrays become tables, JSON `null` is `beavers.null` |
| Outputs | No value or `nil` emits nothing; each returned value other than `nil` is one output, so `return a, b` emits two |
| Output conversion | Tables with consecutive integer keys from 1 become arrays, other tables, including empty ones, objects; a function or other value that JSON cannot represent rejects the input |
| Errors | A Lua error rejects the input; `beavers.retry(message)` raises an error that retries it under the handler retry policy |
| Libraries | `table`, `string`, `utf8`, `math`, and `coroutine`; `io`, `os`, `require`, and `debug` are not available |
| Execution | One Lua state per handler on its own worker thread, one input at a time; globals persist between inputs and retries |

Lua 5.4 is compiled into the crate, so the `lua` feature needs a C compiler
at build time and nothing at run time.

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
