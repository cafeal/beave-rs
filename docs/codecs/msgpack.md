# MessagePack codec

`MessagePack` converts between [MessagePack](https://msgpack.org/) payloads and
any type implementing Serde's traits, the same types `Json` accepts. It is a
unit struct that implements `Default`. Enable the `msgpack` Cargo feature to
use it:

```toml
[dependencies]
beavers = { version = "0.1", features = ["msgpack"] }
```

Encoding calls `rmp_serde::to_vec_named`; decoding reads the complete payload
with `rmp_serde`.

```rust
use beavers::{Decoder, Encoder, MessagePack};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Order {
    id: u64,
}

let bytes = MessagePack.encode(&Order { id: 42 })?;
let order: Order = MessagePack.decode(&bytes)?;
assert_eq!(order, Order { id: 42 });
# Ok::<(), beavers::BoxError>(())
```

## Struct representation

`MessagePack` encodes a struct as a map keyed by field name rather than as a
positional array. Payloads are larger than the array form, but other
MessagePack implementations can read them by name, and a reader tolerates
fields that were added, removed, or reordered, as with JSON. Decoding also
accepts the array form that `rmp_serde::to_vec` writes.

## Failures and framing

Decoding rejects malformed or truncated input and payloads that contain bytes
after the first value, so one payload cannot silently carry more than one value.
These are decode failures; see [lifecycle and failures](../codecs.md#lifecycle-and-failures).

`MessagePack` adds no framing. Its output is binary and can contain newline
bytes, so it does not suit line-based adapters such as `StdinSource`, and
brokers whose bodies carry text only, such as SQS, need the
[`Base64` wrapper](wrappers/base64.md).
