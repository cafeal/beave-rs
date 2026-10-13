# MessagePack and CBOR codecs

`MessagePack` and `Cbor` convert between compact binary payloads and any type
implementing Serde's traits, the same types `Json` accepts. Both are unit
structs that implement `Default`. Enable the `msgpack` or `cbor` Cargo feature
to use them:

```toml
[dependencies]
beavers = { version = "0.1", features = ["msgpack", "cbor"] }
```

| Codec | Format | Encoding | Decoding |
|---|---|---|---|
| `MessagePack` | [MessagePack](https://msgpack.org/) | `rmp_serde::to_vec_named` | `rmp_serde` from the complete payload |
| `Cbor` | CBOR, [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949) | `ciborium::into_writer` | `ciborium::from_reader` over the complete payload |

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

## MessagePack

`MessagePack` encodes a struct as a map keyed by field name rather than as a
positional array. Payloads are larger than the array form, but other
MessagePack implementations can read them by name, and a reader tolerates
fields that were added, removed, or reordered, as with JSON. Decoding also
accepts the array form that `rmp_serde::to_vec` writes.

## CBOR

`Cbor` writes the representation `ciborium` chooses for each Serde type: a
struct is a map keyed by field name, and `Vec<u8>` is an array of integers
unless the field uses a byte-string helper such as `serde_bytes`. It does not
write a self-describe tag, CBOR sequences, or COSE structures.

## Failures

Both codecs reject malformed or truncated input and payloads that contain bytes
after the first value, so one payload cannot silently carry more than one value.
These are decode failures; see [lifecycle and failures](../codecs.md#lifecycle-and-failures).

Neither codec adds framing. Their output is binary and can contain newline
bytes, so it does not suit line-based adapters such as `StdinSource`, and
brokers whose bodies carry text only, such as SQS, need a
[Base64 wrapper](wrappers.md#base64).
