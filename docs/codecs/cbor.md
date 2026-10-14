# CBOR codec

`Cbor` converts between CBOR ([RFC 8949](https://www.rfc-editor.org/rfc/rfc8949))
payloads and a type `T` implementing Serde's traits, the same types `Json`
accepts. `Cbor<T>` holds no state and implements `Default`. Enable the `cbor` Cargo
feature to use it:

```toml
[dependencies]
beavers = { version = "0.1", features = ["cbor"] }
```

Encoding calls `ciborium::into_writer`; decoding calls `ciborium::from_reader`
over the complete payload.

```rust
use beavers::{Cbor, Decoder, Encoder};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Reading {
    sensor: String,
}

let bytes = Cbor::new().encode(&Reading { sensor: "dam-1".into() })?;
let reading: Reading = Cbor::new().decode(&bytes)?;
assert_eq!(reading.sensor, "dam-1");
# Ok::<(), beavers::BoxError>(())
```

## Representation

`Cbor` writes the representation `ciborium` chooses for each Serde type: a
struct is a map keyed by field name, and `Vec<u8>` is an array of integers
unless the field uses a byte-string helper such as `serde_bytes`. It does not
write a self-describe tag, CBOR sequences, or COSE structures.

## Failures and framing

Decoding rejects malformed or truncated input and payloads that contain bytes
after the first value, so one payload cannot silently carry more than one value.
These are decode failures; see [lifecycle and failures](../codecs.md#lifecycle-and-failures).

`Cbor` adds no framing. Its output is binary and can contain newline bytes, so
it does not suit line-based adapters such as `StdinSource`, and brokers whose
bodies carry text only, such as SQS, need the
[`Base64` wrapper](wrappers/base64.md).
