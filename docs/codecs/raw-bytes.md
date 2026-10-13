# Raw bytes codec

`RawBytes` decodes a byte slice to `Vec<u8>` and encodes a `Vec<u8>` to bytes.
It copies the payload without interpreting it. Empty payloads and every byte
value are valid, including zero bytes and malformed UTF-8. It is a unit struct
that implements `Default`, so adapters that construct their codec internally
can use it, and it needs no Cargo feature.

```rust
use beavers::{Decoder, Encoder, RawBytes};

let codec = RawBytes;
let value = codec.decode(&[0, 0xff, b'\n'])?;
assert_eq!(codec.encode(&value)?, vec![0, 0xff, b'\n']);
# Ok::<(), beavers::BoxError>(())
```

Framing remains the adapter's responsibility. A line-based source may remove
or preserve delimiters according to its own contract, and a sink may append
delimiters after encoding. `RawBytes` does not change framing on its own. For
payloads that must be valid text, use [`Utf8`](utf8.md).
