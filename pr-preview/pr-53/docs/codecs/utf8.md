# UTF-8 codec

`Utf8` decodes bytes to `String` only when the complete payload is valid UTF-8,
and encodes a `String` using its UTF-8 representation. It does not trim text,
strip a byte-order mark, normalize Unicode, or remove line endings. An empty
payload decodes to an empty string. Malformed UTF-8 returns an error. It is a
unit struct that implements `Default`, so adapters that construct their codec
internally can use it, and it needs no Cargo feature. Its value type is fixed,
so a source or sink names it without a type argument, as in `KafkaSink<Utf8>`.

```rust
use beavers::{Decoder, Encoder, Utf8};

let codec = Utf8;
let value = codec.decode("hello\n".as_bytes())?;
assert_eq!(value, "hello\n");
assert_eq!(codec.encode(&value)?, b"hello\n");
# Ok::<(), beavers::BoxError>(())
```

Framing remains the adapter's responsibility. A line-based source may remove
or preserve delimiters according to its own contract, and a sink may append
delimiters after encoding. `Utf8` does not change framing on its own. For
arbitrary bytes, use [`RawBytes`](raw-bytes.md).
