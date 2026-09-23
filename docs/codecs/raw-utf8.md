# Raw byte and UTF-8 codecs

`RawBytes` and `Utf8` provide lossless conversions for payloads that do not
need a structured serialization format. Both are unit structs and implement
`Default`, so they can be used by adapters that construct codecs internally.

## RawBytes

`RawBytes` decodes a byte slice to `Vec<u8>` and encodes a `Vec<u8>` to bytes.
It copies the payload without interpreting it. Empty payloads and every byte
value are valid, including zero bytes and malformed UTF-8.

```rust
use beavers::{Decoder, Encoder, RawBytes};

let codec = RawBytes;
let value = codec.decode(&[0, 0xff, b'\n'])?;
assert_eq!(codec.encode(&value)?, vec![0, 0xff, b'\n']);
# Ok::<(), anyhow::Error>(())
```

## Utf8

`Utf8` decodes bytes to `String` only when the complete payload is valid UTF-8,
and encodes a `String` using its UTF-8 representation. It does not trim text,
strip a byte-order mark, normalize Unicode, or remove line endings. An empty
payload decodes to an empty string. Malformed UTF-8 returns an error.

```rust
use beavers::{Decoder, Encoder, Utf8};

let codec = Utf8;
let value = codec.decode("hello\n".as_bytes())?;
assert_eq!(value, "hello\n");
assert_eq!(codec.encode(&value)?, b"hello\n");
# Ok::<(), anyhow::Error>(())
```

Framing remains the adapter's responsibility. In particular, a line-based
source may remove or preserve delimiters according to its own contract, and a
sink may append delimiters after encoding. Neither codec changes framing on its
own.
