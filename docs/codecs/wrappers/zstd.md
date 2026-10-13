# Zstd wrapper

`Zstd<C>` compresses the payload of an inner codec `C` as Zstandard
([RFC 8878](https://www.rfc-editor.org/rfc/rfc8878)) through the `zstd` crate,
which compiles the reference C library. Enable the `zstd` Cargo feature to use
it:

```toml
[dependencies]
beavers = { version = "0.1", features = ["zstd"] }
```

Encoding runs the inner codec and writes its output as one Zstandard frame.
Decoding decompresses every concatenated frame and passes the result to the
inner codec, so payloads from other Zstandard writers decode as well.

| Setting | Default | Range |
|---|---|---|
| `with_level` | 3 | Negative levels for speed up to 22; the linked library's range |
| `with_max_decoded_len` | 64 MiB | Any byte count |

```rust
use beavers::{Decoder, Encoder, Json, Zstd};

let codec = Zstd::new(Json)
    .with_level(9)
    .with_max_decoded_len(1024 * 1024);
let bytes = codec.encode(&vec!["beaver"; 100])?;
let value: Vec<String> = codec.decode(&bytes)?;
assert_eq!(value.len(), 100);
# Ok::<(), beavers::BoxError>(())
```

`with_level` panics on a level outside the library's range, because the value
is fixed when the codec is constructed. Decoding fails when the decompressed
payload would exceed `with_max_decoded_len`, and malformed compressed data is
also a decode failure. See
[codec wrappers](../wrappers.md#choosing-compression) for when to compress in
the codec rather than in the broker client.
