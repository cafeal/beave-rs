# Gzip wrapper

`Gzip<C>` compresses the payload of an inner codec `C` as gzip
([RFC 1952](https://www.rfc-editor.org/rfc/rfc1952)) through `flate2`. Enable
the `gzip` Cargo feature to use it:

```toml
[dependencies]
beavers = { version = "0.1", features = ["gzip"] }
```

Encoding runs the inner codec and writes its output as one gzip member.
Decoding decompresses every concatenated member and passes the result to the
inner codec, so payloads from other gzip writers decode as well.

| Setting | Default | Range |
|---|---|---|
| `with_level` | 6 | 0 (store only) to 9 (smallest output) |
| `with_max_decoded_len` | 64 MiB | Any byte count |

```rust
use beavers::{Decoder, Encoder, Gzip, Json};

let codec = Gzip::new(Json::new()).with_level(9);
let bytes = codec.encode(&vec!["beaver"; 100])?;
let value: Vec<String> = codec.decode(&bytes)?;
assert_eq!(value.len(), 100);
# Ok::<(), beavers::BoxError>(())
```

`with_level` panics on a level above 9, because the value is fixed when the
codec is constructed. Decoding fails when the decompressed payload would exceed
`with_max_decoded_len`, and malformed compressed data is also a decode failure.
See [codec wrappers](../wrappers.md#choosing-compression) for when to compress
in the codec rather than in the broker client.
