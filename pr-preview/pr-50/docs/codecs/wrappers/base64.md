# Base64 wrapper

`Base64<C>` carries the payload of an inner codec `C` as Base64 text. Enable
the `base64` Cargo feature to use it:

```toml
[dependencies]
beavers = { version = "0.1", features = ["base64"] }
```

Encoding runs the inner codec and encodes its output with the standard alphabet
and padding from
[RFC 4648, section 4](https://www.rfc-editor.org/rfc/rfc4648#section-4).
Decoding accepts exactly that form: whitespace, line breaks, missing padding,
and the URL-safe alphabet are errors. The output is ASCII, which makes binary
codecs usable where bodies carry text only, such as [SQS](../../adapters/sqs.md)
bodies.

```rust
use beavers::{Base64, Decoder, Encoder, RawBytes};

let codec = Base64::new(RawBytes);
assert_eq!(codec.encode(&vec![0xfb, 0xff])?, b"+/8=");
assert_eq!(codec.decode(b"+/8=")?, vec![0xfb, 0xff]);
# Ok::<(), beavers::BoxError>(())
```

`Base64::new(inner)` wraps a configured codec, and `Base64::default()` wraps
`C::default()`. See [codec wrappers](../wrappers.md) for nesting and use with
adapters.
