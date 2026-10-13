# Base64 and compression wrappers

`Base64<C>`, `Gzip<C>`, and `Zstd<C>` wrap another codec `C` and transform its
payload. Encoding runs the inner codec first and then the wrapper; decoding runs
the wrapper first and passes its output to the inner codec. They accept any
inner codec, including other wrappers, and implement `Decoder<T>` or
`Encoder<T>` whenever the inner codec does.

| Wrapper | Cargo feature | Payload |
|---|---|---|
| `Base64<C>` | `base64` | Standard Base64 text of the inner payload |
| `Gzip<C>` | `gzip` | One gzip member containing the inner payload |
| `Zstd<C>` | `zstd` | One Zstandard frame containing the inner payload |

Each wrapper implements `Default` when its inner codec does, so it works with
adapters that construct their codec with `C::default()`. Use the adapter's
`with_codec` constructor to pass a wrapper with non-default settings or a
configured inner codec such as `Avro`.

```rust,ignore
use beavers::adapters::sqs::{SqsSink, SqsSinkConfig};
use beavers::{Base64, Gzip, Protobuf};

// Protobuf bytes, gzip-compressed, carried as text in an SQS body.
let sink = SqsSink::<Base64<Gzip<Protobuf>>, Event>::new(config);
```

The order of nested wrappers is the order of the encoding steps from the inside
out: `Base64<Gzip<Json>>` compresses JSON and then encodes the compressed bytes
as Base64. The reader of the payload must use the same nesting.

Wrappers do not record what they did in the payload or in message metadata. A
decoder therefore expects every payload in a stream to use the same wrapping;
a subscription that receives both compressed and uncompressed payloads needs a
custom codec that tells them apart.

## Base64

`Base64<C>` encodes the inner payload with the standard alphabet and padding
from [RFC 4648, section 4](https://www.rfc-editor.org/rfc/rfc4648#section-4).
Decoding accepts exactly that form: whitespace, line breaks, missing padding,
and the URL-safe alphabet are errors. Its output is ASCII, which makes binary
codecs usable where bodies carry text only, such as
[SQS](../adapters/sqs.md) bodies.

```rust
use beavers::{Base64, Decoder, Encoder, RawBytes};

let codec = Base64::new(RawBytes);
assert_eq!(codec.encode(&vec![0xfb, 0xff])?, b"+/8=");
assert_eq!(codec.decode(b"+/8=")?, vec![0xfb, 0xff]);
# Ok::<(), beavers::BoxError>(())
```

## Compression

`Gzip<C>` writes [RFC 1952](https://www.rfc-editor.org/rfc/rfc1952) gzip
through `flate2`. `Zstd<C>` writes
[RFC 8878](https://www.rfc-editor.org/rfc/rfc8878) Zstandard frames through the
`zstd` crate, which compiles the reference C library. Gzip is readable by
almost every platform; Zstandard compresses better and faster where both ends
support it.

| Setting | `Gzip<C>` | `Zstd<C>` |
|---|---|---|
| Default level | 6 | 3 |
| `with_level` range | 0 (store only) to 9 | Negative levels for speed up to 22; the linked library's range |
| Decoding | Every concatenated member | Every concatenated frame |
| `with_max_decoded_len` default | 64 MiB | 64 MiB |

`with_level` panics on a level outside the range, because the value is fixed
when the codec is constructed. Decoding fails when the decompressed payload
would exceed `with_max_decoded_len`, so a small, highly compressible payload
cannot exhaust memory. Malformed compressed data is also a decode failure.

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

On Kafka, prefer the producer's batch compression: set the librdkafka
`compression.type` property, such as `zstd`, in `KafkaSinkConfig::properties`,
and consumers decompress transparently. Batch compression sees repeated
content across records and keeps payloads readable by any Kafka client. The
Pulsar sink does not expose producer compression yet. The wrappers suit
brokers without built-in compression, such as SQS, Pub/Sub, RabbitMQ, and
Azure Service Bus, and payloads that must stay compressed end to end.
Compressed payloads are binary, so SQS also needs `Base64` around them.
