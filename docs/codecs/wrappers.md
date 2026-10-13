# Codec wrappers

`Base64<C>`, `Gzip<C>`, and `Zstd<C>` wrap another codec `C` and transform its
payload. Encoding runs the inner codec first and then the wrapper; decoding runs
the wrapper first and passes its output to the inner codec. They accept any
inner codec, including other wrappers, and implement `Decoder<T>` or
`Encoder<T>` whenever the inner codec does.

| Wrapper | Cargo feature | Payload |
|---|---|---|
| [`Base64<C>`](wrappers/base64.md) | `base64` | Standard Base64 text of the inner payload |
| [`Gzip<C>`](wrappers/gzip.md) | `gzip` | One gzip member containing the inner payload |
| [`Zstd<C>`](wrappers/zstd.md) | `zstd` | One Zstandard frame containing the inner payload |

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

## Choosing compression

Gzip is readable by almost every platform; Zstandard compresses better and
faster where both ends support it. Both decompress into memory and fail to
decode once the output would exceed `with_max_decoded_len`, 64 MiB by default,
so a small, highly compressible payload cannot exhaust memory.

On Kafka, prefer the producer's batch compression: set the librdkafka
`compression.type` property, such as `zstd`, in `KafkaSinkConfig::properties`,
and consumers decompress transparently. Batch compression sees repeated
content across records and keeps payloads readable by any Kafka client. The
Pulsar sink does not expose producer compression yet. The compression wrappers
suit brokers without built-in compression, such as SQS, Pub/Sub, RabbitMQ, and
Azure Service Bus, and payloads that must stay compressed end to end.
Compressed payloads are binary, so SQS also needs `Base64` around them.
