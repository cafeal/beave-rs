# Avro codec

`Avro` encodes and decodes Serde values using a parsed Apache Avro schema. Enable
the `avro` Cargo feature:

```toml
[dependencies]
beavers = { version = "0.1", features = ["avro"] }
```

Construct the codec with a schema string. Construction parses and validates the
schema once; an invalid schema returns an error before any messages are handled.
The same schema is used as the writer and reader schema for the codec:

```rust
use beavers::{Avro, Decoder, Encoder};
use serde::{Deserialize, Serialize};

const SCHEMA: &str = r#"
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "comment", "type": ["null", "string"], "default": null}
  ]
}
"#;

#[derive(Deserialize, Serialize)]
struct Order {
    id: i64,
    comment: Option<String>,
}

let codec = Avro::new(SCHEMA)?;
let bytes = codec.encode(&Order { id: 7, comment: None })?;
let order: Order = codec.decode(&bytes)?;
# Ok::<(), anyhow::Error>(())
```

Each payload is exactly one raw Avro datum. `Avro` does not add an object
container header, sync marker, length prefix, or other framing. A decode succeeds
only when the schema consumes the complete payload; trailing bytes are rejected.
This makes the codec suitable for adapters that already preserve message
boundaries. It is directly compatible with Apache Avro's
`GenericDatumWriter`/`GenericDatumReader` APIs.

Serde types must match the schema's Avro data model. Records map to structs,
unit enums map to Avro enums, `Option<T>` maps to a nullable union, and `Vec<T>`
maps to an array. For Avro `bytes` and `fixed`, use the byte helper modules
documented by `apache-avro` when deriving Serde implementations.

The codec intentionally has no `Default` implementation because a schema is
required. The local stdin and stdout adapters currently default-construct their
codecs, so an `Avro` instance cannot be plugged into those constructors. Use the
codec directly until an adapter constructor that accepts an existing configured
codec is implemented.

Schema evolution is not implicit: this API has no separate reader schema. If a
producer and consumer use different schemas, create a codec with the producer's
schema only when they are known to be compatible, or use Apache Avro's datum
reader directly with explicit writer and reader schemas.
