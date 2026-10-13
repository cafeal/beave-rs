# Avro codec

`Avro<T>` encodes and decodes a Serde type `T` using a parsed Apache Avro schema. Enable
the `avro` Cargo feature:

```toml
[dependencies]
beavers = { version = "0.1", features = ["avro"] }
```

`Avro<T>` is bound to one Avro schema, which it uses as both the writer and the
reader schema. The schema usually comes from `T` through Apache Avro's
`AvroSchema` trait, re-exported as `beavers::AvroSchema`. When `T` implements
it, `Avro<T>` implements `Default`, so sources and sinks can build it
themselves:

```rust,ignore
let invoices = KafkaSink::<Avro<Invoice>>::new(KafkaSinkConfig::new(brokers, "invoices"));
```

`Default` calls `T::get_schema()` once, when the codec is constructed; messages
reuse the parsed schema.

## Schema from an `.avsc` file

When the schema is a contract shared with other teams or languages, keep it in
an `.avsc` file and implement `AvroSchema` by hand. The file stays the source of
truth, and schema changes are reviewed as diffs of that file:

```rust,ignore
use beavers::AvroSchema;
use apache_avro::Schema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct Invoice {
    id: String,
    amount_cents: i64,
}

impl AvroSchema for Invoice {
    fn get_schema() -> Schema {
        Schema::parse_str(include_str!("../schemas/invoice.avsc"))
            .expect("schemas/invoice.avsc is a valid Avro schema")
    }
}
```

The struct can also be generated from the `.avsc` file in a build script with
a code generator such as `rsgen-avro`. Code generation is application-side,
like `prost-build` for Protobuf; beavers only requires the resulting type to
implement `AvroSchema` and Serde's traits.

## Schema derived from the Rust type

When the schema is used only by Rust services, derive it from the struct with
the `derive` feature of `apache-avro`:

```rust,ignore
use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(AvroSchema, Deserialize, Serialize)]
struct Invoice {
    id: String,
    amount_cents: i64,
}
```

The derived schema is rebuilt from the struct, so its details, such as `doc`
and `default` values, can differ from a hand-written `.avsc` file.

Both forms use the application's own `apache-avro` dependency. Its version must
be semver-compatible with the one beavers uses, currently `0.22`, so that both
name the same `AvroSchema` trait.

## Explicit schema

`Avro::<T>::new(schema)` parses a schema string and
`Avro::<T>::from_schema(schema)` takes a parsed one, for schemas loaded at run
time or types that do not implement `AvroSchema`. Construction validates the
schema once; an invalid schema returns an error before any messages are
handled. Pass the codec to an adapter's `with_codec` constructor:

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

let codec = Avro::<Order>::new(SCHEMA)?;
let bytes = codec.encode(&Order { id: 7, comment: None })?;
let order = codec.decode(&bytes)?;
# Ok::<(), beavers::BoxError>(())
```

## Payloads

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

Schema evolution is not implicit: this API has no separate reader schema. If a
producer and consumer use different schemas, create a codec with the producer's
schema only when they are known to be compatible, or use Apache Avro's datum
reader directly with explicit writer and reader schemas.
