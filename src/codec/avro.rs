use crate::error::{BoxError, Error};
use std::io::Cursor;

use apache_avro::{
    Schema, from_value, reader::datum::GenericDatumReader, writer::datum::GenericDatumWriter,
};
use serde::{Serialize, de::DeserializeOwned};

use super::{Decoder, Encoder};

/// A schema-bound codec for one raw Apache Avro datum.
///
/// `Avro` stores a parsed schema and uses it for both encoding and decoding.
/// Payloads contain the Avro binary datum only: this codec does not write an
/// Avro object-container header, sync marker, or record framing. Decoding
/// rejects bytes after the datum so one payload cannot silently contain more
/// than one value.
#[derive(Clone, Debug)]
pub struct Avro {
    schema: Schema,
}

impl Avro {
    /// Parse and validate an Avro schema once for subsequent codec calls.
    pub fn new(schema: &str) -> Result<Self, Error> {
        Ok(Self {
            schema: Schema::parse_str(schema)
                .map_err(|error| Error::config(format!("invalid Avro schema: {error}")))?,
        })
    }

    /// Build a codec from an already parsed and validated schema.
    pub fn from_schema(schema: Schema) -> Self {
        Self { schema }
    }

    /// Return the parsed schema used by this codec.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<T> Decoder<T> for Avro
where
    T: DeserializeOwned,
{
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        let mut input = Cursor::new(bytes);
        let reader = GenericDatumReader::builder(&self.schema).build()?;
        let value = reader.read_value(&mut input)?;

        if input.position() != bytes.len() as u64 {
            return Err(Error::invalid_record(format!(
                "Avro datum has {} trailing byte(s)",
                bytes.len() as u64 - input.position()
            ))
            .into());
        }

        Ok(from_value(&value)?)
    }
}

impl<T> Encoder<T> for Avro
where
    T: Serialize,
{
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        let writer = GenericDatumWriter::builder(&self.schema).build()?;
        Ok(writer.write_ser_to_vec(value)?)
    }
}
