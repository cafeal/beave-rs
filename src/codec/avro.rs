use crate::error::{BoxError, Error};
use std::fmt;
use std::io::Cursor;
use std::marker::PhantomData;

use apache_avro::{
    AvroSchema, Schema, from_value, reader::datum::GenericDatumReader,
    writer::datum::GenericDatumWriter,
};
use serde::{Serialize, de::DeserializeOwned};

use super::{Codec, Decoder, Encoder};

/// A schema-bound codec for one raw Apache Avro datum of type `T`.
///
/// `Avro` stores a parsed schema and uses it for both encoding and decoding.
/// When `T` implements [`AvroSchema`], `Avro::default()` takes the schema from
/// `T`; [`Avro::new`] and [`Avro::from_schema`] supply one explicitly.
///
/// Payloads contain the Avro binary datum only: this codec does not write an
/// Avro object-container header, sync marker, or record framing. Decoding
/// rejects bytes after the datum so one payload cannot silently contain more
/// than one value.
pub struct Avro<T> {
    schema: Schema,
    marker: PhantomData<fn() -> T>,
}

impl<T> Avro<T> {
    /// Parse and validate an Avro schema once for subsequent codec calls.
    pub fn new(schema: &str) -> Result<Self, Error> {
        Ok(Self::from_schema(Schema::parse_str(schema).map_err(
            |error| Error::config(format!("invalid Avro schema: {error}")),
        )?))
    }

    /// Build a codec from an already parsed and validated schema.
    pub fn from_schema(schema: Schema) -> Self {
        Self {
            schema,
            marker: PhantomData,
        }
    }

    /// Return the parsed schema used by this codec.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl<T: AvroSchema> Default for Avro<T> {
    /// Build a codec from the schema `T` declares.
    fn default() -> Self {
        Self::from_schema(T::get_schema())
    }
}

impl<T> Clone for Avro<T> {
    fn clone(&self) -> Self {
        Self::from_schema(self.schema.clone())
    }
}

impl<T> fmt::Debug for Avro<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Avro")
            .field("type", &std::any::type_name::<T>())
            .field("schema", &self.schema)
            .finish()
    }
}

impl<T: 'static> Codec for Avro<T> {
    type Value = T;
}

impl<T> Decoder for Avro<T>
where
    T: DeserializeOwned + 'static,
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

impl<T> Encoder for Avro<T>
where
    T: Serialize + 'static,
{
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        let writer = GenericDatumWriter::builder(&self.schema).build()?;
        Ok(writer.write_ser_to_vec(value)?)
    }
}
