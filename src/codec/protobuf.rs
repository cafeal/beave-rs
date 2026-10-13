use std::marker::PhantomData;

use super::typed::typed_codec;
use super::{Decoder, Encoder};
use crate::error::BoxError;

/// A codec for raw Protocol Buffers messages of type `T`.
///
/// The payload is the message itself. This codec does not add a length
/// delimiter or any other framing; framing belongs to the adapter.
pub struct Protobuf<T>(PhantomData<fn() -> T>);

typed_codec!(Protobuf);

impl<T> Decoder for Protobuf<T>
where
    T: prost::Message + Default + 'static,
{
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        Ok(T::decode(bytes)?)
    }
}

impl<T> Encoder for Protobuf<T>
where
    T: prost::Message + 'static,
{
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        Ok(value.encode_to_vec())
    }
}
