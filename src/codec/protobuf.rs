use super::{Decoder, Encoder};

/// A codec for raw Protocol Buffers messages.
///
/// The payload is the message itself. This codec does not add a length
/// delimiter or any other framing; framing belongs to the adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct Protobuf;

impl<T> Decoder<T> for Protobuf
where
    T: prost::Message + Default,
{
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<T> {
        Ok(T::decode(bytes)?)
    }
}

impl<T> Encoder<T> for Protobuf
where
    T: prost::Message,
{
    fn encode(&self, value: &T) -> anyhow::Result<Vec<u8>> {
        Ok(value.encode_to_vec())
    }
}
