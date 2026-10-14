use base64::{Engine, engine::general_purpose::STANDARD};

use super::{Codec, Decoder, Encoder};
use crate::error::BoxError;

/// A wrapper that carries another codec's payload as Base64 text.
///
/// Encoding applies the inner codec and then the standard Base64 alphabet
/// with padding (RFC 4648, section 4). Decoding reverses those steps and
/// rejects whitespace, line breaks, and the URL-safe alphabet. Use it to send
/// binary formats through brokers or bodies that carry text only.
#[derive(Clone, Copy, Debug, Default)]
pub struct Base64<C> {
    inner: C,
}

impl<C> Base64<C> {
    /// Wrap a configured inner codec.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }

    /// Return the inner codec.
    pub fn inner(&self) -> &C {
        &self.inner
    }
}

impl<C: Codec> Codec for Base64<C> {
    type Value = C::Value;
}

impl<C: Decoder> Decoder for Base64<C> {
    fn decode(&self, bytes: &[u8]) -> Result<C::Value, BoxError> {
        self.inner.decode(&STANDARD.decode(bytes)?)
    }
}

impl<C: Encoder> Encoder for Base64<C> {
    fn encode(&self, value: &C::Value) -> Result<Vec<u8>, BoxError> {
        Ok(STANDARD.encode(self.inner.encode(value)?).into_bytes())
    }
}
