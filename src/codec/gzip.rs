use std::io::Write;

use flate2::{Compression, read::MultiGzDecoder, write::GzEncoder};

use super::bounded::{DEFAULT_MAX_DECODED_LEN, read_to_end};
use super::{Codec, Decoder, Encoder};
use crate::error::BoxError;

/// A wrapper that gzip-compresses another codec's payload.
///
/// Encoding applies the inner codec and compresses its output as one gzip
/// member (RFC 1952). Decoding decompresses every concatenated member and
/// passes the result to the inner codec. Decompressed output is limited to
/// 64 MiB by default; a payload that expands beyond the limit fails to decode.
#[derive(Clone, Copy, Debug)]
pub struct Gzip<C> {
    inner: C,
    level: u32,
    max_decoded_len: usize,
}

impl<C> Gzip<C> {
    /// Wrap a configured inner codec using compression level 6.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            level: 6,
            max_decoded_len: DEFAULT_MAX_DECODED_LEN,
        }
    }

    /// Set the compression level, from 0 (store only) to 9 (smallest output).
    ///
    /// # Panics
    ///
    /// Panics if `level` is greater than 9.
    pub fn with_level(mut self, level: u32) -> Self {
        assert!(
            level <= 9,
            "gzip compression level must be 0 to 9, got {level}"
        );
        self.level = level;
        self
    }

    /// Set the largest decompressed payload, in bytes, that decoding accepts.
    pub fn with_max_decoded_len(mut self, max: usize) -> Self {
        self.max_decoded_len = max;
        self
    }

    /// Return the inner codec.
    pub fn inner(&self) -> &C {
        &self.inner
    }
}

impl<C: Default> Default for Gzip<C> {
    fn default() -> Self {
        Self::new(C::default())
    }
}

impl<C: Codec> Codec for Gzip<C> {
    type Value = C::Value;
}

impl<C: Decoder> Decoder for Gzip<C> {
    fn decode(&self, bytes: &[u8]) -> Result<C::Value, BoxError> {
        let payload = read_to_end(MultiGzDecoder::new(bytes), self.max_decoded_len, "gzip")?;
        self.inner.decode(&payload)
    }
}

impl<C: Encoder> Encoder for Gzip<C> {
    fn encode(&self, value: &C::Value) -> Result<Vec<u8>, BoxError> {
        let payload = self.inner.encode(value)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.level));
        encoder.write_all(&payload)?;
        Ok(encoder.finish()?)
    }
}
