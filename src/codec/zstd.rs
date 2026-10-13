use std::io::BufReader;

use super::bounded::{DEFAULT_MAX_DECODED_LEN, read_to_end};
use super::{Decoder, Encoder};
use crate::error::BoxError;

/// A wrapper that Zstandard-compresses another codec's payload.
///
/// Encoding applies the inner codec and compresses its output as one
/// Zstandard frame (RFC 8878). Decoding decompresses every concatenated frame
/// and passes the result to the inner codec. Decompressed output is limited to
/// 64 MiB by default; a payload that expands beyond the limit fails to decode.
#[derive(Clone, Copy, Debug)]
pub struct Zstd<C> {
    inner: C,
    level: i32,
    max_decoded_len: usize,
}

impl<C> Zstd<C> {
    /// Wrap a configured inner codec using Zstandard's default level, 3.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            level: zstd::DEFAULT_COMPRESSION_LEVEL,
            max_decoded_len: DEFAULT_MAX_DECODED_LEN,
        }
    }

    /// Set the compression level. Higher levels produce smaller output more
    /// slowly; negative levels trade size for speed.
    ///
    /// # Panics
    ///
    /// Panics if `level` is outside the range the linked Zstandard library
    /// supports, which is `-131072..=22` for current releases.
    pub fn with_level(mut self, level: i32) -> Self {
        let range = zstd::compression_level_range();
        assert!(
            range.contains(&level),
            "zstd compression level must be in {range:?}, got {level}"
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

impl<C: Default> Default for Zstd<C> {
    fn default() -> Self {
        Self::new(C::default())
    }
}

impl<T, C: Decoder<T>> Decoder<T> for Zstd<C> {
    fn decode(&self, bytes: &[u8]) -> Result<T, BoxError> {
        let reader = zstd::stream::read::Decoder::with_buffer(BufReader::new(bytes))?;
        let payload = read_to_end(reader, self.max_decoded_len, "Zstandard")?;
        self.inner.decode(&payload)
    }
}

impl<T, C: Encoder<T>> Encoder<T> for Zstd<C> {
    fn encode(&self, value: &T) -> Result<Vec<u8>, BoxError> {
        Ok(zstd::bulk::compress(
            &self.inner.encode(value)?,
            self.level,
        )?)
    }
}
