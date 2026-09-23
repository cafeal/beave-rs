use super::{Decoder, Encoder};
use std::str::from_utf8;

/// A codec for strings containing valid UTF-8.
#[derive(Clone, Copy, Debug, Default)]
pub struct Utf8;

impl Decoder<String> for Utf8 {
    fn decode(&self, bytes: &[u8]) -> anyhow::Result<String> {
        Ok(from_utf8(bytes)?.to_owned())
    }
}

impl Encoder<String> for Utf8 {
    fn encode(&self, value: &String) -> anyhow::Result<Vec<u8>> {
        Ok(value.as_bytes().to_vec())
    }
}
