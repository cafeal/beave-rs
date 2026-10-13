use std::io::Read;

use crate::error::{BoxError, Error};

/// The default upper bound for a decompressed payload: 64 MiB.
pub(super) const DEFAULT_MAX_DECODED_LEN: usize = 64 * 1024 * 1024;

/// Read a decompressing reader to its end, failing once the output exceeds
/// `max` bytes so a small payload cannot expand without bound.
pub(super) fn read_to_end(
    reader: impl Read,
    max: usize,
    format: &str,
) -> Result<Vec<u8>, BoxError> {
    let mut output = Vec::new();
    reader.take(max as u64 + 1).read_to_end(&mut output)?;
    if output.len() > max {
        return Err(Error::invalid_record(format!(
            "{format} payload decompresses to more than {max} byte(s)"
        ))
        .into());
    }
    Ok(output)
}
