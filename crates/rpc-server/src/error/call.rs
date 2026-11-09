//! Errors answered to a call.

use super::ErrorCode;
use alloy_json_rpc::ErrorPayload;
use serde_json::value::{RawValue, to_raw_value};

// Caps the error `data` echoed back to the caller.
const MAX_ERROR_DATA_LEN: usize = 1024;

/// The params of a call don't match the method's arguments.
///
/// Converts into `-32602 Invalid params` carrying the decoder's message.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InvalidParams(#[from] serde_json::Error);

impl From<InvalidParams> for ErrorPayload {
    fn from(err: InvalidParams) -> Self {
        let mut message = err.to_string();
        if message.len() > MAX_ERROR_DATA_LEN {
            message.truncate(message.floor_char_boundary(MAX_ERROR_DATA_LEN));
            message.push('…');
        }
        Self { data: error_data(&message), ..ErrorCode::InvalidParams.into() }
    }
}

// A batch that can't be run, answered with a `null` id.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BatchError {
    // Batches are disabled.
    NotSupported,
    // The body isn't a JSON array.
    Parse,
    // The batch has more entries than allowed.
    TooBig {
        // The most entries allowed.
        limit: usize,
    },
}

impl From<BatchError> for ErrorPayload {
    fn from(err: BatchError) -> Self {
        match err {
            BatchError::NotSupported => ErrorCode::BatchesNotSupported.into(),
            BatchError::Parse => ErrorCode::ParseError.into(),
            BatchError::TooBig { limit } => ErrorCode::TooBigBatchRequest.exceeded(limit),
        }
    }
}

// A message as the JSON string in an error's `data`.
pub(super) fn error_data(message: &str) -> Option<Box<RawValue>> {
    // Never panic while answering a bad request, even though a string always serializes.
    to_raw_value(message).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(json: &str) -> ErrorPayload {
        InvalidParams(serde_json::from_str::<u64>(json).unwrap_err()).into()
    }

    fn data(payload: ErrorPayload) -> String {
        serde_json::from_str(payload.data.unwrap().get()).unwrap()
    }

    #[test]
    fn carries_the_message() {
        let payload = payload(r#""x""#);
        assert_eq!((payload.code, payload.message.as_ref()), (-32602, "Invalid params"));
        assert_eq!(data(payload), "invalid type: string \"x\", expected u64 at line 1 column 3");
    }

    #[test]
    fn long_data_is_cut() {
        let value = "é".repeat(MAX_ERROR_DATA_LEN);
        let message = format!("invalid type: string \"{value}");
        let cut = &message[..message.floor_char_boundary(MAX_ERROR_DATA_LEN)];
        assert_eq!(data(payload(&format!(r#""{value}""#))), format!("{cut}…"));
    }
}
