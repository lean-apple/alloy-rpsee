//! Server errors.

use alloy_json_rpc::ErrorPayload;
use serde_json::value::{RawValue, to_raw_value};
use std::borrow::Cow;

// Caps the error `data` echoed back to the caller.
const MAX_ERROR_DATA_LEN: usize = 1024;

// A response exceeded the size limit.
const OVERSIZED_RESPONSE_CODE: i64 = -32008;

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
        Self { data: error_data(&message), ..Self::invalid_params() }
    }
}

// A response exceeded its size limit. Converts into `-32008 Response is too big` naming the limit.
#[derive(Debug, thiserror::Error)]
#[error("Exceeded max limit of {limit}")]
pub(crate) struct OversizedResponse {
    // The size limit in bytes.
    pub(crate) limit: usize,
}

impl From<OversizedResponse> for ErrorPayload {
    fn from(err: OversizedResponse) -> Self {
        Self {
            code: OVERSIZED_RESPONSE_CODE,
            message: Cow::Borrowed("Response is too big"),
            data: error_data(&err.to_string()),
        }
    }
}

/// A method couldn't be registered.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegisterMethodError {
    /// The name is taken.
    #[error("method `{0}` is already registered")]
    AlreadyRegistered(&'static str),
    /// An alias points to a method that isn't registered.
    #[error("method `{0}` is not registered")]
    MethodNotFound(&'static str),
}

// A message as the JSON string in an error's `data`.
fn error_data(message: &str) -> Option<Box<RawValue>> {
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

    #[test]
    fn oversized_response_names_the_limit() {
        let payload = ErrorPayload::from(OversizedResponse { limit: 64 });
        assert_eq!((payload.code, payload.message.as_ref()), (-32008, "Response is too big"));
        assert_eq!(data(payload), "Exceeded max limit of 64");
    }
}
