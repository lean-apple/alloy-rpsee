//! Errors answered to a call.

use super::ErrorCode;
use alloy_json_rpc::ErrorPayload;
use serde_json::value::{RawValue, to_raw_value};
use std::borrow::Cow;

// Caps the error message echoed back to the caller.
const MAX_ERROR_MESSAGE_LEN: usize = 1024;

/// The params of a call don't match the method's arguments.
///
/// Converts into `-32602` with the reason as its message, as geth words it, for example
/// `invalid argument 0: invalid type: string "x", expected u64`.
#[derive(Debug, thiserror::Error)]
pub enum InvalidParams {
    /// The params aren't a JSON array.
    #[error("non-array args")]
    NonArray,
    /// An argument doesn't decode, or a required one is missing.
    #[error(transparent)]
    Decode(#[from] serde_json::Error),
}

impl From<InvalidParams> for ErrorPayload {
    fn from(err: InvalidParams) -> Self {
        let mut message = err.to_string();
        strip_positions(&mut message);
        if message.len() > MAX_ERROR_MESSAGE_LEN {
            message.truncate(message.floor_char_boundary(MAX_ERROR_MESSAGE_LEN));
            message.push('…');
        }
        Self { message: Cow::Owned(message), ..ErrorCode::InvalidParams.into() }
    }
}

// Removes the ` at line L column C` positions serde_json appends, which point into the params
// rather than the client's request.
fn strip_positions(message: &mut String) {
    while let Some(start) = message.rfind(" at line ")
        && is_position(&message[start..])
    {
        message.truncate(start);
    }
}

// Returns `true` for exactly ` at line L column C`.
fn is_position(suffix: &str) -> bool {
    suffix
        .strip_prefix(" at line ")
        .and_then(|rest| rest.split_once(" column "))
        .is_some_and(|(line, column)| is_number(line) && is_number(column))
}

// Returns `true` for a non-empty run of ASCII digits.
fn is_number(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
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
    use test_case::test_case;

    fn payload(json: &str) -> ErrorPayload {
        InvalidParams::Decode(serde_json::from_str::<u64>(json).unwrap_err()).into()
    }

    #[test]
    fn reason_is_the_message_without_data() {
        let payload = payload(r#""x""#);
        assert_eq!(
            (payload.code, payload.message.as_ref()),
            (-32602, "invalid type: string \"x\", expected u64")
        );
        assert!(payload.data.is_none());
    }

    #[test]
    fn long_message_is_cut() {
        let value = "é".repeat(MAX_ERROR_MESSAGE_LEN);
        let message = format!("invalid type: string \"{value}");
        let cut = &message[..message.floor_char_boundary(MAX_ERROR_MESSAGE_LEN)];
        assert_eq!(payload(&format!(r#""{value}""#)).message, format!("{cut}…"));
    }

    #[test_case("expected u64 at line 1 column 3", "expected u64" ; "one position")]
    #[test_case("invalid argument 0: x at line 1 column 3 at line 1 column 4", "invalid argument 0: x" ; "nested positions")]
    #[test_case(r#"string "at line 1 column 2""#, r#"string "at line 1 column 2""# ; "position inside a value")]
    #[test_case("no position", "no position" ; "none")]
    fn positions_are_dropped(message: &str, expected: &str) {
        let mut message = message.to_owned();
        strip_positions(&mut message);
        assert_eq!(message, expected);
    }
}
