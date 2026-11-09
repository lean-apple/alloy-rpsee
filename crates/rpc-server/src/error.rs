//! Server errors.

use crate::SubscriptionMessage;
use alloy_json_rpc::ErrorPayload;
use serde_json::value::{RawValue, to_raw_value};
use std::borrow::Cow;

// Caps the error `data` echoed back to the caller.
const MAX_ERROR_DATA_LEN: usize = 1024;

// A response exceeded the size limit.
const OVERSIZED_RESPONSE_CODE: i64 = -32008;

// The server doesn't accept batches.
const BATCHES_NOT_SUPPORTED_CODE: i64 = -32005;

// A batch has more entries than allowed.
const TOO_BIG_BATCH_REQUEST_CODE: i64 = -32010;

// A batch's responses exceeded the size limit.
const TOO_BIG_BATCH_RESPONSE_CODE: i64 = -32011;

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

// A batch that can't be run. Each converts into the error answered with a `null` id.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BatchError {
    // Batches are disabled.
    #[error("Batched requests are not supported by this server")]
    NotSupported,
    // The body isn't a JSON array.
    #[error("Parse error")]
    Parse,
    // The batch has more entries than allowed.
    #[error("The batch request was too large")]
    TooBig {
        // The most entries allowed.
        limit: usize,
    },
}

impl From<BatchError> for ErrorPayload {
    fn from(err: BatchError) -> Self {
        match err {
            BatchError::NotSupported => Self {
                code: BATCHES_NOT_SUPPORTED_CODE,
                message: Cow::Owned(err.to_string()),
                data: None,
            },
            BatchError::Parse => Self::parse_error(),
            BatchError::TooBig { limit } => Self {
                code: TOO_BIG_BATCH_REQUEST_CODE,
                message: Cow::Owned(err.to_string()),
                data: error_data(&format!("Exceeded max limit of {limit}")),
            },
        }
    }
}

// A batch's responses exceeded their size limit. Converts into `-32011` naming the limit.
#[derive(Debug, thiserror::Error)]
#[error("Exceeded max limit of {limit}")]
pub(crate) struct OversizedBatchResponse {
    // The size limit in bytes.
    pub(crate) limit: usize,
}

impl From<OversizedBatchResponse> for ErrorPayload {
    fn from(err: OversizedBatchResponse) -> Self {
        Self {
            code: TOO_BIG_BATCH_RESPONSE_CODE,
            message: Cow::Borrowed("The batch response was too large"),
            data: error_data(&err.to_string()),
        }
    }
}

// Too many subscriptions on one connection.
const TOO_MANY_SUBSCRIPTIONS_CODE: i64 = -32006;

// A connection reached its subscription limit. Converts into `-32006` naming the limit.
#[derive(Debug, thiserror::Error)]
#[error("Exceeded max limit of {limit}")]
pub(crate) struct TooManySubscriptions {
    // The most subscriptions active at once.
    pub(crate) limit: u32,
}

impl From<TooManySubscriptions> for ErrorPayload {
    fn from(err: TooManySubscriptions) -> Self {
        Self {
            code: TOO_MANY_SUBSCRIPTIONS_CODE,
            message: Cow::Borrowed("Too many subscriptions on the connection"),
            data: error_data(&err.to_string()),
        }
    }
}

/// A subscription couldn't be accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the subscription couldn't be accepted")]
pub struct PendingSubscriptionAcceptError;

/// A notification wasn't sent because the subscription ended. Holds the notification.
#[derive(Debug, thiserror::Error)]
#[error("the subscription ended")]
pub struct DisconnectError(pub SubscriptionMessage);

/// A notification wasn't sent within the timeout. Holds the notification.
#[derive(Debug, thiserror::Error)]
pub enum SendTimeoutError {
    /// The queue stayed full until the timeout.
    #[error("the subscription queue stayed full")]
    Timeout(SubscriptionMessage),
    /// The subscription ended.
    #[error("the subscription ended")]
    Closed(SubscriptionMessage),
}

/// A notification wasn't sent right away. Holds the notification.
#[derive(Debug, thiserror::Error)]
pub enum TrySendError {
    /// The queue is full.
    #[error("the subscription queue is full")]
    Full(SubscriptionMessage),
    /// The subscription ended.
    #[error("the subscription ended")]
    Closed(SubscriptionMessage),
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
