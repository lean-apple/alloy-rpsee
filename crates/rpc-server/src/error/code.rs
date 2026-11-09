//! The error codes the server answers with, from the JSON-RPC spec and the server's own limits.

use super::call::error_data;
use alloy_json_rpc::ErrorPayload;
use std::{borrow::Cow, fmt};

/// An error the server answers with, with its code and message.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{
///     ErrorPayload,
///     error::{ErrorCode, INVALID_PARAMS_CODE},
/// };
///
/// let err = ErrorPayload::from(ErrorCode::InvalidParams);
/// assert_eq!((err.code, err.message.as_ref()), (INVALID_PARAMS_CODE, "Invalid params"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// The request isn't valid JSON.
    ParseError,
    /// The JSON isn't a valid request.
    InvalidRequest,
    /// No method has the requested name.
    MethodNotFound,
    /// The params don't match the method's arguments.
    InvalidParams,
    /// The server failed to answer.
    InternalError,
    /// The server doesn't accept batches.
    BatchesNotSupported,
    /// The connection reached its subscription limit.
    TooManySubscriptions,
    /// The request exceeds the size limit.
    OversizedRequest,
    /// The response exceeds the size limit.
    OversizedResponse,
    /// The server is at capacity.
    ServerIsBusy,
    /// The batch has more entries than allowed.
    TooBigBatchRequest,
    /// The batch's responses exceed the size limit.
    TooBigBatchResponse,
}

impl ErrorCode {
    /// Returns the code sent to the client.
    pub const fn code(self) -> i64 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::InternalError => -32603,
            Self::BatchesNotSupported => -32005,
            Self::TooManySubscriptions => -32006,
            Self::OversizedRequest => -32007,
            Self::OversizedResponse => -32008,
            Self::ServerIsBusy => -32009,
            Self::TooBigBatchRequest => -32010,
            Self::TooBigBatchResponse => -32011,
        }
    }

    /// Returns the message sent to the client.
    pub const fn message(self) -> &'static str {
        match self {
            Self::ParseError => "Parse error",
            Self::InvalidRequest => "Invalid request",
            Self::MethodNotFound => "Method not found",
            Self::InvalidParams => "Invalid params",
            Self::InternalError => "Internal error",
            Self::BatchesNotSupported => "Batched requests are not supported by this server",
            Self::TooManySubscriptions => "Too many subscriptions on the connection",
            Self::OversizedRequest => "Request is too big",
            Self::OversizedResponse => "Response is too big",
            Self::ServerIsBusy => "Server is busy, try again later",
            Self::TooBigBatchRequest => "The batch request was too large",
            Self::TooBigBatchResponse => "The batch response was too large",
        }
    }

    // The error for a request past `limit`, which names the limit in its `data`.
    pub(crate) fn exceeded(self, limit: impl fmt::Display) -> ErrorPayload {
        ErrorPayload { data: error_data(&format!("Exceeded max limit of {limit}")), ..self.into() }
    }
}

impl From<ErrorCode> for ErrorPayload {
    fn from(code: ErrorCode) -> Self {
        Self { code: code.code(), message: Cow::Borrowed(code.message()), data: None }
    }
}

/// `-32700`, see [`ErrorCode::ParseError`].
pub const PARSE_ERROR_CODE: i64 = ErrorCode::ParseError.code();

/// `-32600`, see [`ErrorCode::InvalidRequest`].
pub const INVALID_REQUEST_CODE: i64 = ErrorCode::InvalidRequest.code();

/// `-32601`, see [`ErrorCode::MethodNotFound`].
pub const METHOD_NOT_FOUND_CODE: i64 = ErrorCode::MethodNotFound.code();

/// `-32602`, see [`ErrorCode::InvalidParams`].
pub const INVALID_PARAMS_CODE: i64 = ErrorCode::InvalidParams.code();

/// `-32603`, see [`ErrorCode::InternalError`].
pub const INTERNAL_ERROR_CODE: i64 = ErrorCode::InternalError.code();

/// `-32005`, see [`ErrorCode::BatchesNotSupported`].
pub const BATCHES_NOT_SUPPORTED_CODE: i64 = ErrorCode::BatchesNotSupported.code();

/// `-32006`, see [`ErrorCode::TooManySubscriptions`].
pub const TOO_MANY_SUBSCRIPTIONS_CODE: i64 = ErrorCode::TooManySubscriptions.code();

/// `-32007`, see [`ErrorCode::OversizedRequest`].
pub const OVERSIZED_REQUEST_CODE: i64 = ErrorCode::OversizedRequest.code();

/// `-32008`, see [`ErrorCode::OversizedResponse`].
pub const OVERSIZED_RESPONSE_CODE: i64 = ErrorCode::OversizedResponse.code();

/// `-32009`, see [`ErrorCode::ServerIsBusy`].
pub const SERVER_IS_BUSY_CODE: i64 = ErrorCode::ServerIsBusy.code();

/// `-32010`, see [`ErrorCode::TooBigBatchRequest`].
pub const TOO_BIG_BATCH_REQUEST_CODE: i64 = ErrorCode::TooBigBatchRequest.code();

/// `-32011`, see [`ErrorCode::TooBigBatchResponse`].
pub const TOO_BIG_BATCH_RESPONSE_CODE: i64 = ErrorCode::TooBigBatchResponse.code();

/// The message of [`ErrorCode::ParseError`].
pub const PARSE_ERROR_MSG: &str = ErrorCode::ParseError.message();

/// The message of [`ErrorCode::InvalidRequest`].
pub const INVALID_REQUEST_MSG: &str = ErrorCode::InvalidRequest.message();

/// The message of [`ErrorCode::MethodNotFound`].
pub const METHOD_NOT_FOUND_MSG: &str = ErrorCode::MethodNotFound.message();

/// The message of [`ErrorCode::InvalidParams`].
pub const INVALID_PARAMS_MSG: &str = ErrorCode::InvalidParams.message();

/// The message of [`ErrorCode::InternalError`].
pub const INTERNAL_ERROR_MSG: &str = ErrorCode::InternalError.message();

/// The message of [`ErrorCode::BatchesNotSupported`].
pub const BATCHES_NOT_SUPPORTED_MSG: &str = ErrorCode::BatchesNotSupported.message();

/// The message of [`ErrorCode::TooManySubscriptions`].
pub const TOO_MANY_SUBSCRIPTIONS_MSG: &str = ErrorCode::TooManySubscriptions.message();

/// The message of [`ErrorCode::OversizedRequest`].
pub const OVERSIZED_REQUEST_MSG: &str = ErrorCode::OversizedRequest.message();

/// The message of [`ErrorCode::OversizedResponse`].
pub const OVERSIZED_RESPONSE_MSG: &str = ErrorCode::OversizedResponse.message();

/// The message of [`ErrorCode::ServerIsBusy`].
pub const SERVER_IS_BUSY_MSG: &str = ErrorCode::ServerIsBusy.message();

/// The message of [`ErrorCode::TooBigBatchRequest`].
pub const TOO_BIG_BATCH_REQUEST_MSG: &str = ErrorCode::TooBigBatchRequest.message();

/// The message of [`ErrorCode::TooBigBatchResponse`].
pub const TOO_BIG_BATCH_RESPONSE_MSG: &str = ErrorCode::TooBigBatchResponse.message();

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(ErrorCode::ParseError, -32700, "Parse error" ; "parse error")]
    #[test_case(ErrorCode::InvalidRequest, -32600, "Invalid request" ; "invalid request")]
    #[test_case(ErrorCode::MethodNotFound, -32601, "Method not found" ; "method not found")]
    #[test_case(ErrorCode::InvalidParams, -32602, "Invalid params" ; "invalid params")]
    #[test_case(ErrorCode::InternalError, -32603, "Internal error" ; "internal error")]
    #[test_case(ErrorCode::BatchesNotSupported, -32005, "Batched requests are not supported by this server" ; "batches not supported")]
    #[test_case(ErrorCode::TooManySubscriptions, -32006, "Too many subscriptions on the connection" ; "too many subscriptions")]
    #[test_case(ErrorCode::OversizedRequest, -32007, "Request is too big" ; "oversized request")]
    #[test_case(ErrorCode::OversizedResponse, -32008, "Response is too big" ; "oversized response")]
    #[test_case(ErrorCode::ServerIsBusy, -32009, "Server is busy, try again later" ; "server is busy")]
    #[test_case(ErrorCode::TooBigBatchRequest, -32010, "The batch request was too large" ; "too big batch request")]
    #[test_case(ErrorCode::TooBigBatchResponse, -32011, "The batch response was too large" ; "too big batch response")]
    fn code_and_message(code: ErrorCode, number: i64, message: &str) {
        let err = ErrorPayload::from(code);
        assert_eq!((err.code, err.message.as_ref()), (number, message));
        assert!(err.data.is_none());
    }

    #[test]
    fn exceeded_names_the_limit() {
        let err = ErrorCode::OversizedResponse.exceeded(64);
        assert_eq!(err.data.unwrap().get(), r#""Exceeded max limit of 64""#);
    }
}
