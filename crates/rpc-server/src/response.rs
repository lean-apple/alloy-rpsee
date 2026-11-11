//! Response serialization.
//!
//! A result is serialized once, by reference, into the buffer that is sent. Serialization stops as
//! soon as the response passes its size limit, so an oversized result never takes more memory than
//! the limit.

use crate::{error::ErrorCode, request::Version, writer::BoundedWriter};
use alloy_json_rpc::{ErrorPayload, Id, Response, ResponsePayload};
use serde::Serialize;
use serde_json::value::RawValue;

// Sent if an error response can't be serialized, which a well-formed `ErrorPayload` never hits.
const INTERNAL_ERROR_JSON: &str =
    r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"Internal error"}}"#;

/// A serialized JSON-RPC response.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{ErrorCode, Id, MethodResponse};
///
/// let response = MethodResponse::success(Id::Number(1), "0x1", usize::MAX);
/// assert_eq!(response.into_json(), r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
///
/// let response = MethodResponse::success(Id::Number(1), &"x".repeat(100), 64);
/// assert!(response.is_error());
///
/// let response = MethodResponse::error(Id::Number(1), ErrorCode::MethodNotFound);
/// assert_eq!(
///     response.into_json(),
///     r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#
/// );
///
/// assert_eq!(MethodResponse::notification().into_message(), None);
/// ```
#[derive(Debug)]
pub struct MethodResponse {
    // The response as sent on the wire.
    json: String,
    // What `json` answers.
    kind: Kind,
}

impl MethodResponse {
    /// Serializes a successful response to `id`, at most `max_response_size` bytes long.
    ///
    /// A larger response becomes `-32008 Response is too big`, and a result that fails to serialize
    /// becomes `-32603 Internal error`.
    pub fn success<T: Serialize + ?Sized>(id: Id, result: &T, max_response_size: usize) -> Self {
        let response = Response { id, payload: ResponsePayload::<&T, ()>::Success(result) };
        match bounded_json(&response, max_response_size) {
            Ok(json) => Self { json, kind: Kind::Success },
            Err(err) => Self::error(response.id, err),
        }
    }

    /// Serializes an error response to `id`.
    pub fn error(id: Id, err: impl Into<ErrorPayload>) -> Self {
        let err = err.into();
        let json = serde_json::to_string(&ErrorResponse::new(&id, &err))
            .unwrap_or_else(|_| INTERNAL_ERROR_JSON.to_owned());
        Self { json, kind: Kind::Error }
    }

    // Serializes an error a method returned to `id`, at most `max_response_size` bytes long. A
    // larger response becomes `-32008 Response is too big`, like an oversized result.
    pub(crate) fn bounded_error(
        id: Id,
        err: impl Into<ErrorPayload>,
        max_response_size: usize,
    ) -> Self {
        let err = err.into();
        match bounded_json(&ErrorResponse::new(&id, &err), max_response_size) {
            Ok(json) => Self { json, kind: Kind::Error },
            Err(err) => Self::error(id, err),
        }
    }

    /// The answer to a notification, which sends nothing.
    pub const fn notification() -> Self {
        Self { json: String::new(), kind: Kind::Notification }
    }

    // The answer to a subscribe call that its subscription already sent.
    pub(crate) const fn subscription() -> Self {
        Self { json: String::new(), kind: Kind::Subscription }
    }

    // A batch's joined responses.
    pub(crate) const fn from_batch(json: String) -> Self {
        Self { json, kind: Kind::Batch }
    }

    /// Returns `true` unless the response is an error.
    pub const fn is_success(&self) -> bool {
        !self.is_error()
    }

    /// Returns `true` if the response is an error.
    pub const fn is_error(&self) -> bool {
        matches!(self.kind, Kind::Error)
    }

    /// Returns `true` for the answer to a notification, which the transport doesn't send.
    pub const fn is_notification(&self) -> bool {
        matches!(self.kind, Kind::Notification)
    }

    /// Returns `true` for an accepted subscribe call, whose answer the subscription already sent
    /// on its connection, so the transport doesn't send it.
    pub const fn is_subscription(&self) -> bool {
        matches!(self.kind, Kind::Subscription)
    }

    /// Takes the serialized JSON.
    pub fn into_json(self) -> String {
        self.json
    }

    /// Takes the JSON a connection sends back, or `None` for the answer to a notification or an
    /// accepted subscribe call, which send nothing.
    pub fn into_message(self) -> Option<String> {
        match self.kind {
            Kind::Notification | Kind::Subscription => None,
            Kind::Success | Kind::Error | Kind::Batch => Some(self.json),
        }
    }
}

// What a response answers.
#[derive(Debug)]
enum Kind {
    // A call, with a `result`.
    Success,
    // A call, with an `error`.
    Error,
    // A notification, with nothing to send.
    Notification,
    // A batch, with an array of responses.
    Batch,
    // An accepted subscribe call, already answered on its connection.
    Subscription,
}

// An error response that leaves out an empty `data`.
#[derive(Serialize)]
struct ErrorResponse<'a> {
    // The protocol version.
    jsonrpc: Version,
    // The id of the call being answered.
    id: &'a Id,
    // The error itself.
    error: ErrorBody<'a>,
}

impl<'a> ErrorResponse<'a> {
    // The response carrying `err` to `id`.
    fn new(id: &'a Id, err: &'a ErrorPayload) -> Self {
        // `ErrorPayload` writes an empty `data` as `null`, while clients expect it left out.
        let error = ErrorBody { code: err.code, message: &err.message, data: err.data.as_deref() };
        Self { jsonrpc: Version::V2, id, error }
    }
}

// The `error` member of an `ErrorResponse`.
#[derive(Serialize)]
struct ErrorBody<'a> {
    // The JSON-RPC error code.
    code: i64,
    // A short description of the error.
    message: &'a str,
    // Extra detail, left out when empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a RawValue>,
}

// Serializes `response` in at most `max_len` bytes, or returns the error to answer instead.
fn bounded_json(response: &impl Serialize, max_len: usize) -> Result<String, ErrorPayload> {
    let mut writer = BoundedWriter::new(max_len);
    match serde_json::to_writer(&mut writer, response) {
        // serde_json only writes UTF-8.
        Ok(()) => {
            String::from_utf8(writer.into_bytes()).map_err(|_| ErrorCode::InternalError.into())
        }
        // The writer's only IO error is the size limit.
        Err(err) if err.is_io() => Err(ErrorCode::OversizedResponse.exceeded(max_len)),
        Err(err) => {
            tracing::error!(%err, "failed to serialize an RPC response");
            Err(ErrorCode::InternalError.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use test_case::test_case;

    #[test_case(Id::Number(1), r#"{"jsonrpc":"2.0","id":1,"result":"a"}"# ; "number id")]
    #[test_case(Id::String("x".into()), r#"{"jsonrpc":"2.0","id":"x","result":"a"}"# ; "string id")]
    #[test_case(Id::None, r#"{"jsonrpc":"2.0","id":null,"result":"a"}"# ; "null id")]
    fn success(id: Id, json: &str) {
        let response = MethodResponse::success(id, "a", usize::MAX);
        assert!(response.is_success() && !response.is_error());
        assert_eq!(response.into_json(), json);
    }

    #[test]
    fn result_of_exactly_the_limit_fits() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":"abc"}"#;
        assert_eq!(MethodResponse::success(Id::Number(1), "abc", json.len()).into_json(), json);
    }

    #[test]
    fn oversized_result_becomes_an_error() {
        let response = MethodResponse::success(Id::Number(1), &"x".repeat(100), 64);
        assert!(response.is_error());
        assert_eq!(
            response.into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32008,"message":"Response is too big","data":"Exceeded max limit of 64"}}"#
        );
    }

    #[test]
    fn unserializable_result_is_an_internal_error() {
        let result = BTreeMap::from([(vec![1u8], 1)]);
        assert_eq!(
            MethodResponse::success(Id::Number(1), &result, usize::MAX).into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"#
        );
    }

    #[test]
    fn error_with_data() {
        let data = RawValue::from_string(r#""0xdead""#.to_owned()).unwrap();
        let err = ErrorPayload { code: 3, message: "execution reverted".into(), data: Some(data) };
        assert_eq!(
            MethodResponse::error(Id::Number(1), err).into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted","data":"0xdead"}}"#
        );
    }

    #[test]
    fn only_answers_are_messages() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":"a"}"#;
        let success = MethodResponse::success(Id::Number(1), "a", usize::MAX);
        assert_eq!(success.into_message().as_deref(), Some(json));
        let error = MethodResponse::error(Id::Number(1), ErrorCode::MethodNotFound);
        assert!(error.into_message().is_some());
        let batch = MethodResponse::from_batch(format!("[{json}]"));
        assert!(batch.into_message().is_some());
        assert_eq!(MethodResponse::notification().into_message(), None);
        assert_eq!(MethodResponse::subscription().into_message(), None);
    }
}
