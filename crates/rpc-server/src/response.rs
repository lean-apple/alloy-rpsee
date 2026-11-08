//! Response serialization.
//!
//! A result is serialized once, by reference, into the buffer that is sent. Serialization stops as
//! soon as the response passes its size limit, so an oversized result never takes more memory than
//! the limit.

use crate::{error::OversizedResponse, request::Version, writer::BoundedWriter};
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
/// use alloy_rpc_server::{ErrorPayload, Id, MethodResponse};
///
/// let response = MethodResponse::success(Id::Number(1), "0x1", usize::MAX);
/// assert_eq!(response.into_json(), r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
///
/// let response = MethodResponse::success(Id::Number(1), &"x".repeat(100), 64);
/// assert!(response.is_error());
///
/// let response = MethodResponse::error(Id::Number(1), ErrorPayload::method_not_found());
/// assert_eq!(
///     response.into_json(),
///     r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"#
/// );
/// ```
#[derive(Debug)]
pub struct MethodResponse {
    // The response as sent on the wire.
    json: String,
    // Whether `json` holds a result or an error.
    kind: Kind,
}

impl MethodResponse {
    /// Serializes a successful response to `id`, at most `max_len` bytes long.
    ///
    /// A larger response becomes `-32008 Response is too big`, and a result that fails to serialize
    /// becomes `-32603 Internal error`.
    pub fn success<T: Serialize + ?Sized>(id: Id, result: &T, max_len: usize) -> Self {
        let mut writer = BoundedWriter::new(max_len);
        let response = Response { id, payload: ResponsePayload::<&T, ()>::Success(result) };
        match serde_json::to_writer(&mut writer, &response) {
            Ok(()) => match String::from_utf8(writer.into_bytes()) {
                Ok(json) => Self { json, kind: Kind::Success },
                // serde_json only writes UTF-8.
                Err(_) => Self::error(response.id, ErrorPayload::internal_error()),
            },
            // The writer's only IO error is the size limit.
            Err(err) if err.is_io() => {
                Self::error(response.id, OversizedResponse { limit: max_len })
            }
            Err(err) => {
                tracing::error!(%err, "failed to serialize an RPC result");
                Self::error(response.id, ErrorPayload::internal_error())
            }
        }
    }

    /// Serializes an error response to `id`.
    pub fn error(id: Id, err: impl Into<ErrorPayload>) -> Self {
        let err = err.into();
        // Alloy's `ErrorPayload` writes `"data": null`, which Reth's clients don't receive today.
        let response = ErrorResponse {
            jsonrpc: Version::V2,
            id: &id,
            error: ErrorBody { code: err.code, message: &err.message, data: err.data.as_deref() },
        };
        let json =
            serde_json::to_string(&response).unwrap_or_else(|_| INTERNAL_ERROR_JSON.to_owned());
        Self { json, kind: Kind::Error }
    }

    /// Returns `true` unless the response is an error.
    pub const fn is_success(&self) -> bool {
        matches!(self.kind, Kind::Success)
    }

    /// Returns `true` if the response is an error.
    pub const fn is_error(&self) -> bool {
        matches!(self.kind, Kind::Error)
    }

    /// Takes the serialized JSON.
    pub fn into_json(self) -> String {
        self.json
    }
}

// Whether a response carries a result or an error.
#[derive(Debug)]
enum Kind {
    // Carries a `result`.
    Success,
    // Carries an `error`.
    Error,
}

// An error response that omits an empty `data`, unlike Alloy's.
#[derive(Serialize)]
struct ErrorResponse<'a> {
    // The protocol version.
    jsonrpc: Version,
    // The id of the call being answered.
    id: &'a Id,
    // The error itself.
    error: ErrorBody<'a>,
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
}
