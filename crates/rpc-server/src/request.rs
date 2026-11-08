//! Inbound request parsing.

use alloy_json_rpc::Id;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use std::borrow::Cow;

/// A JSON-RPC 2.0 request object borrowed from the request body.
///
/// A missing `id` is a notification, which gets no answer. `"id": null` is a call, which does:
///
/// ```
/// use alloy_rpc_server::{Id, Request};
///
/// let notification = serde_json::from_str::<Request<'_>>(r#"{"jsonrpc":"2.0","method":"m"}"#)?;
/// assert_eq!(notification.id, None);
///
/// let call = serde_json::from_str::<Request<'_>>(r#"{"jsonrpc":"2.0","method":"m","id":null}"#)?;
/// assert_eq!(call.id, Some(Id::None));
/// # Ok::<_, serde_json::Error>(())
/// ```
///
/// [`alloy_json_rpc::Request`] can't be reused: it is a client type that reads both cases as
/// [`Id::None`], requires `params`, and rejects unknown members.
#[derive(Debug, Deserialize)]
pub struct Request<'a> {
    /// Must be `"2.0"`.
    #[serde(rename = "jsonrpc")]
    _version: Version,
    /// `None` when the `id` member is absent, `Some(Id::None)` when it is `null`.
    #[serde(default, deserialize_with = "deserialize_present_id")]
    pub id: Option<Id>,
    /// Borrowed from the body unless the JSON string contains escapes.
    #[serde(borrow)]
    pub method: Cow<'a, str>,
    /// `None` when the `params` member is absent or `null`.
    #[serde(borrow, default)]
    pub params: Option<&'a RawValue>,
}

// The only JSON-RPC version, `"2.0"`.
#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum Version {
    // JSON-RPC 2.0.
    #[serde(rename = "2.0")]
    V2,
}

/// Called only when `id` is present, so `null` becomes `Some(Id::None)`. A missing `id` takes
/// `#[serde(default)]` instead.
fn deserialize_present_id<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Id>, D::Error> {
    Id::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    type ClientRequest = alloy_json_rpc::Request<Box<RawValue>>;

    fn parse(body: &[u8]) -> serde_json::Result<Request<'_>> {
        serde_json::from_slice(body)
    }

    #[test]
    fn missing_id_is_a_notification() {
        let request = parse(br#"{"jsonrpc":"2.0","method":"m"}"#).unwrap();
        assert_eq!(request.id, None);
    }

    #[test]
    fn null_id_is_a_call() {
        let request = parse(br#"{"jsonrpc":"2.0","method":"m","id":null}"#).unwrap();
        assert_eq!(request.id, Some(Id::None));
    }

    #[test]
    fn client_request_merges_missing_and_null_id() {
        let missing = br#"{"jsonrpc":"2.0","method":"m","params":[]}"#;
        let null = br#"{"jsonrpc":"2.0","method":"m","params":[],"id":null}"#;
        assert_eq!(serde_json::from_slice::<ClientRequest>(missing).unwrap().meta.id, Id::None);
        assert_eq!(serde_json::from_slice::<ClientRequest>(null).unwrap().meta.id, Id::None);
    }

    #[test]
    fn client_request_requires_params() {
        let body = br#"{"jsonrpc":"2.0","method":"m","id":1}"#;
        let err = serde_json::from_slice::<ClientRequest>(body).unwrap_err();
        assert_eq!(err.to_string(), "missing field `params` at line 1 column 37");
        assert!(parse(body).unwrap().params.is_none());
    }

    #[test]
    fn client_request_rejects_unknown_members() {
        let body = br#"{"jsonrpc":"2.0","method":"m","params":[],"id":1,"extra":true}"#;
        let err = serde_json::from_slice::<ClientRequest>(body).unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown field `extra`, expected one of `id`, `params`, `method`, `jsonrpc` at line 1 column 56"
        );
        assert!(parse(body).is_ok());
    }

    #[test]
    fn borrows_method_and_params() {
        let body = br#"{"jsonrpc":"2.0","id":7,"method":"eth_x","params":[1, "a"]}"#;
        let request = parse(body).unwrap();
        assert!(matches!(request.method, Cow::Borrowed("eth_x")));
        assert_eq!(request.params.unwrap().get(), r#"[1, "a"]"#);
        assert_eq!(request.id, Some(Id::Number(7)));
    }

    #[test]
    fn escaped_method_is_owned() {
        let request = parse(br#"{"jsonrpc":"2.0","id":1,"method":"\"m\""}"#).unwrap();
        assert!(matches!(request.method, Cow::Owned(_)));
        assert_eq!(request.method, "\"m\"");
    }

    #[test]
    fn null_params_are_absent() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"m","params":null}"#;
        assert!(parse(body).unwrap().params.is_none());
    }

    #[test_case(br#"{"jsonrpc":"1.0","method":"m","id":1}"#, "unknown variant `1.0`, expected `2.0` at line 1 column 16" ; "wrong version")]
    #[test_case(br#"{"method":"m","id":1}"#, "missing field `jsonrpc` at line 1 column 21" ; "missing version")]
    #[test_case(br#"{"jsonrpc":"2.0","method":"m","id":{}}"#, "invalid type: map, expected a string, a number, or null at line 1 column 37" ; "object id")]
    #[test_case(br#"{"jsonrpc":"2.0","method":"m","id":true}"#, "invalid type: boolean `true`, expected a string, a number, or null at line 1 column 39" ; "bool id")]
    #[test_case(br#"{"jsonrpc":"2.0","method":"m","id":-1}"#, "invalid type: integer `-1`, expected a string, a number, or null at line 1 column 37" ; "negative id")]
    #[test_case(br#"{"jsonrpc":"2.0","method":"m","id":1.5}"#, "invalid type: floating point `1.5`, expected a string, a number, or null at line 1 column 38" ; "float id")]
    fn rejects(body: &[u8], error: &str) {
        assert_eq!(parse(body).unwrap_err().to_string(), error);
    }
}
