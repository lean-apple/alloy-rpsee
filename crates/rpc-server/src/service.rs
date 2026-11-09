// Copyright 2019-2021 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Request dispatch shared by every transport.

use crate::{
    CallContext, Connection, MethodResponse, Methods, Request,
    batch::{Batch, BatchEntry, BatchEntryErr, BatchResponseBuilder},
    error::{BatchError, ErrorCode},
};
use alloy_json_rpc::Id;
use futures_util::{FutureExt, future::Either};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::{
    future::{Future, ready},
    panic::{AssertUnwindSafe, catch_unwind},
};

/// A service that answers requests: the innermost [`RpcService`], or middleware around it.
///
/// Unlike `tower::Service`, requests borrow the body and calls take `&self`, so one service
/// answers a connection's calls concurrently.
pub trait RpcServiceT {
    /// Answers a call.
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a;

    /// Answers a batch.
    ///
    /// [`RpcService`] runs the entries through its own `call`, so middleware that must see each
    /// entry has to handle them here too.
    fn batch<'a>(&self, batch: Batch<'a>) -> impl Future<Output = MethodResponse> + Send + 'a;

    /// Handles a notification. The returned response is never sent.
    fn notification<'a>(
        &self,
        request: Request<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a;
}

/// The innermost service, which calls the registered methods.
#[derive(Debug, Clone)]
pub struct RpcService {
    // The methods calls are dispatched to.
    methods: Methods,
    // The most bytes a response, or a batch's joined responses, may take.
    max_response_size: usize,
    // The connection calls arrive on, which subscriptions need.
    connection: Option<Connection>,
}

impl RpcService {
    /// Creates a service over `methods` whose responses are at most `max_response_size` bytes.
    ///
    /// Subscriptions need a connection, so their calls are answered with `-32603 Internal error`.
    pub fn new(methods: impl Into<Methods>, max_response_size: usize) -> Self {
        Self { methods: methods.into(), max_response_size, connection: None }
    }

    /// Same as [`Self::new`], for calls arriving on `connection`, which can carry subscriptions.
    pub fn with_connection(
        methods: impl Into<Methods>,
        max_response_size: usize,
        connection: Connection,
    ) -> Self {
        Self { methods: methods.into(), max_response_size, connection: Some(connection) }
    }
}

impl RpcServiceT for RpcService {
    fn call<'a>(&self, request: Request<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let id = request.id.unwrap_or(Id::None);
        match self.methods.method(&request.method) {
            Some(method) => {
                let context = CallContext {
                    max_response_size: self.max_response_size,
                    connection: self.connection.as_ref(),
                };
                Either::Left(method.call(id, request.params, &context))
            }
            None => Either::Right(ready(MethodResponse::error(id, ErrorCode::MethodNotFound))),
        }
    }

    fn batch<'a>(&self, batch: Batch<'a>) -> impl Future<Output = MethodResponse> + Send + 'a {
        let service = self.clone();
        async move {
            let mut responses = BatchResponseBuilder::new(service.max_response_size);
            // Set when an entry needs no answer in the array, so a batch of only those sends
            // nothing.
            let mut answered_elsewhere = false;
            // Entries run one after another, so a batch takes one call at a time.
            for entry in batch {
                let response = match entry {
                    Ok(BatchEntry::Call(request)) => call_guarded(&service, request).await,
                    Ok(BatchEntry::Notification(request)) => {
                        answered_elsewhere = true;
                        service.notification(request).await;
                        continue;
                    }
                    Err(err) => err.into(),
                };
                // An accepted subscription already sent its answer.
                if response.is_subscription() {
                    answered_elsewhere = true;
                    continue;
                }
                if let Err(too_big) = responses.append(response) {
                    return too_big;
                }
            }
            if responses.is_empty() && answered_elsewhere {
                MethodResponse::notification()
            } else {
                responses.finish()
            }
        }
    }

    fn notification<'a>(
        &self,
        _request: Request<'a>,
    ) -> impl Future<Output = MethodResponse> + Send + 'a {
        // Notifications aren't run, as in jsonrpsee.
        ready(MethodResponse::notification())
    }
}

/// How batch requests are handled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BatchRequestConfig {
    /// Batches are rejected.
    Disabled,
    /// Batches may have at most this many entries.
    Limit(u32),
    /// Batches may have any number of entries.
    #[default]
    Unlimited,
}

/// Answers `body`, a single message if `is_single` or else a batch, through `rpc_service`.
///
/// The transport picks `is_single` and limits the body size. It sends the response unless it
/// [is a notification](MethodResponse::is_notification) or
/// [a subscription](MethodResponse::is_subscription), which already answered on its connection. A
/// call that panics is answered with `-32603 Internal error`.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{
///     BatchRequestConfig, ErrorPayload, RpcModule, RpcService, handle_rpc_call,
/// };
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let mut module = RpcModule::new(());
/// module.register_method("eth_chainId", |(): (), _| Ok::<_, ErrorPayload>("0x1")).unwrap();
/// let service = RpcService::new(module, 10 * 1024 * 1024);
///
/// let body =
///     br#"[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","method":"x"}]"#;
/// let response = handle_rpc_call(body, false, BatchRequestConfig::Unlimited, &service).await;
/// assert_eq!(response.into_json(), r#"[{"jsonrpc":"2.0","id":1,"result":"0x1"}]"#);
/// # }
/// ```
pub async fn handle_rpc_call<S: RpcServiceT>(
    body: &[u8],
    is_single: bool,
    batch_config: BatchRequestConfig,
    rpc_service: &S,
) -> MethodResponse {
    if is_single {
        // A body that isn't UTF-8 isn't JSON either.
        let Ok(message) = std::str::from_utf8(body) else {
            return MethodResponse::error(Id::None, ErrorCode::ParseError);
        };
        return match parse_entry(message) {
            Ok(BatchEntry::Call(request)) => call_guarded(rpc_service, request).await,
            Ok(BatchEntry::Notification(request)) => rpc_service.notification(request).await,
            Err(err) => err.into(),
        };
    }
    match parse_batch(body, batch_config) {
        Ok(batch) => rpc_service.batch(batch).await,
        Err(err) => MethodResponse::error(Id::None, err),
    }
}

// Splits a batch into its entries. An entry that isn't a valid request stays in the batch, so it
// is answered in its place.
fn parse_batch(body: &[u8], batch_config: BatchRequestConfig) -> Result<Batch<'_>, BatchError> {
    let max_entries = match batch_config {
        BatchRequestConfig::Disabled => return Err(BatchError::NotSupported),
        BatchRequestConfig::Limit(limit) => usize::try_from(limit).unwrap_or(usize::MAX),
        BatchRequestConfig::Unlimited => usize::MAX,
    };
    // Each entry is kept as raw JSON first, since serde can't continue an array past an element
    // that fails to decode.
    let entries = serde_json::from_slice::<Vec<&RawValue>>(body).map_err(|_| BatchError::Parse)?;
    if entries.len() > max_entries {
        return Err(BatchError::TooBig { limit: max_entries });
    }
    Ok(entries.into_iter().map(|entry| parse_entry(entry.get())).collect())
}

// Sorts one message, alone or in a batch, into a call, a notification, or the error it is
// answered with.
fn parse_entry(message: &str) -> Result<BatchEntry<'_>, BatchEntryErr> {
    match serde_json::from_str::<Request<'_>>(message) {
        Ok(request) if request.id.is_none() => Ok(BatchEntry::Notification(request)),
        Ok(request) => Ok(BatchEntry::Call(request)),
        Err(_) => Err(invalid_entry(message)),
    }
}

// `-32700 Parse error` for a message that isn't JSON, otherwise `-32600 Invalid request` to the id
// the message carries, if any.
fn invalid_entry(message: &str) -> BatchEntryErr {
    let Ok(message) = serde_json::from_str::<&RawValue>(message) else {
        return BatchEntryErr::new(Id::None, ErrorCode::ParseError.into());
    };
    BatchEntryErr::new(recover_id(message), ErrorCode::InvalidRequest.into())
}

// The `id` of an object that isn't a valid request, so the client can still match the error, or
// `null` if it has no valid id.
fn recover_id(message: &RawValue) -> Id {
    // serde would also read an array as the struct's fields.
    if !message.get().starts_with('{') {
        return Id::None;
    }
    serde_json::from_str::<WithId>(message.get())
        .ok()
        .and_then(|request| request.id)
        .unwrap_or(Id::None)
}

// The `id` member of an invalid request.
#[derive(Deserialize)]
struct WithId {
    // The id, if the request has one.
    #[serde(default)]
    id: Option<Id>,
}

// Calls `request` on `service`, answering a panic with `-32603 Internal error` so the client still
// gets a response. A synchronous method runs when the call starts, so starting it is guarded too.
async fn call_guarded<S: RpcServiceT>(service: &S, request: Request<'_>) -> MethodResponse {
    let id = request.id.clone().unwrap_or(Id::None);
    let response = match catch_unwind(AssertUnwindSafe(|| service.call(request))) {
        Ok(call) => AssertUnwindSafe(call).catch_unwind().await,
        Err(panic) => Err(panic),
    };
    response.unwrap_or_else(|_| MethodResponse::error(id, ErrorCode::InternalError))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RpcModule;
    use alloy_json_rpc::ErrorPayload;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use test_case::test_case;

    const NOT_FOUND: &str =
        r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}}"#;

    // A service whose `count` method counts its calls and `boom` panics.
    fn service(max_response_size: usize) -> (RpcService, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut module = RpcModule::new(calls.clone());
        module
            .register_method("count", |(): (), calls: &Arc<AtomicUsize>| {
                Ok::<_, ErrorPayload>(calls.fetch_add(1, Ordering::Relaxed))
            })
            .unwrap();
        module
            .register_method("boom", |(): (), _| -> Result<(), ErrorPayload> { panic!("boom") })
            .unwrap();
        module
            .register_async_method("boomAsync", |(): (), _| async {
                panic!("boom");
                #[expect(unreachable_code, reason = "gives the future its output type")]
                Ok::<(), ErrorPayload>(())
            })
            .unwrap();
        (RpcService::new(module, max_response_size), calls)
    }

    async fn answer(body: &str, batch_config: BatchRequestConfig) -> String {
        let is_single = !body.starts_with('[');
        let (service, _) = service(usize::MAX);
        handle_rpc_call(body.as_bytes(), is_single, batch_config, &service).await.into_json()
    }

    #[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"count"}"#, r#"{"jsonrpc":"2.0","id":1,"result":0}"# ; "call")]
    #[test_case(r#"{"jsonrpc":"2.0","id":2,"method":"x"}"#, NOT_FOUND ; "unknown method")]
    #[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"boom"}"#, r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"# ; "panic")]
    #[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"boomAsync"}"#, r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"# ; "async panic")]
    #[test_case(r#"{"jsonrpc":"2.0","method":"count"}"#, "" ; "notification")]
    #[test_case(r#"{"jsonrpc":"2.0","id":1"#, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"# ; "not json")]
    #[test_case(r#"{"jsonrpc":"1.0","id":"a","method":"count"}"#, r#"{"jsonrpc":"2.0","id":"a","error":{"code":-32600,"message":"Invalid request"}}"# ; "invalid request keeps its id")]
    #[test_case(r#"{"jsonrpc":"1.0","id":{},"method":"count"}"#, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid request"}}"# ; "invalid id is null")]
    #[test_case(r#"[1]"#, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid request"}}"# ; "array as single")]
    #[tokio::test]
    async fn single(body: &str, response: &str) {
        let (service, _) = service(usize::MAX);
        let answer =
            handle_rpc_call(body.as_bytes(), true, BatchRequestConfig::Unlimited, &service);
        assert_eq!(answer.await.into_json(), response);
    }

    #[tokio::test]
    async fn non_utf8_body_is_a_parse_error() {
        let (service, _) = service(usize::MAX);
        let response = handle_rpc_call(b"\xff", true, BatchRequestConfig::Unlimited, &service);
        assert_eq!(
            response.await.into_json(),
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
        );
    }

    #[tokio::test]
    async fn notifications_are_not_run() {
        let (service, calls) = service(usize::MAX);
        let body = r#"[{"jsonrpc":"2.0","method":"count"}]"#;
        let response =
            handle_rpc_call(body.as_bytes(), false, BatchRequestConfig::Unlimited, &service);
        assert!(response.await.is_notification());
        let body = r#"{"jsonrpc":"2.0","method":"count"}"#;
        handle_rpc_call(body.as_bytes(), true, BatchRequestConfig::Unlimited, &service).await;
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn batch_answers_each_entry_in_order() {
        let body = concat!(
            r#"[{"jsonrpc":"2.0","id":1,"method":"count"},"#,
            r#"{"jsonrpc":"2.0","method":"count"},"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"x"},"#,
            r#"{"id":3},"#,
            r#"{"jsonrpc":"2.0","id":4,"method":"boom"},"#,
            r#"{"jsonrpc":"2.0","id":5,"method":"count"}]"#,
        );
        assert_eq!(
            answer(body, BatchRequestConfig::Unlimited).await,
            concat!(
                r#"[{"jsonrpc":"2.0","id":1,"result":0},"#,
                r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32601,"message":"Method not found"}},"#,
                r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32600,"message":"Invalid request"}},"#,
                r#"{"jsonrpc":"2.0","id":4,"error":{"code":-32603,"message":"Internal error"}},"#,
                r#"{"jsonrpc":"2.0","id":5,"result":1}]"#,
            )
        );
    }

    #[test_case("[]", BatchRequestConfig::Unlimited, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid request"}}"# ; "empty")]
    #[test_case("[1", BatchRequestConfig::Unlimited, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"# ; "not json")]
    #[test_case("[1]", BatchRequestConfig::Disabled, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"Batched requests are not supported by this server"}}"# ; "disabled")]
    #[test_case("[1,2]", BatchRequestConfig::Limit(1), r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32010,"message":"The batch request was too large","data":"Exceeded max limit of 1"}}"# ; "too many entries")]
    #[test_case("[1]", BatchRequestConfig::Limit(1), r#"[{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid request"}}]"# ; "at the limit")]
    #[tokio::test]
    async fn batch_errors(body: &str, batch_config: BatchRequestConfig, response: &str) {
        assert_eq!(answer(body, batch_config).await, response);
    }

    #[tokio::test]
    async fn batch_response_is_bounded() {
        let entry = r#"{"jsonrpc":"2.0","id":1,"method":"count"}"#;
        let one = r#"[{"jsonrpc":"2.0","id":1,"result":0}]"#;
        let (service, calls) = service(one.len());
        let body = format!("[{entry}]");
        let response =
            handle_rpc_call(body.as_bytes(), false, BatchRequestConfig::Unlimited, &service);
        assert_eq!(response.await.into_json(), one);

        let body = format!("[{entry},{entry},{entry}]");
        let response =
            handle_rpc_call(body.as_bytes(), false, BatchRequestConfig::Unlimited, &service);
        assert_eq!(
            response.await.into_json(),
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32011,"message":"The batch response was too large","data":"Exceeded max limit of 37"}}"#
        );
        // One call in the first batch, then two before the limit stopped the second.
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
}
