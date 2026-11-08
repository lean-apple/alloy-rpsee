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

//! The method table calls are dispatched to.

use crate::{MethodResponse, Request, error::RegisterMethodError};
use alloy_json_rpc::{ErrorPayload, Id};
use futures_util::future::{BoxFuture, Either};
use rustc_hash::FxHashMap;
use serde::de::IgnoredAny;
use serde_json::value::RawValue;
use std::{collections::hash_map::Entry, fmt, future::Ready, sync::Arc};

/// The future of a method call. A synchronous method's response is ready without an allocation.
pub type ResponseFuture = Either<Ready<MethodResponse>, BoxFuture<'static, MethodResponse>>;

// A method body: takes the call's id, raw params and response size limit.
type Callback = dyn Fn(Id, Option<&RawValue>, usize) -> ResponseFuture + Send + Sync;

/// A table of methods by name.
///
/// Cloning shares the table, and changing a shared table copies it first.
#[derive(Debug, Clone, Default)]
pub struct Methods {
    // The registered methods. The names are the server's own, so callers can't pick colliding
    // keys and a fast hash is safe.
    callbacks: Arc<FxHashMap<&'static str, MethodCallback>>,
}

impl Methods {
    /// Creates an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `callback` as `name`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `name` is taken.
    pub fn verify_and_insert(
        &mut self,
        name: &'static str,
        callback: MethodCallback,
    ) -> Result<&mut MethodCallback, RegisterMethodError> {
        match Arc::make_mut(&mut self.callbacks).entry(name) {
            Entry::Occupied(_) => Err(RegisterMethodError::AlreadyRegistered(name)),
            Entry::Vacant(entry) => Ok(entry.insert(callback)),
        }
    }

    /// Adds every method of `other`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `self` has one of the names, and
    /// then leaves `self` unchanged.
    pub fn merge(&mut self, other: impl Into<Self>) -> Result<(), RegisterMethodError> {
        let other = other.into();
        if let Some(name) = other.callbacks.keys().find(|name| self.callbacks.contains_key(*name)) {
            return Err(RegisterMethodError::AlreadyRegistered(name));
        }
        Arc::make_mut(&mut self.callbacks).extend(Arc::unwrap_or_clone(other.callbacks));
        Ok(())
    }

    /// Returns the method registered as `name`.
    pub fn method(&self, name: &str) -> Option<&MethodCallback> {
        self.callbacks.get(name)
    }

    /// Removes the method registered as `name`.
    pub fn remove_method(&mut self, name: &str) -> Option<MethodCallback> {
        Arc::make_mut(&mut self.callbacks).remove(name)
    }

    /// Returns the registered names, in no particular order.
    pub fn method_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.callbacks.keys().copied()
    }

    /// Answers one JSON-RPC request in process, with no response size limit.
    ///
    /// Returns an empty string for a notification, which gets no answer. Meant for tests.
    pub async fn raw_json_request(&self, request: &str) -> String {
        let Ok(request) = serde_json::from_str::<Request<'_>>(request) else {
            return invalid_request(request).into_json();
        };
        let is_notification = request.id.is_none();
        let id = request.id.unwrap_or(Id::None);
        let response = match self.method(&request.method) {
            Some(method) => method.call(id, request.params, usize::MAX).await,
            None => MethodResponse::error(id, ErrorPayload::method_not_found()),
        };
        if is_notification { String::new() } else { response.into_json() }
    }
}

/// A registered method.
#[derive(Clone)]
pub struct MethodCallback(Arc<Callback>);

impl MethodCallback {
    /// Wraps a method body.
    pub(crate) fn new(
        callback: impl Fn(Id, Option<&RawValue>, usize) -> ResponseFuture + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(callback))
    }

    /// Calls the method with the call's `id` and raw `params`.
    ///
    /// The response is at most `max_response_size` bytes, or `-32008 Response is too big`.
    pub fn call(
        &self,
        id: Id,
        params: Option<&RawValue>,
        max_response_size: usize,
    ) -> ResponseFuture {
        (self.0)(id, params, max_response_size)
    }
}

impl fmt::Debug for MethodCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MethodCallback").finish_non_exhaustive()
    }
}

// `-32700 Parse error` for a body that isn't JSON, otherwise `-32600 Invalid request`.
fn invalid_request(body: &str) -> MethodResponse {
    let err = match serde_json::from_str::<IgnoredAny>(body) {
        Ok(_) => ErrorPayload::invalid_request(),
        Err(_) => ErrorPayload::parse_error(),
    };
    MethodResponse::error(Id::None, err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;
    use test_case::test_case;

    fn ok() -> MethodCallback {
        MethodCallback::new(|id, _, max_response_size| {
            Either::Left(ready(MethodResponse::success(id, "ok", max_response_size)))
        })
    }

    fn methods(names: &[&'static str]) -> Methods {
        let mut methods = Methods::new();
        for name in names {
            methods.verify_and_insert(name, ok()).unwrap();
        }
        methods
    }

    fn names(methods: &Methods) -> Vec<&'static str> {
        let mut names = methods.method_names().collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let mut methods = methods(&["a"]);
        let err = methods.verify_and_insert("a", ok()).unwrap_err();
        assert_eq!(err, RegisterMethodError::AlreadyRegistered("a"));
        assert_eq!(err.to_string(), "method `a` is already registered");
    }

    #[test]
    fn merge_adds_every_method() {
        let mut target = methods(&["a"]);
        target.merge(methods(&["b", "c"])).unwrap();
        assert_eq!(names(&target), ["a", "b", "c"]);
    }

    #[test]
    fn merge_conflict_leaves_self_unchanged() {
        let mut target = methods(&["a"]);
        let err = target.merge(methods(&["b", "a"])).unwrap_err();
        assert_eq!(err, RegisterMethodError::AlreadyRegistered("a"));
        assert_eq!(names(&target), ["a"]);
    }

    #[test]
    fn changing_a_clone_leaves_the_original() {
        let original = methods(&["a"]);
        let mut clone = original.clone();
        assert!(clone.remove_method("a").is_some());
        assert!(clone.method("a").is_none());
        assert!(original.method("a").is_some());
    }

    #[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"a"}"#, r#"{"jsonrpc":"2.0","id":1,"result":"ok"}"# ; "call")]
    #[test_case(r#"{"jsonrpc":"2.0","id":null,"method":"a"}"#, r#"{"jsonrpc":"2.0","id":null,"result":"ok"}"# ; "null id call")]
    #[test_case(r#"{"jsonrpc":"2.0","method":"a"}"#, "" ; "notification")]
    #[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"b"}"#, r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"# ; "unknown method")]
    #[test_case(r#"{"jsonrpc":"2.0","id":1"#, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"# ; "not json")]
    #[test_case(r#"{"id":1,"method":"a"}"#, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}"# ; "not a request")]
    #[tokio::test]
    async fn raw_json_request(request: &str, response: &str) {
        assert_eq!(methods(&["a"]).raw_json_request(request).await, response);
    }
}
