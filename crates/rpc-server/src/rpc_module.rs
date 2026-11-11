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

//! Typed method registration.

use crate::{
    MethodResponse, Methods, PendingSubscriptionSink, SubscriptionId, SubscriptionResult,
    error::{ErrorCode, RegisterMethodError},
    methods::MethodCallback,
    parse_params,
    subscription::SinkInner,
};
use alloy_json_rpc::{ErrorPayload, Id};
use futures_util::future::Either;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    future::{Future, ready},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Methods that share a context, such as one RPC namespace.
///
/// A method takes its params as a type that decodes from positional params, see
/// [`parse_params`]. Params that don't decode are answered with `-32602`, naming the argument,
/// without running the method. The module derefs to its [`Methods`].
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{ErrorPayload, RpcModule};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let mut module = RpcModule::new("0x1");
/// module.register_method("eth_chainId", |(): (), id| Ok::<_, ErrorPayload>(*id)).unwrap();
/// module
///     .register_async_method("echo", |(s,): (String,), _| async move { Ok::<_, ErrorPayload>(s) })
///     .unwrap();
///
/// let response = module.raw_json_request(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#);
/// assert_eq!(response.await, r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
///
/// let response =
///     module.raw_json_request(r#"{"jsonrpc":"2.0","id":2,"method":"echo","params":["hi"]}"#);
/// assert_eq!(response.await, r#"{"jsonrpc":"2.0","id":2,"result":"hi"}"#);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RpcModule<Context> {
    // Shared by every method of the module.
    ctx: Arc<Context>,
    // The registered methods.
    methods: Methods,
}

impl<Context> RpcModule<Context> {
    /// Creates an empty module whose methods share `ctx`.
    pub fn new(ctx: Context) -> Self {
        Self { ctx: Arc::new(ctx), methods: Methods::new() }
    }

    /// Keeps the methods and drops the context type, so modules can be stored together.
    pub fn remove_context(self) -> RpcModule<()> {
        RpcModule { ctx: Arc::new(()), methods: self.methods }
    }
}

impl<Context: Send + Sync + 'static> RpcModule<Context> {
    /// Registers a method that answers without awaiting.
    ///
    /// It runs on the caller's task, so it must not block.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `method_name` is taken.
    pub fn register_method<P, R, E, F>(
        &mut self,
        method_name: &'static str,
        callback: F,
    ) -> Result<&mut MethodCallback, RegisterMethodError>
    where
        P: DeserializeOwned,
        R: Serialize,
        E: Into<ErrorPayload>,
        F: Fn(P, &Context) -> Result<R, E> + Send + Sync + 'static,
    {
        let ctx = self.ctx.clone();
        let method = MethodCallback::new(move |id, params, context| {
            let response = match parse_params::<P>(params) {
                Ok(params) => respond(id, callback(params, &ctx), context.max_response_size),
                Err(err) => MethodResponse::error(id, err),
            };
            Either::Left(ready(response))
        });
        self.methods.verify_and_insert(method_name, method)
    }

    /// Registers a method that returns a future.
    ///
    /// The params are decoded before the future is created, so it owns typed values rather than
    /// the request body.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `method_name` is taken.
    pub fn register_async_method<P, R, E, F, Fut>(
        &mut self,
        method_name: &'static str,
        callback: F,
    ) -> Result<&mut MethodCallback, RegisterMethodError>
    where
        P: DeserializeOwned,
        R: Serialize,
        E: Into<ErrorPayload>,
        F: Fn(P, Arc<Context>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, E>> + Send + 'static,
    {
        let ctx = self.ctx.clone();
        let method =
            MethodCallback::new(move |id, params, context| match parse_params::<P>(params) {
                Ok(params) => {
                    let call = callback(params, ctx.clone());
                    let max_response_size = context.max_response_size;
                    Either::Right(Box::pin(
                        async move { respond(id, call.await, max_response_size) },
                    ))
                }
                Err(err) => Either::Left(ready(MethodResponse::error(id, err))),
            });
        self.methods.verify_and_insert(method_name, method)
    }

    /// Registers a method that may block, run on tokio's blocking thread pool along with the
    /// serialization of its result.
    ///
    /// A method that panics is answered with `-32603 Internal error`. Its calls must run within a
    /// tokio runtime.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `method_name` is taken.
    pub fn register_blocking_method<P, R, E, F>(
        &mut self,
        method_name: &'static str,
        callback: F,
    ) -> Result<&mut MethodCallback, RegisterMethodError>
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize,
        E: Into<ErrorPayload>,
        F: Fn(P, Arc<Context>) -> Result<R, E> + Send + Sync + 'static,
    {
        let ctx = self.ctx.clone();
        let callback = Arc::new(callback);
        let method = MethodCallback::new(move |id, params, context| {
            let params = match parse_params::<P>(params) {
                Ok(params) => params,
                Err(err) => return Either::Left(ready(MethodResponse::error(id, err))),
            };
            let (ctx, callback, error_id) = (ctx.clone(), callback.clone(), id.clone());
            let max_response_size = context.max_response_size;
            let call = tokio::task::spawn_blocking(move || {
                respond(id, callback(params, ctx), max_response_size)
            });
            Either::Right(Box::pin(async move {
                call.await.unwrap_or_else(|err| {
                    // The task also fails when the runtime shuts down, which isn't a fault.
                    if err.is_panic() {
                        tracing::error!(%err, method = method_name, "blocking RPC method panicked");
                    } else {
                        tracing::debug!(%err, method = method_name, "blocking RPC method cancelled");
                    }
                    MethodResponse::error(error_id, ErrorCode::InternalError)
                })
            }))
        });
        self.methods.verify_and_insert(method_name, method)
    }

    /// Registers a subscription, which `subscribe` starts and `unsubscribe` ends, sending its
    /// notifications as `notification`.
    ///
    /// `callback` runs on its own task and answers the call with the [`PendingSubscriptionSink`].
    /// An error it returns once accepted is sent to the client as a last notification.
    /// Subscriptions need a connection, so over HTTP the call is answered with `-32603 Internal
    /// error`, and past the connection's limit with `-32006`. `unsubscribe` answers `true` if it
    /// ended an active subscription of the connection.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::AlreadyRegistered`] if `subscribe` or `unsubscribe` is taken,
    /// or [`RegisterMethodError::SubscriptionNameConflict`] if they are the same name. Nothing is
    /// registered then.
    pub fn register_subscription<P, F, Fut>(
        &mut self,
        subscribe: &'static str,
        notification: &'static str,
        unsubscribe: &'static str,
        callback: F,
    ) -> Result<&mut MethodCallback, RegisterMethodError>
    where
        P: DeserializeOwned,
        F: Fn(P, PendingSubscriptionSink, Arc<Context>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = SubscriptionResult> + Send + 'static,
    {
        if subscribe == unsubscribe {
            return Err(RegisterMethodError::SubscriptionNameConflict(subscribe));
        }
        // Both are checked first, so a taken name leaves neither registered.
        if let Some(taken) =
            [subscribe, unsubscribe].into_iter().find(|name| self.method(name).is_some())
        {
            return Err(RegisterMethodError::AlreadyRegistered(taken));
        }
        let unsubscribe_method = MethodCallback::new(move |id, params, context| {
            let Some(connection) = context.connection else {
                return Either::Left(ready(no_connection(id, unsubscribe)));
            };
            // An id that doesn't decode matches no subscription.
            let ended = parse_params::<(SubscriptionId,)>(params)
                .is_ok_and(|(subscription,)| connection.unsubscribe(subscribe, subscription));
            Either::Left(ready(MethodResponse::success(id, &ended, context.max_response_size)))
        });
        self.methods.verify_and_insert(unsubscribe, unsubscribe_method)?;

        let ctx = self.ctx.clone();
        let subscribe_method = MethodCallback::new(move |id, params, context| {
            let Some(connection) = context.connection else {
                return Either::Left(ready(no_connection(id, subscribe)));
            };
            let params = match parse_params::<P>(params) {
                Ok(params) => params,
                Err(err) => return Either::Left(ready(MethodResponse::error(id, err))),
            };
            let Some(permit) = connection.try_acquire() else {
                let err = ErrorCode::TooManySubscriptions.exceeded(connection.max_subscriptions());
                return Either::Left(ready(MethodResponse::error(id, err)));
            };
            let subscription_id = connection.next_id();
            let (answer, answered) = oneshot::channel();
            let accepted = Arc::new(AtomicBool::new(false));
            let pending = PendingSubscriptionSink {
                id: id.clone(),
                sink: SinkInner {
                    subscribe,
                    method: notification,
                    subscription_id: subscription_id.clone(),
                    connection: connection.clone(),
                    unsubscribed: CancellationToken::new(),
                    max_response_size: context.max_response_size,
                    _permit: permit,
                },
                answer,
                accepted: accepted.clone(),
            };
            let subscription = callback(params, pending, ctx.clone());
            let connection = connection.clone();
            tokio::spawn(async move {
                if let Err(err) = subscription.await
                    && accepted.load(Ordering::Acquire)
                {
                    connection.send_error(notification, &subscription_id, err).await;
                }
            });
            // A subscription that drops its pending sink, or panics, never answers.
            Either::Right(Box::pin(async move {
                answered
                    .await
                    .unwrap_or_else(|_| MethodResponse::error(id, ErrorCode::InternalError))
            }))
        });
        self.methods.verify_and_insert(subscribe, subscribe_method)
    }

    /// Registers `alias` as another name for the method `existing`.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterMethodError::MethodNotFound`] if `existing` isn't registered, or
    /// [`RegisterMethodError::AlreadyRegistered`] if `alias` is taken.
    pub fn register_alias(
        &mut self,
        alias: &'static str,
        existing: &'static str,
    ) -> Result<(), RegisterMethodError> {
        let method =
            self.method(existing).cloned().ok_or(RegisterMethodError::MethodNotFound(existing))?;
        self.methods.verify_and_insert(alias, method)?;
        Ok(())
    }
}

impl<Context> Deref for RpcModule<Context> {
    type Target = Methods;

    fn deref(&self) -> &Methods {
        &self.methods
    }
}

impl<Context> DerefMut for RpcModule<Context> {
    fn deref_mut(&mut self) -> &mut Methods {
        &mut self.methods
    }
}

impl<Context> From<RpcModule<Context>> for Methods {
    fn from(module: RpcModule<Context>) -> Self {
        module.methods
    }
}

// The answer to a subscription call over a transport without connections.
fn no_connection(id: Id, method: &str) -> MethodResponse {
    // Clients choose the transport, so this is theirs to fix.
    tracing::debug!(method, "subscriptions need a connection, such as a WebSocket");
    MethodResponse::error(id, ErrorCode::InternalError)
}

// The response to a method's result.
fn respond<R: Serialize, E: Into<ErrorPayload>>(
    id: Id,
    result: Result<R, E>,
    max_response_size: usize,
) -> MethodResponse {
    match result {
        Ok(result) => MethodResponse::success(id, &result, max_response_size),
        Err(err) => MethodResponse::bounded_error(id, err, max_response_size),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CallContext;
    use serde_json::value::RawValue;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const INVALID_PARAMS: &str = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid argument 0: invalid type: string \"x\", expected u64"}}"#;

    fn module() -> RpcModule<AtomicUsize> {
        let mut module = RpcModule::new(AtomicUsize::new(0));
        module
            .register_method("double", |(n,): (u64,), calls: &AtomicUsize| {
                calls.fetch_add(1, Ordering::Relaxed);
                n.checked_mul(2)
                    .ok_or_else(|| ErrorPayload::internal_error_message("overflow".into()))
            })
            .unwrap();
        module
            .register_async_method("triple", |(n,): (u64,), calls: Arc<AtomicUsize>| async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok::<_, ErrorPayload>(n * 3)
            })
            .unwrap();
        module
    }

    fn call(method: &str, params: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params}}}"#)
    }

    #[tokio::test]
    async fn methods_get_decoded_params_and_the_context() {
        let module = module();
        assert_eq!(
            module.raw_json_request(&call("double", "[2]")).await,
            r#"{"jsonrpc":"2.0","id":1,"result":4}"#
        );
        assert_eq!(
            module.raw_json_request(&call("triple", "[2]")).await,
            r#"{"jsonrpc":"2.0","id":1,"result":6}"#
        );
        assert_eq!(module.ctx.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn invalid_params_skip_the_method() {
        let module = module();
        for method in ["double", "triple"] {
            assert_eq!(module.raw_json_request(&call(method, r#"["x"]"#)).await, INVALID_PARAMS);
        }
        assert_eq!(module.ctx.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn method_error_is_answered() {
        assert_eq!(
            module().raw_json_request(&call("double", &format!("[{}]", u64::MAX))).await,
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"overflow"}}"#
        );
    }

    #[tokio::test]
    async fn response_is_bounded() {
        let raw = RawValue::from_string("[2]".to_owned()).unwrap();
        let response = module().method("double").unwrap().call(
            Id::Number(1),
            Some(&raw),
            &CallContext::new(16),
        );
        assert_eq!(
            response.await.into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32008,"message":"Response is too big","data":"Exceeded max limit of 16"}}"#
        );
    }

    #[tokio::test]
    async fn error_response_is_bounded() {
        let mut module = RpcModule::new(());
        module
            .register_method("revert", |(): (), _| -> Result<(), ErrorPayload> {
                let data = RawValue::from_string(format!("\"0x{}\"", "00".repeat(2048))).unwrap();
                Err(ErrorPayload {
                    code: 3,
                    message: "execution reverted".into(),
                    data: Some(data),
                })
            })
            .unwrap();
        let response =
            module.method("revert").unwrap().call(Id::Number(1), None, &CallContext::new(256));
        assert_eq!(
            response.await.into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32008,"message":"Response is too big","data":"Exceeded max limit of 256"}}"#
        );
    }

    #[tokio::test]
    async fn blocking_method_runs_off_the_runtime() {
        let mut module = RpcModule::new(AtomicUsize::new(7));
        module
            .register_blocking_method("load", |(): (), value: Arc<AtomicUsize>| {
                Ok::<_, ErrorPayload>(value.load(Ordering::Relaxed))
            })
            .unwrap();
        module
            .register_blocking_method("boom", |(): (), _| -> Result<(), ErrorPayload> {
                panic!("boom")
            })
            .unwrap();
        assert_eq!(
            module.raw_json_request(&call("load", "[]")).await,
            r#"{"jsonrpc":"2.0","id":1,"result":7}"#
        );
        assert_eq!(
            module.raw_json_request(&call("boom", "[]")).await,
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"#
        );
    }

    #[tokio::test]
    async fn alias_calls_the_same_method() {
        let mut module = module();
        module.register_alias("twice", "double").unwrap();
        assert_eq!(
            module.raw_json_request(&call("twice", "[2]")).await,
            r#"{"jsonrpc":"2.0","id":1,"result":4}"#
        );
    }

    #[test]
    fn alias_needs_a_free_name_and_an_existing_method() {
        let mut module = module();
        assert_eq!(
            module.register_alias("twice", "missing"),
            Err(RegisterMethodError::MethodNotFound("missing"))
        );
        assert_eq!(
            module.register_alias("triple", "double"),
            Err(RegisterMethodError::AlreadyRegistered("triple"))
        );
    }

    #[test]
    fn subscription_registers_both_names_or_neither() {
        let mut module = module();
        let register = |module: &mut RpcModule<_>, subscribe, unsubscribe| {
            module
                .register_subscription(subscribe, "n", unsubscribe, |(): (), _, _| async { Ok(()) })
                .map(drop)
        };
        assert_eq!(
            register(&mut module, "sub", "sub"),
            Err(RegisterMethodError::SubscriptionNameConflict("sub"))
        );
        assert_eq!(
            register(&mut module, "sub", "double"),
            Err(RegisterMethodError::AlreadyRegistered("double"))
        );
        assert!(module.method("sub").is_none());
    }

    #[test]
    fn remove_context_keeps_the_methods() {
        let mut names = module().remove_context().method_names().collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, ["double", "triple"]);
    }
}
