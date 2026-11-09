//! End-to-end tests of the code `#[rpc]` generates.

use alloy_rpc_macros::rpc;
use alloy_rpc_server::{
    BatchRequestConfig, Connection, ConnectionId, ErrorPayload, IdProvider, MethodResponse,
    RpcResult, RpcService, SubscriptionId, SubscriptionMessage, SubscriptionResult,
    handle_rpc_call,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::mpsc;

#[rpc(server, namespace = "test")]
trait TestApi {
    #[method(name = "none")]
    fn none(&self) -> RpcResult<u64>;

    #[method(name = "add", aliases = ["test_plus"])]
    fn add(&self, a: u64, b: Option<u64>, c: Option<u64>) -> RpcResult<u64>;

    #[method(name = "echo")]
    async fn echo(&self, value: String) -> RpcResult<String>;

    #[method(name = "blocking", blocking)]
    fn blocking(&self) -> RpcResult<&'static str>;

    #[method(name = "fail")]
    fn fail(&self) -> RpcResult<()>;
}

struct Test;

impl TestApiServer for Test {
    fn none(&self) -> RpcResult<u64> {
        Ok(0)
    }

    fn add(&self, a: u64, b: Option<u64>, c: Option<u64>) -> RpcResult<u64> {
        Ok(a.saturating_add(b.unwrap_or(0)).saturating_add(c.unwrap_or(0)))
    }

    async fn echo(&self, value: String) -> RpcResult<String> {
        Ok(value)
    }

    fn blocking(&self) -> RpcResult<&'static str> {
        Ok("done")
    }

    fn fail(&self) -> RpcResult<()> {
        Err(ErrorPayload::internal_error_message("failed".into()))
    }
}

#[rpc(server, namespace = "generic")]
trait GenericApi<T, R> {
    #[method(name = "convert")]
    fn convert(&self, value: T, extra: Option<u64>) -> RpcResult<R>;
}

struct Generic<T, R>(PhantomData<fn(T) -> R>);

impl<T, R> GenericApiServer<T, R> for Generic<T, R>
where
    T: Into<R> + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + Send + Sync + 'static,
{
    fn convert(&self, value: T, _extra: Option<u64>) -> RpcResult<R> {
        Ok(value.into())
    }
}

#[rpc(server, namespace = "sub")]
trait SubApi<T> {
    #[subscription(name = "subscribe" => "subscription", unsubscribe = "unsubscribe", item = T)]
    async fn subscribe(&self, start: u64, step: Option<u64>) -> SubscriptionResult;

    #[subscription(name = "peerEvents", unsubscribe = "peerEvents_unsubscribe", item = String)]
    async fn peer_events(&self) -> SubscriptionResult;
}

struct Sub;

impl SubApiServer<u64> for Sub {
    async fn subscribe(
        &self,
        pending: alloy_rpc_server::PendingSubscriptionSink,
        start: u64,
        step: Option<u64>,
    ) -> SubscriptionResult {
        let sink = pending.accept().await?;
        for item in [start, start.saturating_add(step.unwrap_or(1))] {
            sink.send(SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &item)?)
                .await?;
        }
        Ok(())
    }

    async fn peer_events(
        &self,
        pending: alloy_rpc_server::PendingSubscriptionSink,
    ) -> SubscriptionResult {
        let sink = pending.accept().await?;
        sink.send(SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &"added")?)
            .await?;
        sink.closed().await;
        Ok(())
    }
}

// Ids every subscription `7`, so tests know it.
#[derive(Debug)]
struct Seven;

impl IdProvider for Seven {
    fn next_id(&self) -> SubscriptionId {
        SubscriptionId::Num(7)
    }
}

// A connection serving `Sub`, and the messages it sends.
fn connection() -> (RpcService, mpsc::Receiver<String>) {
    let (sender, received) = mpsc::channel(8);
    let connection = Connection::new(ConnectionId(0), sender, 8, Arc::new(Seven));
    (RpcService::with_connection(Sub.into_rpc(), usize::MAX, connection), received)
}

async fn send(service: &RpcService, body: &str) -> MethodResponse {
    handle_rpc_call(body.as_bytes(), true, BatchRequestConfig::Unlimited, service).await
}

async fn call(method: &str, params: &str) -> String {
    let request = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params}}}"#);
    Test.into_rpc().raw_json_request(&request).await
}

fn result(json: &str) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":1,"result":{json}}}"#)
}

#[tokio::test]
async fn method_without_params() {
    assert_eq!(call("test_none", "[]").await, result("0"));
    assert_eq!(call("test_none", r#"["extra"]"#).await, result("0"));
}

#[tokio::test]
async fn trailing_options_may_be_left_out() {
    assert_eq!(call("test_add", "[1]").await, result("1"));
    assert_eq!(call("test_add", "[1, 2]").await, result("3"));
    assert_eq!(call("test_add", "[1, null, 3]").await, result("4"));
    assert_eq!(call("test_add", "[1, 2, 3, 4]").await, result("6"));
}

#[tokio::test]
async fn required_param_is_required() {
    assert_eq!(
        call("test_add", "[]").await,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"missing value for required argument 0"}}"#
    );
}

#[tokio::test]
async fn alias_and_async_and_blocking() {
    assert_eq!(call("test_plus", "[2, 2]").await, result("4"));
    assert_eq!(call("test_echo", r#"["hi"]"#).await, result(r#""hi""#));
    assert_eq!(call("test_blocking", "[]").await, result(r#""done""#));
}

#[tokio::test]
async fn method_error_is_answered() {
    assert_eq!(
        call("test_fail", "[]").await,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"failed"}}"#
    );
}

#[tokio::test]
async fn generic_trait() {
    let module = Generic::<u32, u64>(PhantomData).into_rpc();
    let request = r#"{"jsonrpc":"2.0","id":1,"method":"generic_convert","params":[7]}"#;
    assert_eq!(module.raw_json_request(request).await, result("7"));
}

#[tokio::test]
async fn subscription_with_a_notification_name() {
    let (service, mut received) = connection();
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"sub_subscribe","params":[5]}"#;
    assert!(send(&service, body).await.is_subscription());
    assert_eq!(received.recv().await.unwrap(), r#"{"jsonrpc":"2.0","id":1,"result":7}"#);
    for item in [5, 6] {
        assert_eq!(
            received.recv().await.unwrap(),
            format!(
                r#"{{"jsonrpc":"2.0","method":"sub_subscription","params":{{"subscription":7,"result":{item}}}}}"#
            )
        );
    }
}

#[tokio::test]
async fn subscription_named_like_its_notifications() {
    let (service, mut received) = connection();
    send(&service, r#"{"jsonrpc":"2.0","id":1,"method":"sub_peerEvents"}"#).await;
    received.recv().await.unwrap();
    assert_eq!(
        received.recv().await.unwrap(),
        r#"{"jsonrpc":"2.0","method":"sub_peerEvents","params":{"subscription":7,"result":"added"}}"#
    );
    let unsubscribe =
        r#"{"jsonrpc":"2.0","id":2,"method":"sub_peerEvents_unsubscribe","params":[7]}"#;
    assert_eq!(
        send(&service, unsubscribe).await.into_json(),
        r#"{"jsonrpc":"2.0","id":2,"result":true}"#
    );
}

#[test]
fn macro_errors() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
