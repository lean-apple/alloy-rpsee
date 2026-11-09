//! End-to-end tests of the code `#[rpc]` generates.

use alloy_rpc_macros::rpc;
use alloy_rpc_server::{ErrorPayload, RpcResult};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

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
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params","data":"invalid length 0, expected tuple struct __TestApiAddParams with 3 elements at line 1 column 2"}}"#
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

#[test]
fn macro_errors() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
