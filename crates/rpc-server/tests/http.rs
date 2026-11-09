//! End-to-end tests of the HTTP server.

use alloy_rpc_server::{
    ErrorPayload, RpcModule, RpcServiceT,
    server::{
        BoxError, HttpBody, HttpRequest, HttpResponse, ServerBuilder, ServerConfig, ServerHandle,
    },
};
use futures_util::future::BoxFuture;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode, header};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::{
    cell::Cell,
    future::Future,
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use test_case::test_case;
use tokio::io::AsyncReadExt;
use tower::{Layer, Service};

// A module with `eth_chainId`, a `big` method answering 100 bytes, and a `slow` one.
fn module() -> RpcModule<()> {
    let mut module = RpcModule::new(());
    module.register_method("eth_chainId", |(): (), _| Ok::<_, ErrorPayload>("0x1")).unwrap();
    module.register_method("big", |(): (), _| Ok::<_, ErrorPayload>("x".repeat(100))).unwrap();
    module
        .register_async_method("slow", |(): (), _| async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, ErrorPayload>(())
        })
        .unwrap();
    module
}

async fn start(config: ServerConfig) -> (SocketAddr, ServerHandle) {
    let server = ServerBuilder::new().set_config(config).build("127.0.0.1:0").await.unwrap();
    (server.local_addr().unwrap(), server.start(module()))
}

// Sends `body` as `method` with `content_type`, and returns the status and body.
async fn send(
    addr: SocketAddr,
    method: Method,
    content_type: &str,
    body: &str,
) -> (StatusCode, String) {
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<hyper::body::Bytes>>();
    let request = Request::builder()
        .method(method)
        .uri(format!("http://{addr}"))
        .header(header::CONTENT_TYPE, content_type)
        .body(Full::from(body.to_owned()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

async fn post(addr: SocketAddr, body: &str) -> (StatusCode, String) {
    send(addr, Method::POST, "application/json", body).await
}

#[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#, r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"# ; "call")]
#[test_case(r#"  [{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}]"#, r#"[{"jsonrpc":"2.0","id":1,"result":"0x1"}]"# ; "batch after whitespace")]
#[test_case(r#"{"jsonrpc":"2.0","method":"eth_chainId"}"#, "" ; "notification")]
#[test_case(r#"{"jsonrpc":"2.0","id":1,"method":"big"}"#, r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32008,"message":"Response is too big","data":"Exceeded max limit of 64"}}"# ; "response too big")]
#[tokio::test]
async fn answers_json_rpc(body: &str, response: &str) {
    let (addr, _handle) = start(ServerConfig::builder().max_response_body_size(64).build()).await;
    assert_eq!(post(addr, body).await, (StatusCode::OK, response.to_owned()));
}

#[test_case(Method::GET, "application/json", "{}", StatusCode::METHOD_NOT_ALLOWED, "Used HTTP Method is not allowed. POST is required\n" ; "get")]
#[test_case(Method::POST, "text/plain", "{}", StatusCode::UNSUPPORTED_MEDIA_TYPE, "Supplied content type is not allowed. Content-Type: application/json is required\n" ; "content type")]
#[test_case(Method::POST, "application/json", "nope", StatusCode::BAD_REQUEST, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"# ; "not json")]
#[test_case(Method::POST, "application/json", "", StatusCode::BAD_REQUEST, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"# ; "empty")]
#[test_case(Method::POST, "application/json", &"1".repeat(65), StatusCode::PAYLOAD_TOO_LARGE, r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32007,"message":"Request is too big","data":"Exceeded max limit of 64"}}"# ; "request too big")]
#[tokio::test]
async fn rejects(
    method: Method,
    content_type: &str,
    body: &str,
    status: StatusCode,
    response: &str,
) {
    let (addr, _handle) = start(ServerConfig::builder().max_request_body_size(64).build()).await;
    assert_eq!(send(addr, method, content_type, body).await, (status, response.to_owned()));
}

#[tokio::test]
async fn requests_past_the_limit_are_refused() {
    let (addr, _handle) = start(ServerConfig::builder().max_connections(1).build()).await;
    let slow = tokio::spawn(post(addr, r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let busy = post(addr, r#"{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}"#).await;
    assert_eq!(
        busy,
        (StatusCode::TOO_MANY_REQUESTS, "Too many connections. Please try again later.".to_owned())
    );
    assert_eq!(slow.await.unwrap().0, StatusCode::OK);
}

#[tokio::test]
async fn stop_finishes_open_requests() {
    let (addr, handle) = start(ServerConfig::default()).await;
    let slow = tokio::spawn(post(addr, r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#));
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.stop().unwrap();
    assert!(handle.stop().is_err());
    assert_eq!(
        slow.await.unwrap(),
        (StatusCode::OK, r#"{"jsonrpc":"2.0","id":1,"result":null}"#.to_owned())
    );
    tokio::time::timeout(Duration::from_secs(5), handle.stopped()).await.unwrap();
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

// An RPC middleware counting the calls it sees.
#[derive(Clone)]
struct Count(Arc<AtomicUsize>);

impl<S> Layer<S> for Count {
    type Service = Counted<S>;

    fn layer(&self, inner: S) -> Counted<S> {
        Counted { inner, calls: self.0.clone() }
    }
}

struct Counted<S> {
    inner: S,
    calls: Arc<AtomicUsize>,
}

impl<S: RpcServiceT> RpcServiceT for Counted<S> {
    fn call<'a>(
        &self,
        request: alloy_rpc_server::Request<'a>,
    ) -> impl Future<Output = alloy_rpc_server::MethodResponse> + Send + 'a {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.call(request)
    }

    fn batch<'a>(
        &self,
        batch: alloy_rpc_server::Batch<'a>,
    ) -> impl Future<Output = alloy_rpc_server::MethodResponse> + Send + 'a {
        self.inner.batch(batch)
    }

    fn notification<'a>(
        &self,
        request: alloy_rpc_server::Request<'a>,
    ) -> impl Future<Output = alloy_rpc_server::MethodResponse> + Send + 'a {
        self.inner.notification(request)
    }
}

#[tokio::test]
async fn middleware_wraps_requests_and_calls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = ServerBuilder::new()
        .set_http_middleware(tower::ServiceBuilder::new().map_response(
            |mut response: hyper::Response<HttpBody>| {
                response.headers_mut().insert("x-test", header::HeaderValue::from_static("1"));
                response
            },
        ))
        .set_rpc_middleware(tower::ServiceBuilder::new().option_layer(Some(Count(calls.clone()))))
        .build("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let _handle = server.start(module());
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<hyper::body::Bytes>>();
    let request = Request::post(format!("http://{addr}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.headers()["x-test"], "1");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

// An RPC middleware that is `Send` but not `Sync`.
#[derive(Clone)]
struct NotSync(PhantomData<Cell<()>>);

impl<S> Layer<S> for NotSync {
    type Service = S;

    fn layer(&self, inner: S) -> S {
        inner
    }
}

#[tokio::test]
async fn rpc_middleware_need_not_be_sync() {
    let server = ServerBuilder::new()
        .set_rpc_middleware(tower::ServiceBuilder::new().layer(NotSync(PhantomData)))
        .build("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let _handle = server.start(module());
    let (status, body) = post(addr, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
}

// An HTTP middleware like an auth layer: it answers requests without a token itself, and fails with
// the inner service's `BoxError`.
#[derive(Clone)]
struct RequireToken;

impl<S> Layer<S> for RequireToken {
    type Service = TokenChecked<S>;

    fn layer(&self, inner: S) -> TokenChecked<S> {
        TokenChecked(inner)
    }
}

#[derive(Clone)]
struct TokenChecked<S>(S);

impl<S> Service<HttpRequest> for TokenChecked<S>
where
    S: Service<HttpRequest, Response = HttpResponse, Error = BoxError>,
    S::Future: Send + 'static,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<HttpResponse, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        if request.headers().contains_key("x-token") {
            return Box::pin(self.0.call(request));
        }
        let mut response = HttpResponse::new(HttpBody::from("missing token"));
        *response.status_mut() = StatusCode::UNAUTHORIZED;
        Box::pin(async { Ok(response) })
    }
}

#[tokio::test]
async fn fallible_http_middleware_wraps_the_server() {
    let server = ServerBuilder::new()
        .set_http_middleware(tower::ServiceBuilder::new().layer(RequireToken))
        .build("127.0.0.1:0")
        .await
        .unwrap();
    let addr = server.local_addr().unwrap();
    let _handle = server.start(module());
    let call = r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#;
    assert_eq!(post(addr, call).await, (StatusCode::UNAUTHORIZED, "missing token".to_owned()));

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<hyper::body::Bytes>>();
    let request = Request::post(format!("http://{addr}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-token", "1")
        .body(Full::from(call))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
}

#[tokio::test(start_paused = true)]
async fn silent_connection_is_closed() {
    let (addr, _handle) = start(ServerConfig::default()).await;
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    // Skip past the server's timeout on the paused clock, then wait for the close in real time.
    tokio::time::sleep(Duration::from_secs(31)).await;
    tokio::time::resume();
    let read = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut [0; 1])).await;
    assert_eq!(read.expect("the connection stayed open").unwrap(), 0);
}
