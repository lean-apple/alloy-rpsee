//! End-to-end tests of the WebSocket server.

use alloy_rpc_server::{
    ErrorPayload, RpcModule, SubscriptionMessage,
    server::{ServerBuilder, ServerConfig, ServerHandle},
};
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Request, StatusCode, header};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as WsError, Message, http::StatusCode as WsStatus},
};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

// A module with `eth_chainId`, a `slow` method, a `ticks` subscription sending 1 then 2, and a
// `flood` subscription sending large items until it ends, reporting its end on `ended`.
fn module(ended: mpsc::Sender<()>) -> RpcModule<mpsc::Sender<()>> {
    let mut module = RpcModule::new(ended);
    module.register_method("eth_chainId", |(): (), _| Ok::<_, ErrorPayload>("0x1")).unwrap();
    module
        .register_async_method("slow", |(): (), _| async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok::<_, ErrorPayload>("done")
        })
        .unwrap();
    module
        .register_subscription("ticks", "tick", "unticks", |(): (), pending, _| async move {
            let sink = pending.accept().await?;
            for item in [1, 2] {
                sink.send(SubscriptionMessage::new(
                    sink.method_name(),
                    sink.subscription_id(),
                    &item,
                )?)
                .await?;
            }
            sink.closed().await;
            Ok(())
        })
        .unwrap();
    module
        .register_subscription("flood", "flooded", "unflood", |(): (), pending, ended| async move {
            let sink = pending.accept().await?;
            let item = "x".repeat(1 << 20);
            while sink
                .send(SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &item)?)
                .await
                .is_ok()
            {}
            let _ = ended.send(()).await;
            Ok(())
        })
        .unwrap();
    module
}

async fn start(config: ServerConfig) -> (SocketAddr, ServerHandle, mpsc::Receiver<()>) {
    let (ended, ends) = mpsc::channel(1);
    let server = ServerBuilder::new().set_config(config).build("127.0.0.1:0").await.unwrap();
    (server.local_addr().unwrap(), server.start(module(ended)), ends)
}

async fn connect(addr: SocketAddr) -> Ws {
    connect_async(format!("ws://{addr}")).await.unwrap().0
}

async fn call(ws: &mut Ws, body: &str) -> String {
    ws.send(Message::text(body)).await.unwrap();
    recv(ws).await
}

async fn recv(ws: &mut Ws) -> String {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap() {
            Some(Ok(Message::Text(text))) => return text.to_string(),
            Some(Ok(_)) => continue,
            other => panic!("expected a message, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn answers_calls_batches_and_skips_notifications() {
    let (addr, _handle, _) = start(ServerConfig::default()).await;
    let mut ws = connect(addr).await;
    ws.send(Message::text(r#"{"jsonrpc":"2.0","method":"eth_chainId"}"#)).await.unwrap();
    assert_eq!(
        call(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await,
        r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#
    );
    assert_eq!(
        call(&mut ws, r#" [{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}]"#).await,
        r#"[{"jsonrpc":"2.0","id":2,"result":"0x1"}]"#
    );
    assert_eq!(
        call(&mut ws, "nope").await,
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#
    );
}

#[tokio::test]
async fn subscription_sends_its_id_then_notifications() {
    let (addr, _handle, _) = start(ServerConfig::default()).await;
    let mut ws = connect(addr).await;
    let response = call(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"ticks"}"#).await;
    let id = response
        .strip_prefix(r#"{"jsonrpc":"2.0","id":1,"result":"#)
        .unwrap()
        .trim_end_matches('}')
        .to_owned();
    for item in [1, 2] {
        assert_eq!(
            recv(&mut ws).await,
            format!(
                r#"{{"jsonrpc":"2.0","method":"tick","params":{{"subscription":{id},"result":{item}}}}}"#
            )
        );
    }
    let unsubscribe = format!(r#"{{"jsonrpc":"2.0","id":2,"method":"unticks","params":[{id}]}}"#);
    assert_eq!(call(&mut ws, &unsubscribe).await, r#"{"jsonrpc":"2.0","id":2,"result":true}"#);
}

#[tokio::test]
async fn http_and_ws_share_the_port() {
    let (addr, _handle, _) = start(ServerConfig::default()).await;
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<hyper::body::Bytes>>();
    let request = Request::post(format!("http://{addr}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from(r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#))
        .unwrap();
    let response = client.request(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#);
    let mut ws = connect(addr).await;
    assert_eq!(
        call(&mut ws, r#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}"#).await,
        r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#
    );
}

#[tokio::test]
async fn http_only_refuses_upgrades() {
    let (addr, _handle, _) = start(ServerConfig::builder().http_only().build()).await;
    match connect_async(format!("ws://{addr}")).await {
        Err(WsError::Http(response)) => assert_eq!(response.status(), WsStatus::FORBIDDEN),
        other => panic!("expected 403, got {other:?}"),
    }
}

#[tokio::test]
async fn ws_only_refuses_plain_http() {
    let (addr, _handle, _) = start(ServerConfig::builder().ws_only().build()).await;
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<hyper::body::Bytes>>();
    let request = Request::post(format!("http://{addr}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::from("{}"))
        .unwrap();
    assert_eq!(client.request(request).await.unwrap().status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn oversized_message_is_answered_then_closed() {
    let (addr, _handle, _) = start(ServerConfig::builder().max_request_body_size(64).build()).await;
    let mut ws = connect(addr).await;
    ws.send(Message::text("x".repeat(65))).await.unwrap();
    assert_eq!(
        recv(&mut ws).await,
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32007,"message":"Request is too big","data":"Exceeded max limit of 64"}}"#
    );
}

#[tokio::test]
async fn calls_wait_for_a_free_slot() {
    let (addr, _handle, _) =
        start(ServerConfig::builder().set_message_buffer_capacity(1).build()).await;
    let mut ws = connect(addr).await;
    ws.send(Message::text(r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#)).await.unwrap();
    ws.send(Message::text(r#"{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}"#)).await.unwrap();
    // With one slot, the second call only starts once the first is answered.
    assert_eq!(recv(&mut ws).await, r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#);
    assert_eq!(recv(&mut ws).await, r#"{"jsonrpc":"2.0","id":2,"result":"0x1"}"#);
}

#[tokio::test]
async fn client_that_stops_reading_is_disconnected() {
    let config = ServerConfig::builder()
        .max_response_body_size(4 << 20)
        .set_message_buffer_capacity(4)
        .set_ws_write_timeout(Duration::from_millis(100))
        .build();
    let (addr, _handle, mut ends) = start(config).await;
    let mut ws = connect(addr).await;
    ws.send(Message::text(r#"{"jsonrpc":"2.0","id":1,"method":"flood"}"#)).await.unwrap();
    // The client never reads, so a write times out and the subscription ends.
    tokio::time::timeout(Duration::from_secs(10), ends.recv()).await.unwrap().unwrap();
}

#[tokio::test]
async fn stop_answers_running_calls_then_closes() {
    let (addr, handle, _) = start(ServerConfig::default()).await;
    let mut ws = connect(addr).await;
    ws.send(Message::text(r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle.stop().unwrap();
    assert_eq!(recv(&mut ws).await, r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#);
    tokio::time::timeout(Duration::from_secs(5), handle.clone().stopped()).await.unwrap();
}
