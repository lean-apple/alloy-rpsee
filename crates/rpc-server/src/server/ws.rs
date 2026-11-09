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

//! Serving one WebSocket connection.
//!
//! The connection reads requests and runs each as its own call, at most
//! `message_buffer_capacity` at once, while a writer sends answers and subscription
//! notifications from the connection's bounded queue. A write that takes longer than the write
//! timeout closes the connection, so a client that stops reading can't hold its messages.

use super::{
    ServerConfig,
    http::{self, HttpRequest, HttpResponse},
    split_message,
};
use crate::{
    BatchRequestConfig, Connection, MethodResponse, RpcServiceT, error::ErrorCode, handle_rpc_call,
};
use alloy_json_rpc::Id;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt, future};
use hyper::{
    HeaderMap, StatusCode,
    header::{self, HeaderValue},
};
use hyper_util::rt::TokioIo;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WsError, Message,
        handshake::derive_accept_key,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

// The WebSocket version clients must speak.
const WS_VERSION: &str = "13";

// Returns `true` if `headers` ask to switch the connection to WebSocket.
pub(crate) fn is_upgrade_request(headers: &HeaderMap) -> bool {
    has_token(headers, header::CONNECTION, "upgrade")
        && has_token(headers, header::UPGRADE, "websocket")
}

// Returns `true` if a comma-separated `name` header lists `token`.
fn has_token(headers: &HeaderMap, name: header::HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}

// Answers an upgrade request with `101 Switching Protocols` and serves the connection once hyper
// hands it over, or answers `400 Bad Request` to a malformed upgrade.
pub(crate) fn upgrade<S>(
    request: HttpRequest,
    connection: WsConnection<S>,
    tasks: &TaskTracker,
) -> HttpResponse
where
    S: RpcServiceT + Send + Sync + 'static,
{
    let headers = request.headers();
    let version = headers.get(header::SEC_WEBSOCKET_VERSION).and_then(|value| value.to_str().ok());
    let Some(key) = headers.get(header::SEC_WEBSOCKET_KEY).filter(|_| version == Some(WS_VERSION))
    else {
        return http::text(StatusCode::BAD_REQUEST, "Invalid WebSocket upgrade request\n");
    };
    let accept = derive_accept_key(key.as_bytes());
    tasks.spawn(async move {
        match hyper::upgrade::on(request).await {
            Ok(upgraded) => connection.serve(TokioIo::new(upgraded)).await,
            Err(err) => tracing::debug!(%err, "failed to upgrade to WebSocket"),
        }
    });
    let mut response = http::empty(StatusCode::SWITCHING_PROTOCOLS);
    let headers = response.headers_mut();
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    if let Ok(accept) = HeaderValue::from_str(&accept) {
        headers.insert(header::SEC_WEBSOCKET_ACCEPT, accept);
    }
    response
}

// A WebSocket connection waiting for hyper to hand over its socket.
pub(crate) struct WsConnection<S> {
    // Answers the connection's calls, with subscriptions sent through `connection`.
    pub(crate) rpc_service: S,
    // The connection subscriptions run on, sending through `outgoing`'s sender.
    pub(crate) connection: Connection,
    // The messages to send to the client.
    pub(crate) outgoing: mpsc::Receiver<String>,
    // The sender answers are queued with.
    pub(crate) sender: mpsc::Sender<String>,
    // The limits.
    pub(crate) config: ServerConfig,
    // Cancelled when the server stops.
    pub(crate) stop: CancellationToken,
    // A slot of the server's connection limit, held while the connection is open.
    pub(crate) permit: OwnedSemaphorePermit,
}

impl<S: RpcServiceT + Send + Sync + 'static> WsConnection<S> {
    // Serves the connection until the client leaves, a write times out, or the server stops.
    async fn serve(self, io: TokioIo<hyper::upgrade::Upgraded>) {
        let Self { rpc_service, connection, outgoing, sender, config, stop, permit } = self;
        let max_message_size = usize::try_from(config.max_request_body_size).unwrap_or(usize::MAX);
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(max_message_size))
            .max_frame_size(Some(max_message_size));
        let ws = WebSocketStream::from_raw_socket(io, Role::Server, Some(ws_config)).await;
        let (sink, stream) = ws.split();
        // Cancelled when the server stops or the writer ends, which ends reading.
        let reading = stop.child_token();
        // Cancelled once no call is left to answer, which lets the writer finish.
        let answered = CancellationToken::new();
        let read = read_loop(stream, Arc::new(rpc_service), sender, &config, &reading, &answered);
        let write = write_loop(sink, outgoing, config.ws_write_timeout, &answered, &reading);
        future::join(read, write).await;
        tracing::debug!(id = connection.id().0, "WebSocket connection closed");
        drop(permit);
    }
}

// Reads requests and runs each as its own call until `reading` is cancelled or the client
// leaves, then waits for the running calls before cancelling `answered`.
async fn read_loop<St, S>(
    mut stream: St,
    rpc_service: Arc<S>,
    sender: mpsc::Sender<String>,
    config: &ServerConfig,
    reading: &CancellationToken,
    answered: &CancellationToken,
) where
    St: futures_util::Stream<Item = Result<Message, WsError>> + Unpin,
    S: RpcServiceT + Send + Sync + 'static,
{
    let capacity = usize::try_from(config.message_buffer_capacity).unwrap_or(usize::MAX);
    // Bounds the calls running at once, so a client sending faster than it reads waits.
    let slots = Arc::new(Semaphore::new(capacity));
    let calls = TaskTracker::new();
    while let Some(Some(message)) = reading.run_until_cancelled(stream.next()).await {
        let body = match message {
            Ok(Message::Text(text)) => Bytes::from(text),
            Ok(Message::Binary(bytes)) => bytes,
            Ok(Message::Close(_)) => break,
            // Pings are answered by the reader itself.
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
            // The read stopped halfway through the message, so the connection can't continue.
            Err(WsError::Capacity(err)) => {
                tracing::debug!(%err, "WebSocket message too large");
                let err = ErrorCode::OversizedRequest.exceeded(config.max_request_body_size);
                let _ = sender.send(MethodResponse::error(Id::None, err).into_json()).await;
                break;
            }
            Err(err) => {
                tracing::debug!(%err, "WebSocket read failed");
                break;
            }
        };
        let Some(Ok(slot)) = reading.run_until_cancelled(slots.clone().acquire_owned()).await
        else {
            break;
        };
        let (rpc_service, sender, batch_config) =
            (rpc_service.clone(), sender.clone(), config.batch_requests_config);
        calls.spawn(async move {
            if let Some(json) = answer(&body, batch_config, &*rpc_service).await {
                // A failed send means the connection is closing, with no one left to answer.
                let _ = sender.send(json).await;
            }
            drop(slot);
        });
    }
    calls.close();
    calls.wait().await;
    answered.cancel();
}

// The JSON to send back for `body`, if any.
async fn answer<S: RpcServiceT>(
    body: &[u8],
    batch_config: BatchRequestConfig,
    rpc_service: &S,
) -> Option<String> {
    let Some((message, is_single)) = split_message(body) else {
        return Some(MethodResponse::error(Id::None, ErrorCode::ParseError).into_json());
    };
    let response = handle_rpc_call(message, is_single, batch_config, rpc_service).await;
    // Notifications get no answer, and a subscription already sent its own.
    (!response.is_notification() && !response.is_subscription()).then(|| response.into_json())
}

// Sends queued messages until every call is answered, a write fails, or a write takes longer than
// `write_timeout`, then closes the queue and cancels `reading`.
async fn write_loop<Si>(
    mut sink: Si,
    mut outgoing: mpsc::Receiver<String>,
    write_timeout: Duration,
    answered: &CancellationToken,
    reading: &CancellationToken,
) where
    Si: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    let mut timed_out = false;
    loop {
        let json = match answered.run_until_cancelled(outgoing.recv()).await {
            Some(Some(json)) => json,
            Some(None) => break,
            // Every call is answered, so only the queued messages are left to send.
            None => {
                outgoing.close();
                match outgoing.recv().await {
                    Some(json) => json,
                    None => break,
                }
            }
        };
        match tokio::time::timeout(write_timeout, sink.send(Message::text(json))).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                tracing::debug!(%err, "WebSocket write failed");
                break;
            }
            Err(_) => {
                tracing::debug!(?write_timeout, "WebSocket write timed out");
                timed_out = true;
                break;
            }
        }
    }
    // Closing the queue ends the connection's subscriptions.
    outgoing.close();
    reading.cancel();
    // A client that stopped reading would block the close frame too.
    if !timed_out {
        let _ = tokio::time::timeout(write_timeout, sink.close()).await;
    }
}
