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
//! `message_buffer_capacity` at once, answering more with `-32009 Server is busy`, while a writer
//! sends answers and subscription notifications from the connection's bounded queue. A write that
//! takes longer than the write timeout closes the connection, so a client that stops reading can't
//! hold its messages.

use super::{
    ServerConfig,
    http::{self, HttpBody, HttpRequest, HttpResponse},
    split_message,
};
use crate::{
    BatchRequestConfig, ConnectionId, MethodResponse, Request, RpcServiceT, error::ErrorCode,
    handle_rpc_call,
};
use alloy_json_rpc::Id;
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use hyper::{HeaderMap, StatusCode, header, upgrade::Upgraded};
use hyper_util::rt::TokioIo;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WsError, Message,
        handshake::server::create_response_with_body,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

// A WebSocket connection, before and after hyper hands over its socket.
pub(crate) struct WsConnection<S> {
    // The connection's id, for logs.
    pub(crate) id: ConnectionId,
    // Answers the connection's calls, sending subscriptions through `sender`.
    pub(crate) rpc_service: S,
    // Queues messages for the writer.
    pub(crate) sender: mpsc::Sender<String>,
    // The messages the writer sends to the client.
    pub(crate) outgoing: mpsc::Receiver<String>,
    // The limits.
    pub(crate) config: ServerConfig,
    // Cancelled when the server stops.
    pub(crate) stop: CancellationToken,
    // A slot of the server's connection limit, held while the connection is open.
    pub(crate) permit: OwnedSemaphorePermit,
}

impl<S: RpcServiceT + Send + Sync + 'static> WsConnection<S> {
    // Answers `request` with `101 Switching Protocols` and serves the connection on `tasks` once
    // hyper hands it over, or answers `400 Bad Request` to an invalid handshake.
    pub(crate) fn upgrade(self, mut request: HttpRequest, tasks: &TaskTracker) -> HttpResponse {
        let response = match create_response_with_body(&request, HttpBody::default) {
            Ok(response) => response,
            Err(err) => {
                tracing::debug!(%err, "invalid WebSocket handshake");
                return http::text(StatusCode::BAD_REQUEST, "Invalid WebSocket upgrade request\n");
            }
        };
        let upgraded = hyper::upgrade::on(&mut request);
        tasks.spawn(async move {
            match upgraded.await {
                Ok(upgraded) => self.serve(TokioIo::new(upgraded)).await,
                Err(err) => tracing::debug!(%err, "failed to upgrade to WebSocket"),
            }
        });
        response
    }

    // Serves the connection until the client leaves, a write times out, or the server stops.
    async fn serve(self, io: TokioIo<Upgraded>) {
        let Self { id, rpc_service, sender, outgoing, config, stop, permit } = self;
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
        let read =
            read_loop(stream, Arc::new(rpc_service), sender, &config, &stop, &reading, &answered);
        let write = write_loop(sink, outgoing, config.ws_write_timeout, &answered, &reading);
        future::join(read, write).await;
        tracing::debug!(id = id.0, "WebSocket connection closed");
        drop(permit);
    }
}

// Reads requests and runs each as its own call until `reading` is cancelled or the client
// leaves, then cancels `answered` once the running calls end. They finish when the server stops,
// and are dropped otherwise, since no one is left to answer.
async fn read_loop<St, S>(
    mut stream: St,
    rpc_service: Arc<S>,
    sender: mpsc::Sender<String>,
    config: &ServerConfig,
    stop: &CancellationToken,
    reading: &CancellationToken,
    answered: &CancellationToken,
) where
    St: Stream<Item = Result<Message, WsError>> + Unpin,
    S: RpcServiceT + Send + Sync + 'static,
{
    let capacity = usize::try_from(config.message_buffer_capacity).unwrap_or(usize::MAX);
    // Bounds the calls running at once, so a client sending faster than it reads waits.
    let slots = Arc::new(Semaphore::new(capacity));
    let calls = TaskTracker::new();
    let disconnected = CancellationToken::new();
    while let Some(Some(message)) = reading.run_until_cancelled(stream.next()).await {
        let body = match message {
            Ok(Message::Text(text)) => Bytes::from(text),
            Ok(Message::Binary(bytes)) => bytes,
            Ok(Message::Close(_)) => break,
            // The stream answers pings itself.
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
            // The read stopped partway through the message, so the connection can't continue.
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
        // Waiting for a slot would stop reading, and with it noticing that the client left.
        let Ok(slot) = slots.clone().try_acquire_owned() else {
            let Some(busy) = busy(&body) else { continue };
            match reading.run_until_cancelled(sender.send(busy)).await {
                Some(Ok(())) => continue,
                // The connection is closing.
                _ => break,
            }
        };
        let (rpc_service, sender, batch_config, disconnected) = (
            rpc_service.clone(),
            sender.clone(),
            config.batch_requests_config,
            disconnected.clone(),
        );
        calls.spawn(async move {
            let answer = answer(&body, batch_config, &*rpc_service);
            if let Some(Some(json)) = disconnected.run_until_cancelled(answer).await {
                // A failed send means the connection is closing, with no one left to answer.
                let _ = sender.send(json).await;
            }
            drop(slot);
        });
    }
    if !stop.is_cancelled() {
        disconnected.cancel();
    }
    calls.close();
    calls.wait().await;
    answered.cancel();
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
    Si: Sink<Message, Error = WsError> + Unpin,
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

// The `-32009 Server is busy` answer to `body`, under its id if it's a single call, or `None` for
// a notification, which gets no answer.
fn busy(body: &[u8]) -> Option<String> {
    match split_message(body) {
        Some((message, true)) => busy_call(message).map(MethodResponse::into_json),
        _ => Some(MethodResponse::error(Id::None, ErrorCode::ServerIsBusy).into_json()),
    }
}

// The busy error for one request, or `None` for a notification. A request that fails to parse is
// answered under a `null` id.
fn busy_call(message: &[u8]) -> Option<MethodResponse> {
    let id = match serde_json::from_slice::<Request<'_>>(message) {
        // An absent id marks a notification, while `"id": null` still expects an answer.
        Ok(request) => request.id?,
        Err(_) => Id::None,
    };
    Some(MethodResponse::error(id, ErrorCode::ServerIsBusy))
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

/// Returns `true` if `headers` ask to switch to WebSocket.
///
/// Only the `Upgrade` header is checked. The handshake validates the rest.
pub fn is_upgrade_request(headers: &HeaderMap) -> bool {
    headers.get(header::UPGRADE).and_then(|value| value.to_str().ok()).is_some_and(|value| {
        value.split(',').any(|token| token.trim().eq_ignore_ascii_case("websocket"))
    })
}
