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
//! notifications from the connection's bounded queue. Messages queued together are written with
//! one flush. A write that takes longer than the write timeout closes the connection, so a client
//! that stops reading can't hold its messages. A message over the request size limit is answered
//! with `-32007 Request is too big` and skipped, unless it only passes the limit in a later frame,
//! which closes the connection. On
//! shutdown, running calls have 30 seconds to finish before their futures are dropped. Already
//! running blocking callbacks continue on their pool.

use super::{
    ServerConfig,
    http::{self, HttpBody, HttpRequest, HttpResponse},
    request_size_limit::RequestSizeLimit,
    split_message,
};
use crate::{
    BatchRequestConfig, ConnectionId, MethodResponse, RpcServiceT, error::ErrorCode,
    handle_rpc_call,
};
use alloy_json_rpc::Id;
use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use hyper::{HeaderMap, StatusCode, header, upgrade::Upgraded};
use hyper_util::rt::TokioIo;
use std::{pin::pin, sync::Arc, time::Duration};
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

// Bounds the grace period for calls after the connection stops accepting requests.
const CALL_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

// The most queued messages written before a flush, so one write timeout covers a bounded write.
const MAX_MESSAGES_PER_FLUSH: usize = 64;

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
        // Skips the messages the decoder would end the connection over.
        let io = RequestSizeLimit::new(io, config.max_request_body_size, sender.clone());
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
// leaves, then cancels `answered` once the running calls end. Shutdown gives calls a bounded
// grace period, while disconnects drop them immediately, since no one is left to answer.
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
    // Shared by reference count, which costs a call less than a clone of the token.
    let disconnected = Arc::new(CancellationToken::new());
    let _cancel_on_drop = disconnected.drop_guard_ref();
    while let Some(Some(message)) = reading.run_until_cancelled(stream.next()).await {
        let body = match message {
            Ok(Message::Text(text)) => Bytes::from(text),
            Ok(Message::Binary(bytes)) => bytes,
            Ok(Message::Close(_)) => break,
            // The stream answers pings itself.
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => continue,
            // The message passed the limit in a later frame, when the decoder already held its
            // first ones and can't skip it, so the connection can't continue.
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
        let disconnected = disconnected.clone();
        calls.spawn(async move {
            let call = pin!(async {
                if let Some(json) = answer(&body, batch_config, &*rpc_service).await {
                    // A failed send means the connection is closing, with no one left to answer.
                    let _ = sender.send(json).await;
                }
            });
            // The call is polled first, so one that answers at once never waits on the disconnect.
            future::select(call, pin!(disconnected.cancelled())).await;
            drop(slot);
        });
    }
    if !stop.is_cancelled() {
        disconnected.cancel();
    }
    calls.close();
    if tokio::time::timeout(CALL_DRAIN_TIMEOUT, calls.wait()).await.is_err() {
        tracing::debug!("WebSocket calls exceeded the drain deadline");
        disconnected.cancel();
        calls.wait().await;
    }
    answered.cancel();
}

// Sends queued messages until every call is answered, a write fails, or a write takes longer than
// `write_timeout`, then closes the queue and cancels `reading`. Messages queued together share a
// flush.
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
    let mut queued = Vec::with_capacity(MAX_MESSAGES_PER_FLUSH);
    loop {
        let next = outgoing.recv_many(&mut queued, MAX_MESSAGES_PER_FLUSH);
        if answered.run_until_cancelled(next).await.is_none() {
            // Every call is answered, so only the queued messages are left to send.
            outgoing.close();
            outgoing.recv_many(&mut queued, MAX_MESSAGES_PER_FLUSH).await;
        }
        // The queue is closed and empty.
        if queued.is_empty() {
            break;
        }
        match tokio::time::timeout(write_timeout, send_all(&mut sink, &mut queued)).await {
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

// Writes `messages` and flushes once, leaving `messages` empty.
async fn send_all<Si>(sink: &mut Si, messages: &mut Vec<String>) -> Result<(), WsError>
where
    Si: Sink<Message, Error = WsError> + Unpin,
{
    for json in messages.drain(..) {
        sink.feed(Message::text(json)).await?;
    }
    sink.flush().await
}

/// Returns `true` if `headers` ask to switch to WebSocket.
///
/// Only the `Upgrade` header is checked. The handshake validates the rest.
pub fn is_upgrade_request(headers: &HeaderMap) -> bool {
    headers.get(header::UPGRADE).and_then(|value| value.to_str().ok()).is_some_and(|value| {
        value.split(',').any(|token| token.trim().eq_ignore_ascii_case("websocket"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorPayload, RpcModule, RpcService};
    use futures_util::FutureExt;
    use std::{
        pin::{Pin, pin},
        task::{Context, Poll},
    };
    use test_case::test_case;
    use tokio::{sync::Notify, time::Instant};

    // Counts the messages written to it and its flushes.
    #[derive(Default)]
    struct CountingSink {
        // The messages written.
        messages: usize,
        // The flushes.
        flushes: usize,
    }

    impl Sink<Message> for CountingSink {
        type Error = WsError;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, _message: Message) -> Result<(), WsError> {
            self.messages += 1;
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), WsError>> {
            self.flushes += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test_case(false, false; "pending call is dropped")]
    #[test_case(true, false; "running call finishes")]
    #[test_case(true, true; "full response queue")]
    #[tokio::test(start_paused = true)]
    async fn call_drain_has_a_deadline(finish: bool, full_queue: bool) {
        let started = Arc::new(Notify::new());
        let complete = Arc::new(Notify::new());
        let mut module = RpcModule::new((started.clone(), complete.clone()));
        module
            .register_async_method("wait", |(): (), context| async move {
                context.0.notify_one();
                context.1.notified().await;
                Ok::<_, ErrorPayload>("done")
            })
            .unwrap();
        let service = Arc::new(RpcService::new(module, 1024));
        let stream = futures_util::stream::iter([Ok(Message::text(
            r#"{"jsonrpc":"2.0","id":1,"method":"wait"}"#,
        ))])
        .chain(futures_util::stream::pending())
        .boxed();
        let (sender, mut outgoing) = mpsc::channel(1);
        if full_queue {
            sender.try_send("queued".to_owned()).unwrap();
        }
        let stop = CancellationToken::new();
        let reading = stop.child_token();
        let answered = CancellationToken::new();
        let config = ServerConfig::default();
        let before = Instant::now();
        let read = read_loop(stream, service.clone(), sender, &config, &stop, &reading, &answered);
        let shutdown = async {
            started.notified().await;
            stop.cancel();
            if finish {
                complete.notify_one();
            }
        };
        tokio::time::timeout(Duration::from_secs(31), future::join(read, shutdown)).await.unwrap();
        assert!(answered.is_cancelled());
        assert_eq!(Arc::strong_count(&service), 1);
        if finish && !full_queue {
            assert_eq!(before.elapsed(), Duration::ZERO);
            assert_eq!(
                outgoing.recv().await.unwrap(),
                r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#
            );
        } else {
            assert_eq!(before.elapsed(), CALL_DRAIN_TIMEOUT);
            if full_queue {
                assert_eq!(outgoing.recv().await.as_deref(), Some("queued"));
            }
            assert_eq!(outgoing.recv().await, None);
        }
    }

    #[tokio::test]
    async fn dropping_reader_cancels_pending_calls() {
        let started = Arc::new(Notify::new());
        let mut module = RpcModule::new(started.clone());
        module
            .register_async_method("wait", |(): (), started| async move {
                started.notify_one();
                future::pending::<Result<(), ErrorPayload>>().await
            })
            .unwrap();
        let service = Arc::new(RpcService::new(module, 1024));
        let stream = futures_util::stream::iter([Ok(Message::text(
            r#"{"jsonrpc":"2.0","id":1,"method":"wait"}"#,
        ))])
        .chain(futures_util::stream::pending())
        .boxed();
        let (sender, mut outgoing) = mpsc::channel(1);
        let stop = CancellationToken::new();
        let reading = stop.child_token();
        let answered = CancellationToken::new();
        let config = ServerConfig::default();
        {
            let mut read = pin!(read_loop(
                stream,
                service.clone(),
                sender,
                &config,
                &stop,
                &reading,
                &answered
            ));
            assert!(read.as_mut().now_or_never().is_none());
            started.notified().await;
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), outgoing.recv()).await.unwrap(),
            None
        );
        assert_eq!(Arc::strong_count(&service), 1);
    }

    #[test_case(3, 1; "queued together")]
    #[test_case(MAX_MESSAGES_PER_FLUSH + 1, 2; "past the cap")]
    #[tokio::test]
    async fn queued_messages_share_a_flush(queued: usize, flushes: usize) {
        let (sender, outgoing) = mpsc::channel(queued);
        for _ in 0..queued {
            sender.try_send(String::new()).unwrap();
        }
        drop(sender);
        let mut sink = CountingSink::default();
        let (answered, reading) = (CancellationToken::new(), CancellationToken::new());
        write_loop(&mut sink, outgoing, Duration::from_secs(1), &answered, &reading).await;
        assert_eq!((sink.messages, sink.flushes), (queued, flushes));
        assert!(reading.is_cancelled());
    }
}
