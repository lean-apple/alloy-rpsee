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

//! Subscriptions: the connection they run on, their ids, and the sinks that send notifications.
//!
//! A subscribe call is answered with the subscription id before any notification, all through
//! the connection's bounded queue, so a slow client holds back its own subscriptions, which can
//! wait, time out, or check the queue's room. A subscription ends when the client unsubscribes,
//! the connection closes, or its last sink is dropped. An error it returns is sent as a last
//! notification.

use crate::{
    MethodResponse,
    error::{DisconnectError, PendingSubscriptionAcceptError, SendTimeoutError, TrySendError},
    request::Version,
};
use alloy_json_rpc::{ErrorPayload, Id};
use alloy_primitives::map::HashMap;
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    fmt,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// The id a subscription is known by, sent back to the client when it is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionId {
    /// A numeric id.
    Num(u64),
    /// A string id.
    Str(String),
}

/// Generates subscription ids.
pub trait IdProvider: Send + Sync + fmt::Debug {
    /// Returns a new id, unique within its connection.
    fn next_id(&self) -> SubscriptionId;
}

/// Random numeric ids that fit a JavaScript number.
#[derive(Debug, Clone, Copy, Default)]
pub struct RandomIntegerIdProvider;

impl IdProvider for RandomIntegerIdProvider {
    fn next_id(&self) -> SubscriptionId {
        // The largest integer a JavaScript number holds exactly is 2^53 - 1.
        const JS_SAFE_INTEGER_MASK: u64 = (1 << 53) - 1;
        SubscriptionId::Num(rand::random::<u64>() & JS_SAFE_INTEGER_MASK)
    }
}

/// Random alphanumeric string ids of a fixed length.
#[derive(Debug, Clone, Copy)]
pub struct RandomStringIdProvider {
    // The id length in characters.
    len: usize,
}

impl RandomStringIdProvider {
    /// Ids of `len` characters.
    pub const fn new(len: usize) -> Self {
        Self { len }
    }
}

impl IdProvider for RandomStringIdProvider {
    fn next_id(&self) -> SubscriptionId {
        SubscriptionId::Str(Alphanumeric.sample_string(&mut rand::rng(), self.len))
    }
}

/// The id of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ConnectionId(pub usize);

impl From<usize> for ConnectionId {
    fn from(id: usize) -> Self {
        Self(id)
    }
}

/// A client connection that can carry subscriptions, such as a WebSocket.
///
/// The transport reads the messages to send from the receiver of `sender`, whose capacity bounds
/// how far a slow client can fall behind. Cloning shares the connection.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{
///     BatchRequestConfig, Connection, ConnectionId, RandomIntegerIdProvider, RpcModule,
///     RpcService, SubscriptionMessage, handle_rpc_call,
/// };
/// use std::sync::Arc;
/// use tokio::sync::mpsc;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let mut module = RpcModule::new(());
/// module
///     .register_subscription(
///         "eth_subscribe",
///         "eth_subscription",
///         "eth_unsubscribe",
///         |(): (), pending, _| async move {
///             let sink = pending.accept().await?;
///             sink.send(SubscriptionMessage::new(
///                 sink.method_name(),
///                 sink.subscription_id(),
///                 &"0x1",
///             )?)
///             .await?;
///             Ok(())
///         },
///     )
///     .unwrap();
///
/// let (sender, mut received) = mpsc::channel(16);
/// let connection =
///     Connection::new(ConnectionId(0), sender, 1024, Arc::new(RandomIntegerIdProvider));
/// let service = RpcService::with_connection(module, 10 * 1024 * 1024, connection);
///
/// let body = br#"{"jsonrpc":"2.0","id":1,"method":"eth_subscribe"}"#;
/// let response = handle_rpc_call(body, true, BatchRequestConfig::Unlimited, &service).await;
/// assert!(response.is_subscription());
/// assert!(received.recv().await.unwrap().contains(r#""id":1,"result":"#));
/// assert!(received.recv().await.unwrap().contains(r#""result":"0x1""#));
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Connection {
    // Shared by every clone.
    inner: Arc<ConnectionInner>,
}

impl Connection {
    /// Creates connection `id`, sending its messages through `sender`, with at most
    /// `max_subscriptions` active at once, identified by ids from `id_provider`.
    pub fn new(
        id: ConnectionId,
        sender: mpsc::Sender<String>,
        max_subscriptions: u32,
        id_provider: Arc<dyn IdProvider>,
    ) -> Self {
        let permits =
            Arc::new(Semaphore::new(usize::try_from(max_subscriptions).unwrap_or(usize::MAX)));
        let subscriptions = Mutex::default();
        Self {
            inner: Arc::new(ConnectionInner {
                id,
                sender,
                permits,
                max_subscriptions,
                id_provider,
                subscriptions,
            }),
        }
    }

    /// Returns the connection's id.
    pub fn id(&self) -> ConnectionId {
        self.inner.id
    }

    // A slot for one more subscription, or `None` at the limit.
    pub(crate) fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.permits.clone().try_acquire_owned().ok()
    }

    // The most subscriptions active at once.
    pub(crate) fn max_subscriptions(&self) -> u32 {
        self.inner.max_subscriptions
    }

    // A new subscription id.
    pub(crate) fn next_id(&self) -> SubscriptionId {
        self.inner.id_provider.next_id()
    }

    // Sends the last notification of a subscription that ended with `err`.
    pub(crate) async fn send_error(
        &self,
        method: &str,
        subscription: &SubscriptionId,
        err: SubscriptionError,
    ) {
        let notification =
            Notification::new(method, subscription, NotificationPayload::<()>::Error(err.0));
        // A string always serializes.
        let json = serde_json::to_string(&notification).unwrap_or_default();
        if self.inner.sender.send(json).await.is_err() {
            tracing::debug!(method, "connection closed before a subscription's last notification");
        }
    }

    // Ends subscription `id` of method `subscribe`. Returns `false` if it isn't active.
    pub(crate) fn unsubscribe(&self, subscribe: &'static str, id: SubscriptionId) -> bool {
        let ended = self.inner.subscriptions().remove(&(subscribe, id));
        ended.inspect(CancellationToken::cancel).is_some()
    }
}

// The state shared by a connection's clones.
#[derive(Debug)]
struct ConnectionInner {
    // The connection's id.
    id: ConnectionId,
    // The messages to send to the client.
    sender: mpsc::Sender<String>,
    // One permit per subscription that may still start.
    permits: Arc<Semaphore>,
    // The most subscriptions active at once.
    max_subscriptions: u32,
    // Generates subscription ids.
    id_provider: Arc<dyn IdProvider>,
    // The active subscriptions, by subscribe method and id, each cancelled when unsubscribed.
    subscriptions: Mutex<HashMap<(&'static str, SubscriptionId), CancellationToken>>,
}

impl ConnectionInner {
    // The active subscriptions. No code panics while holding the lock, so a poisoned lock still
    // holds a consistent map.
    fn subscriptions(
        &self,
    ) -> MutexGuard<'_, HashMap<(&'static str, SubscriptionId), CancellationToken>> {
        self.subscriptions.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A notification to send on a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionMessage(Message);

// A notification, complete or still to be wrapped with the method and the subscription id.
#[derive(Debug, Clone)]
enum Message {
    // The whole notification.
    Complete(String),
    // Only its `result`.
    Result(Box<RawValue>),
}

impl SubscriptionMessage {
    /// A notification for subscription `subscription` of `method` carrying `result`.
    ///
    /// # Errors
    ///
    /// Returns the error if `result` fails to serialize.
    pub fn new(
        method: &str,
        subscription: SubscriptionId,
        result: &impl Serialize,
    ) -> Result<Self, serde_json::Error> {
        let notification =
            Notification::new(method, &subscription, NotificationPayload::Result(result));
        serde_json::to_string(&notification).map(|json| Self(Message::Complete(json)))
    }

    // The notification sent on `sink`.
    fn into_json(self, sink: &SinkInner) -> String {
        match self.0 {
            Message::Complete(json) => json,
            Message::Result(result) => {
                let notification = Notification::new(
                    sink.method,
                    &sink.subscription_id,
                    NotificationPayload::Result(&*result),
                );
                // A raw JSON value and a string always serialize.
                serde_json::to_string(&notification).unwrap_or_default()
            }
        }
    }
}

impl From<Box<RawValue>> for SubscriptionMessage {
    fn from(result: Box<RawValue>) -> Self {
        Self(Message::Result(result))
    }
}

/// What a subscription returns when it ends. An error is sent to the client as a last
/// notification.
pub type SubscriptionResult = Result<(), SubscriptionError>;

/// The reason a subscription ended, sent to the client in its last notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionError(String);

impl<T: fmt::Display> From<T> for SubscriptionError {
    fn from(err: T) -> Self {
        Self(err.to_string())
    }
}

/// A subscription call waiting to be accepted or rejected.
///
/// The call is answered only once [`accept`](Self::accept) or [`reject`](Self::reject) runs.
/// Dropping it without either answers `-32603 Internal error`.
#[derive(Debug)]
#[must_use = "a subscription call is answered only by `accept` or `reject`"]
pub struct PendingSubscriptionSink {
    // The id of the subscribe call.
    pub(crate) id: Id,
    // The subscription as it will run once accepted.
    pub(crate) sink: SinkInner,
    // Answers the subscribe call.
    pub(crate) answer: oneshot::Sender<MethodResponse>,
    // Set once accepted, so a later error is sent as a notification.
    pub(crate) accepted: Arc<AtomicBool>,
}

impl PendingSubscriptionSink {
    /// Accepts the subscription and returns the sink its notifications are sent on.
    ///
    /// # Errors
    ///
    /// Returns [`PendingSubscriptionAcceptError`] if the connection closed, or the id doesn't
    /// fit the response size limit, which also answers the call with `-32008`.
    pub async fn accept(self) -> Result<SubscriptionSink, PendingSubscriptionAcceptError> {
        let Self { id, sink, answer, accepted } = self;
        let response = MethodResponse::success(id, &sink.subscription_id, sink.max_response_size);
        if response.is_error() {
            answer_call(answer, response);
            return Err(PendingSubscriptionAcceptError);
        }
        // The id is sent before any notification, so it goes through the connection too.
        sink.connection
            .inner
            .sender
            .send(response.into_json())
            .await
            .map_err(|_| PendingSubscriptionAcceptError)?;
        sink.connection
            .inner
            .subscriptions()
            .insert((sink.subscribe, sink.subscription_id.clone()), sink.unsubscribed.clone());
        accepted.store(true, Ordering::Release);
        answer_call(answer, MethodResponse::subscription());
        Ok(SubscriptionSink { inner: Arc::new(sink) })
    }

    /// Rejects the subscription, answering the call with `err`.
    pub async fn reject(self, err: impl Into<ErrorPayload>) {
        answer_call(self.answer, MethodResponse::error(self.id, err));
    }

    /// Returns the id the subscription gets if accepted.
    pub fn subscription_id(&self) -> SubscriptionId {
        self.sink.subscription_id.clone()
    }

    /// Returns the id of the connection the call arrived on.
    pub fn connection_id(&self) -> ConnectionId {
        self.sink.connection.id()
    }

    /// Returns the method name notifications are sent with.
    pub fn method_name(&self) -> &str {
        self.sink.method
    }
}

// Answers the subscribe call, unless its client is already gone.
fn answer_call(answer: oneshot::Sender<MethodResponse>, response: MethodResponse) {
    if answer.send(response).is_err() {
        tracing::debug!("subscription call dropped before its answer");
    }
}

/// An accepted subscription, which sends notifications to the client.
///
/// Clones send on the same subscription, which ends when the client unsubscribes, the connection
/// closes, or the last clone is dropped.
#[derive(Debug, Clone)]
pub struct SubscriptionSink {
    // Shared by every clone, and torn down with the last one.
    inner: Arc<SinkInner>,
}

impl SubscriptionSink {
    /// Returns the subscription's id.
    pub fn subscription_id(&self) -> SubscriptionId {
        self.inner.subscription_id.clone()
    }

    /// Returns the method name notifications are sent with.
    pub fn method_name(&self) -> &str {
        self.inner.method
    }

    /// Returns the id of the connection the subscription runs on.
    pub fn connection_id(&self) -> ConnectionId {
        self.inner.connection.id()
    }

    /// Sends `msg`, waiting while the connection's queue is full.
    ///
    /// # Errors
    ///
    /// Returns `msg` in [`DisconnectError`] if the subscription ended.
    pub async fn send(&self, msg: impl Into<SubscriptionMessage>) -> Result<(), DisconnectError> {
        let msg = msg.into();
        if self.is_closed() {
            return Err(DisconnectError(msg));
        }
        // Waiting for room before serializing keeps `msg` to hand back if the subscription ends.
        let reserve = self.inner.connection.inner.sender.reserve();
        match self.inner.unsubscribed.run_until_cancelled(reserve).await {
            Some(Ok(slot)) => {
                slot.send(msg.into_json(&self.inner));
                Ok(())
            }
            Some(Err(_)) | None => Err(DisconnectError(msg)),
        }
    }

    /// Sends `msg`, waiting at most `timeout` while the connection's queue is full.
    ///
    /// # Errors
    ///
    /// Returns `msg` in [`SendTimeoutError`] if the subscription ended or the queue stayed full.
    pub async fn send_timeout(
        &self,
        msg: impl Into<SubscriptionMessage>,
        timeout: Duration,
    ) -> Result<(), SendTimeoutError> {
        let msg = msg.into();
        if self.is_closed() {
            return Err(SendTimeoutError::Closed(msg));
        }
        let json = msg.into_json(&self.inner);
        self.inner.connection.inner.sender.send_timeout(json, timeout).await.map_err(
            |err| match err {
                mpsc::error::SendTimeoutError::Timeout(json) => {
                    SendTimeoutError::Timeout(complete(json))
                }
                mpsc::error::SendTimeoutError::Closed(json) => {
                    SendTimeoutError::Closed(complete(json))
                }
            },
        )
    }

    /// Sends `msg` if the connection's queue has room.
    ///
    /// # Errors
    ///
    /// Returns `msg` in [`TrySendError`] if the subscription ended or the queue is full.
    pub fn try_send(&self, msg: impl Into<SubscriptionMessage>) -> Result<(), TrySendError> {
        let msg = msg.into();
        if self.is_closed() {
            return Err(TrySendError::Closed(msg));
        }
        let json = msg.into_json(&self.inner);
        self.inner.connection.inner.sender.try_send(json).map_err(|err| match err {
            mpsc::error::TrySendError::Full(json) => TrySendError::Full(complete(json)),
            mpsc::error::TrySendError::Closed(json) => TrySendError::Closed(complete(json)),
        })
    }

    /// Returns `true` once the client unsubscribed or the connection closed.
    pub fn is_closed(&self) -> bool {
        self.inner.unsubscribed.is_cancelled() || self.inner.connection.inner.sender.is_closed()
    }

    /// Waits until the client unsubscribes or the connection closes.
    pub async fn closed(&self) {
        self.inner
            .unsubscribed
            .run_until_cancelled(self.inner.connection.inner.sender.closed())
            .await;
    }

    /// Returns how many more messages the connection's queue takes right now.
    pub fn capacity(&self) -> usize {
        self.inner.connection.inner.sender.capacity()
    }

    /// Returns the size of the connection's queue.
    pub fn max_capacity(&self) -> usize {
        self.inner.connection.inner.sender.max_capacity()
    }
}

// A notification handed back unsent.
const fn complete(json: String) -> SubscriptionMessage {
    SubscriptionMessage(Message::Complete(json))
}

// A subscription's state, shared by its sinks.
#[derive(Debug)]
pub(crate) struct SinkInner {
    // The subscribe method, which with the id keys the subscription on its connection.
    pub(crate) subscribe: &'static str,
    // The method name notifications are sent with.
    pub(crate) method: &'static str,
    // The subscription's id.
    pub(crate) subscription_id: SubscriptionId,
    // The connection the subscription runs on.
    pub(crate) connection: Connection,
    // Cancelled when the client unsubscribes.
    pub(crate) unsubscribed: CancellationToken,
    // The most bytes the subscription id response may take.
    pub(crate) max_response_size: usize,
    // Frees the subscription's slot on the connection when dropped.
    pub(crate) _permit: OwnedSemaphorePermit,
}

impl Drop for SinkInner {
    fn drop(&mut self) {
        // The subscription ends with its last sink, so `unsubscribe` no longer finds it.
        let key = (self.subscribe, self.subscription_id.clone());
        self.connection.inner.subscriptions().remove(&key);
    }
}

// A subscription notification.
#[derive(Serialize)]
pub(crate) struct Notification<'a, T> {
    // The protocol version.
    jsonrpc: Version,
    // The method name notifications are sent with.
    method: &'a str,
    // The subscription and what it carries.
    params: NotificationParams<'a, T>,
}

impl<'a, T: Serialize> Notification<'a, T> {
    // A notification on `subscription` of `method`.
    pub(crate) const fn new(
        method: &'a str,
        subscription: &'a SubscriptionId,
        payload: NotificationPayload<T>,
    ) -> Self {
        Self { jsonrpc: Version::V2, method, params: NotificationParams { subscription, payload } }
    }
}

// The `params` of a notification.
#[derive(Serialize)]
struct NotificationParams<'a, T> {
    // The subscription's id.
    subscription: &'a SubscriptionId,
    // The result, or the error that ended the subscription.
    #[serde(flatten)]
    payload: NotificationPayload<T>,
}

// What a notification carries.
#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum NotificationPayload<T> {
    // A subscription item.
    Result(T),
    // Why the subscription ended.
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchRequestConfig, RpcModule, RpcService, handle_rpc_call};
    use std::sync::atomic::AtomicU64;
    use test_case::test_case;

    // Numbers subscriptions from 1, so tests know their ids.
    #[derive(Debug, Default)]
    struct Counter(AtomicU64);

    impl IdProvider for Counter {
        fn next_id(&self) -> SubscriptionId {
            SubscriptionId::Num(self.0.fetch_add(1, Ordering::Relaxed) + 1)
        }
    }

    // The sinks kept by the `keep` subscription.
    type Kept = Mutex<Vec<SubscriptionSink>>;

    // A client of a connection carrying `test_subscribe`, whose param picks what the
    // subscription does.
    struct Client {
        service: RpcService,
        received: mpsc::Receiver<String>,
        kept: Arc<Kept>,
    }

    impl Client {
        fn new(max_subscriptions: u32, queue: usize) -> Self {
            let kept = Arc::new(Kept::default());
            let mut module = RpcModule::new(kept.clone());
            module
                .register_subscription(
                    "test_subscribe",
                    "test_notification",
                    "test_unsubscribe",
                    |(mode,): (String,), pending, kept: Arc<Arc<Kept>>| async move {
                        match mode.as_str() {
                            "items" => {
                                let sink = pending.accept().await?;
                                for item in [1, 2] {
                                    let msg = SubscriptionMessage::new(
                                        sink.method_name(),
                                        sink.subscription_id(),
                                        &item,
                                    )?;
                                    sink.send(msg).await?;
                                }
                                sink.closed().await;
                            }
                            "keep" => {
                                let sink = pending.accept().await?;
                                kept.lock().unwrap_or_else(PoisonError::into_inner).push(sink);
                            }
                            "fail" => {
                                drop(pending.accept().await?);
                                return Err("boom".into());
                            }
                            "reject" => pending.reject(ErrorPayload::invalid_params()).await,
                            _ => drop(pending),
                        }
                        Ok(())
                    },
                )
                .unwrap();
            let (sender, received) = mpsc::channel(queue);
            let connection = Connection::new(
                ConnectionId(7),
                sender,
                max_subscriptions,
                Arc::new(Counter::default()),
            );
            Self {
                service: RpcService::with_connection(module, usize::MAX, connection),
                received,
                kept,
            }
        }

        async fn call(&self, body: &str) -> MethodResponse {
            let is_single = !body.starts_with('[');
            handle_rpc_call(
                body.as_bytes(),
                is_single,
                BatchRequestConfig::Unlimited,
                &self.service,
            )
            .await
        }

        async fn subscribe(&self, mode: &str) -> MethodResponse {
            self.call(&format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"test_subscribe","params":["{mode}"]}}"#
            ))
            .await
        }

        async fn unsubscribe(&self, id: &str) -> String {
            let body = format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"test_unsubscribe","params":[{id}]}}"#
            );
            self.call(&body).await.into_json()
        }

        async fn recv(&mut self) -> String {
            self.received.recv().await.unwrap()
        }

        fn sink(&self) -> SubscriptionSink {
            self.kept.lock().unwrap_or_else(PoisonError::into_inner).remove(0)
        }
    }

    fn notification(id: u64, result: &str) -> String {
        format!(
            r#"{{"jsonrpc":"2.0","method":"test_notification","params":{{"subscription":{id},"result":{result}}}}}"#
        )
    }

    #[tokio::test]
    async fn accepted_subscription_sends_its_id_then_notifications() {
        let mut client = Client::new(8, 8);
        assert!(client.subscribe("items").await.is_subscription());
        assert_eq!(client.recv().await, r#"{"jsonrpc":"2.0","id":1,"result":1}"#);
        assert_eq!(client.recv().await, notification(1, "1"));
        assert_eq!(client.recv().await, notification(1, "2"));
        assert_eq!(client.unsubscribe("1").await, r#"{"jsonrpc":"2.0","id":2,"result":true}"#);
        assert_eq!(client.unsubscribe("1").await, r#"{"jsonrpc":"2.0","id":2,"result":false}"#);
    }

    #[test_case("reject", r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params"}}"# ; "rejected")]
    #[test_case("drop", r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"# ; "dropped unanswered")]
    #[tokio::test]
    async fn unaccepted_subscription_answers_the_call(mode: &str, response: &str) {
        assert_eq!(Client::new(8, 8).subscribe(mode).await.into_json(), response);
    }

    #[tokio::test]
    async fn error_after_accept_is_the_last_notification() {
        let mut client = Client::new(8, 8);
        client.subscribe("fail").await;
        assert_eq!(client.recv().await, r#"{"jsonrpc":"2.0","id":1,"result":1}"#);
        assert_eq!(
            client.recv().await,
            r#"{"jsonrpc":"2.0","method":"test_notification","params":{"subscription":1,"error":"boom"}}"#
        );
    }

    #[tokio::test]
    async fn subscriptions_need_a_connection() {
        let kept = Arc::new(Kept::default());
        let mut module = RpcModule::new(kept);
        module
            .register_subscription(
                "test_subscribe",
                "n",
                "test_unsubscribe",
                |(): (), pending, _| async move {
                    drop(pending);
                    Ok(())
                },
            )
            .unwrap();
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"test_subscribe","params":[]}"#;
        assert_eq!(
            module.raw_json_request(body).await,
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}"#
        );
    }

    #[tokio::test]
    async fn connection_limits_subscriptions_until_one_ends() {
        let mut client = Client::new(1, 8);
        client.subscribe("keep").await;
        client.recv().await;
        assert_eq!(
            client.subscribe("keep").await.into_json(),
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32006,"message":"Too many subscriptions on the connection","data":"Exceeded max limit of 1"}}"#
        );
        drop(client.sink());
        assert!(client.subscribe("keep").await.is_subscription());
    }

    #[tokio::test]
    async fn subscription_ends_with_its_last_clone() {
        let mut client = Client::new(8, 8);
        client.subscribe("keep").await;
        client.recv().await;
        let sink = client.sink();
        let clone = sink.clone();
        drop(sink);
        assert!(!clone.is_closed());
        drop(clone);
        assert_eq!(client.unsubscribe("1").await, r#"{"jsonrpc":"2.0","id":2,"result":false}"#);
    }

    #[tokio::test]
    async fn unsubscribed_sink_is_closed() {
        let mut client = Client::new(8, 8);
        client.subscribe("keep").await;
        client.recv().await;
        let sink = client.sink();
        client.unsubscribe("1").await;
        sink.closed().await;
        assert!(matches!(
            sink.send(SubscriptionMessage::from(raw("1"))).await,
            Err(DisconnectError(_))
        ));
        assert!(matches!(sink.try_send(raw("1")), Err(TrySendError::Closed(_))));
    }

    #[tokio::test]
    async fn full_queue_is_reported() {
        let mut client = Client::new(8, 1);
        client.subscribe("keep").await;
        client.recv().await;
        let sink = client.sink();
        assert_eq!((sink.capacity(), sink.max_capacity()), (1, 1));
        sink.try_send(raw("1")).unwrap();
        assert!(matches!(sink.try_send(raw("2")), Err(TrySendError::Full(_))));
        let timeout = Duration::from_millis(1);
        assert!(matches!(
            sink.send_timeout(raw("2"), timeout).await,
            Err(SendTimeoutError::Timeout(_))
        ));
        assert_eq!(client.recv().await, notification(1, "1"));
    }

    #[tokio::test]
    async fn invalid_unsubscribe_params_match_nothing() {
        let client = Client::new(8, 8);
        assert_eq!(
            client.unsubscribe(r#""x", "y""#).await,
            r#"{"jsonrpc":"2.0","id":2,"result":false}"#
        );
        assert_eq!(client.unsubscribe("{}").await, r#"{"jsonrpc":"2.0","id":2,"result":false}"#);
    }

    #[tokio::test]
    async fn batch_leaves_out_accepted_subscriptions() {
        let mut client = Client::new(8, 8);
        let subscribe = r#"{"jsonrpc":"2.0","id":1,"method":"test_subscribe","params":["keep"]}"#;
        assert!(client.call(&format!("[{subscribe}]")).await.is_notification());
        assert_eq!(client.recv().await, r#"{"jsonrpc":"2.0","id":1,"result":1}"#);
        let unknown = r#"{"jsonrpc":"2.0","id":3,"method":"x"}"#;
        assert_eq!(
            client.call(&format!("[{subscribe},{unknown}]")).await.into_json(),
            r#"[{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"Method not found"}}]"#
        );
    }

    #[test]
    fn integer_ids_fit_a_javascript_number() {
        for _ in 0..100 {
            let SubscriptionId::Num(id) = RandomIntegerIdProvider.next_id() else { unreachable!() };
            assert!(id < 1 << 53);
        }
    }

    #[test]
    fn string_ids_have_the_requested_length() {
        let SubscriptionId::Str(id) = RandomStringIdProvider::new(16).next_id() else {
            unreachable!()
        };
        assert!(id.len() == 16 && id.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    fn raw(json: &str) -> Box<RawValue> {
        RawValue::from_string(json.to_owned()).unwrap()
    }
}
