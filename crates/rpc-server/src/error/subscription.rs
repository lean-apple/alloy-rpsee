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

//! Errors accepting a subscription and sending its notifications.

use crate::SubscriptionMessage;

/// A subscription couldn't be accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the subscription couldn't be accepted")]
pub struct PendingSubscriptionAcceptError;

/// A notification wasn't sent because the subscription ended. Holds the notification.
#[derive(Debug, thiserror::Error)]
#[error("the subscription ended")]
pub struct DisconnectError(pub SubscriptionMessage);

/// A notification wasn't sent within the timeout. Holds the notification.
#[derive(Debug, thiserror::Error)]
pub enum SendTimeoutError {
    /// The queue stayed full until the timeout.
    #[error("the subscription queue stayed full")]
    Timeout(SubscriptionMessage),
    /// The subscription ended.
    #[error("the subscription ended")]
    Closed(SubscriptionMessage),
}

/// A notification wasn't sent right away. Holds the notification.
#[derive(Debug, thiserror::Error)]
pub enum TrySendError {
    /// The queue is full.
    #[error("the subscription queue is full")]
    Full(SubscriptionMessage),
    /// The subscription ended.
    #[error("the subscription ended")]
    Closed(SubscriptionMessage),
}
