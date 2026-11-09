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
