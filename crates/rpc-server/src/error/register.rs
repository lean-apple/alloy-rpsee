//! Errors registering methods.

/// A method couldn't be registered.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegisterMethodError {
    /// The name is taken.
    #[error("method `{0}` is already registered")]
    AlreadyRegistered(&'static str),
    /// An alias points to a method that isn't registered.
    #[error("method `{0}` is not registered")]
    MethodNotFound(&'static str),
    /// A subscription's subscribe and unsubscribe methods have the same name.
    #[error("subscription `{0}` has the same subscribe and unsubscribe name")]
    SubscriptionNameConflict(&'static str),
}
