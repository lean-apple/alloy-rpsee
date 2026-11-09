//! Server errors, grouped by who sees them: the client ([`ErrorCode`], [`InvalidParams`]), the
//! code registering methods ([`RegisterMethodError`]), and a subscription sending notifications.

mod call;
pub(crate) use call::BatchError;
pub use call::InvalidParams;

mod code;
pub use code::*;

mod register;
pub use register::RegisterMethodError;

mod subscription;
pub use subscription::{
    DisconnectError, PendingSubscriptionAcceptError, SendTimeoutError, TrySendError,
};

#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
pub use server::AlreadyStoppedError;
