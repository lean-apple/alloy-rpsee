//! Errors running a server.

/// The server was already stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the server was already stopped")]
pub struct AlreadyStoppedError;
