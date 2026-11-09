//! A JSON-RPC server over HTTP and WebSocket on one port, built on hyper, tower and
//! tokio-tungstenite.
//!
//! [`ServerBuilder`] binds a listener with a [`ServerConfig`] and optional HTTP and RPC
//! middleware, and [`Server::start`] serves [`Methods`](crate::Methods) until its
//! [`ServerHandle`] stops it. Each request is answered by
//! [`handle_rpc_call`](crate::handle_rpc_call) within the configured size and connection limits.

mod builder;
pub use builder::{HttpService, RpcServiceBuilder, Server, ServerBuilder};

mod config;
pub use config::{DEFAULT_SIZE_LIMIT, ServerConfig, ServerConfigBuilder};

mod handle;
pub use handle::ServerHandle;

pub mod http;
pub use http::{BoxError, HttpBody, HttpRequest, HttpResponse};

mod ws;
pub use ws::is_upgrade_request;

// Splits off the whitespace before a message and tells one message from a batch by its first
// byte, or returns `None` for a body that isn't JSON.
pub(crate) fn split_message(body: &[u8]) -> Option<(&[u8], bool)> {
    let start = body.iter().position(|byte| !byte.is_ascii_whitespace())?;
    let message = body.get(start..)?;
    match message.first() {
        Some(b'{') => Some((message, true)),
        Some(b'[') => Some((message, false)),
        _ => None,
    }
}
