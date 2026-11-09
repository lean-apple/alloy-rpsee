//! A JSON-RPC server over HTTP, built on hyper and tower.
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
