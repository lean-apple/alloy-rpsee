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

//! Server configuration.

use crate::{BatchRequestConfig, IdProvider, RandomIntegerIdProvider};
use std::{sync::Arc, time::Duration};
use tokio::runtime::Handle;

/// The default request and response size limit, 10 MiB.
pub const DEFAULT_SIZE_LIMIT: u32 = 10 * 1024 * 1024;

// How long a WebSocket write may take before the connection is closed, as in geth.
const DEFAULT_WS_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// The server's limits and runtime.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // The largest request body, in bytes.
    pub(crate) max_request_body_size: u32,
    // The largest response, or batch of responses, in bytes.
    pub(crate) max_response_body_size: u32,
    // The most requests answered at once.
    pub(crate) max_connections: u32,
    // How batches are handled.
    pub(crate) batch_requests_config: BatchRequestConfig,
    // The runtime connections are served on, the caller's if `None`.
    pub(crate) tokio_runtime: Option<Handle>,
    // Whether plain HTTP requests are answered.
    pub(crate) enable_http: bool,
    // Whether WebSocket upgrades are accepted.
    pub(crate) enable_ws: bool,
    // The most subscriptions one WebSocket connection may hold.
    pub(crate) max_subscriptions_per_connection: u32,
    // The most messages queued for, and calls running on, one WebSocket connection.
    pub(crate) message_buffer_capacity: u32,
    // Generates subscription ids.
    pub(crate) id_provider: Arc<dyn IdProvider>,
    // How long one WebSocket write may take.
    pub(crate) ws_write_timeout: Duration,
}

impl ServerConfig {
    /// Returns a builder starting from the default configuration.
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::default()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_request_body_size: DEFAULT_SIZE_LIMIT,
            max_response_body_size: DEFAULT_SIZE_LIMIT,
            max_connections: 100,
            batch_requests_config: BatchRequestConfig::Unlimited,
            tokio_runtime: None,
            enable_http: true,
            enable_ws: true,
            max_subscriptions_per_connection: 1024,
            message_buffer_capacity: 1024,
            id_provider: Arc::new(RandomIntegerIdProvider),
            ws_write_timeout: DEFAULT_WS_WRITE_TIMEOUT,
        }
    }
}

/// Builds a [`ServerConfig`].
#[derive(Debug, Clone, Default)]
pub struct ServerConfigBuilder(ServerConfig);

impl ServerConfigBuilder {
    /// Returns a builder starting from the default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the largest request body, in bytes. Defaults to 10 MiB.
    pub const fn max_request_body_size(mut self, size: u32) -> Self {
        self.0.max_request_body_size = size;
        self
    }

    /// Sets the largest response, or batch of responses, in bytes. Defaults to 10 MiB.
    pub const fn max_response_body_size(mut self, size: u32) -> Self {
        self.0.max_response_body_size = size;
        self
    }

    /// Sets how many requests are answered at once, past which requests get `429 Too Many
    /// Requests`. Defaults to 100.
    pub const fn max_connections(mut self, max: u32) -> Self {
        self.0.max_connections = max;
        self
    }

    /// Sets how batches are handled. Defaults to [`BatchRequestConfig::Unlimited`].
    pub const fn set_batch_request_config(mut self, config: BatchRequestConfig) -> Self {
        self.0.batch_requests_config = config;
        self
    }

    /// Serves connections on `runtime` instead of the runtime `start` is called on.
    pub fn custom_tokio_runtime(mut self, runtime: Handle) -> Self {
        self.0.tokio_runtime = Some(runtime);
        self
    }

    /// Answers only plain HTTP requests, refusing WebSocket upgrades with `403 Forbidden`.
    pub const fn http_only(mut self) -> Self {
        self.0.enable_http = true;
        self.0.enable_ws = false;
        self
    }

    /// Accepts only WebSocket upgrades, refusing plain HTTP requests with `403 Forbidden`.
    pub const fn ws_only(mut self) -> Self {
        self.0.enable_http = false;
        self.0.enable_ws = true;
        self
    }

    /// Sets how many subscriptions one WebSocket connection may hold. Defaults to 1024.
    pub const fn max_subscriptions_per_connection(mut self, max: u32) -> Self {
        self.0.max_subscriptions_per_connection = max;
        self
    }

    /// Sets how many messages may wait to be sent on one WebSocket connection, which also bounds
    /// the calls it runs at once. Calls past it are answered with `-32009 Server is busy`.
    /// Defaults to 1024.
    pub const fn set_message_buffer_capacity(mut self, capacity: u32) -> Self {
        self.0.message_buffer_capacity = capacity;
        self
    }

    /// Sets how subscription ids are generated. Defaults to [`RandomIntegerIdProvider`].
    pub fn set_id_provider(mut self, id_provider: impl IdProvider + 'static) -> Self {
        self.0.id_provider = Arc::new(id_provider);
        self
    }

    /// Sets how long one WebSocket write may take before the connection is closed, so a client
    /// that stops reading can't hold its messages. Defaults to 10 seconds.
    pub const fn set_ws_write_timeout(mut self, timeout: Duration) -> Self {
        self.0.ws_write_timeout = timeout;
        self
    }

    /// Returns the configuration.
    pub fn build(self) -> ServerConfig {
        self.0
    }
}
