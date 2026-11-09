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

use crate::BatchRequestConfig;
use tokio::runtime::Handle;

/// The default request and response size limit, 10 MiB.
pub const TEN_MB_SIZE_BYTES: u32 = 10 * 1024 * 1024;

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
            max_request_body_size: TEN_MB_SIZE_BYTES,
            max_response_body_size: TEN_MB_SIZE_BYTES,
            max_connections: 100,
            batch_requests_config: BatchRequestConfig::Unlimited,
            tokio_runtime: None,
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

    /// Returns the configuration.
    pub fn build(self) -> ServerConfig {
        self.0
    }
}
