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

//! Building, binding and running a server.

use super::{
    ServerConfig, ServerHandle,
    http::{self, BoxError, HttpBody, HttpRequest, HttpResponse},
    ws,
};
use crate::{Connection, ConnectionId, Methods, RpcService, RpcServiceT};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use http_body::Body;
use hyper::{StatusCode, body::Incoming};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::{conn::auto, graceful::GracefulShutdown},
    service::TowerToHyperService,
};
use std::{
    convert::Infallible,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::{TcpListener, ToSocketAddrs},
    sync::{Semaphore, mpsc},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::{Layer, Service, ServiceBuilder, ServiceExt, layer::util::Identity};

// How long to wait after a failed accept, so a full file table doesn't spin the accept loop.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(50);

/// Layers of RPC middleware, each wrapping an [`RpcServiceT`].
pub type RpcServiceBuilder<L = Identity> = ServiceBuilder<L>;

/// Builds a [`Server`] from a configuration and its middleware.
///
/// # Examples
///
/// ```
/// use alloy_rpc_server::{
///     ErrorPayload, RpcModule,
///     server::{ServerBuilder, ServerConfig},
/// };
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> std::io::Result<()> {
/// let mut module = RpcModule::new(());
/// module.register_method("eth_chainId", |(): (), _| Ok::<_, ErrorPayload>("0x1")).unwrap();
///
/// let config = ServerConfig::builder().max_connections(10).build();
/// let server = ServerBuilder::new().set_config(config).build("127.0.0.1:0").await?;
/// let addr = server.local_addr()?;
/// let handle = server.start(module);
///
/// handle.stop().unwrap();
/// handle.stopped().await;
/// # let _ = addr;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ServerBuilder<HttpMiddleware = Identity, RpcMiddleware = Identity> {
    // The limits and runtime.
    config: ServerConfig,
    // Layers wrapping each HTTP request.
    http_middleware: ServiceBuilder<HttpMiddleware>,
    // Layers wrapping each JSON-RPC call.
    rpc_middleware: RpcServiceBuilder<RpcMiddleware>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            config: ServerConfig::default(),
            http_middleware: ServiceBuilder::new(),
            rpc_middleware: RpcServiceBuilder::new(),
        }
    }
}

impl ServerBuilder {
    /// Returns a builder with the default configuration and no middleware.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<HttpMiddleware, RpcMiddleware> ServerBuilder<HttpMiddleware, RpcMiddleware> {
    /// Sets the configuration.
    pub fn set_config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the tower layers that wrap each HTTP request.
    pub fn set_http_middleware<T>(
        self,
        http_middleware: ServiceBuilder<T>,
    ) -> ServerBuilder<T, RpcMiddleware> {
        let Self { config, rpc_middleware, .. } = self;
        ServerBuilder { config, http_middleware, rpc_middleware }
    }

    /// Sets the layers that wrap each JSON-RPC call, including each entry of a batch.
    pub fn set_rpc_middleware<T>(
        self,
        rpc_middleware: RpcServiceBuilder<T>,
    ) -> ServerBuilder<HttpMiddleware, T> {
        let Self { config, http_middleware, .. } = self;
        ServerBuilder { config, http_middleware, rpc_middleware }
    }

    /// Binds the server to `addr`.
    ///
    /// # Errors
    ///
    /// Returns the error if the address can't be bound.
    pub async fn build(
        self,
        addr: impl ToSocketAddrs,
    ) -> io::Result<Server<HttpMiddleware, RpcMiddleware>> {
        Ok(Server { listener: TcpListener::bind(addr).await?, builder: self })
    }
}

/// A server bound to an address, ready to [`start`](Self::start).
#[derive(Debug)]
pub struct Server<HttpMiddleware = Identity, RpcMiddleware = Identity> {
    // The bound listener.
    listener: TcpListener,
    // The configuration and middleware.
    builder: ServerBuilder<HttpMiddleware, RpcMiddleware>,
}

impl Server {
    /// Returns a builder with the default configuration and no middleware.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::new()
    }
}

impl<HttpMiddleware, RpcMiddleware> Server<HttpMiddleware, RpcMiddleware> {
    /// Returns the address the server is bound to.
    ///
    /// # Errors
    ///
    /// Returns the error if the socket can't report its address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl<HttpMiddleware, RpcMiddleware, S, B> Server<HttpMiddleware, RpcMiddleware>
where
    RpcMiddleware: Layer<RpcService> + Send + Sync + 'static,
    RpcMiddleware::Service: RpcServiceT + Send + Sync,
    HttpMiddleware: Layer<HttpService<RpcMiddleware>, Service = S> + Send + 'static,
    S: Service<HttpRequest, Response = HttpResponse<B>> + Clone + Send + 'static,
    S::Error: Into<BoxError>,
    S::Future: Send,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    /// Serves `methods` until the returned handle stops the server.
    pub fn start(self, methods: impl Into<Methods>) -> ServerHandle {
        let (stop, tasks) = (CancellationToken::new(), TaskTracker::new());
        let handle = ServerHandle::new(stop.clone(), tasks.clone());
        let runtime = self.builder.config.tokio_runtime.clone();
        let serve = self.serve(methods.into(), stop, tasks.clone());
        match runtime {
            Some(runtime) => drop(tasks.spawn_on(serve, &runtime)),
            None => drop(tasks.spawn(serve)),
        }
        handle
    }

    // Accepts connections until `stop`, then waits for their requests to finish.
    async fn serve(self, methods: Methods, stop: CancellationToken, tasks: TaskTracker) {
        let Self { listener, builder: ServerBuilder { config, http_middleware, rpc_middleware } } =
            self;
        let connections =
            Arc::new(Semaphore::new(usize::try_from(config.max_connections).unwrap_or(usize::MAX)));
        let shared = Arc::new(Shared {
            methods,
            config,
            rpc_middleware,
            connections,
            next_connection_id: AtomicUsize::new(0),
            stop: stop.clone(),
            tasks: tasks.clone(),
        });
        let mut hyper = auto::Builder::new(TokioExecutor::new());
        // A timer turns on hyper's header read timeout, which closes connections that send no
        // request.
        hyper.http1().timer(TokioTimer::new());
        let graceful = GracefulShutdown::new();
        while let Some(accepted) = stop.run_until_cancelled(listener.accept()).await {
            let socket = match accepted {
                Ok((socket, _)) => socket,
                Err(err) => {
                    tracing::debug!(%err, "failed to accept a connection");
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                    continue;
                }
            };
            if let Err(err) = socket.set_nodelay(true) {
                tracing::debug!(%err, "failed to set TCP_NODELAY");
            }
            let service = http_middleware.service(HttpService { shared: shared.clone() });
            let service =
                service.map_request(|request: hyper::Request<Incoming>| request.map(HttpBody::new));
            let connection = hyper.serve_connection_with_upgrades(
                TokioIo::new(socket),
                TowerToHyperService::new(service),
            );
            let connection = graceful.watch(connection.into_owned());
            tasks.spawn(async move {
                if let Err(err) = connection.await {
                    tracing::debug!(%err, "HTTP connection failed");
                }
            });
        }
        drop(listener);
        graceful.shutdown().await;
    }
}

// What every request of a server shares.
#[derive(Debug)]
struct Shared<RpcMiddleware> {
    // The methods calls are dispatched to.
    methods: Methods,
    // The limits.
    config: ServerConfig,
    // Layers wrapping each JSON-RPC call.
    rpc_middleware: RpcServiceBuilder<RpcMiddleware>,
    // One permit per HTTP request in flight or open WebSocket connection.
    connections: Arc<Semaphore>,
    // The id the next WebSocket connection gets.
    next_connection_id: AtomicUsize,
    // Cancelled when the server stops.
    stop: CancellationToken,
    // The server's connections, which stopping waits for.
    tasks: TaskTracker,
}

/// The service answering HTTP requests, which the HTTP middleware wraps.
#[derive(Debug)]
pub struct HttpService<RpcMiddleware> {
    // What every request shares.
    shared: Arc<Shared<RpcMiddleware>>,
}

impl<RpcMiddleware> Clone for HttpService<RpcMiddleware> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone() }
    }
}

impl<RpcMiddleware> Service<HttpRequest> for HttpService<RpcMiddleware>
where
    RpcMiddleware: Layer<RpcService> + Send + Sync + 'static,
    RpcMiddleware::Service: RpcServiceT + Send + Sync,
{
    type Response = HttpResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<HttpResponse, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        let shared = self.shared.clone();
        // The permit is held until the response is ready, so it bounds the requests in flight.
        let Ok(permit) = shared.connections.clone().try_acquire_owned() else {
            let busy = "Too many connections. Please try again later.";
            return Box::pin(async move { Ok(http::text(StatusCode::TOO_MANY_REQUESTS, busy)) });
        };
        let config = &shared.config;
        let max_response_size =
            usize::try_from(config.max_response_body_size).unwrap_or(usize::MAX);
        if ws::is_upgrade_request(request.headers()) {
            if !config.enable_ws {
                return Box::pin(async { Ok(http::empty(StatusCode::FORBIDDEN)) });
            }
            let capacity = usize::try_from(config.message_buffer_capacity).unwrap_or(usize::MAX);
            let (sender, outgoing) = mpsc::channel(capacity);
            let id = ConnectionId(shared.next_connection_id.fetch_add(1, Ordering::Relaxed));
            let connection = Connection::new(
                id,
                sender.clone(),
                config.max_subscriptions_per_connection,
                config.id_provider.clone(),
            );
            let rpc_service = shared.rpc_middleware.service(RpcService::with_connection(
                shared.methods.clone(),
                max_response_size,
                connection,
            ));
            let ws = ws::WsConnection {
                id,
                rpc_service,
                sender,
                outgoing,
                config: config.clone(),
                stop: shared.stop.clone(),
                permit,
            };
            let response = ws.upgrade(request, &shared.tasks);
            return Box::pin(async { Ok(response) });
        }
        if !config.enable_http {
            return Box::pin(async { Ok(http::empty(StatusCode::FORBIDDEN)) });
        }
        Box::pin(async move {
            let ServerConfig { max_request_body_size, batch_requests_config, .. } = shared.config;
            let rpc_service = shared
                .rpc_middleware
                .service(RpcService::new(shared.methods.clone(), max_response_size));
            let response = http::call_with_service(
                request,
                batch_requests_config,
                max_request_body_size,
                &rpc_service,
            )
            .await;
            drop(permit);
            Ok(response)
        })
    }
}
