//! A node's RPC server: one trait per namespace, merged into one method table, served over HTTP
//! and WebSocket on one port.
//!
//! Run it with `cargo run -p alloy-rpc-server --example node_server`, then call it:
//!
//! ```sh
//! curl -s localhost:8545 -H 'content-type: application/json' \
//!     -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}'
//! ```

use alloy_rpc_macros::rpc;
use alloy_rpc_server::{
    ErrorCode, Methods, PendingSubscriptionSink, RpcResult, SubscriptionMessage,
    SubscriptionResult,
    server::{ServerBuilder, ServerConfig},
};
use std::time::Duration;

/// The `eth` namespace.
#[rpc(server, namespace = "eth")]
pub trait EthApi {
    /// Returns the chain id.
    #[method(name = "chainId")]
    fn chain_id(&self) -> RpcResult<String>;

    /// Returns the latest block number.
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<String>;

    /// Sends a block number every second, the way `newHeads` sends headers.
    #[subscription(name = "subscribe" => "subscription", unsubscribe = "unsubscribe", item = String)]
    async fn subscribe(&self, kind: String) -> SubscriptionResult;
}

/// The `web3` namespace.
#[rpc(server, namespace = "web3")]
pub trait Web3Api {
    /// Returns the client version.
    #[method(name = "clientVersion")]
    fn client_version(&self) -> RpcResult<String>;
}

/// The node every namespace reads from.
struct Node;

impl EthApiServer for Node {
    fn chain_id(&self) -> RpcResult<String> {
        Ok("0x1".to_owned())
    }

    async fn block_number(&self) -> RpcResult<String> {
        Ok("0x10".to_owned())
    }

    async fn subscribe(
        &self,
        pending: PendingSubscriptionSink,
        kind: String,
    ) -> SubscriptionResult {
        if kind != "newHeads" {
            pending.reject(ErrorCode::InvalidParams).await;
            return Ok(());
        }
        let sink = pending.accept().await?;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        for number in 0u64.. {
            tokio::select! {
                () = sink.closed() => break,
                _ = interval.tick() => {
                    let head = format!("{number:#x}");
                    sink.send(SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &head)?)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

impl Web3ApiServer for Node {
    fn client_version(&self) -> RpcResult<String> {
        Ok("alloy-rpsee/example".to_owned())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One module per namespace, merged into the table the server answers from.
    let mut methods = Methods::new();
    methods.merge(EthApiServer::into_rpc(Node))?;
    methods.merge(Web3ApiServer::into_rpc(Node))?;

    // Limits sized for a public node.
    let config = ServerConfig::builder()
        .max_connections(500)
        .max_request_body_size(15 * 1024 * 1024)
        .max_response_body_size(160 * 1024 * 1024)
        .max_subscriptions_per_connection(1024)
        .build();
    let server = ServerBuilder::new().set_config(config).build("127.0.0.1:8545").await?;
    println!("listening on {}", server.local_addr()?);
    let handle = server.start(methods);

    tokio::signal::ctrl_c().await?;
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
