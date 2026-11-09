# alloy-rpc-server

A JSON-RPC 2.0 server built on Alloy's JSON-RPC types. Register methods on an `RpcModule`, then
serve them over HTTP and WebSocket with `ServerBuilder`, or answer requests directly with
`handle_rpc_call`.

```rust
use alloy_rpc_server::{
    ErrorPayload, RpcModule,
    server::{ServerBuilder, ServerConfig},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut module = RpcModule::new(());
    module.register_method("eth_chainId", |(): (), _| Ok::<_, ErrorPayload>("0x1"))?;

    let config = ServerConfig::builder().max_connections(100).build();
    let server = ServerBuilder::new().set_config(config).build("127.0.0.1:0").await?;
    let handle = server.start(module);

    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
```
