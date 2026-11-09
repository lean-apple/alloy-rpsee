# alloy-rpc-macros

`#[rpc(server, namespace = "..")]` turns an RPC trait into a `<Trait>Server` trait that
implementations write as usual, plus an `into_rpc` method that registers each method and
subscription on an `alloy-rpc-server` `RpcModule`.

```rust
use alloy_rpc_macros::rpc;
use alloy_rpc_server::RpcResult;

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "chainId")]
    async fn chain_id(&self) -> RpcResult<u64>;

    #[method(name = "getBalance")]
    fn balance(&self, address: String, block: Option<String>) -> RpcResult<u64>;
}

struct Node;

impl EthApiServer for Node {
    async fn chain_id(&self) -> RpcResult<u64> {
        Ok(1)
    }

    fn balance(&self, _address: String, block: Option<String>) -> RpcResult<u64> {
        Ok(if block.is_some() { 0 } else { 100 })
    }
}

#[tokio::main]
async fn main() {
    let module = Node.into_rpc();
    let request = r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["0xab"]}"#;
    assert_eq!(module.raw_json_request(request).await, r#"{"jsonrpc":"2.0","id":1,"result":100}"#);
}
```
