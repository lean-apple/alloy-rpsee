#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[subscription(name = "subscribe", item = u64)]
    fn s(&self) -> alloy_rpc_server::RpcResult<()>;
}

fn main() {}
