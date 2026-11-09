#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[method(name = "unsubscribe")]
    fn m(&self) -> alloy_rpc_server::RpcResult<()>;

    #[subscription(name = "subscribe", unsubscribe = "unsubscribe", item = u64)]
    async fn s(&self) -> alloy_rpc_server::SubscriptionResult;
}

fn main() {}
