#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[subscription(name = "subscribe", unsubscribe = "unsubscribe", item = u64)]
    fn s(&self) -> alloy_rpc_server::SubscriptionResult;
}

fn main() {}
