#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[method(aliases = ["b"])]
    fn m(&self) -> alloy_rpc_server::RpcResult<()>;
}

fn main() {}
