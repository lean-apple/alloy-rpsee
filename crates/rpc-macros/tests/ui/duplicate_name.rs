#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[method(name = "m")]
    fn m(&self) -> alloy_rpc_server::RpcResult<()>;

    #[method(name = "n", aliases = ["a_m"])]
    fn n(&self) -> alloy_rpc_server::RpcResult<()>;
}

fn main() {}
