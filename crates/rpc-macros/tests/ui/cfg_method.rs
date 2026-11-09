#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[cfg(any())]
    #[method(name = "m")]
    fn m(&self) -> alloy_rpc_server::RpcResult<()>;
}

fn main() {}
