#[alloy_rpc_macros::rpc(server, namespace = "a")]
pub trait Api {
    #[method(name = "m")]
    fn m(&self) -> alloy_rpc_server::RpcResult<()> {
        Ok(())
    }
}

fn main() {}
