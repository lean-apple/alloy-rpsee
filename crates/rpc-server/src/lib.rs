//! JSON-RPC 2.0 server built on Alloy's JSON-RPC types.

pub use alloy_json_rpc::Id;

mod request;
pub use request::Request;
