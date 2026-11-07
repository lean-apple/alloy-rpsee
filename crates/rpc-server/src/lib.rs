//! JSON-RPC 2.0 server built on Alloy's JSON-RPC types.

pub use alloy_json_rpc::Id;

mod error;

mod params;
pub use params::parse_params;

mod request;
pub use request::Request;
