//! JSON-RPC 2.0 server built on Alloy's JSON-RPC types.

pub use alloy_json_rpc::{ErrorPayload, Id};

mod error;
pub use error::InvalidParams;

mod params;
pub use params::parse_params;

mod request;
pub use request::Request;
