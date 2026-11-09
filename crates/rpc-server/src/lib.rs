//! JSON-RPC 2.0 server built on Alloy's JSON-RPC types.

pub use alloy_json_rpc::{ErrorPayload, Id};

mod batch;
pub use batch::{Batch, BatchEntry, BatchEntryErr};

mod error;
pub use error::{InvalidParams, RegisterMethodError};

mod methods;
pub use methods::{MethodCallback, Methods, ResponseFuture};

mod params;
pub use params::parse_params;

mod request;
pub use request::Request;

mod response;
pub use response::MethodResponse;

mod rpc_module;
pub use rpc_module::RpcModule;

mod service;
pub use service::{BatchRequestConfig, RpcService, RpcServiceT, handle_rpc_call};

mod writer;
