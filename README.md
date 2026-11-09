# alloy-rpsee

A JSON-RPC server for Ethereum nodes, built on [alloy](https://github.com/alloy-rs/alloy).

It proposes an alternative to [jsonrpsee](https://github.com/paritytech/jsonrpsee) with a close
API (`#[rpc]` traits, `RpcModule`, `ServerBuilder`, subscriptions and middleware), so a node can
move over one namespace at a time, while the types underneath are alloy's. It is being tested in
[reth](https://github.com/paradigmxyz/reth).

## what it adds to alloy

alloy's RPC crates are client-side. These crates add the server half:

- **Requests:** parsing that tells calls from notifications, and positional params decoded with
  serde, where trailing `Option` arguments may be left out.
- **Methods:** `RpcModule` for sync, async and blocking methods, and dispatch of single calls and
  batches through middleware.
- **Responses:** serialized once, and stopped as soon as they pass the size limit.
- **Subscriptions:** sinks with a bounded queue per connection and a subscription limit.
- **Server:** HTTP and WebSocket on one port, built on hyper and tower.
- **Macros:** `#[rpc]` turns a trait into a server.
- **Errors:** every error code the server sends, with its message, in one `ErrorCode` table.

## crates

- [`alloy-rpc-server`](crates/rpc-server): methods, dispatch, subscriptions, and the HTTP and
  WebSocket server.
- [`alloy-rpc-macros`](crates/rpc-macros): the `#[rpc]` attribute.

## perf

Where it can improve on jsonrpsee:

- **Params:** decoded in one serde pass over the array, instead of a new parser per argument.
- **Responses:** serialized once into the buffer that is sent, where upstream jsonrpsee parses it
  again.
- **Bounds:** the calls running on one WebSocket connection, panics in sync methods, and how much
  of a bad request is echoed back in an error.
- **Size:** less code to maintain, since the types come from alloy.

## example

[`node_server`](crates/rpc-server/examples/node_server.rs) sets up a node's RPC server: one trait
per namespace, merged into one method table, served over HTTP and WebSocket.

```sh
cargo run -p alloy-rpc-server --example node_server
```
