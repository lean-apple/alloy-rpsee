// Copyright 2019-2021 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Answering one HTTP request.
//!
//! A request is a `POST` with a JSON content type carrying one JSON-RPC message or a batch.
//! Anything else gets an HTTP error: `405` for other methods, `415` for other content types,
//! `413` past the size limit and `400` for a body that isn't JSON.

use super::split_message;
use crate::{BatchRequestConfig, MethodResponse, RpcServiceT, error::ErrorCode, handle_rpc_call};
use alloy_json_rpc::{ErrorPayload, Id};
use bytes::Bytes;
use http::{HeaderValue, Method, StatusCode, header};
use http_body::{Body, Frame, SizeHint};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited, combinators::UnsyncBoxBody};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pub use tower::BoxError;

/// An HTTP request as the server and its HTTP middleware see it.
pub type HttpRequest<B = HttpBody> = http::Request<B>;

/// An HTTP response as the server and its HTTP middleware produce it.
pub type HttpResponse<B = HttpBody> = http::Response<B>;

// The content type of JSON responses.
const JSON: &str = "application/json; charset=utf-8";

// The content type of the plain-text errors.
const TEXT: &str = "text/plain";

// The content types a JSON-RPC request may carry.
const JSON_CONTENT_TYPES: [&str; 6] = [
    "application/json",
    "application/json; charset=utf-8",
    "application/json;charset=utf-8",
    "application/json-rpc",
    "application/json-rpc; charset=utf-8",
    "application/json-rpc;charset=utf-8",
];

/// Answers one HTTP request through `rpc_service`, reading at most `max_request_size` bytes of
/// its body.
pub async fn call_with_service<S: RpcServiceT>(
    request: HttpRequest,
    batch_config: BatchRequestConfig,
    max_request_size: u32,
    rpc_service: &S,
) -> HttpResponse {
    if request.method() != Method::POST {
        return text(
            StatusCode::METHOD_NOT_ALLOWED,
            "Used HTTP Method is not allowed. POST is required\n",
        );
    }
    if !is_json(request.headers().get(header::CONTENT_TYPE)) {
        return text(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Supplied content type is not allowed. Content-Type: application/json is required\n",
        );
    }
    let limit = usize::try_from(max_request_size).unwrap_or(usize::MAX);
    let body = request.into_body();
    // A declared length past the limit is refused before reading anything.
    if usize::try_from(body.size_hint().lower()).unwrap_or(usize::MAX) > limit {
        return too_large(max_request_size);
    }
    let body = match Limited::new(body, limit).collect().await {
        Ok(body) => body.to_bytes(),
        Err(err) if err.is::<LengthLimitError>() => return too_large(max_request_size),
        Err(err) => {
            tracing::debug!(%err, "failed to read an HTTP request body");
            return error(StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::InternalError.into());
        }
    };
    let Some((message, is_single)) = split_message(&body) else {
        return error(StatusCode::BAD_REQUEST, ErrorCode::ParseError.into());
    };
    let response = handle_rpc_call(message, is_single, batch_config, rpc_service).await;
    // A notification is acknowledged with an empty body.
    json(StatusCode::OK, response.into_json())
}

/// The body of the server's HTTP requests and responses, which HTTP middleware may replace.
#[derive(Debug, Default)]
pub struct HttpBody(UnsyncBoxBody<Bytes, BoxError>);

impl HttpBody {
    /// Boxes `body`.
    pub fn new<B>(body: B) -> Self
    where
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        Self(body.map_err(Into::into).boxed_unsync())
    }
}

impl<T> From<T> for HttpBody
where
    Full<Bytes>: From<T>,
{
    fn from(body: T) -> Self {
        Self::new(Full::from(body))
    }
}

impl Body for HttpBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

// Returns `true` for the content types JSON-RPC clients send.
fn is_json(content_type: Option<&HeaderValue>) -> bool {
    content_type
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| JSON_CONTENT_TYPES.iter().any(|json| value.eq_ignore_ascii_case(json)))
}

// `413 Payload Too Large`, with `-32007` naming the limit.
fn too_large(limit: u32) -> HttpResponse {
    error(StatusCode::PAYLOAD_TOO_LARGE, ErrorCode::OversizedRequest.exceeded(limit))
}

// A JSON-RPC error with a `null` id, as `status`.
fn error(status: StatusCode, err: ErrorPayload) -> HttpResponse {
    json(status, MethodResponse::error(Id::None, err).into_json())
}

// A JSON body.
fn json(status: StatusCode, body: String) -> HttpResponse {
    response(status, Full::from(body), JSON)
}

// A response without a body.
pub(crate) fn empty(status: StatusCode) -> HttpResponse {
    let mut response = HttpResponse::new(HttpBody::default());
    *response.status_mut() = status;
    response
}

// A plain-text body.
pub(crate) fn text(status: StatusCode, body: &'static str) -> HttpResponse {
    response(status, Full::from(body), TEXT)
}

// A response with `body` of type `content_type`.
fn response(status: StatusCode, body: Full<Bytes>, content_type: &'static str) -> HttpResponse {
    let mut response = HttpResponse::new(HttpBody::new(body));
    *response.status_mut() = status;
    response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn body_from_text_and_bytes() {
        for body in [HttpBody::from("hello"), HttpBody::from(Bytes::from_static(b"hello"))] {
            assert_eq!(body.collect().await.unwrap().to_bytes(), "hello");
        }
    }
}
