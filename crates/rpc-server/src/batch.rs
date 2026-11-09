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

//! Batch requests and their joined response.

use crate::{MethodResponse, Request, error::OversizedBatchResponse};
use alloy_json_rpc::{ErrorPayload, Id};

// Initial capacity of a batch response.
const INITIAL_CAPACITY: usize = 2048;

/// A batch's entries, in request order.
///
/// An entry that isn't a valid request is an `Err`, answered with `-32600 Invalid request`.
#[derive(Debug)]
pub struct Batch<'a> {
    // The entries.
    entries: Vec<Result<BatchEntry<'a>, BatchEntryErr>>,
}

impl<'a> Batch<'a> {
    /// Returns the entries.
    pub fn iter(&self) -> impl Iterator<Item = &Result<BatchEntry<'a>, BatchEntryErr>> {
        self.entries.iter()
    }

    /// Returns the entries mutably, for middleware that rewrites requests.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Result<BatchEntry<'a>, BatchEntryErr>> {
        self.entries.iter_mut()
    }
}

impl<'a> FromIterator<Result<BatchEntry<'a>, BatchEntryErr>> for Batch<'a> {
    fn from_iter<I: IntoIterator<Item = Result<BatchEntry<'a>, BatchEntryErr>>>(iter: I) -> Self {
        Self { entries: iter.into_iter().collect() }
    }
}

impl<'a> IntoIterator for Batch<'a> {
    type Item = Result<BatchEntry<'a>, BatchEntryErr>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

/// A valid request in a batch.
#[derive(Debug)]
pub enum BatchEntry<'a> {
    /// A call, which is answered.
    Call(Request<'a>),
    /// A notification, which isn't.
    Notification(Request<'a>),
}

impl BatchEntry<'_> {
    /// Returns the method the entry calls.
    pub fn method_name(&self) -> &str {
        match self {
            Self::Call(request) | Self::Notification(request) => &request.method,
        }
    }
}

/// A batch entry that isn't a valid request, answered with an error.
#[derive(Debug)]
pub struct BatchEntryErr {
    // The entry's id, or `null` if it has none.
    id: Id,
    // The error to answer with.
    error: ErrorPayload,
}

impl BatchEntryErr {
    /// Creates the error answered to the entry `id`.
    pub const fn new(id: Id, error: ErrorPayload) -> Self {
        Self { id, error }
    }

    /// Takes the error and the entry's id.
    pub fn into_parts(self) -> (ErrorPayload, Id) {
        (self.error, self.id)
    }
}

// Joins a batch's responses into one array, at most `max_response_size` bytes long.
#[derive(Debug)]
pub(crate) struct BatchResponseBuilder {
    // The joined responses so far, each followed by a comma.
    json: String,
    // The most bytes the array may take.
    max_response_size: usize,
}

impl BatchResponseBuilder {
    // An empty array of at most `max_response_size` bytes.
    pub(crate) fn new(max_response_size: usize) -> Self {
        let mut json = String::with_capacity(INITIAL_CAPACITY.min(max_response_size));
        json.push('[');
        Self { json, max_response_size }
    }

    // Adds `response`, or returns `-32011` once the array would pass its limit, so the remaining
    // entries are never run.
    pub(crate) fn append(&mut self, response: MethodResponse) -> Result<(), MethodResponse> {
        let response = response.into_json();
        // The comma after each response becomes the closing `]` after the last one.
        if self.json.len().saturating_add(response.len()).saturating_add(1) > self.max_response_size
        {
            let limit = self.max_response_size;
            return Err(MethodResponse::error(Id::None, OversizedBatchResponse { limit }));
        }
        self.json.push_str(&response);
        self.json.push(',');
        Ok(())
    }

    // Returns `true` if no response was added.
    pub(crate) fn is_empty(&self) -> bool {
        self.json.len() <= 1
    }

    // The joined array, or `-32600 Invalid request` for an empty batch.
    pub(crate) fn finish(mut self) -> MethodResponse {
        if self.is_empty() {
            return MethodResponse::error(Id::None, ErrorPayload::invalid_request());
        }
        self.json.pop();
        self.json.push(']');
        MethodResponse::from_batch(self.json)
    }
}
