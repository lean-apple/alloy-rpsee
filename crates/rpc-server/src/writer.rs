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

//! A size-limited buffer, derived from jsonrpsee's `BoundedWriter`.

use std::io;

// Initial buffer capacity. Growing from it costs nothing measurable next to serializing a large
// result.
const INITIAL_CAPACITY: usize = 128;

// A buffer that fails writes past `max_len`, so an oversized response stops serializing.
pub(crate) struct BoundedWriter {
    // The serialized response so far.
    buf: Vec<u8>,
    // The most bytes `buf` may hold.
    max_len: usize,
}

impl BoundedWriter {
    // An empty writer that holds at most `max_len` bytes.
    pub(crate) fn new(max_len: usize) -> Self {
        Self { buf: Vec::with_capacity(INITIAL_CAPACITY.min(max_len)), max_len }
    }

    // Takes the bytes written so far.
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

impl io::Write for &mut BoundedWriter {
    // serde_json writes a response in many small pieces, so each write must stay a check and a
    // copy.
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_all(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        // `self.buf` never grows past `max_len`, so this can't underflow.
        if buf.len() > self.max_len - self.buf.len() {
            return Err(io::Error::from(io::ErrorKind::OutOfMemory));
        }
        self.buf.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
