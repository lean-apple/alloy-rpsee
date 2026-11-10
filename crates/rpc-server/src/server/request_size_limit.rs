//! Skipping WebSocket messages that are too large.
//!
//! The WebSocket decoder ends a connection once a message passes its size limit.
//! [`RequestSizeLimit`] sits between the socket and the decoder, discards such a message as it
//! arrives and queues an error for the client, so the decoder never sees it and the connection
//! carries on.

use crate::{MethodResponse, error::ErrorCode};
use alloy_json_rpc::Id;
use std::{
    io::{self, Cursor},
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::protocol::frame::{
    FrameHeader,
    coding::{Data, OpCode},
};

// The most bytes a frame header takes.
const MAX_HEADER_SIZE: usize = 14;

// The most bytes of a skipped message read at once.
const DISCARD_CHUNK_SIZE: usize = 8 * 1024;

// The bits of a header's second byte holding the payload length, or one of the two markers below.
const LENGTH_BITS: u8 = 0x7f;

// The bit of a header's second byte set on a masked frame, whose header ends with a 4 byte key.
const MASK_BIT: u8 = 0x80;

// Marks a payload length held in the next 2 bytes.
const LENGTH_IN_2_BYTES: u8 = 126;

// Marks a payload length held in the next 8 bytes.
const LENGTH_IN_8_BYTES: u8 = 127;

// A socket whose reads leave out the messages that start with a frame longer than the limit.
//
// A message that only passes the limit in a later frame is left to the decoder, which already
// holds its first frames.
pub(crate) struct RequestSizeLimit<Io> {
    // The socket.
    io: Io,
    // The longest payload a message's first frame may have, in bytes.
    max_size: u64,
    // The error queued for the client on each skipped message.
    too_big: String,
    // Queues messages for the connection's writer.
    sender: mpsc::Sender<String>,
    // What the next bytes of the socket are.
    state: State,
    // The current frame's header, of which `filled` bytes are read and `forwarded` handed on.
    header: [u8; MAX_HEADER_SIZE],
    // How many bytes of `header` are read.
    filled: usize,
    // How many bytes of `header` are handed to the decoder.
    forwarded: usize,
    // Whether the rest of a skipped message is still arriving.
    skipping: bool,
}

// What the next bytes of the socket are.
enum State {
    // A frame header.
    Header,
    // This many payload bytes for the decoder, after the bytes of `header` not yet handed on.
    Forward(u64),
    // This many payload bytes of a skipped message.
    Discard(u64),
}

impl<Io> RequestSizeLimit<Io> {
    // Wraps `io`, answering messages over `max_size` bytes on `sender`.
    pub(crate) fn new(io: Io, max_size: u32, sender: mpsc::Sender<String>) -> Self {
        let too_big = ErrorCode::OversizedRequest.exceeded(max_size);
        Self {
            io,
            max_size: max_size.into(),
            too_big: MethodResponse::error(Id::None, too_big).into_json(),
            sender,
            state: State::Header,
            header: [0; MAX_HEADER_SIZE],
            filled: 0,
            forwarded: 0,
            skipping: false,
        }
    }

    // Decides what to do with the frame whose header was just read.
    fn route(&mut self) -> State {
        let read = self.header.get(..self.filled).unwrap_or_default();
        let Ok(Some((header, len))) = FrameHeader::parse(&mut Cursor::new(read)) else {
            // The decoder rejects this header too, which ends the connection.
            return State::Forward(u64::MAX);
        };
        let skip = match header.opcode {
            OpCode::Data(Data::Text | Data::Binary) => len > self.max_size,
            OpCode::Data(Data::Continue) => self.skipping,
            // Control frames belong to no message and may arrive in the middle of one.
            OpCode::Control(_) | OpCode::Data(Data::Reserved(_)) => return State::Forward(len),
        };
        if skip && !self.skipping {
            tracing::debug!(len, max_size = self.max_size, "WebSocket message too large");
            // A full queue drops the answer, as the client isn't reading.
            if self.sender.try_send(self.too_big.clone()).is_err() {
                tracing::debug!("no room to answer an oversized WebSocket message");
            }
        }
        self.skipping = skip && !header.is_final;
        if skip { State::Discard(len) } else { State::Forward(len) }
    }

    // Starts over on the next frame.
    const fn next_frame(&mut self) {
        self.state = State::Header;
        self.filled = 0;
        self.forwarded = 0;
    }
}

impl<Io: AsyncRead + Unpin> AsyncRead for RequestSizeLimit<Io> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            match this.state {
                State::Header => {
                    let wanted = header_len(this.header.get(..this.filled).unwrap_or_default());
                    let missing = this.header.get_mut(this.filled..wanted).unwrap_or_default();
                    if missing.is_empty() {
                        this.state = this.route();
                        continue;
                    }
                    let mut missing = ReadBuf::new(missing);
                    ready!(Pin::new(&mut this.io).poll_read(cx, &mut missing))?;
                    let read = missing.filled().len();
                    if read == 0 {
                        // The socket closed.
                        return Poll::Ready(Ok(()));
                    }
                    this.filled = this.filled.saturating_add(read);
                }
                State::Forward(left) => {
                    let header = this.header.get(this.forwarded..this.filled).unwrap_or_default();
                    if !header.is_empty() {
                        let fitting = header.len().min(buf.remaining());
                        buf.put_slice(header.get(..fitting).unwrap_or_default());
                        this.forwarded = this.forwarded.saturating_add(fitting);
                        return Poll::Ready(Ok(()));
                    }
                    if left == 0 {
                        this.next_frame();
                        continue;
                    }
                    let fitting = usize::try_from(left).unwrap_or(usize::MAX).min(buf.remaining());
                    let mut payload = ReadBuf::new(buf.initialize_unfilled_to(fitting));
                    ready!(Pin::new(&mut this.io).poll_read(cx, &mut payload))?;
                    let read = payload.filled().len();
                    buf.advance(read);
                    this.state = State::Forward(left.saturating_sub(as_u64(read)));
                    // Nothing read means the socket closed, which the decoder sees as well.
                    return Poll::Ready(Ok(()));
                }
                State::Discard(left) => {
                    if left == 0 {
                        this.next_frame();
                        continue;
                    }
                    let mut chunk = [0; DISCARD_CHUNK_SIZE];
                    let fitting = usize::try_from(left).unwrap_or(usize::MAX);
                    let chunk =
                        chunk.get_mut(..fitting.min(DISCARD_CHUNK_SIZE)).unwrap_or_default();
                    let mut discarded = ReadBuf::new(chunk);
                    ready!(Pin::new(&mut this.io).poll_read(cx, &mut discarded))?;
                    let read = discarded.filled().len();
                    if read == 0 {
                        // The socket closed.
                        return Poll::Ready(Ok(()));
                    }
                    this.state = State::Discard(left.saturating_sub(as_u64(read)));
                }
            }
        }
    }
}

impl<Io: AsyncWrite + Unpin> AsyncWrite for RequestSizeLimit<Io> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
}

// How many bytes the header starting with `read` takes, which its second byte tells. See
// https://datatracker.ietf.org/doc/html/rfc6455#section-5.2
fn header_len(read: &[u8]) -> usize {
    let Some(second) = read.get(1) else { return 2 };
    let length = match second & LENGTH_BITS {
        LENGTH_IN_2_BYTES => 2,
        LENGTH_IN_8_BYTES => 8,
        _ => 0,
    };
    let mask = if second & MASK_BIT == 0 { 0 } else { 4 };
    // At most `MAX_HEADER_SIZE`, so this can't overflow.
    2 + length + mask
}

// A byte count as `u64`, which holds any `usize` on supported targets.
fn as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::protocol::frame::Frame;

    // The limit the tests run with.
    const MAX_SIZE: u32 = 64;

    // The answer to a skipped message.
    const OVERSIZED_REQUEST_JSON: &str = r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32007,"message":"Request is too big","data":"Exceeded max limit of 64"}}"#;

    // The bytes of `frame` masked as a client sends it.
    fn masked(mut frame: Frame) -> Vec<u8> {
        frame.header_mut().mask = Some([1, 2, 3, 4]);
        let mut bytes = Vec::new();
        frame.format(&mut bytes).unwrap();
        bytes
    }

    // The first frame of a text message.
    fn text(len: usize, is_final: bool) -> Vec<u8> {
        masked(Frame::message(vec![b'x'; len], OpCode::Data(Data::Text), is_final))
    }

    // A later frame of a message.
    fn continuation(len: usize, is_final: bool) -> Vec<u8> {
        masked(Frame::message(vec![b'x'; len], OpCode::Data(Data::Continue), is_final))
    }

    // A control frame.
    fn ping() -> Vec<u8> {
        masked(Frame::ping(vec![b'x'; 4]))
    }

    // What the decoder reads of `sent` when the socket hands out `step` bytes at a time, and the
    // answers queued.
    async fn filtered(sent: &[Vec<u8>], step: usize) -> (Vec<u8>, Vec<String>) {
        let sent = sent.concat();
        let (mut client, server) = tokio::io::duplex(step);
        let (sender, mut answers) = mpsc::channel(8);
        let mut socket = RequestSizeLimit::new(server, MAX_SIZE, sender);
        let write = async move {
            client.write_all(&sent).await.unwrap();
        };
        let mut read = Vec::new();
        let (_, result) = tokio::join!(write, socket.read_to_end(&mut read));
        result.unwrap();
        drop(socket);
        let mut queued = Vec::new();
        while let Some(answer) = answers.recv().await {
            queued.push(answer);
        }
        (read, queued)
    }

    #[test_case(1 ; "byte by byte")]
    #[test_case(7 ; "split headers")]
    #[test_case(4096 ; "whole frames")]
    #[tokio::test]
    async fn messages_within_the_limit_pass_unchanged(step: usize) {
        let sent = [text(0, true), text(64, true), ping(), text(30, false), continuation(34, true)];
        let (read, answers) = filtered(&sent, step).await;
        assert_eq!(read, sent.concat());
        assert!(answers.is_empty());
    }

    #[test_case(65 ; "short length")]
    #[test_case(126 ; "2 byte length")]
    #[test_case(70_000 ; "8 byte length")]
    #[tokio::test]
    async fn oversized_message_is_skipped_and_answered(len: usize) {
        for step in [7, 4096] {
            let sent = [text(8, true), text(len, true), text(9, true)];
            let (read, answers) = filtered(&sent, step).await;
            assert_eq!(read, [text(8, true), text(9, true)].concat());
            assert_eq!(answers, [OVERSIZED_REQUEST_JSON]);
        }
    }

    #[tokio::test]
    async fn oversized_message_is_skipped_with_its_later_frames() {
        let sent = [
            text(65, false),
            continuation(8, false),
            ping(),
            continuation(8, true),
            text(9, true),
            text(65, true),
        ];
        let (read, answers) = filtered(&sent, 7).await;
        assert_eq!(read, [ping(), text(9, true)].concat());
        assert_eq!(answers, [OVERSIZED_REQUEST_JSON, OVERSIZED_REQUEST_JSON]);
    }

    #[tokio::test]
    async fn message_growing_past_the_limit_is_left_to_the_decoder() {
        let sent = [text(40, false), continuation(65, true)];
        let (read, answers) = filtered(&sent, 7).await;
        assert_eq!(read, sent.concat());
        assert!(answers.is_empty());
    }

    #[tokio::test]
    async fn full_queue_drops_the_answer() {
        let (sender, mut answers) = mpsc::channel(1);
        sender.try_send("queued".to_owned()).unwrap();
        let sent = [text(65, true), text(9, true)].concat();
        let mut socket = RequestSizeLimit::new(sent.as_slice(), MAX_SIZE, sender);
        let mut read = Vec::new();
        socket.read_to_end(&mut read).await.unwrap();
        drop(socket);
        assert_eq!(read, text(9, true));
        assert_eq!(answers.recv().await.as_deref(), Some("queued"));
        assert_eq!(answers.recv().await, None);
    }
}
