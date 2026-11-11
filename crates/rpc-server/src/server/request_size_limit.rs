//! Skipping WebSocket messages that are too large.
//!
//! The WebSocket decoder ends a connection once a message passes its size limit.
//! [`RequestSizeLimit`] sits between the socket and the decoder, discards such a message as it
//! arrives and queues an error for the client, so the decoder never sees it and the connection
//! carries on. A skipped frame is still held to the checks the decoder makes on the frames it
//! reads.

use crate::{
    MethodResponse,
    error::ErrorCode,
    utils::{saturating_u64, saturating_usize},
};
use alloy_json_rpc::Id;
use std::{
    io::{self, Cursor},
    ops::ControlFlow,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncBufRead, AsyncRead, AsyncWrite, BufReader, ReadBuf},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{
    error::ProtocolError,
    protocol::frame::{
        FrameHeader,
        coding::{Data, OpCode},
    },
};

// The most bytes a frame header takes.
const MAX_HEADER_SIZE: usize = 14;

// How many bytes are read from the socket at once, held per connection.
const READ_BUFFER_SIZE: usize = 8 * 1024;

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
    // The socket, read through a buffer so that reading a header apart from its payload costs no
    // read of its own.
    io: BufReader<Io>,
    // The longest payload a message's first frame may have, in bytes.
    max_size: u64,
    // The error queued for the client on each skipped message.
    too_big: String,
    // Queues messages for the connection's writer.
    sender: mpsc::Sender<String>,
    // What the next bytes of the socket are.
    state: State,
    // The header of a frame whose header arrived in pieces, of which `filled` bytes are read and
    // `forwarded` handed on. A header read whole is used where it lies in `io`.
    header: [u8; MAX_HEADER_SIZE],
    // How many bytes of `header` are read.
    filled: usize,
    // How many bytes of `header` are handed to the decoder.
    forwarded: usize,
    // The message whose later frames are still arriving, if any.
    unfinished: Unfinished,
}

// How one step of a read ends: whether the read carries on, or hands the decoder what it has.
type Step = Poll<io::Result<ControlFlow<()>>>;

// The read carries on with the next step.
const CARRY_ON: Step = Poll::Ready(Ok(ControlFlow::Continue(())));

// The read ends, and the decoder sees what was read so far.
const HAND_OVER: Step = Poll::Ready(Ok(ControlFlow::Break(())));

// What became of a message whose later frames are still arriving.
enum Unfinished {
    // No message is unfinished.
    Nothing,
    // Its frames go to the decoder.
    Forwarded,
    // Its frames are skipped.
    Skipped,
}

// What the next bytes of the socket are.
enum State {
    // A frame header.
    Header,
    // This many bytes of a frame for the decoder, after the bytes of `header` not yet handed on.
    Forward(u64),
    // This many bytes of a skipped frame.
    Discard(u64),
}

impl<Io: AsyncRead> RequestSizeLimit<Io> {
    // Wraps `io`, answering messages over `max_size` bytes on `sender`.
    pub(crate) fn new(io: Io, max_size: u32, sender: mpsc::Sender<String>) -> Self {
        let too_big = ErrorCode::OversizedRequest.exceeded(max_size);
        Self {
            io: BufReader::with_capacity(READ_BUFFER_SIZE, io),
            max_size: max_size.into(),
            too_big: MethodResponse::error(Id::None, too_big).into_json(),
            sender,
            state: State::Header,
            header: [0; MAX_HEADER_SIZE],
            filled: 0,
            forwarded: 0,
            unfinished: Unfinished::Nothing,
        }
    }

    // Returns `true` if the frame with `header` and a payload of `len` bytes is skipped.
    //
    // # Errors
    //
    // Returns the error the decoder would raise on a skipped frame, which it never reads.
    fn skips(&mut self, header: &FrameHeader, len: u64) -> Result<bool, ProtocolError> {
        let skip = match (header.opcode, &self.unfinished) {
            (OpCode::Data(Data::Text | Data::Binary), Unfinished::Nothing) => len > self.max_size,
            // The decoder rejects a message that starts inside the one it holds.
            (OpCode::Data(Data::Text | Data::Binary), Unfinished::Forwarded) => false,
            (OpCode::Data(data @ (Data::Text | Data::Binary)), Unfinished::Skipped) => {
                return Err(ProtocolError::ExpectedFragment(data));
            }
            (OpCode::Data(Data::Continue), unfinished) => matches!(unfinished, Unfinished::Skipped),
            // Control frames belong to no message and may arrive in the middle of one.
            (OpCode::Control(_) | OpCode::Data(Data::Reserved(_)), _) => return Ok(false),
        };
        if skip {
            if header.mask.is_none() {
                return Err(ProtocolError::UnmaskedFrameFromClient);
            }
            if header.rsv1 || header.rsv2 || header.rsv3 {
                return Err(ProtocolError::NonZeroReservedBits);
            }
            if matches!(self.unfinished, Unfinished::Nothing) {
                tracing::debug!(len, max_size = self.max_size, "WebSocket message too large");
                // A full queue drops the answer, as the client isn't reading.
                if self.sender.try_send(self.too_big.clone()).is_err() {
                    tracing::debug!("no room to answer an oversized WebSocket message");
                }
            }
        }
        self.unfinished = match (header.is_final, skip) {
            (true, _) => Unfinished::Nothing,
            (false, true) => Unfinished::Skipped,
            (false, false) => Unfinished::Forwarded,
        };
        Ok(skip)
    }

    // Decides what to do with the `len` bytes that follow the header in `read`, which holds
    // `header_size` bytes not handed on yet when it is parsed where it lies in `io`.
    //
    // # Errors
    //
    // Returns the error of [`Self::skips`].
    fn route(&mut self, parsed: Option<(FrameHeader, u64)>, header_size: u64) -> io::Result<State> {
        let Some((header, len)) = parsed else {
            // The decoder rejects this header too, which ends the connection.
            return Ok(State::Forward(u64::MAX));
        };
        let skip = self
            .skips(&header, len)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let left = header_size.saturating_add(len);
        Ok(if skip { State::Discard(left) } else { State::Forward(left) })
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
        let already_filled = buf.filled().len();
        while buf.remaining() > 0 {
            // Once the decoder has bytes, only the frames already read from the socket are added,
            // so it never waits on the socket for more.
            if this.io.buffer().is_empty() && buf.filled().len() > already_filled {
                break;
            }
            let step = match this.state {
                State::Header => this.poll_header(cx),
                State::Forward(left) => this.poll_forward(cx, buf, left),
                State::Discard(left) => this.poll_discard(cx, left),
            };
            if ready!(step)?.is_break() {
                break;
            }
        }
        Poll::Ready(Ok(()))
    }
}

// The steps of a read, one per `State`. Each breaks when the decoder has to see what was read so
// far, which is nothing once the socket closed.
impl<Io: AsyncRead + Unpin> RequestSizeLimit<Io> {
    // Routes the next frame by its header.
    fn poll_header(&mut self, cx: &mut Context<'_>) -> Step {
        if self.filled > 0 {
            return self.poll_split_header(cx);
        }
        if self.io.buffer().is_empty()
            && ready!(Pin::new(&mut self.io).poll_fill_buf(cx))?.is_empty()
        {
            // The socket closed.
            return HAND_OVER;
        }
        // A header read whole is parsed where it lies and handed on, or skipped, with its payload.
        let mut buffered = Cursor::new(self.io.buffer());
        match FrameHeader::parse(&mut buffered) {
            Ok(Some(parsed)) => {
                let header_size = buffered.position();
                self.state = self.route(Some(parsed), header_size)?;
            }
            Err(_) => self.state = self.route(None, 0)?,
            // The rest of the header is still on the socket.
            Ok(None) => return self.poll_split_header(cx),
        }
        CARRY_ON
    }

    // Copies a header that arrives in pieces into `header`, then routes its frame.
    fn poll_split_header(&mut self, cx: &mut Context<'_>) -> Step {
        let wanted = header_len(self.header.get(..self.filled).unwrap_or_default());
        let missing = self.header.get_mut(self.filled..wanted).unwrap_or_default();
        if missing.is_empty() {
            let read = self.header.get(..self.filled).unwrap_or_default();
            let parsed = FrameHeader::parse(&mut Cursor::new(read)).ok().flatten();
            self.state = self.route(parsed, 0)?;
            return CARRY_ON;
        }
        let mut missing = ReadBuf::new(missing);
        ready!(Pin::new(&mut self.io).poll_read(cx, &mut missing))?;
        let read = missing.filled().len();
        if read == 0 {
            // The socket closed.
            return HAND_OVER;
        }
        self.filled = self.filled.saturating_add(read);
        CARRY_ON
    }

    // Hands the decoder up to `left` bytes of the current frame, after its split header.
    fn poll_forward(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>, left: u64) -> Step {
        let header = self.header.get(self.forwarded..self.filled).unwrap_or_default();
        if !header.is_empty() {
            let fitting = header.len().min(buf.remaining());
            buf.put_slice(header.get(..fitting).unwrap_or_default());
            self.forwarded = self.forwarded.saturating_add(fitting);
            return CARRY_ON;
        }
        if left == 0 {
            self.next_frame();
            return CARRY_ON;
        }
        let fitting = saturating_usize(left).min(buf.remaining());
        let buffered = self.io.buffer();
        if buffered.is_empty() {
            // A payload still on the socket is read straight into the decoder's buffer.
            let mut payload = ReadBuf::new(buf.initialize_unfilled_to(fitting));
            ready!(Pin::new(&mut self.io).poll_read(cx, &mut payload))?;
            let read = payload.filled().len();
            buf.advance(read);
            self.state = State::Forward(left.saturating_sub(saturating_u64(read)));
            // Nothing read means the socket closed, which the decoder sees as well.
            return HAND_OVER;
        }
        let fitting = fitting.min(buffered.len());
        buf.put_slice(buffered.get(..fitting).unwrap_or_default());
        Pin::new(&mut self.io).consume(fitting);
        self.state = State::Forward(left.saturating_sub(saturating_u64(fitting)));
        CARRY_ON
    }

    // Drops up to `left` bytes of a skipped frame.
    fn poll_discard(&mut self, cx: &mut Context<'_>, left: u64) -> Step {
        if left == 0 {
            self.next_frame();
            return CARRY_ON;
        }
        let buffered = ready!(Pin::new(&mut self.io).poll_fill_buf(cx))?.len();
        if buffered == 0 {
            // The socket closed.
            return HAND_OVER;
        }
        let read = saturating_usize(left).min(buffered);
        Pin::new(&mut self.io).consume(read);
        self.state = State::Discard(left.saturating_sub(saturating_u64(read)));
        CARRY_ON
    }
}

impl<Io: AsyncRead + AsyncWrite + Unpin> AsyncWrite for RequestSizeLimit<Io> {
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

    // A text message sent without a mask, which a client may not do.
    fn unmasked(len: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        Frame::message(vec![b'x'; len], OpCode::Data(Data::Text), true).format(&mut bytes).unwrap();
        bytes
    }

    // A text message with a reserved bit set, which no extension here allows.
    fn reserved(len: usize) -> Vec<u8> {
        let mut frame = Frame::message(vec![b'x'; len], OpCode::Data(Data::Text), true);
        frame.header_mut().rsv1 = true;
        masked(frame)
    }

    // A control frame.
    fn ping() -> Vec<u8> {
        masked(Frame::ping(vec![b'x'; 4]))
    }

    // A socket holding `unread` that counts the reads made on it.
    struct Counted<'a> {
        unread: &'a [u8],
        reads: usize,
    }

    impl AsyncRead for Counted<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.reads += 1;
            Pin::new(&mut self.unread).poll_read(cx, buf)
        }
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

    #[test_case(continuation(65, true) ; "growing past the limit")]
    #[test_case(text(65, true) ; "followed by a new message")]
    #[tokio::test]
    async fn unfinished_message_is_left_to_the_decoder(second: Vec<u8>) {
        let sent = [text(40, false), second];
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

    #[tokio::test]
    async fn frames_are_read_from_the_socket_together() {
        let sent = vec![text(64, true); 1000].concat();
        let (sender, _answers) = mpsc::channel(1);
        let counted = Counted { unread: &sent, reads: 0 };
        let mut socket = RequestSizeLimit::new(counted, MAX_SIZE, sender);
        let mut read = Vec::new();
        socket.read_to_end(&mut read).await.unwrap();
        assert_eq!(read, sent);
        // Reading each header and payload on its own would take thousands.
        let reads = socket.io.get_ref().reads;
        assert!(reads < 20, "{reads} reads");
    }

    #[tokio::test]
    async fn frames_read_together_reach_the_decoder_together() {
        let sent = [text(8, true), text(65, true), ping(), text(9, true)].concat();
        let (sender, _answers) = mpsc::channel(1);
        let mut socket = RequestSizeLimit::new(sent.as_slice(), MAX_SIZE, sender);
        let mut read = [0; 128];
        let len = socket.read(&mut read).await.unwrap();
        assert_eq!(read.get(..len).unwrap(), [text(8, true), ping(), text(9, true)].concat());
    }

    #[test_case(&[text(65, false), text(9, true)], ProtocolError::ExpectedFragment(Data::Text) ; "new message inside a skipped one")]
    #[test_case(&[unmasked(65)], ProtocolError::UnmaskedFrameFromClient ; "unmasked")]
    #[test_case(&[reserved(65)], ProtocolError::NonZeroReservedBits ; "reserved bit")]
    #[tokio::test]
    async fn invalid_skipped_frame_is_rejected(sent: &[Vec<u8>], expected: ProtocolError) {
        let sent = sent.concat();
        let (sender, _answers) = mpsc::channel(1);
        let mut socket = RequestSizeLimit::new(sent.as_slice(), MAX_SIZE, sender);
        let err = socket.read_to_end(&mut Vec::new()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(err.get_ref().unwrap().downcast_ref(), Some(&expected));
    }
}
