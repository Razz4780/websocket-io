//! Incoming side: frame parsing, streaming binary payloads, control frame handling.
//!
//! Binary payloads are never assembled: bytes are handed to the caller as soon as they arrive,
//! unmasked on the way out of the read buffer. When a large payload is expected and the read
//! buffer is empty, the IO reads straight into the caller's buffer instead.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    close::{self, CloseFrame},
    config::Role,
    error::{self, ProtocolError},
    frame::{self, OpCode},
    mask,
    utf8::Utf8Bytes,
    ws::WebSocketIO,
};

/// Something received from the peer.
///
/// `B` is the representation of binary data: the number of bytes appended to the caller's buffer
/// for [`WebSocketIO::poll_recv`], and the data itself for [`WebSocketIO::poll_recv_bytes`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recv<B = usize> {
    /// A piece of a binary message.
    ///
    /// A piece never spans more than one frame, so it never spans message boundaries.
    Binary {
        /// The binary data.
        data: B,
        /// Whether this piece ends the message.
        end_of_message: bool,
    },
    /// A complete text message.
    Text(Utf8Bytes),
    /// A Ping frame. If [`Config::auto_pong`](crate::Config::auto_pong) is enabled, the Pong has
    /// already been queued.
    Ping(Bytes),
    /// A Pong frame.
    Pong(Bytes),
    /// The peer's Close frame, or `None` if it carried no status code.
    ///
    /// Unless [`Config::defer_close_reply`](crate::Config::defer_close_reply) is enabled, our reply
    /// has already been flushed and the IO has been shut down. Reported again on every later call.
    Close(Option<CloseFrame>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MessageKind {
    Binary,
    Text,
}

#[derive(Clone, Copy, Debug)]
enum FrameState {
    /// Waiting for the next frame header.
    Header,
    /// Inside the payload of a data frame.
    Payload {
        kind: MessageKind,
        remaining: u64,
        /// Masking key, `None` for unmasked frames and all-zero keys.
        mask: Option<[u8; 4]>,
        /// Position of the next payload byte, modulo 4.
        offset: usize,
        fin: bool,
    },
}

pub(crate) struct RecvState {
    buf: BytesMut,
    frame: FrameState,
    /// Kind of the fragmented message in progress, if any.
    message: Option<MessageKind>,
    /// Text message being assembled from fragments.
    text: BytesMut,
    /// Length of the validated prefix of `text`.
    text_valid: usize,
    /// The peer's Close frame, once received. `Some(None)` also for a close without a status code,
    /// and for EOF after our Close frame.
    pub(crate) close_received: Option<Option<CloseFrame>>,
    /// Whether the IO has been shut down after the close handshake.
    pub(crate) shutdown_done: bool,
    failed: bool,
    /// Error to report on the next call, deferred because data was handed out first.
    pending_error: Option<io::Error>,
}

impl RecvState {
    pub(crate) fn new(buf: BytesMut) -> Self {
        Self {
            buf,
            frame: FrameState::Header,
            message: None,
            text: BytesMut::new(),
            text_valid: 0,
            close_received: None,
            shutdown_done: false,
            failed: false,
            pending_error: None,
        }
    }

    pub(crate) fn defer_error(&mut self, error: io::Error) {
        self.pending_error = Some(error);
    }

    /// Frees the read buffer if it holds no data, so that idle connections do not pin memory.
    /// Called when the IO has nothing to read.
    fn release_buf(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > 0 {
            self.buf = BytesMut::new();
        }
    }
}

/// Outcome of [`Dest::poll_read_through`]: `None` if unsupported, otherwise the number of bytes
/// read and the chunk they form.
type ReadThrough<C> = Option<Poll<io::Result<(usize, C)>>>;

/// Destination of binary payload.
pub(crate) trait Dest {
    type Chunk;

    /// How many bytes can be taken.
    fn remaining(&self) -> usize;

    fn empty(&mut self) -> Self::Chunk;

    /// Takes `n` payload bytes from the front of `buf`, unmasking them.
    fn take(
        &mut self,
        buf: &mut BytesMut,
        n: usize,
        mask: Option<[u8; 4]>,
        offset: usize,
    ) -> Self::Chunk;

    /// Reads up to `limit` payload bytes straight from `io`, unmasking them. Returns `None` if
    /// this destination does not support it.
    fn poll_read_through<R: AsyncRead + Unpin>(
        &mut self,
        io: &mut R,
        cx: &mut Context<'_>,
        limit: usize,
        mask: Option<[u8; 4]>,
        offset: usize,
    ) -> ReadThrough<Self::Chunk>;
}

impl Dest for ReadBuf<'_> {
    type Chunk = usize;

    fn remaining(&self) -> usize {
        ReadBuf::remaining(self)
    }

    fn empty(&mut self) -> usize {
        0
    }

    fn take(
        &mut self,
        buf: &mut BytesMut,
        n: usize,
        mask: Option<[u8; 4]>,
        offset: usize,
    ) -> usize {
        match mask {
            Some(key) => {
                // SAFETY: we never de-initialize bytes, `copy_masked` only writes.
                let unfilled = unsafe { self.unfilled_mut() };
                mask::copy_masked(unfilled, &buf[..n], key, offset);
                // SAFETY: `copy_masked` initialized `n` bytes.
                unsafe { self.assume_init(n) };
                self.advance(n);
            }
            None => self.put_slice(&buf[..n]),
        }
        buf.advance(n);
        n
    }

    fn poll_read_through<R: AsyncRead + Unpin>(
        &mut self,
        io: &mut R,
        cx: &mut Context<'_>,
        limit: usize,
        mask: Option<[u8; 4]>,
        offset: usize,
    ) -> ReadThrough<usize> {
        // SAFETY: we never de-initialize bytes, the reader only writes.
        let unfilled = unsafe { self.unfilled_mut() };
        let mut target = ReadBuf::uninit(&mut unfilled[..limit]);
        match Pin::new(io).poll_read(cx, &mut target) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Some(Poll::Ready(Err(error))),
            Poll::Pending => return Some(Poll::Pending),
        }
        let n = target.filled().len();
        if let Some(key) = mask {
            mask::apply_mask(target.filled_mut(), key, offset);
        }
        // SAFETY: the reader initialized `n` bytes at the start of the unfilled region.
        unsafe { self.assume_init(n) };
        self.advance(n);
        Some(Poll::Ready(Ok((n, n))))
    }
}

/// Destination handing out payload as [`Bytes`] split off the read buffer.
pub(crate) struct BytesDest;

impl Dest for BytesDest {
    type Chunk = Bytes;

    fn remaining(&self) -> usize {
        usize::MAX
    }

    fn empty(&mut self) -> Bytes {
        Bytes::new()
    }

    fn take(
        &mut self,
        buf: &mut BytesMut,
        n: usize,
        mask: Option<[u8; 4]>,
        offset: usize,
    ) -> Bytes {
        let mut chunk = buf.split_to(n);
        if let Some(key) = mask {
            mask::apply_mask(&mut chunk, key, offset);
        }
        chunk.freeze()
    }

    fn poll_read_through<R: AsyncRead + Unpin>(
        &mut self,
        _: &mut R,
        _: &mut Context<'_>,
        _: usize,
        _: Option<[u8; 4]>,
        _: usize,
    ) -> ReadThrough<Bytes> {
        None
    }
}

/// Result of a single step of the receive state machine: an event, or `None` to keep going.
type Step<T> = Poll<io::Result<Option<Recv<T>>>>;

impl<IO: AsyncRead + AsyncWrite + Unpin> WebSocketIO<IO> {
    /// Drives the receive state machine until an event is ready.
    ///
    /// With `buffered_only`, stops (returning [`Poll::Pending`] without registering a waker) as
    /// soon as the IO would have to be read, or anything but binary data is next in line.
    pub(crate) fn poll_event<D: Dest>(
        &mut self,
        cx: &mut Context<'_>,
        dest: &mut D,
        buffered_only: bool,
    ) -> Poll<io::Result<Recv<D::Chunk>>> {
        if let Some(error) = self.recv.pending_error.take() {
            return Poll::Ready(Err(error));
        }
        if self.recv.failed {
            return Poll::Ready(Err(error::failed()));
        }
        if self.send.control_pending
            && !buffered_only
            && let Poll::Ready(Err(error)) = self.poll_drain_idle(cx)
        {
            return self.fail(error);
        }

        loop {
            if self.recv.close_received.is_some() {
                if buffered_only {
                    return Poll::Pending;
                }
                ready!(self.poll_finish_close(cx));
                let frame = self.recv.close_received.clone().flatten();
                return Poll::Ready(Ok(Recv::Close(frame)));
            }

            let step = match self.recv.frame {
                FrameState::Header => self.poll_header(cx, dest, buffered_only),
                FrameState::Payload {
                    kind: MessageKind::Binary,
                    ..
                } => self.poll_binary(cx, dest, buffered_only),
                FrameState::Payload {
                    kind: MessageKind::Text,
                    ..
                } => self.poll_text(cx, buffered_only),
            };
            if let Some(event) = ready!(step)? {
                return Poll::Ready(Ok(event));
            }
        }
    }

    fn poll_header<D: Dest>(
        &mut self,
        cx: &mut Context<'_>,
        dest: &mut D,
        buffered_only: bool,
    ) -> Step<D::Chunk> {
        let (header, header_len) = match frame::parse(&self.recv.buf) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => {
                ready!(self.poll_fill(cx, buffered_only))?;
                return Poll::Ready(Ok(None));
            }
            Err(error) => return self.fail_protocol(cx, error),
        };

        match (self.role, header.mask.is_some()) {
            (Role::Server, false) if !self.config.accept_unmasked_frames => {
                return self.fail_protocol(cx, ProtocolError::UnmaskedFrame);
            }
            (Role::Client, true) => return self.fail_protocol(cx, ProtocolError::MaskedFrame),
            _ => {}
        }

        if header.opcode.is_control() {
            if buffered_only {
                return Poll::Pending;
            }
            // Control payloads are tiny, wait until the whole frame is buffered.
            let len = header.len as usize;
            if self.recv.buf.len() < header_len + len {
                ready!(self.poll_fill(cx, false))?;
                return Poll::Ready(Ok(None));
            }
            self.recv.buf.advance(header_len);
            let mut payload = self.recv.buf.split_to(len);
            if let Some(key) = header.mask {
                mask::apply_mask(&mut payload, key, 0);
            }
            let payload = payload.freeze();

            return match header.opcode {
                OpCode::Ping => {
                    if self.config.auto_pong && !self.send.close_sent {
                        self.send.pending_pong = Some(payload.clone());
                        self.send.control_pending = true;
                        if let Poll::Ready(Err(error)) = self.poll_drain_idle(cx) {
                            return self.fail(error);
                        }
                    }
                    Poll::Ready(Ok(Some(Recv::Ping(payload))))
                }
                OpCode::Pong => Poll::Ready(Ok(Some(Recv::Pong(payload)))),
                _ => match close::parse_payload(payload) {
                    Ok(frame) => {
                        self.on_peer_close(frame);
                        Poll::Ready(Ok(None))
                    }
                    Err(error) => self.fail_protocol(cx, error),
                },
            };
        }

        let kind = match (header.opcode, self.recv.message) {
            (OpCode::Continuation, Some(kind)) => kind,
            (OpCode::Continuation, None) => {
                return self.fail_protocol(cx, ProtocolError::UnexpectedContinuation);
            }
            (_, Some(_)) => return self.fail_protocol(cx, ProtocolError::ExpectedContinuation),
            (OpCode::Text, None) => MessageKind::Text,
            (_, None) => MessageKind::Binary,
        };
        if kind == MessageKind::Text {
            if buffered_only {
                return Poll::Pending;
            }
            let room = self
                .config
                .max_message_size
                .saturating_sub(self.recv.text.len());
            if header.len > room as u64 {
                return self.fail_protocol(cx, ProtocolError::MessageTooLarge);
            }
        }

        self.recv.buf.advance(header_len);
        self.recv.message = (!header.fin).then_some(kind);
        if header.len > 0 {
            self.recv.frame = FrameState::Payload {
                kind,
                remaining: header.len,
                mask: header.mask.filter(|key| *key != [0; 4]),
                offset: 0,
                fin: header.fin,
            };
            return Poll::Ready(Ok(None));
        }

        match (kind, header.fin) {
            (_, false) => Poll::Ready(Ok(None)),
            (MessageKind::Binary, true) => Poll::Ready(Ok(Some(Recv::Binary {
                data: dest.empty(),
                end_of_message: true,
            }))),
            (MessageKind::Text, true) => self.finish_text(cx),
        }
    }

    fn poll_binary<D: Dest>(
        &mut self,
        cx: &mut Context<'_>,
        dest: &mut D,
        buffered_only: bool,
    ) -> Step<D::Chunk> {
        let FrameState::Payload {
            kind,
            remaining,
            mask,
            offset,
            fin,
        } = self.recv.frame
        else {
            unreachable!("called only inside of a payload");
        };

        let limit = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(dest.remaining());
        if limit == 0 {
            return Poll::Ready(Ok(Some(Recv::Binary {
                data: dest.empty(),
                end_of_message: false,
            })));
        }

        let (n, chunk) = if !self.recv.buf.is_empty() {
            let n = limit.min(self.recv.buf.len());
            (n, dest.take(&mut self.recv.buf, n, mask, offset))
        } else if buffered_only {
            return Poll::Pending;
        } else if let Some(poll) = (limit >= self.config.read_buffer_size / 2)
            .then(|| dest.poll_read_through(&mut self.io, cx, limit, mask, offset))
            .flatten()
        {
            let Poll::Ready(result) = poll else {
                self.recv.release_buf();
                return Poll::Pending;
            };
            let (n, chunk) = result?;
            if n == 0 {
                self.on_eof()?;
                return Poll::Ready(Ok(None));
            }
            (n, chunk)
        } else {
            ready!(self.poll_fill(cx, false))?;
            return Poll::Ready(Ok(None));
        };

        let remaining = remaining - n as u64;
        self.recv.frame = if remaining == 0 {
            FrameState::Header
        } else {
            FrameState::Payload {
                kind,
                remaining,
                mask,
                offset: (offset + n) % 4,
                fin,
            }
        };
        Poll::Ready(Ok(Some(Recv::Binary {
            data: chunk,
            end_of_message: fin && remaining == 0,
        })))
    }

    fn poll_text<T>(&mut self, cx: &mut Context<'_>, buffered_only: bool) -> Step<T> {
        let FrameState::Payload {
            kind,
            remaining,
            mask,
            offset,
            fin,
        } = self.recv.frame
        else {
            unreachable!("called only inside of a payload");
        };

        if self.recv.buf.is_empty() {
            ready!(self.poll_fill(cx, buffered_only))?;
            return Poll::Ready(Ok(None));
        }
        let n = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(self.recv.buf.len());
        let remaining = remaining - n as u64;

        if remaining == 0 && fin && self.recv.text.is_empty() {
            // The whole message is buffered, hand it out without copying.
            let mut payload = self.recv.buf.split_to(n);
            if let Some(key) = mask {
                mask::apply_mask(&mut payload, key, offset);
            }
            self.recv.frame = FrameState::Header;
            return match Utf8Bytes::try_from(payload.freeze()) {
                Ok(text) => Poll::Ready(Ok(Some(Recv::Text(text)))),
                Err(_) => self.fail_protocol(cx, ProtocolError::InvalidUtf8),
            };
        }

        let text = &mut self.recv.text;
        text.reserve(n);
        let src = &self.recv.buf[..n];
        match mask {
            Some(key) => {
                mask::copy_masked(text.spare_capacity_mut(), src, key, offset);
                // SAFETY: `copy_masked` initialized `n` bytes of spare capacity.
                unsafe { text.set_len(text.len() + n) };
            }
            None => text.extend_from_slice(src),
        }
        self.recv.buf.advance(n);

        // Validate incrementally to fail fast. A code point split between frames stays in the
        // unvalidated suffix until the rest of it arrives.
        match std::str::from_utf8(&self.recv.text[self.recv.text_valid..]) {
            Ok(_) => self.recv.text_valid = self.recv.text.len(),
            Err(error) if error.error_len().is_none() => {
                self.recv.text_valid += error.valid_up_to()
            }
            Err(_) => return self.fail_protocol(cx, ProtocolError::InvalidUtf8),
        }

        if remaining > 0 {
            self.recv.frame = FrameState::Payload {
                kind,
                remaining,
                mask,
                offset: (offset + n) % 4,
                fin,
            };
            return Poll::Ready(Ok(None));
        }
        self.recv.frame = FrameState::Header;
        if fin {
            self.finish_text(cx)
        } else {
            Poll::Ready(Ok(None))
        }
    }

    fn finish_text<T>(&mut self, cx: &mut Context<'_>) -> Step<T> {
        if self.recv.text_valid != self.recv.text.len() {
            return self.fail_protocol(cx, ProtocolError::InvalidUtf8);
        }
        // Take the whole buffer, so that we do not keep its allocation alive after the caller drops
        // the message.
        let text = std::mem::take(&mut self.recv.text).freeze();
        self.recv.text_valid = 0;
        // SAFETY: validated above.
        let text = unsafe { Utf8Bytes::from_bytes_unchecked(text) };
        Poll::Ready(Ok(Some(Recv::Text(text))))
    }

    /// Reads more data from the IO into the read buffer.
    fn poll_fill(&mut self, cx: &mut Context<'_>, buffered_only: bool) -> Poll<io::Result<()>> {
        if buffered_only {
            return Poll::Pending;
        }
        let buf = &mut self.recv.buf;
        if buf.capacity() - buf.len() < self.config.read_buffer_size / 2 {
            buf.reserve(self.config.read_buffer_size);
        }
        let mut target = ReadBuf::uninit(buf.spare_capacity_mut());
        let Poll::Ready(result) = Pin::new(&mut self.io).poll_read(cx, &mut target) else {
            self.recv.release_buf();
            return Poll::Pending;
        };
        result?;
        let n = target.filled().len();
        // SAFETY: the reader initialized `n` bytes of spare capacity.
        unsafe { buf.set_len(buf.len() + n) };
        if n == 0 {
            self.on_eof()?;
        }
        Poll::Ready(Ok(()))
    }

    fn on_eof(&mut self) -> io::Result<()> {
        if self.send.close_sent {
            // We are closing, the peer just did not bother to reply.
            self.recv.close_received = Some(None);
            Ok(())
        } else {
            self.recv.failed = true;
            Err(error::unexpected_eof())
        }
    }

    fn on_peer_close(&mut self, frame: Option<CloseFrame>) {
        if !self.send.close_sent && !self.config.defer_close_reply {
            // Echo the status code, as RFC 6455 suggests.
            self.send
                .queue_close(frame.as_ref().map(|frame| frame.code), "");
            self.send.control_pending = true;
        }
        self.recv.close_received = Some(frame);
    }

    /// Flushes our reply to the peer's Close frame and shuts the IO down.
    fn poll_finish_close(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.send.close_sent || self.recv.shutdown_done {
            return Poll::Ready(());
        }
        // The peer is gone either way, errors do not matter anymore.
        if ready!(self.poll_drain_idle(cx)).is_err() {
            self.send.buf.clear();
        }
        let _ = ready!(Pin::new(&mut self.io).poll_shutdown(cx));
        self.recv.shutdown_done = true;
        Poll::Ready(())
    }

    fn fail<T>(&mut self, error: io::Error) -> Poll<io::Result<T>> {
        self.recv.failed = true;
        Poll::Ready(Err(error))
    }

    /// Fails the connection, sending a Close frame with a matching status code on a best-effort
    /// basis.
    fn fail_protocol<T>(
        &mut self,
        cx: &mut Context<'_>,
        error: ProtocolError,
    ) -> Poll<io::Result<T>> {
        self.recv.failed = true;
        if !self.send.close_sent {
            self.send.queue_close(Some(error.close_code()), "");
            let _ = self.poll_drain_idle(cx);
        }
        Poll::Ready(Err(error.into()))
    }
}
