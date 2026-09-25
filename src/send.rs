//! Outgoing side: frame encoding, write buffering, direct vectored writes.
//!
//! Invariant: every [`WebSocketIO::poll_write`] that reports `n` accepted bytes leaves a complete
//! frame for those bytes behind. Its head is already in the IO, its tail (if any) sits in the
//! write buffer. No frame ever waits for more user data, so a flush only has to drain the buffer.

use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll, ready},
};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    close::{self, CloseCode, CloseFrame},
    error,
    frame::{self, Header, MAX_CONTROL_PAYLOAD, MAX_HEADER_LEN, OpCode},
    mask::{self, MaskKeys},
    ws::WebSocketIO,
};

/// Unmasked writes at least this large skip the write buffer and go to the IO with `writev`.
const WRITE_THROUGH_MIN: usize = 16 * 1024;

pub(crate) struct SendState {
    /// Encoded frames waiting for the IO. Only the first one may be partially written.
    pub(crate) buf: BytesMut,
    /// Key source, present only for clients.
    keys: Option<MaskKeys>,
    /// Payload of the latest unanswered ping. Newer pings replace older ones, as allowed by RFC
    /// 6455, which bounds the memory a ping flood can take.
    pub(crate) pending_pong: Option<Bytes>,
    /// Whether our Close frame has been queued. No frames may follow it.
    pub(crate) close_sent: bool,
    /// Whether a pong or a Close reply queued by the read side waits to be written.
    pub(crate) control_pending: bool,
    write_buffer_size: usize,
}

impl SendState {
    pub(crate) fn new(masked: bool, write_buffer_size: usize) -> Self {
        Self {
            buf: BytesMut::new(),
            keys: masked.then(MaskKeys::new),
            pending_pong: None,
            close_sent: false,
            control_pending: false,
            write_buffer_size,
        }
    }

    /// Appends a complete, single-frame message to the buffer. Masks the payload while copying it
    /// in if this is a client.
    pub(crate) fn push_frame(&mut self, opcode: OpCode, payload: &[u8]) {
        let mask = self.keys.as_mut().map(MaskKeys::next);
        let mut header = [0; MAX_HEADER_LEN];
        let header_len = frame::encode(
            &mut header,
            Header {
                fin: true,
                opcode,
                mask,
                len: payload.len() as u64,
            },
        );

        self.buf.reserve(header_len + payload.len());
        self.buf.extend_from_slice(&header[..header_len]);
        match mask {
            Some(key) => {
                mask::copy_masked(self.buf.spare_capacity_mut(), payload, key, 0);
                // SAFETY: `copy_masked` initialized `payload.len()` bytes of spare capacity.
                unsafe { self.buf.set_len(self.buf.len() + payload.len()) };
            }
            None => self.buf.extend_from_slice(payload),
        }
    }

    /// Queues our Close frame. `reason` must not exceed 123 bytes.
    pub(crate) fn queue_close(&mut self, code: Option<CloseCode>, reason: &str) {
        let mut payload = [0; MAX_CONTROL_PAYLOAD];
        let len = match code {
            Some(code) => {
                payload[..2].copy_from_slice(&code.0.to_be_bytes());
                payload[2..2 + reason.len()].copy_from_slice(reason.as_bytes());
                2 + reason.len()
            }
            None => 0,
        };
        self.pending_pong = None;
        self.push_frame(OpCode::Close, &payload[..len]);
        self.close_sent = true;
    }

    /// Moves the pending pong into the buffer, unless the buffer is already full.
    fn stage_pong(&mut self) {
        if self.buf.len() < self.write_buffer_size
            && let Some(payload) = self.pending_pong.take()
        {
            self.push_frame(OpCode::Pong, &payload);
        }
    }

    /// Whether appending a frame with `len` bytes of payload should wait for the buffer to drain.
    fn would_overflow(&self, len: usize) -> bool {
        !self.buf.is_empty() && self.buf.len() + len + MAX_HEADER_LEN > self.write_buffer_size
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> WebSocketIO<IO> {
    /// Writes a prefix of `data` as one binary message and returns its length.
    ///
    /// This is the byte-stream half of the connection, with the semantics of
    /// [`AsyncWrite::poll_write`]: the returned count may be shorter than `data` (at most
    /// [`Config::max_frame_size`](crate::Config::max_frame_size)), and small writes are buffered
    /// until [`poll_flush`](Self::poll_flush) or until the buffer fills up.
    ///
    /// Clients mask the payload while copying it into the write buffer. Servers send writes of
    /// 16 KiB and more straight from `data` with a vectored write, and copy only the part the IO
    /// did not accept.
    ///
    /// Fails with [`io::ErrorKind::BrokenPipe`] once our Close frame has been queued.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        if self.send.close_sent {
            return Poll::Ready(Err(error::closed()));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let data = &data[..data.len().min(self.config.max_frame_size)];

        if self.send.keys.is_none()
            && data.len() >= WRITE_THROUGH_MIN
            && self.io.is_write_vectored()
        {
            return self.poll_write_through(cx, data);
        }

        if self.send.would_overflow(data.len()) {
            ready!(self.poll_drain(cx))?;
        }
        self.send.push_frame(OpCode::Binary, data);
        Poll::Ready(Ok(data.len()))
    }

    /// Writes buffered frames and a new unmasked frame with `data` in one vectored write.
    ///
    /// Once any byte of the new frame reaches the IO, the frame is committed: the remainder is
    /// copied into the write buffer and the whole `data` is reported as written. If the IO does not
    /// take any byte of the new frame, nothing is accepted.
    fn poll_write_through(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        let mut header = [0; MAX_HEADER_LEN];
        let header_len = frame::encode(
            &mut header,
            Header {
                fin: true,
                opcode: OpCode::Binary,
                mask: None,
                len: data.len() as u64,
            },
        );
        let header = &header[..header_len];

        loop {
            self.send.stage_pong();
            let buffered = self.send.buf.len();
            let slices = [
                IoSlice::new(&self.send.buf),
                IoSlice::new(header),
                IoSlice::new(data),
            ];
            let slices = if buffered == 0 {
                &slices[1..]
            } else {
                &slices[..]
            };
            let written = ready!(Pin::new(&mut self.io).poll_write_vectored(cx, slices))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            if written <= buffered {
                self.send.buf.advance(written);
                continue;
            }

            self.send.buf.advance(buffered);
            self.send.control_pending = false;
            let written = written - buffered;
            if written < header_len {
                self.send.buf.extend_from_slice(&header[written..]);
                self.send.buf.extend_from_slice(data);
            } else {
                self.send
                    .buf
                    .extend_from_slice(&data[written - header_len..]);
            }
            return Poll::Ready(Ok(data.len()));
        }
    }

    /// Queues a text message. Buffered like [`poll_write`](Self::poll_write).
    ///
    /// The message is sent as a single frame, regardless of its length.
    pub fn poll_send_text(&mut self, cx: &mut Context<'_>, text: &str) -> Poll<io::Result<()>> {
        self.poll_send_frame(cx, OpCode::Text, text.as_bytes())
    }

    /// Queues a Ping frame. Buffered like [`poll_write`](Self::poll_write).
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if `payload` exceeds 125 bytes.
    pub fn poll_send_ping(&mut self, cx: &mut Context<'_>, payload: &[u8]) -> Poll<io::Result<()>> {
        self.poll_send_frame(cx, OpCode::Ping, payload)
    }

    /// Queues an unsolicited Pong frame. Buffered like [`poll_write`](Self::poll_write).
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if `payload` exceeds 125 bytes.
    pub fn poll_send_pong(&mut self, cx: &mut Context<'_>, payload: &[u8]) -> Poll<io::Result<()>> {
        self.poll_send_frame(cx, OpCode::Pong, payload)
    }

    fn poll_send_frame(
        &mut self,
        cx: &mut Context<'_>,
        opcode: OpCode,
        payload: &[u8],
    ) -> Poll<io::Result<()>> {
        if self.send.close_sent {
            return Poll::Ready(Err(error::closed()));
        }
        if opcode.is_control() && payload.len() > MAX_CONTROL_PAYLOAD {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "control frame payload exceeds 125 bytes",
            )));
        }
        if self.send.would_overflow(payload.len()) {
            ready!(self.poll_drain(cx))?;
        }
        self.send.push_frame(opcode, payload);
        Poll::Ready(Ok(()))
    }

    /// Writes all buffered frames to the IO and flushes it.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(cx))?;
        Pin::new(&mut self.io).poll_flush(cx)
    }

    /// Queues our Close frame (on the first call) and flushes it.
    ///
    /// `frame` is used only if our Close frame has not been queued yet, e.g. automatically in
    /// reply to the peer's Close frame. Without a frame, the Close frame carries no status code.
    ///
    /// Completes once our Close frame is flushed, it does not wait for the peer's reply. Keep
    /// receiving to observe the reply: data sent by the peer before its Close frame is still
    /// delivered. Once both Close frames have been exchanged, the IO is shut down.
    pub fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
        frame: Option<&CloseFrame>,
    ) -> Poll<io::Result<()>> {
        if !self.send.close_sent {
            if let Some(frame) = frame {
                close::validate(frame)?;
            }
            self.send.queue_close(
                frame.map(|frame| frame.code),
                frame.map_or("", |frame| &frame.reason),
            );
        }
        ready!(self.poll_drain(cx))?;
        ready!(Pin::new(&mut self.io).poll_flush(cx))?;
        if self.recv.close_received.is_some() && !self.recv.shutdown_done {
            // The handshake is complete, a failure to shut down cannot lose any data.
            let _ = ready!(Pin::new(&mut self.io).poll_shutdown(cx));
            self.recv.shutdown_done = true;
        }
        Poll::Ready(Ok(()))
    }

    /// Writes all buffered frames to the IO, without flushing it.
    pub(crate) fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            self.send.stage_pong();
            if self.send.buf.is_empty() {
                break;
            }
            let written = ready!(Pin::new(&mut self.io).poll_write(cx, &self.send.buf))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.send.buf.advance(written);
        }
        self.send.control_pending = false;
        Poll::Ready(Ok(()))
    }
}
