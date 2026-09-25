//! Outgoing side: frame encoding, write buffering, direct vectored writes.
//!
//! Invariant: every [`WebSocketIO::poll_write_vectored`] that reports `n` accepted bytes leaves
//! complete frames for those bytes behind. Their head is already in the IO, their tail (if any)
//! sits in the write buffer. No frame ever waits for more user data, so a flush only has to drain
//! the buffer.

use std::{
    cell::RefCell,
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

/// Most frames sent by one vectored write.
const MAX_FRAMES: usize = 16;

/// Most data sent by one vectored write. Larger writes are more likely to be taken only in part,
/// and the rest of a partially written frame has to be copied.
const MAX_WRITE_BYTES: usize = 256 * 1024;

/// Most buffers passed to one vectored write of the IO (Linux accepts up to 1024).
const MAX_IOVECS: usize = 128;

/// Pieces of a server's data shorter than this are copied into the scratch buffer rather than
/// passed to the IO as buffers of their own: the kernel handles every buffer of a vectored write
/// separately, which costs more than copying a short one.
const COPY_BELOW: usize = 256;

/// Size of the per-thread scratch buffer.
const SCRATCH_SIZE: usize = 64 * 1024;

thread_local! {
    /// Frame headers, a server's short pieces of data and a client's masked data, laid out for one
    /// vectored write. Shared by all connections of a thread, so it costs no memory per connection.
    static SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// A buffer of a vectored write.
#[derive(Clone, Copy)]
enum Segment<'a> {
    /// A range of the scratch buffer.
    Scratch(usize, usize),
    /// A piece of the caller's data.
    Direct(&'a [u8]),
}

impl Segment<'_> {
    fn len(&self) -> usize {
        match *self {
            Self::Scratch(start, end) => end - start,
            Self::Direct(piece) => piece.len(),
        }
    }

    fn extend_to(&mut self, new_end: usize) {
        if let Self::Scratch(_, end) = self {
            *end = new_end;
        }
    }

    fn start_at(&mut self, new_start: usize) {
        if let Self::Scratch(start, _) = self {
            *start = new_start;
        }
    }
}

/// Counts the segments holding the first `len` bytes. Frames never share a segment: every frame's
/// header starts a new one.
fn segments_of_frame(segments: &[Segment<'_>], mut len: usize) -> usize {
    let mut count = 0;
    for segment in segments {
        if len == 0 {
            break;
        }
        len -= segment.len().min(len);
        count += 1;
    }
    count
}

/// A position in the concatenation of a list of buffers.
#[derive(Clone, Copy, Default)]
struct Cursor {
    index: usize,
    offset: usize,
}

impl Cursor {
    /// Calls `f` with the pieces of the next `len` bytes, and moves past them.
    fn take<'a>(&mut self, bufs: &'a [IoSlice<'_>], mut len: usize, mut f: impl FnMut(&'a [u8])) {
        while len > 0 {
            let buf = &bufs[self.index][self.offset..];
            let take = buf.len().min(len);
            if take > 0 {
                f(&buf[..take]);
            }
            len -= take;
            self.offset += take;
            if self.offset == bufs[self.index].len() {
                self.index += 1;
                self.offset = 0;
            }
        }
    }
}

pub(crate) struct SendState {
    /// Encoded frames waiting for the IO. Only the first one may be partially written.
    pub(crate) buf: BytesMut,
    /// Key source, present only for clients.
    keys: Option<Box<MaskKeys>>,
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
            keys: masked.then(|| Box::new(MaskKeys::new())),
            pending_pong: None,
            close_sent: false,
            control_pending: false,
            write_buffer_size,
        }
    }

    /// Appends a complete, single-frame message to the buffer. Masks the payload while copying it
    /// in if this is a client.
    pub(crate) fn push_frame(&mut self, opcode: OpCode, payload: &[u8]) {
        self.push_frame_vectored(opcode, &[IoSlice::new(payload)], payload.len());
    }

    /// Appends a complete, single-frame message with the first `len` bytes of `bufs` to the
    /// buffer. Masks the payload while copying it in if this is a client.
    fn push_frame_vectored(&mut self, opcode: OpCode, bufs: &[IoSlice<'_>], len: usize) {
        let mask = self.keys.as_mut().map(|keys| keys.next());
        let mut header = [0; MAX_HEADER_LEN];
        let header_len = frame::encode(
            &mut header,
            Header {
                fin: true,
                opcode,
                mask,
                len: len as u64,
            },
        );

        let needed = header_len + len;
        if self.buf.capacity() == 0 {
            // The buffer was released while idle, allocate it in one go rather than growing it.
            self.buf.reserve(needed.max(self.write_buffer_size));
        } else {
            self.buf.reserve(needed);
        }
        self.buf.extend_from_slice(&header[..header_len]);
        let mut offset = 0;
        Cursor::default().take(bufs, len, |piece| {
            match mask {
                Some(key) => {
                    mask::copy_masked(self.buf.spare_capacity_mut(), piece, key, offset);
                    // SAFETY: `copy_masked` initialized `piece.len()` bytes of spare capacity.
                    unsafe { self.buf.set_len(self.buf.len() + piece.len()) };
                }
                None => self.buf.extend_from_slice(piece),
            }
            offset += piece.len();
        });
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

    /// Frees the write buffer if it holds no data, so that idle connections do not pin memory.
    fn release_buf(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > 0 {
            self.buf = BytesMut::new();
        }
    }

    /// Whether appending a frame with `len` bytes of payload should wait for the buffer to drain.
    fn would_overflow(&self, len: usize) -> bool {
        !self.buf.is_empty() && self.buf.len() + len + MAX_HEADER_LEN > self.write_buffer_size
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> WebSocketIO<IO> {
    /// Writes a prefix of `data` as binary messages and returns its length.
    ///
    /// This is the byte-stream half of the connection, with the semantics of
    /// [`AsyncWrite::poll_write`]. Same as [`poll_write_vectored`](Self::poll_write_vectored) with
    /// a single buffer.
    pub fn poll_write(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(data)])
    }

    /// Writes a prefix of the concatenation of `bufs` as binary messages and returns its length.
    ///
    /// This is the byte-stream half of the connection, with the semantics of
    /// [`AsyncWrite::poll_write_vectored`]: the returned count may be shorter than the data, and
    /// small writes are buffered until [`poll_flush`](Self::poll_flush) or until the buffer fills
    /// up. Every message is one frame of at most
    /// [`Config::max_frame_size`](crate::Config::max_frame_size) bytes, and may span buffers.
    ///
    /// Writes of at least half the [write buffer size](crate::Config::write_buffer_size) go to the
    /// IO right away, with one vectored write of up to 256 KiB. A server sends pieces of data of
    /// 256 bytes and more straight from `bufs`, and copies frame headers and shorter pieces into a
    /// per-thread scratch buffer, so that neighbouring ones go out as one buffer. A client masks
    /// its data into the scratch buffer (up to 64 KiB per call). Only the unsent part of a
    /// partially sent frame is copied into the write buffer. Smaller writes, and all writes to an
    /// IO without vectored writes, are copied into the write buffer, where they coalesce into
    /// fewer, larger writes to the IO.
    ///
    /// Fails with [`io::ErrorKind::BrokenPipe`] once our Close frame has been queued.
    pub fn poll_write_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if self.send.close_sent {
            return Poll::Ready(Err(error::closed()));
        }
        let total = bufs
            .iter()
            .fold(0usize, |total, buf| total.saturating_add(buf.len()));
        if total == 0 {
            return Poll::Ready(Ok(0));
        }

        if total >= self.config.write_buffer_size / 2 && self.io.is_write_vectored() {
            return self.poll_write_through(cx, bufs, total);
        }

        let len = total.min(self.config.max_frame_size);
        if self.send.would_overflow(len) {
            ready!(self.poll_drain(cx))?;
        }
        self.send.push_frame_vectored(OpCode::Binary, bufs, len);
        Poll::Ready(Ok(len))
    }

    /// Writes buffered frames and new frames with the data of `bufs` in one vectored write.
    ///
    /// Frame headers, and (for servers) pieces of data shorter than [`COPY_BELOW`], are copied
    /// into a per-thread scratch buffer, so that neighbouring ones go to the IO as one buffer.
    /// Clients mask all their data into the scratch buffer. Longer pieces of a server's data go to
    /// the IO straight from `bufs`.
    ///
    /// Once any byte of a new frame reaches the IO, the frame is committed: its remainder is copied
    /// into the write buffer and its whole payload is reported as written. Frames the IO did not
    /// take any byte of are not accepted.
    fn poll_write_through(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        total: usize,
    ) -> Poll<io::Result<usize>> {
        SCRATCH.with(|scratch| match scratch.try_borrow_mut() {
            Ok(mut scratch) => self.poll_write_through_with(cx, bufs, total, &mut scratch),
            // Only when writes nest, e.g. a `WebSocketIO` over a `WebSocketIO`.
            Err(_) => self.poll_write_through_with(cx, bufs, total, &mut Vec::new()),
        })
    }

    fn poll_write_through_with(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        total: usize,
        scratch: &mut Vec<u8>,
    ) -> Poll<io::Result<usize>> {
        scratch.clear();
        scratch.reserve(SCRATCH_SIZE);
        // Room for the buffered data.
        let mut segments = [Segment::Scratch(0, 0); MAX_IOVECS - 1];
        let mut used = 0;
        let mut headers = [[0; MAX_HEADER_LEN]; MAX_FRAMES];
        let mut header_lens = [0; MAX_FRAMES];
        let mut payload_lens = [0; MAX_FRAMES];
        let mut frames = 0;
        let (mut index, mut offset) = (0, 0);
        let mut remaining = total;

        // Lay the frames out, as many as fit the limits.
        let mut planned = 0;
        while frames < MAX_FRAMES
            && remaining > 0
            && planned < MAX_WRITE_BYTES
            && used < segments.len()
            && scratch.len() + MAX_HEADER_LEN < SCRATCH_SIZE
        {
            let mask = self.send.keys.as_mut().map(|keys| keys.next());
            // The header's length depends on the payload's, which depends on the limits. Reserve
            // room for the longest header, and write the actual one at the end of it, right in
            // front of the payload.
            let slot = scratch.len();
            scratch.resize(slot + MAX_HEADER_LEN, 0);
            let header_segment = used;
            segments[used] = Segment::Scratch(slot, scratch.len());
            used += 1;
            let mut run_open = true;

            let max_len = remaining
                .min(self.config.max_frame_size)
                .min(MAX_WRITE_BYTES - planned);
            let mut len = 0;
            while len < max_len {
                let buf = &bufs[index][offset..];
                if buf.is_empty() {
                    index += 1;
                    offset = 0;
                    continue;
                }
                let want = buf.len().min(max_len - len);
                let n = if mask.is_some() || want < COPY_BELOW {
                    let n = want.min(SCRATCH_SIZE - scratch.len());
                    if n == 0 || (!run_open && used == segments.len()) {
                        break;
                    }
                    let start = scratch.len();
                    match mask {
                        Some(key) => {
                            scratch.reserve(n);
                            mask::copy_masked(scratch.spare_capacity_mut(), &buf[..n], key, len);
                            // SAFETY: `copy_masked` initialized `n` bytes of spare capacity.
                            unsafe { scratch.set_len(start + n) };
                        }
                        None => scratch.extend_from_slice(&buf[..n]),
                    }
                    if run_open {
                        segments[used - 1].extend_to(scratch.len());
                    } else {
                        segments[used] = Segment::Scratch(start, scratch.len());
                        used += 1;
                        run_open = true;
                    }
                    n
                } else {
                    if used == segments.len() {
                        break;
                    }
                    segments[used] = Segment::Direct(&buf[..want]);
                    used += 1;
                    run_open = false;
                    want
                };
                len += n;
                offset += n;
                if offset == bufs[index].len() {
                    index += 1;
                    offset = 0;
                }
            }
            if len == 0 {
                scratch.truncate(slot);
                used = header_segment;
                break;
            }

            let header_len = frame::encode(
                &mut headers[frames],
                Header {
                    fin: true,
                    opcode: OpCode::Binary,
                    mask,
                    len: len as u64,
                },
            );
            let header_start = slot + MAX_HEADER_LEN - header_len;
            scratch[header_start..slot + MAX_HEADER_LEN]
                .copy_from_slice(&headers[frames][..header_len]);
            segments[header_segment].start_at(header_start);
            header_lens[frames] = header_len;
            payload_lens[frames] = len;
            frames += 1;
            remaining -= len;
            planned += len;
        }

        loop {
            self.send.stage_pong();
            let buffered = self.send.buf.len();
            let mut slices = [IoSlice::new(&[]); MAX_IOVECS];
            let mut count = 0;
            if buffered > 0 {
                slices[0] = IoSlice::new(&self.send.buf);
                count = 1;
            }
            for segment in &segments[..used] {
                slices[count] = match *segment {
                    Segment::Scratch(start, end) => IoSlice::new(&scratch[start..end]),
                    Segment::Direct(piece) => IoSlice::new(piece),
                };
                count += 1;
            }

            let written = ready!(Pin::new(&mut self.io).poll_write_vectored(cx, &slices[..count]))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            if written <= buffered {
                self.send.buf.advance(written);
                continue;
            }

            self.send.buf.advance(buffered);
            self.send.control_pending = false;
            let mut written = written - buffered;
            let mut accepted = 0;
            // Index of the current frame's first segment.
            let mut segment = 0;
            for frame in 0..frames {
                if written == 0 {
                    break;
                }
                let frame_len = header_lens[frame] + payload_lens[frame];
                accepted += payload_lens[frame];
                if written >= frame_len {
                    written -= frame_len;
                    segment += segments_of_frame(&segments[segment..used], frame_len);
                    continue;
                }
                // The frame is partially written, its remainder (as it goes on the wire, so
                // already masked) has to follow.
                let mut skip = written;
                let mut left = frame_len - written;
                for piece in &segments[segment..used] {
                    if left == 0 {
                        break;
                    }
                    let bytes = match *piece {
                        Segment::Scratch(start, end) => &scratch[start..end],
                        Segment::Direct(piece) => piece,
                    };
                    let bytes = &bytes[skip.min(bytes.len())..];
                    skip = skip.saturating_sub(piece.len());
                    let take = bytes.len().min(left);
                    self.send.buf.extend_from_slice(&bytes[..take]);
                    left -= take;
                }
                break;
            }
            return Poll::Ready(Ok(accepted));
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
    ///
    /// Frees the write buffer afterwards, so that an idle connection does not pin memory.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_drain_idle(cx))?;
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
        ready!(self.poll_drain_idle(cx))?;
        ready!(Pin::new(&mut self.io).poll_flush(cx))?;
        if self.recv.close_received.is_some() && !self.recv.shutdown_done {
            // The handshake is complete, a failure to shut down cannot lose any data.
            let _ = ready!(Pin::new(&mut self.io).poll_shutdown(cx));
            self.recv.shutdown_done = true;
        }
        Poll::Ready(Ok(()))
    }

    /// Writes all buffered frames to the IO, without flushing it, and keeps the buffer for more
    /// frames.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
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

    /// Like [`poll_drain`](Self::poll_drain), for when no more frames are expected soon: frees the
    /// buffer once drained, so that an idle connection does not pin memory.
    pub(crate) fn poll_drain_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_drain(cx))?;
        self.send.release_buf();
        Poll::Ready(Ok(()))
    }
}
