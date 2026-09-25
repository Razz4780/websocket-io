use std::{
    fmt,
    future::poll_fn,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    close::{CloseCode, CloseFrame},
    config::{Config, Role},
    recv::{BytesDest, Recv, RecvState},
    send::SendState,
};

/// A WebSocket connection working as a byte duplex.
///
/// Created from a connection that has already completed the HTTP upgrade (the opening handshake is
/// out of scope of this crate).
///
/// # Data
///
/// Outgoing data is sent in binary messages, one frame each. Incoming binary messages are treated
/// as one continuous byte stream, delivered as soon as the bytes arrive, without waiting for
/// complete frames or messages.
///
/// # Interfaces
///
/// * [`poll_recv`](Self::poll_recv) and [`poll_recv_bytes`](Self::poll_recv_bytes) report
///   binary data along with everything else the peer sends: text messages, pings, pongs, and
///   the Close frame.
/// * [`poll_write`](Self::poll_write), [`poll_send_text`](Self::poll_send_text),
///   [`poll_send_ping`](Self::poll_send_ping), [`poll_send_pong`](Self::poll_send_pong),
///   [`poll_flush`](Self::poll_flush) and [`poll_close`](Self::poll_close) send.
/// * [`AsyncRead`] and [`AsyncWrite`] give a plain byte stream: pings and pongs are handled
///   internally, text messages are an error, and a close handshake is an EOF (and is what
///   `poll_shutdown` performs). An EOF without a close handshake is an error, so a truncated
///   stream is always detected.
/// * `async` wrappers for the above.
///
/// # Control frames
///
/// Pings are answered from the receiving methods, as are the peer's Close frames. The replies go
/// out without waiting for a flush from the sending side, but reading never waits for a blocked
/// write. Keep receiving to keep the connection responsive.
///
/// # Buffering
///
/// Like with tokio's `BufWriter`, small writes are buffered until flushed. Call
/// [`poll_flush`](Self::poll_flush) (or `flush`) when the data should go out.
pub struct WebSocketIO<IO> {
    pub(crate) io: IO,
    pub(crate) role: Role,
    pub(crate) config: Config,
    pub(crate) recv: RecvState,
    pub(crate) send: SendState,
}

impl<IO> WebSocketIO<IO> {
    /// Wraps a connection that has just completed the HTTP upgrade.
    pub fn new(io: IO, role: Role, config: Config) -> Self {
        Self::with_read_buf(io, role, config, BytesMut::new())
    }

    /// Like [`WebSocketIO::new`], for when the HTTP layer has read past the end of the upgrade
    /// response. `read_buf` holds those bytes, it is used as the initial read buffer.
    pub fn with_read_buf(io: IO, role: Role, config: Config, read_buf: BytesMut) -> Self {
        let send = SendState::new(role == Role::Client, config.write_buffer_size);
        Self {
            io,
            role,
            config,
            recv: RecvState::new(read_buf),
            send,
        }
    }

    /// Our side of the connection.
    pub fn role(&self) -> Role {
        self.role
    }

    /// The configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a reference to the underlying IO.
    pub fn get_ref(&self) -> &IO {
        &self.io
    }

    /// Returns a mutable reference to the underlying IO.
    ///
    /// Reading from or writing to it directly corrupts the WebSocket stream.
    pub fn get_mut(&mut self) -> &mut IO {
        &mut self.io
    }

    /// Returns the underlying IO, dropping any buffered data.
    pub fn into_inner(self) -> IO {
        self.io
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> WebSocketIO<IO> {
    /// Receives the next event from the peer.
    ///
    /// Binary data is appended to `buf` and reported as [`Recv::Binary`] with the number of bytes
    /// appended. Masked data is unmasked while being copied. If `buf` has room for a large part of
    /// a big frame and nothing is buffered internally, the IO reads straight into `buf`.
    ///
    /// Returns [`Recv::Binary`] with 0 bytes if `buf` is full and binary data is next.
    pub fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<Recv>> {
        if let Some((data, end_of_message)) = self.read_buffered(buf) {
            return Poll::Ready(Ok(Recv::Binary {
                data,
                end_of_message,
            }));
        }
        self.poll_event(cx, buf, false)
    }

    /// Receives the next event from the peer, handing out binary data as [`Bytes`] split off the
    /// internal read buffer, without copying.
    ///
    /// Each piece holds whatever part of the current frame has already been read.
    pub fn poll_recv_bytes(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Recv<Bytes>>> {
        self.poll_event(cx, &mut BytesDest, false)
    }

    /// Receives the next event from the peer, see [`poll_recv`](Self::poll_recv).
    ///
    /// Binary data is written to the start of `buf`.
    pub async fn recv(&mut self, buf: &mut [u8]) -> io::Result<Recv> {
        poll_fn(|cx| self.poll_recv(cx, &mut ReadBuf::new(buf))).await
    }

    /// Receives the next event from the peer, see [`poll_recv_bytes`](Self::poll_recv_bytes).
    pub async fn recv_bytes(&mut self) -> io::Result<Recv<Bytes>> {
        poll_fn(|cx| self.poll_recv_bytes(cx)).await
    }

    /// Writes all of `data`, see [`poll_write`](Self::poll_write).
    pub async fn write_all(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let written = poll_fn(|cx| self.poll_write(cx, data)).await?;
            data = &data[written..];
        }
        Ok(())
    }

    /// Queues a text message, see [`poll_send_text`](Self::poll_send_text).
    pub async fn send_text(&mut self, text: &str) -> io::Result<()> {
        poll_fn(|cx| self.poll_send_text(cx, text)).await
    }

    /// Queues a Ping frame, see [`poll_send_ping`](Self::poll_send_ping).
    pub async fn send_ping(&mut self, payload: &[u8]) -> io::Result<()> {
        poll_fn(|cx| self.poll_send_ping(cx, payload)).await
    }

    /// Queues a Pong frame, see [`poll_send_pong`](Self::poll_send_pong).
    pub async fn send_pong(&mut self, payload: &[u8]) -> io::Result<()> {
        poll_fn(|cx| self.poll_send_pong(cx, payload)).await
    }

    /// Writes buffered frames and flushes the IO, see [`poll_flush`](Self::poll_flush).
    pub async fn flush(&mut self) -> io::Result<()> {
        poll_fn(|cx| self.poll_flush(cx)).await
    }

    /// Sends our Close frame, see [`poll_close`](Self::poll_close).
    pub async fn close(&mut self, frame: Option<&CloseFrame>) -> io::Result<()> {
        poll_fn(|cx| self.poll_close(cx, frame)).await
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncRead for WebSocketIO<IO> {
    /// Reads binary data.
    ///
    /// Pings and pongs are consumed (pings are still answered if
    /// [`Config::auto_pong`](Config::auto_pong) is enabled), a text message fails the read with
    /// [`io::ErrorKind::InvalidData`], and the peer's Close frame reads as EOF.
    ///
    /// Unlike [`WebSocketIO::poll_recv`], fills `buf` across frame and message boundaries, as long
    /// as the data is already buffered.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let start = buf.filled().len();
        // Whatever comes after already delivered data may only be taken if it is buffered.
        let mut buffered_only = this.read_buffered(buf).is_some();

        while buf.remaining() > 0 {
            let event = match this.poll_event(cx, buf, buffered_only) {
                Poll::Pending if buffered_only => break,
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) if buffered_only => {
                    this.recv.defer_error(error);
                    break;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(event)) => event,
            };
            match event {
                Recv::Binary { .. } => buffered_only = buf.filled().len() > start,
                Recv::Ping(..) | Recv::Pong(..) => {}
                Recv::Text(..) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received a text message",
                    )));
                }
                Recv::Close(..) => break,
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> AsyncWrite for WebSocketIO<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush(cx)
    }

    /// Sends a Close frame with [`CloseCode::NORMAL`] and flushes it.
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        const NORMAL: CloseFrame = CloseFrame::new(CloseCode::NORMAL);
        self.get_mut().poll_close(cx, Some(&NORMAL))
    }
}

impl<IO: fmt::Debug> fmt::Debug for WebSocketIO<IO> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebSocketIO")
            .field("io", &self.io)
            .field("role", &self.role)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
