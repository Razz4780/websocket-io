/// The side of the connection a [`WebSocketIO`](crate::WebSocketIO) plays.
///
/// Clients mask every frame they send and expect unmasked frames from the server. Servers send
/// unmasked frames and expect masked frames from the client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Role {
    /// The endpoint that sent the HTTP upgrade request.
    Client,
    /// The endpoint that accepted the HTTP upgrade request.
    Server,
}

/// Tuning knobs and protocol options of a [`WebSocketIO`](crate::WebSocketIO).
///
/// ```
/// use websocket_io::Config;
///
/// let config = Config::default()
///     .read_buffer_size(64 * 1024)
///     .max_frame_size(256 * 1024);
/// ```
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) read_buffer_size: usize,
    pub(crate) write_buffer_size: usize,
    pub(crate) max_frame_size: usize,
    pub(crate) max_message_size: usize,
    pub(crate) auto_pong: bool,
    pub(crate) defer_close_reply: bool,
    pub(crate) accept_unmasked_frames: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            read_buffer_size: 16 * 1024,
            write_buffer_size: 16 * 1024,
            max_frame_size: 64 * 1024,
            max_message_size: 16 * 1024 * 1024,
            auto_pong: true,
            defer_close_reply: false,
            accept_unmasked_frames: false,
        }
    }
}

impl Config {
    /// Size of the internal read buffer, and the amount of data requested from the IO per read.
    ///
    /// The buffer is allocated when data arrives and freed as soon as a read finds nothing more to
    /// read, so idle connections hold no read buffer. Payload of large frames is read straight into
    /// the caller's buffer instead, when this buffer is empty.
    ///
    /// Larger buffers mean fewer reads (and less CPU per byte) when data is queued up, at the cost
    /// of memory per busy connection. Defaults to 16 KiB. Values below 1 KiB are rounded up.
    #[must_use]
    pub fn read_buffer_size(mut self, size: usize) -> Self {
        self.read_buffer_size = size.max(1024);
        self
    }

    /// Amount of outgoing data buffered before writes start to drain the buffer to the IO.
    ///
    /// Small writes are coalesced into this buffer and reach the IO on flush, or once the buffer
    /// fills up. Clients also mask their payload into it. The buffer is freed once a flush (or a
    /// pong or Close reply sent by the receiving side) drains it, so idle connections hold no write
    /// buffer.
    ///
    /// Larger buffers mean fewer writes (and less CPU per byte) for streams of small writes, at the
    /// cost of memory per busy connection. Defaults to 16 KiB.
    #[must_use]
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.write_buffer_size = size;
        self
    }

    /// Maximum payload size of outgoing binary frames.
    ///
    /// A single write never produces a frame larger than this, it accepts only a prefix of the
    /// data instead. Larger frames mean fewer headers and syscalls for large writes, and let the
    /// peer read large payloads straight into its caller's buffer. They also raise memory use:
    /// a client copies (and masks) a whole frame into its write buffer, and when the IO accepts
    /// only a part of an unmasked frame written straight from the caller's buffer, the rest is
    /// copied into the write buffer. Defaults to 64 KiB. Values below 1 byte are rounded up.
    #[must_use]
    pub fn max_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = size.max(1);
        self
    }

    /// Maximum size of incoming text messages.
    ///
    /// Text messages are assembled in memory before they are handed out, so they need a limit.
    /// Binary data is streamed and is not subject to this limit. Defaults to 16 MiB.
    #[must_use]
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Whether received pings are answered automatically. Defaults to `true`.
    ///
    /// Pings are reported to the caller either way.
    #[must_use]
    pub fn auto_pong(mut self, enabled: bool) -> Self {
        self.auto_pong = enabled;
        self
    }

    /// Whether the reply to the peer's Close frame waits for the local side to close.
    ///
    /// By default (`false`) the reply is sent as soon as the peer's Close frame is read, which
    /// ends the outgoing stream as well. With `true` the outgoing stream stays open until
    /// [`WebSocketIO::poll_close`](crate::WebSocketIO::poll_close) (or `poll_shutdown`) is called,
    /// which gives TCP-like half-close semantics.
    #[must_use]
    pub fn defer_close_reply(mut self, enabled: bool) -> Self {
        self.defer_close_reply = enabled;
        self
    }

    /// Whether a server accepts unmasked frames from the client. Defaults to `false`.
    ///
    /// RFC 6455 requires servers to fail the connection when a client frame is not masked.
    #[must_use]
    pub fn accept_unmasked_frames(mut self, enabled: bool) -> Self {
        self.accept_unmasked_frames = enabled;
        self
    }
}
