# websocket-io

A Tokio-based WebSocket ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)) implementation that
turns a connection into a fast byte duplex, carried in binary messages.

`Sink` + `Stream` interfaces force data through owned messages, and plain `AsyncRead` +
`AsyncWrite` hide pings, text messages and close frames. `WebSocketIO` offers poll-based methods
that report all of them, while binary data flows as cheaply as through `AsyncRead`/`AsyncWrite`,
which are implemented too.

The crate starts where the HTTP upgrade ends: `WebSocketIO` wraps a connection that has already
switched protocols.

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use websocket_io::{Config, Recv, Role, WebSocketIO};

// `upgraded` is the connection after the `101 Switching Protocols` response.
let mut ws = WebSocketIO::new(upgraded, Role::Client, Config::default());

// Byte stream, via AsyncWrite or the inherent methods.
ws.write_all(b"hello").await?;
ws.flush().await?;

// Everything the peer sends, with binary data landing in `buf`.
let mut buf = vec![0; 64 * 1024];
match ws.recv(&mut buf).await? {
    Recv::Binary { data, end_of_message } => println!("{:?}", &buf[..data]),
    Recv::Text(text) => println!("text: {text}"),
    Recv::Ping(payload) => {} // already answered
    Recv::Pong(payload) => {}
    Recv::Close(frame) => {} // already replied to
}
```

## API

| Receiving | Sending |
|-----------|---------|
| `poll_recv(cx, &mut ReadBuf)`: binary data into the caller's buffer | `poll_write(cx, &[u8])`: binary data |
| `poll_recv_bytes(cx)`: binary data as `Bytes` split off the read buffer | `poll_send_text` / `poll_send_ping` / `poll_send_pong` |
| `AsyncRead`: binary data only, Close is EOF | `poll_flush`, `poll_close`, `AsyncWrite` (`poll_shutdown` sends Close) |

Every `poll_*` method has an `async` counterpart (`recv`, `recv_bytes`, `write_all`, `send_text`,
...). All errors are `io::Error`; protocol violations carry a `ProtocolError`.

## Design

**Receiving.** Binary payloads are streamed, never assembled: bytes are handed out as soon as they
arrive, whatever the frame and message sizes. A server unmasks the payload while copying it into
the caller's buffer, in one pass. When a large frame is in flight and nothing is buffered
internally, the IO reads straight into the caller's buffer. Text and control payloads, which have
to be complete to be useful, are the only ones collected, with size limits.

**Sending.** Every write becomes one complete binary message (one frame of at most
`max_frame_size` bytes). A client masks the payload while copying it into the write buffer, in one
pass. A server writes large payloads (at least half the write buffer) straight from the caller's
buffer, header and payload in one vectored write. Smaller writes are buffered until flushed, like
with tokio's `BufWriter`, and coalesce into fewer, larger writes to the IO.

A frame never waits for more data: when a write returns `Ready(n)`, the frame for those `n` bytes is
complete, its head already in the IO and its tail (if the IO did not take everything) copied into
the write buffer. Writing one byte and flushing sends one 1-byte frame. When the IO takes none of a
large write, nothing is accepted and the write returns `Pending`.

**Masking.** The kernels XOR 8-byte lanes in 64-byte blocks, which LLVM vectorizes. On x86-64 an
AVX2 build of the same code is selected at runtime. Client masking keys come from the OS entropy
source, 1024 keys per syscall. A peer's all-zero key skips the unmasking pass.

`Config::zero_mask_key(true)` makes a client mask its frames with an all-zero key: the frames stay
formally masked (servers accept them), but the payload goes out unchanged, so the client sends like
a server. **This violates RFC 6455**, which requires unpredictable keys to protect intermediaries
that do not understand WebSocket from payloads crafted to look like HTTP requests. Use it only when
an attacker cannot choose the payload, or no such intermediary can see the plaintext (e.g. TLS
terminated by the server).

**Control frames.** Pings are answered, and the peer's Close frame is replied to, by the receiving
methods themselves: the replies go out without waiting for the sending side to flush, so a separate
writer task is not needed. Receiving never blocks on a stalled write. Unanswered pings are
coalesced, so a ping flood cannot grow the write buffer unboundedly.

**Closing.** `poll_close` (and `poll_shutdown`) sends our Close frame; data the peer sent before its
Close frame is still delivered. By default the reply to the peer's Close frame is sent right away;
with `Config::defer_close_reply` it waits for our own `poll_close`, which gives TCP-like half-close.
Once both Close frames are exchanged, the IO is shut down. An EOF without a close handshake is an
error, so a truncated stream is always detected.

Not supported: the HTTP upgrade itself, extensions (permessage-deflate), splitting into halves.

## Performance

`cargo bench --bench throughput` moves 4 MiB one way over loopback TCP, in writes of the given
size, with both ends in one task on a current-thread runtime (so it compares CPU efficiency).
The websocket-io receiver reads through `AsyncRead` into a 64 KiB buffer. tokio-tungstenite gets
zero-copy `Bytes` slices, its best case. Throughput in GB/s, median, on a 4-vCPU x86-64 VM:

| Write size | websocket-io | websocket-io, zero key | tokio-tungstenite 0.30 | fastwebsockets 0.10 |
|---:|---:|---:|---:|---:|
| **Client → server (masked)** | | | | |
| 64 B | 0.64 | **0.82** | 0.24 | 0.02 |
| 1 KiB | 1.65 | **1.71** | 1.11 | 0.33 |
| 16 KiB | 1.92 | 1.90 | 1.57 | **2.10** |
| 256 KiB | 1.91 | **1.99** | 1.61 | 1.81 |
| **Server → client (unmasked)** | | | | |
| 64 B | **0.78** | | 0.24 | 0.02 |
| 1 KiB | **1.90** | | 1.20 | 0.33 |
| 16 KiB | 1.95 | | 1.69 | **2.36** |
| 256 KiB | 1.94 | | 1.75 | **2.24** |

Over TCP, the kernel copy dominates large writes, which hides most of the cost of masking.
`cargo bench --bench cpu` isolates the WebSocket layer: in-memory IO that is always ready and copies
every byte once (like a kernel would). GB/s, median:

| Write size | Send: client | Send: client, zero key | Send: server | Receive: from client | Receive: from zero-key client | Receive: from server |
|---:|---:|---:|---:|---:|---:|---:|
| 64 B | 2.03 | 3.94 | 4.21 | 2.05 | 2.41 | 2.44 |
| 1 KiB | 11.5 | 13.7 | 14.2 | 11.5 | 12.1 | 11.7 |
| 16 KiB | 15.0 | 17.1 | 16.9 | 15.3 | 16.2 | 15.6 |
| 256 KiB | 16.8 | 33.4 | 30.7 | 16.8 | 20.2 | 22.1 |

`cargo bench --bench mask` compares the masking kernels (in-place, GB/s):

| Size | websocket-io | tungstenite's kernel | byte-by-byte |
|---:|---:|---:|---:|
| 16 B | 6.3 | 2.0 | 1.7 |
| 256 B | 40.7 | 18.1 | 2.1 |
| 4 KiB | 60.3 | 45.3 | 2.2 |
| 64 KiB | 48.8 | 39.1 | 2.2 |
| 1 MiB | 46.5 | 36.9 | 2.2 |

## Testing

* Property tests of the masking kernels and the frame header codec.
* Interop tests against tokio-tungstenite, in both roles.
* Stress tests over an IO wrapper that splits reads and writes into random pieces and returns
  spurious `Pending`s, in both directions at once.
* Hand-crafted frames: fragmentation, control frames interleaved with fragments, UTF-8 split
  across frames, every protocol violation (checking the Close status code sent back).

## License

Apache-2.0
