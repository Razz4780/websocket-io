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

All numbers are medians from a 4-vCPU x86-64 VM.

**CPU time per MiB moved.** `cargo bench --bench cpu_time` (Linux only) moves data one way over
loopback TCP. The sender and the receiver run on separate threads, pinned to separate CPUs, each
with its own current-thread runtime, and measure their own thread's CPU time. The websocket-io
receiver reads through `AsyncRead` into a 64 KiB buffer; tokio-tungstenite sends zero-copy `Bytes`
slices, its best case. Microseconds of CPU per MiB, total (user + kernel), and user alone in
parentheses; the average of two runs, each the median of five one-second transfers. Kernel time includes the socket copies; on loopback, part of the receiving network
stack runs in the sender's context, the same way for every implementation.

| Write size | | websocket-io | tokio-tungstenite 0.30 | fastwebsockets 0.10 |
|---:|---|---:|---:|---:|
| **Client → server (masked)** | | | | |
| 64 B | sender | **1,669 (883)** | 1,869 (1,623) | 85,046 (8,128) |
|  | receiver | **828 (582)** | 3,132 (2,752) | 52,983 (11,704) |
| 1 KiB | sender | 901 (228) | **780 (260)** | 5,428 (786) |
|  | receiver | **365 (94)** | 510 (301) | 3,315 (635) |
| 16 KiB | sender | **435 (143)** | 816 (190) | 784 (200) |
|  | receiver | **252 (57)** | 397 (165) | 513 (96) |
| 256 KiB | sender | **656 (121)** | 729 (141) | 700 (153) |
|  | receiver | **240 (45)** | 287 (105) | 256 (57) |
| **Server → client (unmasked)** | | | | |
| 64 B | sender | **1,349 (754)** | 1,624 (1,394) | 75,510 (7,956) |
|  | receiver | **689 (460)** | 2,863 (2,554) | 47,596 (9,808) |
| 1 KiB | sender | **424 (176)** | 831 (244) | 5,606 (545) |
|  | receiver | **238 (86)** | 493 (279) | 3,472 (678) |
| 16 KiB | sender | **392 (135)** | 789 (177) | 674 (32) |
|  | receiver | **218 (56)** | 368 (140) | 449 (62) |
| 256 KiB | sender | **523 (4)** | 711 (138) | 531 (6) |
|  | receiver | 218 (37) | 257 (76) | **217 (25)** |

**Throughput, both ends on one thread.** `cargo bench --bench throughput` runs the same transfer
with both ends in one task on a current-thread runtime, so the throughput reflects the combined
CPU cost of both sides. GB/s:

| Write size | websocket-io | tokio-tungstenite 0.30 | fastwebsockets 0.10 |
|---:|---:|---:|---:|
| **Client → server (masked)** | | | |
| 64 B | **0.64** | 0.24 | 0.02 |
| 1 KiB | **1.65** | 1.11 | 0.33 |
| 16 KiB | 1.92 | 1.57 | **2.10** |
| 256 KiB | **1.91** | 1.61 | 1.81 |
| **Server → client (unmasked)** | | | |
| 64 B | **0.78** | 0.24 | 0.02 |
| 1 KiB | **1.90** | 1.20 | 0.33 |
| 16 KiB | 1.95 | 1.69 | **2.36** |
| 256 KiB | 1.94 | 1.75 | **2.24** |

**WebSocket layer alone.** `cargo bench --bench cpu` runs each side over in-memory IO that is
always ready and copies every byte once (like a kernel would), on one thread that never blocks, so
wall time is CPU time. Microseconds of CPU per MiB:

| Write size | Send: client (masking) | Send: server | Receive: from client (unmasking) | Receive: from server |
|---:|---:|---:|---:|---:|
| 64 B | 516 | 249 | 512 | 430 |
| 1 KiB | 91 | 74 | 92 | 90 |
| 16 KiB | 70 | 62 | 68 | 67 |
| 256 KiB | 62 | 34 | 63 | 47 |

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
