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
| `poll_recv(cx, &mut ReadBuf)`: binary data into the caller's buffer | `poll_write(cx, &[u8])`, `poll_write_vectored(cx, &[IoSlice])`: binary data |
| `poll_recv_bytes(cx)`: binary data as `Bytes` split off the read buffer | `poll_send_text` / `poll_send_ping` / `poll_send_pong` |
| `AsyncRead`: binary data only, Close is EOF | `poll_flush`, `poll_close`, `AsyncWrite` (vectored; `poll_shutdown` sends Close) |

Every `poll_*` method has an `async` counterpart (`recv`, `recv_bytes`, `write_all`, `send_text`,
...). All errors are `io::Error`; protocol violations carry a `ProtocolError`.

## Design

**Receiving.** Binary payloads are streamed, never assembled: bytes are handed out as soon as they
arrive, whatever the frame and message sizes. A server unmasks the payload in bulk, everything
buffered of the current frame at once, so that small reads are plain copies. Reads of buffered
payload take a fast path. When a large frame is in flight and nothing is buffered internally, the
IO reads straight into the caller's buffer. Text and control payloads, which have to be complete to
be useful, are the only ones collected, with size limits.

**Sending.** Every write becomes complete binary messages, frames of at most `max_frame_size`
bytes, and a vectored write's frames span its buffers. Writes of at least half the write buffer go
to the IO right away, in one vectored write of up to 256 KiB:

* a server sends pieces of data of 256 bytes and more straight from the caller's buffers, and
  copies frame headers and shorter pieces (like length prefixes) into a per-thread scratch buffer,
  so that neighbouring ones go out as one buffer;
* a client masks its data into the scratch buffer, in one pass.

Smaller writes are buffered until flushed, like with tokio's `BufWriter`, and coalesce into fewer,
larger writes to the IO.

A frame never waits for more data: when a write returns `Ready(n)`, the frames for those `n` bytes
are complete, their head already in the IO and the tail of a partially written one copied into the
write buffer. Writing one byte and flushing sends one 1-byte frame. When the IO takes none of a
write, nothing is accepted and the write returns `Pending`.

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

**Memory.** Buffers are held only while data is in flight. The read buffer is freed as soon as a
read finds nothing more to read, the write buffer once a flush (or an automatic pong or Close reply)
drains it, so an idle connection holds no buffer memory; nothing is allocated per message. A busy
connection holds a read buffer (`read_buffer_size`, 16 KiB by default) and a write buffer
(`write_buffer_size`, 16 KiB by default; up to one frame, `max_frame_size`, 64 KiB by default, when
the IO takes only part of a large write). The scratch buffer for large writes is one 64 KiB buffer
per thread, shared by all connections.

Not supported: the HTTP upgrade itself, extensions (permessage-deflate), splitting into halves.

## Performance

All numbers are medians from a 4-vCPU x86-64 VM, over loopback TCP unless noted, with the default
configuration. Loopback results vary by about 10% between runs.

**Memory per connection.** 200 connections transfer data at once (1 MiB one way in 16 KiB writes,
256 KiB back in 1 KiB writes), then go idle, their readers waiting for more data. Heap growth per
connection pair, both ends:

| | Peak | Idle |
|---|---:|---:|
| **websocket-io** | **32 KiB** | **0** |
| websocket-io, 128 KiB read and write buffers | 256 KiB | 0 |
| tokio-tungstenite 0.30 | 389 KiB | 389 KiB |
| fastwebsockets 0.10 | 392 KiB | 392 KiB |

**Records: small reads, large vectored writes.** `cargo bench --bench usecase` streams records of a
4-byte length and a body of up to 16 KiB (or up to 1 KiB). The writer sends them in ~256 KiB
batches with vectored writes, one buffer per length and per body; the reader reads each record
with `read_exact` of the length, then of the body, so half of its reads are 4 bytes long. The
sender and the receiver run on separate threads, pinned to separate CPUs, and measure their own
CPU time. Microseconds of CPU per MiB:

| Records | Direction | Sender | Receiver | MB/s |
|---|---|---:|---:|---:|
| ≤16 KiB | server → client | 462 | 245 | 2,216 |
| ≤16 KiB | client → server | 578 | 276 | 1,795 |
| ≤1 KiB | server → client | 295 | 284 | 3,503 |
| ≤1 KiB | client → server | 416 | 327 | 2,525 |

Most of that is the kernel. The same workload over in-memory IO that copies every byte once (like a
kernel would) isolates the WebSocket layer; for reference, the receiving side compared to reading
the same records without any WebSocket framing, through a 16 KiB `BufReader`:

| Records | Send: server | Send: client | Receive from server | Receive from client | Plain `BufReader` |
|---|---:|---:|---:|---:|---:|
| ≤16 KiB | 19 | 59 | 142 | 163 | 131 |
| ≤1 KiB | 101 | 155 | 247 | 264 | 210 |

**Compared to other libraries.** `cargo bench --bench cpu_time` (Linux only) moves data one way in
plain (not vectored) writes of the given size, each a message; the websocket-io receiver reads
through `AsyncRead` into a 64 KiB buffer, and tokio-tungstenite sends zero-copy `Bytes` slices, its
best case. The other libraries use 128 KiB buffers by default, so websocket-io is shown both with
its memory-lean defaults and with 128 KiB buffers (and 256 KiB frames). Microseconds of CPU per MiB,
total (user + kernel) and user alone in parentheses:

| Write size | | websocket-io | websocket-io, 128 KiB buffers | tokio-tungstenite 0.30 | fastwebsockets 0.10 |
|---:|---|---:|---:|---:|---:|
| **Client → server (masked)** | | | | | |
| 64 B | sender | 1,932 (923) | **1,715 (982)** | 1,869 (1,623) | 85,046 (8,128) |
|  | receiver | 1,288 (786) | **926 (692)** | 3,132 (2,752) | 52,983 (11,704) |
| 1 KiB | sender | 780 (266) | 887 (261) | **780 (260)** | 5,428 (786) |
|  | receiver | 522 (184) | **370 (131)** | 510 (301) | 3,315 (635) |
| 16 KiB | sender | 823 (174) | **430 (144)** | 816 (190) | 784 (200) |
|  | receiver | 512 (101) | **259 (74)** | 397 (165) | 513 (96) |
| 256 KiB | sender | **589 (111)** | 741 (147) | 729 (141) | 700 (153) |
|  | receiver | **228 (74)** | 318 (116) | 287 (105) | 256 (57) |
| **Server → client (unmasked)** | | | | | |
| 64 B | sender | 1,403 (818) | **1,377 (845)** | 1,624 (1,394) | 75,510 (7,956) |
|  | receiver | 949 (545) | **702 (495)** | 2,863 (2,554) | 47,596 (9,808) |
| 1 KiB | sender | 812 (273) | **437 (190)** | 831 (244) | 5,606 (545) |
|  | receiver | 499 (127) | **242 (81)** | 493 (279) | 3,472 (678) |
| 16 KiB | sender | 621 (63) | **401 (132)** | 789 (177) | 674 (32) |
|  | receiver | 386 (83) | **232 (57)** | 368 (140) | 449 (62) |
| 256 KiB | sender | **527 (8)** | 535 (8) | 711 (138) | 531 (6) |
|  | receiver | 248 (37) | 236 (47) | 257 (76) | **217 (25)** |

**Masking kernels.** `cargo bench --bench mask`, in place, GB/s:

| Size | websocket-io | tungstenite's kernel | byte-by-byte |
|---:|---:|---:|---:|
| 16 B | 6.3 | 2.0 | 1.7 |
| 256 B | 40.7 | 18.1 | 2.1 |
| 4 KiB | 60.3 | 45.3 | 2.2 |
| 64 KiB | 48.8 | 39.1 | 2.2 |
| 1 MiB | 46.5 | 36.9 | 2.2 |

The `cpu` and `throughput` benches measure each side over in-memory IO, and both sides on one
thread, respectively.

## Testing

* Property tests of the masking kernels and the frame header codec.
* Interop tests against tokio-tungstenite, in both roles.
* Stress tests over an IO wrapper that splits reads and writes into random pieces and returns
  spurious `Pending`s, in both directions at once, with plain and vectored writes (random slices,
  some empty) and tiny reads through every receiving interface.
* Hand-crafted frames: fragmentation, control frames interleaved with fragments, UTF-8 split
  across frames, every protocol violation (checking the Close status code sent back), and the
  frames produced by vectored writes, including partially written ones.
* Memory: an idle connection retains no heap memory, and a burst of 1100 messages allocates at
  most one buffer per side.

## License

Apache-2.0
