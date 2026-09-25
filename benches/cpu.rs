//! CPU cost of the data paths, over in-memory IO that is always ready.
//!
//! The IO copies every byte once, like a kernel would, so the numbers include one unavoidable copy
//! on top of what the WebSocket layer does. Compares a server (no masking) with a client (random
//! masking keys). Everything runs on one thread and never blocks, so wall time is CPU time: the
//! reported time per iteration is the CPU cost of moving 4 MiB through one side.

use std::{
    hint::black_box,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll, Waker},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use websocket_io::{Config, Role, WebSocketIO};

const TOTAL: usize = 4 * 1024 * 1024;
const SIZES: [usize; 4] = [64, 1024, 16 * 1024, 256 * 1024];

#[derive(Clone, Copy)]
enum Mode {
    /// Server: unmasked frames.
    Server,
    /// Client: frames masked with random keys.
    Client,
}

impl Mode {
    const ALL: [Self; 2] = [Self::Server, Self::Client];

    fn name(self) -> &'static str {
        match self {
            Self::Server => "server",
            Self::Client => "client",
        }
    }

    fn role(self) -> Role {
        match self {
            Self::Server => Role::Server,
            Self::Client => Role::Client,
        }
    }

    fn peer_role(self) -> Role {
        match self.role() {
            Role::Server => Role::Client,
            Role::Client => Role::Server,
        }
    }
}

/// Accepts every write in full, copying the bytes into a scratch buffer. Reads never complete.
struct SinkIo {
    scratch: Vec<u8>,
    vectored: bool,
}

impl SinkIo {
    fn absorb(&mut self, data: &[u8]) {
        for chunk in data.chunks(self.scratch.len()) {
            self.scratch[..chunk.len()].copy_from_slice(chunk);
        }
    }
}

impl AsyncRead for SinkIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

impl AsyncWrite for SinkIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().absorb(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut written = 0;
        for buf in bufs {
            this.absorb(buf);
            written += buf.len();
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Serves `data` in a loop. Writes are discarded.
struct ReplayIo {
    data: Vec<u8>,
    pos: usize,
}

impl AsyncRead for ReplayIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let n = buf.remaining().min(this.data.len() - this.pos);
        buf.put_slice(&this.data[this.pos..this.pos + n]);
        this.pos = (this.pos + n) % this.data.len();
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ReplayIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Polls until ready. The IO types above never return `Pending` where it matters.
fn ready<T>(mut poll: impl FnMut(&mut Context<'_>) -> Poll<T>) -> T {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = poll(&mut cx) {
            return value;
        }
    }
}

fn write_all<IO: AsyncRead + AsyncWrite + Unpin>(
    ws: &mut WebSocketIO<IO>,
    data: &[u8],
    size: usize,
) {
    for chunk in data.chunks(size) {
        let mut rest = chunk;
        while !rest.is_empty() {
            let written = ready(|cx| ws.poll_write(cx, rest)).unwrap();
            rest = &rest[written..];
        }
    }
    ready(|cx| ws.poll_flush(cx)).unwrap();
}

/// Encodes `TOTAL` bytes, written in pieces of `size`, as `mode` would send them.
fn encode(mode: Mode, data: &[u8], size: usize) -> Vec<u8> {
    struct VecIo(Vec<u8>);
    impl AsyncRead for VecIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for VecIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().0.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let mut ws = WebSocketIO::new(VecIo(Vec::new()), mode.role(), Config::default());
    write_all(&mut ws, data, size);
    ws.into_inner().0
}

fn send(c: &mut Criterion) {
    let data = (0..TOTAL).map(|i| i as u8).collect::<Vec<_>>();
    let mut group = c.benchmark_group("send");
    group.throughput(Throughput::Bytes(TOTAL as u64));
    for size in SIZES {
        for mode in Mode::ALL {
            let io = SinkIo {
                scratch: vec![0; 1024 * 1024],
                vectored: true,
            };
            let mut ws = WebSocketIO::new(io, mode.role(), Config::default());
            group.bench_function(BenchmarkId::new(mode.name(), size), |b| {
                b.iter(|| write_all(&mut ws, black_box(&data), size))
            });
        }
    }
    group.finish();
}

fn recv(c: &mut Criterion) {
    let data = (0..TOTAL).map(|i| i as u8).collect::<Vec<_>>();
    let mut group = c.benchmark_group("recv");
    group.throughput(Throughput::Bytes(TOTAL as u64));
    let mut buf = vec![0; 64 * 1024];
    for size in SIZES {
        for mode in Mode::ALL {
            // The stream as `mode` sends it, received by its peer.
            let io = ReplayIo {
                data: encode(mode, &data, size),
                pos: 0,
            };
            let mut ws = WebSocketIO::new(io, mode.peer_role(), Config::default());
            group.bench_function(BenchmarkId::new(mode.name(), size), |b| {
                b.iter(|| {
                    let mut received = 0;
                    while received < TOTAL {
                        let limit = buf.len().min(TOTAL - received);
                        let mut read_buf = ReadBuf::new(&mut buf[..limit]);
                        ready(|cx| Pin::new(&mut ws).poll_read(cx, &mut read_buf)).unwrap();
                        received += read_buf.filled().len();
                    }
                    black_box(&buf);
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, send, recv);
criterion_main!(benches);
