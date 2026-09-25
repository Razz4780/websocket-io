//! A record-oriented workload: the stream is a sequence of records, each a 4-byte big-endian
//! length followed by a body.
//!
//! * The writer sends batches of about 256 KiB of records with vectored writes, one `IoSlice` per
//!   header and per body.
//! * The reader reads each record with two `read_exact` calls: the 4-byte header, then the body.
//!   Half of its reads are 4 bytes long, none is longer than 16 KiB.
//!
//! Reports CPU microseconds per MiB of stream data, for the sender and the receiver.
//!
//! * `memory` mode runs each side alone on one thread, against in-memory IO that is always ready
//!   and copies every byte once (like a kernel would). Wall time is CPU time.
//! * `tcp` mode (Linux only) runs both sides at once over loopback TCP, on separate threads pinned
//!   to separate CPUs, and measures each thread's CPU time.
//!
//! Run with `cargo bench --bench usecase [-- memory|tcp [FILTER]]`. In tcp mode, FILTER selects
//! configurations, e.g. `16k/server` or `1k/client`.

use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use websocket_io::{Config, Role, WebSocketIO};

/// Stream bytes per measured run in `memory` mode.
const MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Target size of a vectored write.
const BATCH_BYTES: usize = 256 * 1024;
const RUNS: usize = 5;
const MIB: f64 = (1024 * 1024) as f64;

/// A deterministic sequence of records, with bodies sliced from shared data.
struct Records {
    headers: Vec<[u8; 4]>,
    bodies: Vec<(usize, usize)>,
    data: Vec<u8>,
    /// Records grouped into batches, as ranges of record indices.
    batches: Vec<(usize, usize)>,
    /// Total stream bytes, headers included.
    bytes: usize,
}

impl Records {
    fn new(max_body: usize, target_bytes: usize) -> Self {
        let data = (0..64 * 1024).map(|i| i as u8).collect::<Vec<_>>();
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let (mut headers, mut bodies, mut batches) = (Vec::new(), Vec::new(), Vec::new());
        let (mut bytes, mut batch_start, mut batch_bytes) = (0, 0, 0);
        while bytes < target_bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = 1 + (state % max_body as u64) as usize;
            let start = (state >> 32) as usize % (data.len() - len);
            headers.push((len as u32).to_be_bytes());
            bodies.push((start, len));
            bytes += 4 + len;
            batch_bytes += 4 + len;
            if batch_bytes >= BATCH_BYTES {
                batches.push((batch_start, headers.len()));
                batch_start = headers.len();
                batch_bytes = 0;
            }
        }
        if batch_start < headers.len() {
            batches.push((batch_start, headers.len()));
        }
        Self {
            headers,
            bodies,
            data,
            batches,
            bytes,
        }
    }

    fn slices(&self, (start, end): (usize, usize)) -> Vec<IoSlice<'_>> {
        let mut slices = Vec::with_capacity(2 * (end - start));
        for i in start..end {
            let (offset, len) = self.bodies[i];
            slices.push(IoSlice::new(&self.headers[i]));
            slices.push(IoSlice::new(&self.data[offset..offset + len]));
        }
        slices
    }
}

/// Writes all of `slices`, like the unstable `write_all_vectored`.
async fn write_all_vectored<W: AsyncWrite + Unpin>(
    writer: &mut W,
    mut slices: &mut [IoSlice<'_>],
) -> io::Result<()> {
    while !slices.is_empty() {
        let written =
            std::future::poll_fn(|cx| Pin::new(&mut *writer).poll_write_vectored(cx, slices))
                .await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}

async fn send_records<W: AsyncWrite + Unpin>(writer: &mut W, records: &Records) {
    for &batch in &records.batches {
        let mut slices = records.slices(batch);
        write_all_vectored(writer, &mut slices).await.unwrap();
    }
    std::future::poll_fn(|cx| Pin::new(&mut *writer).poll_flush(cx))
        .await
        .unwrap();
}

async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) {
    let mut read_buf = ReadBuf::new(buf);
    while read_buf.remaining() > 0 {
        let before = read_buf.filled().len();
        std::future::poll_fn(|cx| Pin::new(&mut *reader).poll_read(cx, &mut read_buf))
            .await
            .unwrap();
        assert!(read_buf.filled().len() > before, "unexpected EOF");
    }
}

/// Reads records until `bytes` stream bytes are consumed.
async fn receive_records<R: AsyncRead + Unpin>(reader: &mut R, bytes: usize, body: &mut [u8]) {
    let mut consumed = 0;
    while consumed < bytes {
        let mut header = [0; 4];
        read_exact(reader, &mut header).await;
        let len = u32::from_be_bytes(header) as usize;
        read_exact(reader, &mut body[..len]).await;
        consumed += 4 + len;
    }
}

/// Runs a future that never waits (the IO below is always ready).
fn run<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
    }
}

/// Accepts every write in full, copying the bytes into a scratch buffer.
struct SinkIo {
    scratch: Vec<u8>,
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
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Collects writes.
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

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut written = 0;
        for buf in bufs {
            this.0.extend_from_slice(buf);
            written += buf.len();
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Serves `data` in a loop, in pieces of at most `max_read` bytes (like a socket delivering what
/// has arrived). Writes are discarded.
struct ReplayIo {
    data: Vec<u8>,
    pos: usize,
    max_read: usize,
}

impl AsyncRead for ReplayIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let n = buf
            .remaining()
            .min(this.max_read)
            .min(this.data.len() - this.pos);
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

fn peer(role: Role) -> Role {
    match role {
        Role::Client => Role::Server,
        Role::Server => Role::Client,
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn per_mib(time: Duration, bytes: usize) -> f64 {
    time.as_secs_f64() * 1e6 / (bytes as f64 / MIB)
}

/// The floor for the receiving side: the same records without WebSocket framing, read through a
/// plain 16 KiB `BufReader`.
fn plain_reader_floor(records: &Records) -> f64 {
    let mut stream = Vec::with_capacity(records.bytes);
    for (header, &(offset, len)) in records.headers.iter().zip(&records.bodies) {
        stream.extend_from_slice(header);
        stream.extend_from_slice(&records.data[offset..offset + len]);
    }
    let mut reader = tokio::io::BufReader::with_capacity(
        16 * 1024,
        ReplayIo {
            data: stream,
            pos: 0,
            max_read: 64 * 1024,
        },
    );
    let mut body = vec![0; 16 * 1024];
    run(receive_records(&mut reader, records.bytes, &mut body));
    median(
        (0..RUNS)
            .map(|_| {
                let start = Instant::now();
                run(receive_records(&mut reader, records.bytes, &mut body));
                per_mib(start.elapsed(), records.bytes)
            })
            .collect(),
    )
}

fn memory_mode() {
    println!("memory: CPU µs per MiB of stream data (median of {RUNS} runs)");
    println!(
        "{:<10} {:<8} {:>10} {:>10}",
        "records", "sender", "send", "receive"
    );
    for max_body in [16 * 1024, 1024] {
        let records = Records::new(max_body, MEMORY_BYTES);
        println!(
            "{:<10} {:<8} {:>10} {:>10.1}   (plain BufReader, no WebSocket)",
            format!("≤{} KiB", max_body / 1024),
            "-",
            "-",
            plain_reader_floor(&records),
        );
        for role in [Role::Server, Role::Client] {
            // Sending.
            let mut ws = WebSocketIO::new(
                SinkIo {
                    scratch: vec![0; 1024 * 1024],
                },
                role,
                Config::default(),
            );
            run(send_records(&mut ws, &records));
            let send = median(
                (0..RUNS)
                    .map(|_| {
                        let start = Instant::now();
                        run(send_records(&mut ws, &records));
                        per_mib(start.elapsed(), records.bytes)
                    })
                    .collect(),
            );

            // Receiving what `role` sends, as its peer. The replay hands out 64 KiB per read.
            let mut encoder = WebSocketIO::new(VecIo(Vec::new()), role, Config::default());
            run(send_records(&mut encoder, &records));
            let stream = encoder.into_inner().0;
            let mut ws = WebSocketIO::new(
                ReplayIo {
                    data: stream,
                    pos: 0,
                    max_read: 64 * 1024,
                },
                peer(role),
                Config::default(),
            );
            let mut body = vec![0; 16 * 1024];
            run(receive_records(&mut ws, records.bytes, &mut body));
            let receive = median(
                (0..RUNS)
                    .map(|_| {
                        let start = Instant::now();
                        run(receive_records(&mut ws, records.bytes, &mut body));
                        per_mib(start.elapsed(), records.bytes)
                    })
                    .collect(),
            );

            println!(
                "{:<10} {:<8} {send:>10.1} {receive:>10.1}",
                format!("≤{} KiB", max_body / 1024),
                format!("{role:?}").to_lowercase(),
            );
        }
    }
}

#[cfg(target_os = "linux")]
mod tcp {
    use std::{
        mem::MaybeUninit,
        net::{TcpListener, TcpStream as StdTcpStream},
        sync::{Arc, Barrier},
        thread,
        time::{Duration, Instant},
    };

    use tokio::net::TcpStream;
    use websocket_io::{Config, Role, WebSocketIO};

    use super::{RUNS, Records, median, peer, per_mib, receive_records, send_records};

    /// Stream bytes per measured run; about a second on loopback. `USECASE_TCP_MIB` overrides it.
    fn tcp_bytes() -> usize {
        std::env::var("USECASE_TCP_MIB")
            .ok()
            .and_then(|mib| mib.parse::<usize>().ok())
            .unwrap_or(1024)
            * 1024
            * 1024
    }

    fn thread_cpu() -> Duration {
        let mut time = MaybeUninit::<libc::timespec>::zeroed();
        // SAFETY: `time` is a valid out pointer.
        let ret = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) };
        assert_eq!(ret, 0);
        // SAFETY: initialized by `clock_gettime`.
        let time = unsafe { time.assume_init() };
        Duration::new(time.tv_sec as u64, time.tv_nsec as u32)
    }

    fn pin(cpu: usize) {
        if thread::available_parallelism().map_or(0, |n| n.get()) <= cpu {
            return;
        }
        // SAFETY: `set` is a valid, initialized CPU set.
        unsafe {
            let mut set = MaybeUninit::<libc::cpu_set_t>::zeroed().assume_init();
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set);
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn tokio_stream(stream: StdTcpStream) -> TcpStream {
        stream.set_nodelay(true).unwrap();
        stream.set_nonblocking(true).unwrap();
        TcpStream::from_std(stream).unwrap()
    }

    pub(super) fn tcp_mode(filter: &str) {
        println!("tcp: CPU µs per MiB of stream data (median of {RUNS} runs of ~1 GiB)");
        println!(
            "{:<10} {:<8} {:>10} {:>10} {:>10}",
            "records", "sender", "send", "receive", "MB/s"
        );
        for max_body in [16 * 1024, 1024] {
            let records = Arc::new(Records::new(max_body, tcp_bytes()));
            for role in [Role::Server, Role::Client] {
                let role_name = format!("{role:?}").to_lowercase();
                if !format!("{}k/{role_name}", max_body / 1024).contains(filter) {
                    continue;
                }
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let a = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
                let b = listener.accept().unwrap().0;
                let barrier = Arc::new(Barrier::new(2));

                let sender = {
                    let (records, barrier) = (records.clone(), barrier.clone());
                    thread::spawn(move || {
                        pin(1);
                        runtime().block_on(async {
                            let mut ws = WebSocketIO::new(tokio_stream(a), role, Config::default());
                            let mut samples = Vec::new();
                            for run in 0..=RUNS {
                                barrier.wait();
                                let cpu = thread_cpu();
                                send_records(&mut ws, &records).await;
                                if run > 0 {
                                    samples.push(thread_cpu() - cpu);
                                }
                            }
                            samples
                        })
                    })
                };
                let receiver = {
                    let records = records.clone();
                    thread::spawn(move || {
                        pin(3);
                        runtime().block_on(async {
                            let mut ws =
                                WebSocketIO::new(tokio_stream(b), peer(role), Config::default());
                            let mut body = vec![0; 16 * 1024];
                            let mut samples = Vec::new();
                            for run in 0..=RUNS {
                                barrier.wait();
                                let (cpu, start) = (thread_cpu(), Instant::now());
                                receive_records(&mut ws, records.bytes, &mut body).await;
                                if run > 0 {
                                    samples.push((thread_cpu() - cpu, start.elapsed()));
                                }
                            }
                            samples
                        })
                    })
                };
                let sent = sender.join().unwrap();
                let received = receiver.join().unwrap();

                let send = median(
                    sent.iter()
                        .map(|&cpu| per_mib(cpu, records.bytes))
                        .collect(),
                );
                let receive = median(
                    received
                        .iter()
                        .map(|&(cpu, _)| per_mib(cpu, records.bytes))
                        .collect(),
                );
                let rate = median(
                    received
                        .iter()
                        .map(|&(_, wall)| records.bytes as f64 / wall.as_secs_f64() / 1e6)
                        .collect(),
                );
                println!(
                    "{:<10} {:<8} {send:>10.1} {receive:>10.1} {rate:>10.0}",
                    format!("≤{} KiB", max_body / 1024),
                    format!("{role:?}").to_lowercase(),
                );
            }
        }
    }
}

fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|arg| !arg.starts_with("--"))
        .collect::<Vec<_>>();
    let mode = args.first().map_or("", String::as_str);
    let filter = args.get(1).map_or("", String::as_str);
    if mode.is_empty() || mode == "memory" {
        memory_mode();
    }
    if mode.is_empty() || mode == "tcp" {
        #[cfg(target_os = "linux")]
        tcp::tcp_mode(filter);
        #[cfg(not(target_os = "linux"))]
        eprintln!("tcp mode measures per-thread CPU time, which it supports on Linux only");
    }
}
