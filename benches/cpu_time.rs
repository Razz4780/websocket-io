//! CPU time spent per MiB moved, on each side of a loopback TCP connection.
//!
//! The sender and the receiver run on separate threads, pinned to separate CPUs, each with its own
//! current-thread runtime, and measure the CPU time of their own thread: the total with
//! `CLOCK_THREAD_CPUTIME_ID`, the user share with `getrusage(RUSAGE_THREAD)`. The user share is
//! sampled on scheduler ticks, so each measured run lasts about a second to keep it meaningful;
//! the total is exact.
//!
//! System time includes the kernel's socket copies. On loopback, part of the receiving network
//! stack runs in the sender's context, so some of the receiver's kernel work is charged to the
//! sender, the same way for every implementation.
//!
//! Run with `cargo bench --bench cpu_time [-- FILTER]`, where FILTER is matched against
//! `direction/implementation/size`. "masked" sends from the client, "unmasked" from the server.
//! Linux only.

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!(
        "the cpu_time benchmark measures per-thread CPU time, which it supports on Linux only"
    );
}

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        mem::MaybeUninit,
        net::{TcpListener, TcpStream as StdTcpStream},
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    use bytes::Bytes;
    use fastwebsockets::{Frame, Payload};
    use futures_util::{SinkExt, StreamExt};
    use tokio::{io::AsyncReadExt, net::TcpStream};
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role as TungRole},
    };
    use websocket_io::{Config, Role, WebSocketIO};

    /// Bytes moved in the warm-up run, which also sizes the measured runs.
    const WARM_UP_BYTES: usize = 32 * 1024 * 1024;
    /// Target duration of a measured run.
    const RUN_TIME: Duration = Duration::from_secs(1);
    /// Source data, sent in a loop. A multiple of every write size.
    const DATA_BYTES: usize = 64 * 1024 * 1024;
    /// Measured runs per configuration, after one warm-up run.
    const RUNS: usize = 5;
    const SIZES: [usize; 4] = [64, 1024, 16 * 1024, 256 * 1024];
    const MIB: f64 = (1024 * 1024) as f64;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Impl {
        WebSocketIO,
        Tungstenite,
        FastWebSockets,
    }

    impl Impl {
        fn name(self) -> &'static str {
            match self {
                Self::WebSocketIO => "websocket-io",
                Self::Tungstenite => "tokio-tungstenite",
                Self::FastWebSockets => "fastwebsockets",
            }
        }
    }

    /// CPU time of the calling thread.
    #[derive(Clone, Copy)]
    struct Cpu {
        total: Duration,
        system: Duration,
    }

    impl Cpu {
        fn now() -> Self {
            let mut time = MaybeUninit::<libc::timespec>::zeroed();
            // SAFETY: `time` is a valid out pointer.
            let ret =
                unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) };
            assert_eq!(ret, 0, "clock_gettime failed");
            // SAFETY: initialized by `clock_gettime`.
            let time = unsafe { time.assume_init() };

            let mut usage = MaybeUninit::<libc::rusage>::zeroed();
            // SAFETY: `usage` is a valid out pointer.
            let ret = unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) };
            assert_eq!(ret, 0, "getrusage failed");
            // SAFETY: initialized by `getrusage`.
            let usage = unsafe { usage.assume_init() };

            Self {
                total: Duration::new(time.tv_sec as u64, time.tv_nsec as u32),
                system: Duration::new(
                    usage.ru_stime.tv_sec as u64,
                    usage.ru_stime.tv_usec as u32 * 1000,
                ),
            }
        }

        fn since(self, earlier: Self) -> Self {
            Self {
                total: self.total - earlier.total,
                system: self.system.saturating_sub(earlier.system),
            }
        }
    }

    /// One measured run, as seen by one side.
    #[derive(Clone, Copy)]
    struct Sample {
        bytes: usize,
        cpu: Cpu,
        wall: Duration,
    }

    /// State shared by the two sides of one configuration.
    struct Shared {
        barrier: Barrier,
        /// Bytes per measured run, set by the receiver after the warm-up.
        run_bytes: AtomicUsize,
    }

    /// Runs `RUNS + 1` transfers (the first one is a warm-up), each started in lockstep with the
    /// other side, and returns the measured samples. `$bytes` is bound to the size of each run.
    /// The side with `$sizes_runs` sizes the measured runs after the warm-up.
    macro_rules! measure {
        ($shared:expr, $sizes_runs:expr, $bytes:ident, $run:expr) => {{
            let mut samples = Vec::with_capacity(RUNS);
            for run in 0..=RUNS {
                $shared.barrier.wait();
                let $bytes = if run == 0 {
                    WARM_UP_BYTES
                } else {
                    $shared.run_bytes.load(Ordering::Relaxed)
                };
                let (cpu, start) = (Cpu::now(), Instant::now());
                $run;
                let sample = Sample {
                    bytes: $bytes,
                    cpu: Cpu::now().since(cpu),
                    wall: start.elapsed(),
                };
                if run > 0 {
                    samples.push(sample);
                } else if $sizes_runs {
                    // Size the runs to last about `RUN_TIME`, in a multiple of every write size.
                    let rate = WARM_UP_BYTES as f64 / sample.wall.as_secs_f64();
                    let unit = 256 * 1024;
                    let bytes = ((rate * RUN_TIME.as_secs_f64()) as usize / unit).max(16) * unit;
                    $shared.run_bytes.store(bytes, Ordering::Relaxed);
                }
            }
            samples
        }};
    }

    /// Pins the calling thread to `cpu`, if the machine has it.
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

    /// Chunks of `size` bytes adding up to `bytes`, taken from `data` in a loop.
    fn chunks(data: &Bytes, size: usize, bytes: usize) -> impl Iterator<Item = Bytes> + '_ {
        (0..bytes / size).map(move |i| {
            let offset = (i * size) % DATA_BYTES;
            data.slice(offset..offset + size)
        })
    }

    fn tokio_stream(stream: StdTcpStream) -> TcpStream {
        stream.set_nodelay(true).unwrap();
        stream.set_nonblocking(true).unwrap();
        TcpStream::from_std(stream).unwrap()
    }

    fn send(
        imp: Impl,
        role: Role,
        stream: StdTcpStream,
        size: usize,
        data: Bytes,
        shared: &Shared,
    ) -> Vec<Sample> {
        pin(1);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stream = tokio_stream(stream);
            match imp {
                Impl::WebSocketIO => {
                    let mut ws = WebSocketIO::new(stream, role, Config::default());
                    measure!(shared, false, bytes, {
                        for chunk in chunks(&data, size, bytes) {
                            ws.write_all(&chunk).await.unwrap();
                        }
                        ws.flush().await.unwrap();
                    })
                }
                Impl::Tungstenite => {
                    let mut ws =
                        WebSocketStream::from_raw_socket(stream, tung_role(role), None).await;
                    measure!(shared, false, bytes, {
                        for chunk in chunks(&data, size, bytes) {
                            ws.feed(Message::Binary(chunk)).await.unwrap();
                        }
                        ws.flush().await.unwrap();
                    })
                }
                Impl::FastWebSockets => {
                    let mut ws =
                        fastwebsockets::WebSocket::after_handshake(stream, fast_role(role));
                    measure!(shared, false, bytes, {
                        for chunk in chunks(&data, size, bytes) {
                            ws.write_frame(Frame::binary(Payload::Borrowed(&chunk)))
                                .await
                                .unwrap();
                        }
                        ws.flush().await.unwrap();
                    })
                }
            }
        })
    }

    fn receive(imp: Impl, role: Role, stream: StdTcpStream, shared: &Shared) -> Vec<Sample> {
        pin(3);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let stream = tokio_stream(stream);
            match imp {
                Impl::WebSocketIO => {
                    let mut ws = WebSocketIO::new(stream, role, Config::default());
                    let mut buf = vec![0; 64 * 1024];
                    measure!(shared, true, bytes, {
                        let mut received = 0;
                        while received < bytes {
                            received += ws.read(&mut buf).await.unwrap();
                        }
                    })
                }
                Impl::Tungstenite => {
                    let mut ws =
                        WebSocketStream::from_raw_socket(stream, tung_role(role), None).await;
                    measure!(shared, true, bytes, {
                        let mut received = 0;
                        while received < bytes {
                            match ws.next().await.unwrap().unwrap() {
                                Message::Binary(data) => received += data.len(),
                                other => panic!("unexpected {other:?}"),
                            }
                        }
                    })
                }
                Impl::FastWebSockets => {
                    let mut ws =
                        fastwebsockets::WebSocket::after_handshake(stream, fast_role(role));
                    measure!(shared, true, bytes, {
                        let mut received = 0;
                        while received < bytes {
                            received += ws.read_frame().await.unwrap().payload.len();
                        }
                    })
                }
            }
        })
    }

    fn tung_role(role: Role) -> TungRole {
        match role {
            Role::Client => TungRole::Client,
            Role::Server => TungRole::Server,
        }
    }

    fn fast_role(role: Role) -> fastwebsockets::Role {
        match role {
            Role::Client => fastwebsockets::Role::Client,
            Role::Server => fastwebsockets::Role::Server,
        }
    }

    fn median(mut values: Vec<f64>) -> f64 {
        values.sort_by(f64::total_cmp);
        values[values.len() / 2]
    }

    /// Median CPU microseconds per MiB, total and user.
    fn per_mib(samples: &[Sample]) -> (f64, f64) {
        let scale = |sample: &Sample, time: Duration| {
            time.as_secs_f64() * 1e6 / (sample.bytes as f64 / MIB)
        };
        let total = samples.iter().map(|sample| scale(sample, sample.cpu.total));
        let user = samples
            .iter()
            .map(|sample| scale(sample, sample.cpu.total.saturating_sub(sample.cpu.system)));
        (median(total.collect()), median(user.collect()))
    }

    pub(super) fn main() {
        let filter = std::env::args()
            .skip(1)
            .find(|arg| !arg.starts_with("--"))
            .unwrap_or_default();
        let data = Bytes::from((0..DATA_BYTES).map(|i| i as u8).collect::<Vec<_>>());

        for (direction, sender_role) in [("masked", Role::Client), ("unmasked", Role::Server)] {
            let receiver_role = match sender_role {
                Role::Client => Role::Server,
                Role::Server => Role::Client,
            };
            let impls = [Impl::WebSocketIO, Impl::Tungstenite, Impl::FastWebSockets];

            println!(
                "\n{direction}: CPU µs per MiB moved (median of {RUNS} runs of ~{}s)",
                RUN_TIME.as_secs_f64()
            );
            println!(
                "{:>8}  {:<22} {:>12} {:>12} {:>14} {:>14} {:>10}",
                "size",
                "implementation",
                "sender",
                "sender user",
                "receiver",
                "receiver user",
                "wall MB/s"
            );
            for size in SIZES {
                for imp in impls {
                    let id = format!("{direction}/{}/{size}", imp.name());
                    if !id.contains(&filter) {
                        continue;
                    }

                    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                    let sender_stream =
                        StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
                    let receiver_stream = listener.accept().unwrap().0;
                    let shared = Arc::new(Shared {
                        barrier: Barrier::new(2),
                        run_bytes: AtomicUsize::new(0),
                    });

                    let sender = {
                        let (shared, data) = (shared.clone(), data.clone());
                        thread::spawn(move || {
                            send(imp, sender_role, sender_stream, size, data, &shared)
                        })
                    };
                    let receiver = thread::spawn(move || {
                        receive(imp, receiver_role, receiver_stream, &shared)
                    });
                    let sent = sender.join().unwrap();
                    let received = receiver.join().unwrap();

                    let (sender_total, sender_user) = per_mib(&sent);
                    let (receiver_total, receiver_user) = per_mib(&received);
                    let wall = median(
                        received
                            .iter()
                            .map(|sample| sample.bytes as f64 / sample.wall.as_secs_f64() / 1e6)
                            .collect(),
                    );
                    println!(
                        "{size:>8}  {:<22} {sender_total:>12.1} {sender_user:>12.1} {receiver_total:>14.1} {receiver_user:>14.1} {wall:>10.0}",
                        imp.name()
                    );
                }
            }
        }
    }
}
