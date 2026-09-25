//! Moves data one way over a loopback TCP connection, in messages (writes) of a given size.
//!
//! "masked" sends from the client (the payload has to be masked), "unmasked" from the server.
//! Both ends run in one task on a current-thread runtime, so the numbers compare CPU efficiency.

use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fastwebsockets::{Frame, Payload};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    runtime::Builder,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role as TungRole},
};
use websocket_io::{Config, Role, WebSocketIO};

const TOTAL: usize = 4 * 1024 * 1024;
const SIZES: [usize; 4] = [64, 1024, 16 * 1024, 256 * 1024];

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    let (client, server) = (client.unwrap(), server.unwrap().0);
    client.set_nodelay(true).unwrap();
    server.set_nodelay(true).unwrap();
    (client, server)
}

/// Sender role, receiver role.
fn roles(masked: bool) -> (Role, Role) {
    if masked {
        (Role::Client, Role::Server)
    } else {
        (Role::Server, Role::Client)
    }
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

async fn run_websocket_io(masked: bool, size: usize, data: &Bytes, iters: u64) -> Duration {
    let (a, b) = tcp_pair().await;
    let (send_role, recv_role) = roles(masked);
    let mut sender = WebSocketIO::new(a, send_role, Config::default());
    let mut receiver = WebSocketIO::new(b, recv_role, Config::default());
    let mut buf = vec![0; 64 * 1024];

    let start = Instant::now();
    for _ in 0..iters {
        let send = async {
            for chunk in data.chunks(size) {
                sender.write_all(chunk).await.unwrap();
            }
            sender.flush().await.unwrap();
        };
        let receive = async {
            let mut received = 0;
            while received < TOTAL {
                received += receiver.read(&mut buf).await.unwrap();
            }
        };
        tokio::join!(send, receive);
    }
    start.elapsed()
}

async fn run_tungstenite(masked: bool, size: usize, data: &Bytes, iters: u64) -> Duration {
    let (a, b) = tcp_pair().await;
    let (send_role, recv_role) = roles(masked);
    let mut sender = WebSocketStream::from_raw_socket(a, tung_role(send_role), None).await;
    let mut receiver = WebSocketStream::from_raw_socket(b, tung_role(recv_role), None).await;

    let start = Instant::now();
    for _ in 0..iters {
        let send = async {
            for offset in (0..TOTAL).step_by(size) {
                // Zero-copy slices: the best case for a `Sink` of owned messages.
                let message = data.slice(offset..offset + size);
                sender.feed(Message::Binary(message)).await.unwrap();
            }
            sender.flush().await.unwrap();
        };
        let receive = async {
            let mut received = 0;
            while received < TOTAL {
                match receiver.next().await.unwrap().unwrap() {
                    Message::Binary(data) => received += data.len(),
                    other => panic!("unexpected {other:?}"),
                }
            }
        };
        tokio::join!(send, receive);
    }
    start.elapsed()
}

async fn run_fastwebsockets(masked: bool, size: usize, data: &Bytes, iters: u64) -> Duration {
    let (a, b) = tcp_pair().await;
    let (send_role, recv_role) = roles(masked);
    let mut sender = fastwebsockets::WebSocket::after_handshake(a, fast_role(send_role));
    let mut receiver = fastwebsockets::WebSocket::after_handshake(b, fast_role(recv_role));

    let start = Instant::now();
    for _ in 0..iters {
        let send = async {
            for chunk in data.chunks(size) {
                sender
                    .write_frame(Frame::binary(Payload::Borrowed(chunk)))
                    .await
                    .unwrap();
            }
            sender.flush().await.unwrap();
        };
        let receive = async {
            let mut received = 0;
            while received < TOTAL {
                received += receiver.read_frame().await.unwrap().payload.len();
            }
        };
        tokio::join!(send, receive);
    }
    start.elapsed()
}

fn throughput(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let data = Bytes::from((0..TOTAL).map(|i| i as u8).collect::<Vec<_>>());

    for masked in [true, false] {
        let mut group = c.benchmark_group(if masked { "masked" } else { "unmasked" });
        group.throughput(Throughput::Bytes(TOTAL as u64));
        group.sample_size(10);
        for size in SIZES {
            group.bench_function(BenchmarkId::new("websocket-io", size), |b| {
                b.to_async(&rt)
                    .iter_custom(|iters| run_websocket_io(masked, size, &data, iters))
            });
            group.bench_function(BenchmarkId::new("tokio-tungstenite", size), |b| {
                b.to_async(&rt)
                    .iter_custom(|iters| run_tungstenite(masked, size, &data, iters))
            });
            group.bench_function(BenchmarkId::new("fastwebsockets", size), |b| {
                b.to_async(&rt)
                    .iter_custom(|iters| run_fastwebsockets(masked, size, &data, iters))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, throughput);
criterion_main!(benches);
