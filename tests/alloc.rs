//! Memory behavior: buffers are held only while data is in flight, and the data path does not
//! allocate per message.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
};

use tokio::{
    io::{AsyncRead, AsyncReadExt, ReadBuf},
    net::{TcpListener, TcpStream},
};
use websocket_io::{Config, Recv, Role, WebSocketIO};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_add(
            new_size as isize - layout.size() as isize,
            Ordering::Relaxed,
        );
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// The counters are global, tests must not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Polls a read once, like a read loop waiting for more data, and expects nothing to be there.
fn park_reader<IO: AsyncRead + Unpin>(reader: &mut IO) {
    let mut buf = [0u8; 16];
    let mut buf = ReadBuf::new(&mut buf);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(reader).poll_read(&mut cx, &mut buf).is_pending());
}

#[test]
fn idle_connections_hold_no_buffers() {
    let _serial = SERIAL.lock().unwrap();
    runtime().block_on(async {
        for (sender_role, receiver_role) in
            [(Role::Client, Role::Server), (Role::Server, Role::Client)]
        {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (a, b) = tokio::join!(
                TcpStream::connect(listener.local_addr().unwrap()),
                listener.accept()
            );
            let mut sender = WebSocketIO::new(a.unwrap(), sender_role, Config::default());
            let mut receiver = WebSocketIO::new(b.unwrap().0, receiver_role, Config::default());
            let data = vec![42u8; 300 * 1024];
            let mut buf = vec![0u8; 64 * 1024];
            let mut pong = [0u8; 8];
            let baseline = LIVE_BYTES.load(Ordering::Relaxed);

            for size in [1, 100, 5000, 70_000, 300 * 1024] {
                let send = async {
                    sender.send_ping(b"ping").await.unwrap();
                    sender.write_all(&data[..size]).await.unwrap();
                    sender.flush().await.unwrap();
                };
                let receive = async {
                    let mut received = 0;
                    while received < size {
                        received += receiver.read(&mut buf).await.unwrap();
                    }
                };
                tokio::join!(send, receive);
                receiver.flush().await.unwrap();
                assert!(matches!(
                    sender.recv(&mut pong).await.unwrap(),
                    Recv::Pong(_)
                ));
            }

            park_reader(&mut sender);
            park_reader(&mut receiver);
            let retained = LIVE_BYTES.load(Ordering::Relaxed) - baseline;
            assert_eq!(
                retained, 0,
                "{sender_role:?} -> {receiver_role:?}: idle connection retains {retained} bytes"
            );
        }
    });
}

#[test]
fn allocations_do_not_grow_with_messages() {
    let _serial = SERIAL.lock().unwrap();
    runtime().block_on(async {
        for (sender_role, receiver_role) in
            [(Role::Client, Role::Server), (Role::Server, Role::Client)]
        {
            // Large enough to hold a whole burst, so that the reader never waits mid-burst.
            let (a, b) = tokio::io::duplex(16 * 1024 * 1024);
            let mut sender = WebSocketIO::new(a, sender_role, Config::default());
            let mut receiver = WebSocketIO::new(b, receiver_role, Config::default());
            let data = vec![42u8; 5000];
            let mut buf = vec![0u8; 64 * 1024];

            let mut burst = async || {
                let mut total = 0;
                for i in 0..1100 {
                    let size = if i % 11 == 0 { 5000 } else { 100 };
                    sender.write_all(&data[..size]).await.unwrap();
                    total += size;
                }
                sender.flush().await.unwrap();
                let mut received = 0;
                while received < total {
                    received += receiver.read(&mut buf).await.unwrap();
                }
            };

            // The first burst also grows the in-memory pipe's own buffer.
            burst().await;
            let before = ALLOCATIONS.load(Ordering::Relaxed);
            burst().await;
            let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
            // One buffer per side, whatever the number of messages.
            assert!(
                allocations <= 4,
                "{sender_role:?} -> {receiver_role:?}: {allocations} allocations for 1100 messages"
            );
        }
    });
}
