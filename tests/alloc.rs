//! The binary data path does not allocate once warmed up.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

use tokio::io::AsyncReadExt;
use websocket_io::{Config, Recv, Role, WebSocketIO};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[test]
fn steady_state_does_not_allocate() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        for (sender_role, receiver_role) in
            [(Role::Client, Role::Server), (Role::Server, Role::Client)]
        {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            let mut sender = WebSocketIO::new(a, sender_role, Config::default());
            let mut receiver = WebSocketIO::new(b, receiver_role, Config::default());
            let data = vec![42u8; 300 * 1024];
            let mut buf = vec![0u8; 64 * 1024];

            let mut round = async |sizes: &[usize]| {
                for &size in sizes {
                    sender.send_ping(b"ping").await.unwrap();
                    sender.write_all(&data[..size]).await.unwrap();
                    sender.flush().await.unwrap();
                    let mut received = 0;
                    while received < size {
                        received += receiver.read(&mut buf).await.unwrap();
                    }
                    // The pong is written by the receiving side, but only as far as the IO takes it
                    // without blocking; a flush guarantees it is out.
                    receiver.flush().await.unwrap();
                    // The pong comes back to the sender.
                    let mut pong = [0u8; 8];
                    assert!(matches!(
                        sender.recv(&mut pong).await.unwrap(),
                        Recv::Pong(_)
                    ));
                }
            };

            let sizes = [1, 100, 5000, 70_000, 300 * 1024];
            round(&sizes).await;
            let before = ALLOCATIONS.load(Ordering::Relaxed);
            for _ in 0..20 {
                round(&sizes).await;
            }
            let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before;
            // Received pings and pongs are handed out as `Bytes` split off the read buffer, which
            // may cost a reallocation of the buffer now and then. Binary data must not allocate.
            assert!(
                allocations < 10,
                "{sender_role:?} -> {receiver_role:?}: {allocations} allocations"
            );
        }
    });
}
