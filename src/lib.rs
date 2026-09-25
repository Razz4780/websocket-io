//! A Tokio-based WebSocket ([RFC 6455]) implementation that turns a connection into a fast byte
//! duplex, carried in binary messages.
//!
//! The crate starts where the HTTP upgrade ends: [`WebSocketIO`] wraps a connection that has
//! already switched protocols. It exposes:
//!
//! * [`AsyncRead`](tokio::io::AsyncRead) + [`AsyncWrite`](tokio::io::AsyncWrite), for when the
//!   connection is just a byte pipe;
//! * poll-based methods ([`WebSocketIO::poll_recv`], [`WebSocketIO::poll_write`], ...), for when
//!   text messages, pings, pongs and close frames matter too. The binary data path costs the same
//!   as in the trait implementations.
//!
//! # Performance
//!
//! * Binary payloads are streamed, never assembled into messages.
//! * Incoming data is unmasked while being copied into the caller's buffer, or read straight into
//!   it when a large frame is in flight.
//! * Outgoing data is masked while being copied into the write buffer (clients), or written
//!   straight from the caller's buffer with vectored writes (servers, large writes).
//! * Masking uses vectorized kernels, with an AVX2 variant picked at runtime on x86-64.
//! * Buffers are held only while data is in flight: an idle connection holds no buffer memory,
//!   and nothing is allocated per message.
//!
//! Extensions (e.g. permessage-deflate) are not supported, frames with RSV bits set fail the
//! connection.
//!
//! # Example
//!
//! ```
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! use websocket_io::{Config, Role, WebSocketIO};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> std::io::Result<()> {
//! // Stand-ins for the two ends of an upgraded HTTP/1 connection.
//! let (client_io, server_io) = tokio::io::duplex(64 * 1024);
//! let mut client = WebSocketIO::new(client_io, Role::Client, Config::default());
//! let mut server = WebSocketIO::new(server_io, Role::Server, Config::default());
//!
//! client.write_all(b"hello").await?;
//! client.shutdown().await?; // sends a Close frame
//!
//! let mut received = Vec::new();
//! server.read_to_end(&mut received).await?; // the Close frame is EOF
//! assert_eq!(received, b"hello");
//! # Ok(())
//! # }
//! ```
//!
//! [RFC 6455]: https://www.rfc-editor.org/rfc/rfc6455

#![warn(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

mod close;
mod config;
mod error;
mod frame;
pub mod mask;
mod recv;
mod send;
mod utf8;
mod ws;

pub use close::{CloseCode, CloseFrame};
pub use config::{Config, Role};
pub use error::ProtocolError;
pub use recv::Recv;
pub use utf8::Utf8Bytes;
pub use ws::WebSocketIO;
