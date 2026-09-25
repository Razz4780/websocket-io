//! Our client against our server.

mod common;

use std::{io, time::Duration};

use common::{Chaos, Rng};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use websocket_io::{CloseCode, CloseFrame, Config, Recv, Role, WebSocketIO};

/// Writes `data` in random pieces with occasional flushes, then shuts down.
async fn write_randomly<W: AsyncWrite + Unpin>(
    mut writer: W,
    data: &[u8],
    seed: u64,
    max_piece: usize,
) -> io::Result<()> {
    let mut rng = Rng::new(seed);
    let mut rest = data;
    while !rest.is_empty() {
        let piece = (1 + rng.below(max_piece)).min(rest.len());
        writer.write_all(&rest[..piece]).await?;
        rest = &rest[piece..];
        if rng.below(8) == 0 {
            writer.flush().await?;
        }
    }
    writer.shutdown().await
}

async fn read_all<R: AsyncRead + Unpin>(mut reader: R) -> io::Result<Vec<u8>> {
    let mut received = Vec::new();
    reader.read_to_end(&mut received).await?;
    Ok(received)
}

/// Both sides stream data at each other at the same time and half-close when done.
async fn exchange<IO>(client: IO, server: IO, seed: u64, len: usize, max_piece: usize)
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut rng = Rng::new(seed);
    let to_server = rng.bytes(len);
    let to_client_len = len / 2 + rng.below(len);
    let to_client = rng.bytes(to_client_len);

    let config = Config::default().defer_close_reply(true);
    let client = WebSocketIO::new(client, Role::Client, config.clone());
    let server = WebSocketIO::new(server, Role::Server, config);
    let (client_read, client_write) = tokio::io::split(client);
    let (server_read, server_write) = tokio::io::split(server);

    let (client_sent, server_sent, client_received, server_received) = tokio::join!(
        write_randomly(client_write, &to_server, seed + 1, max_piece),
        write_randomly(server_write, &to_client, seed + 2, max_piece),
        read_all(client_read),
        read_all(server_read),
    );
    client_sent.unwrap();
    server_sent.unwrap();
    assert!(client_received.unwrap() == to_client, "seed {seed}");
    assert!(server_received.unwrap() == to_server, "seed {seed}");
}

#[tokio::test]
async fn byte_stream_over_chaotic_io() {
    for seed in 0..40 {
        let vectored = seed % 2 == 0;
        let (client, server) = tokio::io::duplex(8 * 1024);
        let max_piece = [16, 1024, 100_000][seed as usize % 3];
        exchange(
            Chaos::new(client, seed * 3, 5000, vectored),
            Chaos::new(server, seed * 3 + 1, 5000, vectored),
            seed,
            300_000,
            max_piece,
        )
        .await;
    }
}

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (client.unwrap(), server.unwrap().0)
}

#[tokio::test]
async fn byte_stream_over_tcp() {
    for seed in 0..4 {
        let (client, server) = tcp_pair().await;
        exchange(client, server, seed, 8 * 1024 * 1024, 1024 * 1024).await;
    }
}

#[tokio::test]
async fn poll_recv_reports_message_boundaries() {
    let (client, server) = tokio::io::duplex(1024);
    let config = Config::default().max_frame_size(1000);
    let mut client = WebSocketIO::new(Chaos::new(client, 1, 300, false), Role::Client, config);
    let mut server = WebSocketIO::new(
        Chaos::new(server, 2, 300, true),
        Role::Server,
        Config::default(),
    );

    let sizes = [1, 999, 1000, 1001, 2500, 7];
    let mut rng = Rng::new(3);
    let data = sizes.map(|size| rng.bytes(size));

    let send = async {
        for message in &data {
            client.write_all(message).await.unwrap();
        }
        client.close(None).await.unwrap();
        client
    };
    let receive = async {
        let mut messages = Vec::new();
        let mut current = Vec::new();
        let mut buf = vec![0; 700];
        loop {
            match server.recv(&mut buf).await.unwrap() {
                Recv::Binary {
                    data,
                    end_of_message,
                } => {
                    current.extend_from_slice(&buf[..data]);
                    if end_of_message {
                        messages.push(std::mem::take(&mut current));
                    }
                }
                Recv::Close(frame) => {
                    assert_eq!(frame, None);
                    break;
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        messages
    };
    let (_, messages) = tokio::join!(send, receive);

    // Every write is a message, capped at `max_frame_size`.
    let expected = data
        .iter()
        .flat_map(|message| message.chunks(1000))
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    assert_eq!(messages, expected);
}

#[tokio::test]
async fn recv_bytes_zero_copy() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut client = WebSocketIO::new(client, Role::Client, Config::default());
    let mut server = WebSocketIO::new(server, Role::Server, Config::default());
    let data = Rng::new(5).bytes(1_000_000);

    let send = async {
        client.write_all(&data).await.unwrap();
        client.close(None).await.unwrap();
    };
    let receive = async {
        let mut received = Vec::new();
        loop {
            match server.recv_bytes().await.unwrap() {
                Recv::Binary { data, .. } => received.extend_from_slice(&data),
                Recv::Close(_) => break received,
                other => panic!("unexpected {other:?}"),
            }
        }
    };
    let (_, received) = tokio::join!(send, receive);
    assert!(received == data);
}

#[tokio::test]
async fn single_byte_then_flush() {
    for role in [Role::Client, Role::Server] {
        let other = match role {
            Role::Client => Role::Server,
            Role::Server => Role::Client,
        };
        let (a, b) = tokio::io::duplex(1024);
        let mut sender = WebSocketIO::new(a, role, Config::default());
        let mut receiver = WebSocketIO::new(b, other, Config::default());

        sender.write_all(b"x").await.unwrap();
        // Buffered until flushed.
        let mut buf = [0; 16];
        let early = tokio::time::timeout(Duration::from_millis(50), receiver.read(&mut buf)).await;
        assert!(early.is_err(), "data arrived before a flush");

        sender.flush().await.unwrap();
        let n = tokio::time::timeout(Duration::from_secs(5), receiver.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], b"x");
    }
}

#[tokio::test]
async fn truncated_stream_is_an_error() {
    let (client, server) = tokio::io::duplex(1024);
    let mut client = WebSocketIO::new(client, Role::Client, Config::default());
    let mut server = WebSocketIO::new(server, Role::Server, Config::default());
    client.write_all(b"partial").await.unwrap();
    client.flush().await.unwrap();
    drop(client);

    let mut received = Vec::new();
    let error = server.read_to_end(&mut received).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(received, b"partial");
}

#[tokio::test]
async fn close_handshake() {
    let (client, server) = tokio::io::duplex(1024);
    let mut client = WebSocketIO::new(client, Role::Client, Config::default());
    let mut server = WebSocketIO::new(server, Role::Server, Config::default());
    let mut buf = [0; 16];

    let frame = CloseFrame {
        code: CloseCode::AWAY,
        reason: "bye".into(),
    };
    client.write_all(b"last words").await.unwrap();
    client.close(Some(&frame)).await.unwrap();
    assert_eq!(
        client.write_all(b"more").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    // Data before the close frame is delivered.
    let Recv::Binary { data, .. } = server.recv(&mut buf).await.unwrap() else {
        panic!("expected data")
    };
    assert_eq!(&buf[..data], b"last words");
    assert_eq!(
        server.recv(&mut buf).await.unwrap(),
        Recv::Close(Some(frame.clone()))
    );
    // Reported again, the reply has been sent, so writing fails.
    assert_eq!(
        server.recv(&mut buf).await.unwrap(),
        Recv::Close(Some(frame))
    );
    assert_eq!(
        server.write_all(b"late").await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );

    // The client gets the echoed status code.
    assert_eq!(
        client.recv(&mut buf).await.unwrap(),
        Recv::Close(Some(CloseFrame::new(CloseCode::AWAY)))
    );
}

#[tokio::test]
async fn half_close() {
    let (client, server) = tokio::io::duplex(1024);
    let config = Config::default().defer_close_reply(true);
    let mut client = WebSocketIO::new(client, Role::Client, config.clone());
    let mut server = WebSocketIO::new(server, Role::Server, config);

    client.write_all(b"request").await.unwrap();
    client.shutdown().await.unwrap();

    let mut request = Vec::new();
    server.read_to_end(&mut request).await.unwrap();
    assert_eq!(request, b"request");

    // The server can still respond after the client finished.
    server.write_all(b"response").await.unwrap();
    server.shutdown().await.unwrap();

    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"response");
}

#[tokio::test]
async fn ping_pong_and_text() {
    let (client, server) = tokio::io::duplex(1024);
    let mut client = WebSocketIO::new(client, Role::Client, Config::default());
    let mut server = WebSocketIO::new(server, Role::Server, Config::default());
    let mut buf = [0; 16];

    let long_text = "żółw 🐢 ".repeat(20_000);
    let send = async {
        client.send_ping(b"are you there").await.unwrap();
        client.send_text(&long_text).await.unwrap();
        client.write_all(b"bytes").await.unwrap();
        client.flush().await.unwrap();
    };
    let receive = async {
        assert_eq!(
            server.recv(&mut buf).await.unwrap(),
            Recv::Ping(bytes::Bytes::from_static(b"are you there"))
        );
        let Recv::Text(text) = server.recv(&mut buf).await.unwrap() else {
            panic!("expected text")
        };
        assert_eq!(text, long_text.as_str());
        let Recv::Binary { data, .. } = server.recv(&mut buf).await.unwrap() else {
            panic!("expected data")
        };
        assert_eq!(&buf[..data], b"bytes");
    };
    tokio::join!(send, receive);
    // The pong was sent by the server's receiving side, without an explicit flush.
    assert_eq!(
        client.recv(&mut buf).await.unwrap(),
        Recv::Pong(bytes::Bytes::from_static(b"are you there"))
    );
}

#[tokio::test]
async fn read_through_large_frames() {
    // Large reads into an empty read buffer bypass it; exercise both roles.
    for (sender_role, receiver_role) in [(Role::Client, Role::Server), (Role::Server, Role::Client)]
    {
        let (a, b) = tcp_pair().await;
        let config = Config::default().max_frame_size(1024 * 1024);
        let mut sender = WebSocketIO::new(a, sender_role, config.clone());
        let mut receiver = WebSocketIO::new(b, receiver_role, config);
        let data = Rng::new(9).bytes(4 * 1024 * 1024);

        let send = async {
            sender.write_all(&data).await.unwrap();
            sender.shutdown().await.unwrap();
        };
        let receive = async {
            let mut received = Vec::new();
            let mut buf = vec![0; 512 * 1024];
            loop {
                let n = receiver.read(&mut buf).await.unwrap();
                if n == 0 {
                    break received;
                }
                received.extend_from_slice(&buf[..n]);
            }
        };
        let (_, received) = tokio::join!(send, receive);
        assert!(received == data);
    }
}
