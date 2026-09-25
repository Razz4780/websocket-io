//! Our endpoints against tokio-tungstenite.

mod common;

use bytes::Bytes;
use common::{Chaos, Rng};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        self, Message,
        protocol::{
            CloseFrame as TungCloseFrame, Role as TungRole, frame::coding::CloseCode as TungCode,
        },
    },
};
use websocket_io::{CloseCode, CloseFrame, Config, Recv, Role, WebSocketIO};

#[tokio::test]
async fn our_client_to_tungstenite_server() {
    for seed in 0..10 {
        let (ours, theirs) = tokio::io::duplex(16 * 1024);
        let mut client = WebSocketIO::new(
            Chaos::new(ours, seed, 3000, seed % 2 == 0),
            Role::Client,
            // tungstenite accepts frames masked with a zero key.
            Config::default().zero_mask_key(seed % 4 < 2),
        );
        let data = Rng::new(seed).bytes(500_000);

        let server = tokio::spawn(async move {
            let mut server = WebSocketStream::from_raw_socket(theirs, TungRole::Server, None).await;
            let mut received = Vec::new();
            while let Some(message) = server.next().await {
                match message.unwrap() {
                    Message::Binary(data) => received.extend_from_slice(&data),
                    Message::Ping(payload) => assert_eq!(payload, "ping"),
                    Message::Close(frame) => {
                        assert_eq!(frame.unwrap().code, TungCode::Normal);
                        break;
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            // Let tungstenite flush its automatic close reply.
            let _ = server.flush().await;
            received
        });

        let mut rng = Rng::new(seed + 100);
        let mut rest = &data[..];
        while !rest.is_empty() {
            let piece = (1 + rng.below(70_000)).min(rest.len());
            client.write_all(&rest[..piece]).await.unwrap();
            rest = &rest[piece..];
        }
        client.send_ping(b"ping").await.unwrap();
        client.shutdown().await.unwrap();

        // The server's close reply is the end of the stream.
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());
        assert!(server.await.unwrap() == data, "seed {seed}");
    }
}

#[tokio::test]
async fn tungstenite_client_to_our_server() {
    let (ours, theirs) = tokio::io::duplex(16 * 1024);
    let mut server = WebSocketIO::new(
        Chaos::new(ours, 1, 3000, true),
        Role::Server,
        Config::default(),
    );
    let mut rng = Rng::new(1);
    let messages = (0..200)
        .map(|_| {
            let len = rng.below(40_000);
            rng.bytes(len)
        })
        .collect::<Vec<_>>();

    let expected = messages.concat();
    let client = tokio::spawn(async move {
        let mut client = WebSocketStream::from_raw_socket(theirs, TungRole::Client, None).await;
        client
            .send(Message::Ping(Bytes::from_static(b"hello?")))
            .await
            .unwrap();
        client.send(Message::text("some text")).await.unwrap();
        // A fragmented message.
        client
            .send(Message::Frame(
                tungstenite::protocol::frame::Frame::message(
                    Bytes::from_static(b"frag"),
                    tungstenite::protocol::frame::coding::OpCode::Data(
                        tungstenite::protocol::frame::coding::Data::Binary,
                    ),
                    false,
                ),
            ))
            .await
            .unwrap();
        client
            .send(Message::Frame(
                tungstenite::protocol::frame::Frame::message(
                    Bytes::from_static(b"ment"),
                    tungstenite::protocol::frame::coding::OpCode::Data(
                        tungstenite::protocol::frame::coding::Data::Continue,
                    ),
                    true,
                ),
            ))
            .await
            .unwrap();
        for message in messages {
            client.feed(Message::Binary(message.into())).await.unwrap();
        }
        client
            .send(Message::Close(Some(TungCloseFrame {
                code: TungCode::Away,
                reason: "done".into(),
            })))
            .await
            .unwrap();

        let mut events = Vec::new();
        while let Some(message) = client.next().await {
            match message {
                Ok(message) => events.push(message),
                Err(tungstenite::Error::ConnectionClosed) => break,
                Err(error) => panic!("{error}"),
            }
        }
        events
    });

    let mut events = Vec::new();
    let mut data = Vec::new();
    let mut buf = vec![0; 10_000];
    loop {
        match server.recv(&mut buf).await.unwrap() {
            Recv::Binary { data: n, .. } => data.extend_from_slice(&buf[..n]),
            event @ Recv::Close(_) => {
                events.push(event);
                break;
            }
            event => events.push(event),
        }
    }
    assert!(data == [b"fragment".as_slice(), &expected].concat());
    assert_eq!(
        events,
        [
            Recv::Ping(Bytes::from_static(b"hello?")),
            Recv::Text("some text".into()),
            Recv::Close(Some(CloseFrame {
                code: CloseCode::AWAY,
                reason: "done".into()
            })),
        ]
    );

    // tungstenite got the pong and the echoed close.
    let client_events = client.await.unwrap();
    assert_eq!(
        client_events,
        [
            Message::Pong(Bytes::from_static(b"hello?")),
            Message::Close(Some(TungCloseFrame {
                code: TungCode::Away,
                reason: "".into(),
            })),
        ]
    );
}

#[tokio::test]
async fn our_server_to_tungstenite_client() {
    let (ours, theirs) = tokio::io::duplex(16 * 1024);
    let mut server = WebSocketIO::new(
        Chaos::new(ours, 2, 20_000, true),
        Role::Server,
        Config::default().max_frame_size(100_000),
    );
    let data = Rng::new(2).bytes(3_000_000);

    let client = tokio::spawn(async move {
        let mut client = WebSocketStream::from_raw_socket(theirs, TungRole::Client, None).await;
        let mut received = Vec::new();
        while let Some(message) = client.next().await {
            match message.unwrap() {
                Message::Binary(data) => received.extend_from_slice(&data),
                Message::Text(text) => assert_eq!(text, "text"),
                Message::Close(_) => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        received
    });

    server.write_all(&data[..1_000_000]).await.unwrap();
    server.send_text("text").await.unwrap();
    server.write_all(&data[1_000_000..]).await.unwrap();
    server.shutdown().await.unwrap();
    let mut tail = Vec::new();
    server.read_to_end(&mut tail).await.unwrap();

    assert!(client.await.unwrap() == data);
}
