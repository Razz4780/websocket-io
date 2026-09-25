//! Hand-crafted frames against our endpoints.

mod common;

use std::io;

use bytes::Bytes;
use common::{Chaos, parse_raw_frames, raw_frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use websocket_io::{CloseCode, CloseFrame, Config, ProtocolError, Recv, Role, WebSocketIO};

const KEY: Option<[u8; 4]> = Some([0x37, 0xfa, 0x21, 0x3d]);

const TEXT: u8 = 0x1;
const BINARY: u8 = 0x2;
const CONTINUATION: u8 = 0x0;
const CLOSE: u8 = 0x8;
const PING: u8 = 0x9;
const PONG: u8 = 0xA;

/// Our server, with the raw client end of the connection.
fn server(config: Config) -> (WebSocketIO<Chaos<DuplexStream>>, DuplexStream) {
    let (ours, raw) = tokio::io::duplex(1024 * 1024);
    (
        WebSocketIO::new(Chaos::new(ours, 7, 7, false), Role::Server, config),
        raw,
    )
}

/// Collects everything the peer wrote until it shut down its side.
async fn written_frames(raw: &mut DuplexStream) -> Vec<(bool, u8, Vec<u8>)> {
    let mut written = Vec::new();
    raw.read_to_end(&mut written).await.unwrap();
    parse_raw_frames(&written)
}

async fn expect_violation(frames: &[Vec<u8>], config: Config, expected: ProtocolError) {
    let (mut ws, mut raw) = server(config);
    raw.write_all(&frames.concat()).await.unwrap();

    let error = loop {
        match ws.recv(&mut [0; 1024]).await {
            Ok(_) => continue,
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    let error = error.get_ref().unwrap().downcast_ref::<ProtocolError>();
    assert_eq!(error, Some(&expected));

    // The connection is failed with a matching status code.
    ws.flush().await.unwrap();
    drop(ws);
    let frames = written_frames(&mut raw).await;
    let (_, opcode, payload) = frames.last().expect("a close frame was sent");
    assert_eq!(*opcode, CLOSE);
    let code = match expected {
        ProtocolError::InvalidUtf8 => 1007,
        ProtocolError::MessageTooLarge => 1009,
        _ => 1002,
    };
    assert_eq!(payload[..2], u16::to_be_bytes(code));
}

#[tokio::test]
async fn violations() {
    let cases = [
        (
            vec![raw_frame(true, BINARY, None, b"unmasked")],
            ProtocolError::UnmaskedFrame,
        ),
        (
            vec![raw_frame(true, CONTINUATION, KEY, b"x")],
            ProtocolError::UnexpectedContinuation,
        ),
        (
            vec![
                raw_frame(false, BINARY, KEY, b"x"),
                raw_frame(true, TEXT, KEY, b"y"),
            ],
            ProtocolError::ExpectedContinuation,
        ),
        (
            vec![raw_frame(false, PING, KEY, b"x")],
            ProtocolError::FragmentedControlFrame,
        ),
        (
            vec![raw_frame(true, PING, KEY, &[0; 126])],
            ProtocolError::ControlFrameTooLarge,
        ),
        (
            vec![{
                let mut frame = raw_frame(true, BINARY, KEY, b"x");
                frame[0] |= 0x40;
                frame
            }],
            ProtocolError::ReservedBits,
        ),
        (
            vec![raw_frame(true, 0x3, KEY, b"x")],
            ProtocolError::UnknownOpCode(3),
        ),
        (
            vec![raw_frame(true, TEXT, KEY, b"\xff")],
            ProtocolError::InvalidUtf8,
        ),
        (
            // Invalid in the first fragment, detected before the message ends.
            vec![raw_frame(false, TEXT, KEY, b"ok\xc0\x80")],
            ProtocolError::InvalidUtf8,
        ),
        (
            // Truncated code point at the end of the message.
            vec![raw_frame(true, TEXT, KEY, "ż".as_bytes().split_at(1).0)],
            ProtocolError::InvalidUtf8,
        ),
        (
            vec![raw_frame(true, CLOSE, KEY, &1005u16.to_be_bytes())],
            ProtocolError::InvalidCloseCode(1005),
        ),
        (
            vec![raw_frame(true, CLOSE, KEY, &[3])],
            ProtocolError::InvalidClosePayload,
        ),
    ];
    for (frames, expected) in cases {
        expect_violation(&frames, Config::default(), expected).await;
    }

    expect_violation(
        &[
            raw_frame(false, TEXT, KEY, &[b'a'; 60]),
            raw_frame(true, CONTINUATION, KEY, &[b'a'; 60]),
        ],
        Config::default().max_message_size(100),
        ProtocolError::MessageTooLarge,
    )
    .await;
}

#[tokio::test]
async fn client_rejects_masked_frames() {
    let (ours, mut raw) = tokio::io::duplex(1024);
    let mut ws = WebSocketIO::new(ours, Role::Client, Config::default());
    raw.write_all(&raw_frame(true, BINARY, KEY, b"masked"))
        .await
        .unwrap();
    let error = ws.recv(&mut [0; 16]).await.unwrap_err();
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<ProtocolError>(),
        Some(&ProtocolError::MaskedFrame)
    );
}

#[tokio::test]
async fn fragmented_messages_with_interleaved_control_frames() {
    let (mut ws, mut raw) = server(Config::default());

    let text = "zażółć gęślą jaźń";
    let (text_a, text_b) = text.as_bytes().split_at(3); // splits 'ż'
    let frames = [
        raw_frame(false, BINARY, KEY, b"frag"),
        raw_frame(true, PING, KEY, b"p1"),
        raw_frame(false, CONTINUATION, KEY, b""),
        raw_frame(true, CONTINUATION, KEY, b"mented"),
        raw_frame(false, TEXT, KEY, text_a),
        raw_frame(true, PONG, KEY, b"p2"),
        raw_frame(true, CONTINUATION, Some([0; 4]), text_b),
        raw_frame(true, BINARY, KEY, b""),
        raw_frame(true, CLOSE, KEY, b"\x03\xe8done"),
    ];
    raw.write_all(&frames.concat()).await.unwrap();

    let mut events = Vec::new();
    let mut data = Vec::new();
    loop {
        match ws.recv_bytes().await.unwrap() {
            Recv::Binary {
                data: chunk,
                end_of_message,
            } => {
                data.extend_from_slice(&chunk);
                if end_of_message {
                    events.push(Recv::Binary {
                        data: Bytes::from(std::mem::take(&mut data)),
                        end_of_message,
                    });
                }
            }
            event @ Recv::Close(_) => {
                events.push(event);
                break;
            }
            event => events.push(event),
        }
    }

    assert_eq!(
        events,
        [
            Recv::Ping(Bytes::from_static(b"p1")),
            Recv::Binary {
                data: Bytes::from_static(b"fragmented"),
                end_of_message: true
            },
            Recv::Pong(Bytes::from_static(b"p2")),
            Recv::Text(text.into()),
            Recv::Binary {
                data: Bytes::new(),
                end_of_message: true
            },
            Recv::Close(Some(CloseFrame {
                code: CloseCode::NORMAL,
                reason: "done".into()
            })),
        ]
    );

    // Pong and the Close reply went out automatically, and the IO was shut down.
    drop(ws);
    let written = written_frames(&mut raw).await;
    assert_eq!(
        written,
        [
            (true, PONG, b"p1".to_vec()),
            (true, CLOSE, 1000u16.to_be_bytes().to_vec()),
        ]
    );
}

#[tokio::test]
async fn pings_answered_by_async_read() {
    let (mut ws, mut raw) = server(Config::default());
    raw.write_all(
        &[
            raw_frame(true, BINARY, KEY, b"a"),
            raw_frame(true, PING, KEY, b"1"),
            raw_frame(true, PING, KEY, b"2"),
            raw_frame(true, BINARY, KEY, b"b"),
            raw_frame(true, CLOSE, KEY, b""),
        ]
        .concat(),
    )
    .await
    .unwrap();

    let mut data = Vec::new();
    ws.read_to_end(&mut data).await.unwrap();
    assert_eq!(data, b"ab");

    drop(ws);
    let written = written_frames(&mut raw).await;
    // Pongs to both pings (or only to the latest one), then the Close reply without a code.
    assert_eq!(written.last(), Some(&(true, CLOSE, Vec::new())));
    let pongs = &written[..written.len() - 1];
    assert!(pongs.iter().all(|(_, opcode, _)| *opcode == PONG));
    assert_eq!(pongs.last().unwrap().2, b"2");
}

#[tokio::test]
async fn text_fails_async_read() {
    let (mut ws, mut raw) = server(Config::default());
    raw.write_all(&raw_frame(true, TEXT, KEY, b"text"))
        .await
        .unwrap();
    let error = ws.read(&mut [0; 16]).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn outgoing_frames() {
    let (ours, mut raw) = tokio::io::duplex(1024 * 1024);
    let mut ws = WebSocketIO::new(ours, Role::Client, Config::default().max_frame_size(100));

    ws.write_all(&[7; 250]).await.unwrap();
    ws.send_text("hi").await.unwrap();
    ws.send_ping(b"ping").await.unwrap();
    ws.send_pong(b"pong").await.unwrap();
    assert_eq!(
        ws.send_ping(&[0; 126]).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    ws.close(Some(&CloseFrame {
        code: CloseCode(4000),
        reason: "custom".into(),
    }))
    .await
    .unwrap();
    drop(ws);

    let mut written = Vec::new();
    raw.read_to_end(&mut written).await.unwrap();
    // Every client frame is masked.
    let mut rest = &written[..];
    while !rest.is_empty() {
        assert_ne!(rest[1] & 0x80, 0);
        let len = usize::from(rest[1] & 0x7F);
        rest = &rest[2 + 4 + len..];
    }
    assert_eq!(
        parse_raw_frames(&written),
        [
            (true, BINARY, vec![7; 100]),
            (true, BINARY, vec![7; 100]),
            (true, BINARY, vec![7; 50]),
            (true, TEXT, b"hi".to_vec()),
            (true, PING, b"ping".to_vec()),
            (true, PONG, b"pong".to_vec()),
            (
                true,
                CLOSE,
                [&4000u16.to_be_bytes()[..], b"custom"].concat()
            ),
        ]
    );
}

#[tokio::test]
async fn leftover_bytes_from_upgrade() {
    let (ours, mut raw) = tokio::io::duplex(1024);
    let leftover = raw_frame(true, BINARY, KEY, b"early");
    let mut ws = WebSocketIO::with_read_buf(
        ours,
        Role::Server,
        Config::default(),
        leftover.as_slice().into(),
    );
    raw.write_all(&raw_frame(true, BINARY, KEY, b" bird"))
        .await
        .unwrap();
    raw.write_all(&raw_frame(true, CLOSE, KEY, b""))
        .await
        .unwrap();

    let mut data = Vec::new();
    ws.read_to_end(&mut data).await.unwrap();
    assert_eq!(data, b"early bird");
}

#[tokio::test]
async fn zero_mask_key() {
    let (ours, mut raw) = tokio::io::duplex(1024 * 1024);
    // Vectored, so that large writes take the write-through path.
    let mut ws = WebSocketIO::new(
        Chaos::new(ours, 3, 50_000, true),
        Role::Client,
        Config::default().zero_mask_key(true),
    );
    let large = (0..100_000).map(|i| i as u8).collect::<Vec<_>>();

    ws.write_all(b"small").await.unwrap();
    ws.write_all(&large).await.unwrap();
    ws.send_text("text").await.unwrap();
    ws.send_ping(b"ping").await.unwrap();
    ws.close(None).await.unwrap();
    drop(ws);

    let mut written = Vec::new();
    raw.read_to_end(&mut written).await.unwrap();
    // Every frame is masked, with an all-zero key, so the payload is sent as is.
    let mut rest = &written[..];
    while !rest.is_empty() {
        assert_ne!(rest[1] & 0x80, 0, "frame is not masked");
        let (len, key_at) = match rest[1] & 0x7F {
            126 => (usize::from(u16::from_be_bytes([rest[2], rest[3]])), 4),
            127 => (
                u64::from_be_bytes(rest[2..10].try_into().unwrap()) as usize,
                10,
            ),
            len => (usize::from(len), 2),
        };
        assert_eq!(rest[key_at..key_at + 4], [0; 4]);
        rest = &rest[key_at + 4 + len..];
    }
    assert_eq!(
        parse_raw_frames(&written),
        [
            (true, BINARY, b"small".to_vec()),
            (true, BINARY, large),
            (true, TEXT, b"text".to_vec()),
            (true, PING, b"ping".to_vec()),
            (true, CLOSE, Vec::new()),
        ]
    );
}
