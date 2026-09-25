#![allow(dead_code)]

use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// xorshift64*, good enough for test data and chaos decisions.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..bound`.
    pub fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

/// IO wrapper that splits reads and writes into random pieces and randomly returns `Pending`.
pub struct Chaos<T> {
    inner: T,
    rng: Rng,
    max_chunk: usize,
    vectored: bool,
}

impl<T> Chaos<T> {
    pub fn new(inner: T, seed: u64, max_chunk: usize, vectored: bool) -> Self {
        Self {
            inner,
            rng: Rng::new(seed),
            max_chunk,
            vectored,
        }
    }

    /// Returns `true` if the operation should spuriously return `Pending` this time.
    fn stall(&mut self, cx: &mut Context<'_>) -> bool {
        if self.rng.below(4) == 0 {
            cx.waker().wake_by_ref();
            true
        } else {
            false
        }
    }

    fn limit(&mut self) -> usize {
        1 + self.rng.below(self.max_chunk)
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Chaos<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.stall(cx) {
            return Poll::Pending;
        }
        let limit = this.limit().min(buf.remaining());
        let mut limited = buf.take(limit);
        let poll = Pin::new(&mut this.inner).poll_read(cx, &mut limited);
        let n = limited.filled().len();
        // SAFETY: the inner reader initialized `n` bytes.
        unsafe { buf.assume_init(n) };
        buf.advance(n);
        poll
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Chaos<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.stall(cx) {
            return Poll::Pending;
        }
        let limit = this.limit().min(buf.len());
        Pin::new(&mut this.inner).poll_write(cx, &buf[..limit])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.stall(cx) {
            return Poll::Pending;
        }
        let mut limit = this.limit();
        let mut gathered = Vec::new();
        for buf in bufs {
            let take = limit.min(buf.len());
            gathered.extend_from_slice(&buf[..take]);
            limit -= take;
        }
        Pin::new(&mut this.inner).poll_write(cx, &gathered)
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// Encodes a raw frame, independently of the crate's encoder.
pub fn raw_frame(fin: bool, opcode: u8, mask: Option<[u8; 4]>, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(u8::from(fin) << 7) | opcode];
    let mask_bit = if mask.is_some() { 0x80 } else { 0 };
    match payload.len() {
        len @ 0..=125 => out.push(mask_bit | len as u8),
        len @ 126..=65535 => {
            out.push(mask_bit | 126);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            out.push(mask_bit | 127);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    match mask {
        Some(key) => {
            out.extend_from_slice(&key);
            out.extend(
                payload
                    .iter()
                    .enumerate()
                    .map(|(i, byte)| byte ^ key[i % 4]),
            );
        }
        None => out.extend_from_slice(payload),
    }
    out
}

/// Parses raw frames from `data`, returning `(fin, opcode, unmasked payload)` for each.
pub fn parse_raw_frames(mut data: &[u8]) -> Vec<(bool, u8, Vec<u8>)> {
    let mut frames = Vec::new();
    while !data.is_empty() {
        let fin = data[0] & 0x80 != 0;
        let opcode = data[0] & 0x0F;
        let masked = data[1] & 0x80 != 0;
        let (len, mut pos) = match data[1] & 0x7F {
            126 => (u16::from_be_bytes([data[2], data[3]]) as usize, 4),
            127 => (
                u64::from_be_bytes(data[2..10].try_into().unwrap()) as usize,
                10,
            ),
            len => (len as usize, 2),
        };
        let key = masked.then(|| {
            let key: [u8; 4] = data[pos..pos + 4].try_into().unwrap();
            pos += 4;
            key
        });
        let mut payload = data[pos..pos + len].to_vec();
        if let Some(key) = key {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= key[i % 4];
            }
        }
        frames.push((fin, opcode, payload));
        data = &data[pos + len..];
    }
    frames
}
