use std::{hint::black_box, mem::MaybeUninit};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use websocket_io::mask::{self, variants};

const SIZES: [usize; 5] = [16, 256, 4 * 1024, 64 * 1024, 1024 * 1024];
const KEY: [u8; 4] = [0x12, 0x34, 0x56, 0x78];

/// tungstenite's `apply_mask_fast32`, for reference.
fn apply_mask_tungstenite(buf: &mut [u8], mask: [u8; 4]) {
    let mask_u32 = u32::from_ne_bytes(mask);
    // SAFETY: reinterpreting bytes as u32 is fine, `align_to_mut` handles alignment.
    let (prefix, words, suffix) = unsafe { buf.align_to_mut::<u32>() };
    variants::apply_mask_naive(prefix, mask, 0);
    let head = prefix.len() & 3;
    let mask_u32 = if head > 0 {
        if cfg!(target_endian = "big") {
            mask_u32.rotate_left(8 * head as u32)
        } else {
            mask_u32.rotate_right(8 * head as u32)
        }
    } else {
        mask_u32
    };
    for word in words.iter_mut() {
        *word ^= mask_u32;
    }
    variants::apply_mask_naive(suffix, mask_u32.to_ne_bytes(), 0);
}

fn apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_mask");
    for size in SIZES {
        // Offset by one byte to keep the buffer unaligned.
        let mut data = vec![0u8; size + 1];
        let buf = &mut data[1..];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("naive", size), |b| {
            b.iter(|| variants::apply_mask_naive(black_box(&mut *buf), KEY, 0))
        });
        group.bench_function(BenchmarkId::new("tungstenite", size), |b| {
            b.iter(|| apply_mask_tungstenite(black_box(&mut *buf), KEY))
        });
        group.bench_function(BenchmarkId::new("portable", size), |b| {
            b.iter(|| variants::apply_mask_portable(black_box(&mut *buf), KEY))
        });
        if let Some(kernel) = variants::apply_mask_avx2() {
            group.bench_function(BenchmarkId::new("avx2", size), |b| {
                b.iter(|| kernel(black_box(&mut *buf), KEY))
            });
        }
        group.bench_function(BenchmarkId::new("dispatch", size), |b| {
            b.iter(|| mask::apply_mask(black_box(&mut *buf), KEY, 1))
        });
    }
    group.finish();
}

fn copy(c: &mut Criterion) {
    let mut group = c.benchmark_group("copy_masked");
    for size in SIZES {
        let src = vec![7u8; size + 1];
        let src = &src[1..];
        let mut dst = vec![MaybeUninit::<u8>::uninit(); size + 1];
        let dst = &mut dst[1..];
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_function(BenchmarkId::new("memcpy", size), |b| {
            b.iter(|| {
                // SAFETY: `MaybeUninit<u8>` and `u8` have the same layout.
                let src: &[MaybeUninit<u8>] = unsafe { std::mem::transmute(black_box(src)) };
                dst.copy_from_slice(src);
            })
        });
        group.bench_function(BenchmarkId::new("copy_masked", size), |b| {
            b.iter(|| mask::copy_masked(black_box(&mut *dst), black_box(src), KEY, 3))
        });
    }
    group.finish();
}

criterion_group!(benches, apply, copy);
criterion_main!(benches);
