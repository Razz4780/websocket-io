//! Payload masking kernels.
//!
//! Masking XORs the payload with a 4-byte key, repeated. The kernels XOR 8-byte lanes with the key
//! expanded to 8 bytes, grouped into fixed 64-byte blocks, which LLVM turns into plain vector XORs.
//! On x86-64 the same code is additionally compiled for AVX2 and picked at runtime (AVX-512 was
//! measured to be slower); on other targets it uses whatever vector unit the baseline target
//! provides (SSE2, NEON, ...).
//!
//! Both kernels take an `offset`: the position of the first byte within the payload. This lets
//! payloads be (un)masked in pieces, as they arrive.

use std::mem::MaybeUninit;

const BLOCK: usize = 64;

/// XORs `buf` with `key` in place, treating `buf[0]` as the byte at position `offset` of the
/// masked payload.
#[inline]
pub fn apply_mask(buf: &mut [u8], key: [u8; 4], offset: usize) {
    let key = rotate(key, offset);
    #[cfg(target_arch = "x86_64")]
    if buf.len() >= BLOCK && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU supports the required features.
        return unsafe { x86::apply_avx2(buf, key) };
    }
    apply_portable(buf, key);
}

/// Writes `src` XORed with `key` to `dst`, treating `src[0]` as the byte at position `offset` of
/// the masked payload.
///
/// Initializes exactly `src.len()` bytes at the start of `dst`.
///
/// # Panics
///
/// If `dst` is shorter than `src`.
#[inline]
pub fn copy_masked(dst: &mut [MaybeUninit<u8>], src: &[u8], key: [u8; 4], offset: usize) {
    let dst = &mut dst[..src.len()];
    let key = rotate(key, offset);
    #[cfg(target_arch = "x86_64")]
    if src.len() >= BLOCK && std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU supports the required features.
        return unsafe { x86::copy_avx2(dst, src, key) };
    }
    copy_portable(dst, src, key);
}

/// Rotates the key so that `key[0]` applies to the byte at `offset`.
#[inline(always)]
fn rotate(key: [u8; 4], offset: usize) -> [u8; 4] {
    let r = offset % 4;
    [key[r], key[(r + 1) % 4], key[(r + 2) % 4], key[(r + 3) % 4]]
}

#[inline(always)]
fn lane_key(key: [u8; 4]) -> u64 {
    u64::from_ne_bytes([
        key[0], key[1], key[2], key[3], key[0], key[1], key[2], key[3],
    ])
}

#[inline(always)]
fn xor_lane(lane: [u8; 8], key: u64) -> [u8; 8] {
    (u64::from_ne_bytes(lane) ^ key).to_ne_bytes()
}

#[inline(always)]
fn apply_portable(buf: &mut [u8], key: [u8; 4]) {
    let lane_key = lane_key(key);
    let (blocks, tail) = buf.as_chunks_mut::<BLOCK>();
    for block in blocks {
        for lane in block.as_chunks_mut::<8>().0 {
            *lane = xor_lane(*lane, lane_key);
        }
    }
    // Blocks and lanes are multiples of 4 bytes long, the key phase is unchanged.
    let (lanes, bytes) = tail.as_chunks_mut::<8>();
    for lane in lanes {
        *lane = xor_lane(*lane, lane_key);
    }
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte ^= key[i % 4];
    }
}

#[inline(always)]
fn copy_portable(dst: &mut [MaybeUninit<u8>], src: &[u8], key: [u8; 4]) {
    debug_assert_eq!(dst.len(), src.len());
    let lane_key = lane_key(key);
    let (dst_blocks, dst_tail) = dst.as_chunks_mut::<BLOCK>();
    let (src_blocks, src_tail) = src.as_chunks::<BLOCK>();
    for (dst, src) in dst_blocks.iter_mut().zip(src_blocks) {
        let dst_lanes = dst.as_chunks_mut::<8>().0;
        let src_lanes = src.as_chunks::<8>().0;
        for (dst, src) in dst_lanes.iter_mut().zip(src_lanes) {
            *dst = xor_lane(*src, lane_key).map(MaybeUninit::new);
        }
    }
    // Blocks and lanes are multiples of 4 bytes long, the key phase is unchanged.
    let (dst_lanes, dst_bytes) = dst_tail.as_chunks_mut::<8>();
    let (src_lanes, src_bytes) = src_tail.as_chunks::<8>();
    for (dst, src) in dst_lanes.iter_mut().zip(src_lanes) {
        *dst = xor_lane(*src, lane_key).map(MaybeUninit::new);
    }
    for (i, (dst, src)) in dst_bytes.iter_mut().zip(src_bytes).enumerate() {
        *dst = MaybeUninit::new(src ^ key[i % 4]);
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::mem::MaybeUninit;

    #[target_feature(enable = "avx2")]
    pub(super) fn apply_avx2(buf: &mut [u8], key: [u8; 4]) {
        super::apply_portable(buf, key)
    }

    #[target_feature(enable = "avx2")]
    pub(super) fn copy_avx2(dst: &mut [MaybeUninit<u8>], src: &[u8], key: [u8; 4]) {
        super::copy_portable(dst, src, key)
    }
}

/// Individual kernel variants, exposed for benchmarks. Not part of the public API.
#[doc(hidden)]
pub mod variants {
    /// An in-place masking kernel, taking an already rotated key.
    pub type Kernel = fn(&mut [u8], [u8; 4]);

    /// Kernel compiled for the baseline target features.
    pub fn apply_mask_portable(buf: &mut [u8], key: [u8; 4]) {
        super::apply_portable(buf, key)
    }

    /// Byte-at-a-time reference implementation.
    pub fn apply_mask_naive(buf: &mut [u8], key: [u8; 4], offset: usize) {
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte ^= key[(offset + i) % 4];
        }
    }

    /// Returns the AVX2 kernel, if the CPU supports it.
    pub fn apply_mask_avx2() -> Option<Kernel> {
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: the CPU supports the required features.
            return Some(|buf, key| unsafe { super::x86::apply_avx2(buf, key) });
        }
        None
    }
}

/// Source of client masking keys.
///
/// Keys are drawn from the OS entropy source in batches, one syscall serves 64 frames.
pub(crate) struct MaskKeys {
    buf: [u8; 256],
    pos: usize,
}

impl MaskKeys {
    pub(crate) fn new() -> Self {
        Self {
            buf: [0; 256],
            pos: 256,
        }
    }

    #[inline]
    pub(crate) fn next(&mut self) -> [u8; 4] {
        if self.pos == self.buf.len() {
            self.refill();
        }
        let key = self.buf[self.pos..self.pos + 4]
            .try_into()
            .expect("slice has 4 bytes");
        self.pos += 4;
        key
    }

    #[cold]
    fn refill(&mut self) {
        if getrandom::fill(&mut self.buf).is_err() {
            // The OS entropy source is unavailable, which should never happen. Fall back to a
            // SplitMix64 stream seeded from the clock and the previous keys.
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos() as u64)
                .unwrap_or_default();
            let mut state = nanos ^ u64::from_ne_bytes(self.buf[..8].try_into().unwrap());
            for chunk in self.buf.chunks_exact_mut(8) {
                state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = state;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                chunk.copy_from_slice(&(z ^ (z >> 31)).to_ne_bytes());
            }
        }
        self.pos = 0;
    }
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::{variants::apply_mask_naive, *};

    fn copy_masked_vec(src: &[u8], key: [u8; 4], offset: usize) -> Vec<u8> {
        let mut dst = Vec::with_capacity(src.len() + 7);
        copy_masked(&mut dst.spare_capacity_mut()[..src.len()], src, key, offset);
        // SAFETY: `copy_masked` initialized `src.len()` bytes.
        unsafe { dst.set_len(src.len()) };
        dst
    }

    proptest! {
        #[test]
        fn apply_matches_naive(
            data in proptest::collection::vec(any::<u8>(), 0..1000),
            key in any::<[u8; 4]>(),
            offset in 0usize..16,
            start in 0usize..8,
        ) {
            // `start` shifts the data to exercise unaligned buffers.
            let start = start.min(data.len());
            let mut expected = data.clone();
            apply_mask_naive(&mut expected[start..], key, offset);
            let mut actual = data.clone();
            apply_mask(&mut actual[start..], key, offset);
            prop_assert_eq!(&actual, &expected);

            let mut portable = data.clone();
            apply_portable(&mut portable[start..], rotate(key, offset));
            prop_assert_eq!(&portable, &expected);

            if let Some(kernel) = variants::apply_mask_avx2() {
                let mut vector = data.clone();
                kernel(&mut vector[start..], rotate(key, offset));
                prop_assert_eq!(&vector, &expected);
            }
        }

        #[test]
        fn copy_matches_naive(
            data in proptest::collection::vec(any::<u8>(), 0..1000),
            key in any::<[u8; 4]>(),
            offset in 0usize..16,
            start in 0usize..8,
        ) {
            let start = start.min(data.len());
            let mut expected = data[start..].to_vec();
            apply_mask_naive(&mut expected, key, offset);
            prop_assert_eq!(copy_masked_vec(&data[start..], key, offset), expected);
        }

        #[test]
        fn piecewise_equals_whole(
            data in proptest::collection::vec(any::<u8>(), 0..1000),
            key in any::<[u8; 4]>(),
            splits in proptest::collection::vec(0usize..1000, 0..5),
        ) {
            let mut expected = data.clone();
            apply_mask(&mut expected, key, 0);

            let mut splits = splits.into_iter().map(|split| split.min(data.len())).collect::<Vec<_>>();
            splits.push(0);
            splits.push(data.len());
            splits.sort_unstable();
            let mut actual = data.clone();
            for window in splits.windows(2) {
                apply_mask(&mut actual[window[0]..window[1]], key, window[0]);
            }
            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn keys_vary() {
        let mut keys = MaskKeys::new();
        let drawn = (0..200)
            .map(|_| keys.next())
            .collect::<std::collections::HashSet<_>>();
        assert!(drawn.len() > 190);
    }
}
