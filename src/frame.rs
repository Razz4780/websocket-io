//! Frame header codec. Pure functions, no IO.

use crate::error::ProtocolError;

/// Longest possible header: 2 fixed bytes, 8 bytes of extended length, 4 bytes of masking key.
pub(crate) const MAX_HEADER_LEN: usize = 14;

/// Longest payload a control frame may carry.
pub(crate) const MAX_CONTROL_PAYLOAD: usize = 125;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpCode {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

impl OpCode {
    fn from_bits(bits: u8) -> Option<Self> {
        Some(match bits {
            0x0 => Self::Continuation,
            0x1 => Self::Text,
            0x2 => Self::Binary,
            0x8 => Self::Close,
            0x9 => Self::Ping,
            0xA => Self::Pong,
            _ => return None,
        })
    }

    pub(crate) fn is_control(self) -> bool {
        self as u8 & 0x8 != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) fin: bool,
    pub(crate) opcode: OpCode,
    pub(crate) mask: Option<[u8; 4]>,
    pub(crate) len: u64,
}

/// Parses a frame header from the start of `buf`.
///
/// Returns the header and its encoded length, or [`None`] if `buf` does not hold the whole header
/// yet. Validates everything that can be validated without connection state.
#[inline]
pub(crate) fn parse(buf: &[u8]) -> Result<Option<(Header, usize)>, ProtocolError> {
    let [b0, b1, ..] = *buf else {
        return Ok(None);
    };

    if b0 & 0x70 != 0 {
        return Err(ProtocolError::ReservedBits);
    }
    let opcode = OpCode::from_bits(b0 & 0x0F).ok_or(ProtocolError::UnknownOpCode(b0 & 0x0F))?;
    let fin = b0 & 0x80 != 0;
    let len7 = b1 & 0x7F;

    if opcode.is_control() {
        if !fin {
            return Err(ProtocolError::FragmentedControlFrame);
        }
        if usize::from(len7) > MAX_CONTROL_PAYLOAD {
            return Err(ProtocolError::ControlFrameTooLarge);
        }
    }

    let (len, mut header_len) = match len7 {
        126 => {
            let Some(bytes) = buf.get(2..4) else {
                return Ok(None);
            };
            (u64::from(u16::from_be_bytes([bytes[0], bytes[1]])), 4)
        }
        127 => {
            let Some(bytes) = buf.get(2..10) else {
                return Ok(None);
            };
            let len = u64::from_be_bytes(bytes.try_into().expect("slice has 8 bytes"));
            if len >> 63 != 0 {
                return Err(ProtocolError::InvalidFrameLength);
            }
            (len, 10)
        }
        len => (u64::from(len), 2),
    };

    let mask = if b1 & 0x80 != 0 {
        let Some(key) = buf.get(header_len..header_len + 4) else {
            return Ok(None);
        };
        header_len += 4;
        Some(key.try_into().expect("slice has 4 bytes"))
    } else {
        None
    };

    Ok(Some((
        Header {
            fin,
            opcode,
            mask,
            len,
        },
        header_len,
    )))
}

/// Encodes a frame header into `out`, returning the encoded length.
#[inline]
pub(crate) fn encode(out: &mut [u8; MAX_HEADER_LEN], header: Header) -> usize {
    out[0] = (u8::from(header.fin) << 7) | header.opcode as u8;
    let mask_bit = if header.mask.is_some() { 0x80 } else { 0 };
    let mut len = if header.len < 126 {
        out[1] = mask_bit | header.len as u8;
        2
    } else if header.len <= u64::from(u16::MAX) {
        out[1] = mask_bit | 126;
        out[2..4].copy_from_slice(&(header.len as u16).to_be_bytes());
        4
    } else {
        out[1] = mask_bit | 127;
        out[2..10].copy_from_slice(&header.len.to_be_bytes());
        10
    };
    if let Some(key) = header.mask {
        out[len..len + 4].copy_from_slice(&key);
        len += 4;
    }
    len
}

#[cfg(test)]
mod test {
    use proptest::prelude::*;

    use super::*;

    fn opcode() -> impl Strategy<Value = OpCode> {
        prop_oneof![
            Just(OpCode::Continuation),
            Just(OpCode::Text),
            Just(OpCode::Binary),
            Just(OpCode::Close),
            Just(OpCode::Ping),
            Just(OpCode::Pong),
        ]
    }

    fn header() -> impl Strategy<Value = Header> {
        (
            opcode(),
            any::<bool>(),
            any::<Option<[u8; 4]>>(),
            prop_oneof![0..126u64, 126..=65535u64, 65536..=u64::MAX >> 1],
        )
            .prop_map(|(opcode, fin, mask, len)| {
                if opcode.is_control() {
                    Header {
                        fin: true,
                        opcode,
                        mask,
                        len: len % 126,
                    }
                } else {
                    Header {
                        fin,
                        opcode,
                        mask,
                        len,
                    }
                }
            })
    }

    proptest! {
        #[test]
        fn roundtrip(header in header()) {
            let mut out = [0; MAX_HEADER_LEN];
            let len = encode(&mut out, header);
            prop_assert_eq!(parse(&out[..len]), Ok(Some((header, len))));
            // Every proper prefix is incomplete.
            for prefix in 0..len {
                prop_assert_eq!(parse(&out[..prefix]), Ok(None));
            }
        }
    }

    #[test]
    fn minimal_length_encoding() {
        let mut out = [0; MAX_HEADER_LEN];
        let header = |len| Header {
            fin: true,
            opcode: OpCode::Binary,
            mask: None,
            len,
        };
        assert_eq!(encode(&mut out, header(125)), 2);
        assert_eq!(encode(&mut out, header(126)), 4);
        assert_eq!(encode(&mut out, header(65535)), 4);
        assert_eq!(encode(&mut out, header(65536)), 10);
    }

    #[test]
    fn violations() {
        assert_eq!(parse(&[0xC2, 0x00]), Err(ProtocolError::ReservedBits));
        assert_eq!(parse(&[0x83, 0x00]), Err(ProtocolError::UnknownOpCode(3)));
        assert_eq!(parse(&[0x8B, 0x00]), Err(ProtocolError::UnknownOpCode(0xB)));
        assert_eq!(
            parse(&[0x09, 0x00]),
            Err(ProtocolError::FragmentedControlFrame)
        );
        assert_eq!(
            parse(&[0x89, 0x7E]),
            Err(ProtocolError::ControlFrameTooLarge)
        );
        assert_eq!(
            parse(&[0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0]),
            Err(ProtocolError::InvalidFrameLength)
        );
    }
}
