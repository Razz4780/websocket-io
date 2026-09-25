use std::io;

use bytes::Bytes;

use crate::{error::ProtocolError, utf8::Utf8Bytes};

/// Maximum length of a close reason, the payload of a control frame minus the status code.
pub(crate) const MAX_REASON_LEN: usize = 123;

/// Status code of a Close frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CloseCode(pub u16);

impl CloseCode {
    /// 1000, the purpose of the connection has been fulfilled.
    pub const NORMAL: Self = Self(1000);
    /// 1001, the endpoint is going away.
    pub const AWAY: Self = Self(1001);
    /// 1002, the endpoint received a malformed frame.
    pub const PROTOCOL: Self = Self(1002);
    /// 1003, the endpoint received a type of data it cannot accept.
    pub const UNSUPPORTED: Self = Self(1003);
    /// 1007, the endpoint received data inconsistent with the message type.
    pub const INVALID_DATA: Self = Self(1007);
    /// 1008, the endpoint received a message that violates its policy.
    pub const POLICY: Self = Self(1008);
    /// 1009, the endpoint received a message that is too big to process.
    pub const TOO_BIG: Self = Self(1009);
    /// 1010, the client expected the server to negotiate an extension.
    pub const EXTENSION: Self = Self(1010);
    /// 1011, the server encountered an unexpected condition.
    pub const INTERNAL_ERROR: Self = Self(1011);

    /// Whether the code may appear in a Close frame on the wire.
    pub fn is_sendable(self) -> bool {
        matches!(self.0, 1000..=1003 | 1007..=1014 | 3000..=4999)
    }
}

/// Status code and reason carried by a Close frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseFrame {
    /// Status code.
    pub code: CloseCode,
    /// Human readable reason, at most 123 bytes long.
    pub reason: Utf8Bytes,
}

impl CloseFrame {
    /// Creates a frame without a reason.
    pub const fn new(code: CloseCode) -> Self {
        Self {
            code,
            reason: Utf8Bytes::from_static(""),
        }
    }
}

/// Parses the payload of a received Close frame.
pub(crate) fn parse_payload(payload: Bytes) -> Result<Option<CloseFrame>, ProtocolError> {
    match payload.len() {
        0 => Ok(None),
        1 => Err(ProtocolError::InvalidClosePayload),
        _ => {
            let code = CloseCode(u16::from_be_bytes([payload[0], payload[1]]));
            if !code.is_sendable() {
                return Err(ProtocolError::InvalidCloseCode(code.0));
            }
            let reason =
                Utf8Bytes::try_from(payload.slice(2..)).map_err(|_| ProtocolError::InvalidUtf8)?;
            Ok(Some(CloseFrame { code, reason }))
        }
    }
}

/// Checks that a Close frame provided by the user can be sent.
pub(crate) fn validate(frame: &CloseFrame) -> io::Result<()> {
    if !frame.code.is_sendable() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("close code {} cannot be sent", frame.code.0),
        ));
    }
    if frame.reason.len() > MAX_REASON_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "close reason exceeds 123 bytes",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(parse_payload(Bytes::new()), Ok(None));
        assert_eq!(
            parse_payload(Bytes::from_static(&[3])),
            Err(ProtocolError::InvalidClosePayload)
        );
        assert_eq!(
            parse_payload(Bytes::from_static(b"\x03\xe8bye")),
            Ok(Some(CloseFrame {
                code: CloseCode::NORMAL,
                reason: "bye".into(),
            }))
        );
        assert_eq!(
            parse_payload(Bytes::from_static(b"\x03\xed")),
            Err(ProtocolError::InvalidCloseCode(1005))
        );
        assert_eq!(
            parse_payload(Bytes::from_static(b"\x03\xe8\xff")),
            Err(ProtocolError::InvalidUtf8)
        );
    }

    #[test]
    fn sendable_codes() {
        for code in [0, 999, 1004, 1005, 1006, 1015, 1016, 2999, 5000] {
            assert!(!CloseCode(code).is_sendable(), "{code}");
        }
        for code in [1000, 1001, 1002, 1003, 1007, 1011, 1014, 3000, 4999] {
            assert!(CloseCode(code).is_sendable(), "{code}");
        }
    }
}
