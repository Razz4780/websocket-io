use std::{error::Error, fmt, io};

use crate::close::CloseCode;

/// A violation of RFC 6455 detected in the incoming stream.
///
/// Returned wrapped in an [`io::Error`] of kind [`io::ErrorKind::InvalidData`], retrieve it with
/// [`io::Error::get_ref`] and `downcast_ref`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolError {
    /// A frame had some of the RSV bits set, but no extension was negotiated.
    ReservedBits,
    /// A frame used a reserved opcode.
    UnknownOpCode(u8),
    /// A control frame had the FIN bit unset.
    FragmentedControlFrame,
    /// A control frame carried more than 125 bytes of payload.
    ControlFrameTooLarge,
    /// A frame declared a 64-bit payload length with the most significant bit set.
    InvalidFrameLength,
    /// A server received an unmasked frame.
    UnmaskedFrame,
    /// A client received a masked frame.
    MaskedFrame,
    /// A continuation frame arrived outside of a fragmented message.
    UnexpectedContinuation,
    /// A new data message started before the previous fragmented message was finished.
    ExpectedContinuation,
    /// A text message or a close reason was not valid UTF-8.
    InvalidUtf8,
    /// A Close frame carried a status code that must not appear on the wire.
    InvalidCloseCode(u16),
    /// A Close frame carried a 1-byte payload.
    InvalidClosePayload,
    /// A text message exceeded [`Config::max_message_size`](crate::Config::max_message_size).
    MessageTooLarge,
}

impl ProtocolError {
    /// Status code sent to the peer when the connection is failed because of this error.
    pub(crate) fn close_code(self) -> CloseCode {
        match self {
            Self::InvalidUtf8 => CloseCode::INVALID_DATA,
            Self::MessageTooLarge => CloseCode::TOO_BIG,
            _ => CloseCode::PROTOCOL,
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservedBits => f.write_str("frame has reserved bits set"),
            Self::UnknownOpCode(opcode) => write!(f, "frame has reserved opcode {opcode:#x}"),
            Self::FragmentedControlFrame => f.write_str("control frame is fragmented"),
            Self::ControlFrameTooLarge => f.write_str("control frame payload exceeds 125 bytes"),
            Self::InvalidFrameLength => f.write_str("frame payload length has the MSB set"),
            Self::UnmaskedFrame => f.write_str("received an unmasked frame from the client"),
            Self::MaskedFrame => f.write_str("received a masked frame from the server"),
            Self::UnexpectedContinuation => {
                f.write_str("continuation frame outside of a fragmented message")
            }
            Self::ExpectedContinuation => {
                f.write_str("new message started inside of a fragmented message")
            }
            Self::InvalidUtf8 => f.write_str("text payload is not valid UTF-8"),
            Self::InvalidCloseCode(code) => write!(f, "invalid close code {code}"),
            Self::InvalidClosePayload => f.write_str("close frame has a 1-byte payload"),
            Self::MessageTooLarge => f.write_str("text message exceeds the size limit"),
        }
    }
}

impl Error for ProtocolError {}

impl From<ProtocolError> for io::Error {
    fn from(error: ProtocolError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
}

/// Returned by writes after our Close frame was queued.
pub(crate) fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "websocket close frame already sent",
    )
}

/// Returned by reads after the connection has failed.
pub(crate) fn failed() -> io::Error {
    io::Error::other("websocket connection has failed earlier")
}

/// Returned when the IO reaches EOF without a close handshake.
pub(crate) fn unexpected_eof() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "websocket connection closed without a close frame",
    )
}
