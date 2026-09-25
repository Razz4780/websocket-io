use std::{fmt, ops::Deref, str::Utf8Error};

use bytes::Bytes;

/// [`Bytes`] that are guaranteed to be valid UTF-8.
///
/// Used for text messages and close reasons, so that they can be handed out without copying.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Utf8Bytes(Bytes);

impl Utf8Bytes {
    /// Creates a value from a static string without copying.
    pub const fn from_static(text: &'static str) -> Self {
        Self(Bytes::from_static(text.as_bytes()))
    }

    /// Returns the text.
    pub fn as_str(&self) -> &str {
        // SAFETY: the contents are validated on construction.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    /// Returns the underlying bytes.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// # Safety
    ///
    /// `bytes` must be valid UTF-8.
    pub(crate) unsafe fn from_bytes_unchecked(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl Deref for Utf8Bytes {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for Utf8Bytes {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Utf8Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Utf8Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Utf8Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), f)
    }
}

impl TryFrom<Bytes> for Utf8Bytes {
    type Error = Utf8Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        std::str::from_utf8(&bytes)?;
        Ok(Self(bytes))
    }
}

impl From<String> for Utf8Bytes {
    fn from(text: String) -> Self {
        Self(Bytes::from(text))
    }
}

impl From<&'static str> for Utf8Bytes {
    fn from(text: &'static str) -> Self {
        Self::from_static(text)
    }
}

impl From<Utf8Bytes> for Bytes {
    fn from(text: Utf8Bytes) -> Self {
        text.0
    }
}

impl PartialEq<str> for Utf8Bytes {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Utf8Bytes {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}
