use std::fmt;
use std::io;

#[derive(Debug)]
pub enum PebbleError {
    Io(io::Error),
    WalCorruption(String),
    WalIncomplete,
    SSTableCorruption(String),
    InjectedFault(String),
    CacheCorruption(String),
}

impl fmt::Display for PebbleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PebbleError::Io(e) => write!(f, "I/O error: {}", e),
            PebbleError::WalCorruption(msg) => write!(f, "WAL corruption: {}", msg),
            PebbleError::WalIncomplete => write!(f, "incomplete WAL record"),
            PebbleError::SSTableCorruption(msg) => write!(f, "SSTable corruption: {}", msg),
            PebbleError::InjectedFault(msg) => write!(f, "injected fault: {}", msg),
            PebbleError::CacheCorruption(msg) => write!(f, "cache corruption: {}", msg),
        }
    }
}

impl std::error::Error for PebbleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PebbleError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for PebbleError {
    fn from(e: io::Error) -> Self {
        PebbleError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, PebbleError>;
