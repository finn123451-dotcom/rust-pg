use thiserror::Error;

#[derive(Error, Debug)]
pub enum HeapError {
    #[error("Invalid page: {0}")]
    InvalidPage(String),

    #[error("Invalid tuple: {0}")]
    InvalidTuple(String),

    #[error("Invalid transaction: {0}")]
    InvalidTransaction(String),

    #[error("Invalid visibility: {0}")]
    InvalidVisibility(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Page not found: {0}")]
    PageNotFound(u32),

    #[error("No free space available")]
    NoFreeSpace,

    #[error("Corrupted data: {0}")]
    CorruptedData(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Lock error: {0}")]
    LockError(String),
}

pub type Result<T> = std::result::Result<T, HeapError>;
