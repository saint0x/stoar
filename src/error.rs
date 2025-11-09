use thiserror::Error;

/// Errors that can occur in stoar operations
#[derive(Error, Debug)]
pub enum StoreError {
    /// SQLite error
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// JSON serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid key format
    #[error("Invalid key: {0}")]
    InvalidKey(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Result type for stoar operations
pub type Result<T> = std::result::Result<T, StoreError>;
