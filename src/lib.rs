#![deny(
    warnings,
    missing_docs,
    unused_results,
    unused_qualifications,
    clippy::all
)]

//! stoar - Ultra-lightweight unified storage
//! Structured data + blobs in a single file with a sleek, single-word API

/// Error types for stoar operations
pub mod error;
/// Core store implementation
pub mod store;

pub use error::{Result, StoreError};
pub use store::{BlobMeta, Store, TxHandle};

// Re-export commonly used serde traits
pub use serde::{Deserialize, Serialize};
pub use serde_json::{json, Value};
