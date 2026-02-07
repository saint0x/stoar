#![deny(
    warnings,
    missing_docs,
    unused_results,
    unused_qualifications,
    clippy::all
)]

//! stoar - Unified storage for structured data + blobs in a single SQLite file

/// Error types for stoar operations
pub mod error;
/// Core store implementation
pub mod store;

pub use error::{Result, StoreError};
pub use store::{BlobMeta, Store, TxHandle};

pub use serde::{Deserialize, Serialize};
pub use serde_json::{json, Value};
