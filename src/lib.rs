#![deny(
    warnings,
    missing_docs,
    unused_results,
    unused_qualifications,
    clippy::all
)]

//! stoar - SQLite + content-addressable object storage in one binary and library.

/// Error types for stoar operations.
pub mod error;
/// Core store implementation.
pub mod store;

pub use error::{Result, StoreError};
pub use store::{
    AliasRecord, ContentMeta, GcReport, NamespaceConfig, SqlRows, Store, VerifyResult,
};

pub use serde::{Deserialize, Serialize};
pub use serde_json::{json, Value};
