use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

use crate::error::{Result, StoreError};

/// Ultra-lightweight unified storage for structured data + blobs
/// Single file, single transaction boundary, maximum performance
pub struct Store {
    conn: Mutex<Connection>,
    instance_id: String,
}

impl Store {
    /// Open or create a stoar database file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;

        // Performance tuning for low latency
        let _ = conn.execute_batch("PRAGMA journal_mode = WAL")?;
        let _ = conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        let _ = conn.execute_batch("PRAGMA cache_size = 10000")?;
        let _ = conn.execute_batch("PRAGMA foreign_keys = ON")?;

        let instance_id = Uuid::new_v4().to_string();

        let store = Store {
            conn: Mutex::new(conn),
            instance_id,
        };

        Ok(store)
    }

    /// Memory-only database (useful for tests)
    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        let _ = conn.execute_batch("PRAGMA journal_mode = WAL")?;
        let _ = conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        let _ = conn.execute_batch("PRAGMA cache_size = 10000")?;
        let _ = conn.execute_batch("PRAGMA foreign_keys = ON")?;

        let instance_id = Uuid::new_v4().to_string();
        let store = Store {
            conn: Mutex::new(conn),
            instance_id,
        };

        Ok(store)
    }

    /// Initialize internal schema (idempotent) - called on first put/get
    #[allow(clippy::str_to_string)]
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e))
        })?;

        // Create tables individually - use single line SQL to avoid execute_batch issues
        let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS __meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")?;

        let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS __objects (key TEXT PRIMARY KEY, data BLOB NOT NULL, mime_type TEXT, size INTEGER, hash TEXT, created_at INTEGER, updated_at INTEGER)")?;

        // Initialize meta table with instance_id and schema version
        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta VALUES ('instance_id', ?)",
            params![&self.instance_id],
        )?;
        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta VALUES ('schema_version', '1')",
            params![],
        )?;
        let now = chrono::Utc::now().to_rfc3339();
        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta VALUES ('initialized_at', ?)",
            params![&now],
        )?;

        Ok(())
    }

    // ============================================================================
    // Metadata API
    // ============================================================================

    /// Get the unique instance ID for this store
    #[inline]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    // ============================================================================
    // Structured Data API - Clean & Sleek
    // ============================================================================

    /// Put a JSON-serializable value into a collection
    /// Auto-creates collection if it doesn't exist
    #[inline]
    pub fn put<T: serde::Serialize>(&self, collection: &str, key: &str, value: &T) -> Result<()> {
        validate_key(key)?;
        let data = serde_json::to_string(value)?;
        self.put_raw(collection, key, &data)
    }

    /// Get a value from a collection
    #[inline]
    pub fn get<T: serde::de::DeserializeOwned>(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<Option<T>> {
        match self.get_raw(collection, key)? {
            Some(data) => Ok(Some(serde_json::from_str(&data)?)),
            None => Ok(None),
        }
    }

    /// Delete a value from a collection
    #[inline]
    pub fn delete(&self, collection: &str, key: &str) -> Result<()> {
        validate_key(key)?;
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;

        let _ = conn.execute(
            &format!("DELETE FROM [{}] WHERE key = ?", collection),
            [key],
        )?;

        Ok(())
    }

    /// Query a collection with SQL
    #[inline]
    pub fn query<T: serde::de::DeserializeOwned>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<T>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let mut stmt = conn.prepare(sql)?;
        let results = stmt
            .query_map(params, |row| {
                let json_str: String = row.get(0)?;
                Ok(json_str)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results
            .into_iter()
            .map(|json_str| serde_json::from_str(&json_str))
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Get all values from a collection
    #[inline]
    pub fn all<T: serde::de::DeserializeOwned>(&self, collection: &str) -> Result<Vec<T>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;
        let sql = format!("SELECT data FROM [{}] ORDER BY rowid", collection);
        drop(conn);
        self.query::<T>(&sql, &[])
    }

    /// Count items in a collection
    #[inline]
    pub fn count(&self, collection: &str) -> Result<usize> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;
        let count: usize = conn.query_row(
            &format!("SELECT COUNT(*) FROM [{}]", collection),
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Check if a key exists in a collection
    #[inline]
    pub fn exists(&self, collection: &str, key: &str) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;
        let exists: bool = conn.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM [{}] WHERE key = ?)", collection),
            [key],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    // ============================================================================
    // Blob Storage API - Integrated
    // ============================================================================

    /// Write binary data with optional metadata
    #[inline]
    pub fn write(&self, key: &str, data: &[u8]) -> Result<()> {
        self.write_with_meta(key, data, None)
    }

    /// Write with MIME type
    #[inline]
    pub fn write_with_meta(&self, key: &str, data: &[u8], mime_type: Option<&str>) -> Result<()> {
        validate_key(key)?;

        // Initialize schema on first write
        self.init_schema()?;

        let hash = format!("{:x}", calculate_hash(data));
        let now = chrono::Utc::now().timestamp();
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let _ = conn.execute(
            "INSERT OR REPLACE INTO __objects (key, data, mime_type, size, hash, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM __objects WHERE key = ?), ?), ?)",
            params![
                key,
                data,
                mime_type,
                data.len() as i64,
                hash,
                key,
                now,
                now
            ],
        )?;

        Ok(())
    }

    /// Read binary data
    #[inline]
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;

        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let data = conn
            .query_row(
                "SELECT data FROM __objects WHERE key = ?",
                [key],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        Ok(data)
    }

    /// Get metadata about a blob
    #[inline]
    pub fn meta(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_key(key)?;

        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let meta = conn
            .query_row(
                "SELECT size, mime_type, hash, created_at, updated_at FROM __objects WHERE key = ?",
                [key],
                |row| {
                    Ok(BlobMeta {
                        size: row.get(0)?,
                        mime_type: row.get(1)?,
                        hash: row.get(2)?,
                        created_at: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                },
            )
            .optional()?;

        Ok(meta)
    }

    /// Remove a blob
    #[inline]
    pub fn remove(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        let _ = conn.execute("DELETE FROM __objects WHERE key = ?", [key])?;
        Ok(())
    }

    // ============================================================================
    // Transactions
    // ============================================================================

    /// Execute a transaction - automatically commits on success
    pub fn tx<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&TxHandle) -> Result<T>,
    {
        let mut conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        let tx = conn.transaction()?;
        let handle = TxHandle { tx: &tx };
        let result = f(&handle)?;
        tx.commit()?;
        Ok(result)
    }

    // ============================================================================
    // Internal Helpers
    // ============================================================================

    fn put_raw(&self, collection: &str, key: &str, data: &str) -> Result<()> {
        // Initialize schema on first write
        self.init_schema()?;

        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;

        let _ = conn.execute(
            &format!(
                "INSERT OR REPLACE INTO [{}] (key, data) VALUES (?, ?)",
                collection
            ),
            params![key, data],
        )?;

        Ok(())
    }

    fn get_raw(&self, collection: &str, key: &str) -> Result<Option<String>> {
        validate_key(key)?;
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;
        self.ensure_collection_inner(&conn, collection)?;

        let data = conn
            .query_row(
                &format!("SELECT data FROM [{}] WHERE key = ?", collection),
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        Ok(data)
    }

    fn ensure_collection_inner(
        &self,
        conn: &Connection,
        name: &str,
    ) -> Result<()> {
        validate_key(name)?;

        let _ = conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS [{}] (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )",
                name
            ),
            [],
        )?;

        Ok(())
    }
}

/// Transaction handle for batch operations
pub struct TxHandle<'a> {
    tx: &'a rusqlite::Transaction<'a>,
}

impl<'a> TxHandle<'a> {
    /// Put during transaction
    #[inline]
    pub fn put<T: serde::Serialize>(
        &self,
        collection: &str,
        key: &str,
        value: &T,
    ) -> Result<()> {
        validate_key(key)?;
        let data = serde_json::to_string(value)?;
        ensure_collection_tx(self.tx, collection)?;

        let _ = self.tx.execute(
            &format!(
                "INSERT OR REPLACE INTO [{}] (key, data) VALUES (?, ?)",
                collection
            ),
            params![key, data],
        )?;

        Ok(())
    }

    /// Get during transaction
    #[inline]
    pub fn get<T: serde::de::DeserializeOwned>(
        &self,
        collection: &str,
        key: &str,
    ) -> Result<Option<T>> {
        validate_key(key)?;
        ensure_collection_tx(self.tx, collection)?;

        let data = self
            .tx
            .query_row(
                &format!("SELECT data FROM [{}] WHERE key = ?", collection),
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;

        match data {
            Some(json_str) => Ok(Some(serde_json::from_str(&json_str)?)),
            None => Ok(None),
        }
    }

    /// Delete a value from a collection during transaction
    #[inline]
    pub fn delete(&self, collection: &str, key: &str) -> Result<()> {
        validate_key(key)?;
        ensure_collection_tx(self.tx, collection)?;

        let _ = self.tx.execute(
            &format!("DELETE FROM [{}] WHERE key = ?", collection),
            [key],
        )?;

        Ok(())
    }

    /// Write blob during transaction
    #[inline]
    pub fn write(&self, key: &str, data: &[u8]) -> Result<()> {
        self.write_with_meta(key, data, None)
    }

    /// Write blob with metadata during transaction
    #[inline]
    pub fn write_with_meta(&self, key: &str, data: &[u8], mime_type: Option<&str>) -> Result<()> {
        validate_key(key)?;

        let hash = format!("{:x}", calculate_hash(data));
        let now = chrono::Utc::now().timestamp();

        let _ = self.tx.execute(
            "INSERT OR REPLACE INTO __objects (key, data, mime_type, size, hash, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM __objects WHERE key = ?), ?), ?)",
            params![
                key,
                data,
                mime_type,
                data.len() as i64,
                hash,
                key,
                now,
                now
            ],
        )?;

        Ok(())
    }

    /// Read binary blob during transaction
    #[inline]
    pub fn read(&self, key: &str) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;

        let data = self
            .tx
            .query_row(
                "SELECT data FROM __objects WHERE key = ?",
                [key],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;

        Ok(data)
    }

    /// Get blob metadata during transaction
    #[inline]
    pub fn meta(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_key(key)?;

        let meta = self
            .tx
            .query_row(
                "SELECT size, mime_type, hash, created_at, updated_at FROM __objects WHERE key = ?",
                [key],
                |row| {
                    Ok(BlobMeta {
                        size: row.get(0)?,
                        mime_type: row.get(1)?,
                        hash: row.get(2)?,
                        created_at: row.get(3)?,
                        updated_at: row.get(4)?,
                    })
                },
            )
            .optional()?;

        Ok(meta)
    }

    /// Remove a blob during transaction
    #[inline]
    pub fn remove(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        let _ = self.tx.execute("DELETE FROM __objects WHERE key = ?", [key])?;
        Ok(())
    }

    /// Get all items from a collection during transaction
    #[inline]
    pub fn all<T: serde::de::DeserializeOwned>(&self, collection: &str) -> Result<Vec<T>> {
        ensure_collection_tx(self.tx, collection)?;
        let sql = format!("SELECT data FROM [{}] ORDER BY rowid", collection);
        self.query::<T>(&sql, &[])
    }

    /// Count items in a collection during transaction
    #[inline]
    pub fn count(&self, collection: &str) -> Result<usize> {
        ensure_collection_tx(self.tx, collection)?;
        let count: usize = self.tx.query_row(
            &format!("SELECT COUNT(*) FROM [{}]", collection),
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Check if a key exists in a collection during transaction
    #[inline]
    pub fn exists(&self, collection: &str, key: &str) -> Result<bool> {
        ensure_collection_tx(self.tx, collection)?;
        let exists: bool = self.tx.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM [{}] WHERE key = ?)", collection),
            [key],
            |row| row.get(0),
        )?;
        Ok(exists)
    }

    /// Query a collection with SQL during transaction
    #[inline]
    pub fn query<T: serde::de::DeserializeOwned>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<T>> {
        let mut stmt = self.tx.prepare(sql)?;
        let results = stmt
            .query_map(params, |row| {
                let json_str: String = row.get(0)?;
                Ok(json_str)
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results
            .into_iter()
            .map(|json_str| serde_json::from_str(&json_str))
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }
}

// ============================================================================
// Blob Metadata
// ============================================================================

/// Metadata about a stored blob
#[derive(Debug, Clone)]
pub struct BlobMeta {
    /// Size of the blob in bytes
    pub size: i64,
    /// MIME type of the blob
    pub mime_type: Option<String>,
    /// Hash of the blob content
    pub hash: String,
    /// Unix timestamp of creation
    pub created_at: i64,
    /// Unix timestamp of last update
    pub updated_at: i64,
}

// ============================================================================
// Helpers
// ============================================================================

/// Validates a key for proper format
#[inline]
fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(StoreError::InvalidKey("Key cannot be empty".to_string()));
    }
    if key.len() > 1024 {
        return Err(StoreError::InvalidKey(
            "Key cannot exceed 1024 bytes".to_string(),
        ));
    }
    Ok(())
}

/// Ensures a collection table exists within a transaction
fn ensure_collection_tx(tx: &rusqlite::Transaction, name: &str) -> Result<()> {
    validate_key(name)?;

    let _ = tx.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS [{}] (
                key TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )",
            name
        ),
        [],
    )?;

    Ok(())
}

/// Calculates a hash for blob data
#[inline]
fn calculate_hash(data: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}
