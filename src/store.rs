use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;

use crate::error::{Result, StoreError};

/// Unified storage for structured data and binary blobs in a single SQLite file
pub struct Store {
    conn: Mutex<Connection>,
    instance_id: String,
}

impl Store {
    /// Open or create a stoar database file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;

        let _ = conn.execute_batch("PRAGMA journal_mode = WAL")?;
        let _ = conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        let _ = conn.execute_batch("PRAGMA cache_size = 10000")?;
        let _ = conn.execute_batch("PRAGMA foreign_keys = ON")?;

        let instance_id = Uuid::new_v4().to_string();

        let store = Store {
            conn: Mutex::new(conn),
            instance_id,
        };

        store.init_schema()?;
        Ok(store)
    }

    /// Create an in-memory database (no file on disk)
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

        store.init_schema()?;
        Ok(store)
    }

    /// Initialize internal schema (idempotent, called eagerly on construction)
    #[allow(clippy::str_to_string)]
    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| {
            StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e))
        })?;

        let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS __meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")?;

        let _ = conn.execute_batch("CREATE TABLE IF NOT EXISTS __objects (key TEXT PRIMARY KEY, data BLOB NOT NULL, mime_type TEXT, size INTEGER, hash TEXT, created_at INTEGER, updated_at INTEGER)")?;

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

    /// List all user-created collections (excludes internal tables)
    #[inline]
    pub fn collections(&self) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let mut stmt = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\'"
        )?;

        let names = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(names)
    }

    /// List all stored blobs with metadata
    #[inline]
    pub fn blobs(&self) -> Result<Vec<(String, BlobMeta)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let mut stmt = conn.prepare(
            "SELECT key, size, mime_type, hash, created_at, updated_at FROM __objects ORDER BY key"
        )?;

        let items = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    BlobMeta {
                        size: row.get(1)?,
                        mime_type: row.get(2)?,
                        hash: row.get(3)?,
                        created_at: row.get(4)?,
                        updated_at: row.get(5)?,
                    },
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(items)
    }

    /// Get database metadata as key-value pairs
    #[inline]
    pub fn info(&self) -> Result<Vec<(String, String)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| StoreError::InvalidConfig(format!("Failed to acquire lock: {}", e)))?;

        let mut stmt = conn.prepare("SELECT key, value FROM __meta ORDER BY key")?;

        let pairs = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(pairs)
    }

    // ============================================================================
    // Structured Data API
    // ============================================================================

    /// Put a JSON-serializable value into a collection (auto-creates if needed)
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
    // Blob Storage API
    // ============================================================================

    /// Write binary data
    #[inline]
    pub fn write(&self, key: &str, data: &[u8]) -> Result<()> {
        self.write_with_meta(key, data, None)
    }

    /// Write binary data with MIME type metadata
    #[inline]
    pub fn write_with_meta(&self, key: &str, data: &[u8], mime_type: Option<&str>) -> Result<()> {
        validate_key(key)?;

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

    /// Execute operations atomically; commits on success, rolls back on error
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
    /// Put a JSON-serializable value into a collection
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

    /// Get a value from a collection
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

    /// Delete a value from a collection
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

    /// Write binary data
    #[inline]
    pub fn write(&self, key: &str, data: &[u8]) -> Result<()> {
        self.write_with_meta(key, data, None)
    }

    /// Write binary data with MIME type metadata
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

    /// Read binary data
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

    /// Get metadata about a blob
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

    /// Remove a blob
    #[inline]
    pub fn remove(&self, key: &str) -> Result<()> {
        validate_key(key)?;
        let _ = self.tx.execute("DELETE FROM __objects WHERE key = ?", [key])?;
        Ok(())
    }

    /// Get all values from a collection
    #[inline]
    pub fn all<T: serde::de::DeserializeOwned>(&self, collection: &str) -> Result<Vec<T>> {
        ensure_collection_tx(self.tx, collection)?;
        let sql = format!("SELECT data FROM [{}] ORDER BY rowid", collection);
        self.query::<T>(&sql, &[])
    }

    /// Count items in a collection
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

    /// Check if a key exists in a collection
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

    /// Execute a raw SQL query
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Validates a key is non-empty and within 1024 bytes
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestUser {
        name: String,
        age: u32,
    }

    // ============================
    // Schema initialization
    // ============================

    #[test]
    fn fresh_store_get_returns_none() {
        let store = Store::memory().unwrap();
        let result: Option<TestUser> = store.get("users", "nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fresh_store_read_returns_none() {
        let store = Store::memory().unwrap();
        let result = store.read("nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn fresh_store_meta_returns_none() {
        let store = Store::memory().unwrap();
        let result = store.meta("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fresh_store_collections_returns_empty() {
        let store = Store::memory().unwrap();
        let collections = store.collections().unwrap();
        assert!(collections.is_empty());
    }

    #[test]
    fn fresh_store_blobs_returns_empty() {
        let store = Store::memory().unwrap();
        let blobs = store.blobs().unwrap();
        assert!(blobs.is_empty());
    }

    // ============================
    // Structured data
    // ============================

    #[test]
    fn put_get_roundtrip() {
        let store = Store::memory().unwrap();
        let user = TestUser { name: "Alice".to_string(), age: 30 };
        store.put("users", "alice", &user).unwrap();
        let retrieved: Option<TestUser> = store.get("users", "alice").unwrap();
        assert_eq!(retrieved, Some(user));
    }

    #[test]
    fn put_overwrites_existing() {
        let store = Store::memory().unwrap();
        let user1 = TestUser { name: "Alice".to_string(), age: 30 };
        let user2 = TestUser { name: "Alice Updated".to_string(), age: 31 };
        store.put("users", "alice", &user1).unwrap();
        store.put("users", "alice", &user2).unwrap();
        let retrieved: Option<TestUser> = store.get("users", "alice").unwrap();
        assert_eq!(retrieved, Some(user2));
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let store = Store::memory().unwrap();
        store.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 }).unwrap();
        let result: Option<TestUser> = store.get("users", "missing").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn delete_then_get_returns_none() {
        let store = Store::memory().unwrap();
        let user = TestUser { name: "Alice".to_string(), age: 30 };
        store.put("users", "alice", &user).unwrap();
        store.delete("users", "alice").unwrap();
        let result: Option<TestUser> = store.get("users", "alice").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn exists_returns_correct_values() {
        let store = Store::memory().unwrap();
        store.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 }).unwrap();
        assert!(store.exists("users", "alice").unwrap());
        assert!(!store.exists("users", "missing").unwrap());
    }

    #[test]
    fn count_returns_correct_number() {
        let store = Store::memory().unwrap();
        assert_eq!(store.count("users").unwrap(), 0);
        store.put("users", "a", &TestUser { name: "A".to_string(), age: 1 }).unwrap();
        store.put("users", "b", &TestUser { name: "B".to_string(), age: 2 }).unwrap();
        store.put("users", "c", &TestUser { name: "C".to_string(), age: 3 }).unwrap();
        assert_eq!(store.count("users").unwrap(), 3);
    }

    #[test]
    fn all_returns_items_in_order() {
        let store = Store::memory().unwrap();
        let users = vec![
            TestUser { name: "Alice".to_string(), age: 30 },
            TestUser { name: "Bob".to_string(), age: 25 },
            TestUser { name: "Charlie".to_string(), age: 35 },
        ];
        for (i, u) in users.iter().enumerate() {
            store.put("users", &format!("user{}", i), u).unwrap();
        }
        let all: Vec<TestUser> = store.all("users").unwrap();
        assert_eq!(all, users);
    }

    #[test]
    fn query_with_json_extract() {
        let store = Store::memory().unwrap();
        store.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 }).unwrap();
        store.put("users", "bob", &TestUser { name: "Bob".to_string(), age: 20 }).unwrap();
        store.put("users", "charlie", &TestUser { name: "Charlie".to_string(), age: 40 }).unwrap();

        let old_users: Vec<TestUser> = store.query(
            "SELECT data FROM [users] WHERE json_extract(data, '$.age') > ?",
            &[&25],
        ).unwrap();
        assert_eq!(old_users.len(), 2);
    }

    #[test]
    fn put_get_json_value() {
        let store = Store::memory().unwrap();
        let data = serde_json::json!({"nested": {"key": "value"}, "array": [1, 2, 3]});
        store.put("config", "settings", &data).unwrap();
        let retrieved: Option<serde_json::Value> = store.get("config", "settings").unwrap();
        assert_eq!(retrieved, Some(data));
    }

    // ============================
    // Blob storage
    // ============================

    #[test]
    fn write_read_roundtrip() {
        let store = Store::memory().unwrap();
        let data = b"hello world binary data\x00\x01\x02";
        store.write("test.bin", data).unwrap();
        let read = store.read("test.bin").unwrap();
        assert_eq!(read, Some(data.to_vec()));
    }

    #[test]
    fn write_with_meta_stores_mime() {
        let store = Store::memory().unwrap();
        let data = b"PNG fake data";
        store.write_with_meta("photo.png", data, Some("image/png")).unwrap();
        let meta = store.meta("photo.png").unwrap().unwrap();
        assert_eq!(meta.mime_type, Some("image/png".to_string()));
        assert_eq!(meta.size, data.len() as i64);
        assert!(!meta.hash.is_empty());
    }

    #[test]
    fn blob_read_nonexistent_returns_none() {
        let store = Store::memory().unwrap();
        let result = store.read("nonexistent.bin").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn blob_meta_nonexistent_returns_none() {
        let store = Store::memory().unwrap();
        let result = store.meta("nonexistent.bin").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn remove_then_read_returns_none() {
        let store = Store::memory().unwrap();
        store.write("test.bin", b"data").unwrap();
        store.remove("test.bin").unwrap();
        let result = store.read("test.bin").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn large_blob() {
        let store = Store::memory().unwrap();
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        store.write("large.bin", &data).unwrap();
        let read = store.read("large.bin").unwrap().unwrap();
        assert_eq!(read.len(), data.len());
        assert_eq!(read, data);
    }

    // ============================
    // Transactions
    // ============================

    #[test]
    fn tx_commit_persists() {
        let store = Store::memory().unwrap();
        store.tx(|tx| {
            tx.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 })?;
            tx.put("users", "bob", &TestUser { name: "Bob".to_string(), age: 25 })?;
            Ok(())
        }).unwrap();
        assert_eq!(store.count("users").unwrap(), 2);
    }

    #[test]
    fn tx_error_rolls_back() {
        let store = Store::memory().unwrap();
        let result: Result<()> = store.tx(|tx| {
            tx.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 })?;
            Err(StoreError::InvalidKey("deliberate error".to_string()))
        });
        assert!(result.is_err());
        assert_eq!(store.count("users").unwrap(), 0);
    }

    #[test]
    fn tx_mixed_operations() {
        let store = Store::memory().unwrap();
        store.tx(|tx| {
            tx.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 })?;
            tx.write("doc.txt", b"hello")?;
            Ok(())
        }).unwrap();
        let user: Option<TestUser> = store.get("users", "alice").unwrap();
        assert!(user.is_some());
        let blob = store.read("doc.txt").unwrap();
        assert_eq!(blob, Some(b"hello".to_vec()));
    }

    #[test]
    fn tx_read_after_write() {
        let store = Store::memory().unwrap();
        store.tx(|tx| {
            tx.put("users", "alice", &TestUser { name: "Alice".to_string(), age: 30 })?;
            let user: Option<TestUser> = tx.get("users", "alice")?;
            assert_eq!(user.unwrap().name, "Alice");
            Ok(())
        }).unwrap();
    }

    // ============================
    // Validation
    // ============================

    #[test]
    fn empty_key_returns_error() {
        let store = Store::memory().unwrap();
        let result = store.put("users", "", &TestUser { name: "A".to_string(), age: 1 });
        assert!(matches!(result, Err(StoreError::InvalidKey(_))));
    }

    #[test]
    fn key_over_1024_bytes_returns_error() {
        let store = Store::memory().unwrap();
        let long_key = "x".repeat(1025);
        let result = store.put("users", &long_key, &TestUser { name: "A".to_string(), age: 1 });
        assert!(matches!(result, Err(StoreError::InvalidKey(_))));
    }

    #[test]
    fn empty_collection_returns_error() {
        let store = Store::memory().unwrap();
        let result = store.put("", "key", &TestUser { name: "A".to_string(), age: 1 });
        assert!(matches!(result, Err(StoreError::InvalidKey(_))));
    }

    // ============================
    // Edge cases
    // ============================

    #[test]
    fn multiple_collections_isolated() {
        let store = Store::memory().unwrap();
        store.put("users", "key1", &"user_value").unwrap();
        store.put("config", "key1", &"config_value").unwrap();

        let user: Option<String> = store.get("users", "key1").unwrap();
        let config: Option<String> = store.get("config", "key1").unwrap();

        assert_eq!(user, Some("user_value".to_string()));
        assert_eq!(config, Some("config_value".to_string()));
    }

    #[test]
    fn collections_lists_user_tables() {
        let store = Store::memory().unwrap();
        store.put("users", "a", &"val").unwrap();
        store.put("config", "b", &"val").unwrap();
        let mut collections = store.collections().unwrap();
        collections.sort();
        assert_eq!(collections, vec!["config", "users"]);
    }

    #[test]
    fn blobs_lists_all_stored() {
        let store = Store::memory().unwrap();
        store.write_with_meta("a.txt", b"aaa", Some("text/plain")).unwrap();
        store.write_with_meta("b.png", b"bbb", Some("image/png")).unwrap();
        let blobs = store.blobs().unwrap();
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].0, "a.txt");
        assert_eq!(blobs[1].0, "b.png");
    }

    #[test]
    fn info_returns_metadata() {
        let store = Store::memory().unwrap();
        let info = store.info().unwrap();
        let keys: Vec<&str> = info.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"instance_id"));
        assert!(keys.contains(&"schema_version"));
        assert!(keys.contains(&"initialized_at"));
    }

    #[test]
    fn instance_id_is_uuid_v4() {
        let store = Store::memory().unwrap();
        let id = store.instance_id();
        assert_eq!(id.len(), 36);
        assert_eq!(id.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn blob_hash_is_consistent() {
        let store = Store::memory().unwrap();
        let data = b"consistent data";
        store.write("test1", data).unwrap();
        let meta1 = store.meta("test1").unwrap().unwrap();
        store.remove("test1").unwrap();
        store.write("test1", data).unwrap();
        let meta2 = store.meta("test1").unwrap().unwrap();
        assert_eq!(meta1.hash, meta2.hash);
    }

    #[test]
    fn write_preserves_created_at_on_update() {
        let store = Store::memory().unwrap();
        store.write("test", b"v1").unwrap();
        let meta1 = store.meta("test").unwrap().unwrap();
        store.write("test", b"v2").unwrap();
        let meta2 = store.meta("test").unwrap().unwrap();
        assert_eq!(meta1.created_at, meta2.created_at);
        assert_eq!(meta2.size, 2);
    }
}
