use base64::Engine;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, types::ValueRef, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::error::{Result, StoreError};

const ALGO_SHA256: &str = "sha256";
const INLINE_THRESHOLD_BYTES: u64 = 1024 * 1024;
const CHUNK_SIZE_BYTES: usize = 1024 * 1024;

/// Result rowset from a SQL query.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SqlRows {
    /// Ordered column names.
    pub columns: Vec<String>,
    /// Rows as JSON values aligned with `columns`.
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Metadata about content stored in CAS.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContentMeta {
    /// Canonical content identifier, e.g. `sha256:<hex>`.
    pub content_id: String,
    /// Hash algorithm.
    pub algo: String,
    /// Hex digest.
    pub digest: String,
    /// Size in bytes.
    pub size_bytes: i64,
    /// Optional MIME type from ingest.
    pub mime_type: Option<String>,
    /// Storage layout.
    pub storage_kind: String,
    /// Creation timestamp (unix seconds).
    pub created_at: i64,
    /// Last successful verify timestamp (unix seconds).
    pub verified_at: Option<i64>,
}

/// Alias record mapping a logical name to a content id.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AliasRecord {
    /// Namespace partition.
    pub namespace: String,
    /// Alias inside namespace.
    pub alias: String,
    /// Current content id target.
    pub content_id: String,
    /// Monotonic version for CAS-style compare-and-swap.
    pub version: i64,
    /// Update timestamp (unix seconds).
    pub updated_at: i64,
}

/// Content verification result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerifyResult {
    /// Content id requested.
    pub content_id: String,
    /// Whether payload hash matched content id.
    pub ok: bool,
    /// Message describing failure or success mode.
    pub message: String,
}

/// Garbage collection run report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GcReport {
    /// Dry run mode leaves data untouched.
    pub dry_run: bool,
    /// Number of candidate unreferenced, unpinned objects.
    pub candidates: i64,
    /// Number of deleted objects (0 for dry run).
    pub deleted: i64,
}

/// Namespace configuration for multi-tenant policy.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NamespaceConfig {
    /// Namespace identifier.
    pub namespace: String,
    /// Optional bearer token for namespace-scoped API access.
    pub token: Option<String>,
    /// Read-only namespace flag.
    pub read_only: bool,
    /// Optional max unique objects attached to namespace.
    pub max_objects: Option<i64>,
    /// Optional max total bytes attached to namespace.
    pub max_bytes: Option<i64>,
    /// Created-at unix timestamp.
    pub created_at: i64,
    /// Updated-at unix timestamp.
    pub updated_at: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncObject {
    content_id: String,
    algo: String,
    digest: String,
    size_bytes: i64,
    mime_type: Option<String>,
    created_at: i64,
    verified_at: Option<i64>,
    storage_kind: String,
    inline_blob_b64: Option<String>,
    external_uri: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncChunk {
    content_id: String,
    chunk_index: i64,
    chunk_size: i64,
    chunk_hash: String,
    payload_b64: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncRef {
    content_id: String,
    ref_count: i64,
    pinned: i64,
    updated_at: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncBundle {
    objects: Vec<SyncObject>,
    chunks: Vec<SyncChunk>,
    aliases: Vec<AliasRecord>,
    refs: Vec<SyncRef>,
    namespaces: Vec<NamespaceConfig>,
    namespace_objects: Vec<(String, String, i64)>,
}

/// Unified SQLite + CAS store.
pub struct Store {
    pool: Pool<SqliteConnectionManager>,
    db_path: Option<PathBuf>,
    instance_id: String,
}

impl Store {
    /// Open or create a stoar database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        let manager = SqliteConnectionManager::file(&db_path);
        let pool = Pool::builder()
            .max_size(16)
            .build(manager)
            .map_err(|e| StoreError::InvalidConfig(format!("pool build failed: {e}")))?;

        {
            let conn = pool
                .get()
                .map_err(|e| StoreError::InvalidConfig(format!("pool get failed: {e}")))?;
            Self::configure_connection(&conn)?;
        }

        let store = Self {
            pool,
            db_path: Some(db_path),
            instance_id: Uuid::new_v4().to_string(),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Create an in-memory database.
    pub fn memory() -> Result<Self> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .build(manager)
            .map_err(|e| StoreError::InvalidConfig(format!("pool build failed: {e}")))?;

        {
            let conn = pool
                .get()
                .map_err(|e| StoreError::InvalidConfig(format!("pool get failed: {e}")))?;
            Self::configure_connection(&conn)?;
        }

        let store = Self {
            pool,
            db_path: None,
            instance_id: Uuid::new_v4().to_string(),
        };
        store.init_schema()?;
        Ok(store)
    }

    /// Get process-local store instance id.
    #[inline]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Execute SQL that does not return rows.
    pub fn execute_sql(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<usize> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(sql)?;
        let changed = stmt.execute(params)?;
        Ok(changed)
    }

    /// Execute SQL and return raw row/column values.
    pub fn query_sql(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Result<SqlRows> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(sql)?;

        let columns = stmt
            .column_names()
            .iter()
            .map(|n| (*n).to_string())
            .collect::<Vec<_>>();

        let col_count = stmt.column_count();
        let mut rows = Vec::new();
        let mut cursor = stmt.query(params)?;

        while let Some(row) = cursor.next()? {
            let mut vals = Vec::with_capacity(col_count);
            for idx in 0..col_count {
                vals.push(value_ref_to_json(row.get_ref(idx)?));
            }
            rows.push(vals);
        }

        Ok(SqlRows { columns, rows })
    }

    /// Execute SQL that does not return rows and append an audit event.
    pub fn execute_sql_as(
        &self,
        actor: &str,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        params_json: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(sql)?;
        let changed = stmt.execute(params)?;
        drop(stmt);
        self.audit_sql(&conn, Some(actor), sql, params_json, Some(changed as i64))?;
        Ok(changed)
    }

    /// Execute SQL query and append an audit event.
    pub fn query_sql_as(
        &self,
        actor: &str,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        params_json: Option<&str>,
    ) -> Result<SqlRows> {
        let rows = self.query_sql(sql, params)?;
        let conn = self.conn()?;
        self.audit_sql(
            &conn,
            Some(actor),
            sql,
            params_json,
            Some(rows.rows.len() as i64),
        )?;
        Ok(rows)
    }

    /// Put content and return canonical content id. Duplicate content is deduplicated.
    pub fn put_content(&self, data: &[u8], mime_type: Option<&str>) -> Result<String> {
        let digest = sha256_hex(data);
        let content_id = format!("{ALGO_SHA256}:{digest}");
        let now = chrono::Utc::now().timestamp();

        self.with_tx(|tx| {
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM cas_objects WHERE content_id = ?",
                    [content_id.as_str()],
                    |r| r.get(0),
                )
                .optional()?;

            if exists.is_none() {
                let size = data.len() as i64;
                let storage_kind = if data.len() as u64 <= INLINE_THRESHOLD_BYTES {
                    "inline"
                } else {
                    "chunked"
                };

                let _ = tx.execute(
                    "INSERT INTO cas_objects
                     (content_id, algo, digest, size_bytes, mime_type, created_at, verified_at, storage_kind, inline_blob, external_uri)
                     VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, NULL)",
                    params![
                        content_id,
                        ALGO_SHA256,
                        digest,
                        size,
                        mime_type,
                        now,
                        storage_kind,
                        if storage_kind == "inline" { Some(data) } else { None }
                    ],
                )?;

                if storage_kind == "chunked" {
                    for (idx, chunk) in data.chunks(CHUNK_SIZE_BYTES).enumerate() {
                        let _ = tx.execute(
                            "INSERT INTO cas_object_chunks (content_id, chunk_index, chunk_size, chunk_hash, payload)
                             VALUES (?, ?, ?, ?, ?)",
                            params![
                                content_id,
                                idx as i64,
                                chunk.len() as i64,
                                sha256_hex(chunk),
                                chunk
                            ],
                        )?;
                    }
                }
            }

            let _ = tx.execute(
                "INSERT INTO cas_refs (content_id, ref_count, pinned, updated_at)
                 VALUES (?, 0, 0, ?)
                 ON CONFLICT(content_id) DO NOTHING",
                params![content_id, now],
            )?;

            Ok(())
        })?;

        Ok(content_id)
    }

    /// Put content from a file path using streaming hashing/chunking.
    pub fn put_content_file<P: AsRef<Path>>(
        &self,
        path: P,
        mime_type: Option<&str>,
    ) -> Result<String> {
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; CHUNK_SIZE_BYTES];
        let mut total: u64 = 0;

        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            hasher.update(&buf[..n]);
        }

        let digest = base16_encode(&hasher.finalize());
        let content_id = format!("{ALGO_SHA256}:{digest}");

        let exists = self.head_content(&content_id)?.is_some();
        if exists {
            return Ok(content_id);
        }

        if total <= INLINE_THRESHOLD_BYTES {
            let bytes = std::fs::read(path)?;
            return self.put_content(&bytes, mime_type);
        }

        let now = chrono::Utc::now().timestamp();
        self.with_tx(|tx| {
            let _ = tx.execute(
                "INSERT INTO cas_objects
                 (content_id, algo, digest, size_bytes, mime_type, created_at, verified_at, storage_kind, inline_blob, external_uri)
                 VALUES (?, ?, ?, ?, ?, ?, NULL, 'chunked', NULL, NULL)",
                params![content_id, ALGO_SHA256, digest, total as i64, mime_type, now],
            )?;

            let mut file = std::fs::File::open(path)?;
            let mut idx = 0_i64;
            let mut chunk = vec![0u8; CHUNK_SIZE_BYTES];
            loop {
                let n = file.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                let bytes = &chunk[..n];
                let _ = tx.execute(
                    "INSERT INTO cas_object_chunks (content_id, chunk_index, chunk_size, chunk_hash, payload)
                     VALUES (?, ?, ?, ?, ?)",
                    params![content_id, idx, n as i64, sha256_hex(bytes), bytes],
                )?;
                idx += 1;
            }

            let _ = tx.execute(
                "INSERT INTO cas_refs (content_id, ref_count, pinned, updated_at)
                 VALUES (?, 0, 0, ?)
                 ON CONFLICT(content_id) DO NOTHING",
                params![content_id, now],
            )?;
            Ok(())
        })?;

        Ok(content_id)
    }

    /// Put content from a reader by spooling to a temporary file.
    pub fn put_content_reader<R: Read>(
        &self,
        mut reader: R,
        mime_type: Option<&str>,
    ) -> Result<String> {
        let temp = std::env::temp_dir().join(format!("stoar-upload-{}.bin", Uuid::new_v4()));
        {
            let mut out = std::fs::File::create(&temp)?;
            let mut buf = vec![0u8; CHUNK_SIZE_BYTES];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                out.write_all(&buf[..n])?;
            }
            out.flush()?;
        }

        let result = self.put_content_file(&temp, mime_type);
        let _ = std::fs::remove_file(&temp);
        result
    }

    /// Get full content payload by content id.
    pub fn get_content(&self, content_id: &str) -> Result<Option<Vec<u8>>> {
        validate_content_id(content_id)?;
        let conn = self.conn()?;

        let row = conn
            .query_row(
                "SELECT storage_kind, inline_blob FROM cas_objects WHERE content_id = ?",
                [content_id],
                |r| {
                    let kind: String = r.get(0)?;
                    let inline: Option<Vec<u8>> = r.get(1)?;
                    Ok((kind, inline))
                },
            )
            .optional()?;

        match row {
            None => Ok(None),
            Some((kind, inline)) if kind == "inline" => Ok(inline),
            Some((kind, _)) if kind == "chunked" => {
                let mut stmt = conn.prepare(
                    "SELECT payload FROM cas_object_chunks WHERE content_id = ? ORDER BY chunk_index ASC",
                )?;
                let mut out = Vec::new();
                let mut rows = stmt.query([content_id])?;
                while let Some(r) = rows.next()? {
                    let chunk: Vec<u8> = r.get(0)?;
                    out.extend_from_slice(&chunk);
                }
                Ok(Some(out))
            }
            Some((kind, _)) => Err(StoreError::InvalidConfig(format!(
                "unsupported storage_kind: {kind}"
            ))),
        }
    }

    /// Stream content directly to a writer without assembling a full in-memory payload.
    pub fn stream_content_to_writer<W: Write>(
        &self,
        content_id: &str,
        mut writer: W,
    ) -> Result<bool> {
        validate_content_id(content_id)?;
        let conn = self.conn()?;

        let row = conn
            .query_row(
                "SELECT storage_kind, inline_blob FROM cas_objects WHERE content_id = ?",
                [content_id],
                |r| {
                    let kind: String = r.get(0)?;
                    let inline: Option<Vec<u8>> = r.get(1)?;
                    Ok((kind, inline))
                },
            )
            .optional()?;

        let Some((kind, inline)) = row else {
            return Ok(false);
        };

        if kind == "inline" {
            if let Some(bytes) = inline {
                writer.write_all(&bytes)?;
            }
            return Ok(true);
        }

        if kind == "chunked" {
            let mut stmt = conn.prepare(
                "SELECT payload FROM cas_object_chunks WHERE content_id = ? ORDER BY chunk_index ASC",
            )?;
            let mut rows = stmt.query([content_id])?;
            while let Some(r) = rows.next()? {
                let chunk: Vec<u8> = r.get(0)?;
                writer.write_all(&chunk)?;
            }
            return Ok(true);
        }

        Err(StoreError::InvalidConfig(format!(
            "unsupported storage_kind: {kind}"
        )))
    }

    /// Read a byte range from content.
    pub fn read_range(
        &self,
        content_id: &str,
        offset: usize,
        length: usize,
    ) -> Result<Option<Vec<u8>>> {
        validate_content_id(content_id)?;
        let conn = self.conn()?;

        let row = conn
            .query_row(
                "SELECT storage_kind, inline_blob, size_bytes FROM cas_objects WHERE content_id = ?",
                [content_id],
                |r| {
                    let kind: String = r.get(0)?;
                    let inline: Option<Vec<u8>> = r.get(1)?;
                    let size: i64 = r.get(2)?;
                    Ok((kind, inline, size))
                },
            )
            .optional()?;

        let Some((kind, inline, size_bytes)) = row else {
            return Ok(None);
        };

        let total = size_bytes.max(0) as usize;
        if offset >= total {
            return Ok(Some(Vec::new()));
        }

        let to_read = length.min(total - offset);

        if kind == "inline" {
            let bytes = inline.unwrap_or_default();
            let end = offset + to_read;
            return Ok(Some(bytes[offset..end].to_vec()));
        }

        if kind == "chunked" {
            let mut out = Vec::with_capacity(to_read);
            let mut remaining = to_read;
            let mut cursor = offset;

            let mut stmt = conn.prepare(
                "SELECT chunk_size, payload FROM cas_object_chunks WHERE content_id = ? ORDER BY chunk_index ASC",
            )?;
            let mut rows = stmt.query([content_id])?;
            let mut file_pos = 0usize;

            while let Some(r) = rows.next()? {
                if remaining == 0 {
                    break;
                }
                let chunk_size: i64 = r.get(0)?;
                let payload: Vec<u8> = r.get(1)?;
                let chunk_size = chunk_size.max(0) as usize;

                if cursor >= file_pos + chunk_size {
                    file_pos += chunk_size;
                    continue;
                }

                let local_start = cursor.saturating_sub(file_pos);
                let available = chunk_size - local_start;
                let take = available.min(remaining);
                out.extend_from_slice(&payload[local_start..local_start + take]);
                cursor += take;
                remaining -= take;
                file_pos += chunk_size;
            }

            return Ok(Some(out));
        }

        Err(StoreError::InvalidConfig(format!(
            "unsupported storage_kind: {kind}"
        )))
    }

    /// Head metadata for a content id.
    pub fn head_content(&self, content_id: &str) -> Result<Option<ContentMeta>> {
        validate_content_id(content_id)?;
        let conn = self.conn()?;

        let meta = conn
            .query_row(
                "SELECT content_id, algo, digest, size_bytes, mime_type, storage_kind, created_at, verified_at
                 FROM cas_objects WHERE content_id = ?",
                [content_id],
                |r| {
                    Ok(ContentMeta {
                        content_id: r.get(0)?,
                        algo: r.get(1)?,
                        digest: r.get(2)?,
                        size_bytes: r.get(3)?,
                        mime_type: r.get(4)?,
                        storage_kind: r.get(5)?,
                        created_at: r.get(6)?,
                        verified_at: r.get(7)?,
                    })
                },
            )
            .optional()?;

        Ok(meta)
    }

    /// Atomically set alias -> content_id, optional compare-and-swap on expected version.
    pub fn set_alias(
        &self,
        namespace: &str,
        alias: &str,
        content_id: &str,
        expected_version: Option<i64>,
    ) -> Result<AliasRecord> {
        validate_key(namespace, "namespace")?;
        validate_key(alias, "alias")?;
        validate_content_id(content_id)?;

        if self.head_content(content_id)?.is_none() {
            return Err(StoreError::InvalidKey(format!(
                "content_id not found: {content_id}"
            )));
        }

        let now = chrono::Utc::now().timestamp();

        self.with_tx(|tx| {
            ensure_namespace_exists_tx(tx, namespace, now)?;
            let current: Option<(String, i64)> = tx
                .query_row(
                    "SELECT content_id, version FROM cas_aliases WHERE namespace = ? AND alias = ?",
                    params![namespace, alias],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;

            if let Some(expected) = expected_version {
                let existing = current.as_ref().map(|(_, v)| *v);
                if existing != Some(expected) {
                    return Err(StoreError::InvalidConfig(format!(
                        "alias version mismatch: expected {expected}, got {:?}",
                        existing
                    )));
                }
            }

            attach_namespace_object_tx(tx, namespace, content_id, now)?;

            if let Some((old_content_id, _)) = &current {
                if old_content_id != content_id {
                    update_refcount_tx(tx, old_content_id, -1, now)?;
                    update_refcount_tx(tx, content_id, 1, now)?;
                }
            } else {
                update_refcount_tx(tx, content_id, 1, now)?;
            }

            let next_version = current.as_ref().map(|(_, v)| *v + 1).unwrap_or(1);
            let _ = tx.execute(
                "INSERT INTO cas_aliases (namespace, alias, content_id, version, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(namespace, alias)
                 DO UPDATE SET content_id = excluded.content_id, version = excluded.version, updated_at = excluded.updated_at",
                params![namespace, alias, content_id, next_version, now],
            )?;

            if let Some((old_content_id, _)) = &current {
                if old_content_id != content_id {
                    let still_used: i64 = tx.query_row(
                        "SELECT COUNT(*) FROM cas_aliases WHERE namespace = ? AND content_id = ?",
                        params![namespace, old_content_id],
                        |r| r.get(0),
                    )?;
                    if still_used == 0 {
                        let _ = tx.execute(
                            "DELETE FROM cas_namespace_objects WHERE namespace = ? AND content_id = ?",
                            params![namespace, old_content_id],
                        )?;
                    }
                }
            }

            Ok(AliasRecord {
                namespace: namespace.to_string(),
                alias: alias.to_string(),
                content_id: content_id.to_string(),
                version: next_version,
                updated_at: now,
            })
        })
    }

    /// Resolve alias to current content id and metadata.
    pub fn resolve_alias(&self, namespace: &str, alias: &str) -> Result<Option<AliasRecord>> {
        validate_key(namespace, "namespace")?;
        validate_key(alias, "alias")?;

        let conn = self.conn()?;
        let out = conn
            .query_row(
                "SELECT namespace, alias, content_id, version, updated_at
                 FROM cas_aliases
                 WHERE namespace = ? AND alias = ?",
                params![namespace, alias],
                |r| {
                    Ok(AliasRecord {
                        namespace: r.get(0)?,
                        alias: r.get(1)?,
                        content_id: r.get(2)?,
                        version: r.get(3)?,
                        updated_at: r.get(4)?,
                    })
                },
            )
            .optional()?;
        Ok(out)
    }

    /// Remove an alias mapping and decrement reference count.
    pub fn remove_alias(&self, namespace: &str, alias: &str) -> Result<bool> {
        validate_key(namespace, "namespace")?;
        validate_key(alias, "alias")?;
        let now = chrono::Utc::now().timestamp();

        self.with_tx(|tx| {
            let current: Option<String> = tx
                .query_row(
                    "SELECT content_id FROM cas_aliases WHERE namespace = ? AND alias = ?",
                    params![namespace, alias],
                    |r| r.get(0),
                )
                .optional()?;

            let Some(content_id) = current else {
                return Ok(false);
            };

            let _ = tx.execute(
                "DELETE FROM cas_aliases WHERE namespace = ? AND alias = ?",
                params![namespace, alias],
            )?;
            update_refcount_tx(tx, &content_id, -1, now)?;

            let still_used: i64 = tx.query_row(
                "SELECT COUNT(*) FROM cas_aliases WHERE namespace = ? AND content_id = ?",
                params![namespace, content_id],
                |r| r.get(0),
            )?;
            if still_used == 0 {
                let _ = tx.execute(
                    "DELETE FROM cas_namespace_objects WHERE namespace = ? AND content_id = ?",
                    params![namespace, content_id],
                )?;
            }

            Ok(true)
        })
    }

    /// Create or update namespace policy.
    pub fn upsert_namespace(
        &self,
        namespace: &str,
        token: Option<&str>,
        read_only: bool,
        max_objects: Option<i64>,
        max_bytes: Option<i64>,
    ) -> Result<NamespaceConfig> {
        validate_key(namespace, "namespace")?;
        let now = chrono::Utc::now().timestamp();

        self.with_tx(|tx| {
            let _ = tx.execute(
                "INSERT INTO cas_namespaces (namespace, token, read_only, max_objects, max_bytes, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(namespace)
                 DO UPDATE SET token = excluded.token, read_only = excluded.read_only, max_objects = excluded.max_objects, max_bytes = excluded.max_bytes, updated_at = excluded.updated_at",
                params![namespace, token, read_only as i64, max_objects, max_bytes, now, now],
            )?;
            get_namespace_tx(tx, namespace)
                .map(|v| v.expect("namespace must exist after upsert"))
        })
    }

    /// Get namespace policy by name.
    pub fn get_namespace(&self, namespace: &str) -> Result<Option<NamespaceConfig>> {
        validate_key(namespace, "namespace")?;
        let conn = self.conn()?;
        get_namespace_conn(&conn, namespace)
    }

    /// List configured namespaces.
    pub fn list_namespaces(&self) -> Result<Vec<NamespaceConfig>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT namespace, token, read_only, max_objects, max_bytes, created_at, updated_at FROM cas_namespaces ORDER BY namespace",
        )?;
        let out = stmt
            .query_map([], |r| {
                Ok(NamespaceConfig {
                    namespace: r.get(0)?,
                    token: r.get(1)?,
                    read_only: r.get::<_, i64>(2)? != 0,
                    max_objects: r.get(3)?,
                    max_bytes: r.get(4)?,
                    created_at: r.get(5)?,
                    updated_at: r.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(out)
    }

    /// Validate namespace bearer token.
    pub fn namespace_token_valid(&self, namespace: &str, token: &str) -> Result<bool> {
        validate_key(namespace, "namespace")?;
        let conn = self.conn()?;
        let stored: Option<String> = conn
            .query_row(
                "SELECT token FROM cas_namespaces WHERE namespace = ?",
                [namespace],
                |r| r.get(0),
            )
            .optional()?;
        Ok(matches!(stored, Some(t) if t == token))
    }

    /// Attach an existing content object to namespace ownership (enforces quotas).
    pub fn attach_content_to_namespace(&self, namespace: &str, content_id: &str) -> Result<()> {
        validate_key(namespace, "namespace")?;
        validate_content_id(content_id)?;
        let now = chrono::Utc::now().timestamp();
        self.with_tx(|tx| {
            ensure_namespace_exists_tx(tx, namespace, now)?;
            attach_namespace_object_tx(tx, namespace, content_id, now)?;
            Ok(())
        })
    }

    /// Check whether namespace can access content via attachment mapping.
    pub fn namespace_has_content(&self, namespace: &str, content_id: &str) -> Result<bool> {
        validate_key(namespace, "namespace")?;
        validate_content_id(content_id)?;
        let conn = self.conn()?;
        let exists: i64 = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM cas_namespace_objects WHERE namespace = ? AND content_id = ?)",
            params![namespace, content_id],
            |r| r.get(0),
        )?;
        Ok(exists != 0)
    }

    /// Increment reference count by one.
    pub fn inc_ref(&self, content_id: &str) -> Result<()> {
        self.apply_ref_delta(content_id, 1)
    }

    /// Decrement reference count by one (never below zero).
    pub fn dec_ref(&self, content_id: &str) -> Result<()> {
        self.apply_ref_delta(content_id, -1)
    }

    /// Mark content as pinned.
    pub fn pin(&self, content_id: &str) -> Result<()> {
        self.set_pin(content_id, true)
    }

    /// Mark content as unpinned.
    pub fn unpin(&self, content_id: &str) -> Result<()> {
        self.set_pin(content_id, false)
    }

    /// Verify payload checksum against content id.
    pub fn verify_content(&self, content_id: &str) -> Result<VerifyResult> {
        validate_content_id(content_id)?;
        let payload = match self.get_content(content_id)? {
            Some(v) => v,
            None => {
                return Ok(VerifyResult {
                    content_id: content_id.to_string(),
                    ok: false,
                    message: "content not found".to_string(),
                });
            }
        };

        let expected = content_id
            .split_once(':')
            .map(|(_, digest)| digest)
            .unwrap_or_default();
        let actual = sha256_hex(&payload);
        let ok = expected == actual;

        if ok {
            let now = chrono::Utc::now().timestamp();
            let conn = self.conn()?;
            let _ = conn.execute(
                "UPDATE cas_objects SET verified_at = ? WHERE content_id = ?",
                params![now, content_id],
            )?;
        }

        Ok(VerifyResult {
            content_id: content_id.to_string(),
            ok,
            message: if ok {
                "ok".to_string()
            } else {
                format!("digest mismatch: expected {expected}, got {actual}")
            },
        })
    }

    /// Verify every object and return results.
    pub fn verify_all(&self) -> Result<Vec<VerifyResult>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare("SELECT content_id FROM cas_objects ORDER BY created_at")?;
        let ids = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);

        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.verify_content(&id)?);
        }
        Ok(out)
    }

    /// Garbage collect unreferenced, unpinned content.
    pub fn gc_run(&self, dry_run: bool) -> Result<GcReport> {
        let now = chrono::Utc::now().timestamp();
        self.with_tx(|tx| {
            let candidates: i64 = tx.query_row(
                "SELECT COUNT(*)
                 FROM cas_objects o
                 LEFT JOIN cas_refs r ON r.content_id = o.content_id
                 WHERE COALESCE(r.ref_count, 0) <= 0 AND COALESCE(r.pinned, 0) = 0",
                [],
                |r| r.get(0),
            )?;

            let mut deleted = 0_i64;
            if !dry_run {
                deleted = tx.execute(
                    "DELETE FROM cas_objects
                     WHERE content_id IN (
                        SELECT o.content_id
                        FROM cas_objects o
                        LEFT JOIN cas_refs r ON r.content_id = o.content_id
                        WHERE COALESCE(r.ref_count, 0) <= 0 AND COALESCE(r.pinned, 0) = 0
                     )",
                    [],
                )? as i64;

                let _ = tx.execute(
                    "DELETE FROM cas_refs
                     WHERE content_id NOT IN (SELECT content_id FROM cas_objects)",
                    [],
                )?;
            }

            let _ = tx.execute(
                "INSERT INTO cas_gc_log (run_at, dry_run, candidates, deleted)
                 VALUES (?, ?, ?, ?)",
                params![now, dry_run as i64, candidates, deleted],
            )?;

            Ok(GcReport {
                dry_run,
                candidates,
                deleted,
            })
        })
    }

    /// Create a point-in-time snapshot via `VACUUM INTO`.
    pub fn snapshot_create<P: AsRef<Path>>(&self, out_path: P) -> Result<()> {
        let out_path = out_path.as_ref();
        let out_path = out_path
            .to_str()
            .ok_or_else(|| StoreError::InvalidConfig("snapshot path must be UTF-8".to_string()))?;
        let escaped = out_path.replace('"', "\"\"");

        let conn = self.conn()?;
        conn.execute_batch(&format!("VACUUM INTO \"{escaped}\";"))?;
        Ok(())
    }

    /// Export CAS state to a JSON file for replication/sync.
    pub fn sync_export<P: AsRef<Path>>(&self, out_path: P) -> Result<()> {
        let conn = self.conn()?;

        let mut obj_stmt = conn.prepare(
            "SELECT content_id, algo, digest, size_bytes, mime_type, created_at, verified_at, storage_kind, inline_blob, external_uri
             FROM cas_objects ORDER BY created_at",
        )?;
        let objects = obj_stmt
            .query_map([], |r| {
                let blob: Option<Vec<u8>> = r.get(8)?;
                Ok(SyncObject {
                    content_id: r.get(0)?,
                    algo: r.get(1)?,
                    digest: r.get(2)?,
                    size_bytes: r.get(3)?,
                    mime_type: r.get(4)?,
                    created_at: r.get(5)?,
                    verified_at: r.get(6)?,
                    storage_kind: r.get(7)?,
                    inline_blob_b64: blob
                        .map(|b| base64::engine::general_purpose::STANDARD.encode(b)),
                    external_uri: r.get(9)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut chunk_stmt = conn.prepare(
            "SELECT content_id, chunk_index, chunk_size, chunk_hash, payload
             FROM cas_object_chunks ORDER BY content_id, chunk_index",
        )?;
        let chunks = chunk_stmt
            .query_map([], |r| {
                let payload: Vec<u8> = r.get(4)?;
                Ok(SyncChunk {
                    content_id: r.get(0)?,
                    chunk_index: r.get(1)?,
                    chunk_size: r.get(2)?,
                    chunk_hash: r.get(3)?,
                    payload_b64: base64::engine::general_purpose::STANDARD.encode(payload),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut alias_stmt = conn.prepare(
            "SELECT namespace, alias, content_id, version, updated_at FROM cas_aliases ORDER BY namespace, alias",
        )?;
        let aliases = alias_stmt
            .query_map([], |r| {
                Ok(AliasRecord {
                    namespace: r.get(0)?,
                    alias: r.get(1)?,
                    content_id: r.get(2)?,
                    version: r.get(3)?,
                    updated_at: r.get(4)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut ref_stmt = conn.prepare(
            "SELECT content_id, ref_count, pinned, updated_at FROM cas_refs ORDER BY content_id",
        )?;
        let refs = ref_stmt
            .query_map([], |r| {
                Ok(SyncRef {
                    content_id: r.get(0)?,
                    ref_count: r.get(1)?,
                    pinned: r.get(2)?,
                    updated_at: r.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut ns_stmt = conn.prepare(
            "SELECT namespace, token, read_only, max_objects, max_bytes, created_at, updated_at
             FROM cas_namespaces ORDER BY namespace",
        )?;
        let namespaces = ns_stmt
            .query_map([], |r| {
                Ok(NamespaceConfig {
                    namespace: r.get(0)?,
                    token: r.get(1)?,
                    read_only: r.get::<_, i64>(2)? != 0,
                    max_objects: r.get(3)?,
                    max_bytes: r.get(4)?,
                    created_at: r.get(5)?,
                    updated_at: r.get(6)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut nso_stmt = conn.prepare(
            "SELECT namespace, content_id, attached_at FROM cas_namespace_objects ORDER BY namespace, content_id",
        )?;
        let namespace_objects = nso_stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let bundle = SyncBundle {
            objects,
            chunks,
            aliases,
            refs,
            namespaces,
            namespace_objects,
        };

        let json = serde_json::to_vec_pretty(&bundle)?;
        std::fs::write(out_path, json)?;
        Ok(())
    }

    /// Import CAS state from a JSON sync file.
    pub fn sync_import<P: AsRef<Path>>(&self, in_path: P) -> Result<()> {
        let data = std::fs::read(in_path)?;
        let bundle: SyncBundle = serde_json::from_slice(&data)?;

        self.with_tx(|tx| {
            for obj in &bundle.objects {
                let inline_blob = match &obj.inline_blob_b64 {
                    Some(s) => Some(base64::engine::general_purpose::STANDARD.decode(s).map_err(|e| {
                        StoreError::InvalidConfig(format!("invalid base64 inline blob: {e}"))
                    })?),
                    None => None,
                };
                let _ = tx.execute(
                    "INSERT OR IGNORE INTO cas_objects
                     (content_id, algo, digest, size_bytes, mime_type, created_at, verified_at, storage_kind, inline_blob, external_uri)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params![
                        obj.content_id,
                        obj.algo,
                        obj.digest,
                        obj.size_bytes,
                        obj.mime_type,
                        obj.created_at,
                        obj.verified_at,
                        obj.storage_kind,
                        inline_blob,
                        obj.external_uri
                    ],
                )?;
            }

            for chunk in &bundle.chunks {
                let payload = base64::engine::general_purpose::STANDARD
                    .decode(&chunk.payload_b64)
                    .map_err(|e| StoreError::InvalidConfig(format!("invalid base64 chunk: {e}")))?;
                let _ = tx.execute(
                    "INSERT OR IGNORE INTO cas_object_chunks (content_id, chunk_index, chunk_size, chunk_hash, payload)
                     VALUES (?, ?, ?, ?, ?)",
                    params![chunk.content_id, chunk.chunk_index, chunk.chunk_size, chunk.chunk_hash, payload],
                )?;
            }

            for alias in &bundle.aliases {
                let _ = tx.execute(
                    "INSERT OR REPLACE INTO cas_aliases (namespace, alias, content_id, version, updated_at)
                     VALUES (?, ?, ?, ?, ?)",
                    params![alias.namespace, alias.alias, alias.content_id, alias.version, alias.updated_at],
                )?;
            }

            for r in &bundle.refs {
                let _ = tx.execute(
                    "INSERT OR REPLACE INTO cas_refs (content_id, ref_count, pinned, updated_at)
                     VALUES (?, ?, ?, ?)",
                    params![r.content_id, r.ref_count, r.pinned, r.updated_at],
                )?;
            }

            for ns in &bundle.namespaces {
                let _ = tx.execute(
                    "INSERT OR REPLACE INTO cas_namespaces (namespace, token, read_only, max_objects, max_bytes, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params![
                        ns.namespace,
                        ns.token,
                        ns.read_only as i64,
                        ns.max_objects,
                        ns.max_bytes,
                        ns.created_at,
                        ns.updated_at
                    ],
                )?;
            }

            for (namespace, content_id, attached_at) in &bundle.namespace_objects {
                let _ = tx.execute(
                    "INSERT OR IGNORE INTO cas_namespace_objects (namespace, content_id, attached_at)
                     VALUES (?, ?, ?)",
                    params![namespace, content_id, attached_at],
                )?;
            }

            Ok(())
        })
    }

    /// Get a path to the database file when file-backed.
    pub fn db_path(&self) -> Option<&Path> {
        self.db_path.as_deref()
    }

    fn configure_connection(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = 10000;
             PRAGMA foreign_keys = ON;",
        )?;
        Ok(())
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.conn()?;

        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS __meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cas_objects (
                content_id TEXT PRIMARY KEY,
                algo TEXT NOT NULL,
                digest TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                mime_type TEXT,
                created_at INTEGER NOT NULL,
                verified_at INTEGER,
                storage_kind TEXT NOT NULL,
                inline_blob BLOB,
                external_uri TEXT,
                UNIQUE(algo, digest, size_bytes)
            );

            CREATE TABLE IF NOT EXISTS cas_object_chunks (
                content_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(content_id, chunk_index),
                FOREIGN KEY(content_id) REFERENCES cas_objects(content_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS cas_aliases (
                namespace TEXT NOT NULL,
                alias TEXT NOT NULL,
                content_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(namespace, alias),
                FOREIGN KEY(content_id) REFERENCES cas_objects(content_id)
            );

            CREATE TABLE IF NOT EXISTS cas_namespaces (
                namespace TEXT PRIMARY KEY,
                token TEXT,
                read_only INTEGER NOT NULL DEFAULT 0,
                max_objects INTEGER,
                max_bytes INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cas_namespace_objects (
                namespace TEXT NOT NULL,
                content_id TEXT NOT NULL,
                attached_at INTEGER NOT NULL,
                PRIMARY KEY(namespace, content_id),
                FOREIGN KEY(namespace) REFERENCES cas_namespaces(namespace) ON DELETE CASCADE,
                FOREIGN KEY(content_id) REFERENCES cas_objects(content_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS cas_refs (
                content_id TEXT PRIMARY KEY,
                ref_count INTEGER NOT NULL,
                pinned INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(content_id) REFERENCES cas_objects(content_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS cas_gc_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at INTEGER NOT NULL,
                dry_run INTEGER NOT NULL,
                candidates INTEGER NOT NULL,
                deleted INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cas_sql_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                executed_at INTEGER NOT NULL,
                actor TEXT,
                sql TEXT NOT NULL,
                params_json TEXT,
                changed_rows INTEGER
            );
            ",
        )?;

        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta (key, value) VALUES ('instance_id', ?)",
            params![&self.instance_id],
        )?;
        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta (key, value) VALUES ('schema_version', '4')",
            [],
        )?;
        let _ = conn.execute(
            "INSERT OR IGNORE INTO __meta (key, value) VALUES ('initialized_at', ?)",
            params![chrono::Utc::now().to_rfc3339()],
        )?;

        Ok(())
    }

    fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool
            .get()
            .map_err(|e| StoreError::InvalidConfig(format!("pool get failed: {e}")))
    }

    fn with_tx<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> Result<T>,
    {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let out = f(&tx)?;
        tx.commit()?;
        Ok(out)
    }

    fn apply_ref_delta(&self, content_id: &str, delta: i64) -> Result<()> {
        validate_content_id(content_id)?;
        let now = chrono::Utc::now().timestamp();
        self.with_tx(|tx| {
            update_refcount_tx(tx, content_id, delta, now)?;
            Ok(())
        })
    }

    fn set_pin(&self, content_id: &str, pinned: bool) -> Result<()> {
        validate_content_id(content_id)?;
        let now = chrono::Utc::now().timestamp();

        let conn = self.conn()?;
        let changed = conn.execute(
            "UPDATE cas_refs SET pinned = ?, updated_at = ? WHERE content_id = ?",
            params![pinned as i64, now, content_id],
        )?;

        if changed == 0 {
            if self.head_content(content_id)?.is_none() {
                return Err(StoreError::InvalidKey(format!(
                    "content_id not found: {content_id}"
                )));
            }

            let _ = conn.execute(
                "INSERT INTO cas_refs (content_id, ref_count, pinned, updated_at)
                 VALUES (?, 0, ?, ?)",
                params![content_id, pinned as i64, now],
            )?;
        }

        Ok(())
    }

    fn audit_sql(
        &self,
        conn: &Connection,
        actor: Option<&str>,
        sql: &str,
        params_json: Option<&str>,
        changed_rows: Option<i64>,
    ) -> Result<()> {
        let now = chrono::Utc::now().timestamp();
        let _ = conn.execute(
            "INSERT INTO cas_sql_audit (executed_at, actor, sql, params_json, changed_rows)
             VALUES (?, ?, ?, ?, ?)",
            params![now, actor, sql, params_json, changed_rows],
        )?;
        Ok(())
    }
}

fn update_refcount_tx(
    tx: &rusqlite::Transaction<'_>,
    content_id: &str,
    delta: i64,
    now: i64,
) -> Result<()> {
    let existing: Option<i64> = tx
        .query_row(
            "SELECT ref_count FROM cas_refs WHERE content_id = ?",
            [content_id],
            |r| r.get(0),
        )
        .optional()?;

    let current = existing.unwrap_or(0);
    let next = (current + delta).max(0);

    if existing.is_some() {
        let _ = tx.execute(
            "UPDATE cas_refs SET ref_count = ?, updated_at = ? WHERE content_id = ?",
            params![next, now, content_id],
        )?;
    } else {
        let _ = tx.execute(
            "INSERT INTO cas_refs (content_id, ref_count, pinned, updated_at)
             VALUES (?, ?, 0, ?)",
            params![content_id, next, now],
        )?;
    }

    Ok(())
}

fn get_namespace_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
) -> Result<Option<NamespaceConfig>> {
    let out = tx
        .query_row(
            "SELECT namespace, token, read_only, max_objects, max_bytes, created_at, updated_at
             FROM cas_namespaces WHERE namespace = ?",
            [namespace],
            |r| {
                Ok(NamespaceConfig {
                    namespace: r.get(0)?,
                    token: r.get(1)?,
                    read_only: r.get::<_, i64>(2)? != 0,
                    max_objects: r.get(3)?,
                    max_bytes: r.get(4)?,
                    created_at: r.get(5)?,
                    updated_at: r.get(6)?,
                })
            },
        )
        .optional()?;
    Ok(out)
}

fn get_namespace_conn(conn: &Connection, namespace: &str) -> Result<Option<NamespaceConfig>> {
    let out = conn
        .query_row(
            "SELECT namespace, token, read_only, max_objects, max_bytes, created_at, updated_at
             FROM cas_namespaces WHERE namespace = ?",
            [namespace],
            |r| {
                Ok(NamespaceConfig {
                    namespace: r.get(0)?,
                    token: r.get(1)?,
                    read_only: r.get::<_, i64>(2)? != 0,
                    max_objects: r.get(3)?,
                    max_bytes: r.get(4)?,
                    created_at: r.get(5)?,
                    updated_at: r.get(6)?,
                })
            },
        )
        .optional()?;
    Ok(out)
}

fn ensure_namespace_exists_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    now: i64,
) -> Result<()> {
    let _ = tx.execute(
        "INSERT OR IGNORE INTO cas_namespaces (namespace, token, read_only, max_objects, max_bytes, created_at, updated_at)
         VALUES (?, NULL, 0, NULL, NULL, ?, ?)",
        params![namespace, now, now],
    )?;
    Ok(())
}

fn attach_namespace_object_tx(
    tx: &rusqlite::Transaction<'_>,
    namespace: &str,
    content_id: &str,
    now: i64,
) -> Result<()> {
    let already: i64 = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM cas_namespace_objects WHERE namespace = ? AND content_id = ?)",
        params![namespace, content_id],
        |r| r.get(0),
    )?;
    if already != 0 {
        return Ok(());
    }

    let ns = get_namespace_tx(tx, namespace)?
        .ok_or_else(|| StoreError::InvalidConfig(format!("namespace not found: {namespace}")))?;
    if ns.read_only {
        return Err(StoreError::InvalidConfig(format!(
            "namespace is read-only: {namespace}"
        )));
    }

    let size: i64 = tx.query_row(
        "SELECT size_bytes FROM cas_objects WHERE content_id = ?",
        [content_id],
        |r| r.get(0),
    )?;

    let (obj_count, byte_total): (i64, i64) = tx.query_row(
        "SELECT COUNT(*), COALESCE(SUM(o.size_bytes), 0)
         FROM cas_namespace_objects n
         JOIN cas_objects o ON o.content_id = n.content_id
         WHERE n.namespace = ?",
        [namespace],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;

    if let Some(max_objects) = ns.max_objects {
        if obj_count + 1 > max_objects {
            return Err(StoreError::InvalidConfig(format!(
                "namespace object quota exceeded: namespace={namespace} current={} max={max_objects}",
                obj_count
            )));
        }
    }

    if let Some(max_bytes) = ns.max_bytes {
        if byte_total + size > max_bytes {
            return Err(StoreError::InvalidConfig(format!(
                "namespace byte quota exceeded: namespace={namespace} current_bytes={} max_bytes={max_bytes}",
                byte_total
            )));
        }
    }

    let _ = tx.execute(
        "INSERT INTO cas_namespace_objects (namespace, content_id, attached_at)
         VALUES (?, ?, ?)",
        params![namespace, content_id, now],
    )?;
    Ok(())
}

fn value_ref_to_json(v: ValueRef<'_>) -> serde_json::Value {
    match v {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(i) => serde_json::Value::from(i),
        ValueRef::Real(f) => serde_json::Value::from(f),
        ValueRef::Text(t) => serde_json::Value::String(String::from_utf8_lossy(t).to_string()),
        ValueRef::Blob(b) => serde_json::Value::String(base16_encode(b)),
    }
}

fn validate_key(value: &str, kind: &str) -> Result<()> {
    if value.is_empty() {
        return Err(StoreError::InvalidKey(format!("{kind} cannot be empty")));
    }
    if value.len() > 1024 {
        return Err(StoreError::InvalidKey(format!(
            "{kind} cannot exceed 1024 bytes"
        )));
    }
    Ok(())
}

fn validate_content_id(content_id: &str) -> Result<()> {
    let (algo, digest) = content_id
        .split_once(':')
        .ok_or_else(|| StoreError::InvalidKey("invalid content_id format".to_string()))?;

    if algo != ALGO_SHA256 {
        return Err(StoreError::InvalidKey(format!(
            "unsupported algo: {algo}; expected {ALGO_SHA256}"
        )));
    }

    if digest.len() != 64 || !digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(StoreError::InvalidKey("invalid sha256 digest".to_string()));
    }

    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    base16_encode(&digest)
}

fn base16_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_put_dedupes_identical_content() {
        let store = Store::memory().expect("store");
        let a = store
            .put_content(b"hello", Some("text/plain"))
            .expect("put a");
        let b = store
            .put_content(b"hello", Some("text/plain"))
            .expect("put b");
        assert_eq!(a, b);

        let rows = store
            .query_sql("SELECT COUNT(*) FROM cas_objects", &[])
            .expect("query");
        assert_eq!(rows.rows[0][0], serde_json::Value::from(1));
    }

    #[test]
    fn alias_updates_refcounts() {
        let store = Store::memory().expect("store");
        let c1 = store.put_content(b"v1", None).expect("c1");
        let c2 = store.put_content(b"v2", None).expect("c2");

        let first = store
            .set_alias("default", "asset", &c1, None)
            .expect("set first");
        assert_eq!(first.version, 1);

        let second = store
            .set_alias("default", "asset", &c2, Some(1))
            .expect("set second");
        assert_eq!(second.version, 2);

        let r1 = store
            .query_sql(
                "SELECT ref_count FROM cas_refs WHERE content_id = ?",
                &[&c1],
            )
            .expect("r1");
        let r2 = store
            .query_sql(
                "SELECT ref_count FROM cas_refs WHERE content_id = ?",
                &[&c2],
            )
            .expect("r2");
        assert_eq!(r1.rows[0][0], serde_json::Value::from(0));
        assert_eq!(r2.rows[0][0], serde_json::Value::from(1));
    }

    #[test]
    fn verify_and_range_read_work() {
        let store = Store::memory().expect("store");
        let cid = store.put_content(b"abcdef", None).expect("cid");

        let verify = store.verify_content(&cid).expect("verify");
        assert!(verify.ok);

        let part = store
            .read_range(&cid, 2, 3)
            .expect("range")
            .expect("exists");
        assert_eq!(part, b"cde");
    }

    #[test]
    fn gc_deletes_unreferenced_unpinned() {
        let store = Store::memory().expect("store");
        let dead = store.put_content(b"dead", None).expect("dead");
        let live = store.put_content(b"live", None).expect("live");

        store.inc_ref(&live).expect("inc live");
        store.pin(&live).expect("pin live");

        let dry = store.gc_run(true).expect("dry");
        assert_eq!(dry.candidates, 1);
        assert_eq!(dry.deleted, 0);

        let run = store.gc_run(false).expect("run");
        assert_eq!(run.deleted, 1);

        assert!(store.get_content(&dead).expect("read dead").is_none());
        assert!(store.get_content(&live).expect("read live").is_some());
    }

    #[test]
    fn sync_export_import_roundtrip() {
        let from = Store::memory().expect("from");
        let cid = from.put_content(b"syncme", None).expect("put");
        let _ = from.set_alias("ns", "a", &cid, None).expect("alias");

        let path = std::env::temp_dir().join(format!("stoar-sync-{}.json", Uuid::new_v4()));
        from.sync_export(&path).expect("export");

        let to = Store::memory().expect("to");
        to.sync_import(&path).expect("import");
        let got = to.get_content(&cid).expect("get").expect("exists");
        assert_eq!(got, b"syncme");

        let _ = std::fs::remove_file(path);
    }
}
