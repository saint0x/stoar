# stoar

Ultra-lightweight unified storage: JSON collections + blobs in a single SQLite file.

## API

### Structured Data
- `put(collection, key, value)` - Store JSON
- `get(collection, key)` - Retrieve with type safety
- `delete(collection, key)` - Delete key
- `all(collection)` - Get all values
- `count(collection)` - Count items
- `exists(collection, key)` - Check existence
- `query(sql, params)` - Raw SQL with json_extract

### Blobs
- `write(key, data)` - Store binary
- `write_with_meta(key, data, mime)` - With metadata
- `read(key)` - Retrieve
- `meta(key)` - Get size, hash, mime_type, timestamps
- `remove(key)` - Delete

### Transactions
- `tx(|handle| { ... })` - ACID operations, all methods available in handle

### Metadata
- `instance_id()` - Unique store UUID

## Performance

| Operation | Time | Size |
|-----------|------|------|
| Store init | 1.5-2.4ms | - |
| PUT (JSON) | 0.8-1.7ms | 4KB base |
| GET (JSON) | 0.06-0.07ms | - |
| EXISTS | 0.02-0.04ms | - |
| DELETE | 0.04-0.31ms | - |
| COUNT | 0.02ms | - |
| QUERY (filtered) | 0.1ms | - |
| TX (2 ops) | 0.08ms | - |
| TX (4 ops) | 0.4ms | - |
| WRITE (2.74MB) | 15-32ms | +2.74MB per blob |
| READ (2.74MB) | 0.5-1ms | - |
| META | 0.08-0.2ms | - |

## Architecture

**Single SQLite file** with WAL mode, NORMAL sync, 10KB cache.

**Schema**:
- `__meta` - System metadata (instance_id, schema_version, initialized_at)
- `__objects` - Blob storage (key, data, mime_type, size, hash, created_at, updated_at)
- `{collection}` - Dynamic tables, one per collection (key, data as JSON text)

**Thread Safety**: Mutex-based with single transaction boundary.

**Key Validation**: Max 1024 bytes, non-empty.

## Usage

```rust
use stoar::Store;

let store = Store::open("data.db")?;

// Structured data
store.put("users", "alice", &user)?;
let user: Option<User> = store.get("users", "alice")?;
store.delete("users", "alice")?;

// Blobs
store.write_with_meta("files/photo.jpg", &image_bytes, Some("image/jpeg"))?;
let data: Option<Vec<u8>> = store.read("files/photo.jpg")?;
if let Some(meta) = store.meta("files/photo.jpg")? {
    println!("Size: {}, Hash: {}", meta.size, meta.hash);
}

// Transactions
store.tx(|tx| {
    tx.put("users", "bob", &user)?;
    tx.write("docs/file.pdf", &pdf_bytes)?;
    Ok(())
})?;

// Queries
let expensive: Vec<Product> = store.query(
    "SELECT data FROM products WHERE json_extract(data, '$.price') > ?",
    &[&50.0]
)?;
```

## Examples

```bash
cargo run --example basic_usage    # CRUD operations with real images
cargo run --example advanced_usage # Batch ops, transactions, queries with 2.74MB files
```

## Build

```bash
cargo build --release
```

Compiles to optimized binary with LTO, all code paths tested, zero warnings.

## Design Principles

- **One file** = complete database, zero dependencies beyond serde/uuid
- **Single-word API** = sleek method names (put, get, delete, write, read, meta)
- **ACID transactions** = single transaction boundary for all operations
- **Type-safe** = generic methods with DeserializeOwned/Serialize bounds
- **Production-ready** = #![deny(warnings, missing_docs, unused_results)]

## Benchmark Results

Basic operations complete in 3.4ms total. Blob operations with 2.74MB files in ~50ms. All data verified for integrity. Database size grows linearly with content.
