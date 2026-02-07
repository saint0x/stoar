# stoar

Unified storage: JSON collections + binary blobs in a single SQLite file. Available as a Rust library or standalone CLI binary (2.4MB, zero runtime dependencies).

## Install

### CLI binary

```bash
cargo build --release --features cli
cp target/aarch64-apple-darwin/release/stoar /usr/local/bin/stoar
```

### Rust dependency

```toml
[dependencies]
stoar = { path = "/path/to/stoar" }
```

## CLI

```bash
stoar --db app.db put users alice '{"name":"Alice","age":30}'
stoar --db app.db get users alice
stoar --db app.db delete users alice
stoar --db app.db list users
stoar --db app.db count users
stoar --db app.db exists users alice
stoar --db app.db query "SELECT data FROM [users] WHERE json_extract(data, '$.age') > ?" 25
stoar --db app.db write photos/logo.png ./logo.png
stoar --db app.db write photos/logo.png ./logo.png --mime image/png
stoar --db app.db read photos/logo.png -o ./out.png
stoar --db app.db meta photos/logo.png
stoar --db app.db remove photos/logo.png
stoar --db app.db collections
stoar --db app.db blobs
stoar --db app.db info
stoar --db app.db open
```

Data goes to stdout (JSON), status messages to stderr. Non-zero exit on "not found" for scripting. Default database path is `stoar.db`.

## Library API

### Structured Data

```rust
use stoar::{Store, json};

let store = Store::open("data.db")?;

store.put("users", "alice", &json!({"name": "Alice", "age": 30}))?;
let user: Option<serde_json::Value> = store.get("users", "alice")?;
store.delete("users", "alice")?;

let all_users: Vec<User> = store.all("users")?;
let count = store.count("users")?;
let exists = store.exists("users", "alice")?;

let expensive: Vec<Product> = store.query(
    "SELECT data FROM [products] WHERE json_extract(data, '$.price') > ?",
    &[&50.0],
)?;
```

### Blobs

```rust
store.write("files/photo.jpg", &bytes)?;
store.write_with_meta("files/photo.jpg", &bytes, Some("image/jpeg"))?;

let data: Option<Vec<u8>> = store.read("files/photo.jpg")?;

if let Some(meta) = store.meta("files/photo.jpg")? {
    // meta.size, meta.mime_type, meta.hash, meta.created_at, meta.updated_at
}

store.remove("files/photo.jpg")?;
```

### Transactions

```rust
store.tx(|tx| {
    tx.put("users", "bob", &user)?;
    tx.write("docs/file.pdf", &pdf_bytes)?;
    let count = tx.count("users")?;
    Ok(count)
})?;
// Commits on Ok, rolls back on Err
```

### Metadata

```rust
store.instance_id()    // Unique UUID per store instance
store.collections()?   // List all user-created collections
store.blobs()?         // List all blobs with metadata
store.info()?          // Database metadata (instance_id, schema_version, initialized_at)
```

## Architecture

Single SQLite file. WAL journal mode, NORMAL synchronous, 10K page cache, foreign keys on.

**Schema:**
- `__meta` — instance_id, schema_version, initialized_at
- `__objects` — key, data (BLOB), mime_type, size, hash, created_at, updated_at
- `[collection_name]` — key, data (JSON as TEXT). Created on first `put()`.

**Thread safety:** `Mutex<Connection>`. Single writer, serialized access.

**Key constraints:** Non-empty, max 1024 bytes. Validated on every operation.

**Schema initialization:** Eager on `open()` / `memory()`. Reads on a fresh database return `None`, not errors.

## Build

```bash
cargo build --release                  # Library only
cargo build --release --features cli   # Library + CLI binary
cargo test                             # 34 tests
```

Release binary: 2.4MB, LTO, single codegen unit, stripped symbols. SQLite statically linked via `bundled` feature.

## Performance

| Operation | Time |
|-----------|------|
| Store init | 1.5-2.4ms |
| PUT (JSON) | 0.8-1.7ms |
| GET (JSON) | 0.06-0.07ms |
| EXISTS | 0.02-0.04ms |
| DELETE | 0.04-0.31ms |
| COUNT | 0.02ms |
| QUERY (filtered) | 0.1ms |
| TX (2 ops) | 0.08ms |
| TX (4 ops) | 0.4ms |
| WRITE (2.74MB blob) | 15-32ms |
| READ (2.74MB blob) | 0.5-1ms |
| META | 0.08-0.2ms |

## Tests

```bash
cargo test
```

34 tests covering: schema initialization, structured data CRUD, blob operations, transactions (commit + rollback), key validation, collection isolation, and edge cases. All in-memory, no filesystem side effects.
