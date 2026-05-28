# stoar

`stoar` is an embedded SQLite-backed data engine that combines:

- relational SQL in the same `.db` file
- schemaless JSON document collections
- a deduplicated object store built on content-addressable storage

It ships as a Rust library and an optional CLI/server binary.

## Product Model

`stoar` has three layers that share one SQLite file:

1. Documents
   User collections are JSON tables with `key`, `data`, `created_at`, and `updated_at`.
2. Objects
   High-level objects live behind `namespace + key` aliases and point to immutable CAS content.
3. SQL
   You can create and query arbitrary relational tables directly through SQLite.

The low-level CAS layer adds deduplication, range reads, verification, pinning, refcounts, snapshotting, and sync export/import.

## Install

### Library

```toml
[dependencies]
stoar = { path = "/path/to/stoar" }
```

### CLI

```bash
cargo build --release --features cli
cp target/release/stoar /usr/local/bin/stoar
```

## Rust API

### Documents

```rust
use stoar::{json, Store};

let store = Store::open("app.db")?;

store.put_value("users", "alice", &json!({
    "name": "Alice",
    "role": "admin"
}))?;

let user: Option<serde_json::Value> = store.get_value("users", "alice")?;
let exists = store.exists("users", "alice")?;
let keys = store.list("users")?;
let rows = store.all_values("users")?;
let count = store.count("users")?;
```

### Objects

```rust
let object = store.put_object(
    "assets",
    "logo.svg",
    br#"<svg viewBox="0 0 10 10"></svg>"#,
    Some("image/svg+xml"),
    None,
)?;

let bytes = store.get_object("assets", "logo.svg")?;
let head = store.head_object("assets", "logo.svg")?;
let all_objects = store.list_objects(Some("assets"), Some("logo"))?;
let deleted = store.delete_object("assets", "logo.svg")?;
```

### SQL

```rust
store.execute_sql(
    "CREATE TABLE IF NOT EXISTS invoices (id TEXT PRIMARY KEY, total_cents INTEGER NOT NULL)",
    &[],
)?;

store.execute_sql(
    "INSERT INTO invoices (id, total_cents) VALUES (?, ?)",
    &[&"inv_1", &2500_i64],
)?;

let rows = store.query_sql("SELECT id, total_cents FROM invoices ORDER BY id", &[])?;
```

### Typed JSON Queries

```rust
#[derive(serde::Deserialize)]
struct Product {
    name: String,
    price: f64,
}

let expensive: Vec<Product> = store.query(
    "SELECT data FROM products WHERE json_extract(data, '$.price') > ?",
    &[&100.0],
)?;
```

### CAS / Replication / Maintenance

```rust
let cid = store.put_content(b"hello", Some("text/plain"))?;
let verify = store.verify_content(&cid)?;
let report = store.gc_run(true)?;
store.snapshot_create("snapshot.db")?;
store.sync_export("sync.json")?;
```

## CLI

### Documents

```bash
stoar --db app.db info
stoar --db app.db collections
stoar --db app.db doc put users alice '{"name":"Alice","role":"admin"}'
stoar --db app.db doc get users alice
stoar --db app.db doc list users
stoar --db app.db doc all users
stoar --db app.db doc count users
stoar --db app.db doc exists users alice
stoar --db app.db doc delete users alice
```

### Objects

```bash
stoar --db app.db object put assets logo.svg ./logo.svg --mime image/svg+xml
stoar --db app.db object get assets logo.svg -o ./logo.out.svg
stoar --db app.db object head assets logo.svg
stoar --db app.db object list --namespace assets --prefix logo
stoar --db app.db object delete assets logo.svg
```

### SQL and CAS

```bash
stoar --db app.db sql "SELECT name FROM sqlite_master WHERE type = 'table'"
stoar --db app.db cas put ./movie.mp4 --mime video/mp4
stoar --db app.db verify --all
stoar --db app.db gc run --dry-run
stoar --db app.db snapshot create ./snapshot.db
stoar --db app.db sync export ./sync.json
```

### Embedded HTTP Server

```bash
stoar --db app.db serve --listen 127.0.0.1:7777 --token secret
```

Admin routes include:

- `GET /healthz`
- `GET /docs`
- `GET /docs/:collection`
- `GET|POST|DELETE /docs/:collection/:key`
- `GET /objects`
- `GET|POST|DELETE /objects/:namespace/*key`
- `GET /objects/:namespace/*key/head`
- `POST /sql`
- `POST /gc`
- `POST /verify`

Low-level CAS and namespace-scoped routes remain available for advanced use.

## Schema

Internal tables:

- `__meta`
- `cas_objects`
- `cas_object_chunks`
- `cas_aliases`
- `cas_namespaces`
- `cas_namespace_objects`
- `cas_refs`
- `cas_gc_log`
- `cas_sql_audit`

User document collections are regular SQLite tables created on first write.

## Operational Notes

- SQLite is bundled through `rusqlite` for portability.
- Connections are pooled with `r2d2`.
- Every acquired connection is configured with WAL, `synchronous=NORMAL`, cache sizing, and foreign keys.
- Store identity is persisted in `__meta` and exposed via `store.info()`.
- Object deletion removes aliases; unreferenced content is reclaimed by `gc run`.

## Development

```bash
cargo test
cargo check --features cli
cargo check --examples
cargo check --manifest-path server/Cargo.toml
```

For deterministic scenario testing, use Fozzy from the repo root after validating the scenarios in `tests/`.
