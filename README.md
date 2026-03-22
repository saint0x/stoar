# stoar

SQLite-backed content-addressable storage with aliasing, namespace policy, and an optional CLI/HTTP server.

`stoar` is a small Rust crate that stores immutable payloads by content hash and layers mutable references and tenancy controls on top. It can be embedded as a library or built with the `cli` feature to get a standalone `stoar` binary.

## What It Does

- Stores immutable objects by `sha256:<digest>`
- Deduplicates identical content automatically
- Stores small payloads inline in SQLite and large payloads in chunk rows
- Exposes mutable aliases: `namespace + alias -> content_id`
- Tracks refcounts and pin state for retention / garbage collection
- Supports namespace policies: bearer token, read-only mode, object quota, byte quota
- Verifies stored content against its digest
- Exports/imports full CAS state for replication or migration
- Can run as an HTTP service with admin and namespace-scoped auth

## Install

### Library

```toml
[dependencies]
stoar = { path = "/path/to/stoar" }
```

### CLI / Server Binary

```bash
cargo build --release --features cli
cp target/release/stoar /usr/local/bin/stoar
```

## Library API

### Open a Store

```rust
use stoar::Store;

let store = Store::open("stoar.db")?;
```

### Put and Get Content

```rust
let content_id = store.put_content(b"hello world", Some("text/plain"))?;

let data = store.get_content(&content_id)?.expect("content exists");
assert_eq!(data, b"hello world");
```

### Stream from a File

```rust
let content_id = store.put_content_file("./logo.png", Some("image/png"))?;
```

### Read Metadata

```rust
let meta = store.head_content(&content_id)?.expect("metadata exists");
println!("{}", meta.size_bytes);
println!("{}", meta.storage_kind);
```

### Aliases

```rust
let record = store.set_alias("images", "logo", &content_id, None)?;

let resolved = store
    .resolve_alias("images", "logo")?
    .expect("alias exists");

assert_eq!(record.content_id, resolved.content_id);
```

### Namespace Policy

```rust
store.upsert_namespace(
    "images",
    Some("secret-token"),
    false,
    Some(10_000),
    Some(100 * 1024 * 1024),
)?;

store.attach_content_to_namespace("images", &content_id)?;
assert!(store.namespace_has_content("images", &content_id)?);
```

### Retention and GC

```rust
store.inc_ref(&content_id)?;
store.pin(&content_id)?;

let dry_run = store.gc_run(true)?;
println!("candidates: {}", dry_run.candidates);
```

### Verification

```rust
let result = store.verify_content(&content_id)?;
assert!(result.ok);
```

### Snapshot and Sync

```rust
store.snapshot_create("backup.db")?;
store.sync_export("replica-sync.json")?;
store.sync_import("replica-sync.json")?;
```

### Raw SQL

```rust
let rows = store.query_sql(
    "SELECT namespace, alias, content_id FROM cas_aliases WHERE namespace = ?",
    &[&"images"],
)?;
```

## CLI

The binary is feature-gated behind `--features cli`.

### CAS

```bash
stoar --db stoar.db cas put ./logo.png --mime image/png
stoar --db stoar.db cas get sha256:... -o ./logo.png
stoar --db stoar.db cas head sha256:...
```

### Aliases

```bash
stoar --db stoar.db alias set images logo sha256:...
stoar --db stoar.db alias get images logo
stoar --db stoar.db alias delete images logo
```

### Refs, Pinning, GC, Verify

```bash
stoar --db stoar.db ref inc sha256:...
stoar --db stoar.db ref pin sha256:...
stoar --db stoar.db gc run --dry-run
stoar --db stoar.db verify sha256:...
stoar --db stoar.db verify --all
```

### Namespace Management

```bash
stoar --db stoar.db namespace set images --token secret --max-objects 1000 --max-bytes 104857600
stoar --db stoar.db namespace get images
stoar --db stoar.db namespace list
```

### SQL / Snapshot / Sync

```bash
stoar --db stoar.db sql "SELECT * FROM cas_aliases" --mode json
stoar --db stoar.db snapshot create ./snapshot.db
stoar --db stoar.db sync export ./sync.json
stoar --db stoar.db sync import ./sync.json
```

## HTTP Server

Build with `--features cli` and run:

```bash
stoar --db stoar.db serve --listen 127.0.0.1:7777 --token admin-token
```

HTTP surface:

- Admin routes:
  - `POST /cas`
  - `GET /cas/:content_id`
  - `GET /cas/:content_id/head`
  - `POST|GET|DELETE /alias/:namespace/:alias`
  - `POST /ref/:action/:content_id`
  - `POST /gc`
  - `POST /verify`
  - `POST /sql`
  - `GET /admin/namespaces`
  - `POST /admin/namespace/:namespace`
- Namespace-scoped routes:
  - `POST /ns/:namespace/cas`
  - `GET /ns/:namespace/cas/:content_id`
  - `GET /ns/:namespace/cas/:content_id/head`
  - `POST|GET /ns/:namespace/alias/:alias`
  - `POST /ns/:namespace/ref/:action/:content_id`
  - `POST /ns/:namespace/verify`
- Utility:
  - `GET /healthz`
  - `GET /readyz`
  - `GET /metrics`

Auth model:

- Admin routes require `Authorization: Bearer <admin-token>`
- Namespace routes accept either the admin token or that namespace's configured token
- Namespace write operations are blocked when the namespace is read-only

## Storage Model

The schema is initialized eagerly on `open()` / `memory()` and currently includes:

- `__meta`
- `cas_objects`
- `cas_object_chunks`
- `cas_aliases`
- `cas_namespaces`
- `cas_namespace_objects`
- `cas_refs`
- `cas_gc_log`
- `cas_sql_audit`

Storage strategy:

- Payloads up to 1 MiB are stored inline in `cas_objects.inline_blob`
- Larger payloads are split into 1 MiB chunks in `cas_object_chunks`
- `content_id` is canonical and content-addressed
- Alias rows provide mutable names without changing immutable content rows

SQLite connection settings:

- WAL journal mode
- `synchronous = NORMAL`
- foreign keys enabled
- pooled connections via `r2d2_sqlite`

## Build

```bash
cargo build --release
cargo build --release --features cli
```

## Test

```bash
cargo test
```

The current repository test suite is small and focused on core CAS behavior:

- deduplication
- alias refcount updates
- verification and range reads
- garbage collection
- sync export/import roundtrip
