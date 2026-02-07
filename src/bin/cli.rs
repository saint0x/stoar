//! stoar CLI - Command-line interface for stoar databases

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use stoar::{Store, Value};

/// stoar - Ultra-lightweight unified storage for structured data + blobs
#[derive(Parser)]
#[command(name = "stoar", version, about)]
struct Cli {
    /// Path to the database file
    #[arg(long, short, global = true, default_value = "stoar.db")]
    db: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Open or create a database and print instance info
    Open,

    /// Store a JSON value in a collection
    Put {
        /// Collection name
        collection: String,
        /// Key
        key: String,
        /// JSON value (as string)
        json: String,
    },

    /// Retrieve a value from a collection
    Get {
        /// Collection name
        collection: String,
        /// Key
        key: String,
    },

    /// Delete a value from a collection
    Delete {
        /// Collection name
        collection: String,
        /// Key
        key: String,
    },

    /// List all items in a collection
    List {
        /// Collection name
        collection: String,
    },

    /// Count items in a collection
    Count {
        /// Collection name
        collection: String,
    },

    /// Check if a key exists in a collection
    Exists {
        /// Collection name
        collection: String,
        /// Key
        key: String,
    },

    /// Execute a raw SQL query
    Query {
        /// SQL statement
        sql: String,
        /// Query parameters (strings, integers, or floats)
        params: Vec<String>,
    },

    /// Store a file as a blob
    Write {
        /// Blob key
        key: String,
        /// Path to the file to store
        file: PathBuf,
        /// MIME type (auto-detected from extension if omitted)
        #[arg(long)]
        mime: Option<String>,
    },

    /// Read a blob and output to file or stdout
    Read {
        /// Blob key
        key: String,
        /// Output file path (defaults to stdout)
        #[arg(long, short)]
        output: Option<PathBuf>,
    },

    /// Show metadata for a blob
    Meta {
        /// Blob key
        key: String,
    },

    /// Remove a blob
    Remove {
        /// Blob key
        key: String,
    },

    /// List all collections
    Collections,

    /// List all blobs with metadata
    Blobs,

    /// Show database info and statistics
    Info,
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> stoar::Result<()> {
    let store = Store::open(&cli.db)?;

    match cli.command {
        Commands::Open => {
            let info = store.info()?;
            let meta: std::collections::HashMap<_, _> = info.into_iter().collect();
            let obj = serde_json::json!({
                "instance_id": store.instance_id(),
                "path": cli.db.display().to_string(),
                "meta": meta,
            });
            println!("{}", serde_json::to_string_pretty(&obj)?);
        }

        Commands::Put { collection, key, json } => {
            let value: Value = serde_json::from_str(&json)?;
            store.put(&collection, &key, &value)?;
        }

        Commands::Get { collection, key } => {
            let value: Option<Value> = store.get(&collection, &key)?;
            match value {
                Some(v) => println!("{}", serde_json::to_string_pretty(&v)?),
                None => {
                    eprintln!("not found: {}/{}", collection, key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Delete { collection, key } => {
            store.delete(&collection, &key)?;
        }

        Commands::List { collection } => {
            let items: Vec<Value> = store.all(&collection)?;
            println!("{}", serde_json::to_string_pretty(&items)?);
        }

        Commands::Count { collection } => {
            let count = store.count(&collection)?;
            println!("{}", count);
        }

        Commands::Exists { collection, key } => {
            let exists = store.exists(&collection, &key)?;
            println!("{}", exists);
            if !exists {
                std::process::exit(1);
            }
        }

        Commands::Query { sql, params } => {
            let parsed: Vec<Box<dyn rusqlite::ToSql>> = params
                .iter()
                .map(|p| -> Box<dyn rusqlite::ToSql> {
                    if let Ok(i) = p.parse::<i64>() {
                        return Box::new(i);
                    }
                    if let Ok(f) = p.parse::<f64>() {
                        return Box::new(f);
                    }
                    Box::new(p.clone())
                })
                .collect();
            let refs: Vec<&dyn rusqlite::ToSql> = parsed.iter().map(|b| b.as_ref()).collect();
            let results: Vec<Value> = store.query(&sql, &refs)?;
            println!("{}", serde_json::to_string_pretty(&results)?);
        }

        Commands::Write { key, file, mime } => {
            let data = std::fs::read(&file)?;
            let mime_ref = mime.as_deref().or_else(|| mime_from_extension(&file));
            store.write_with_meta(&key, &data, mime_ref)?;
            let meta = store.meta(&key)?;
            if let Some(m) = meta {
                eprintln!("wrote {} ({} bytes, hash: {})", key, m.size, m.hash);
            }
        }

        Commands::Read { key, output } => {
            match store.read(&key)? {
                Some(data) => {
                    if let Some(path) = output {
                        std::fs::write(&path, &data)?;
                        eprintln!("wrote {} bytes to {}", data.len(), path.display());
                    } else {
                        use std::io::Write;
                        std::io::stdout().write_all(&data)?;
                    }
                }
                None => {
                    eprintln!("not found: {}", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Meta { key } => {
            match store.meta(&key)? {
                Some(meta) => println!("{}", serde_json::to_string_pretty(&meta)?),
                None => {
                    eprintln!("not found: {}", key);
                    std::process::exit(1);
                }
            }
        }

        Commands::Remove { key } => {
            store.remove(&key)?;
        }

        Commands::Collections => {
            let collections = store.collections()?;
            println!("{}", serde_json::to_string_pretty(&collections)?);
        }

        Commands::Blobs => {
            let blobs = store.blobs()?;
            let items: Vec<Value> = blobs
                .into_iter()
                .map(|(key, meta)| {
                    serde_json::json!({
                        "key": key,
                        "size": meta.size,
                        "mime_type": meta.mime_type,
                        "hash": meta.hash,
                        "created_at": meta.created_at,
                        "updated_at": meta.updated_at,
                    })
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&items)?);
        }

        Commands::Info => {
            let info = store.info()?;
            let collections = store.collections()?;
            let blobs = store.blobs()?;
            let meta: std::collections::HashMap<_, _> = info.into_iter().collect();
            let obj = serde_json::json!({
                "instance_id": store.instance_id(),
                "path": cli.db.display().to_string(),
                "meta": meta,
                "collections": collections,
                "collection_count": collections.len(),
                "blob_count": blobs.len(),
            });
            println!("{}", serde_json::to_string_pretty(&obj)?);
        }
    }

    Ok(())
}

/// Infer MIME type from file extension
fn mime_from_extension(path: &std::path::Path) -> Option<&'static str> {
    match path.extension()?.to_str()? {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "svg" => Some("image/svg+xml"),
        "pdf" => Some("application/pdf"),
        "json" => Some("application/json"),
        "csv" => Some("text/csv"),
        "txt" => Some("text/plain"),
        "md" => Some("text/markdown"),
        "html" | "htm" => Some("text/html"),
        "css" => Some("text/css"),
        "js" => Some("application/javascript"),
        "mp4" => Some("video/mp4"),
        "mov" => Some("video/quicktime"),
        "mp3" => Some("audio/mpeg"),
        "wav" => Some("audio/wav"),
        "zip" => Some("application/zip"),
        "tar" => Some("application/x-tar"),
        "gz" => Some("application/gzip"),
        "wasm" => Some("application/wasm"),
        _ => Some("application/octet-stream"),
    }
}
