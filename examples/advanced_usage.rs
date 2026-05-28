//! Advanced stoar features: typed queries, namespaces, snapshots, and sync.
use serde::{Deserialize, Serialize};
use stoar::Store;

#[derive(Debug, Serialize, Deserialize)]
struct Product {
    name: String,
    price: f64,
}

fn main() -> stoar::Result<()> {
    let store = Store::open("advanced-example.db")?;

    store.put(
        "products",
        "laptop",
        &Product {
            name: "Laptop".to_string(),
            price: 1299.0,
        },
    )?;
    store.put(
        "products",
        "mouse",
        &Product {
            name: "Mouse".to_string(),
            price: 39.0,
        },
    )?;

    let expensive: Vec<Product> = store.query(
        "SELECT data FROM products WHERE json_extract(data, '$.price') > ? ORDER BY key",
        &[&100.0],
    )?;
    println!("expensive = {expensive:#?}");

    let _ = store.upsert_namespace(
        "assets",
        Some("secret-token"),
        false,
        Some(10),
        Some(1_000_000),
    )?;
    let logo = store.put_object(
        "assets",
        "logo.svg",
        br#"<svg viewBox="0 0 10 10"></svg>"#,
        Some("image/svg+xml"),
        None,
    )?;
    println!("logo = {}", serde_json::to_string_pretty(&logo)?);

    let snapshot = std::env::temp_dir().join("stoar-advanced-snapshot.db");
    store.snapshot_create(&snapshot)?;

    let sync_file = std::env::temp_dir().join("stoar-advanced-sync.json");
    store.sync_export(&sync_file)?;

    let imported = Store::memory()?;
    imported.sync_import(&sync_file)?;
    let imported_logo = imported.head_object("assets", "logo.svg")?;
    println!(
        "imported_logo = {}",
        serde_json::to_string_pretty(&imported_logo)?
    );

    let _ = std::fs::remove_file(snapshot);
    let _ = std::fs::remove_file(sync_file);
    Ok(())
}
