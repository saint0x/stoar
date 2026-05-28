//! Basic document, object, and SQL usage for stoar.
use serde::{Deserialize, Serialize};
use stoar::Store;

#[derive(Debug, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

fn main() -> stoar::Result<()> {
    let store = Store::open("basic-example.db")?;

    store.put(
        "users",
        "alice",
        &User {
            name: "Alice".to_string(),
            age: 30,
        },
    )?;

    let alice: Option<User> = store.get("users", "alice")?;
    println!("alice = {alice:?}");
    println!("collections = {:?}", store.collections()?);

    let object = store.put_object(
        "assets",
        "hello.txt",
        b"hello from stoar",
        Some("text/plain"),
        None,
    )?;
    println!("object = {}", serde_json::to_string_pretty(&object)?);

    let bytes = store.get_object("assets", "hello.txt")?.unwrap_or_default();
    println!("object bytes = {}", String::from_utf8_lossy(&bytes));

    let rows = store.query_sql("SELECT COUNT(*) AS users FROM users", &[])?;
    println!("sql rows = {}", serde_json::to_string_pretty(&rows)?);
    println!("info = {}", serde_json::to_string_pretty(&store.info()?)?);

    Ok(())
}
