//! Debug test
use stoar::Store;

fn main() -> stoar::Result<()> {
    println!("Creating in-memory store...");
    let _store = Store::memory()?;
    println!("In-memory store created successfully");
    Ok(())
}
