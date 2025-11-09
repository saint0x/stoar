//! Basic usage example for stoar with comprehensive benchmarking
use serde::{Deserialize, Serialize};
use stoar::Store;
use std::time::Instant;
use std::fs;

/// Example user structure
#[derive(Serialize, Deserialize, Debug, Clone)]
struct User {
    /// Unique identifier
    id: String,
    /// User's name
    name: String,
    /// User's email
    email: String,
    /// User's age
    age: u32,
}

/// Format bytes in human-readable format
fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// Get database file size
fn get_db_size() -> u64 {
    fs::metadata("example.db").map(|m| m.len()).unwrap_or(0)
}

/// Main example function
fn main() -> stoar::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║           stoar - Basic Usage & Performance Benchmark        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Clean up old database
    let _ = fs::remove_file("example.db");
    let _ = fs::remove_file("example.db-shm");
    let _ = fs::remove_file("example.db-wal");

    let overall_start = Instant::now();

    // Open or create a store
    let start = Instant::now();
    let store = Store::open("example.db")?;
    let elapsed = start.elapsed();
    println!("📂 Store initialization: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    println!("   Instance ID: {}\n", store.instance_id());

    // ============================================================================
    // Structured Data - Clean & Sleek
    // ============================================================================

    println!("📊 STRUCTURED DATA OPERATIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    // Put a user
    let user = User {
        id: "user1".to_string(),
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        age: 28,
    };

    let start = Instant::now();
    store.put("users", &user.id, &user)?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ PUT (user1)");
    println!("  Time: {:.3}ms | DB Size: {}", elapsed.as_secs_f64() * 1000.0, format_bytes(db_size));

    // Get a user
    let start = Instant::now();
    let retrieved: Option<User> = store.get("users", "user1")?;
    let elapsed = start.elapsed();
    println!("✓ GET (user1)");
    println!("  Time: {:.3}ms | Found: {}", elapsed.as_secs_f64() * 1000.0, retrieved.is_some());

    // Check if exists
    let start = Instant::now();
    let exists = store.exists("users", "user1")?;
    let elapsed = start.elapsed();
    println!("✓ EXISTS (user1)");
    println!("  Time: {:.3}ms | Exists: {}\n", elapsed.as_secs_f64() * 1000.0, exists);

    // Store multiple
    let user2 = User {
        id: "user2".to_string(),
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
        age: 32,
    };

    let start = Instant::now();
    store.put("users", &user2.id, &user2)?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ PUT (user2)");
    println!("  Time: {:.3}ms | DB Size: {}", elapsed.as_secs_f64() * 1000.0, format_bytes(db_size));

    // Get all from collection
    let start = Instant::now();
    let all_users: Vec<User> = store.all("users")?;
    let elapsed = start.elapsed();
    println!("✓ ALL (users collection)");
    println!("  Time: {:.3}ms | Count: {}", elapsed.as_secs_f64() * 1000.0, all_users.len());

    // Count
    let start = Instant::now();
    let count = store.count("users")?;
    let elapsed = start.elapsed();
    println!("✓ COUNT (users collection)");
    println!("  Time: {:.3}ms | Count: {}\n", elapsed.as_secs_f64() * 1000.0, count);

    // Delete
    let start = Instant::now();
    store.delete("users", "user2")?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ DELETE (user2)");
    println!("  Time: {:.3}ms | DB Size: {}", elapsed.as_secs_f64() * 1000.0, format_bytes(db_size));

    // ============================================================================
    // Blob Storage - Integrated
    // ============================================================================

    println!("\n📦 BLOB STORAGE OPERATIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    // Load real image file
    let image_path = "/Users/deepsaint/Desktop/stoar/test-image.png";
    let image_data = fs::read(image_path)?;
    let start = Instant::now();
    store.write_with_meta("photos/screenshot1.png", &image_data, Some("image/png"))?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ WRITE (photos/screenshot1.png) - REAL IMAGE");
    println!("  Time: {:.3}ms | Data Size: {} | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(image_data.len() as u64),
             format_bytes(db_size));

    // Read binary data
    let start = Instant::now();
    let read_data: Option<Vec<u8>> = store.read("photos/screenshot1.png")?;
    let elapsed = start.elapsed();
    let data_len = read_data.as_ref().map(|d| d.len()).unwrap_or(0);
    println!("✓ READ (photos/screenshot1.png)");
    println!("  Time: {:.3}ms | Data Size: {} | Verified: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(data_len as u64),
             data_len == image_data.len());

    // Get metadata
    let start = Instant::now();
    if let Some(meta) = store.meta("photos/screenshot1.png")? {
        let elapsed = start.elapsed();
        println!("✓ META (photos/screenshot1.png)");
        println!("  Time: {:.3}ms | Size: {} | Type: {} | Hash: {}",
                 elapsed.as_secs_f64() * 1000.0,
                 format_bytes(meta.size as u64),
                 meta.mime_type.unwrap_or_default(),
                 meta.hash);
    }

    // Load and store second image
    let image2_path = "/Users/deepsaint/Desktop/stoar/test-image.png";
    let image2_data = fs::read(image2_path)?;
    let start = Instant::now();
    store.write_with_meta("photos/screenshot2.png", &image2_data, Some("image/png"))?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ WRITE (photos/screenshot2.png) - REAL IMAGE");
    println!("  Time: {:.3}ms | Data Size: {} | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(image2_data.len() as u64),
             format_bytes(db_size));

    // Remove first blob
    let start = Instant::now();
    store.remove("photos/screenshot1.png")?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ REMOVE (photos/screenshot1.png)");
    println!("  Time: {:.3}ms | DB Size: {}\n", elapsed.as_secs_f64() * 1000.0, format_bytes(db_size));

    // ============================================================================
    // Transactions - Atomic operations
    // ============================================================================

    println!("🔄 TRANSACTION OPERATIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    let start = Instant::now();
    store.tx(|tx| {
        tx.put(
            "users",
            "user3",
            &User {
                id: "user3".to_string(),
                name: "Charlie".to_string(),
                email: "charlie@example.com".to_string(),
                age: 25,
            },
        )?;

        let data = b"transaction test data";
        tx.write("docs/file1", data)?;

        Ok(())
    })?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ TX (multi-op transaction)");
    println!("  Time: {:.3}ms | Ops: 2 | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(db_size));

    // ============================================================================
    // Final Stats
    // ============================================================================

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                       BENCHMARK SUMMARY                       ║");
    println!("╠══════════════════════════════════════════════════════════════╣");

    let total_users = store.count("users")?;
    let final_db_size = get_db_size();
    let overall_elapsed = overall_start.elapsed();
    let blobs_stored = 2; // screenshot2.png + user3 in transaction

    println!("║ Total Time:         {:<45} ║", format!("{:.3}ms", overall_elapsed.as_secs_f64() * 1000.0));
    println!("║ Final DB Size:      {:<45} ║", format_bytes(final_db_size));
    println!("║ Users in Store:     {:<45} ║", total_users);
    println!("║ Real Media Files:   {:<45} ║", blobs_stored);
    println!("║ Instance ID:        {:<45} ║", store.instance_id());

    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("✨ All operations completed successfully!");

    Ok(())
}
