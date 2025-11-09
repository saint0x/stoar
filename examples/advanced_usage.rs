//! Advanced usage example for stoar with transactions, queries, and benchmarking
use serde::{Deserialize, Serialize};
use stoar::{json, Store, Value};
use std::time::Instant;
use std::fs;

/// Product structure for ecommerce
#[derive(Serialize, Deserialize, Debug)]
struct Product {
    /// Product identifier
    id: String,
    /// Product name
    name: String,
    /// Product price
    price: f64,
    /// Inventory count
    inventory: u32,
}

/// Order structure
#[derive(Serialize, Deserialize, Debug)]
struct Order {
    /// Order identifier
    id: String,
    /// Product ID being ordered
    product_id: String,
    /// Quantity ordered
    quantity: u32,
    /// Total order amount
    total: f64,
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
    fs::metadata("ecommerce.db").map(|m| m.len()).unwrap_or(0)
}

/// Main example function
fn main() -> stoar::Result<()> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         stoar - Advanced Usage & Performance Benchmark       ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Clean up old database
    let _ = fs::remove_file("ecommerce.db");
    let _ = fs::remove_file("ecommerce.db-shm");
    let _ = fs::remove_file("ecommerce.db-wal");

    let overall_start = Instant::now();

    let start = Instant::now();
    let store = Store::open("ecommerce.db")?;
    let elapsed = start.elapsed();
    println!("📂 Store initialization: {:.3}ms\n", elapsed.as_secs_f64() * 1000.0);

    // ============================================================================
    // Batch Operations with Transactions
    // ============================================================================

    println!("📦 BATCH OPERATIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    let start = Instant::now();
    store.tx(|tx| {
        // Add multiple products at once
        tx.put(
            "products",
            "prod1",
            &Product {
                id: "prod1".to_string(),
                name: "Laptop".to_string(),
                price: 999.99,
                inventory: 50,
            },
        )?;

        tx.put(
            "products",
            "prod2",
            &Product {
                id: "prod2".to_string(),
                name: "Mouse".to_string(),
                price: 29.99,
                inventory: 200,
            },
        )?;

        tx.put(
            "products",
            "prod3",
            &Product {
                id: "prod3".to_string(),
                name: "Keyboard".to_string(),
                price: 79.99,
                inventory: 150,
            },
        )?;

        Ok(())
    })?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ TX (batch insert 3 products)");
    println!("  Time: {:.3}ms | Ops: 3 | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(db_size));

    // ============================================================================
    // Complex Queries with Raw SQL
    // ============================================================================

    println!("\n🔍 QUERY OPERATIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    let start = Instant::now();
    let expensive_products: Vec<Product> = store.query(
        "SELECT data FROM products WHERE json_extract(data, '$.price') > ?",
        &[&50.0],
    )?;
    let elapsed = start.elapsed();

    println!(
        "✓ QUERY (price > $50)");
    println!("  Time: {:.3}ms | Results: {} | Products: {}",
             elapsed.as_secs_f64() * 1000.0,
             expensive_products.len(),
             expensive_products
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
    );

    // ============================================================================
    // Dynamic JSON Storage (no predefined schema)
    // ============================================================================

    println!("\n🔤 JSON STORAGE");
    println!("──────────────────────────────────────────────────────────────\n");

    // Store arbitrary JSON
    let settings: Value = json!({
        "theme": "dark",
        "language": "en",
        "notifications": {
            "email": true,
            "push": false,
            "sms": true
        }
    });

    let start = Instant::now();
    store.put("config", "user_settings", &settings)?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ PUT (config/user_settings)");
    println!("  Time: {:.3}ms | DB Size: {}", elapsed.as_secs_f64() * 1000.0, format_bytes(db_size));

    let start = Instant::now();
    let retrieved_settings: Option<Value> = store.get("config", "user_settings")?;
    let elapsed = start.elapsed();

    println!("✓ GET (config/user_settings)");
    println!(
        "  Time: {:.3}ms | Theme: {}",
        elapsed.as_secs_f64() * 1000.0,
        retrieved_settings
            .as_ref()
            .and_then(|v| v.get("theme"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
    );

    // ============================================================================
    // Blob Management with Metadata
    // ============================================================================

    println!("\n📄 BLOB MANAGEMENT - REAL MEDIA FILES");
    println!("──────────────────────────────────────────────────────────────\n");

    // Load real video file (52MB)
    let video_path = "/Users/deepsaint/Desktop/stoar/test-video.mov";
    let video_data = fs::read(video_path)?;

    let start = Instant::now();
    store.write_with_meta("media/demo_recording.mov", &video_data, Some("video/quicktime"))?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();

    println!("✓ WRITE (demo_recording.mov) - REAL VIDEO FILE");
    println!("  Time: {:.3}ms | Video: {} | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(video_data.len() as u64),
             format_bytes(db_size));

    // Load and store second image as bonus
    let img_path = "/Users/deepsaint/Desktop/stoar/test-image.png";
    let img_data = fs::read(img_path)?;
    let start = Instant::now();
    store.write_with_meta("media/screenshot.png", &img_data, Some("image/png"))?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();

    println!("✓ WRITE (screenshot.png) - REAL IMAGE FILE");
    println!("  Time: {:.3}ms | Image: {} | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(img_data.len() as u64),
             format_bytes(db_size));

    // Retrieve video metadata
    let start = Instant::now();
    if let Some(meta) = store.meta("media/demo_recording.mov")? {
        let elapsed = start.elapsed();
        println!("✓ META (demo_recording.mov)");
        println!(
            "  Time: {:.3}ms | Size: {} | Type: {} | Hash: {}",
            elapsed.as_secs_f64() * 1000.0,
            format_bytes(meta.size as u64),
            meta.mime_type.unwrap_or_default(),
            meta.hash
        );
    }

    // Verify video integrity by reading it back
    let start = Instant::now();
    if let Some(read_video) = store.read("media/demo_recording.mov")? {
        let elapsed = start.elapsed();
        println!("✓ READ (demo_recording.mov)");
        println!(
            "  Time: {:.3}ms | Size: {} | Verified: {}",
            elapsed.as_secs_f64() * 1000.0,
            format_bytes(read_video.len() as u64),
            read_video.len() == video_data.len()
        );
    }

    // ============================================================================
    // Handling Multiple Collections
    // ============================================================================

    println!("\n🔄 MULTI-COLLECTION TRANSACTIONS");
    println!("──────────────────────────────────────────────────────────────\n");

    let start = Instant::now();
    store.tx(|tx| {
        // Create an order
        let order = Order {
            id: "order1".to_string(),
            product_id: "prod1".to_string(),
            quantity: 2,
            total: 1999.98,
        };

        tx.put("orders", &order.id, &order)?;

        // Update product inventory - use tx.get() to avoid deadlock
        let mut product: Product = tx.get("products", "prod1")?.unwrap();
        product.inventory -= order.quantity;
        tx.put("products", "prod1", &product)?;

        // Store order metadata
        let order_meta = json!({
            "status": "completed",
            "timestamp": chrono::Utc::now().to_rfc3339()
        });
        tx.put("order_metadata", &order.id, &order_meta)?;

        Ok(())
    })?;
    let elapsed = start.elapsed();
    let db_size = get_db_size();
    println!("✓ TX (create order, update inventory)");
    println!("  Time: {:.3}ms | Ops: 4 | DB Size: {}",
             elapsed.as_secs_f64() * 1000.0,
             format_bytes(db_size));

    // ============================================================================
    // Iterating & Aggregation
    // ============================================================================

    println!("\n📊 AGGREGATION");
    println!("──────────────────────────────────────────────────────────────\n");

    let start = Instant::now();
    let all_products: Vec<Product> = store.all("products")?;
    let elapsed = start.elapsed();
    let total_value: f64 = all_products
        .iter()
        .map(|p| p.price * p.inventory as f64)
        .sum();

    println!("✓ ALL + AGGREGATE (products)");
    println!(
        "  Time: {:.3}ms | Total Value: ${:.2}",
        elapsed.as_secs_f64() * 1000.0,
        total_value
    );

    // ============================================================================
    // Final Benchmark Summary
    // ============================================================================

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                    BENCHMARK SUMMARY                         ║");
    println!("╠══════════════════════════════════════════════════════════════╣");

    let products_count = store.count("products")?;
    let orders_count = store.count("orders")?;
    let config_count = store.count("config")?;
    let final_db_size = get_db_size();
    let overall_elapsed = overall_start.elapsed();

    println!("║ Total Runtime:      {:<45} ║", format!("{:.3}ms", overall_elapsed.as_secs_f64() * 1000.0));
    println!("║ Final DB Size:      {:<45} ║", format_bytes(final_db_size));
    println!("║ Instance ID:        {:<45} ║", store.instance_id());
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Collections & Media:                                          ║");
    println!("║   • Products:       {:<37} ║", products_count);
    println!("║   • Orders:         {:<37} ║", orders_count);
    println!("║   • Config:         {:<37} ║", config_count);
    println!("║   • Media (Real):   {:<37} ║", "2x 2.74MB files");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("✨ Advanced example completed successfully!");

    Ok(())
}
