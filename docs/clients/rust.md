---
title: Rust
sidebar_position: 6
---

# Rust

SereneDB works with [tokio-postgres](https://docs.rs/tokio-postgres/), an async PostgreSQL client for Rust.

## Dependencies

Add to your `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-postgres = "0.7.15"
```

## Connect

```rust
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=7890", NoTls
    ).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
```

## Create a table and insert data

```rust
    client.execute(
        "CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            views INTEGER
        )", &[]
    ).await?;

    client.execute(
        "INSERT INTO articles VALUES ($1, $2, $3)",
        &[&1i32, &"Introduction to Vector Search", &4200i32],
    ).await?;
```

## Query

```rust
    let rows = client.query(
        "SELECT title, views FROM articles ORDER BY views DESC", &[]
    ).await?;

    for row in &rows {
        let title: &str = row.get(0);
        let views: i32 = row.get(1);
        println!("{title} — {views} views");
    }

    Ok(())
}
```
