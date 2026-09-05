---
title: "C#"
sidebar_position: 5
---

# C#

SereneDB works with [Npgsql](https://www.npgsql.org/), the open-source .NET data provider for PostgreSQL.

## Install

```sh
dotnet add package Npgsql
```

## Connect

```csharp
using Npgsql;

var connString = "Host=localhost;Port=7890";
var dataSource = NpgsqlDataSource.Create(connString);
```

## Create a table and insert data

```csharp
await using var createCmd = dataSource.CreateCommand("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY,
        title TEXT,
        views INTEGER
    )
""");
await createCmd.ExecuteNonQueryAsync();

await using var insertCmd = dataSource.CreateCommand(
    "INSERT INTO articles VALUES ($1, $2, $3)");
insertCmd.Parameters.AddWithValue(1);
insertCmd.Parameters.AddWithValue("Introduction to Vector Search");
insertCmd.Parameters.AddWithValue(4200);
await insertCmd.ExecuteNonQueryAsync();
```

## Query

```csharp
await using var readCmd = dataSource.CreateCommand(
    "SELECT title, views FROM articles ORDER BY views DESC");
await using var reader = await readCmd.ExecuteReaderAsync();
while (await reader.ReadAsync()) {
    Console.WriteLine($"{reader.GetString(0)} — {reader.GetInt32(1)} views");
}
```
