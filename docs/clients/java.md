---
title: Java
sidebar_position: 2
---

# Java

SereneDB works with the standard [PostgreSQL JDBC driver](https://jdbc.postgresql.org/).

## Install

Download the driver:

```sh
wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

Add it to your classpath, or include it as a Maven/Gradle dependency:

```xml
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.3</version>
</dependency>
```

## Connect

```java
import java.sql.*;

String url = "jdbc:postgresql://localhost:7890/_system";
Connection conn = DriverManager.getConnection(url);
```

:::caution Known Issue
The JDBC driver may report a `DateStyle` error on connect. This is a known compatibility issue — a fix is in progress.
:::

## Create a table and insert data

```java
try (Statement stmt = conn.createStatement()) {
    stmt.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            views INTEGER
        )
    """);
}

try (PreparedStatement ps = conn.prepareStatement(
        "INSERT INTO articles VALUES (?, ?, ?)")) {
    ps.setInt(1, 1);
    ps.setString(2, "Introduction to Vector Search");
    ps.setInt(3, 4200);
    ps.executeUpdate();
}
```

## Query

```java
try (Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery(
         "SELECT title, views FROM articles ORDER BY views DESC")) {
    while (rs.next()) {
        System.out.printf("%s — %d views%n",
            rs.getString("title"), rs.getInt("views"));
    }
}
```

## Bulk loading with COPY

Use the `CopyManager` for large imports:

```java
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

CopyManager cm = new CopyManager((BaseConnection) conn);
FileReader reader = new FileReader("movies.csv");
cm.copyIn("COPY articles FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", reader);
```

## Cleanup

```java
conn.close();
```
