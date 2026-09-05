---
title: "Apps & Clients"
sidebar_position: 7
---

# Apps & Clients

Use SereneDB applications, or connect with any PostgreSQL-compatible client or driver through the PostgreSQL wire protocol.

## Apps

| App | Purpose | Guide |
|---|---|---|
| Serene Docs Search | Self-hosted full-text, hybrid, and AI-assisted search for documentation | [Guide](./serene-docs-search.mdx) |

## Programming Languages

| Language | Driver | Version | Support | Guide |
|---|---|---|---|---|
| Python | psycopg2 | 2.9.10 | Full | [Guide](./python.md) |
| Python | psycopg3 | 3.2.3 | Full | [Guide](./python.md) |
| Java | JDBC | 42.7.4 | Partial | [Guide](./java.md) |
| JavaScript | node-postgres | 8.13.0 | Full | [Guide](./javascript.md) |
| C++ | libpqxx | 7.9.1 | Full | [Guide](./cpp.md) |
| C# | Npgsql | 8.0.4 | Full | [Guide](./csharp.md) |
| Rust | tokio-postgres | 0.7.12 | Full | [Guide](./rust.md) |
| R | RPostgres | 1.4.7 | Full | [Guide](./r.md) |

## Tools

SereneDB also ships its own command-line clients inside the `serened` binary: [`serened psql`](./serened-psql.md), a PostgreSQL-compatible client for a running server, and [`serened shell`](./serened-shell.md), a local shell for querying database files directly.

| Tool | Version | Support | Guide |
|---|---|---|---|
| psql | any | Full | [Guide](./psql.md) |
| serened psql | bundled | Full | [Guide](./serened-psql.md) |
| serened shell | bundled | Full | [Guide](./serened-shell.md) |
| DBeaver | 24.2.2 | Partial | [Guide](./dbeaver.md) |
| DataGrip | 2024.2.2 | Partial | [Guide](./datagrip.md) |
| Grafana | 10.4.2 | Partial | [Guide](./grafana.md) |
