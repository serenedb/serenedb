---
title: "Apps & Clients"
sidebar_position: 7
split: page
---

# Apps & Clients

Use SereneDB applications, or connect with any PostgreSQL-compatible client or driver through the PostgreSQL wire protocol. Framework integrations build higher-level features — such as a LangChain vector store — on the same protocol.

PostgreSQL connections require a user name. If the startup packet omits the database name or sends an empty one, SereneDB connects to the database named after that user. An explicit database name takes precedence.

A connection keeps one paged result open at a time. A result fetched a page at a time (a portal executed with a row limit, as JDBC's `setFetchSize`, psycopg's server-side cursors and asyncpg's cursors do) is closed by any other statement run on the same connection, and its next fetch fails with SQLSTATE `22023`. Read such a result to its end before running another statement, or run the other statements on another connection.

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

## Frameworks & Integrations

| Framework | Package | Purpose | Guide |
|---|---|---|---|
| LangChain (Python) | `langchain-serenedb` | Vector store: embeddings, metadata filtering, hybrid BM25 + vector retrieval | [Guide](./langchain-serenedb/index.md) |

## Elasticsearch clients

Elasticsearch clients and tools connect over HTTP to the subset of the Elasticsearch REST API that SereneDB serves.

| Client | Version | Support | Guide |
|---|---|---|---|
| `elasticsearch` (Python) | 8.19, 9.5 | Partial | [Guide](./elasticsearch-api.md) |

## Quack clients

Another SereneDB server, `serenedb shell` and DuckDB attach a SereneDB server over HTTP with Quack, DuckDB's client/server protocol.

| Client | Version | Support | Guide |
|---|---|---|---|
| SereneDB, `serenedb shell` | bundled | Partial | [Guide](./quack.md) |
| DuckDB with the `quack` extension | 2.0 | Partial | [Guide](./quack.md#duckdb-clients) |

## Tools

SereneDB also ships its own command-line clients inside the `serenedb` binary: [`serenedb psql`](./serened-psql.md), a PostgreSQL-compatible client for a running server, and [`serenedb shell`](./serened-shell.md), a local shell for querying database files directly.

| Tool | Version | Support | Guide |
|---|---|---|---|
| psql | any | Full | [Guide](./psql.md) |
| serenedb psql | bundled | Full | [Guide](./serened-psql.md) |
| serenedb shell | bundled | Full | [Guide](./serened-shell.md) |
| DBeaver | 24.2.2 | Partial | [Guide](./dbeaver.md) |
| DataGrip | 2024.2.2 | Partial | [Guide](./datagrip.md) |
| Grafana | 10.4.2 | Partial | [Guide](./grafana.md) |
