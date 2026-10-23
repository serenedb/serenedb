---
title: Compatibility
sidebar_position: 9
split: page
---

# Compatibility

SereneDB uses PostgreSQL's SQL parser and speaks the PostgreSQL wire protocol (version 3.0). Most PostgreSQL clients, drivers and tools connect without any code changes. The SQL dialect covers the majority of day-to-day PostgreSQL syntax — data types, DML, DDL, transactions and functions.

That said, SereneDB is a different engine built from scratch. Some PostgreSQL features behave differently, and some are not yet implemented.

## PostgreSQL Compatibility

See [PostgreSQL Compatibility](./core-sql-compatibility.md) for a detailed breakdown of supported statements, types and functions, together with the [behavioral differences](./core-sql-compatibility.md#behavioral-differences-from-postgresql) where SereneDB intentionally diverges from PostgreSQL.

A file that cannot be opened, read or written, whether by `COPY`, a file reader such as `read_csv`, `ATTACH` or a search index, fails with the SQLSTATE PostgreSQL gives the operating system's error: `58P01` (`undefined_file`) when the file or its directory does not exist, `42501` (`insufficient_privilege`) when permission is denied and `53100` (`disk_full`) when the disk is full. Any other I/O failure, including a file pattern that matches no file, fails with `58030` (`io_error`).

## SQL Dialect

Beyond core compatibility, SereneDB's dialect adds a few deliberate behaviors and extensions:

- [SQL Extensions](./sql_extensions.md) — dialect extensions such as `GROUP BY ALL`, `CREATE OR REPLACE TABLE` and `SELECT * EXCLUDE`.
- [Keywords and Identifiers](./keywords_and_identifiers.md) — case-sensitivity rules and identifier handling.
- [Order Preservation](./order_preservation.md) — when and how insertion order is preserved.
- [SQL Quirks](./sql_quirks.md) — idiosyncrasies worth knowing about.

## System Tables

SereneDB provides the `pg_catalog` schema with PostgreSQL-compatible system tables. These are used by clients and tools for schema introspection — listing tables, columns, types and other metadata. See [System Table Compatibility](./system-table-compatibility.md) for the full matrix.

## What is not supported

### XML types

PostgreSQL's XML data type and related functions are not planned.
