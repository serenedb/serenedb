---
title: Blob
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name   | Aliases                        | Description                 |
| :----- | :----------------------------- | :-------------------------- |
| `BLOB` | `BYTEA`, `BINARY`, `VARBINARY` | Variable-length binary data |

The blob (**B**inary **L**arge **OB**ject) type represents an arbitrary binary object stored in the database system. The blob type can contain any type of binary data with no restrictions. What the actual bytes represent is opaque to the database system.

Create a `BLOB` value with a single byte (170):

<SqlLogicTest id="sql/data_types/blob/example_001" />

Create a `BLOB` value with three bytes (170, 171, 172):

<SqlLogicTest id="sql/data_types/blob/example_002" />

Create a `BLOB` value with two bytes (65, 66):

<SqlLogicTest id="sql/data_types/blob/example_003" />

A `BLOB` cast follows the PostgreSQL `bytea` hex format: the `\x` is a single leading marker followed by one continuous run of hex digit pairs, so three bytes are written as `'\xAAABAC'` rather than a per-byte `'\xAA\xAB\xAC'`. The latter raises `invalid hexadecimal digit: "\"` because the second `\` is not a hex digit. Whitespace between pairs is ignored, so `'\xAA AB AC'` is equivalent.

Blobs are typically used to store non-textual objects that the database does not provide explicit support for, such as images. While blobs can hold objects up to 4 GB in size, typically it is not recommended to store very large objects within the database system. In many situations it is better to store the large file on the file system, and store the path to the file in the database system in a `VARCHAR` field.

## Functions

See [Blob Functions](../../sql/functions/blob.md).
