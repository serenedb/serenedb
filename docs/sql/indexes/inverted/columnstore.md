---
title: Columnstore Storage & Compression
sidebar_position: 13
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Tables, and the `INCLUDE`d columns of an inverted index, store their columns compressed. This page covers which codecs are available, how to choose one, and how to see what a table uses.

## Segments and row groups

A column is compressed in **row groups** of `row_group_size` rows (122,880 by default) and stored in **segments**, which [`pragma_storage_info`](#inspecting-storage) lists. A table's [`block_size`](./maintenance.md#performance) sets how large its text segments get: a smaller value reads less around a single row and compresses a little worse.

A list or map column whose values repeat, such as attribute maps shared by every record of the same resource, takes little more space than its distinct values, and expressions over it get cheaper the more its values repeat.

## Codecs

A column's codec is either picked automatically (`auto`, the default) or named explicitly:

| Codec | Types | Suits |
| :--- | :--- | :--- |
| `uncompressed` | any | values that do not compress; the fastest to write and read, the largest |
| `alp` | floating point | decimal-like values, such as prices and measurements |
| `alprd` | floating point | values with many significant digits |
| `roaring` | boolean / `NULL` flags | flags that are mostly one value, or clustered |
| `bitpacking` | numeric / boolean | values in a narrow range, and values written in order, such as timestamps, row ids and sequences |
| `delta` | numeric | sorted or clustered values; smaller than `bitpacking` on them, slower to scan |
| `rle` | numeric / boolean | long runs of repeated values |
| `dict` | numeric | few distinct wide values, such as repeated hashes |
| `lz4`, `delta_lz4`, `rle_lz4`, `dict_lz4` | numeric | the same, smaller, slower to decode |
| `zstd`, `delta_zstd`, `rle_zstd`, `dict_zstd` | numeric | the same, smaller still, several times slower to decode |
| `dict_fsst` | text | values that repeat; fast filters and single-row reads |
| `fsst` | text | values that seldom repeat; reading a few rows costs little |
| `dict_lz4` | text | values that repeat; scans faster than `dict_fsst`, stores more |
| `dict_zstd` | text | values that repeat and are seldom read; the smallest, the slowest to decode |
| `lz4` | text | long text with few repeats that queries read whole; reads single rows slowly |
| `zstd` | text | long text that is seldom read; smaller than `lz4`, slower to decode |

A row group whose values are all equal is stored as `constant` whatever the codec, and so is a text row group of only `NULL`s.

Filters and expressions on a text column stored with a dictionary codec (`dict_fsst`, `dict_lz4`, `dict_zstd`) get cheaper the more its values repeat.

Decompressed data that queries read again stays in memory while there is room, so repeated scans and lookups of the same columns do not decompress it each time. `SET GLOBAL sdb_object_cache_size = <bytes>` sets how much memory that takes (by default an eighth of `memory_limit`, at least 256 MB; `0` restores the default): a smaller value saves memory and makes repeated reads slower.

## How `auto` chooses

`auto` picks a codec for every row group: the candidate that stores it smallest, a codec that reads slower counting as somewhat larger. A row group that compression would make larger is stored `uncompressed`.

- **Text:** a column whose values repeat takes `dict_fsst` or `dict_lz4`, one whose values seldom repeat `fsst` or `dict_fsst`. `auto` never picks `zstd` for text: it is 10-25% smaller than the `lz4` codecs but decodes about three times slower, so [name `dict_zstd`](#choosing-a-codec) on text columns that queries seldom read.
- **Numbers:** integers, decimals, dates and timestamps (128-bit integers and `UUID` included) choose among `bitpacking`, `rle` and `dict` and their `lz4` and `zstd` forms; floating point among `alp`, `alprd`, `bitpacking`, `rle` and `dict`; booleans among `bitpacking`, `rle` and `roaring`. `delta` and its forms are never picked: name one where size matters more than scan speed.
- **When rows are written:** a statement that writes its rows straight to disk (see [Write memory](./maintenance.md#write-memory)) compresses for speed; the background write of the rows a table holds in memory, and compaction, spend more time to store them smaller.

A filter on an integer of up to 64 bits, a decimal of up to 18 digits, a date or a timestamp is cheap on a sorted or clustered column: rows that cannot match are skipped without being read.

## Choosing a codec

Name the codec on the column with `USING COMPRESSION`, optionally with a level in parentheses, as in `USING COMPRESSION zstd(compression_level = 9)`. A named codec is used as named, without comparing it to others, at its default level unless a level is given: 3 for `zstd` and `dict_zstd` on text columns, 1 for every other codec that takes one. `lz4`, `dict_lz4`, `delta_lz4` and `rle_lz4` take levels 1 to 12, `zstd`, `dict_zstd`, `delta_zstd` and `rle_zstd` 1 to 22; the other codecs take none. `lz4`, `zstd`, `dict_lz4` and `dict_zstd` name a text codec on a text column and a numeric one on a numeric column. A row group that a named numeric codec cannot store, such as one without repeated neighbours under `rle` or with too many distinct values under `dict`, is stored `uncompressed`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_001" />

`zstd` decodes several times slower than `fsst` or `lz4`, which shows on point lookups and cold scans. `lz4` on a text column reads single rows several times slower than `fsst` but scans faster: name it on long text that queries read whole.

A codec named on a list, map, struct, tuple or union column applies to the values inside it, a union's tag among them. A column whose values are all text takes the text codecs, and the codec then compresses every string inside it, the keys and the values of a map alike; a column whose values are all numbers takes the numeric codecs, such as `alp` on a `DOUBLE[]` column; a column that holds both, such as a `MAP(TEXT, INTEGER)`, takes the codecs both have: `uncompressed`, `lz4`, `zstd`, `dict_lz4` and `dict_zstd`. Naming `dict_zstd` on attribute maps that queries return but seldom filter on stores them smaller; leave the columns that queries search or filter on to `auto`.

On an inverted index, an `INCLUDE`d column takes the same names through `included (compression = '...')`, and a level through `compression_level`, as in `included (compression = 'dict_zstd', compression_level = 9)`:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_002" />

An index stores no copy of its `INCLUDE`d columns: a codec named there applies to the table's column, overriding the column's own `USING COMPRESSION` for the rows written while the index exists. Dropping the index hands the column back to its own declaration.

A codec named with `ALTER TABLE ... SET COMPRESSION` applies to the rows written afterwards; the existing rows keep their codecs until [background compaction](./maintenance.md#background-compaction) writes them anew with the column's codec. Rows that a statement wrote straight to disk likewise keep their faster codecs until then.

The global `force_compression` setting wins over `auto` and loses to a codec named on the column. A forced codec applies only to the columns it can store and leaves the others to `auto`: `rle` stores no text and `dictionary` is not a columnstore codec, so both leave text columns to `auto`, while `uncompressed` stores them as-is, and a codec for text only, such as `fsst`, leaves numeric columns to `auto`. A codec that cannot encode the column's type is rejected at `CREATE TABLE` / `ALTER TABLE`. A table in an attached [DuckDB file](../../statements/attach/duckdb.md) takes DuckDB's own compressions only, and no `compression_level`.

The global `disabled_compression_methods` setting removes the compressions it lists from what `auto` considers, and a forced compression it disables falls back to `auto`. A text row group whose every candidate is disabled is stored `uncompressed`.

## Inspecting storage

[`pragma_storage_info`](../../../configuration/pragmas.md#storage-information) lists every segment of a table or index with its codec (`compression`), its row range (`start`, `count`) and its position (`block_offset`); with `include_segment_info = true` it adds `segment_info`, which starts with the segment's size (`byte_size: N`) and goes on with details of the codec. The rows a table still holds in memory appear as `Uncompressed` segments without a size, and a column's NULL flags as rows with `segment_type = 'VALIDITY'`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_004" />
