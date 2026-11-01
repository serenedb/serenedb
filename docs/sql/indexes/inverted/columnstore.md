---
title: Columnstore Storage & Compression
sidebar_position: 13
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Search tables (`WITH (storage = 'search')`) and the `INCLUDE`d columns of an inverted index keep their values in a **columnstore**: every index segment carries a `.col` file with one compressed column per stored column. This page covers how those columns are laid out, which codecs are available, how to choose one, and how to inspect the result.

## Segments and row groups

A stored column is written in **row groups** of `row_group_size` rows (122,880 by default, see [row-group size](./maintenance.md#performance)). Each row group is compressed into one or more **segments**; a segment is the unit a scan decodes and a filter prunes with its zonemap. Unlike the fixed-size segments of a transactional table, a columnstore segment is as long as its codec needs, so a long string makes its segment longer instead of splitting it.

A list or map column stores each distinct value of a row group once. Columns whose values repeat, such as attribute maps shared by every record of the same resource, shrink by their repeat factor, and an expression over such a column runs once per distinct value.

A search table or an inverted index created with a sort key ([`SORTED BY` on a table](../../statements/create_table/index.md#sort-keys), [on an index](../../statements/create_index/inverted.md#sort-keys)) writes the rows of every segment in key order, so the key columns' zonemaps cover narrow ranges and a filter on the key skips more segments.

## Codecs

A column's codec is either picked automatically (`auto`, the default: each segment takes the smallest of the candidates measured on its own rows) or named explicitly. Numeric columns use the transactional codecs and, under `auto`, the columnstore's [numeric codec](#numeric-columns); text columns use the columnstore's own string codecs, which differ in their *shape* (a dictionary of distinct values with row codes, or one entry per row) and their *leaf* (how the entry bytes are encoded):

| Codec | Types | Shape and leaf |
| :--- | :--- | :--- |
| `uncompressed` | any | values as-is |
| `bitpacking`, `rle`, `alp`, `alprd`, `roaring` | numeric / boolean | the transactional codecs, unchanged |
| `dict_fsst` | text | sorted dictionary, front-coded, FSST-encoded |
| `fsst` | text | one entry per row, front-coded against the previous row, FSST-encoded |
| `dict_lz4` | text | dictionary, lz4 over the dictionary bytes |
| `dict_zstd` | text | dictionary, zstd over the dictionary bytes |
| `lz4` | text | one entry per row, lz4 over the row bytes, for long text with few repeats |
| `dict_zxc` | text | dictionary, [zxc](https://github.com/hellobertrand/zxc) over the dictionary bytes |
| `zxc` | text | one entry per row, zxc over the row bytes |
| `zstd` | text | one entry per row, zstd over the row bytes; on a transactional table, duckdb's codec (zstd over each 2048-row vector, no level) |

A scan that reads whole vectors of a dictionary column hands out **dictionary vectors**, so an expression over the column runs once per distinct value. On `dict_fsst`, whose dictionary is sorted, a comparison of the column with a constant (`=`, `<>`, `<`, `<=`, `>`, `>=`), an `IN` list, or an `AND` / `OR` of them is answered without decoding the dictionary; `IS NULL` / `IS NOT NULL` need no decoding on any dictionary codec, and any other filter on a dictionary column is evaluated once per distinct value. The one-entry-per-row codecs answer filters by decoding the rows. A sparse read, such as a point lookup, a `LIMIT` or the few rows a selective filter keeps, decodes only the parts of a segment it touches.

A refresh writes text with the codecs that encode and decode fastest, `fsst` and level-1 `lz4`, and prefers `fsst` when it is about as small, since it reads a single value without decompressing anything else. A compaction re-encodes text with every leaf and level `auto` considers, and gives the `lz4` and `zstd` leaves a compression dictionary trained for the whole column, so that a point lookup decodes only a small part of a segment. Under `auto` it leaves `zstd` out: on text, `zstd` is 10-25% smaller than the `lz4` leaves but decodes about three times slower, so [name `dict_zstd`](#choosing-a-codec) on the text columns that queries seldom read. When values repeat, the dictionary shape is kept unless one entry per row is clearly smaller; a row group that compression would make larger is stored as-is (`uncompressed`).

A dictionary decoded in full is kept in the database's object cache while the cache has room, so repeated scans and filters of a dictionary column do not decode it again. The cache holds an eighth of `memory_limit` (at least 256 MB) unless `SET GLOBAL sdb_object_cache_size = <bytes>` says otherwise (0 restores the automatic size); a dictionary that does not fit is decoded again by every scan that needs it whole.

[Background compaction](./maintenance.md#background-compaction) merges neighbouring segments as they accumulate and re-encodes them this way, without a `VACUUM`; a merge becomes visible with the table's next refresh. A segment that nothing merges with, such as a table written by a single refresh, keeps the refresh codecs until a merge includes it; `VACUUM (OPTIMIZE_TABLE)` also rewrites such a segment when re-encoding makes it noticeably smaller.

## Numeric columns

Under `auto`, a numeric column (integers, decimals, floating point, dates and timestamps) chooses, row group by row group, between the transactional codecs and the columnstore's numeric codec. The numeric codec rewrites a row group with one transform; the bit-packed transforms store every block of values in as many bits as the block needs:

| Transform | Stores | Suits |
| :--- | :--- | :--- |
| raw | the values as they are | random values, such as identifiers and hashes |
| frame of reference | each value minus the row group's smallest | values within a narrow range |
| bit-packed frame of reference | each value minus the smallest of its block | integers that vary less within a block than across the row group |
| delta | each value minus the previous one | sorted or clustered values, such as timestamps and row ids |
| run length | each run of equal values once, with its length | long runs of repeated values |
| bit-packed run length | each run's value and length, bit-packed | runs of small integers, short runs of wide values such as identifiers repeated over a few rows, and the row codes of text dictionaries |
| dictionary | a code into the row group's sorted distinct values | few distinct wide values, such as repeated hashes |
| bit-packed dictionary | the same codes, bit-packed | few distinct wide values without long runs |

Integer values that share a common factor, such as timestamps that hold whole seconds or decimals with unused precision, are stored divided by it, which saves the bits that only ever hold zeros. Each frame of a numeric segment records the range of its values, so a filter on an integer, decimal, date or timestamp column skips the frames whose range cannot match, and a point lookup or sparse read decodes only the frames it touches.

A refresh stores the transformed values as they are or compressed with `lz4`. A compaction also tries `zstd`, which decodes several times slower than bit-packing and wins only where it shrinks a row group noticeably, as on timestamps, hashes and timings. Floating-point columns usually take `alp` or `alprd`, which decode about as fast as bit-packing and compress floats better than a byte codec. `pragma_storage_info` names the codec `Numeric`.

## Choosing a codec

Name the codec on the column with `USING COMPRESSION`, optionally with a level in parentheses, as in `USING COMPRESSION zstd(compression_level = 9)`. A named codec is used as named: it is not measured against other codecs, and without a level it runs at its default level (1 for `lz4` and `zstd`, 3 for `zxc`). `lz4` and `dict_lz4` take levels 1 to 12, `zstd` and `dict_zstd` 1 to 22, `zxc` and `dict_zxc` 1 to 7; `fsst` and `dict_fsst` take none. A `zstd` frame decodes several times slower than an `fsst` or `lz4` one, which shows on point lookups and cold scans; `zxc` reads like `fsst` at levels 3 to 5 and trades read and write speed for size at 6 and 7.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_001" />

A codec named on a list, map or struct column applies to the values inside it. A column whose values are all text takes the text codecs, and the codec then compresses every string inside it, the keys and the values of a map alike; a column whose values are all numbers takes the numeric codecs, such as `alp` on a `DOUBLE[]` column. Naming a codec on a nested column suits attribute maps that queries return but seldom filter on: name `dict_zstd` on them to store them smaller, and leave the text columns that queries search or filter on to `auto`, whose codecs decode faster.

On an inverted index, an `INCLUDE`d column takes the same names through `included (compression = '...')`, and a level through `compression_level`, as in `included (compression = 'dict_zstd', compression_level = 9)`:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_002" />

An index on a search table has no columnstore of its own: its `INCLUDE`d columns are the table's, and a codec named there applies to the table's column, overriding the column's own `USING COMPRESSION` for the segments written while the index exists. Dropping the index hands the column back to its own declaration.

The global `force_compression` setting applies to search tables as it does to transactional ones: it wins over `auto` and loses to a codec named on the column. A forced codec applies only to the columns it can store and leaves the others to `auto`: text never takes a transactional compression, so `rle` or `dictionary` leave text columns to `auto` while `uncompressed` stores them as-is, and a codec for text only, such as `zstd`, leaves numeric columns to `auto`, whose candidates include the [numeric codec](#numeric-columns). A codec that cannot encode the column's type, or a columnstore-only codec or a `compression_level` on a transactional or temporary table, is rejected at `CREATE TABLE` / `ALTER TABLE`.

## Inspecting storage

[`pragma_storage_info`](../../../configuration/pragmas.md#storage-information) lists every segment of a search table or index with its codec (`compression`), its row range (`start`, `count`), its offset in the `.col` file (`block_offset`) and its size (`segment_info` reads `byte_size=N`); validity streams appear as rows with `segment_type = 'VALIDITY'`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_004" />
