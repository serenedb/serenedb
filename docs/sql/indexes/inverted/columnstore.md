---
title: Columnstore Storage & Compression
sidebar_position: 13
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Tables and the `INCLUDE`d columns of an inverted index keep their values in a **columnstore**: every segment carries a `.col` file with one compressed column per stored column. The rows a table still holds in memory are not compressed; they take their codecs when they are written to a segment. This page covers how those columns are laid out, which codecs are available, how to choose one, and how to inspect the result.

## Segments and row groups

A stored column is written in **row groups** of `row_group_size` rows (122,880 by default, see [row-group size](./maintenance.md#performance)). Each row group is compressed into one or more **segments**: a row group's text is cut into segments of about twice the table's [`block_size`](./maintenance.md#performance), 512 KB by default.

A list or map column whose values repeat, such as attribute maps shared by every record of the same resource, stores each value once per row group: it shrinks by its repeat factor, and an expression over it can run once per distinct value.

## Codecs

A column's codec is either picked automatically (`auto`, the default: every row group takes the candidate that stores it smallest, a codec that reads slower counting as somewhat larger) or named explicitly. Numeric columns use the [numeric codecs](#numeric-columns), floating-point ones also `alp` and `alprd`, boolean ones also `roaring`; text columns use the string codecs, which differ in their *shape* (a dictionary of distinct values with row codes, or one entry per row) and their *leaf* (how the entry bytes are encoded):

| Codec | Types | Shape and leaf |
| :--- | :--- | :--- |
| `uncompressed` | any | values as-is |
| `alp` | floating point | values that are short decimals, as bit-packed integers with the exceptions kept aside |
| `alprd` | floating point | each value split into a front part coded against a small dictionary and a bit-packed rest |
| `roaring` | boolean / `NULL` flags | the flags of each block as a list of positions, as runs or as a bitmap, whichever is smallest |
| `bitpacking` | numeric / boolean | the values as they are or minus the smallest, in whole bytes or bit-packed, or the differences between neighbours where they pack smaller |
| `delta` | numeric | each value minus the previous one, in whole bytes or bit-packed |
| `rle` | numeric / boolean | each run of equal values once with its length, in whole bytes or bit-packed |
| `dict` | numeric | a code into the row group's sorted distinct values, in whole bytes or bit-packed |
| `lz4`, `delta_lz4`, `rle_lz4`, `dict_lz4` | numeric | the same four in whole bytes, compressed with lz4 |
| `zstd`, `delta_zstd`, `rle_zstd`, `dict_zstd` | numeric | the same four in whole bytes, compressed with zstd |
| `dict_fsst` | text | sorted dictionary, each value without the prefix it shares with the previous one, FSST-encoded |
| `fsst` | text | one entry per row, without the prefix it shares with the previous row, FSST-encoded |
| `dict_lz4` | text | dictionary, sorted when that makes the column smaller, lz4 over the dictionary bytes |
| `dict_zstd` | text | dictionary, sorted when that makes the column smaller, zstd over the dictionary bytes |
| `lz4` | text | one entry per row, lz4 over the row bytes, for long text with few repeats |
| `zstd` | text | one entry per row, zstd over the row bytes |

A row group in which every value is the same is stored as `constant`, under `auto` and under a named codec alike. A text row group of only NULLs is `constant` too and takes no bytes; one that holds NULLs beside a value keeps its codec.

An expression over a text column stored with a dictionary codec can run once per distinct value instead of once per row. On `dict_fsst`, whose dictionary is sorted, a comparison of the column with a constant (`=`, `<>`, `<`, `<=`, `>`, `>=`), or an `AND` / `OR` of such comparisons, is answered without decoding the dictionary; on a `dict_lz4` or `dict_zstd` segment whose dictionary is sorted, the same filters are answered by a search in the decoded dictionary. `IS NULL` / `IS NOT NULL` need no decoding on any dictionary codec, and any other filter on a dictionary column is evaluated once per distinct value. The one-entry-per-row codecs answer filters by decoding the rows.

Under `auto`, a statement that writes its rows straight to segments (see [Write memory](./maintenance.md#write-memory)) stores text with the fastest leaves, `fsst` and `lz4` at level 1; the background write of the rows a table holds in memory, and a compaction for the rows it writes anew, try higher `lz4` levels instead of level 1. `auto` never picks `zstd` for text: on text, `zstd` is 10-25% smaller than the `lz4` leaves but decodes about three times slower, so [name `dict_zstd`](#choosing-a-codec) on the text columns that queries seldom read. `auto` stores a row group that compression would make larger as-is (`uncompressed`).

Dictionaries decoded in full, and the numeric data that scans and lookups decompress, are kept in the database's object cache while it has room, so scans, filters and lookups that read a segment again and again do not decode it each time. The cache holds an eighth of `memory_limit` (at least 256 MB) unless `SET GLOBAL sdb_object_cache_size = <bytes>` says otherwise (0 restores the automatic size); a dictionary that does not fit is decoded again by every scan that needs it whole.

[Background compaction](./maintenance.md#background-compaction) copies a column of a segment as it is stored, keeping its codecs, wherever it can. It writes the column anew, with the codecs above, where it cannot, such as in a segment that has deleted or updated rows or fewer rows than a row group, for a nested or `VARIANT` column, for a column under a vector index, and where the column names another codec or level than the segment was written with. A column that goes back to `auto` keeps the codecs of the segments written without a named level. Rows that a statement wrote straight to segments therefore keep that statement's codecs until a compaction writes them anew.

## Numeric columns

Under `auto`, a numeric column (integers, decimals, floating point, dates and timestamps) chooses, row group by row group, among the numeric codecs: integers, decimals, dates and timestamps store their values with `bitpacking`, their runs with `rle`, `rle_lz4` or `rle_zstd`, few distinct wide values with `dict`, `dict_lz4` or `dict_zstd`, and values that none of these shrinks enough with `lz4` or `zstd`. A column of 128-bit integers (`HUGEINT`, `UUID`, decimals of more than 18 digits) takes the same codecs. A boolean column chooses between `bitpacking`, `rle` and `roaring`, a floating-point one between `alp`, `alprd`, `bitpacking`, `rle` and `dict`; on a floating-point column `bitpacking` and `delta` store whole bytes. `bitpacking` stores the differences between neighbouring values where they pack smaller, as in columns written in order such as timestamps, row ids and sequences. `delta`, `delta_lz4` and `delta_zstd` are not candidates under `auto`: `delta` stores the differences throughout, and the other two store sorted or clustered columns such as timestamps smaller still, but scans and sparse reads of such a column take about twice as long; name one where size matters more than read speed. `lz4`, `zstd`, `dict_lz4` and `dict_zstd` name a text codec on a text column and a numeric one on a numeric column. A numeric codec named on a column stores every row group it can; a row group it cannot store that way, such as one without repeated neighbours under `rle` or with too many distinct values under `dict`, is stored `uncompressed`. Each codec stores a row group with one transform; the bit-packed transforms store every block of values in as many bits as the block needs:

| Transform | Stores | Suits |
| :--- | :--- | :--- |
| raw | the values as they are | random values, such as identifiers and hashes |
| frame of reference | each value minus the row group's smallest | values within a narrow range |
| bit-packed frame of reference | each value minus the smallest of its block | integers that vary less within a block than across the row group |
| delta | each value minus the previous one | sorted or clustered values, such as timestamps and row ids; only when `delta`, `delta_lz4` or `delta_zstd` is named |
| bit-packed delta | the same differences, bit-packed; values that advance by one constant step, such as a sequence, take no data bytes | integers written in order whose steps fit in a few bits, such as timestamps, row ids and sequences |
| run length | each run of equal values once, with its length | long runs of repeated values |
| bit-packed run length | each run's value and length, bit-packed | runs of small integers, short runs of wide values such as identifiers repeated over a few rows |
| dictionary | a code into the row group's sorted distinct values | few distinct wide values, such as repeated hashes |
| bit-packed dictionary | the same codes, bit-packed | few distinct wide values without long runs |

Integer values that share a common factor, such as timestamps that hold whole seconds or decimals with unused precision, are stored divided by it, so they take fewer bits. A filter on an integer of up to 64 bits, a decimal of up to 18 digits, a date or a timestamp column skips, without decoding them, the groups of rows whose range of values cannot match, which makes it cheap on a sorted or clustered column.

A statement that writes its rows straight to segments stores the transformed values as they are or compressed with `lz4`. The background write of in-memory rows, and a compaction for the rows it writes anew, also try `zstd`, which decodes several times slower than bit-packing and wins only where it shrinks a row group noticeably, as on timestamps, hashes and timings. Floating-point columns usually take `alp` or `alprd`, which decode about as fast as bit-packing and compress floats better than a byte codec. `pragma_storage_info` names the codec of every segment, such as `RLE_ZSTD`.

## Choosing a codec

Name the codec on the column with `USING COMPRESSION`, optionally with a level in parentheses, as in `USING COMPRESSION zstd(compression_level = 9)`. A named codec is used as named: it is not measured against other codecs, and without a level it runs at its default level (3 for `zstd` and `dict_zstd` on text columns, 1 for every other codec that takes one). `lz4`, `dict_lz4`, `delta_lz4` and `rle_lz4` take levels 1 to 12, `zstd`, `dict_zstd`, `delta_zstd` and `rle_zstd` 1 to 22; the other codecs take none. `zstd` decodes several times slower than `fsst` or `lz4`, which shows on point lookups and cold scans.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_001" />

Under `auto`, a text column whose values repeat is stored with a dictionary codec, `dict_fsst` or `dict_lz4`, and one whose values seldom repeat with `fsst` or `dict_fsst`, which read a row by decoding little more than the row itself: a query that reads a small share of such a column's rows, such as the rows a filter on other columns keeps, pays for those rows only. `lz4` decompresses the neighbouring rows stored with a row to read it, so it reads single rows several times slower, but it scans such a column faster: name it on long text that queries read whole.

A codec named on a list, map, struct, tuple or union column applies to the values inside it, a union's tag among them. A column whose values are all text takes the text codecs, and the codec then compresses every string inside it, the keys and the values of a map alike; a column whose values are all numbers takes the numeric codecs, such as `alp` on a `DOUBLE[]` column; a column that holds both, such as a `MAP(TEXT, INTEGER)`, takes the codecs both have: `uncompressed`, `lz4`, `zstd`, `dict_lz4` and `dict_zstd`. Naming a codec on a nested column suits attribute maps that queries return but seldom filter on: name `dict_zstd` on them to store them smaller, and leave the text columns that queries search or filter on to `auto`, whose codecs decode faster.

On an inverted index, an `INCLUDE`d column takes the same names through `included (compression = '...')`, and a level through `compression_level`, as in `included (compression = 'dict_zstd', compression_level = 9)`:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_002" />

An index on a table has no columnstore of its own: its `INCLUDE`d columns are the table's, and a codec named there applies to the table's column, overriding the column's own `USING COMPRESSION` for the segments written while the index exists. Dropping the index hands the column back to its own declaration.

The global `force_compression` setting wins over `auto` and loses to a codec named on the column. A forced codec applies only to the columns it can store and leaves the others to `auto`: `rle` stores no text and the columnstore has no codec named `dictionary`, so both leave text columns to `auto`, while `uncompressed` stores them as-is, and a codec for text only, such as `fsst`, leaves numeric columns to `auto`. A codec that cannot encode the column's type is rejected at `CREATE TABLE` / `ALTER TABLE`. A table in an attached [DuckDB file](../../statements/attach/duckdb.md) takes DuckDB's own compressions only, and no `compression_level`.

The global `disabled_compression_methods` setting removes the compressions it lists from what `auto` considers, for text as for numbers, and a forced compression it disables falls back to `auto`. A text row group whose every candidate is disabled is stored uncompressed.

## Inspecting storage

[`pragma_storage_info`](../../../configuration/pragmas.md#storage-information) lists every segment of a table or index with its codec (`compression`), its row range (`start`, `count`) and its offset in the `.col` file (`block_offset`); with `include_segment_info = true` it adds `segment_info`, which starts with the segment's size (`byte_size: N`) and goes on with what the codec stored, such as its shape, leaf, level and frames, and for a dictionary whether it is `sorted`. The rows a table still holds in memory appear as `Uncompressed` segments without a size, and a column's NULL flags as rows with `segment_type = 'VALIDITY'`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_004" />
