---
title: Columnstore Storage & Compression
sidebar_position: 13
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Tables and the `INCLUDE`d columns of an inverted index keep their values in a **columnstore**: every segment carries a `.col` file with one compressed column per stored column. The rows a table still holds in memory are not compressed; they take their codecs when they are written to a segment. This page covers how those columns are laid out, which codecs are available, how to choose one, and how to inspect the result.

## Segments and row groups

A stored column is written in **row groups** of `row_group_size` rows (122,880 by default, see [row-group size](./maintenance.md#performance)). Each row group is compressed into one or more **segments**; a segment is the unit a scan decodes and a filter prunes with its zonemap. A text segment is as long as its codec needs, so a long string makes its segment longer instead of splitting it; a row group's text is cut into segments of about twice the table's [`block_size`](./maintenance.md#performance), 512 KB by default. A segment of a dictionary codec that holds at least 8,192 rows ends on a multiple of 2,048 rows, so a scan reads it in whole vectors.

A list or map column stores each distinct value of a row group once. Columns whose values repeat, such as attribute maps shared by every record of the same resource, shrink by their repeat factor, and an expression over such a column runs once per distinct value.

## Codecs

A column's codec is either picked automatically (`auto`, the default: every row group takes the candidate that stores it smallest, a codec that reads slower counting as somewhat larger; a candidate that was behind on the last row groups is not tried on each one, but again once the values change enough for it to win, and every candidate from time to time; the `lz4` level and dictionary of a text column are chosen again when a row group brings several times more text than they were chosen on) or named explicitly. Numeric columns use the [numeric codecs](#numeric-columns), floating-point ones also `alp` and `alprd`, boolean ones also `roaring`; text columns use the string codecs, which differ in their *shape* (a dictionary of distinct values with row codes, or one entry per row) and their *leaf* (how the entry bytes are encoded):

| Codec | Types | Shape and leaf |
| :--- | :--- | :--- |
| `uncompressed` | any | values as-is |
| `alp` | floating point | values that are short decimals, as bit-packed integers with the exceptions kept aside |
| `alprd` | floating point | each value split into a front part coded against a small dictionary and a bit-packed rest |
| `roaring` | boolean / `NULL` flags | the flags of each block as a list of positions, as runs or as a bitmap, whichever is smallest |
| `bitpacking` | numeric / boolean | the values as they are or minus the smallest, in whole bytes or bit-packed; a bit-packed block of 1,024 values stores the differences between neighbours instead where that is smaller; when `bitpacking` is named, a block that holds NULLs keeps the offsets, which read faster row by row |
| `delta` | numeric | each value minus the previous one, in whole bytes or bit-packed, in every block |
| `rle` | numeric / boolean | each run of equal values once with its length, in whole bytes or bit-packed |
| `dict` | numeric | a code into the row group's sorted distinct values, in whole bytes or bit-packed |
| `lz4`, `delta_lz4`, `rle_lz4`, `dict_lz4` | numeric | the same four in whole bytes, compressed with lz4 |
| `zstd`, `delta_zstd`, `rle_zstd`, `dict_zstd` | numeric | the same four in whole bytes, compressed with zstd |
| `dict_fsst` | text | sorted dictionary, front-coded, FSST-encoded |
| `fsst` | text | one entry per row, front-coded against the previous row, FSST-encoded |
| `dict_fsst_plus` | text | sorted dictionary, a prefix shared by neighbouring entries stored once, FSST-encoded; reading one entry decodes only that entry |
| `fsst_plus` | text | one entry per row, a prefix shared by neighbouring rows stored once, FSST-encoded; reading one row decodes only that row |
| `dict_onpair` | text | sorted dictionary, each entry stored as codes for frequent substrings of up to 16 bytes learned per segment; reading one entry decodes only that entry |
| `onpair` | text | one entry per row, each row stored as codes for frequent substrings of up to 16 bytes learned per segment; reading one row decodes only that row |
| `dict_fsst12` | text | sorted dictionary, front-coded, encoded with FSST's 12-bit variant: up to 4096 symbols of up to 8 bytes instead of 255 |
| `fsst12` | text | one entry per row, front-coded against the previous row, encoded with FSST's 12-bit variant |
| `dict_lz4` | text | dictionary, sorted when that makes the column smaller, lz4 over the dictionary bytes |
| `dict_zstd` | text | dictionary, sorted when that makes the column smaller, zstd over the dictionary bytes |
| `lz4` | text | one entry per row, lz4 over the row bytes, for long text with few repeats |
| `dict_zxc` | text | dictionary, sorted when that makes the column smaller, [zxc](https://github.com/hellobertrand/zxc) over the dictionary bytes |
| `zxc` | text | one entry per row, zxc over the row bytes |
| `zstd` | text | one entry per row, zstd over the row bytes |

A row group in which every value is the same is stored as `constant`, under `auto` and under a named codec alike: a number is kept in the row group's statistics alone, a text value is stored once, and a scan hands out constant vectors. A text row group that holds NULLs keeps its codec.

A scan that reads whole vectors of a text column stored with a dictionary codec hands out **dictionary vectors**, so an expression over the column runs once per distinct value. On `dict_fsst`, `dict_fsst_plus`, `dict_onpair` and `dict_fsst12`, whose dictionaries are sorted, a comparison of the column with a constant (`=`, `<>`, `<`, `<=`, `>`, `>=`), an `IN` list, or an `AND` / `OR` of them is answered without decoding the dictionary. `dict_lz4`, `dict_zstd` and `dict_zxc` store both orders of a column's first row group, keep their dictionaries sorted when that is clearly smaller, and then answer the same filters by searching the decoded dictionary; otherwise the values stay in the order the rows first use them, which keeps the row codes small and scans them faster. `IS NULL` / `IS NOT NULL` need no decoding on any dictionary codec, and any other filter on a dictionary column is evaluated once per distinct value. The one-entry-per-row codecs answer filters by decoding the rows. A sparse read, such as a point lookup, a `LIMIT` or the few rows a selective filter keeps, decodes only the parts of a segment it touches.

A statement that writes its rows straight to segments (see [Write memory](./maintenance.md#write-memory)) stores text with the codecs that encode and decode fastest, `fsst` and level-1 `lz4`, and prefers `fsst` when it is about as small, since it reads a single value without decompressing anything else. The background write of the rows a table holds in memory, and a compaction for the rows it writes anew, encode text with every leaf and level `auto` considers, and train a compression dictionary for each `lz4`, `zstd` and `zxc` segment on the segment's own values, so that values are compressed in small blocks and a point lookup decodes only one of them; a segment keeps its dictionary only when that makes it smaller. Under `auto` they leave `zstd` out: on text, `zstd` is 10-25% smaller than the `lz4` leaves but decodes about three times slower, so [name `dict_zstd`](#choosing-a-codec) on the text columns that queries seldom read. `auto` never picks `zxc`, `fsst_plus`, `onpair`, `fsst12` or their dictionary forms. When values repeat, the dictionary shape is kept unless one entry per row is clearly smaller. Values that seldom repeat are stored with the `fsst` leaf alone, one entry per row unless their sorted dictionary is clearly smaller. A row group that compression would make larger is stored as-is (`uncompressed`).

A dictionary decoded in full, and the parts of a numeric column that scans and lookups decompress, are kept in the database's object cache while the cache has room, so repeated scans, filters and lookups do not decode them again. Scans add the decompressed parts of numeric columns only while the cache is less than half full, which leaves the rest to point lookups and to dictionaries. The cache holds an eighth of `memory_limit` (at least 256 MB) unless `SET GLOBAL sdb_object_cache_size = <bytes>` says otherwise (0 restores the automatic size); a dictionary that does not fit is decoded again by every scan that needs it whole.

[Background compaction](./maintenance.md#background-compaction) copies a column of a segment as it is stored whenever it can, so merging segments costs little more than reading and writing their bytes. It writes a column anew, with the codecs above, where it cannot: in a segment that has deleted or updated rows or fewer rows than a row group, for a nested or `VARIANT` column, for a column under a vector index, and after the column's codec or level has changed. Rows that a statement wrote straight to segments therefore keep that statement's codecs until a compaction writes them anew.

## Numeric columns

Under `auto`, a numeric column (integers, decimals, floating point, dates and timestamps) chooses, row group by row group, among the numeric codecs: integers, decimals, dates and timestamps store their values with `bitpacking`, their runs with `rle`, `rle_lz4` or `rle_zstd`, few distinct wide values with `dict`, `dict_lz4` or `dict_zstd`, and values that none of these shrinks enough with `lz4` or `zstd`. A column of 128-bit integers (`HUGEINT`, `UUID`, decimals of more than 18 digits) takes the same codecs: a row group whose values lie within 2^64 of each other is stored as 64-bit values, a wider one, such as time-ordered UUIDs, as its two 64-bit halves when that makes it about an eighth smaller or more (under `auto`, as a dictionary of the halves only up to about two thousand distinct values in a row group), and one that does not shrink that much stays `uncompressed`. A boolean column chooses between `bitpacking`, `rle` and `roaring`, a floating-point one between `alp`, `alprd`, `bitpacking`, `rle` and `dict`; on a floating-point column `bitpacking` and `delta` store whole bytes. `bitpacking` decides block by block: a block of 1,024 values whose differences between neighbours pack at least 5% smaller than its offsets from the block's smallest value stores the differences, as blocks of columns written in order do, such as timestamps, row ids and sequences; reading a single row of such a block adds up its differences. A column that names `bitpacking` takes the smaller form of every block. `delta`, `delta_lz4` and `delta_zstd` are not candidates under `auto`: `delta` stores differences in every block, and the other two store sorted or clustered columns such as timestamps smaller still, but scans and sparse reads of such a column take about twice as long; name one where size matters more than read speed. `lz4`, `zstd`, `dict_lz4` and `dict_zstd` name a text codec on a text column and a numeric one on a numeric column. A numeric codec named on a column stores every row group it can; a row group it cannot store that way, such as one without repeated neighbours under `rle` or with too many distinct values under `dict`, is stored `uncompressed`. Each codec rewrites a row group with one transform; the bit-packed transforms store every block of values in as many bits as the block needs:

| Transform | Stores | Suits |
| :--- | :--- | :--- |
| raw | the values as they are | random values, such as identifiers and hashes |
| frame of reference | each value minus the row group's smallest | values within a narrow range |
| bit-packed frame of reference | each value minus the smallest of its block | integers that vary less within a block than across the row group |
| delta | each value minus the previous one | sorted or clustered values, such as timestamps and row ids; only when a `delta_*` codec is named |
| bit-packed delta | the same differences, bit-packed; values that advance by one constant step, such as a sequence, take no data bytes | integers written in order whose steps fit in a few bits, such as timestamps, row ids and sequences |
| run length | each run of equal values once, with its length | long runs of repeated values |
| bit-packed run length | each run's value and length, bit-packed | runs of small integers, short runs of wide values such as identifiers repeated over a few rows, and the row codes of text dictionaries |
| dictionary | a code into the row group's sorted distinct values | few distinct wide values, such as repeated hashes |
| bit-packed dictionary | the same codes, bit-packed | few distinct wide values without long runs |

Integer values that share a common factor, such as timestamps that hold whole seconds or decimals with unused precision, are stored divided by it, which saves the bits that only ever hold zeros. Each frame of a numeric segment records the range of its values, and a bit-packed block of sorted or clustered values bounds its own, so a filter on an integer of up to 64 bits, a decimal of up to 18 digits, a date or a timestamp column skips the frames and the vectors of rows whose range cannot match, and a point lookup or sparse read decodes only the frames it touches.

A statement that writes its rows straight to segments stores the transformed values as they are or compressed with `lz4`. The background write of in-memory rows, and a compaction for the rows it writes anew, also try `zstd`, which decodes several times slower than bit-packing and wins only where it shrinks a row group noticeably, as on timestamps, hashes and timings. Floating-point columns usually take `alp` or `alprd`, which decode about as fast as bit-packing and compress floats better than a byte codec. `pragma_storage_info` names the codec of every row group, such as `RLE_ZSTD`.

## Choosing a codec

Name the codec on the column with `USING COMPRESSION`, optionally with a level in parentheses, as in `USING COMPRESSION zstd(compression_level = 9)`. A named codec is used as named: it is not measured against other codecs, and without a level it runs at its default level (1 for `lz4` and `zstd`, 3 for `zxc`). `lz4`, `dict_lz4`, `delta_lz4` and `rle_lz4` take levels 1 to 12, `zstd`, `dict_zstd`, `delta_zstd` and `rle_zstd` 1 to 22, `zxc` and `dict_zxc` 1 to 7; `fsst`, `dict_fsst`, `fsst_plus`, `dict_fsst_plus`, `onpair`, `dict_onpair`, `fsst12`, `dict_fsst12`, `bitpacking`, `delta`, `rle` and `dict` take none. A `zstd` frame decodes several times slower than an `fsst` or `lz4` one, which shows on point lookups and cold scans; `zxc` reads like `fsst` at levels 3 to 5 and trades read and write speed for size at 6 and 7.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_001" />

Under `auto`, a text column whose values seldom repeat is stored with `fsst` or `dict_fsst`, which read a row by decoding little more than the row itself: a query that reads a small share of such a column's rows, such as the rows a filter on other columns keeps, pays for those rows only. `lz4` decompresses the rows stored before a row in its frame, so it reads single rows several times slower, but it scans such a column faster: name it on long text that queries read whole. Of the other one-entry-per-row codecs, `onpair` is the smallest and `fsst_plus` the largest.

A codec named on a list, map or struct column applies to the values inside it. A column whose values are all text takes the text codecs, and the codec then compresses every string inside it, the keys and the values of a map alike; a column whose values are all numbers takes the numeric codecs, such as `alp` on a `DOUBLE[]` column; a column that holds both, such as a `MAP(TEXT, INTEGER)`, takes the codecs both have: `uncompressed`, `lz4`, `zstd`, `dict_lz4` and `dict_zstd`. Naming a codec on a nested column suits attribute maps that queries return but seldom filter on: name `dict_zstd` on them to store them smaller, and leave the text columns that queries search or filter on to `auto`, whose codecs decode faster.

On an inverted index, an `INCLUDE`d column takes the same names through `included (compression = '...')`, and a level through `compression_level`, as in `included (compression = 'dict_zstd', compression_level = 9)`:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_002" />

An index on a table has no columnstore of its own: its `INCLUDE`d columns are the table's, and a codec named there applies to the table's column, overriding the column's own `USING COMPRESSION` for the segments written while the index exists. Dropping the index hands the column back to its own declaration.

The global `force_compression` setting wins over `auto` and loses to a codec named on the column. A forced codec applies only to the columns it can store and leaves the others to `auto`: `rle` stores no text and the columnstore has no codec named `dictionary`, so both leave text columns to `auto`, while `uncompressed` stores them as-is, and a codec for text only, such as `fsst`, leaves numeric columns to `auto`. A codec that cannot encode the column's type is rejected at `CREATE TABLE` / `ALTER TABLE`. A table in an attached [DuckDB file](../../statements/attach/duckdb.md) takes DuckDB's own compressions only, and no `compression_level`.

The global `disabled_compression_methods` setting removes the compressions it lists from what `auto` considers, for text as for numbers, and a forced compression it disables falls back to `auto`. A text row group whose every candidate is disabled is stored uncompressed.

## Inspecting storage

[`pragma_storage_info`](../../../configuration/pragmas.md#storage-information) lists every segment of a table or index with its codec (`compression`), its row range (`start`, `count`) and its offset in the `.col` file (`block_offset`); with `include_segment_info = true` it adds `segment_info`, which starts with the segment's size (`byte_size: N`) and goes on with what the codec stored, such as its shape, leaf, level and frames, and for a dictionary whether it is `sorted`. A segment of `fsst12` or `dict_fsst12` also names the block that holds its symbol table (`shared_offset: N, shared_size: N`): the segments of a row group share one block, stored once in the `.col` file, so it counts once towards the size of the column. The rows a table still holds in memory appear as `Uncompressed` segments without a size, and validity streams as rows with `segment_type = 'VALIDITY'`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_004" />
