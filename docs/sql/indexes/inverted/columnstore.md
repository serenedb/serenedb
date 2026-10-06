---
title: Columnstore Storage & Compression
sidebar_position: 13
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Search tables (`WITH (storage = 'search')`) and the `INCLUDE`d columns of an inverted index keep their values in a **columnstore**: every index segment carries a `.col` file with one compressed column per stored column. This page covers how those columns are laid out, which codecs are available, how to choose one, and how to inspect the result.

## Segments and row groups

A stored column is written in **row groups** of `row_group_size` rows (122,880 by default, see [row-group size](./maintenance.md#performance)). Each row group is compressed into one or more **segments**; a segment is the unit a scan decodes and a filter prunes with its zonemap.

A list or map column stores each distinct value of a row group once: a row keeps a code for its value, and only the distinct values' elements reach the element column. Columns whose values repeat, such as attribute maps shared by every record of the same resource, shrink by their repeat factor, and a scan hands each vector of such a column out as a dictionary over its distinct values, so an expression over the column can run once per distinct value. A row group whose lists are almost all distinct skips the dictionary and keeps one entry per row.

Unlike a transactional table, whose segments must fit a fixed 256 KiB block, a columnstore segment can be as long as its codec needs. The `.col` codecs use that freedom: they seal a segment after the vector whose estimated encoded size crosses the table's `segment_target` (256 KiB by default), never rewinding to fit, so a long string only makes its segment longer.

## Codecs

A column's codec is either picked automatically (`auto`, the default: the candidates the table's `compression_objective` allows are encoded on the column's own rows and the smallest result wins, segment by segment) or named explicitly. Numeric columns use the transactional codecs; text columns use the columnstore's own string codecs, which share one layout and differ in their *shape* (a dictionary of distinct values with bitpacked row codes, or one entry per row) and their *leaf* (how the entry bytes are encoded):

| Codec | Types | Shape and leaf |
| :--- | :--- | :--- |
| `uncompressed` | any | values as-is |
| `bitpacking`, `rle`, `alp`, `alprd`, `roaring` | numeric / boolean | the transactional codecs, unchanged |
| `dict_fsst` | text | dictionary, sorted and front-coded (each entry keeps only what it does not share with its predecessor), FSST-encoded suffixes; NULL is a dictionary slot |
| `fsst` | text | one entry per row, front-coded against the previous row, FSST-encoded |
| `dict_lz4` | text | dictionary, lz4 over the dictionary bytes |
| `dict_zstd` | text | dictionary, zstd over the dictionary bytes at the table's `compression_level` |
| `lz4` | text | one entry per row, lz4 over the row bytes, for long text with few repeats |
| `dict_zxc` | text | dictionary, [zxc](https://github.com/hellobertrand/zxc) over the dictionary bytes at the table's `compression_level` (1–7) |
| `zxc` | text | one entry per row, zxc over the row bytes |
| `zstd` | text | one entry per row, zstd over the row bytes at the table's `compression_level` (1–22); on a transactional table, duckdb's codec (zstd over each 2048-row vector, no level) |

The row codes of a dictionary are bitpacked, or run-length coded when the column is sorted or clustered enough for that to be smaller; the choice is made per segment. A scan that reads whole vectors of a dictionary column decodes the segment's dictionary once and hands out **dictionary vectors**, so it pays nothing per row. Both shapes store their entries in frames, and a sparse read, such as a point lookup, a `LIMIT` or the few rows a selective filter keeps, decodes only the frames it touches; with the `fsst` leaf (16 KiB frames) it decodes only the runs of 16 front-coded entries it touches. The `lz4`, `zstd` and `zxc` leaves use one of two frame layouts per segment. In the default layout the first 32 KiB of a segment's entries form its first frame, and every later frame (16 KiB of text, 32 KiB for `zstd`) is compressed with that first frame as its dictionary, so small frames keep the ratio of large ones; a read that needs a later frame decodes the first frame once and keeps it for the following reads of the segment. The other layout stores 64 KiB frames without a dictionary, for text that shares little between entries, such as random identifiers. When a segment is compacted, the `lz4` and `zstd` leaves can also use a dictionary trained for the whole column: before writing a text column, the compaction samples about 4 MiB of its distinct values from 32 places spread evenly over all the segments it merges, trains a 64 KiB dictionary from them and stores it once for the column in the new segment's `.col` file. Every frame of the column, the first one included, is then 16 KiB and compressed with that dictionary. A column with too little distinct text for a full sample keeps sampling the row groups as they are written and trains once it has enough. A point lookup then decodes only the frame it needs, since the dictionary is loaded once per column and kept in memory. Columns with little text keep the first-frame layout, and a refresh never trains a dictionary. A `dict_fsst` dictionary is sorted, so a comparison of the column with a constant (`=`, `<>`, `<`, `<=`, `>`, `>=`), an `IN` list, or an `AND` / `OR` of them is answered by a binary search over the dictionary without decoding it, and `IS NULL` / `IS NOT NULL` need no decoding on any dictionary codec. Any other filter on a dictionary column is evaluated once per distinct value; the one-entry-per-row codecs answer filters by decoding the vector.

Under `auto`, a text column measures its candidates on the segment being written. A measurement takes the first segment of a row group, encodes it in both shapes with `lz4` to settle the shape, then measures every leaf the table's `compression_objective` allows in the shape that won. `fsst` and level-1 `lz4` are cheap and are encoded in full; every other leaf and level is priced instead. A price takes the frame table, lengths and codes from the segment's counts, and the compressed bytes from compressing, exactly as the encoder would, the segment's first frame (which the later frames use as their dictionary) and one frame from each of eight equal stretches of the segment. The leaf with the smallest size wins and is encoded; the segment itself is written with the smallest encoding measured, which can be a higher level of the same leaf than the one the column keeps, and the following segments use the winner's codec. Under `balanced`, `fsst` wins whenever it is at most 5% larger than the smallest, since it decodes a single value without decompressing a frame. A column measures every row group until a measurement picks the same codec as the one before it; from then on it measures less often: two row groups later, then four, eight, and every 16th row group while that holds. A measurement that changes the codec returns the column to measuring every row group. Every segment is also checked against its codec: when its size per input byte moves more than 25% away from what the codec measured, that segment is measured again against every candidate and the column returns to measuring every row group, so a column whose values change partway through a row group changes codec where they change. `speed` measures `lz4` only. `balanced`, the default, measures the leaves whose point lookups stay fast and whose writes stay cheap: `fsst`, `lz4` at levels 1, 4 and 6 (4 and 6 are lz4hc, which decodes as fast as level 1) and `zxc` at levels 1 and 3. `size` adds `lz4` level 9, `zstd` (levels 1, 3, 6, 9 and 12) and `zxc` levels 5 and 7. With `compression_level` left at 0, each column finds its own levels: its first measurement in an index segment prices every level of each leaf's ladder and keeps the lowest level within 2% of the smallest, and the rest of the column keeps the level found; a table-level `compression_level` pins every leaf to that level instead. The same measurement picks the frame layout of each `lz4`, `zstd` and `zxc` leaf: the dictionary layout stays unless 64 KiB frames without a dictionary are at least 3% smaller. A codec named on a column always uses the dictionary layout. When values repeat (at most half as many distinct values as rows), the dictionary shape wins unless the one-entry-per-row shape is at least 10% and at least 4 KiB smaller per segment: a dictionary gives dictionary vectors, once-per-value filters and in-band NULLs, which a small size edge does not pay for. A column of mostly distinct values takes whichever shape is smaller. When a row group's first segment comes out larger than the same rows stored as-is, the row group falls back to the transactional codecs.

A dictionary decoded in full is kept in the database's object cache the second time its segment decodes it, while the cache has room, so repeated scans and filters of a dictionary column do not decode it again. The cache holds an eighth of `memory_limit` (at least 256 MB) unless `SET GLOBAL sdb_object_cache_size = <bytes>` says otherwise (0 restores the automatic size); a dictionary that does not fit is decoded again by every scan that needs it whole.

The objective applies when segments are compacted. A segment written by a refresh measures only the two cheap leaves, `fsst` and level-1 `lz4` (`lz4` alone under `speed`), and trains no dictionary: ingest pays the cheapest encoders, and the footprint of a table follows its objective once compaction has re-encoded the fresh segments. The background compaction task (every `compaction_interval`, 1 second by default) merges segments of similar size as they accumulate, up to 10 segments and 5 GB per merge, so this happens without a `VACUUM`; a merge becomes visible with the table's next refresh (every `refresh_interval`, or `VACUUM (REFRESH_TABLE)` when the background refresh is off). A segment that nothing merges with, such as a table written by a single refresh, keeps the refresh codecs until the next merge or a `VACUUM (COMPACT_TABLE)`. zstd frames decode several times slower than `fsst` on scans and point lookups; zxc reads like `fsst` at levels 3 to 5 and trades read and write speed for size at 6 and 7.

## Choosing a codec

Name the codec on the column with `USING COMPRESSION`, optionally with the codec's own level in parentheses, which overrides the table's `compression_level` for that column:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_001" />

On an inverted index, an `INCLUDE`d column takes the same names, with the same optional level, through `included (compression = '...')`:

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_002" />

An index on a search table has no columnstore of its own: its `INCLUDE`d columns are the table's, and a codec named there applies to the table's column, overriding the column's own `USING COMPRESSION` for the segments written while the index exists. Dropping the index hands the column back to its own declaration.

The global `force_compression` setting applies to search tables as it does to transactional ones: it wins over `auto` and loses to a codec named on the column. A codec that cannot encode the column's type, or a columnstore-only codec or a `compression_level` on a transactional or temporary table, is rejected at `CREATE TABLE` / `ALTER TABLE`.

## Table options

Three `WITH` options of a search table tune the columnstore codecs. [`ALTER TABLE … SET`](../../statements/alter_table/index.md#set--reset-storage-options) changes them later, as [`ALTER COLUMN … SET COMPRESSION`](../../statements/alter_table/index.md#alter-column--set-compression) changes a column's codec; either applies to the segments written afterwards, and compaction rewrites the older ones.

| Option | Default | Effect |
| :--- | :--- | :--- |
| `compression_objective` | `'balanced'` | What `auto` optimizes for when a segment is compacted: `'speed'` keeps the `lz4` leaves (the fastest reads and writes), `'balanced'` takes the smallest of the leaves whose point lookups stay fast and whose writes stay cheap (`fsst`, `lz4` up to level 6, `zxc` up to level 3), and `'size'` also considers `lz4` level 9, `zstd` and `zxc` levels 5 and 7, which make compaction slower and, for `zstd` and high `zxc` levels, scans and point lookups too. A codec named on a column ignores it. |
| `compression_level` | `0` (tuned per column under `auto`; a named codec uses its own default) | The byte-codec level of every column that does not name its own (`USING COMPRESSION dict_zstd(compression_level = 9)`, or `'dict_zstd(compression_level = 9)'` on an INCLUDE column): the level of `dict_lz4` and `lz4` (1 = lz4, 2–12 = lz4hc), `dict_zstd` and `zstd` (1–22) and `dict_zxc` / `zxc` (1–7, 3 by default; 6 and 7 add Huffman-coded literals and need several times the frame in encoder memory). Set, it replaces the per-column tuning of `auto` for every leaf. Higher levels are smaller and slower to write; reads are unaffected for lz4, and slower only at the high zstd and zxc levels. |
| `segment_target` | `262144` | The size in bytes a `.col` codec seals a segment at (multiple of 4096, at least 16384). Larger segments give the dictionary codecs more rows to deduplicate over and longer code runs, at the cost of a larger dictionary to decode per segment; a point lookup decodes one frame (the row's frame of an `lz4`, `zstd` or `zxc` leaf, plus the segment's first frame when that frame is the dictionary, both kept for the following lookups into the segment, or at most 16 front-coded entries of `fsst` / `dict_fsst`) whatever the segment size. |

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_003" />

## Inspecting storage

[`pragma_storage_info`](../../../configuration/pragmas.md#storage-information) lists every segment of a search table or index with its codec (`compression`), its row range (`start`, `count`), its offset in the `.col` file (`block_offset`) and its size (`segment_info` reads `byte_size=N`); validity streams appear as rows with `segment_type = 'VALIDITY'`.

<SqlLogicTest id="sql/indexes/inverted/columnstore/example_004" />
