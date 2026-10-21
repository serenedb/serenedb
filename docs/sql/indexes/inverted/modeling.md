---
title: What to Index
sidebar_position: 3
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Deciding *what* goes into an [inverted index](./index.md) -- which columns, whether to index a raw column or an expression over it, what to merely store for retrieval, and how big the result will be -- is the main modeling decision. This page covers those choices. For the exact `CREATE INDEX` grammar, see the [statement reference](../../statements/create_index/inverted.md).

## Indexed vs. `INCLUDE`d columns

A column can play one of two roles:

- **Indexed** (`USING inverted (col ...)`) -- searchable with `@@`. The column's tokens go into the inverted structure.
- **`INCLUDE`d** (`INCLUDE (col)`) -- *stored but not searchable*. The raw value is kept in the index's columnstore so a query against the index can return it without a second lookup against the base table.

Use `INCLUDE` for columns you frequently *return or filter on* but never *full-text search* -- ids, URLs, prices, timestamps, JSON payloads. An indexed numeric or temporal column serves range filters from the index; to return it or sort by it from the index as well, `INCLUDE` it too.

<DocCallout type="tip">

Returning an `INCLUDE`d column is cheap at query time (it comes straight from the index) but not free overall: the value is **duplicated** into the index's columnstore, so it costs extra disk space and slightly more write/build work. Returning a column the index doesn't store avoids that copy but forces a materialization step against the base table or source -- see [Indexing Views](./views.md#materializing-real-columns). Besides `INCLUDE`d columns and `included()` expressions, the index stores only vector columns and geo columns indexed with `encode_geopoint` or with the default `source` coding of `encode_geojson`, so an indexed text, numeric or temporal column is materialized too. The trade-off is **disk for query speed**: `INCLUDE` the columns you routinely return, and leave rarely-projected ones to materialize.

</DocCallout>

## Indexing expressions

You can index an **expression** over one or more columns, not just a bare column. The query must use the same expression. For example, index `lower(s)` to get case-insensitive exact matching on a verbatim column:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_001" />

To store an indexed expression's value in the index, list it a second time with `included()`, as in `(lower(s)) words, (lower(s)) included()`. A query from the index that returns, filters, groups or sorts on the same expression then reads the stored value instead of computing it, so the columns the expression reads don't have to be in the index, and [phrases without positions](./full-text-search.md#phrases-without-positions) work on it.

An expression whose value depends on the session and not only on its row is indexed as it reads with `TimeZone` set to `UTC` and `bytea_output` set to `hex`, whichever session writes the row: functions and casts over `TIMESTAMPTZ`, `TIMESTAMPTZ_NS`, `TIMETZ` and `VARIANT` values follow the `TimeZone` setting, and a cast of a `bytea` value to text follows `bytea_output`. A query that returns, compares, groups or sorts on such an expression computes it from its columns under its own settings, so `=`, `<`, `BETWEEN`, `IN`, `LIKE` and `IS NULL` return the same rows as without the index, while a full-text search with `@@` matches the indexed text. `AT TIME ZONE` fixes the zone: `(ts AT TIME ZONE 'UTC')::VARCHAR` reads the same in every session, and queries on it use the index like on any other expression.

Indexed expressions must reference at least one column, and every function they call must return the same result for the same arguments. Aggregates, window functions, subqueries, volatile functions such as `random()`, and functions that read the clock, the session or the catalog, such as `now()`, `current_user` or `has_table_privilege()`, are rejected at `CREATE INDEX`. Aggregates, window functions and subqueries each fail with an error of their own. For a function the error names it, except for some calls whose arguments are all constants, such as `has_table_privilege('t', 'SELECT')::VARCHAR`, which fail with `Index keys cannot contain expressions with side effects.` alone:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_006" />

<SqlLogicTest id="sql/indexes/inverted/modeling/example_007" />

Conversions to or from the `reg*` types, such as `regclass` and `regtype`, are rejected too, since their result changes when an object is renamed or dropped. Index the `oid` column instead and compare it with a `reg*` value in the query, as in `oid_column = 'name'::regclass`.

A constant in the expression that can't be computed, such as `CAST(300 AS TINYINT)` or `chr(-1)`, fails `CREATE INDEX` with its error, even in a `CASE` branch that no row reaches.

## Generated columns

A [generated column](../../statements/create_table/index.md) is indexed like any other column -- index the column itself rather than repeating its expression. A `STORED` generated column works directly:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_002" />

## Indexing JSON

Semi-structured JSON is indexed by its sub-fields. Prefer the shredded [`VARIANT`](../../data_types/variant.md) type -- it is the fastest way to store, search and return JSON -- and index typed sub-field extractions such as `(doc['title']::VARCHAR)`.

The dedicated **[Indexing JSON](./json.md)** page covers the full treatment:

- [converting JSON to `VARIANT`](./json.md#converting-json-to-variant) and [how each sub-field type is indexed](./json.md#how-each-sub-field-type-is-indexed),
- [nested fields](./json.md#nested-fields) and [array fields](./json.md#array-fields),
- [assigning a tokenizer per sub-field](./json.md#assigning-a-tokenizer-per-sub-field), and
- [indexing a `JSON` column as `VARIANT`](./json.md#indexing-a-json-column-as-variant) without changing the column type.

## Indexing arrays

A `TEXT[]` / `VARCHAR[]` column is indexed element-by-element: a row matches if **any** element matches. Tokenization and the operator class apply to each element just as for a scalar text column:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_004" />

## Sizing and cost

The index only stores what you ask it to -- keep it lean:

- **Feature flags** (`frequency`, `position`, `offset`, `norm`) each enlarge the index. Enable only what your queries need -- see [Text Analysis](./text-analysis.md#token-positions-and-feature-flags).
- **`INCLUDE` codecs**: each `INCLUDE`d column can set a `compression` codec (`auto`, the default, or `uncompressed`, `rle`, `bitpacking`, `zstd`, `alp`, `alprd`, `roaring` or `dict_fsst`).
- **Vector columns**: the index kind (`ivf` or `hnsw`) decides how a search narrows -- an `ivf` index takes no sizing options (its number of clusters follows each segment's row count), `m` / `ef_construction` trade build time against query precision for an HNSW graph, and `quant` trades index size against recall (recoverable with `sdb_ann_oversample`) -- see [Vector Search](./vector-search.md).
- **Partial indexes**: a `WHERE <predicate>` on `CREATE INDEX` keeps only matching rows in the index -- see [Partial indexes](../../statements/create_index/inverted.md#partial-indexes).
- Prefer `INCLUDE` over post-hoc materialization for columns you routinely return.

## Limitations

Some column types cannot be searchable keys, and a column or expression is listed once as a key; see the [Inverted Index limitations](./index.md#limitations). To search the fields of a `STRUCT` column, index each field as an expression, such as `(s['id']::INTEGER)`.

## See also

- [Inverted Index](./index.md) · [Text Analysis](./text-analysis.md)
- [`CREATE INDEX ... USING inverted`](../../statements/create_index/inverted.md) -- full syntax
- [`VARIANT` type](../../data_types/variant.md) · [JSON overview](../../../data_import_and_export/json/overview.md)
- [Indexing Views](./views.md) -- indexing expressions over external/view data
