---
title: What to Index
sidebar_position: 3
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Deciding *what* goes into an [inverted index](./index.md) — which columns, whether to index a raw column or an expression over it, what to merely store for retrieval, and how big the result will be — is the main modeling decision. This page covers those choices. For the exact `CREATE INDEX` grammar, see the [statement reference](../../statements/create_index/inverted.md).

## Indexed vs. `INCLUDE`d columns

A column can play one of two roles:

- **Indexed** (`USING inverted (col …)`) — searchable with `@@`. The column's tokens go into the inverted structure.
- **`INCLUDE`d** (`INCLUDE (col)`) — *stored but not searchable*. The raw value is kept in the index's columnstore so a query against the index can return it without a second lookup against the base table.

Use `INCLUDE` for columns you frequently *return or filter on* but never *full-text search* — ids, URLs, prices, timestamps, JSON payloads. Numeric and temporal indexed columns are stored in a columnar form too, so they support fast range filtering and sorting alongside search.

<DocCallout type="tip">

Returning an `INCLUDE`d column is cheap at query time (it comes straight from the index) but not free overall: the value is **duplicated** into the index's columnstore, so it costs extra disk space and slightly more write/build work. Returning a column that is neither indexed nor `INCLUDE`d avoids that copy but forces a materialization step against the base table or source — see [Indexing Views](./views.md#materializing-real-columns). The trade-off is **disk for query speed**: `INCLUDE` the columns you routinely return, and leave rarely-projected ones to materialize.

</DocCallout>

## Indexing expressions

You can index an **expression** over one or more columns, not just a bare column. The query must use the same expression. For example, index `lower(s)` to get case-insensitive exact matching on a verbatim column:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_001" />

Indexed expressions must be deterministic and reference at least one column. Aggregates, subqueries and volatile functions are rejected at `CREATE INDEX`:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_006" />

<SqlLogicTest id="sql/indexes/inverted/modeling/example_007" />

## Generated columns

A [generated column](../../statements/create_table/index.md) is indexed like any other column — index the column itself rather than repeating its expression. A `STORED` generated column works directly:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_002" />

## Indexing JSON

Semi-structured JSON is indexed by its sub-fields. Prefer the shredded [`VARIANT`](../../data_types/variant.md) type — it is the fastest way to store, search and return JSON — and index typed sub-field extractions such as `(doc['title']::VARCHAR)`.

The dedicated **[Indexing JSON](./json.md)** page covers the full treatment:

- [converting JSON to `VARIANT`](./json.md#converting-json-to-variant) and [how each sub-field type is indexed](./json.md#how-each-sub-field-type-is-indexed),
- [nested fields](./json.md#nested-fields) and [array fields](./json.md#array-fields),
- [assigning a tokenizer per sub-field](./json.md#assigning-a-tokenizer-per-sub-field), and
- [indexing a `JSON` column as `VARIANT`](./json.md#indexing-a-json-column-as-variant) without changing the column type.

## Indexing arrays

A `TEXT[]` / `VARCHAR[]` column is indexed element-by-element: a row matches if **any** element matches. Tokenization and the operator class apply to each element just as for a scalar text column:

<SqlLogicTest id="sql/indexes/inverted/modeling/example_004" />

## Sizing and cost

The index only stores what you ask it to — keep it lean:

- **Feature flags** (`frequency`, `position`, `offset`, `norm`) each enlarge the index. Enable only what your queries need — see [Text Analysis](./text-analysis.md#token-positions-and-feature-flags).
- **`INCLUDE` codecs**: each `INCLUDE`d column can set a `compression` codec (`uncompressed`, `bitpacking`, `alp`, `rle`, `fsst`).
- **Vector columns**: `nlist` / `nlist_factor` trade build time against query precision, and `quant` trades index size against recall (recoverable with `sdb_rerank_factor`) — see [Vector Search](./vector-search.md).
- **Partial indexes**: a `WHERE <predicate>` on `CREATE INDEX` keeps only matching rows in the index — see [Partial indexes](../../statements/create_index/inverted.md#partial-indexes).
- Prefer `INCLUDE` over post-hoc materialization for columns you routinely return.

## Limitations

- Composite `ROW(...)` / `STRUCT` columns cannot be indexed; list the columns individually instead.
- The same indexed expression cannot be listed twice with different dictionaries.
- `HUGEINT`, `DECIMAL`, `UUID` and `INTERVAL` columns cannot be indexed.

## See also

- [Inverted Index](./index.md) · [Text Analysis](./text-analysis.md)
- [`CREATE INDEX … USING inverted`](../../statements/create_index/inverted.md) — full syntax
- [`VARIANT` type](../../data_types/variant.md) · [JSON overview](../../../data_import_and_export/json/overview.md)
- [Indexing Views](./views.md) — indexing expressions over external/view data
