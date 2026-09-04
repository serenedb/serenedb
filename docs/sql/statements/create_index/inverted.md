---
title: CREATE INDEX … USING inverted
sidebar_label: Inverted Indexes
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './inverted_diagram.js';

Adding `USING inverted` to [`CREATE INDEX`](./index.md) builds an [inverted index](../../indexes/inverted/index.md), which powers [full-text](../../indexes/inverted/full-text-search.md), [vector](../../indexes/inverted/vector-search.md) and [geospatial](../../indexes/inverted/geospatial-search.md) search. This page is the syntax reference; for a conceptual guide with query examples, see [Inverted Index](../../indexes/inverted/index.md).

## Column specification

Each entry in the `USING inverted (...)` list is an **indexed column**.

- **Text and scalar columns** may name a [text search dictionary](../create_text_search_dictionary/index.md) (the analyzer). With no dictionary the column is indexed verbatim — one token per value — giving exact, case-sensitive matching suitable for identifiers, tags and categories. Numeric and temporal columns are indexed for [exact and range](../../indexes/inverted/full-text-search.md#range-queries) queries.
- **Vector columns** use `ivf (...)` to build an [IVF](../../indexes/inverted/vector-search.md) index. `metric` is required; `nlist` / `nlist_factor` size the coarse clustering and `quant` enables optional compression (`sq8`, `sq4`, `pq`, `rabitq`).
- **Expressions** — an entry may be a parenthesized expression over one or more columns, e.g. `(lower(name)) my_dict` or `(price * 110 / 100)`. The query must use the identical expression. Aggregates, subqueries and volatile functions (e.g. `random()`) are rejected.
- **JSON** — index a JSON extraction expression such as `(doc ->> 'host') my_dict`, optionally with a dictionary; this indexes exactly the sub-field you search. (See also the [`VARIANT`](../../data_types/variant.md) type for storing typed payloads.)
- **Generated columns** — index a [generated column](./index.md) the same as any other column (index the column itself, not its defining expression).
- **Array columns** — a `TEXT[]` / `VARCHAR[]` column is indexed element-by-element; a row matches if any element matches.

`HUGEINT`, `DECIMAL`, `UUID` and `INTERVAL` columns cannot be indexed. Composite `ROW(...)` columns are not supported. For a guided treatment of these choices, see [What to Index](../../indexes/inverted/modeling.md).

### Operator-class options (feature flags)

A column's trailing `WITH (...)` sets per-column **feature flags** controlling what the index records. They are off by default; enable only what your queries need.

| Flag | Default | Enables |
| :--- | :--- | :--- |
| `frequency` | `false` | Term frequency — required for [relevance scoring](../../indexes/inverted/ranking.md) |
| `position` | `false` | Term positions — required for [phrase / proximity](../../indexes/inverted/full-text-search.md#phrase-search) queries |
| `offset` | `false` | Character offsets — required for [highlighting](../../indexes/inverted/full-text-search.md#highlighting) |
| `norm` | `false` | The length-normalization factor used by some scorers |

The same flags can be set on the dictionary itself, in which case every column using it inherits them. See the [feature flags](../create_text_search_dictionary/index.md#feature-flags) reference.

## `INCLUDE` columns

Columns in `INCLUDE (...)` are **stored but not indexed**: they cannot be searched, but a query that selects from the index can return them without a separate base-table lookup. Each may set a storage `compression` codec — one of `uncompressed`, `bitpacking`, `alp`, `rle` or `fsst` — for example `INCLUDE (payload included (compression = 'alp'))`.

## Index options

The trailing `WITH (...)` clause sets index-level options.

| Option | Default | Description |
| :--- | :--- | :--- |
| `refresh_interval` | `1000` | Background refresh interval in milliseconds; `0` disables it |
| `compaction_interval` | `1000` | Background compaction interval in milliseconds; `0` disables it |
| `cleanup_interval_step` | `1` | Commit ticks between cleanup passes; `0` disables it |
| `row_group_size` | `122880` | Row-group size for stored (`INCLUDE`d) columns |
| `norm_row_group_size` | `122880` | Row-group size for norm columns when `norm` is enabled |
| `optimize_top_k` | — | Scorer expression enabling top-K (WAND) pruning, e.g. `'bm25(1.2, 0.75)'` |
| `pk` | auto | Primary-key column to use as row identity when indexing a [view](../../indexes/inverted/index.md#indexing-a-table-or-a-view) |

## Partial indexes

A trailing `WHERE <predicate>` builds a **partial index** containing only the rows that satisfy the predicate:

```sql
CREATE INDEX recent_errors ON logs USING inverted(message log_dict)
  WHERE level = 'error';
```

The predicate is a boolean expression over the table's columns; a row whose predicate evaluates to `NULL` is treated as non-matching, following PostgreSQL semantics. DML keeps membership current — rows enter and leave the index as updates move them across the predicate boundary — and queries against the index only ever see matching rows.

Partial indexes are an inverted-index feature: a plain (ART) `CREATE INDEX … WHERE …` is rejected.

## Indexing tables and views

An inverted index can be built over a base table or a view (including a view over `read_parquet`/`read_csv` on local disk or S3). A view has no primary key, so the index materializes its columns at build time and resolves a row identity — automatically for base-table and fast-path-reader views, or via `WITH (pk = '...')` for generic views. See [Indexing a table vs. a view](../../indexes/inverted/index.md#indexing-a-table-or-a-view).

## Examples

Create a dictionary, index two text columns with it, then query the index:

<SqlLogicTest id="sql/indexes/inverted/index/example_001" />

Store an extra column alongside the index with `INCLUDE` so it can be returned without touching the base table:

<SqlLogicTest id="sql/indexes/inverted/index/example_002" />

## See also

- [Inverted Index](../../indexes/inverted/index.md) — conceptual guide
- [CREATE INDEX](./index.md) — the general statement (ART)
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md) — analyzer reference
- [Full-Text Search Functions](../../functions/search/full-text.md)

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

A `column_spec` is an indexed column with an optional analyzer ([dictionary](../create_text_search_dictionary/index.md)) or, for vectors, an `ivf` configuration:

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

An `include_spec` is a stored-only column with an optional compression codec:

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />
