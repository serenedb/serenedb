---
title: CREATE INDEX … USING inverted
sidebar_label: Inverted Indexes
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './inverted_diagram.js';

Adding `USING inverted` to [`CREATE INDEX`](./index.md) builds an [inverted index](../../indexes/inverted/index.md), which powers [full-text](../../indexes/inverted/full-text-search.md), [vector](../../indexes/inverted/vector-search.md) and [geospatial](../../indexes/inverted/geospatial-search.md) search. This page is the syntax reference; for a conceptual guide with query examples, see [Inverted Index](../../indexes/inverted/index.md).

## Column specification

Each entry in the `USING inverted (...)` list is an **indexed column**.

- **Text and scalar columns** may name a [text search dictionary](../create_text_search_dictionary/index.md) (the analyzer). With no dictionary the column is indexed verbatim — one token per value — giving exact, case-sensitive matching suitable for identifiers, tags and categories. Numeric and temporal columns are indexed for [exact and range](../../indexes/inverted/full-text-search.md#range-queries) queries.
- **Vector columns** use `ivf (...)` or `hnsw (...)` to build an [IVF or HNSW](../../indexes/inverted/vector-search.md) index. `metric` is required; the other options, among them the vector compression `quant`, are listed on the [Vector Search](../../indexes/inverted/vector-search.md) page.
- **Expressions** — an entry may be a parenthesized expression over one or more columns, e.g. `(lower(name)) my_dict` or `(price * 110 / 100)`. The query must use the identical expression. Aggregates, window functions, subqueries, volatile functions (e.g. `random()`) and functions that read the clock, the session or the catalog (e.g. `now()`, `current_user`) are rejected.
- **JSON** — index a JSON extraction expression such as `(doc ->> 'host') my_dict`, optionally with a dictionary; this indexes exactly the sub-field you search. (See also the [`VARIANT`](../../data_types/variant.md) type for storing typed payloads.)
- **Generated columns** — index a [generated column](./index.md) the same as any other column (index the column itself, not its defining expression).
- **Array columns** — a `TEXT[]` / `VARCHAR[]` column is indexed element-by-element; a row matches if any element matches.
- **`INET` and `CIDR` columns** take no dictionary. The index answers `=`, `IN`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `<<=`, `>>=`, `<<`, `>>` and `&&`; see [Inet](../../data_types/inet.md#indexing).

Columns of type `UBIGINT`, `HUGEINT`, `UHUGEINT`, `DECIMAL`, `BIGNUM`, `UUID`, `INTERVAL`, `BIT`, `ENUM`, `MAP`, `UNION`, `VARIANT` or `STRUCT` (`ROW(...)`) cannot be indexed, and neither can arrays of these types, arrays of arrays or `GEOMETRY` arrays; such a column fails with `Column '<name>' has unsupported type <type> and can not be indexed`. A `GEOMETRY` column is indexed only with a [geo dictionary](../../indexes/inverted/geospatial-search.md). `INCLUDE` stores values of every type. A column or expression is listed once as an indexed key, with one dictionary or none; besides that, only `included()` can list it again. For a guided treatment of these choices, see [What to Index](../../indexes/inverted/modeling.md).

### Feature flags

A column takes the **feature flags** of its text search dictionary, which control what the index records; `CREATE INDEX` has no per-column flags, so columns that need different flags use different dictionaries. The flags are off by default; enable only what your queries need.

| Flag | Default | Enables |
| :--- | :--- | :--- |
| `frequency` | `false` | Term frequency — lets [relevance scoring](../../indexes/inverted/ranking.md) rank by how often terms occur; without it every match of a term scores the same |
| `position` | `false` | Term positions — answer [phrase / proximity](../../indexes/inverted/full-text-search.md#phrase-search) queries from the index alone; without them a phrase needs the column's text in the index ([phrases without positions](../../indexes/inverted/full-text-search.md#phrases-without-positions)) |
| `offset` | `false` | Byte offsets — stored for [highlighting](../../indexes/inverted/full-text-search.md#highlighting); without them `ts_offsets` recomputes them for each matching row |
| `norm` | `false` | The length-normalization factor used by some scorers |

They are set in the dictionary's `WITH` clause; see the [feature flags](../create_text_search_dictionary/index.md#feature-flags) reference.

## `INCLUDE` columns

Columns in `INCLUDE (...)` are **stored but not indexed**: they cannot be searched, but a query that selects from the index can return them without a separate base-table lookup. A column listed both in `USING inverted (...)` and in `INCLUDE (...)` is searchable and stored, which lets [phrase queries](../../indexes/inverted/full-text-search.md#phrases-without-positions) on it work without `position`; an indexed expression gets the same when it is listed a second time with `included()` to store its value, as in `USING inverted (id, (lower(body)) words, (lower(body)) included())`. Each may set a storage `compression` codec: `auto`, the default, or one of `uncompressed`, `rle`, `bitpacking`, `zstd`, `alp`, `alprd`, `roaring` or `dict_fsst`, for example `INCLUDE (payload included (compression = 'alp'))`.

## Index options

The trailing `WITH (...)` clause sets index-level options.

| Option | Default | Description |
| :--- | :--- | :--- |
| `refresh_interval` | `1000` | Background refresh interval in milliseconds; `0` disables it |
| `compaction_interval` | `1000` | Background compaction interval in milliseconds; `0` disables it |
| `cleanup_interval_step` | `1` | Automatic refreshes between cleanup passes, which delete the files no segment uses anymore; `0` disables the periodic passes, leaving cleanup to background compaction |
| `row_group_size` | `122880` | Row-group size for stored (`INCLUDE`d) columns and for norm columns; there is no separate norm setting. Must be a multiple of the vector size (2048). A scan claims one row group per worker, so a smaller value spreads one segment over more threads at the cost of more per-unit setup |
| `optimize_top_k` | — | Scorer expression enabling top-K (WAND) pruning, e.g. `'bm25(1.2, 0.75)'` |

An index on a search table (`CREATE TABLE … WITH (storage = 'search')`) takes none of these options. It has no storage or background tasks of its own: the table's options run refresh and compaction for every index on it, and the table keeps the score bounds that `optimize_top_k` prunes with. So `CREATE INDEX` rejects all five on a search table, with an error that names the option, such as `row_group_size is a table option on a search-backed table`. Set them on the table instead; see [Background intervals](../../indexes/inverted/maintenance.md#background-intervals).

## Partial indexes

A trailing `WHERE <predicate>` builds a **partial index** containing only the rows that satisfy the predicate:

```sql
CREATE INDEX recent_errors ON logs USING inverted(message log_dict)
  WHERE level = 'error';
```

The predicate is a boolean expression over the table's columns; a row whose predicate evaluates to `NULL` is treated as non-matching, following PostgreSQL semantics. DML keeps membership current — rows enter and leave the index as updates move them across the predicate boundary — and queries against the index only ever see matching rows.

A constant predicate, one that reads no column and calls no function whose result can change (such as `now()`), is evaluated once by `CREATE INDEX`, the way a query's `WHERE` evaluates it. `WHERE 'true'` or `WHERE 1 + 1 = 2` builds an ordinary index, which `pg_indexes` shows without a `WHERE`; `WHERE 'f'` is stored as `WHERE false` and the index stays empty. A string that is not a boolean fails as in a query: `WHERE 'abc'` raises `Could not convert string 'abc' to BOOL`. This holds for plain indexes too.

Partial indexes are an inverted-index feature. A plain (ART) `CREATE INDEX … WHERE …` is accepted, but the predicate filters only the rows present when the index is built; see [PostgreSQL compatibility](../../../compatibility/core-sql-compatibility.md#indexes).

An index on a search table can't be partial: there, `CREATE INDEX … WHERE …` fails with `partial indexes are not supported on a search-backed table`.

## Indexing tables and views

An inverted index can be built over a base table or a view (including a view over `read_parquet`/`read_csv` on local disk or S3). The index derives each row's identity automatically from the view's source; there is no `pk` option, and only attached external databases accept an explicit key through `key_columns` (see [Row identity](../../indexes/inverted/views.md#row-identity)). The postings are a snapshot of the source, refreshed by `REINDEX INDEX` or the `reindex_interval` option (see [Refreshing the index](../../indexes/inverted/views.md#refreshing-the-index)). See also [Indexing a table vs. a view](../../indexes/inverted/index.md#indexing-a-table-or-a-view).

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

A `column_spec` is an indexed column with an optional analyzer ([dictionary](../create_text_search_dictionary/index.md)) or, for vectors, an `ivf` or `hnsw` configuration:

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

An `include_spec` is a stored-only column with an optional compression codec:

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />
