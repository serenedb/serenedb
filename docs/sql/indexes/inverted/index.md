---
title: Inverted Index
sidebar_position: 1
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

An **inverted index** maps each token back to the rows that contain it. Instead of scanning every row, the engine looks up the query's tokens and jumps straight to the matching rows — which is what makes full-text search fast. Beyond text, the same index also powers [vector / approximate-nearest-neighbor search](./vector-search.md) and [geospatial search](./geospatial-search.md).

```mermaid
flowchart LR
    docs["rows<br/>1 · quick brown fox<br/>2 · quick red fox"] -->|analyze| post
    subgraph post["inverted index · term → row ids"]
        direction TB
        pq["quick → 1, 2"]
        pb["brown → 1"]
        pr["red → 2"]
        pf["fox → 1, 2"]
    end
    q["query · fox"] -->|same analysis| pf
    pf -->|matches| res["rows 1, 2"]
```

The guiding principle is that **the same analysis is applied at index time and at query time** (see [Text Analysis](./text-analysis.md)). The [text search dictionary](./text-analysis.md) attached to a column decides how its text is split into tokens and normalized; that identical pipeline runs on the query, so the search terms always match the stored tokens even when the surface forms differ.

## Choosing a search type

One inverted index can serve several kinds of search, each documented on its own page:

| You want to… | Use | Page |
|---|---|---|
| Match natural-language text, rank by relevance | Full-text search (`@@`, BM25) | [Full-Text Search](./full-text-search.md) · [Ranking](./ranking.md) |
| Find nearest vectors / semantic similarity | Vector / ANN search (IVF, HNSW) | [Vector Search](./vector-search.md) |
| Combine a lexical signal with a vector signal | Hybrid search | [Hybrid Search](./hybrid-search.md) |
| Query points, shapes and distances | Geospatial search (`ST_*`) | [Geospatial Search](./geospatial-search.md) |
| Match exact values, ids, tags, enums | Verbatim columns + range predicates | [Full-Text Search](./full-text-search.md#range-queries) |

## Creating an inverted index

An inverted index is created by adding `USING inverted` to [`CREATE INDEX`](../../statements/create_index/inverted.md). The example below indexes two text columns with a dictionary, then queries the index by name:

<SqlLogicTest id="sql/indexes/inverted/index/example_001" />

This page covers the concepts. For the complete `CREATE INDEX … USING inverted` grammar — every operator-class option, `INCLUDE` codec, index `WITH` option and supported column type — see the [statement reference](../../statements/create_index/inverted.md).

A trailing `WHERE <predicate>` builds a [partial index](../../statements/create_index/inverted.md#partial-indexes) that contains only the matching rows; DML keeps membership current as rows cross the predicate boundary.

<DocCallout type="tip">

After loading data, run `VACUUM (REFRESH_TABLE) table_name` to make newly inserted rows visible to the index immediately. A background thread otherwise refreshes the index on an interval — see [Maintenance](./maintenance.md).

</DocCallout>

## Operator classes and fields

Each indexed column carries its **own operator class** — the `column [dictionary]` form in the column list tells the index how to analyze and store *that* column. Only the built-in operator classes `ivf (...)`, `hnsw (...)` and `included (...)` take options, in parentheses after the name; a column's [feature flags](./text-analysis.md#token-positions-and-feature-flags) come from its dictionary. Different columns in the same index can use different operator classes:

- a column with a **dictionary** is analyzed into tokens (full-text);
- a column with **no dictionary** is indexed verbatim — one token per value — giving exact, case-sensitive matching, ideal for ids, tags and enum-like categories;
- a numeric or temporal column is indexed for [exact and range queries](./full-text-search.md#range-queries);
- an `INET` or `CIDR` column, indexed with no dictionary, serves equality, range and network-containment predicates (see [`INET` indexing](../../data_types/inet.md#indexing));
- a `FLOAT[N]` column with `ivf (...)` or `hnsw (...)` is indexed for [vector search](./vector-search.md), and a `JSON`/`GEOMETRY` column with a geo dictionary for [geospatial search](./geospatial-search.md).

The example below puts an analyzed column (`name`) and a verbatim column (`category`) in one index:

<SqlLogicTest id="sql/indexes/inverted/index/example_003" />

Because operator classes are per-column, a single index can mix full-text, verbatim and numeric (and vector and geo) columns, and a query can constrain several of them at once:

<SqlLogicTest id="sql/indexes/inverted/index/example_006" />

<DocCallout type="tip">

**Field order does not matter.** This is a key difference from a regular composite (ART) index, where column order is significant and only a query constraining a *leading prefix* of the columns can use the index. An inverted index indexes each field independently: the order of fields in `USING inverted (...)` carries no meaning, and a query may constrain **any subset of the indexed fields, in any combination**, and still be served entirely by the index.

</DocCallout>

<DocCallout type="tip">

**Analyzed vs. verbatim.** A column *with* a dictionary is tokenized and normalized, so `'Running Shoes'` matches `shoes`. A column *without* a dictionary stores the value verbatim, so it only matches the exact string. Choose verbatim for identifiers, codes and categories; choose a dictionary for natural-language text. See [Text Analysis](./text-analysis.md).

</DocCallout>

## Querying an inverted index

An inverted index behaves as a **queryable relation**: full-text, verbatim, range and geospatial predicates are issued by selecting **from the index by name**, with the [`@@` match operator](./full-text-search.md) on the indexed column.

```mermaid
flowchart LR
    q["col @@ 'fox'"] -->|index analyzes 'fox'| idx[("articles_idx")]
    idx --> ids["row ids"]
    ids --> rows["matching rows + INCLUDEd columns"]
```

```sql
SELECT id, title
FROM articles_idx          -- the index, by name
WHERE body @@ 'search';    -- @@ match on an indexed column
```

On a regular table, a `TSQUERY` predicate (`@@`, `ST_*`, range functions) only resolves **against an inverted-indexed column inside the index relation** — issuing it against the table raises an error. A table created `WITH (storage = 'search')` also answers `@@` on itself, for the columns its inverted indexes cover. [Vector kNN](./vector-search.md) reads the index the same way: `ORDER BY emb <-> $q LIMIT k` against the base table still answers, but from a full scan of the table.

<DocCallout type="tip">

**Filtering vs. ranking.** `WHERE col @@ query` is a yes/no filter (does the row match?). Relevance is separate: order matches with a scorer such as `ORDER BY BM25(idx.tableoid) DESC` — see [Ranking](./ranking.md).

</DocCallout>

## Indexed vs. `INCLUDE`d columns

Columns in the `USING inverted (...)` list are **indexed** — searchable with `@@`. Columns in the `INCLUDE (...)` list are **stored but not indexed**: they cannot be searched, but they can be returned by a query against the index, avoiding a separate lookup against the base table.

<SqlLogicTest id="sql/indexes/inverted/index/example_002" />

See [What to Index](./modeling.md) for choosing indexed vs. `INCLUDE`d columns, indexing expressions and JSON, and sizing trade-offs.

## Indexing a table or a view

An inverted index can be built over a base **table** or a **view**:

- **Base tables** need no `PRIMARY KEY`: the index identifies each row by the table's hidden row id, and the background refresh tracks inserts, updates and deletes.
- **Views** let you index data the database does not own a primary copy of — including [external Parquet/CSV/JSON files](./external-data.md) on disk or S3. A view-backed index holds a snapshot of its source that `REINDEX INDEX` or the `reindex_interval` option refreshes; see [Refreshing the index](./views.md#refreshing-the-index).

<SqlLogicTest id="sql/indexes/inverted/index/example_005" />

See [Indexing Views](./views.md) and [Indexing External Data](./external-data.md).

## Lifecycle

An inverted index is **eventually consistent**: after a write, the new rows become searchable once the index is refreshed — immediately with `VACUUM (REFRESH_TABLE)`, or automatically by the background refresh thread. Compaction merges segments in the background. See [Maintenance & Introspection](./maintenance.md) for the full refresh / compaction / statistics model and how to inspect an index.

## Limitations

- Indexed expressions cannot contain aggregates, window functions, subqueries, volatile functions (e.g. `random()`) or functions that read the clock, the session or the catalog (e.g. `now()`, `current_user`).
- Columns of type `UBIGINT`, `HUGEINT`, `UHUGEINT`, `DECIMAL`, `BIGNUM`, `UUID`, `INTERVAL`, `BIT`, `ENUM`, `MAP`, `UNION`, `VARIANT` or `STRUCT` (`ROW(...)`) cannot be searchable keys, and neither can arrays of these types, arrays of arrays or `GEOMETRY` arrays. A `GEOMETRY` column is a key only with a [geo dictionary](./geospatial-search.md). `INCLUDE` stores values of every one of these types.
- A column or expression is listed once as a searchable key, with one dictionary or none; besides that, only `included()` can list it again.

## See also

- [Text Analysis](./text-analysis.md) · [What to Index](./modeling.md)
- [Full-Text Search](./full-text-search.md) · [Ranking](./ranking.md) · [Vector Search](./vector-search.md) · [Hybrid Search](./hybrid-search.md) · [Geospatial Search](./geospatial-search.md)
- [Indexing Views](./views.md) · [Indexing External Data](./external-data.md) · [Maintenance & Introspection](./maintenance.md)
- [`CREATE INDEX … USING inverted`](../../statements/create_index/inverted.md) — full syntax reference
- [Migrating from Elasticsearch](./migrating-from-elasticsearch.md) · [Search cookbook](../../../cookbook/search/index.md)
