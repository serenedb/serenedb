---
title: Indexing Views
sidebar_position: 31
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Indexing Views

An [inverted index](../../sql/indexes/inverted/index.md) in SereneDB is a named relation, and you can build it on a [view](../../sql/indexes/inverted/views.md) just as easily as on a table. That is the hook for searching anything a view can express: a projection of a table, a join, a union or a pile of [Parquet and CSV files](./indexing-external-data.md). You point the index at the view and query the index relation, and every search feature works the same as on a table.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/indexing-views/setup" />

</details>

## Index a view and search it

Build the index on the view with `CREATE INDEX ... ON v_docs USING inverted(...)`, then query the index relation `v_docs_idx`. Full-text `@@` and [BM25](./ranking.md) ranking work exactly as they do on a table-backed index.

<SqlLogicTest id="cookbook/search/indexing-views/example_001" />

## Facet through it

`GROUP BY` and the [`ts_dict_*`](../../sql/functions/search/term-dictionary.md) aggregates read that same index, so faceting a search over a view is no different from faceting one over a table.

<SqlLogicTest id="cookbook/search/indexing-views/example_002" />

## A reusable saved search

Wrap a search in a view and it becomes a named query you point applications at. Callers add their own predicates on top and they fuse into the same index scan, so `recent_hits` filtered by category stays a single indexed lookup, not a scan over the results. Re-point it with `CREATE OR REPLACE VIEW` when you want the name to follow a new search, which is how you stand in for an index alias.

<SqlLogicTest id="cookbook/search/indexing-views/example_003" />

## No materialized views

SereneDB has no `MATERIALIZED VIEW`. You do not need one here: an index built on a view over a base table already holds a postings snapshot that only moves when you refresh or rebuild, which covers the usual reason to reach for a materialized view in the first place.

## See also

- [Indexing Views](../../sql/indexes/inverted/views.md): the reference on building an index over a view, including the `pk` rule for union and join views
- [Indexing External Data](./indexing-external-data.md): the real payoff, an index over a view of Parquet, CSV or S3 files
- [Migrating from Elasticsearch](../../sql/indexes/inverted/migrating-from-elasticsearch.md): where index aliases map to views
