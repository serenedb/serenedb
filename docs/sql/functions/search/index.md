---
title: Search Functions
sidebar_label: Overview
---

These functions operate on an [inverted index](../../indexes/inverted/index.md) — SereneDB's index for full-text, vector and geospatial search. You build a query (most often with the [`@@`](./full-text.md#match-operator) operator and a [`TSQUERY`](../../data_types/tsquery.md)), optionally rank matches with a relevance scorer and highlight them. The reference is grouped by purpose:

| Page | Functions |
| :--- | :--- |
| [Full-Text](./full-text.md) | The `@@` match operator, `TSQUERY` constructors and operators, PostgreSQL-compatible parsers, convenience predicates and `ts_lexize` |
| [Term Dictionary](./term-dictionary.md) | Facet counts, distinct values, autocomplete and min/max via the `ts_dict_*` aggregates read from the index dictionary |
| [Vector](../vector.md) | `l2_distance`, `cosine_distance`, `l1_distance` and the `<->` operator |
| [Geo](./geo.md) | `ST_Intersects`, `ST_Contains`, `ST_Distance_Between`, `ST_Distance_Centroid` |
| [Scoring](./scoring.md) | `BM25`, `TFIDF` and the other relevance scorers |
| [Highlighting](./highlighting.md) | `ts_highlight`, `ts_offsets` |

For task-oriented guides see [Full-Text Search](../../indexes/inverted/full-text-search.md), [Vector Search](../../indexes/inverted/vector-search.md) and [Geospatial Search](../../indexes/inverted/geospatial-search.md).
