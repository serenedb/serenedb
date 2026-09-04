---
title: Faceted Search & Term Dictionary
sidebar_label: Term Dictionary
sidebar_position: 2
---

<!-- markdownlint-disable MD001 -->

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Faceted navigation, distinct-value lists, autocomplete and min/max all ask one question: what values does this field hold and how are they distributed? The `ts_dict_*` aggregates answer it by reading a field's term dictionary straight from an [inverted index](../../indexes/inverted/index.md), with no document scan and no postings, so facet counts over indexed text cost about as much as walking the dictionary. They run against the index relation (`FROM my_index`), and for keyword-analyzed columns the optimizer also serves plain `count(DISTINCT)`, `min`, `max`, `array_agg(DISTINCT)` and `GROUP BY` facets from the same dictionary [automatically](#implicit-rewrites).

For task-oriented walkthroughs see the [Faceted Search](../../../cookbook/search/faceted-search.md), [Autocomplete](../../../cookbook/search/autocomplete.md) and [Spell Correction](../../../cookbook/search/spell-correction.md) cookbook recipes.

:::note Inverted index only
The `ts_dict_*` aggregates read an inverted index's on-disk dictionary, so they only work over an inverted index relation (`FROM my_index`). Point one at a base table or any other relation and the query fails with `ts_dict_agg() requires an inverted index scan in the same sub-query`.
:::

## Setup {#setup}

<details>
<summary>The examples on this page share one dataset. Expand to see the schema and sample data.</summary>

<SqlLogicTest id="sql/functions/term_dictionary/setup" />

`cat` and `promo` are keyword columns (the whole value is one term, the default when a column is listed with no dictionary), `body` uses a text dictionary (terms are lowercased tokens). The index options disable background maintenance so the examples control visibility explicitly with `VACUUM (REFRESH_TABLE)`.

</details>

## Dictionary aggregates {#dictionary-aggregates}

| Function | Description |
| :--- | :--- |
| [`ts_dict_agg(column)`](#ts_dict_agg) | `LIST(VARCHAR)` of the field's terms, in byte order per segment. |
| [`ts_dict_raw_agg(column)`](#ts_dict_agg) | `LIST(BLOB)` of the raw term bytes. |
| [`ts_dict_count(column)`](#ts_dict_agg) | `LIST(INTEGER)`: live documents per term, aligned with `ts_dict_agg`. |
| [`ts_dict_freq(column)`](#ts_dict_agg) | `LIST(BIGINT)`: total term occurrences, aligned. Needs `frequency = true` on the dictionary. |
| [`ts_dict_score(column)`](#ts_dict_agg) | `LIST(FLOAT)`: per-term score of the driving `WHERE` acceptor — the fuzzy similarity under `ts_levenshtein`, `1` otherwise. |
| [`ts_dict_min(column)`](#ts_dict_min) | The smallest live term. |
| [`ts_dict_max(column)`](#ts_dict_min) | The greatest live term. |

#### `ts_dict_agg` and the aligned lists {#ts_dict_agg}

All list aggregates over the same column are positionally aligned, so `unnest` zips them into rows.

<SqlLogicTest id="sql/functions/term_dictionary/aligned_lists" />

**How it works.** The scan streams each segment's dictionary as rows and a `GROUP BY` injected by the optimizer merges them: counts and frequencies sum across segments, a term present in several segments appears once. Output order is unspecified — sort or `list_sort` when order matters.

#### `ts_dict_min`, `ts_dict_max` {#ts_dict_min}

Scalar forms of the same read. A field whose only consumers are `min`/`max` never enumerates the dictionary: `min` stops at the first live term per segment and `max` seeks directly to a clean segment's greatest term.

<SqlLogicTest id="sql/functions/term_dictionary/min_max" />

## Filtering the dictionary {#where}

A `WHERE` clause splits by what each conjunct means:

- **Term matching** — comparisons on the enumerated field (`=`, `IN`, `LIKE 'x%'`, `BETWEEN`, range comparisons and boolean combinations of them) select terms directly and push into the scan as one fused filter tree: the most selective seekable acceptor drives the enumeration, automaton-expressible acceptors fuse with it into one product automaton pruning the dictionary, disjunctions union into one automaton, and the rest are checked per emitted term. On a keyword column this is also exactly document filtering, since each document carries one term.
- **Document filtering** — any [`@@ ts_*`](./full-text.md#tsquery-constructors) matcher on a tokenized column, and conditions on *other* indexed columns, filter documents: the aggregate returns **all terms of matching documents** with counts over that document set. `ts_dict_agg(cat) ... WHERE body @@ ts_starts_with('err')` is facet counting: category terms over the documents that match. The filter executes once per segment; each candidate term's postings are intersected with the cached result.
- **Scalar predicates** on the enumerated field the index cannot claim (`length(col) = 5`, expressions over the term text) post-filter the emitted term rows.

Filtering the *emitted terms* by arbitrary conditions belongs to the outer query (or a `HAVING` on the term key): `SELECT t FROM (SELECT unnest(ts_dict_agg(body)) AS t FROM idx) sub WHERE t LIKE 'ap%'` pushes down into the enumeration the same way as a claimed term acceptor.

<SqlLogicTest id="sql/functions/term_dictionary/where_predicates" />

`EXPLAIN` shows the split: the whole claimed tree renders as `Filter:` inside the `IRESEARCH_SCAN` box and scalar post-filters as a `FILTER` node above the scan.

:::note Scores follow the driver
`ts_dict_score` reflects the driving acceptor only, so scores need a term-driving acceptor — a comparison or a matcher on a keyword column, not a `@@ ts_*` document filter over a tokenized column. In `cat @@ ts_starts_with('p') AND cat @@ ts_levenshtein('phon', 2)` the prefix drives, so every score is `1`; swap the query so the fuzzy matcher drives and the scores become similarities.
:::

:::note The fuzzy expansion cap follows the field
A [`ts_levenshtein`](./full-text.md#ts_levenshtein) predicate on the enumerated field is exempt from [`sdb_levenshtein_max_terms`](../../indexes/inverted/maintenance.md#session-settings), because there the terms are the result. One that filters documents keeps the cap:

```sql
SELECT unnest(ts_dict_agg(cat)) FROM idx
WHERE cat @@ ts_levenshtein('phon', 2)      -- enumerates: uncapped
  AND body @@ ts_levenshtein('quikc', 2);   -- filters documents: capped
```

Since the aggregate returns terms of *matching* documents, capping the second predicate also narrows the terms returned.
:::

## Standard SQL served from the dictionary {#implicit-rewrites}

For keyword-analyzed columns on the index relation the optimizer rewrites ordinary aggregates onto the dictionary — no `ts_dict_*` spelling required:

| Query shape | Requirement |
| :--- | :--- |
| `count(DISTINCT col)`, `min(col)`, `max(col)` | keyword column |
| `array_agg(DISTINCT col)` | keyword column, `NOT NULL` |
| `SELECT col, count(*) ... GROUP BY col` (facets) | keyword column |
| `SELECT col ... GROUP BY col` | keyword column |
| `GROUP BY GROUPING SETS ((a), (b), ...)` | distinct single-column sets over `NOT NULL` keyword columns |

The whole aggregate node must be servable (mixing in `count(*)` without a `GROUP BY`, `sum(id)` or a second group key falls back to the document scan, with unchanged results) and any `WHERE` must reference the grouped column only. `EXPLAIN` shows `TsDict:` on the scan when the rewrite fired.

`GROUPING SETS` of single-column sets convert together, counting every dimension's marginal in one dictionary pass — the [faceted sidebar](../../../cookbook/search/faceted-search.md#count-every-dimension-in-one-query) shape. Combining columns inside one set (`GROUP BY a, b`) asks for the cross-product, which the per-column dictionaries cannot serve, so it stays a document scan.

<SqlLogicTest id="sql/functions/term_dictionary/facets" />

Facets over a nullable column include the `NULL` group, synthesized from the column's null-marker field — no `NOT NULL` constraint needed. A nullable facet only converts in the bare shape: with a `WHERE` clause or `count(col)` it falls back to the document scan, which handles the NULL semantics.

<SqlLogicTest id="sql/functions/term_dictionary/facets_null" />

## Consistency under writes {#consistency}

The index relation reflects the last [refresh](../../indexes/inverted/maintenance.md) minus every deleted document: inserts become visible at the next refresh, deletes apply immediately. Terms whose documents are all deleted are never returned and `ts_dict_count` always counts live documents, even before the segment is compacted.

<SqlLogicTest id="sql/functions/term_dictionary/deletes" />

:::note ts_dict_freq is an index statistic
Like Lucene's `docFreq`, `ts_dict_freq` keeps counting occurrences from deleted documents until compaction rewrites the segment (`VACUUM (COMPACT_TABLE)` or the background task). Everything else on this page is exact.
:::
