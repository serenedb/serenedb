---
title: Full-Text Search
sidebar_position: 5
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Once a column is covered by an [inverted index](./index.md), you search it with the `@@` **match operator**. The left side is the indexed column; the right side is a query expression of type [`TSQUERY`](../../data_types/tsquery.md). Queries select **from the index by name**:

```sql
SELECT ... FROM index_name WHERE column @@ query;
```

Every query family below produces a `TSQUERY`. The simplest is a bare string literal, which is analyzed by the column's [text search dictionary](../../statements/create_text_search_dictionary/index.md) into one or more tokens; multi-token input matches any token (`OR` semantics):

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_001" />

<DocCallout type="tip">

All examples on this page use a `sentences` table whose `b` column is indexed with a lower-casing, non-stemming dictionary. For a full reference of every function and operator, see [Full-Text Search Functions](../../functions/search/full-text.md).

</DocCallout>

## Term and phrase search {#phrase-search}

[`ts_phrase`](../../functions/search/full-text.md#ts_phrase) matches a run of tokens in order. It is fastest with `position` enabled on the column ([feature flags](./text-analysis.md#token-positions-and-feature-flags)); a column without it still answers phrases when the index keeps its text, see [Phrases without positions](#phrases-without-positions):

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_002" />

The `##` operator builds a **proximity phrase** by chaining parts left to right:

- `a ## b` — `a` and `b` must be **strictly adjacent**, in order;
- `a ## N ## b` — exactly `N` tokens may sit between them;
- `a ## [min, max] ## b` — the gap may be anywhere in that range;
- the chain extends to any length — `a ## b ## c` — and each pair can carry its own gap, e.g. `a ## 1 ## b ## c`.

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_012" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_013" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_016" />

**How phrase matching works.** A phrase matches only when its tokens occur **adjacent and in order**; proximity (`##`) relaxes "adjacent" to "within a gap". Tested against the document `quick brown fox jumps over lazy dog`:

| Query | Matches? | Why |
|---|:---:|---|
| `ts_phrase('quick brown')` | ✅ | `quick`, `brown` are adjacent, in order |
| `ts_phrase('brown quick')` | ❌ | both tokens present, but not in that order |
| `ts_phrase('quick fox')` | ❌ | `brown` sits between them — not contiguous |
| `'quick' ## 1 ## 'fox'` | ✅ | proximity allows exactly one token (`brown`) between |

### What can be chained with `##` {#phrase-parts}

Each side of a `##` is a single **phrase part**. A part may be:

- a **bare word** — but only a *single* token (a multi-word string is not a phrase part; use `ts_phrase` for that), or
- one of [`ts_phrase`](../../functions/search/full-text.md#ts_phrase), [`ts_starts_with`](../../functions/search/full-text.md#ts_starts_with), [`ts_like`](../../functions/search/full-text.md#ts_like), [`ts_regexp`](../../functions/search/full-text.md#ts_regexp), [`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein), [`ts_any`](../../functions/search/full-text.md#ts_any) or [`ts_between`](../../functions/search/full-text.md#ts_between).

These part types **mix freely**: any of them can occupy any position in a single chain, in any combination, with an independent gap between any pair — for example `ts_starts_with('qu') ## 1 ## ts_any(['fox', 'dog'])` chains a prefix part, a gap and an alternatives part.

The boolean operators `&&`, `||` and `!!` are **not** phrase parts — they combine *whole* queries, not positions within a phrase. So they cannot appear directly inside a `##` chain:

| Goal | ✅ Do this | ❌ Not this |
|---|---|---|
| Alternatives at one position | `'quick' ## ts_any(['brown', 'grey'])` | `'quick' ## ('brown' \|\| 'grey')` |
| Prefix / fuzzy at a position | `'quick' ## ts_starts_with('bro')` | — |
| AND / OR / NOT *around* a phrase | `('quick' ## 'brown') && 'dog'` | `'quick' ## ('brown' && 'dog')` |

In short: build the phrase with `##` and the allowed parts, then combine the finished phrase with other queries using `&&` / `||` / `!!` on the **outside**.

### Phrases without positions {#phrases-without-positions}

A column indexed without `position` answers every phrase query when the index also keeps the column's text. A table created `WITH (storage = 'search')` keeps every column. In an inverted index, list the column in `INCLUDE` as well as in `USING inverted`:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/phrase_without_positions" />

An [indexed expression](../../statements/create_index/inverted.md) such as `(lower(body))` needs its own value in the index: list it a second time with `included()`, for example `USING inverted (id, (lower(body)) words, (lower(body)) included())`. The check then reads the stored value like a column's text, also for an expression over several columns. Keeping only the columns the expression reads in `INCLUDE` is not enough.

The index finds the rows that contain every word of the phrase. Then it reads the text of each of those rows, analyzes it with the column's dictionary and keeps the rows where the words line up. Matches and scores are the same as with `position`, for every phrase form: gaps, `[min, max]` intervals and `slop` in `ts_phrase`, `##` chains with pattern parts, `phraseto_tsquery` and the quoted phrases of `to_tsquery` and `websearch_to_tsquery`. `EXPLAIN` shows where the text is checked:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/phrase_without_positions_plan" />

- `Verify: table filter`: the text is checked last, only for the rows that pass every other condition of the query. The phrase is listed under `Table Filter`, and the `Index Filter` only looks up its words. A phrase gets this when every row has to match it and its score doesn't depend on how often it occurs: the query doesn't rank by score, ranks by a constant score or scores the phrase with [`::score('constant(1)')` or `::score(NULL)`](../../functions/search/scoring.md#score-modifier).
- `Verify: inline`: the phrase stays in the `Index Filter` and its text is checked as the index finds the rows. A query that ranks the phrase with any other score gets this, because the score counts the phrase's occurrences. So do a query ranked by vector distance, a phrase under `OR` or `NOT`, a query that calls [`ts_offsets`](../../functions/search/highlighting.md), a phrase in `ts_dict_agg`, a phrase with both `slop` and a pattern part on a dictionary that puts several tokens at one position, and a phrase with a `ts_levenshtein` part while [`sdb_levenshtein_max_terms`](./maintenance.md#session-settings) isn't `0`.

The index stays smaller without `position`, but a phrase reads more data: the fewer rows contain all of its words, the cheaper it is. A phrase of common words over long texts is where `position` pays off. Compressing the kept text, for example with `INCLUDE (body included (compression = 'zstd'))`, shrinks the index further, but every row a phrase checks is decompressed first, so phrases run several times slower.

Without `position` and without the column's text, a phrase of two or more words fails with `ts_phrase on a column without positions needs the column's text in the index`.

## Prefix, wildcard and regex

[`ts_starts_with`](../../functions/search/full-text.md#ts_starts_with) matches any token with the given prefix:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_009" />

[`ts_like`](../../functions/search/full-text.md#ts_like) matches tokens against a SQL `LIKE` pattern (`%` = any run of characters, `_` = a single character):

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_010" />

[`ts_regexp`](../../functions/search/full-text.md#ts_regexp) matches tokens against a regular expression. The default syntax is Perl-compatible (RE2); pass `'posix'` for POSIX ERE:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_011" />

**How pattern matching works.** Each pattern is tested against the indexed **tokens**, not the raw field. Against the tokens of `quick brown fox jumps over lazy dog`:

| Query | Matching token | Note |
|---|---|---|
| `ts_starts_with('qu')` | `quick` | any token with that prefix |
| `ts_like('%zy')` | `lazy` | `%` = any run of chars, `_` = one char |
| `ts_regexp('f.x')` | `fox` | RE2 by default; `'posix'` for POSIX ERE |

## Fuzzy and similarity search

[`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein) matches tokens within a given edit distance — typo-tolerant search:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_022" />

[`ts_ngram`](../../functions/search/full-text.md#ts_ngram) matches by n-gram similarity against an [n-gram-tokenized](../../functions/search/tokenizers/generate_ngrams.md) column; the optional threshold (0–1) trades precision for recall:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_023" />

**How fuzzy matching works.** Each query term matches any indexed token within the given [edit distance](https://en.wikipedia.org/wiki/Levenshtein_distance) (insertions, deletions, substitutions):

| Query | Closest token | Edit distance | `ts_levenshtein(…, 1)` |
|---|---|:---:|:---:|
| `jumxs` | `jumps` | 1 | ✅ |
| `cats` | _none within 1_ | ≥ 2 | ❌ |

See the [Fuzzy Search](../../../cookbook/search/fuzzy-search.md) recipe for a deeper walkthrough.

## Boolean composition

Build compound queries from `TSQUERY` expressions with `||` (OR), `&&` (AND) and the unary `!!` (NOT):

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_003" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_004" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_005" />

When the alternatives are a list, [`ts_any`](../../functions/search/full-text.md#ts_any) (OR) and [`ts_all`](../../functions/search/full-text.md#ts_all) (AND) are more convenient. `ts_any` takes an optional minimum number of alternatives that must match:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_006" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_007" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_008" />

[`ts_compound`](../../functions/search/full-text.md#ts_compound) builds an Elasticsearch-style boolean query from `must`, `must_not` and `should` buckets:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_021" />

## Range queries {#range-queries}

Numeric, temporal and verbatim text columns support range matching. [`ts_between`](../../functions/search/full-text.md#ts_between) takes lower and upper bounds (either may be `NULL` for unbounded) and inclusivity flags; [`ts_lt`](../../functions/search/full-text.md#ts_lt), `ts_le`, `ts_gt` and `ts_ge` are single-bound shortcuts:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_014" />

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_015" />

See the [Range Queries](../../../cookbook/search/range-queries.md) recipe for lexicographic ordering details.

## PostgreSQL-compatible query parsers

For compatibility with PostgreSQL full-text search, SereneDB accepts the familiar parser functions. They produce a `TSQUERY` from a single string. Note these are SereneDB inverted-index queries, not PG `tsvector`/`tsquery`.

- [`plainto_tsquery`](../../functions/search/full-text.md#postgresql-compatible-parsers) — tokenize and `AND` the terms:

  <SqlLogicTest id="sql/indexes/inverted/full-text-search/example_017" />

- [`phraseto_tsquery`](../../functions/search/full-text.md#postgresql-compatible-parsers) — treat the input as a phrase:

  <SqlLogicTest id="sql/indexes/inverted/full-text-search/example_018" />

- [`to_tsquery`](../../functions/search/full-text.md#to_tsquery) — parse a Lucene-style query string (`AND`/`OR`, `+`required / `-`excluded, `*` prefix, `~` fuzzy, `"phrase"`, grouping, `^` boost):

  <SqlLogicTest id="sql/indexes/inverted/full-text-search/example_019" />

- [`websearch_to_tsquery`](../../functions/search/full-text.md#postgresql-compatible-parsers) — web-search syntax, where quoted substrings are phrases and `OR` separates alternatives:

  <SqlLogicTest id="sql/indexes/inverted/full-text-search/example_020" />

## Highlighting {#highlighting}

[`ts_highlight`](../../functions/search/highlighting.md) wraps matched terms in markup. Its standalone form takes a text and an array of start/end byte offsets (such as those produced by `ts_offsets`); the default markup is `<b>...</b>`, and `StartSel`/`StopSel` options override it:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_026" />

[`ts_offsets`](../../functions/search/highlighting.md) returns the byte offsets of the matched tokens as interleaved `start, end` pairs — the building block for custom highlighting in the client. Enabling `offset` on the column stores those offsets in the index; without it they are recomputed for each matching row:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/example_027" />

## Convenience predicates

Several wrapper functions read more naturally than `col @@ ts_*(...)` and expand to exactly that — [`phrase_matches`](../../functions/search/full-text.md#phrase_matches), [`has_all_tokens`](../../functions/search/full-text.md#has_all_tokens), [`has_any_tokens`](../../functions/search/full-text.md#has_any_tokens), [`ngram_matches`](../../functions/search/full-text.md#ngram_matches) and [`levenshtein_matches`](../../functions/search/full-text.md#levenshtein_matches). Plain SQL [`IS NULL` / `IS NOT NULL`](../../functions/search/full-text.md#is-null) on an indexed column is claimed by the index as well. See the [function reference](../../functions/search/full-text.md) for the full list.

## Inspecting the query plan

A full-text search is a first-class part of the SQL query plan, not a black box bolted on the side. The `@@` predicate compiles to an **`IRESEARCH_SCAN`** over the inverted index, with the matched terms pushed into the scan as its `Index Filter`. `EXPLAIN` shows it:

<SqlLogicTest id="sql/indexes/inverted/full-text-search/inspect_plan" />

A predicate the index cannot serve shows up as a `Column Filter` instead, checked row by row after the scan.

Because the search executes inside the scan, it composes with the rest of SQL: a `JOIN`, a `GROUP BY`, or an `ORDER BY <scorer>` over the same query is planned and run as one statement. See [Profiling](../../../cookbook/performance/profiling.md) for reading plans, and [Ranking](./ranking.md#top-k-queries-and-wand-pruning) for the WAND-optimized `Top: k, optimized` plan.

## Relevance ranking {#relevance-ranking}

Matching is a yes/no filter. To score and order matches by relevance — `BM25` and other scorers, boosting, and WAND-accelerated top-K queries — see the dedicated [Ranking](./ranking.md) page.

## See also

- [Inverted Index](./index.md) — creating the index
- [Full-Text Search Functions](../../functions/search/full-text.md) — complete function reference
- [`tsquery` data type](../../data_types/tsquery.md)
- [Vector Search](./vector-search.md) · [Hybrid Search](./hybrid-search.md) · [Geospatial Search](./geospatial-search.md)
- [Search cookbook](../../../cookbook/search/index.md)
