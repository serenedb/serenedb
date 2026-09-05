---
title: Highlighting
sidebar_label: Highlighting
sidebar_position: 5
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

**Highlighting** marks the query terms inside a matching document's text so a search UI can show *why* a result matched — the bold terms in a results list or a short "keyword in context" snippet (passage) drawn from a long field. It is a presentation step applied to the rows a query already matched: it does not change which rows match or how they rank.

## Highlighting Functions {#highlighting-functions}

SereneDB highlights matches in two steps: `ts_offsets` locates the matched tokens, then `ts_highlight` wraps them in markup. Used together they produce search-result snippets. For the common case `ts_highlight(column)` does both at once.

| Function | Description |
| :--- | :--- |
| [`ts_highlight(text, offsets [, options])`](#ts_highlight) | Wrap the tokens at the given offsets in markup. |
| [`ts_highlight(column [, options])`](#virtual-column) | Sugar form — derive offsets from the FTS predicate automatically. |
| [`ts_offsets(column [, limit])`](#ts_offsets) | Byte offsets of the matched tokens for an indexed column. |
| [`ts_offsets(dictionary, text, query)`](#ts_offsets) | Compute offsets on the fly for arbitrary text. |

#### `ts_highlight(text, offsets [, options])` {#ts_highlight}

Wraps the tokens of `text` located at `offsets` in markup. `offsets` is an `INTEGER[]` of interleaved `start, end` byte pairs — typically the output of [`ts_offsets`](#ts_offsets). By default each match is wrapped in `<b>...</b>`:

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `text` | `VARCHAR` | The document text to render. `NULL` text yields `NULL`. |
| `offsets` | `INTEGER[]` | Interleaved `start, end` byte pairs, sorted ascending by `start`. Must have an even length; `NULL` yields `NULL`. |
| `options` | `VARCHAR` | Optional comma-separated `key = value` string. Must be a constant — per-row option expressions are rejected at bind time. |

<SqlLogicTest id="sql/functions/full_text_search/ts_highlight" />

The optional third argument is a string of comma-separated `key = value` options:

| Option | Default | Description |
| :--- | :--- | :--- |
| `StartSel` | `<b>` | Markup inserted before each matched range. |
| `StopSel` | `</b>` | Markup inserted after each matched range. |
| `HighlightAll` | `false` | Highlight the whole text verbatim, skipping passage selection. |
| `MaxWords` | `35` | Hard cap on tokens per fragment. Must be a positive integer. |
| `MaxFragments` | `0` | `0` returns the single best-scoring fragment; a value `> 0` returns up to that many top-scored fragments. |
| `FragmentDelimiter` | ` ... ` | String used to join multiple fragments. |
| `MaxOffsets` | `0` | Caps how many offset pairs are considered per document (`0` means unlimited). |

By default (`HighlightAll = false`) SereneDB picks the best-scoring passage around the matches and clips it to `MaxWords` tokens. `StartSel` and `StopSel` override the surrounding markup — here, `<mark>` tags for a custom search UI:

<SqlLogicTest id="sql/functions/full_text_search/ts_highlight_options" />

**How it works.**

- **Passage selection vs. whole text.** With `HighlightAll = false` the function returns the single best-scoring window of up to `MaxWords` tokens around the matches — ideal for a long body field where you want a focused snippet. Set `HighlightAll = true` to wrap every match in the full, unclipped text instead:

  <SqlLogicTest id="sql/functions/full_text_search/ts_highlight_highlightall" />

- **Multiple fragments.** Raising `MaxFragments` returns several top-scored passages joined by `FragmentDelimiter` (`" ... "` by default) — useful when matches are scattered through a document. Adjacent matched tokens merge into one wrapped span, and trailing sentence punctuation is omitted (the analyzer does not tokenize it):

  <SqlLogicTest id="sql/functions/full_text_search/ts_highlight_fragments" />

- **Validation.** `offsets` must have an even length and be sorted ascending by `start`, and each `start` must be `≤ end` and within the document — malformed pairs are rejected. Unknown option keys raise an error with a "did you mean" suggestion, and `options` must be a constant expression.

#### `ts_highlight(column [, options])` — virtual-column form {#virtual-column}

In the common case — highlighting the same column you searched — pass the indexed column directly and SereneDB synthesizes the offsets for you from the query predicate. This is exactly equivalent to `ts_highlight(column, ts_offsets(column) [, options])` but spares you spelling out the offset call:

<SqlLogicTest id="sql/functions/full_text_search/ts_highlight_pipeline" />

The sugar form takes the same `options` string. It requires an inverted-index scan with an FTS predicate on the column (`WHERE body @@ ...`); calling it on a plain scan or on a literal raises an error.

#### `ts_offsets(column [, limit])` {#ts_offsets}

Returns the byte offsets of the matched tokens as interleaved `start, end` pairs — the building block for [`ts_highlight`](#ts_highlight). The optional `limit` caps the number of pairs returned per row.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `column` | indexed column | The FTS-indexed column whose matches to locate (column-reference form, inside an index scan). |
| `limit` | `INTEGER` | Optional cap on pairs returned per row. |
| `dictionary`, `text`, `query` | `VARCHAR`, `VARCHAR`, `TSQUERY` | Standalone form: compute offsets for arbitrary `text` against `query` using the analyzer of `dictionary`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_offsets" />

The standalone three-argument form computes offsets for arbitrary text — no index scan required — by analyzing `text` with a named dictionary and matching it against `query`:

<SqlLogicTest id="sql/functions/full_text_search/ts_offsets_standalone" />

:::note Stored vs. on-the-fly offsets — the `offset` feature flag
`ts_offsets` can source offsets two ways. When the column's text search dictionary is created with **`offset = true`** (which in turn requires **`position = true`**), the byte offsets are stored in the inverted index and read back directly — fast at query time, at the cost of a larger index. Otherwise SereneDB derives the offsets on the fly, re-analyzing the matched text for each result — no extra storage, but more work per query. The standalone form `ts_offsets(dictionary, text, query)` always computes offsets on the fly.
:::

## Coming from Elasticsearch

[Elasticsearch / OpenSearch highlighting](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html) options map onto SereneDB's `ts_highlight` as follows:

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| [`pre_tags` / `post_tags`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#specify-highlight-tags) | `StartSel` / `StopSel` |
| [`number_of_fragments`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#control-highlighted-frags) (1) | `ts_highlight` (default, `MaxFragments = 0`) |
| [`number_of_fragments`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#control-highlighted-frags) (> 1) | `MaxFragments > 0` (joined by `FragmentDelimiter`) |
| [`fragment_size`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#highlighting-settings) (characters) | `MaxWords` (**tokens**, not characters) |
| [`number_of_fragments = 0`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#control-highlighted-frags) (whole field) | `HighlightAll = true` |
| term offsets | `ts_offsets` |
| [`index_options: offsets`](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-options.html) / term vectors | dictionary `offset = true` |

**Notable differences.** SereneDB caps fragments by **token count** (`MaxWords`), whereas Elasticsearch caps by characters (`fragment_size`). It returns the top-scoring fragments only, with no equivalent of:

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| per-fragment `order` / pagination | none (top fragments only) |
| [`highlight_query`](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#override-global-settings) (highlight a different query) | none (highlights the search predicate) |
| [highlighter types](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#specify-highlighter-type) `unified` / `fvh` / `plain` | single highlighter |

## See also

- [Full-Text Search Functions](./full-text.md) — operators, constructors, parsers
- [Inverted Index](../../indexes/inverted/index.md) · [Highlighting guide](../../indexes/inverted/full-text-search.md#highlighting)
- [CREATE TEXT SEARCH DICTIONARY](../../statements/create_text_search_dictionary/index.md) — the `offset` and `position` flags
