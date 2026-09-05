---
title: Highlighting
sidebar_position: 24
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Highlighting

A results page is easier to trust when it shows why each row matched. [`ts_highlight`](../../sql/functions/search/highlighting.md) wraps the matched terms in a snippet of the document and [`ts_offsets`](../../sql/functions/search/highlighting.md) hands you the raw byte spans if you would rather render the markup yourself. Both read the match from the same [inverted index](../../sql/indexes/inverted/index.md) scan that answered the `WHERE`, so highlighting costs one extra column, not a second query.

Four short articles fill a `body` column here. Highlighting needs to know where each token sits, so the dictionary on that column sets `position = true`.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/highlighting/setup" />

</details>

## Wrap the matched terms

Pass the searched column to `ts_highlight` and it returns the body with every hit in `<b>` tags. The match comes from whatever the `@@` predicate found, so you never repeat the query.

<SqlLogicTest id="cookbook/search/highlighting/example_001" />

## Use your own tags

`StartSel` and `StopSel` set the opening and closing markup, so point them at whatever your frontend styles.

<SqlLogicTest id="cookbook/search/highlighting/example_002" />

## Highlight a phrase as one span

A phrase match is a single span from the first token to the last, not one box per word, so `inverted index` highlights as a unit.

<SqlLogicTest id="cookbook/search/highlighting/example_003" />

## Get the raw offsets instead

When you build the markup in the application layer, `ts_offsets` gives you the interleaved `start, end` byte pairs of each match and stays out of the rendering. `ts_highlight(body)` is exactly `ts_highlight(body, ts_offsets(body))` with the default tags.

<SqlLogicTest id="cookbook/search/highlighting/example_004" />

## Rank, then highlight the winners

Highlighting is a projection, so it composes with ranking and `LIMIT`. Order by [`BM25`](./ranking.md) and highlight only the page you return.

<SqlLogicTest id="cookbook/search/highlighting/example_005" />

## Trim the snippet to a window

On a long document you want a snippet around the hit, not the whole thing. `MaxWords` caps the fragment length and `ts_highlight` keeps the window tight around the match.

<SqlLogicTest id="cookbook/search/highlighting/example_006" />

Other options tune the output: `MaxFragments` returns several passages joined by `FragmentDelimiter`, and `HighlightAll` wraps every match in the full untrimmed text. See the [reference](../../sql/functions/search/highlighting.md) for the full list.

## See also

- [Highlighting reference](../../sql/functions/search/highlighting.md): every `ts_highlight` option, `ts_offsets` and the standalone dictionary forms
- [BM25/TFIDF Ranking](./ranking.md): order the results before you highlight them
- [Phrase and Proximity Search](./phrase-and-proximity-search.md): the phrase queries that highlight as one span
