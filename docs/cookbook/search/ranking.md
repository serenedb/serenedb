---
title: BM25/TFIDF Ranking
sidebar_position: 11
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# BM25/TFIDF Ranking

Relevance scoring ranks search results by how well they match a query. The two most common algorithms are [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) and [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf), both based on term frequency and inverse document frequency.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## How ranking works

Both algorithms rely on two statistical measures:

- **Term frequency (TF)** — how often a search term appears in a given document. A document mentioning "galaxy" five times is considered more relevant than one mentioning it once.
- **Inverse document frequency (IDF)** — how rare the term is across all indexed documents. Common words like "the" appear everywhere and carry little signal. A rare term like "paleontologist" is a much stronger match indicator.

## BM25 vs TF-IDF

BM25 improves on TF-IDF in two ways. It saturates term frequency: a word's contribution levels off as the word repeats, so repeating one query word only goes so far. It normalizes for length: a match in a short row counts for more than the same match in a long row, so long rows don't win just by containing more words.

TF-IDF has no ceiling on repeats and ignores length unless you pass `with_norms`, which is why `BM25` is the better default for ranking search results:

| | TF-IDF (`TFIDF`) | BM25 (`BM25`) |
|---|---|---|
| Repeats of a query word | Score grows with the square root of the count, with no ceiling | Score levels off toward the word's IDF; `k1` sets how quickly |
| Length normalization | Off by default; `with_norms` divides by the square root of the row's length | On by default, relative to the column's average length; `b` sets the strength |
| IDF of a word in every row | `ln 2` ≈ 0.69 | Close to `0` |
| Parameters | `with_norms` | `k1`, `b` |
| Needs the `norm` flag | Only with `with_norms` | For length normalization (`b` above `0`) |
| Best for | A simple baseline; short values of similar length, such as titles or tags | Text of mixed length, multi-word queries, general-purpose ranking |

### How IDF is calculated

IDF measures how rare a word is in the searched column. With `N` rows that have a value in the column and `n` of them containing the word, the textbook IDF is `log(N / n)`: the rarer the word, the higher its weight. Both scorers use a smoothed form of it:

```
TF-IDF:  idf = ln(1 + (N + 1) / (n + 1))
BM25:    idf = ln(1 + (N - n + 0.5) / (n + 0.5))
```

The classic BM25 IDF, `ln((N - n + 0.5) / (n + 0.5))`, turns negative for a word in more than half the rows; the `1 +` inside the logarithm keeps both forms positive.

In the shared dataset all 10 movies have a description and "alien" occurs in one of them, so `N = 10` and `n = 1`:

```
TF-IDF:  idf = ln(1 + 11 / 2)    = ln 6.5   ≈ 1.872
BM25:    idf = ln(1 + 9.5 / 1.5) = ln 7.333 ≈ 1.992
```

The [TFIDF example](#tfidf-scoring) returns 1.8718022 for "alien": this IDF times `√1` for its single occurrence.

### How TF-IDF and BM25 combine TF and IDF

Each scorer gives every query word a row contains a score and adds the scores up. For a word that occurs `tf` times in a row of length `dl`, in a column whose average length is `avgdl`:

```
TF-IDF:  score = √tf × idf                    (divided by √dl with with_norms)
BM25:    score = idf × tf / (tf + k1 × (1 - b + b × dl / avgdl))
```

The BM25 fraction approaches 1 as `tf` grows, so a word never adds more than its IDF, while `√tf` keeps growing. The textbook BM25 formula also multiplies by `k1 + 1`; that scales every score by the same constant and leaves the order unchanged. The [BM15 example](#named-variants) sets `b = 0`, so for one occurrence of "alien" the score is 1.992 × 1 / (1 + 1.2) ≈ 0.906, and the query returns 0.90565.

[`BM25(tableoid[, k1, b])`](../../sql/functions/search/scoring.md#bm25) takes two optional parameters:

- `k1` (default `1.2`) controls term-frequency saturation. Higher values let extra occurrences keep mattering; lower values level off sooner, and `0` ignores frequency, leaving the IDF alone.
- `b` (default `0.75`) controls length normalization, from `0` to `1`. `0` turns it off; `1` normalizes fully by length.

[`TFIDF(tableoid[, with_norms])`](../../sql/functions/search/scoring.md#tfidf) takes one: `with_norms` (default `false`) divides the score by `√dl`.

Length normalization needs each row's length, which the index stores only when the column's dictionary sets the `norm` [feature flag](../../sql/statements/create_text_search_dictionary/index.md#feature-flags). Without it every row counts as one token long, so neither scorer can tell long rows from short ones. The shared dataset doesn't set `norm`; the example below does.

### Same query, two rankings

A query can apply only one scorer per index, so to compare the two, run each in its own branch of a `UNION ALL`. This four-row catalog sets `norm` on its dictionary, so length normalization is active:

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/ranking/bm25_vs_tfidf_setup" />

</details>

Search the page bodies for "trail shoes" with both scorers. A bare string matches rows that contain any of its words:

<SqlLogicTest id="cookbook/search/ranking/bm25_vs_tfidf" />

TF-IDF ranks the river guide first because it credits each of the guide's five "trail"s, while BM25 caps what one repeated word can add and discounts the guide for being eight times longer than the listing, so the page that matches both words comes first and the guide drops to last. With `b = 0`, BM25 still puts the listing above the guide: saturation alone changes the top result.

### Is TF-IDF still used?

Yes. TF-IDF weights remain a standard way to turn text into features for classification, clustering and keyword extraction, and a common baseline when evaluating search quality. For ranking search results BM25 is the usual default, and in SereneDB it is the better choice for most columns.

`TFIDF` is a reasonable choice when:

- you want a simple baseline with little to tune, to compare a tuned ranking against;
- the column holds short values of similar length, such as titles, names or tags: words rarely repeat and lengths barely differ, so BM25's two corrections have little to act on.

`BM25` is the better default when:

- values vary in length, as descriptions, articles, reviews and support tickets do;
- queries have several words, and one repeated word should not outweigh matching the others;
- authors can repeat words to climb the ranking, as in marketplace listings or user-generated content.

## BM25 scoring

Use `BM25(<index>.tableoid)` in the SELECT and ORDER BY clauses to rank results by relevance:

<SqlLogicTest id="cookbook/search/ranking/example_001" />

### Custom parameters

Pass `k1` and `b` to tune the ranking:

<SqlLogicTest id="cookbook/search/ranking/example_002" />

| Parameter | Default | Description |
|---|---|---|
| `k1` | `1.2` | Term frequency saturation. Higher values increase the impact of term frequency |
| `b` | `0.75` | Document length normalization. `0` disables normalization, `1` fully normalizes |

Favor exact matches over frequency by lowering `k1`, and disable length normalization with `b = 0`:

<SqlLogicTest id="cookbook/search/ranking/example_003" />

Increase `k1` to reward documents that mention the term many times:

<SqlLogicTest id="cookbook/search/ranking/example_004" />

### Named variants

Specific combinations of `k1` and `b` produce well-known BM25 variants:

| Variant | Parameters | Behavior |
|---|---|---|
| **BM25** | `BM25(1.2, 0.75)` | Default — balanced saturation and length normalization |
| **BM15** | `BM25(1.2, 0)` | No length normalization (`b=0`). Treats all documents equally regardless of length |
| **BM11** | `BM25(1.2, 1)` | Full length normalization (`b=1`). Strongly penalizes long documents |
| **BM1** | `BM25(0, 0)` | Pure IDF — term frequency is ignored entirely. Only document rarity matters |

<SqlLogicTest id="cookbook/search/ranking/example_005" />

### Combine with filters

<SqlLogicTest id="cookbook/search/ranking/example_006" />

### Combine with analytics

<SqlLogicTest id="cookbook/search/ranking/example_007" />

### Pagination with stable ordering

When paginating, add a tiebreaker column to ensure consistent ordering across pages:

<SqlLogicTest id="cookbook/search/ranking/example_008" />

## TFIDF scoring

Use `TFIDF(<index>.tableoid)` as an alternative scoring function:

<SqlLogicTest id="cookbook/search/ranking/example_009" />

### With normalization

Pass `true` to divide each score by the square root of the row's length. Length normalization needs the `norm` flag on the dictionary; the shared dataset doesn't set it, so the score below is the same as without it:

<SqlLogicTest id="cookbook/search/ranking/example_010" />

## Custom scoring

Combine relevance scores with other columns for domain-specific ranking:

<SqlLogicTest id="cookbook/search/ranking/example_011" />

## Dictionary requirements

Scoring functions rank rows by how often the query's terms occur only when the dictionary has `FREQUENCY = true`; without it, every row that matches a term scores the same for that term:

<SqlLogicTest id="cookbook/search/ranking/example_012" />

The `FREQUENCY` flag stores term frequency data in the index, which BM25 and TF-IDF use to rank rows. Length normalization, used by `BM25` with `b` above `0` and by `TFIDF` with `with_norms`, also needs the `NORM` flag; without it every row counts as one token long. See [scorer requirements](../../sql/functions/search/scoring.md#requirements).

## See also

- [Relevance Scoring](../../sql/functions/search/scoring.md) — `BM25` and `TFIDF` signatures and the other scorers
- [Phrase and Proximity Search](./phrase-and-proximity-search.md) — finding phrase matches to rank
- [CREATE TEXT SEARCH DICTIONARY](../../sql/statements/create_text_search_dictionary/index.md) — frequency and position flags
