---
title: Phrase and Proximity Search
sidebar_position: 5
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Phrase and Proximity Search

Search for tokens appearing in a specific order. This allows matching partial or full sentences within indexed text, and — with [slop](#proximity-search-with-slop) — sentences whose wording drifts from the query. Requires `POSITION = true` in the dictionary.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## Basic phrase search

Use the `@@` operator with `ts_phrase` to find documents containing tokens in sequence:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_001" />

Both documents contain the phrase "biggest blockbuster" in their descriptions.

## Multi-word phrases

Search for longer sequences:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_002" />

## Combining phrase conditions with AND

Find documents matching multiple phrases:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_003" />

## Combining phrase conditions with OR

Find documents matching any of several phrases:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_004" />

## Phrase search across columns

Search different columns in the same query:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_005" />

## Combine with exact matching

Use phrase search together with term operations:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_006" />

## Proximity search with slop

An exact phrase is brittle: it fails on the words a writer put *between* the ones a searcher typed. Nobody searching for "group children" wants to miss "a group **of** children". Pass `slop := N` to buy the phrase a budget of `N` position moves:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_010" />

Nothing — the tokens are not adjacent. With one unit of slop, the intervening `of` is affordable:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_011" />

`slop` is an edit budget for phrases, playing the role [Levenshtein distance](./fuzzy-search.md#levenshtein-matching-with-ts_levenshtein) plays for a single word — except the only edit it can buy is *moving* a term, never substituting or dropping one. It is the total number of positions the query tokens may be shifted to line up with the document, shared across the whole phrase: each token sitting between two query tokens costs one unit, and `slop := 0` is an exact phrase, identical to omitting it. Every token you type must still appear in the document, so no amount of slop rescues a misspelled word — that is [`ts_levenshtein`](./fuzzy-search.md)'s job, and the two compose.

### Widening the window

Raise the budget and more distant co-occurrences come into range. "Zion falls to the machine army" needs three units:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_012" />

At five, "Zion defends itself against the massive machine invasion" joins it:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_013" />

That is the whole tuning trade-off. A low budget keeps the phrase tight and precise; a high one drifts toward "these words appear near each other", and eventually toward a plain AND of the terms.

### Matching words out of order

A budget of 2 also pays for one swap of an adjacent pair, so a phrase can match text that reverses it. Searching "spacecraft alien" finds the film that says "alien spacecraft":

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_014" />

Reordering is strictly more expensive than insertion: one intervening word costs 1, one transposition costs 2. If word order matters to you, keep `slop` at `0` or `1`.

### Other spellings

`::slop(N)` applies the same budget as a modifier on an existing phrase — useful when the phrase comes from somewhere you would rather not edit, such as [`phraseto_tsquery`](../../sql/functions/search/full-text.md#phraseto_tsquery) or a stored query string:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_015" />

Lucene's `"..."~N` proximity syntax also carries through [`to_tsquery`](../../sql/functions/search/full-text.md#to_tsquery), which matters when you are porting queries from Elasticsearch:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_016" />

The two spellings are mutually exclusive — combining `slop := N` with `::slop(N)` on one phrase is an error rather than a silently chosen winner.

### Slop and explicit gaps

`slop` composes with the gap arguments, and this is the one case where its meaning needs care: the budget counts deviation from the gap you *declared*, not from adjacency. Here `group`, gap `1`, `children` declares "exactly one token between", which "a group of children" satisfies outright, so a zero budget suffices:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_017" />

Raising the budget to `1` would then admit anything one step off that declaration — adjacent tokens, or two tokens apart. Interval gaps are the exception: `[min, max]` already expresses a range, so pairing it with `slop` is rejected rather than compounded.

## Combine with analytics

The power of SereneDB: search and aggregate in a single query:

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_007" />

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_008" />

## How phrase search works

1. The query text goes through the **same dictionary** as the indexed data
2. The resulting tokens must appear in the **same order** and at **consecutive positions** in the document -- unless a gap or [slop](#proximity-search-with-slop) budget loosens one or both of those requirements
3. Because both sides use the same normalization (case, stemming, accents), matching is consistent

For example, with `basic_dict` (`CASE = 'lower'`, `ACCENT = false`):

<SqlLogicTest id="cookbook/search/phrase-and-proximity-search/example_009" />

## See also

- [Case-Sensitivity and Diacritics](./case-sensitivity-and-diacritics.md) — how normalization affects phrase matching
- [Exact Value Matching](./exact-value-matching.md) — single-token matching
- [BM25/TFIDF Ranking](./ranking.md) — ordering results by relevance
