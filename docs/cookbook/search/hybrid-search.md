---
title: Semantic and Hybrid Search
sidebar_position: 16
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Semantic and Hybrid Search

Keyword search matches the words a shopper typed. Vector search matches what they meant. SereneDB stores both a text field and its embedding in one [inverted index](../../sql/indexes/inverted/index.md), so you can rank by meaning, filter by keyword and fuse the two without leaving the query. This recipe walks from a plain nearest-neighbor search up to a hybrid that catches results keyword search alone would miss.

Five products sit in an `items` table, each with a `name` and a three-dimensional `emb` vector. In production you fill `emb` by running the text through an embedding model and storing the vector it returns, usually hundreds of dimensions wide. The three numbers here are written by hand so the distances stay easy to read: the first axis is roughly "made for running". `emb` is indexed with `ivf` on the `l2` metric, so `<->` gives Euclidean distance and smaller means closer.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/hybrid-search/setup" />

</details>

## Rank by meaning

`emb <-> query` is the distance from each row to your query vector. Order by it, take the top k and you have semantic search. Notice "marathon racer" ranks second even though it shares no words with a running query, because its embedding sits right next to the running shoes.

<SqlLogicTest id="cookbook/search/hybrid-search/example_001" />

## Where keyword search falls short

Search the word "running" and you only get the rows that literally contain it. "Marathon racer" is exactly what the shopper wants and it is nowhere to be found.

<SqlLogicTest id="cookbook/search/hybrid-search/example_002" />

## Filter by keyword, rank by vector

The simplest hybrid uses the keyword match as a hard filter and the vector as the sort. Great when the keyword is a firm requirement and you just want the closest matches ordered by meaning.

<SqlLogicTest id="cookbook/search/hybrid-search/example_003" />

## Fuse both with reciprocal rank fusion

When neither signal should be a gate, run both and blend the rankings with [reciprocal rank fusion](./reciprocal-rank-fusion.md). The one thing that changes when a branch ranks by vector distance is the sort direction: BM25 scores are better when *higher* (`ORDER BY s DESC`), but distance is better when *smaller*, so the vector branch ranks ascending (`ORDER BY dist`). Flip either one and the fusion rewards the worst matches of that branch.

Fused, "marathon racer" finally surfaces: the keyword branch never sees it, but the vector branch ranks it high enough to make the cut.

<SqlLogicTest id="cookbook/search/hybrid-search/example_004" />

## See also

- [Vector Search guide](../../sql/indexes/inverted/vector-search.md): building `ivf` indexes, choosing a metric and tuning recall
- [Hybrid Search guide](../../sql/indexes/inverted/hybrid-search.md): the filtered-ANN and fusion strategies in depth
- [Reciprocal Rank Fusion](./reciprocal-rank-fusion.md): the fusion pattern on its own, for blending any two ranked queries
- [Vector functions reference](../../sql/functions/vector.md): `l2_distance`, `cosine_distance` and the distance operators
