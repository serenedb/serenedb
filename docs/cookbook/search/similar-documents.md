---
title: Finding Similar Documents
sidebar_position: 17
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Finding Similar Documents

Point at one row and get back the rows most like it. That is more-like-this: the related-articles rail under a support page, the "you might also want" list, the pass that flags an article someone published twice. It runs on embeddings, so two documents count as similar when their vectors sit close together even when they share no words. This is a different job from typing a query into a search box and ranking by it. For that reach for the [Semantic and Hybrid Search](./hybrid-search.md) recipe. Here the probe is always an existing row.

Eight help-center articles carry a `title` and a three-dimensional `emb`. The three numbers are written by hand so the distances read cleanly: the axes stand in for account, billing and connectivity topics. `emb` is indexed with `ivf` on the `l2` metric, so `<->` gives Euclidean distance and smaller means closer. Two pairs of articles hold the same content under a reworded title, which is exactly what the near-duplicate pass exists to catch.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/similar-documents/setup" />

</details>

## More like this

Name the row by id and use its own embedding as the probe. The subquery reads the vector for article 1, then `emb <-> ...` scores every other row by distance to it and the closest rows are the ones most like it. The nearest result here is a reworded copy of the same article sitting at distance 0, with the genuinely related account articles just behind it. This form scores every other row inline, which is fine at this size. On a large collection split it into two steps: fetch the row's embedding first, then issue the nearest-neighbor query with that vector as the probe so the planner can drive the IVF index instead of scoring the whole table.

<SqlLogicTest id="cookbook/search/similar-documents/example_001" />

## Catching near-duplicates

To find the copies you compare every pair and keep the ones inside a tiny radius. A self-join does exactly that: join the table to itself on `a.id < b.id` so each pair appears once, then filter on distance. Anything under a small threshold is effectively the same document. Here it surfaces both reworded copies at distance 0. A self-join scores every pair so it is fine for small tables. On a big collection you would cluster on the MinHash signatures below instead.

<SqlLogicTest id="cookbook/search/similar-documents/example_002" />

## MinHash for near-duplicate text

Distances work once you have vectors. To catch near-duplicate text from the words alone, with no embedding model in the loop, SereneDB has the [`minhash`](../../sql/statements/create_text_search_dictionary/minhash/index.md) dictionary template. It wraps a tokenizer and reduces each document to a fixed set of `numhashes` hash values, its signature. Two documents that share most of their words share most of their signature, so the fraction of matching hashes estimates the Jaccard similarity of their word sets. That estimate is the standard signal for approximate deduplication across a large collection.

Build one and inspect a signature:

```sql
CREATE TEXT SEARCH DICTIONARY doc_minhash (
    template = 'minhash',
    numhashes = 64,
    tokenizer_template = 'text',
    tokenizer_locale = 'en_US.UTF-8',
    tokenizer_case = 'lower',
    tokenizer_stemming = false
);

SELECT ts_lexize('doc_minhash', 'the quick brown fox jumps over the lazy dog');
```

The values that come back are opaque hashes like `c1vB9RXiIbY`, not readable words: the same token always hashes the same way, so overlapping vocabulary shows up as overlapping hashes. `numhashes` sets how many hashes form the signature, trading index size for a sharper similarity estimate. The exact bytes are specific to the engine build, so compare whole signatures between documents and never match on an individual value. That is why this stays an overview rather than a worked query with fixed expected output.

## See also

- [Semantic and Hybrid Search](./hybrid-search.md): ranking by a search-box query vector and fusing it with keyword matches
- [Vector Search guide](../../sql/indexes/inverted/vector-search.md): building `ivf` indexes, choosing a metric and tuning recall
- [Vector functions reference](../../sql/functions/vector.md): `l2_distance`, `cosine_distance` and the distance operators
- [`minhash` template](../../sql/statements/create_text_search_dictionary/minhash/index.md): the signature options and how overlap estimates Jaccard similarity
