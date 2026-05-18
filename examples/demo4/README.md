# Demo 4 -- Hybrid vector + full-text search over a movie catalogue

Indexes the [MongoDB embedded_movies dataset](https://huggingface.co/datasets/MongoDB/embedded_movies) (~3,500 movies, each with a 1536-dim OpenAI ada-002 `plot_embedding`) in a single SereneDB inverted index that carries both an FTS analyzer on `fullplot` and an HNSW graph on `plot_embedding`.

## What it shows

- **One index, two retrieval modes.** A single `CREATE INDEX` declares both the text dictionary (BM25, phrase, positional) and the HNSW graph (cosine ANN). Every query in the demo hits the same physical structure.
- **Vector ANN top-K.** Cosine-nearest neighbours of a reference movie via the `<=>` operator and the IRESEARCH_ANN_SCAN plan.
- **Range search.** Everything within a cosine-distance radius -- IRESEARCH_ANN_RANGE_SCAN with constant vectors.
- **Hybrid text + vector.** A `fullplot @@ ts_any(...)` predicate prunes the candidate set and the survivors are ordered by vector distance, all in one SQL statement.
- **Structured filters compose for free.** `year`/`genres` predicates push into the same scan; no separate pre-filter pipeline, no over-fetch-then-rerank.
- **Vector retrieval feeding SQL analytics.** `UNNEST`/`GROUP BY` over a vector-ordered window -- semantic search as a subquery.
