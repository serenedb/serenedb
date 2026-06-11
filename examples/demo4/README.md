# Demo 4 -- Hybrid vector + full-text search over 100K DBpedia abstracts

Indexes the [Qdrant dbpedia-entities-openai3-text-embedding-3-small-1536-100K dataset](https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K) -- 100K abstracts with 1536-dim OpenAI vectors -- in one SereneDB inverted index with both a BM25 analyzer and an HNSW graph.

## What it shows

- **One index, two retrieval modes.** BM25 (phrase, positional) and HNSW cosine ANN on the same physical structure.
- **Vector ANN top-K, range search, and hybrid `text @@ ... ORDER BY embedding <=> ...`** -- one SQL statement, one index hit.
- **Two ingestion shapes, one demo:** `bootstrap.sql` loads into a native table; `bootstrap_view.sql` builds the index over a remote-parquet view. `demo.sql` runs unchanged against either.
