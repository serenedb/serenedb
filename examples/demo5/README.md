# Demo 5 -- Hybrid vector + full-text search over 100K DBpedia abstracts

Indexes the [Qdrant dbpedia-entities-openai3-text-embedding-3-small-1536-100K dataset](https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K) (100,000 DBpedia entity abstracts, each with a 1536-dim OpenAI `text-embedding-3-small` vector) in a single SereneDB inverted index that carries both a BM25 analyzer on `text` and an HNSW graph on `embedding`.

## What it shows

- **One index, two retrieval modes.** A single `CREATE INDEX` declares both the text dictionary (BM25, phrase, positional) and the HNSW graph (cosine ANN). Every query in the demo hits the same physical structure.
- **Vector ANN top-K.** Cosine-nearest neighbours of a reference entity via the `<=>` operator and the IRESEARCH_ANN_SCAN plan.
- **Range search.** Everything within a cosine-distance radius -- IRESEARCH_ANN_RANGE_SCAN with constant vectors.
- **Hybrid text + vector.** A `text @@ ts_any(...)` predicate prunes the candidate set and the survivors are ordered by vector distance, all in one SQL statement.
- **Two ingestion shapes:**
  - `bootstrap.sql` -- pulls the four parquet shards down once and writes them into a native SereneDB table. Everything lives locally after the load.
  - `bootstrap_view.sql` -- registers a view straight over the remote parquet URLs. Only the inverted index + HNSW graph land on disk; column projections fetch from Hugging Face on demand.
