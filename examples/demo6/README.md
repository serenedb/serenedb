# Demo 6 -- Self-embedding semantic search over arXiv ML abstracts

Ingests a 2,000-row slice of [neuralwork/arxiver](https://huggingface.co/datasets/neuralwork/arxiver) (63k arXiv papers with title + abstract + markdown, single parquet shard) and computes embeddings on the fly via `generate_embedding(...)` against a local Ollama server. The same function is reused at query time to turn natural-language search strings into vectors -- no precomputed embeddings, no external Python glue.

## What it shows

- **Embeddings as a scalar function.** `generate_embedding(text, protocol, model, secret_name)` returns a `LIST(FLOAT)`. The bootstrap calls it row-by-row inside `INSERT ... SELECT`; the demo calls it inline inside `ORDER BY ... <=> generate_embedding(...)::FLOAT[1024]`.
- **Local-only stack.** No paid API. Ollama serves an OpenAI-compatible `/v1/embeddings` endpoint; the connector talks to it through the same `openai` protocol used for the real OpenAI API -- only `base_url` changes.
- **One hybrid index, NL queries.** A single `CREATE INDEX ... USING inverted` carries both a BM25 dictionary over `abstract` and an HNSW graph over `embedding`. Queries are written as you'd ask them in English.

## Prerequisites

```bash
# Ollama daemon on localhost:11434
ollama pull snowflake-arctic-embed:22m         # 384-dim (small + fast)
ollama serve &                                 # if not already a service
```

The demo uses model `snowflake-arctic-embed:22m` (384 dims). To swap models, change the model name in both `bootstrap.sql` and `demo.sql` and update the `FLOAT[N]` column / cast width accordingly.

## Run

```bash
# Stage data + embed (one-time, ~1-2 minutes on CPU Ollama):
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

# Run the queries:
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```

The secret created by `bootstrap.sql` and `demo.sql` is session-scoped (`CREATE SECRET IF NOT EXISTS my_ollama ...`). If your Ollama lives elsewhere -- container, remote host -- edit the `base_url` in both files.
