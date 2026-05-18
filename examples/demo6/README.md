# Demo 6 -- Self-embedding semantic search over arXiv ML abstracts

Ingests a 100-row slice of [neuralwork/arxiver](https://huggingface.co/datasets/neuralwork/arxiver) (63k arXiv papers with title + abstract + authors) and computes embeddings on the fly via `ai_embed(...)` against Google's Gemini API through its OpenAI-compatible endpoint. The same function is reused at query time to turn natural-language search strings into vectors -- no precomputed embeddings, no external Python glue.

## What it shows

- **Embeddings as a scalar function.** `ai_embed(text, model, secret_name)` returns a `LIST(FLOAT)`. The bootstrap calls it inside `INSERT ... SELECT`; the demo calls it inline inside `ORDER BY ... <=> ai_embed(...)::FLOAT[3072]`.
- **Provider-agnostic via the `openai` secret type.** Gemini exposes an OpenAI-compatible `/v1beta/openai/embeddings` endpoint; the connector talks to it through the same `openai` protocol used for the real OpenAI API -- only `base_url` and `embeddings_path` change.
- **One hybrid index, NL queries.** A single `CREATE INDEX ... USING inverted` carries both a BM25 dictionary over `abstract` and an HNSW graph over `embedding`. Queries are written as you'd ask them in English.

## Prerequisites

A Gemini API key (free tier works). Edit `bootstrap.sql` and replace the `api_key` value in the `CREATE SECRET gemini` block. To swap models or providers, change `base_url` / `embeddings_path` to point at any OpenAI-compatible embeddings endpoint, then update the model name and `FLOAT[N]` width to match.

## Run

```bash
# Stage data + embed (one-time):
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

# Run the queries:
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```
