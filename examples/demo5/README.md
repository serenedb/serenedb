# Demo 5 -- Self-embedding semantic search over arXiv ML abstracts

Ingests a slice of [neuralwork/arxiver](https://huggingface.co/datasets/neuralwork/arxiver) and computes embeddings on the fly via `ai_embed(...)` against Google's Gemini API through its OpenAI-compatible endpoint. The same function is reused at query time for natural-language search.

## What it shows

- **Embeddings as a scalar function.** `ai_embed(text, model, secret_name)` returns `LIST(FLOAT)`. Used in `INSERT ... SELECT` and inline inside `ORDER BY ... <=> ai_embed(...)::FLOAT[3072]`.
- **Provider-agnostic via the `openai` secret type.** Any OpenAI-compatible embeddings endpoint works -- only `base_url` / `embeddings_path` change.
- **One hybrid index** -- BM25 over `abstract` + HNSW over `embedding`, queried with English-language strings.

## Run

Set your API key in `bootstrap.sql` (`CREATE SECRET gemini`), then:

```bash
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```
