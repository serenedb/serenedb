---
title: AI Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

AI functions call an external model provider from SQL. Today this is [`ai_embed`](#ai_embed), which turns text into an embedding vector — the bridge between your text and SereneDB's [vector search](../indexes/inverted/vector-search.md): embed documents once and store the vectors in an `ivf` column to rank by semantic similarity, or pair that with full-text matching for [hybrid search](../indexes/inverted/hybrid-search.md).

<DocCallout type="attention">

`ai_embed` performs a network request to the configured provider, so it needs a [secret](#providers) and outbound connectivity, and the embedding dimension depends on the model.

</DocCallout>

## Providers {#providers}

`ai_embed` talks to any service that exposes an **OpenAI-compatible** embeddings API. The `TYPE openai` of the [secret](../statements/create_secret/index.md) names that wire protocol — not a specific vendor — so a single mechanism reaches many providers:

- **OpenAI** (hosted) — just supply an `api_key`, the default endpoint is used:

  ```sql
  CREATE SECRET openai (TYPE openai, api_key '⟨sk-...⟩');
  ```

- **Other hosted providers** with an OpenAI-compatible endpoint — point `base_url` at it and supply that provider's `api_key`. For example Google Gemini through its OpenAI-compatibility layer:

  ```sql
  CREATE SECRET gemini (
      TYPE openai,
      api_key '⟨gemini-key⟩',
      base_url 'https://generativelanguage.googleapis.com/v1beta/openai'
  );
  ```

- **Locally-hosted models** served by [Ollama](https://ollama.com/), vLLM, LM Studio or llama.cpp — point `base_url` at the local server; an `api_key` is needed only if the server enforces one.

The runnable examples below use a local Ollama server running the `all-minilm` model:

<SqlLogicTest id="sql/functions/ai_ollama/secret" />

| Secret parameter | Description |
| :--- | :--- |
| `api_key` | API key for the provider (required by OpenAI; optional for open local endpoints). |
| `base_url` | Base URL of an OpenAI-compatible server. Omit for OpenAI itself. |
| `embeddings_path` | Path of the embeddings endpoint, if it differs from the default. |

## `ai_embed` {#ai_embed}

`ai_embed(text, model, secret_name)` sends `text` to the embedding `model` of the provider named by `secret_name` and returns the embedding as a `FLOAT[]`. The vector's length is the model's embedding dimension — 384 for `all-minilm`:

<SqlLogicTest id="sql/functions/ai_ollama/embed_dim" />

A `NULL` `text` returns `NULL`, so rows without text are simply skipped:

<SqlLogicTest id="sql/functions/ai_ollama/embed_null" />

| Argument | Description |
| :--- | :--- |
| `text` | The text to embed. `NULL` yields `NULL`. |
| `model` | The provider's embedding model name, e.g. `'all-minilm'` or `'text-embedding-3-small'`. |
| `secret_name` | Name of the `openai`-type secret holding the endpoint and/or API key. |

**Returns** a variable-length `FLOAT[]`. To store embeddings in an [IVF vector column](../indexes/inverted/vector-search.md) — which requires a *fixed* size — cast to `FLOAT[N]` with the model's dimension, e.g. `ai_embed(...)::FLOAT[384]`. Every stored row and the query vector must use the **same model and dimension**, or the index and the distance comparisons will not line up.

### Choosing a model

The embedding dimension `N` is fixed by the model. A few common ones:

| Model | Provider | Dimension `N` |
| :--- | :--- | :---: |
| `text-embedding-3-small` | OpenAI | 1536 |
| `text-embedding-3-large` | OpenAI | 3072 |
| `all-minilm` (all-MiniLM-L6-v2) | Ollama / local | 384 |

Check your provider's documentation for the exact dimension and use it as the `N` in the stored `FLOAT[N]` column. Match the index's [distance metric](../indexes/inverted/vector-search.md) to how the model's vectors are meant to be compared — most text-embedding models are tuned for **cosine** similarity.

### Performance

Each `ai_embed` call is a network request to the provider, so **embed documents once at write time** and store the vectors; only the *query* text is embedded at search time. A `NULL` input short-circuits to `NULL` without a request. Provider failures (authentication, rate limits, connectivity) surface as a query error.

Embedding a column is just a `SELECT` — `NULL`s pass through and are easy to count or filter:

<SqlLogicTest id="sql/functions/ai_ollama/embed_table" />

## End-to-end: semantic search

Embed each row once, store the vector in a fixed-size `FLOAT[N]` column and build an [IVF](../indexes/inverted/vector-search.md) index over it:

<SqlLogicTest id="sql/functions/ai_ollama/build_index" />

Then embed the query text at search time and rank by vector distance — the embedding model maps semantically related words close together:

```sql
SELECT id, name
FROM catalog_idx
ORDER BY embedding <-> ai_embed('tropical fruit', 'all-minilm', 'local_ai')::FLOAT[384]
LIMIT 3;
```

Because `name` is also full-text indexed in the same index, you can pair a lexical filter with semantic ranking — see [Hybrid Search](../indexes/inverted/hybrid-search.md).

## See also

- [Vector Search](../indexes/inverted/vector-search.md) — IVF indexing and the `<->` operator
- [Hybrid Search](../indexes/inverted/hybrid-search.md) — combine full-text filters with vector ranking
- [CREATE SECRET](../statements/create_secret/index.md) — configure the provider
