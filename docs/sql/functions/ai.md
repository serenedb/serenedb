---
title: AI Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

AI functions call an external model provider from SQL. They cover three kinds of work:

- **Text with a chat model**: [`ai_generate`](#ai_generate) answers a prompt, [`ai_classify`](#ai_classify) picks a label, [`ai_extract`](#ai_extract) pulls out a value or a JSON object, [`ai_filter`](#ai_filter) decides whether a condition holds, [`ai_translate`](#ai_translate) translates and [`ai_redact`](#ai_redact) masks personal information.
- **Embeddings**: [`ai_embed`](#ai_embed) turns text into a vector for [vector search](../indexes/inverted/vector-search.md), and [`ai_similarity`](#ai_similarity) compares two texts by the cosine similarity of their embeddings.
- **Typed decisions**: [`prompt_jev`](#prompt_jev) asks a Jev decision model a yes/no, multiple-choice or rating question and returns calibrated probabilities instead of prose.

<DocCallout type="attention">

Every AI function sends the text to the configured provider over the network. That costs money on hosted providers, the text leaves the server, and the text can steer the model's answer (prompt injection). `ai_redact` depends entirely on the model and is not a sufficient anonymization mechanism on its own.

</DocCallout>

## Providers {#providers}

The functions read the provider's endpoint and API key from a [secret](../statements/create_secret/index.md). There are two secret types:

- **`TYPE openai`** is used by every `ai_*` function. It names the OpenAI wire protocol, not a vendor, so it reaches OpenAI itself, any hosted provider with an OpenAI-compatible endpoint (for example Google Gemini through `base_url 'https://generativelanguage.googleapis.com/v1beta/openai'`), and locally hosted models served by [Ollama](https://ollama.com/), vLLM, LM Studio, LiteLLM or llama.cpp.
- **`TYPE typesafe`** is used by `prompt_jev`. It reaches the hosted [TypeSafe](https://typesafe.ai/) Jev API, or a self-hosted [Kev](https://github.com/jaredpalmer/kev) server, which serves the same API from open models.

For OpenAI itself, an `api_key` is all you need:

```sql
CREATE SECRET openai (TYPE openai, api_key '⟨sk-...⟩', model 'gpt-4o-mini');
```

The runnable examples on this page use a local Ollama server with the `all-minilm` embedding model and the `qwen2.5:0.5b` chat model, and a local Kev server:

<SqlLogicTest id="sql/functions/ai_ollama/secret" />

<SqlLogicTest id="sql/functions/ai_ollama/chat_secret" />

<SqlLogicTest id="sql/functions/ai_kev/secret" />

| `openai` parameter | Description |
| :--- | :--- |
| `api_key` | API key for the provider. Required by OpenAI; optional for open local endpoints. |
| `base_url` | Base URL of an OpenAI-compatible server. Omit for OpenAI itself. |
| `model` | Default chat model for the text functions. A `model` argument overrides it. |
| `chat_path` | Path of the chat completions endpoint, if it differs from `/v1/chat/completions`. |
| `embeddings_path` | Path of the embeddings endpoint, if it differs from `/v1/embeddings`. |

| `typesafe` parameter | Description |
| :--- | :--- |
| `api_key` | TypeSafe API key. Omit for a Kev server started without `KEV_API_KEY`. |
| `base_url` | Base URL of the API. Defaults to `https://api.typesafe.ai`; point it at a Kev server to run locally. |
| `model` | Model name. Defaults to `jev-latest`, which Kev also answers to. |

### Choosing the secret {#default-secret}

Each call names its secret with `secret_name`. When it doesn't, the function uses the secret named by a setting, and fails if that setting is empty:

| Setting | Used by |
| :--- | :--- |
| `sdb_ai_text_default_secret` | `ai_generate`, `ai_classify`, `ai_extract`, `ai_filter`, `ai_translate`, `ai_redact` |
| `sdb_ai_embedding_default_secret` | `ai_embed`, `ai_similarity` |
| `sdb_ai_jev_default_secret` | `prompt_jev` |

<SqlLogicTest id="sql/functions/ai_ollama/default_secret" hideResult />

## Common parameters {#parameters}

The text functions take the per-row text as their first argument, then their own arguments, then these optional named parameters:

| Parameter | Description |
| :--- | :--- |
| `model` | Chat model to use. Defaults to the secret's `model`. |
| `secret_name` | Name of the `openai` secret. Defaults to `sdb_ai_text_default_secret`. |
| `temperature` | Sampling temperature. The default depends on the function: 0.7 for `ai_generate`, 0.3 for `ai_translate` and 0 for the others. |
| `max_tokens` | Maximum number of tokens in the model's reply. Default 1024. |

Every argument except the per-row text must be a constant. A `NULL` text returns `NULL` without a request.

## `ai_generate` {#ai_generate}

`ai_generate(prompt [, system_prompt])` sends `prompt` to the chat model and returns its reply as `VARCHAR`:

<SqlLogicTest id="sql/functions/ai_ollama/generate" />

`system_prompt` replaces the default instruction, *"You are a helpful assistant. Provide a clear and concise response."*:

<SqlLogicTest id="sql/functions/ai_ollama/generate_system_prompt" hideResult />

## `ai_classify` {#ai_classify}

`ai_classify(text, categories)` returns the one label from the `VARCHAR[]` `categories` that best fits `text`. The labels must be unique and non-empty. A reply that matches no label, even case-insensitively, returns `NULL`:

<SqlLogicTest id="sql/functions/ai_ollama/classify" />

## `ai_extract` {#ai_extract}

`ai_extract(text, instruction_or_schema)` has two modes. With a free-form instruction it returns the extracted value as `VARCHAR`, or `NULL` when the text doesn't contain it:

<SqlLogicTest id="sql/functions/ai_ollama/extract" />

When the second argument is a JSON object, each key names a field and each value describes it. The function then returns `JSON` with exactly those keys, and `null` for any value the text doesn't contain:

<SqlLogicTest id="sql/functions/ai_ollama/extract_schema" />

## `ai_filter` {#ai_filter}

`ai_filter(text, condition)` returns `true` when the model decides that `condition` holds for `text`, and `false` otherwise, so it fits in a `WHERE` clause:

<SqlLogicTest id="sql/functions/ai_ollama/filter" />

## `ai_translate` {#ai_translate}

`ai_translate(text, target_language [, instructions])` translates `text` into `target_language`, given as a language name or a BCP-47 code. `instructions` adds guidance such as *"Use the polite form"*:

<SqlLogicTest id="sql/functions/ai_ollama/translate" />

## `ai_redact` {#ai_redact}

`ai_redact(text, categories [, replacement])` rewrites `text` with every occurrence of the listed kinds of personal information replaced by `replacement` (default `[REDACTED]`). An empty `categories` array stands for person names, email addresses, phone numbers, postal addresses, credit card numbers and IP addresses. Control characters other than tab, newline and carriage return are replaced by spaces before the text is sent.

<SqlLogicTest id="sql/functions/ai_ollama/redact" hideResult />

## `ai_embed` {#ai_embed}

`ai_embed(text, model [, secret_name] [, dimensions])` sends `text` to the embedding `model` and returns the embedding as a `FLOAT[]`. The vector's length is the model's embedding dimension, 384 for `all-minilm`:

<SqlLogicTest id="sql/functions/ai_ollama/embed_dim" />

A `NULL` `text` returns `NULL`, so rows without text are simply skipped:

<SqlLogicTest id="sql/functions/ai_ollama/embed_null" />

| Argument | Description |
| :--- | :--- |
| `text` | The text to embed. `NULL` yields `NULL`. |
| `model` | The provider's embedding model name, for example `'all-minilm'` or `'text-embedding-3-small'`. |
| `secret_name` | Name of the `openai` secret. Defaults to `sdb_ai_embedding_default_secret`. |
| `dimensions` | Requested vector size, for models that can shorten their embeddings (such as OpenAI's `text-embedding-3-*`). 0 or omitted keeps the native size. |

**Returns** a variable-length `FLOAT[]`. To store embeddings in an [IVF vector column](../indexes/inverted/vector-search.md), which requires a *fixed* size, cast to `FLOAT[N]` with the model's dimension, for example `ai_embed(...)::FLOAT[384]`. Every stored row and the query vector must use the **same model and dimension**, or the index and the distance comparisons will not line up.

### Choosing a model

The embedding dimension `N` is fixed by the model. A few common ones:

| Model | Provider | Dimension `N` |
| :--- | :--- | :---: |
| `text-embedding-3-small` | OpenAI | 1536 |
| `text-embedding-3-large` | OpenAI | 3072 |
| `all-minilm` (all-MiniLM-L6-v2) | Ollama / local | 384 |

Check your provider's documentation for the exact dimension and use it as the `N` in the stored `FLOAT[N]` column. Match the index's [distance metric](../indexes/inverted/vector-search.md) to how the model's vectors are meant to be compared. Most text-embedding models are tuned for **cosine** similarity.

### Performance

Each `ai_embed` call is a network request to the provider, so **embed documents once at write time** and store the vectors; only the *query* text is embedded at search time. Rows are sent in batches of up to `sdb_ai_embedding_max_batch_size` texts per request. Embedding a column is just a `SELECT`, and `NULL`s pass through and are easy to count or filter:

<SqlLogicTest id="sql/functions/ai_ollama/embed_table" />

## `ai_similarity` {#ai_similarity}

`ai_similarity(text1, text2, model [, secret_name] [, dimensions])` embeds both texts with `model` and returns their cosine similarity as a `DOUBLE` in `[-1, 1]`. It returns `NULL` when either text is `NULL` or empty. The arguments after the texts work as in [`ai_embed`](#ai_embed):

<SqlLogicTest id="sql/functions/ai_ollama/similarity" />

For ranking many rows against one query, store the embeddings and use [vector search](../indexes/inverted/vector-search.md) instead; `ai_similarity` embeds both texts on every call.

## `prompt_jev` {#prompt_jev}

`prompt_jev` asks a Jev decision model a closed question about a text and returns a typed answer: a probability, a choice from a fixed list, or a position on an ordered scale. It uses the [TypeSafe System One API](https://docs.typesafe.ai/), which a local [Kev](https://github.com/jaredpalmer/kev) server also serves.

```sql
prompt_jev(input, instructions [, noul | choice | score] [, batch_size] [, model] [, secret_name])
prompt_jev(input, questions := ... [, model] [, secret_name])
```

`input` is the per-row text; every other argument must be a constant. The question type is set by which of `noul`, `choice` and `score` you pass. They can't be combined, and passing none asks a `noul` question.

| Question | Criteria | Returns |
| :--- | :--- | :--- |
| `noul` (yes/no) | optional, exactly the labels `true` and `false` | `DOUBLE`, the probability that the answer is yes |
| `choice` | 2 to 255 labels | `STRUCT(choice VARCHAR, probabilities STRUCT(value VARCHAR, probability DOUBLE)[], confidence DOUBLE)` |
| `score` | 2 to 10 levels, lowest first | `STRUCT(score DOUBLE, probabilities STRUCT(index INTEGER, value VARCHAR, probability DOUBLE)[], confidence DOUBLE)` |

`probabilities` lists every label in the order given. For `score`, `score` is the expected level index, between 0 and the number of levels minus one, so 1.4 sits between the second and third level. `confidence` is between 0 and 1 and shows how concentrated the probabilities are.

A `noul` question returns the probability of *yes*:

<SqlLogicTest id="sql/functions/ai_kev/noul" />

A `choice` question picks one label:

<SqlLogicTest id="sql/functions/ai_kev/choice" />

Criteria are either a `VARCHAR[]` of labels, or a `STRUCT(label VARCHAR, description VARCHAR)[]` that explains what each label means. Labels must be unique and non-empty; a description may be `NULL`:

<SqlLogicTest id="sql/functions/ai_kev/choice_descriptions" />

A `score` question rates the text on an ordered scale:

<SqlLogicTest id="sql/functions/ai_kev/score" />

### Several questions at once {#prompt_jev_questions}

`questions` asks several questions about the same text in one request and returns a `STRUCT` with one field per question, each in its type's shape above. Each question is a `STRUCT` with `type`, `instructions` and, for `choice` and `score`, `criteria`. `questions` can't be combined with `instructions`, `noul`, `choice`, `score` or `batch_size`.

<SqlLogicTest id="sql/functions/ai_kev/questions" />

`questions` can also be a JSON object in the API's own format, which allows structured instructions and criteria descriptions with fields such as `examples`:

<SqlLogicTest id="sql/functions/ai_kev/questions_json" />

### Batching {#prompt_jev_batch}

A single-question call packs up to `batch_size` rows (1 to 64, default 32) into one request. Packing sends fewer requests, but the model sees several rows at once, so answers can drift compared with asking about each row alone. Set `batch_size := 1` for strict per-row isolation. If the provider rejects a packed request as invalid (HTTP 422, for example because the batch is too long), the batch is split in half and retried, down to single rows. `questions` calls always send one request per row.

<SqlLogicTest id="sql/functions/ai_kev/table" />

<SqlLogicTest id="sql/functions/ai_kev/batch_size" hideResult />

Because each call is a request, materialize results you reuse, for example in a table or a `MATERIALIZED` CTE, instead of calling `prompt_jev` again.

## Errors, retries and quotas {#errors}

These settings apply to every AI function:

| Setting | Default | Description |
| :--- | :--- | :--- |
| `sdb_ai_throw_on_error` | `true` | When `false`, a row whose request fails returns `NULL` instead of failing the query. HTTP 401, 403, 404 and 422 always fail the query, because they mean a wrong key, endpoint, model or question. |
| `sdb_ai_max_retries` | `3` | Retries after a connection error or HTTP 408, 429, 5xx or 529. |
| `sdb_ai_retry_initial_delay_ms` | `500` | Delay before the first retry; each further retry doubles it. A `Retry-After` response header takes precedence. |
| `sdb_ai_request_timeout` | `120` | Timeout of a single request, in seconds. |
| `sdb_ai_max_concurrent_requests` | `16` | Requests one executing thread keeps in flight at once. Lower it for a local server that can't keep up. |
| `sdb_ai_max_api_calls_per_query` | `0` | Maximum requests a query may send. 0 = unlimited. |
| `sdb_ai_max_output_tokens_per_query` | `0` | Maximum output tokens a query may consume, as reported by the provider. The check happens before each request, so requests already in flight can go over it. 0 = unlimited. |
| `sdb_ai_throw_on_quota_exceeded` | `true` | When `false`, rows after a quota is exhausted return `NULL` instead of failing the query. |
| `sdb_ai_embedding_max_batch_size` | `64` | Maximum texts per embeddings request. |

There is no input-token quota: the number of input tokens is only known to the provider.

<SqlLogicTest id="sql/functions/ai_ollama/quota" />

## End-to-end: semantic search

Embed each row once, store the vector in a fixed-size `FLOAT[N]` column and build an [IVF](../indexes/inverted/vector-search.md) index over it:

<SqlLogicTest id="sql/functions/ai_ollama/build_index" />

Then embed the query text at search time and rank by vector distance. The embedding model maps semantically related words close together:

```sql
SELECT id, name
FROM catalog_idx
ORDER BY embedding <-> ai_embed('tropical fruit', 'all-minilm', 'local_ai')::FLOAT[384]
LIMIT 3;
```

Because `name` is also full-text indexed in the same index, you can pair a lexical filter with semantic ranking; see [Hybrid Search](../indexes/inverted/hybrid-search.md).

## See also

- [Vector Search](../indexes/inverted/vector-search.md): IVF indexing and the `<->` operator
- [Hybrid Search](../indexes/inverted/hybrid-search.md): combine full-text filters with vector ranking
- [CREATE SECRET](../statements/create_secret/index.md): configure the provider
- [Secrets Manager](../../configuration/secrets_manager.md): secret types and scopes
