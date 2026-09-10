---
title: Async API
sidebar_position: 6
split: headings
---

# Async API

`AsyncSereneDBVectorStore` is the actual implementation. [`SereneDBVectorStore`](./vector-store.md) is a thin wrapper that forwards every call to it through the engine's background event loop, so the two behave identically and the parameter tables on the [Vector Store](./vector-store.md) page apply here too.

This page covers what is specific to the async class: how the loop is managed, the parameters and methods that exist only here, and the store's public attributes.

## Which class do I use? {#choosing}

| Situation | Use |
| :--- | :--- |
| A script, notebook or synchronous service | [`SereneDBVectorStore`](./vector-store.md) with [`create_sync()`](./vector-store.md#create_sync) |
| An `async` application — FastAPI, an async worker, an async LangChain chain | `AsyncSereneDBVectorStore` with [`create()`](#create) |
| Async setup code, but the store is consumed synchronously | `SereneDBVectorStore` with [`create()`](./vector-store.md#create) |
| You already own an `AsyncConnectionPool` | `AsyncSereneDBVectorStore` on an engine from [`from_pool()`](./engine.md#from_pool) |

:::caution
Awaiting an `a`-prefixed method on the **synchronous** store does not give you a native async path. `SereneDBVectorStore` overrides none of them, so `await sync_store.aadd_texts(...)` falls through to LangChain's default, which runs the *synchronous* method in a thread executor — which in turn hands the work back to the background loop. It works, but it is two hops and a thread you did not need. Use `AsyncSereneDBVectorStore` for real async work.
:::

## The event loop model {#event-loop}

[`SereneDBEngine.from_connection_string()`](./engine.md#from_connection_string) creates one event loop on a daemon thread the first time it is called, stores it on the class, and shares it across every engine built that way. The connection pool is constructed unopened and opened lazily on that loop, because an `AsyncConnectionPool` must be opened from inside the loop that drives it.

That indirection is what makes the synchronous API possible, and it is harmless for async callers: an `await` on an async method hands the coroutine to the background loop with `run_coroutine_threadsafe` and awaits the result on your own loop.

If you would rather everything ran on your loop, build the engine from your own pool:

```python
import asyncio

from psycopg_pool import AsyncConnectionPool

from langchain_serenedb import AsyncSereneDBVectorStore, SereneDBEngine

pool = AsyncConnectionPool(
    "host=127.0.0.1 port=7890 user=postgres dbname=postgres", open=False
)
await pool.open()

engine = SereneDBEngine.from_pool(pool, loop=asyncio.get_running_loop())
store = await AsyncSereneDBVectorStore.create(engine, embeddings, "my_docs")
```

Pass `loop` even for a purely async application: without it, any synchronous method — including the DDL helpers such as [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) — raises `Exception("Engine was initialized without a background loop and cannot call sync methods.")`. The pool must also already be open, since `from_pool()` marks the engine as opened.

## Creating an async store {#creating}

### `create()` {#create}

```python
store = await AsyncSereneDBVectorStore.create(
    engine, embedding_service, table_name, **kwargs
)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `engine` | `SereneDBEngine` | — | The engine to run on. |
| `embedding_service` | `Embeddings` | — | Any LangChain embeddings model. |
| `table_name` | `str` | — | Table to bind to. |
| `schema_name` | `str` | `"public"` | Schema the table lives in. |
| `content_column` | `str` | `"content"` | Column holding page content. |
| `embedding_column` | `str` | `"embedding"` | Column holding the vector. |
| `metadata_columns` | `Optional[list[str]]` | `None` | Typed metadata columns to read and write. |
| `ignore_metadata_columns` | `Optional[list[str]]` | `None` | Treat every other column as metadata. Mutually exclusive with `metadata_columns`. |
| `id_column` | `str` | `"langchain_id"` | Primary key column. |
| `metadata_json_column` | `Optional[str]` | `"langchain_metadata"` | JSON metadata column; set to `None` if absent from the table. |
| `distance_strategy` | `DistanceStrategy` | `COSINE_DISTANCE` | Distance measure. See [`DistanceStrategy`](./indexes.md#distancestrategy). |
| `k` | `int` | `4` | Default number of results. |
| `fetch_k` | `int` | `20` | Default MMR candidate pool. |
| `lambda_mult` | `float` | `0.5` | Default MMR diversity weight. |
| `index_query_options` | `Optional[QueryOptions]` | `None` | See [`IVFQueryOptions`](./indexes.md#ivfqueryoptions). |
| `hybrid_search_config` | `Optional[HybridSearchConfig]` | `None` | Query-time fusion settings; the combined index must already exist. See [Hybrid Search](./hybrid-search.md). |
| `metadata_index` | `Optional[MetadataIndexConfig]` | `None` | See [Metadata index configuration](./indexes.md#metadata-index). |
| `sync_load` | `bool` | `True` | When `False`, writes do not auto-refresh the inverted index. |

The column validation and its error messages are the same as for the synchronous factory — see [Creating a store](./vector-store.md#creating).

`ignore_metadata_columns` is the inverse of `metadata_columns`: the metadata set becomes every column in the table except the ignored ones and the id, content and embedding columns. Giving both raises `ValueError("Can not use both metadata_columns and ignore_metadata_columns.")`.

Calling `AsyncSereneDBVectorStore(...)` directly raises — always go through `create()`.

## Sync ↔ async method map {#methods}

| Synchronous | Asynchronous | Notes |
| :--- | :--- | :--- |
| [`create_sync()`](./vector-store.md#create_sync) | [`create()`](#create) | The async form adds `ignore_metadata_columns`. |
| [`add_texts()`](./vector-store.md#add_texts) | `aadd_texts()` | |
| [`add_documents()`](./vector-store.md#add_documents) | `aadd_documents()` | |
| — | [`aadd_embeddings()`](#aadd_embeddings) | Async only. |
| [`delete()`](./vector-store.md#delete) | `adelete()` | |
| [`similarity_search()`](./vector-store.md#similarity_search) | `asimilarity_search()` | |
| [`similarity_search_with_score()`](./vector-store.md#similarity_search_with_score) | `asimilarity_search_with_score()` | |
| [`similarity_search_by_vector()`](./vector-store.md#similarity_search_by_vector) | `asimilarity_search_by_vector()` | |
| — | [`asimilarity_search_with_score_by_vector()`](#asimilarity_search_with_score_by_vector) | Async only. |
| [`max_marginal_relevance_search()`](./vector-store.md#max_marginal_relevance_search) | `amax_marginal_relevance_search()` | |
| — | [`amax_marginal_relevance_search_by_vector()`](#ammr_by_vector) | Async only. |
| — | [`amax_marginal_relevance_search_with_score_by_vector()`](#ammr_by_vector) | Async only. |
| [`get_by_ids()`](./vector-store.md#get_by_ids) | `aget_by_ids()` | |
| [`apply_vector_index()`](./indexes.md#apply_vector_index) | `aapply_vector_index()` | |
| [`apply_hybrid_search_index()`](./indexes.md#apply_hybrid_search_index) | `aapply_hybrid_search_index()` | `index_config` and `concurrently` are keyword-only on the sync form, positional-or-keyword on the async one. |
| [`drop_vector_index()`](./indexes.md#drop_vector_index) | `adrop_vector_index()` | |
| [`reindex()`](./indexes.md#reindex) | `areindex()` | Both raise `NotImplementedError` for now. |
| [`is_valid_index()`](./indexes.md#is_valid_index) | `is_valid_index()` | Same name; on this class it is a coroutine. |
| — | [`afrom_texts()`](#afrom_texts) / [`afrom_documents()`](#afrom_texts) | Async only. The sync `from_texts()` raises. |

Three synchronous methods exist on this class only to satisfy LangChain's `VectorStore` interface, and raise `NotImplementedError` if called:

| Method | Message |
| :--- | :--- |
| `add_texts()` | `Use SereneDBVectorStore for sync access, or await aadd_texts.` |
| `similarity_search()` | `Use SereneDBVectorStore for sync access, or await asimilarity_search.` |
| `from_texts()` | `Use afrom_texts.` |

## Async-only methods {#async-only}

### `aadd_embeddings()` {#aadd_embeddings}

```python
ids = await store.aadd_embeddings(texts, embeddings, metadatas=None, ids=None)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `texts` | `Iterable[str]` | — | Page content for each row. |
| `embeddings` | `list[list[float]]` | — | One pre-computed vector per text. |
| `metadatas` | `Optional[list[dict]]` | `None` | One metadata dict per text. |
| `ids` | `Optional[list]` | `None` | One id per text; missing entries get a UUID4 string. |

Writes rows with vectors you already have, skipping the embeddings model entirely — useful when embeddings arrive from a batch job or another service. This is the method [`aadd_texts()`](#methods) delegates to.

The vector dimension is taken from `embeddings[0]` and bound as `%(embedding)s::FLOAT[N]`, so every vector in the batch must have the same length as the column. Like the other writes, it upserts on the id column and refreshes the index unless `sync_load=False`.

### `asimilarity_search_with_score_by_vector()` {#asimilarity_search_with_score_by_vector}

```python
pairs = await store.asimilarity_search_with_score_by_vector(
    embedding, k=None, filter=None, **kwargs
)
```

Search by vector, returning `(Document, float)` pairs where the float is a vector distance — see [Score semantics](./vector-store.md#scores).

Like its sync counterpart this path is always dense: a store-level `hybrid_search_config` is bypassed, and passing `fts_query` or `hybrid_search_config` raises `NotImplementedError`. Use `asimilarity_search()` for [hybrid ranking](./hybrid-search.md#scored-hybrid).

### `amax_marginal_relevance_search_by_vector()` and `..._with_score_by_vector()` {#ammr_by_vector}

```python
docs = await store.amax_marginal_relevance_search_by_vector(
    embedding, k=None, fetch_k=None, lambda_mult=None, filter=None
)
pairs = await store.amax_marginal_relevance_search_with_score_by_vector(
    embedding, k=None, fetch_k=None, lambda_mult=None, filter=None
)
```

MMR starting from a vector rather than a query string. The `_with_score` variant returns the candidates' distances alongside the selected documents. Both always take the [dense path](./vector-store.md#dense).

### `afrom_texts()` and `afrom_documents()` {#afrom_texts}

```python
store = await AsyncSereneDBVectorStore.afrom_texts(
    texts, embedding, engine, table_name, metadatas=None, ids=None, **kwargs
)
store = await AsyncSereneDBVectorStore.afrom_documents(
    documents, embedding, engine, table_name, ids=None, **kwargs
)
```

Convenience constructors that call [`create()`](#create) and then write the given texts or documents. Note the signature: unlike LangChain's base `from_texts`, both require `engine` and `table_name`, and the **table must already exist** — they do not create it. Extra keywords are forwarded to `create()`.

## Store attributes {#attributes}

Every configuration value is a plain public attribute on the store, readable after construction.

| Attribute | Type | Notes |
| :--- | :--- | :--- |
| `engine` | `SereneDBEngine` | |
| `embedding_service` | `Embeddings` | Also exposed as the `embeddings` property, per the `VectorStore` interface. |
| `table_name` | `str` | |
| `schema_name` | `str` | |
| `content_column` | `str` | |
| `embedding_column` | `str` | |
| `metadata_columns` | `dict[str, Optional[str]]` | An ordered map of column name to declared SQL type — **not** the `list[str]` that `create()` accepts. Iterating it yields the names. |
| `id_column` | `str` | |
| `metadata_json_column` | `Optional[str]` | `None` when the table has no JSON metadata column. |
| `distance_strategy` | `DistanceStrategy` | |
| `k` | `int` | |
| `fetch_k` | `int` | |
| `lambda_mult` | `float` | |
| `index_query_options` | `Optional[QueryOptions]` | |
| `hybrid_search_config` | `Optional[HybridSearchConfig]` | |
| `metadata_index` | `Optional[MetadataIndexConfig]` | |
| `sync_load` | `bool` | |

Assigning to these after construction is not supported — the store derives internal state (the distance-column alias, for one) from them at construction time.

## Full example {#example}

```python
import asyncio

from langchain_core.embeddings import DeterministicFakeEmbedding

from langchain_serenedb import AsyncSereneDBVectorStore, IVFIndex, SereneDBEngine

CONNINFO = "host=127.0.0.1 port=7890 user=postgres dbname=postgres"


async def main() -> None:
    embeddings = DeterministicFakeEmbedding(size=768)
    engine = SereneDBEngine.from_connection_string(CONNINFO)

    await engine.ainit_vectorstore_table(
        "async_docs", 768, overwrite_existing=True, vector_index=IVFIndex()
    )

    store = await AsyncSereneDBVectorStore.create(engine, embeddings, "async_docs")

    await store.aadd_texts(
        ["SereneDB indexes vectors with IVF.", "BM25 ranks full-text matches."],
        metadatas=[{"topic": "vector"}, {"topic": "text"}],
    )

    docs = await store.asimilarity_search("how are vectors indexed?", k=2)
    for doc in docs:
        print(doc.page_content, doc.metadata)

    await engine.adrop_table("async_docs")
    await engine.aclose()


asyncio.run(main())
```

## See also

- [Vector Store](./vector-store.md) — the shared parameter and method reference
- [Engine and Tables](./engine.md#from_pool) — bringing your own pool
- [FAQ](./faq.md)
