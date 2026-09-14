---
title: Vector Store
sidebar_position: 2
split: headings
---

# Vector Store

`SereneDBVectorStore` is the synchronous LangChain `VectorStore` for SereneDB. It is a thin wrapper around [`AsyncSereneDBVectorStore`](./async.md), delegating each call to the async implementation through the engine's background event loop, so both classes behave identically.

A store binds to a table that already exists. Create the table with [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) first.

| Method | Purpose |
| :--- | :--- |
| [`create_sync()`](#create_sync) | Bind a store to an existing table. |
| [`add_texts()`](#add_texts) / [`add_documents()`](#add_documents) | Embed and upsert rows. |
| [`delete()`](#delete) | Delete by id and/or metadata filter. |
| [`similarity_search()`](#similarity_search) | Dense (or hybrid) search by query text. |
| [`similarity_search_with_score()`](#similarity_search_with_score) | Dense search with the distance. Never hybrid. |
| [`similarity_search_by_vector()`](#similarity_search_by_vector) | Search with an embedding you already have. |
| [`max_marginal_relevance_search()`](#max_marginal_relevance_search) | Diversity-aware search (MMR). |
| [`get_by_ids()`](#get_by_ids) | Fetch documents by primary key. |
| [Index management](#index-management) | Build, drop and inspect the index. |

## Creating a store {#creating}

### `create_sync()` {#create_sync}

```python
SereneDBVectorStore.create_sync(engine, embedding_service, table_name, **kwargs)
```

The synchronous factory. `engine`, `embedding_service` and `table_name` are positional; every remaining keyword is forwarded to [`AsyncSereneDBVectorStore.create()`](./async.md#create), so the full option set is available here — including `ignore_metadata_columns`, which is the one keyword missing from [`create()`](#create)'s explicit signature.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `engine` | `SereneDBEngine` | — | The engine to run on. |
| `embedding_service` | `Embeddings` | — | Any LangChain embeddings model. Its dimension must match the table's `FLOAT[N]`. |
| `table_name` | `str` | — | Table to bind to. |
| `schema_name` | `str` | `"public"` | Schema the table lives in. |
| `content_column` | `str` | `"content"` | Column holding page content. |
| `embedding_column` | `str` | `"embedding"` | Column holding the vector. |
| `metadata_columns` | `Optional[list[str]]` | `None` | Typed metadata columns to read and write. Mutually exclusive with `ignore_metadata_columns`. |
| `ignore_metadata_columns` | `Optional[list[str]]` | `None` | Treat every column except these (and id/content/embedding) as a metadata column. |
| `id_column` | `str` | `"langchain_id"` | Primary key column. |
| `metadata_json_column` | `Optional[str]` | `"langchain_metadata"` | JSON metadata column. Downgraded to `None` if the table has no such column. |
| `distance_strategy` | `DistanceStrategy` | `COSINE_DISTANCE` | Distance measure for search. See [`DistanceStrategy`](./indexes.md#distancestrategy). |
| `k` | `int` | `4` | Default number of results. |
| `fetch_k` | `int` | `20` | Default candidate pool for MMR. |
| `lambda_mult` | `float` | `0.5` | Default MMR diversity weight (`0` = maximal diversity, `1` = pure relevance). |
| `index_query_options` | `Optional[QueryOptions]` | `None` | Per-query index tuning. See [`IVFQueryOptions`](./indexes.md#ivfqueryoptions). |
| `hybrid_search_config` | `Optional[HybridSearchConfig]` | `None` | Query-time fusion settings. The combined index must already exist — see [Hybrid Search](./hybrid-search.md). |
| `metadata_index` | `Optional[MetadataIndexConfig]` | `None` | Which indexed metadata this store filters on. May be a subset of what the index covers, but each declared field must use the type the index was built with. |
| `sync_load` | `bool` | `True` | When `False`, writes do not auto-refresh the inverted index. |

**Validation.** The factory queries `information_schema.columns` and fails fast if the table does not match:

| Condition | Error |
| :--- | :--- |
| Table missing, or has no columns | `Table "<schema>"."<table>" does not exist or has no columns.` |
| `id_column` absent | `Id column, <name>, does not exist.` |
| `content_column` absent | `Content column, <name>, does not exist.` |
| `content_column` is not a character type | `Content column, <name>, is type, <type>. It must be a type of character string.` |
| `embedding_column` absent | `Embedding column, <name>, does not exist.` |
| `embedding_column` is not an array | `Embedding column, <name>, is type <type>. It must be a FLOAT[N] array.` |
| A name in `metadata_columns` is absent | `Metadata column, <name>, does not exist.` |
| Both `metadata_columns` and `ignore_metadata_columns` given | `Can not use both metadata_columns and ignore_metadata_columns.` |

SereneDB reports a `FLOAT[N]` column as `data_type = 'ARRAY'`; the check also accepts `USER-DEFINED` and `vector`.

:::note
A missing `metadata_json_column` is **not** an error — it is silently set to `None`. That is what makes a store over a `store_metadata=False` table work, but it also means a typo in the column name degrades to "no JSON metadata" instead of raising.
:::

### `create()` {#create}

```python
store = await SereneDBVectorStore.create(engine, embedding_service, table_name, ...)
```

An `async` factory that returns the same **synchronous** store. Use it when your setup code is already `async` but the store will be consumed synchronously. It takes the parameters listed above with the sole exception of `ignore_metadata_columns`. For an actual async store, use [`AsyncSereneDBVectorStore.create()`](./async.md#create).

## Adding documents {#adding}

### `add_texts()` {#add_texts}

```python
ids = store.add_texts(texts, metadatas=None, ids=None, **kwargs)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `texts` | `Iterable[str]` | — | Page content for each row. |
| `metadatas` | `Optional[list[dict]]` | `None` | One metadata dict per text. Defaults to empty dicts. |
| `ids` | `Optional[list]` | `None` | One id per text. Missing or `None` entries get a fresh UUID4 string. |

Returns the list of ids actually written.

**How it works.** The texts are embedded with `embedding_service.aembed_documents()`, then written as a single `INSERT ... ON CONFLICT (<id_column>) DO UPDATE SET ...` executed once for the whole batch — one connection checkout, one transaction. This means the write is an **upsert**, not an append: reusing an id replaces that row's content, embedding and metadata.

Metadata is routed per key: a key matching a declared metadata column is bound to that column (a `dict` value is JSON-serialized), and what remains goes into the JSON column. A declared column with no matching key binds `NULL`.

Unless the store was created with `sync_load=False`, the write ends with a [`refresh_table()`](./engine.md#refresh_table) so the new rows are immediately searchable.

For a bulk load, skip the per-batch refresh and build the index afterwards:

```python
store = SereneDBVectorStore.create_sync(engine, embeddings, "my_docs", sync_load=False)

for batch in batches:
    store.add_texts(batch)

engine.refresh_table("my_docs")
store.apply_vector_index(IVFIndex())
```

### `add_documents()` {#add_documents}

```python
ids = store.add_documents(documents, ids=None, **kwargs)
```

Takes `list[Document]` and forwards `page_content` and `metadata` to [`add_texts()`](#add_texts). When `ids` is not given, each document's own `Document.id` is used (and `None` entries still get a UUID4).

## Deleting {#deleting}

### `delete()` {#delete}

```python
store.delete(ids=None, filter=None)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `ids` | `Optional[list]` | `None` | Ids to delete. |
| `filter` | `Optional[dict]` | `None` | A [metadata filter](./filtering.md); rows matching it are deleted. |

Both may be given, in which case they are ANDed — the deleted rows are those whose id is in the list **and** which match the filter. Returns `True` when a `DELETE` was issued, and `False` when neither argument was supplied (nothing is deleted in that case). Refreshes the index afterwards unless `sync_load=False`.

```python
store.delete(filter={"category": {"$in": ["draft", "archived"]}})
```

## Searching {#searching}

Every search accepts `filter`, a [metadata filter dict](./filtering.md). Passing `k=None` (the default) uses the store's configured `k`; the same applies to `fetch_k` and `lambda_mult` for MMR.

### Dense and hybrid searches {#dense}

A search takes one of two paths, and the rest of these docs refer to them by name:

- A **dense** search ranks purely by vector distance — it embeds the query (or takes your embedding), orders by the [distance strategy](./indexes.md#distancestrategy)'s operator and returns the nearest `k`. "Dense" is the usual name for this because an embedding is a dense vector, as opposed to the sparse term vector a keyword index works with. This is what every search method does by default.
- A **hybrid** search additionally runs a lexical BM25 branch over the content column and fuses the two rankings into one order. It happens only when the store has a [`hybrid_search_config`](./hybrid-search.md) *and* a full-text query can be resolved for the call.

The distinction shows up in three places worth knowing about: the two paths report [scores with opposite polarity](#scores); a dense search falls back to an exact table scan when the index is missing, while a hybrid search fails outright; and [MMR](#max_marginal_relevance_search) is always dense, even on a hybrid store.

The dense path reads from the inverted index by name when the index exists, and falls back to an exact scan of the base table when it does not — so search works before the index is built, just more slowly.

### `similarity_search()` {#similarity_search}

```python
docs = store.similarity_search(query, k=None, filter=None, **kwargs)
```

Embeds `query`, then returns the `k` nearest documents. When the store has a [`hybrid_search_config`](./hybrid-search.md), the query text is also passed as the full-text query, so this one call runs the fused BM25 + vector search.

### `similarity_search_with_score()` {#similarity_search_with_score}

```python
pairs = store.similarity_search_with_score(query, k=None, filter=None, **kwargs)
```

Returns `list[tuple[Document, float]]`, where the float is a **vector distance** — see [Score semantics](#scores).

This path is always dense. Even on a store configured for [hybrid search](./hybrid-search.md), it runs the vector query and reports a real distance, because a fused ranking is not a distance and would make the number meaningless. Passing `fts_query` or `hybrid_search_config` here raises `NotImplementedError`; use [`similarity_search()`](#similarity_search) for hybrid ranking.

### `similarity_search_by_vector()` {#similarity_search_by_vector}

```python
docs = store.similarity_search_by_vector(embedding, k=None, filter=None, **kwargs)
```

Skips the embedding step and searches with the vector you supply. Its length must match the table's `FLOAT[N]`.

:::note
This method has no query text to derive a full-text query from, so on a hybrid store it runs the dense path by default. Pass `fts_query="..."` explicitly to get the fused query — see [Choosing the query text](./hybrid-search.md#fts-query).
:::

### `max_marginal_relevance_search()` {#max_marginal_relevance_search}

```python
docs = store.max_marginal_relevance_search(
    query, k=None, fetch_k=None, lambda_mult=None, filter=None, **kwargs
)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `query` | `str` | — | Query text. |
| `k` | `Optional[int]` | store's `k` | Documents to return. |
| `fetch_k` | `Optional[int]` | store's `fetch_k` | Candidates to retrieve before re-selecting. |
| `lambda_mult` | `Optional[float]` | store's `lambda_mult` | `0` maximizes diversity, `1` maximizes relevance. |

Fetches `fetch_k` nearest candidates, then re-selects `k` of them with maximal marginal relevance so the results are less redundant. The selection runs client-side over the candidates' embeddings.

:::caution
MMR always takes the dense path, even on a hybrid store — it never consults the lexical branch.
:::

### `get_by_ids()` {#get_by_ids}

```python
docs = store.get_by_ids(ids)
```

Fetches documents by primary key, in the order the ids were requested. Ids with no matching row are skipped rather than raising, so the result can be shorter than the input.

### Score semantics {#scores}

A dense search reports a **distance**: lower is closer. The value is exactly what the strategy's named SQL function returns.

| `distance_strategy` | Reported value | Range |
| :--- | :--- | :--- |
| `COSINE_DISTANCE` | `cosine_distance` | `0` (identical) to `2` |
| `EUCLIDEAN` | `l2_distance` | `0` upwards |
| `INNER_PRODUCT` | `negative_inner_product`, i.e. `-IP` | unbounded; smaller is more similar |
| `MANHATTAN` | `l1_distance` | `0` upwards |

Because inner product is reported *negated*, all four are genuine distances and LangChain's normalization helpers apply directly. `similarity_search_with_relevance_scores()` and the `similarity_score_threshold` retriever therefore work out of the box; the Manhattan case maps through `1 - d/2`, which assumes unit-normalized embeddings.

The reported value is always a distance, on every store. Scored search does not do [hybrid fusion](./hybrid-search.md#scores) — a fused value is a ranking rather than a distance, so mixing the two in one return type would make the number meaningless. On a hybrid store a scored call therefore runs the dense path and gives you real distances, and passing `fts_query` or `hybrid_search_config` to it raises `NotImplementedError`. See [Asking for a scored hybrid search](./hybrid-search.md#scored-hybrid).

The relevance mapping is also clamped to `[0, 1]`, so an exact match whose distance lands a hair below zero through floating-point error cannot push `1 - distance` above `1.0` and trip LangChain's range check.

## Using it as a retriever {#retriever}

`as_retriever()` is inherited from LangChain and works unchanged:

```python
retriever = store.as_retriever(
    search_type="similarity_score_threshold",
    search_kwargs={"score_threshold": 0.9, "k": 3},
)
docs = retriever.invoke("the quick brown fox")
```

All three search types are supported, and on a hybrid store they do not all use the lexical branch:

| `search_type` | Underlying call | Hybrid on a hybrid store? |
| :--- | :--- | :--- |
| `"similarity"` (default) | [`similarity_search()`](#similarity_search) | Yes |
| `"mmr"` | [`max_marginal_relevance_search()`](#max_marginal_relevance_search) | No — always dense |
| `"similarity_score_threshold"` | `similarity_search_with_relevance_scores()` | No — scored paths are dense, so the threshold applies to distance-derived relevance |

Anything the underlying method accepts can go in `search_kwargs`, including `filter`:

```python
retriever = store.as_retriever(
    search_kwargs={"k": 5, "filter": {"category": "science"}},
)
```

Because the score-threshold retriever goes through a scored path, do not put `fts_query` in its `search_kwargs` — that combination raises. Use the default `"similarity"` type when you want fused ranking.

## Index management {#index-management}

These methods live on the store but are documented with the classes they take, on [Indexes and Tuning](./indexes.md):

| Method | Purpose |
| :--- | :--- |
| [`apply_vector_index()`](./indexes.md#apply_vector_index) | Build the vector (or combined) inverted index. |
| [`apply_hybrid_search_index()`](./indexes.md#apply_hybrid_search_index) | Build the combined index for hybrid search. |
| [`drop_vector_index()`](./indexes.md#drop_vector_index) | Drop it. |
| [`reindex()`](./indexes.md#reindex) | Rebuild in place — raises `NotImplementedError` for now. |
| [`is_valid_index()`](./indexes.md#is_valid_index) | Whether the index currently exists. |

## Not implemented {#not-implemented}

`from_texts()` raises `NotImplementedError`, and `from_documents()` raises through it, because the table's shape — dimension, id type, metadata columns — cannot be inferred from texts alone:

```text
Initialize the table with SereneDBEngine.init_vectorstore_table, then use create_sync().
```

[`reindex()`](./indexes.md#reindex) also raises for now, because SereneDB's `REINDEX` applies to view-backed indexes rather than to the base table a store lives in. Drop and re-apply the index to rebuild it.

The synchronous class also does not implement `add_embeddings()`, `similarity_search_with_score_by_vector()`, `max_marginal_relevance_search_by_vector()` or `max_marginal_relevance_search_with_score_by_vector()`. They exist on the async store — see [Async-only methods](./async.md#async-only).

## See also

- [Metadata Filtering](./filtering.md) — the `filter` argument in full
- [Hybrid Search](./hybrid-search.md) · [Indexes and Tuning](./indexes.md) · [Async API](./async.md)
- [FAQ](./faq.md)
