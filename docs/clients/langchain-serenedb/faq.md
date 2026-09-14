---
title: FAQ and Troubleshooting
sidebar_label: FAQ
sidebar_position: 7
split: headings
---

# FAQ and Troubleshooting

Questions that come up when working with the integration, each with the cause and the fix. Every answer links to the reference section that covers the behavior in full.

## Quick index {#quick-index}

| Symptom | Fix |
| :--- | :--- |
| Rows written a moment ago are missing from results | [Refresh the index](#refresh) |
| A bulk load is slow | [Turn off the per-write refresh](#bulk-load) |
| Adding a document replaced an existing one | [Writes are upserts on the id column](#upsert) |
| `add_texts(ids=...)` fails on the id column | [Declare the id column as `VARCHAR`](#ids) |
| Search is slow even though the index exists | [Build the index through the store](#index-routing) |
| `reindex()` raises `NotImplementedError` | [Drop and re-apply the index instead](#reindex) |
| A metadata filter is not using the index | [Pass `metadata_index` in both places](#metadata-index-config) |
| A metadata column was left out of the index | [Its type is not inverted-indexable](#unindexable-types) |
| Hybrid search raises instead of scanning | [Build the combined index first](#hybrid-index) |
| `similarity_search_with_score()` ignores the hybrid config | [Scored search is dense by design](#hybrid-scores) |
| `fts_query` on a scored search raises | [Use `similarity_search()` for hybrid](#scored-hybrid) |
| MMR results ignore the keyword half | [MMR is always dense](#mmr) |
| `from_texts()` raises `NotImplementedError` | [Create the table, then `create_sync()`](#from-texts) |
| `await store.aadd_texts(...)` is slower than expected | [Use `AsyncSereneDBVectorStore`](#async) |
| An `IVFIndex` changed after being passed to the store | [It is mutated in place](#mutation) |
| `concurrently=True` still locks | [The flag is ignored](#concurrently) |

## Writes and visibility {#writes}

### Why don't rows I just added show up in search results? {#refresh}

SereneDB's inverted index is eventually consistent. A row written to the base table is invisible to full-text queries, and to vector queries routed through the index, until a refresh publishes it — either the index's own background `refresh_interval` (1000 ms by default) or an explicit `VACUUM (REFRESH_TABLE)`.

The store refreshes after every `add_*` and `delete` call, so normally you never see this. If you created the store with `sync_load=False`, publishing became your responsibility:

```python
engine.refresh_table("my_docs")
```

Note that the background refresh may publish the rows on its own before you get there, so a missing call shows up as an intermittent failure rather than a reproducible one.

See [Publishing writes](./engine.md#eventual-consistency) and [Visibility and the refresh model](../../sql/indexes/inverted/maintenance.md#visibility-and-the-refresh-model).

### How do I make a large bulk load faster? {#bulk-load}

The default refresh-after-every-write is right for incremental writes and wasteful for a bulk load, where it means one refresh per batch. Turn it off, refresh once at the end, and build the index last — SereneDB trains better IVF clusters when the data is already in place:

```python
store = SereneDBVectorStore.create_sync(engine, embeddings, "my_docs", sync_load=False)

for batch in batches:
    store.add_texts(batch)

engine.refresh_table("my_docs")
store.apply_vector_index(IVFIndex())
```

See [Adding documents](./vector-store.md#adding).

### Why did adding a document replace an existing one? {#upsert}

Writes are upserts. Each row is written with `INSERT ... ON CONFLICT (<id_column>) DO UPDATE`, so reusing an id replaces that row's content, embedding and metadata instead of adding a second row.

This also applies to [`add_documents()`](./vector-store.md#add_documents), which falls back to each `Document.id` when you do not pass `ids` explicitly. Pass fresh ids, or let the store generate them, if you want distinct rows.

### Why are the ids I supply rejected? {#ids}

`id_column="langchain_id"` — a plain string — declares the column as `UUID PRIMARY KEY`. The ids the store generates are UUID4 strings, so the default works as long as you let it assign them. Your own non-UUID ids fail at insert time.

Declare the column to match what you intend to write:

```python
from langchain_serenedb import Column

engine.init_vectorstore_table(
    "my_docs", 768, id_column=Column("langchain_id", "VARCHAR", nullable=False)
)
```

See [Choosing the id column type](./engine.md#id-column).

## Indexes {#indexes}

### Why is search still slow after I created the index? {#index-routing}

A [dense search](./vector-store.md#dense) reads from the inverted index only when it selects from the index *by name*; reading the base table always scans exactly. The store therefore has to know whether the index exists, and it finds out once — on the first search — then caches the answer for the lifetime of that store object.

That cache is updated by [`apply_vector_index()`](./indexes.md#apply_vector_index) and [`drop_vector_index()`](./indexes.md#drop_vector_index) and by nothing else. An index created with raw SQL, by a migration, or by another process *after* that first search will not be used by this store instance — searches keep scanning the table, correctly but slowly.

Either build the index through the store's own method, or construct a new store once the index exists.

### Why does `reindex()` raise `NotImplementedError`? {#reindex}

SereneDB's `REINDEX` applies to view-backed indexes, where it runs a refresh pass against the view's source. A vector store lives in a base table, so there is nothing for it to act on. Rather than quietly doing something adjacent, [`reindex()`](./indexes.md#reindex) raises until `REINDEX` on tables is supported.

To rebuild the index, drop it and apply it again:

```python
store.drop_vector_index()
store.apply_vector_index(IVFIndex())
```

If what you actually wanted was fresher relevance statistics, that is a separate operation you can run yourself — see [Recomputing index statistics](./indexes.md#statistics).

### Why does `concurrently=True` still lock the table? {#concurrently}

`concurrently` is accepted by [`apply_vector_index()`](./indexes.md#apply_vector_index) and [`apply_hybrid_search_index()`](./indexes.md#apply_hybrid_search_index) for API parity with other LangChain vector stores, and is never emitted in the DDL. The index build takes the same locks a plain `CREATE INDEX` takes. Plan the build accordingly rather than relying on the flag.

### Why isn't my metadata filter using the index? {#metadata-index-config}

`metadata_index` is read on two sides, and the store never recovers it from the database. [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) and [`apply_vector_index()`](./indexes.md#apply_vector_index) use it to build the index entries; the store's filter translator uses it to build the query expressions. For a predicate to push into the index scan, the query expression has to be the one the index was built with — same arrow, same cast.

For a JSON field, that means the store needs it declared, with the same `data_type`:

```python
metadata_index = MetadataIndexConfig(json_fields=[JsonFieldIndex("tag", "TEXT")])

engine.init_vectorstore_table("my_docs", 768, metadata_index=metadata_index)
store = SereneDBVectorStore.create_sync(
    engine, embeddings, "my_docs", metadata_index=metadata_index
)
```

The store's copy does not have to be the whole thing — it is consulted per field, so declaring only the fields this store filters on is fine. An omitted field still filters correctly, just without pushdown; a [full-text operator](./filtering.md#full-text) on an omitted field raises. A field declared with a *different* type on the two sides is the case to avoid — see [Making filters index-covered](./filtering.md#pushdown).

Hybrid search has no equivalent trap: its build-time and query-time settings are two different classes, [`HybridIndexConfig`](./hybrid-search.md#hybridindexconfig) and [`HybridSearchConfig`](./hybrid-search.md#hybridsearchconfig), each passed once to its own side.

### Why was one of my metadata columns left out of the index? {#unindexable-types}

When the index column set is auto-derived — `metadata_index` unset, or its `columns` left as `None` — a column whose declared type the inverted index cannot accept verbatim is silently skipped, so the automatic build can never fail at `CREATE INDEX`. The notable unindexable types are `NUMERIC` / `DECIMAL` / `HUGEINT`, `UUID`, `INTERVAL` and `VARIANT`.

Filters on such a column still return correct results; they run as a post-filter rather than in the index scan. A column you list explicitly in `columns` is never skipped — there the database decides, and an unindexable entry surfaces its DDL error.

See [Metadata index configuration](./indexes.md#metadata-index).

### Can I choose the index name? {#index-naming}

No. There is one index per collection and its name is always `<table_name>langchainvectorindex`, derived so the store always knows it without tracking user input. See [One index per collection](./indexes.md#one-index).

## Hybrid search {#hybrid}

### Why does hybrid search raise instead of falling back to a scan? {#hybrid-index}

Both branches of the fused query select from the combined index *by name* — `BM25` needs the index's `tableoid`, and `@@` only resolves against an indexed column. There is no exact-scan fallback, unlike a dense search.

So a store carrying a `HybridSearchConfig` fails at query time if no combined index was ever built for its table — giving the store a search config does not create one. Build it either with the table:

```python
engine.init_vectorstore_table("my_docs", 768, hybrid_index_config=HybridIndexConfig())
```

or afterwards, from the store:

```python
store.apply_hybrid_search_index()
```

Note that [`apply_vector_index()`](./indexes.md#apply_vector_index) will not do it — that always builds a plain vector index.

And pass the same `cfg` to the store factory as well — with the config on only one side, searches silently run the plain dense path instead. See [Setting it up](./hybrid-search.md#setup).

### Why did `similarity_search_with_score()` ignore my hybrid config? {#hybrid-scores}

Because scored search is deliberately dense-only. Hybrid fusion produces a *ranking* — under the default RRF it is a sum of reciprocal ranks, carrying no distance semantics at all — so returning it in the slot where callers expect a distance would make the number meaningless and would break the relevance helpers built on it.

So on a hybrid store, [`similarity_search_with_score()`](./vector-store.md#similarity_search_with_score) bypasses the config and reports a genuine vector distance. Use [`similarity_search()`](./vector-store.md#similarity_search) when you want the fused ordering.

### Why does passing `fts_query` to a scored search raise? {#scored-hybrid}

Same reason, stated explicitly rather than silently. `fts_query` or `hybrid_search_config` on `similarity_search_with_score()` or `similarity_search_with_score_by_vector()` raises `NotImplementedError`:

```text
Scored search does not support hybrid fusion: fused scores are rankings, not
distances. Use similarity_search()/as_retriever() for hybrid ranking, or drop
fts_query/hybrid_search_config for distance scores.
```

Drop the argument for distance scores, or move to `similarity_search()` for hybrid ranking. Note this also applies to the `similarity_score_threshold` retriever, which goes through a scored path — don't put `fts_query` in its `search_kwargs`. See [Asking for a scored hybrid search](./hybrid-search.md#scored-hybrid).

### Why don't my MMR results reflect the keyword half? {#mmr}

[`max_marginal_relevance_search()`](./vector-store.md#max_marginal_relevance_search) always takes the dense path, even when the store has a `hybrid_search_config`. It never consults the lexical branch, and its scores stay distances.

If you need hybrid retrieval, use [`similarity_search()`](./vector-store.md#similarity_search). There is no hybrid MMR.

## Constructors, sync and async {#api}

### Why does `from_texts()` raise `NotImplementedError`? {#from-texts}

The table's shape — embedding dimension, id column type, metadata columns — cannot be inferred from a list of texts, so the integration will not guess it. Create the table first:

```python
engine.init_vectorstore_table("my_docs", 768, vector_index=IVFIndex())
store = SereneDBVectorStore.create_sync(engine, embeddings, "my_docs")
store.add_texts(texts)
```

`from_documents()` raises through the same path. On the async class, [`afrom_texts()`](./async.md#afrom_texts) and `afrom_documents()` do work, but they also require an existing table. See [Not implemented](./vector-store.md#not-implemented).

### Why is awaiting a method on `SereneDBVectorStore` slower than expected? {#async}

`SereneDBVectorStore` overrides none of the `a`-prefixed methods, so `await sync_store.aadd_texts(...)` falls through to LangChain's default implementation, which runs the *synchronous* method in a thread executor — which in turn hands the work back to the engine's background loop. It gives correct results through two hops and a thread that is not needed.

Use [`AsyncSereneDBVectorStore`](./async.md) for async work; it is the native implementation.

### Which methods exist only on the async class? {#async-only}

`aadd_embeddings()`, `asimilarity_search_with_score_by_vector()`, `amax_marginal_relevance_search_by_vector()`, `amax_marginal_relevance_search_with_score_by_vector()`, `afrom_texts()` and `afrom_documents()`. See [Async-only methods](./async.md#async-only).

In the other direction, three synchronous methods on `AsyncSereneDBVectorStore` exist only to satisfy LangChain's interface and raise if called: `add_texts()`, `similarity_search()` and `from_texts()`.

### Why did the `IVFIndex` I passed change? {#mutation}

[`apply_vector_index()`](./indexes.md#apply_vector_index) overwrites `index.distance_strategy` with the store's strategy, so that the index metric always matches the query operator. The change persists on your object:

```python
index = IVFIndex(distance_strategy=DistanceStrategy.EUCLIDEAN)
store_cosine.apply_vector_index(index)
index.distance_strategy  # now DistanceStrategy.COSINE_DISTANCE
```

Build a fresh `IVFIndex` per store rather than sharing one instance.

[`init_vectorstore_table()`](./engine.md#init_vectorstore_table) likewise replaces the `name` of each `Column` (or `ColumnDict`) you pass with its SQL-escaped form. Escaping changes only names containing a double quote, so this is invisible in practice — but reusing the same `Column` object across two calls does double-escape such a name.

## Smaller behaviors {#misc}

| Question | Answer |
| :--- | :--- |
| Can I create a partial index? | No. `partial_indexes` on `IVFIndex` raises — SereneDB inverted indexes have no `CREATE INDEX ... WHERE` form. |
| Why does `IVFQueryOptions.to_string()` warn? | It is deprecated. Use [`to_parameter()`](./indexes.md#ivfqueryoptions). |
| Can I have a metadata column named `distance`? | Yes. The computed score column is renamed to `distance_1` (or the next free name) so it does not shadow yours. |
| Why does a text search dictionary survive `drop_table()`? | [`drop_table()`](./engine.md#drop_table) drops the table and its index, not the dictionary. It is created `IF NOT EXISTS`, so a recreated table reuses the existing options — drop it by hand if you change `dictionary_options`. |


## See also

- [Overview](./index.md) · [Engine and Tables](./engine.md) · [Vector Store](./vector-store.md)
- [Metadata Filtering](./filtering.md) · [Hybrid Search](./hybrid-search.md) · [Indexes and Tuning](./indexes.md) · [Async API](./async.md)
- [Maintenance & Introspection](../../sql/indexes/inverted/maintenance.md)
