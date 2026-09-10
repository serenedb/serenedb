---
title: Indexes and Tuning
sidebar_position: 5
split: headings
---

# Indexes and Tuning

SereneDB serves approximate nearest neighbor search from an [inverted index](../../sql/indexes/inverted/index.md) with an `ivf` operator class on the `FLOAT[N]` embedding column:

```sql
CREATE INDEX idx ON tbl USING inverted (embedding ivf (metric = 'cosine', quant = 'sq8'));
```

This page covers the classes that describe that index, the store methods that build and drop it, the session settings that tune a query, and the configuration that decides which metadata joins the index so [filters](./filtering.md) are index-covered.

| Symbol | Purpose |
| :--- | :--- |
| [`DistanceStrategy`](#distancestrategy) | Which distance measure to use, and the SQL it maps to. |
| [`IVFIndex`](#ivfindex) | The ANN index configuration. |
| [`IVFQueryOptions`](#ivfqueryoptions) | Per-query recall/latency tuning. |
| [`MetadataIndexConfig`](#metadataindexconfig) | Which metadata joins the index. |
| [`MetadataColumnIndex`](#metadatacolumnindex) | One typed metadata column to index. |
| [`JsonFieldIndex`](#jsonfieldindex) | One JSON metadata sub-field to index. |
| [`apply_vector_index()`](#apply_vector_index) and friends | Build, drop and inspect the index. |
| [Recomputing index statistics](#statistics) | Running `VACUUM (RECOMPUTE_STATS_TABLE)` yourself. |

## One index per collection {#one-index}

A store has exactly **one** inverted index, and its name is derived from the table: `<table_name>langchainvectorindex`. It is not configurable — the store always knows the name without you tracking it. That single index is either vector-only, or the combined content + embedding index used for [hybrid search](./hybrid-search.md).

Whether it exists changes how a [dense search](./vector-store.md#dense) is executed. SereneDB routes an `ORDER BY embedding <op> query LIMIT k` through the IVF index only when the query selects *from the index by name*; querying the base table scans every row. So the store reads from the index when it is there and falls back to an exact scan when it is not — correct either way, just slower without the index.

:::caution
The store checks for the index once, on the first search, and caches the answer. The cache is updated by [`apply_vector_index()`](#apply_vector_index) and [`drop_vector_index()`](#drop_vector_index), but not by anything happening outside the store. If you create the index with raw SQL after a search has already run, that store instance keeps scanning the table — build the index through the store, or create a fresh one.
:::

## `DistanceStrategy` {#distancestrategy}

The distance measure a store searches with, passed as `distance_strategy` to the store factory. Each member bundles the query operator, the named SQL function, and the `metric` an IVF index must be built with for that operator to accelerate.

| Member | `.operator` | `.search_function` | `.index_metric` | Measure |
| :--- | :--- | :--- | :--- | :--- |
| `EUCLIDEAN` | `<->` | `l2_distance` | `l2` | Euclidean (L2) |
| `COSINE_DISTANCE` | `<=>` | `cosine_distance` | `cosine` | Cosine distance — the default |
| `INNER_PRODUCT` | `<#>` | `negative_inner_product` | `ip` | Negated inner product |
| `MANHATTAN` | `<+>` | `l1_distance` | `l1` | Manhattan (L1) |

`DEFAULT_DISTANCE_STRATEGY` is `COSINE_DISTANCE`.

`INNER_PRODUCT` reports the **negative** inner product, so smaller still means more similar and the value behaves like a true distance throughout the API — which is why LangChain's max-inner-product relevance helper applies unchanged.

The index metric has to match the operator the query uses, or the index cannot accelerate the search. You never wire that up by hand: the store overwrites the index's `distance_strategy` with its own when building — see [`apply_vector_index()`](#apply_vector_index).

## `IVFIndex` {#ivfindex}

The IVF index configuration. Every field is optional.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `index_type` | `str` | `"ivf"` | Identifies the index kind. Leave it alone. |
| `distance_strategy` | `DistanceStrategy` | `COSINE_DISTANCE` | Determines the emitted `metric`. Overwritten by the store when building. |
| `partial_indexes` | `Optional[list[str]]` | `None` | Not supported — a non-empty value raises. |
| `quant` | `Optional[str]` | `None` | Vector quantization: `"sq8"`, `"sq4"`, `"pq"`, `"rabitq"` or `"none"`. Quantized modes require metric `l2` or `ip`. |
| `pq_m` | `Optional[int]` | `None` | Number of PQ sub-quantizers; must divide the vector dimension. Valid only with `quant="pq"`. |
| `rabitq_bits` | `Optional[int]` | `None` | RaBitQ bit count, 1–9. Valid only with `quant="rabitq"`. |
| `compression` | `Optional[bool]` | `None` | `False` stores index vectors uncompressed. The database default is `True`. |

**How it works.** A field left `None` is omitted from the DDL, so SereneDB applies its own default — including the number of coarse clusters, which is auto-scaled from the row count. `index_options()` renders the operator-class spec:

```python
IVFIndex().index_options()
# "ivf (metric = 'cosine')"

IVFIndex(
    distance_strategy=DistanceStrategy.EUCLIDEAN, quant="sq8", compression=False
).index_options()
# "ivf (metric = 'l2', quant = 'sq8', compression = false)"
```

Option combinations are validated by the database at `CREATE INDEX` time, not in Python, so an invalid pairing surfaces as a SereneDB error. `partial_indexes` is the exception — it raises immediately, because SereneDB inverted indexes have no `CREATE INDEX ... WHERE` form.

`get_index_metric()` returns the metric string the current `distance_strategy` implies.

See [Quantization](../../sql/indexes/inverted/vector-search.md#quantization) for the recall/memory trade-offs, and note that quantized indexes benefit from [`rerank_factor`](#ivfqueryoptions).

## Building and dropping the index {#index-lifecycle}

These are methods on the vector store. Each has an `a`-prefixed async form on [`AsyncSereneDBVectorStore`](./async.md).

### `apply_vector_index()` / `aapply_vector_index()` {#apply_vector_index}

```python
store.apply_vector_index(index, *, concurrently=False)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `index` | `BaseIndex` | — | The index to build, in practice an [`IVFIndex`](#ivfindex). |
| `concurrently` | `bool` | `False` | Accepted for API parity and **ignored** — never emitted in the DDL. |

Creates the collection's single index on the embedding column. This always builds a **plain vector** index — it does not consult the store's `hybrid_search_config`, so for the combined full-text + vector index use [`apply_hybrid_search_index()`](#apply_hybrid_search_index).

Metadata entries from the store's [`metadata_index`](#metadata-index) are appended to the index in the same statement.

```python
store.apply_vector_index(IVFIndex(quant="sq8"))
```

To remove an index, call [`drop_vector_index()`](#drop_vector_index).

:::caution
This method **mutates the index object you pass in**: `index.distance_strategy` is overwritten with the store's strategy, so the index metric always matches the query operator. Do not reuse one `IVFIndex` instance across stores with different strategies and expect it to keep its own.
:::

### `apply_hybrid_search_index()` / `aapply_hybrid_search_index()` {#apply_hybrid_search_index}

```python
store.apply_hybrid_search_index(index=None, *, index_config=None, concurrently=False)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `index` | `Optional[BaseIndex]` | `None` | Vector configuration for the combined index. Defaults to a plain `IVFIndex` using the store's distance strategy. |
| `index_config` | `Optional[HybridIndexConfig]` | `None` | The text search dictionary for the content column. Defaults to [`HybridIndexConfig()`](./hybrid-search.md#hybridindexconfig). |
| `concurrently` | `bool` | `False` | Ignored. |

Builds one combined index over the content column (analyzed for BM25 with `index_config`'s dictionary) and the embedding column, with the id column stored via `INCLUDE` so the lexical branch can return it. This is the only method that creates a hybrid index.

It does not require the store to carry a `HybridSearchConfig` — building the index and configuring how queries fuse are independent, so you can create the index from any store over the table. Both arguments are optional:

```python
store.apply_hybrid_search_index()                            # default IVF + default dictionary
store.apply_hybrid_search_index(IVFIndex(quant="sq8"))       # quantized combined index
store.apply_hybrid_search_index(
    index_config=HybridIndexConfig(dictionary_name="my_dict")
)
```

On the async class, `index_config` and `concurrently` are positional-or-keyword; on the sync wrapper they are keyword-only.

### `drop_vector_index()` / `adrop_vector_index()` {#drop_vector_index}

```python
store.drop_vector_index()
```

Issues `DROP INDEX IF EXISTS` for the derived index name. Subsequent dense searches fall back to an exact scan; hybrid searches fail, since they have no fallback.

### `reindex()` / `areindex()` {#reindex}

```python
store.reindex()  # raises NotImplementedError
```

Would rebuild the store's index in place. **Not supported yet** — the method raises `NotImplementedError`:

```text
SereneDB REINDEX currently supports views only, not the store's base table, so
reindexing in place is not available yet. Drop and recreate the index instead
(drop_vector_index + apply_vector_index).
```

SereneDB's [`REINDEX`](../../sql/indexes/inverted/views.md#refreshing-the-index) applies to view-backed indexes, where it runs a refresh pass against the view's source. A vector store lives in a base table, so there is nothing for it to act on here. The method exists, and raises rather than silently doing something else, so that the intent is unambiguous; it will perform a real reindex once SereneDB supports `REINDEX` on tables.

To rebuild the index today, drop it and apply it again:

```python
store.drop_vector_index()
store.apply_vector_index(IVFIndex())
```

See [`drop_vector_index()`](#drop_vector_index) and [`apply_vector_index()`](#apply_vector_index).

### Recomputing index statistics {#statistics}

Relevance scoring and query planning use per-term statistics that the index maintains. After a large shift in data distribution they can be recomputed with `VACUUM (RECOMPUTE_STATS_TABLE)`, which is a separate operation from rebuilding the index.

The integration does not wrap this, so run it through any PostgreSQL client — including [`serened psql`](../serened-psql.md):

```sql
VACUUM (RECOMPUTE_STATS_TABLE) "public"."my_docs";
```

or on a psycopg connection of your own:

```python
import psycopg

with psycopg.connect("host=127.0.0.1 port=7890 user=postgres dbname=postgres") as conn:
    conn.autocommit = True  # VACUUM cannot run inside a transaction
    conn.execute('VACUUM (RECOMPUTE_STATS_TABLE) "public"."my_docs";')
```

Recomputing statistics is optional tuning — it does not affect which rows match, only how they are scored and planned. See [Manual maintenance with `VACUUM`](../../sql/indexes/inverted/maintenance.md#manual-maintenance-with-vacuum) for the full family of operations, and [Publishing writes](./engine.md#eventual-consistency) for the refresh that makes new rows visible in the first place.

### `is_valid_index()` {#is_valid_index}

```python
store.is_valid_index()  # -> bool
```

Probes `pg_indexes` for the derived index name and returns whether it exists. Note that this method has no `a` prefix on the async class either — it is `await store.is_valid_index()` there.

## `IVFQueryOptions` {#ivfqueryoptions}

Per-query IVF tuning, passed once as `index_query_options` to the store factory and applied as `SET LOCAL` on the connection running each search — dense and hybrid alike.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `nprobe` | `Optional[int]` | `None` | IVF cluster lists scanned per query, as `sdb_nprobe`. Higher means better recall and slower queries. |
| `rerank_factor` | `Optional[int]` | `None` | For a **quantized** index, the exact-distance rerank pool is `rerank_factor * k`, as `sdb_rerank_factor`. `0` disables reranking; ignored for unquantized indexes. |

A field left `None` is omitted, so SereneDB's own default applies (`8` and `4` respectively).

```python
from langchain_serenedb import IVFQueryOptions

store = SereneDBVectorStore.create_sync(
    engine, embeddings, "my_docs", index_query_options=IVFQueryOptions(nprobe=32)
)
```

`to_parameter()` returns the setting bodies that get emitted:

```python
IVFQueryOptions().to_parameter()                            # []
IVFQueryOptions(nprobe=10, rerank_factor=4).to_parameter()  # ["sdb_nprobe = 10", "sdb_rerank_factor = 4"]
```

`to_string()` is **deprecated** — it joins the same values with `"; "` and emits a `DeprecationWarning`. Use `to_parameter()`.

See [Session settings](../../sql/indexes/inverted/maintenance.md#session-settings) for the full list of `sdb_` settings, including ones this class does not cover.

## Metadata index configuration {#metadata-index}

Which metadata joins the inverted index decides whether a [metadata filter](./filtering.md) is evaluated during the index scan or as a post-filter. The configuration is read on two sides: [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) and [`apply_vector_index()`](#apply_vector_index) use it to build the index entries, and the store's filter translator uses it to build matching query expressions.

The store's copy is consulted one field at a time, so it can be a subset of what the index actually covers — declare the fields this store filters on, with the same types the index used. See [Making filters index-covered](./filtering.md#pushdown) for what changes when a field is omitted or its type disagrees.

### `MetadataIndexConfig` {#metadataindexconfig}

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `columns` | `Optional[list[MetadataColumnIndex]]` | `None` | `None` indexes **all** declared metadata columns verbatim. An explicit list narrows that; an empty list indexes none. |
| `json_fields` | `list[JsonFieldIndex]` | `[]` | JSON metadata sub-fields to index. None by default. |

```python
from langchain_serenedb import (
    JsonFieldIndex,
    MetadataColumnIndex,
    MetadataIndexConfig,
)

metadata_index = MetadataIndexConfig(
    columns=[
        MetadataColumnIndex("category"),                          # verbatim: =, IN, range
        MetadataColumnIndex("title", dictionary="langchain_fts_dict"),  # analyzed: full-text
    ],
    json_fields=[JsonFieldIndex("attrs.brand", "TEXT")],
)
```

When the column set is auto-derived (`metadata_index` is `None`, or its `columns` is `None`), a column whose declared type the inverted index cannot accept verbatim is **silently skipped**, so an auto-built index can never fail at `CREATE INDEX`. Explicitly listed columns are never filtered — there the database decides, and an unindexable entry surfaces its DDL error.

The accepted base types are in `INDEXABLE_METADATA_TYPES`: the character, integer, boolean, floating-point, date/time, binary and `JSON` families. Notably **not** indexable: `NUMERIC` / `DECIMAL` / `HUGEINT`, `UUID`, `INTERVAL` and `VARIANT`.

Requesting `json_fields` on a store with no JSON metadata column raises `ValueError("Cannot index JSON metadata fields: the store has no JSON metadata column (store_metadata=False).")`.

### `MetadataColumnIndex` {#metadatacolumnindex}

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `name` | `str` | — | Metadata column to add to the index. |
| `dictionary` | `Optional[str]` | `None` | Text search dictionary to analyze the column with. |

`dictionary=None` indexes the column **verbatim** — one token per whole value — which is what lets plain `=`, `IN` and range filters be served by the index scan. Attaching a dictionary analyzes the column for full-text instead: that enables the [full-text operators](./filtering.md#full-text) on it, but defeats plain-equality pushdown, since such queries then need the `@@` operator. The dictionary must already exist; the integration only creates the one named in a [`HybridSearchConfig`](./hybrid-search.md#dictionary).

### `JsonFieldIndex` {#jsonfieldindex}

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `field` | `str` | — | Key, or dotted path such as `"attrs.brand"`, inside the JSON metadata column. |
| `data_type` | `str` | — | SQL type the sub-field is indexed and queried as: `"TEXT"`, `"INTEGER"`, `"DOUBLE"`, `"BOOLEAN"`, `"DATE"`, … |
| `dictionary` | `Optional[str]` | `None` | Text search dictionary to analyze the extracted value with. |

`data_type` is not validated in Python — an unsupported type surfaces the database's error at `CREATE INDEX` time. The value is extracted with `->>`, and for any type other than `TEXT` a `::data_type` cast is applied to **both** the index entry and the query predicate, so the two expressions match byte for byte and the typed comparison pushes into the index scan. Casting only on the query side pushes down but returns wrong results, because the index would be comparing raw string tokens against a typed bound.

Only a `TEXT` declared field can be used with the [full-text operators](./filtering.md#full-text); a cast expression is not a text term.

## Internals {#internals}

:::note
The following are importable from `langchain_serenedb.indexes` but are not exported in `__all__`. They are implementation detail and may change without notice.
:::

| Symbol | What it is |
| :--- | :--- |
| `BaseIndex` | Abstract dataclass behind `IVFIndex`; the extension point for a new index type. Subclasses implement `index_options()` and inherit `get_index_metric()`. |
| `QueryOptions` | Abstract base of `IVFQueryOptions`, declaring `to_parameter()` and `to_string()`. |
| `StrategyMixin` | Supplies `operator`, `search_function` and `index_metric` to `DistanceStrategy`. |
| `DEFAULT_DISTANCE_STRATEGY` | `DistanceStrategy.COSINE_DISTANCE`. |
| `DEFAULT_INDEX_NAME_SUFFIX` | `"langchainvectorindex"` — appended to the table name to derive the index name. |
| `INDEXABLE_METADATA_TYPES` | Frozen set of base SQL types the inverted index accepts as a verbatim scalar member (verified on SereneDB 26.06.3). |
| `validate_identifier()` | Rejects anything that is not a bare SQL identifier. |
| `build_json_field_selector()` | Renders the `->>` extraction plus cast used identically on the index and query sides. |
| `build_dictionary_ddl()` | Renders the `CREATE TEXT SEARCH DICTIONARY` statement. |
| `build_metadata_index_entries()` | Renders the extra `USING inverted (...)` entries for metadata columns and JSON fields. |
| `build_vector_index_ddl()` | Renders the vector-only `CREATE INDEX`. |
| `build_hybrid_index_ddl()` | Renders the combined content + embedding `CREATE INDEX ... INCLUDE (id)`. |

`BaseIndex` is the one worth knowing about in practice: subclass it and implement `index_options()` if you need to emit an operator-class spec `IVFIndex` does not cover.

## See also

- [Vector Store](./vector-store.md) · [Metadata Filtering](./filtering.md) · [Hybrid Search](./hybrid-search.md)
- [Vector Search](../../sql/indexes/inverted/vector-search.md) · [Maintenance & Introspection](../../sql/indexes/inverted/maintenance.md)
- [FAQ](./faq.md)
