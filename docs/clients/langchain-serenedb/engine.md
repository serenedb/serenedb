---
title: Engine and Tables
sidebar_position: 1
split: headings
---

# Engine and Tables

`SereneDBEngine` owns the psycopg 3 connection pool and issues every DDL statement the integration needs. It is the first object you create and the last one you close; a vector store is always built on top of an engine.

| Method | Purpose |
| :--- | :--- |
| [`from_connection_string()`](#from_connection_string) | Build an engine from a libpq connection string. |
| [`from_pool()`](#from_pool) | Wrap an `AsyncConnectionPool` you already own. |
| [`init_vectorstore_table()`](#init_vectorstore_table) | Create the table, and optionally its index, in one call. |
| [`refresh_table()`](#refresh_table) | Publish buffered writes to the inverted index. |
| [`drop_table()`](#drop_table) | Drop the table. |
| [`close()`](#close) | Dispose of the connection pool. |

Every method comes in a synchronous form and an `a`-prefixed asynchronous form; both are documented in one entry.

## Connecting {#connecting}

`SereneDBEngine(...)` cannot be called directly — it raises `Exception("Only create class through 'from_connection_string' or 'from_pool' methods!")`. Use one of the two factories below.

### `from_connection_string()` {#from_connection_string}

```python
SereneDBEngine.from_connection_string(conninfo, **kwargs)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `conninfo` | `str` | — | libpq connection string, e.g. `"host=127.0.0.1 port=7890 user=postgres dbname=postgres"`. |
| `**kwargs` | `Any` | — | Forwarded to `psycopg_pool.AsyncConnectionPool` (`min_size`, `max_size`, `timeout`, …). A nested `kwargs={...}` entry is forwarded to each *connection* instead. |

**How it works.** The first call creates a process-wide background event loop running on a daemon thread; every engine built this way shares it, which is what lets the synchronous API delegate to the async implementation. The pool itself is constructed with `open=False` and opened lazily on that loop before its first use, because an `AsyncConnectionPool` has to be opened from inside the loop that will drive it.

Per-connection options go in the nested `kwargs` dict. `row_factory` is forced to psycopg's `dict_row` — the integration reads rows as dictionaries, so do not override it.

```python
engine = SereneDBEngine.from_connection_string(
    "host=127.0.0.1 port=7890 user=postgres dbname=postgres",
    min_size=2,
    max_size=10,
    kwargs={"application_name": "my-rag-service"},
)
```

The bootstrap superuser `postgres` connects locally without a password; other roles authenticate with their [role password](../../security/client_authentication.md), which belongs in the connection string.

### `from_pool()` {#from_pool}

```python
SereneDBEngine.from_pool(pool, loop=None)
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `pool` | `AsyncConnectionPool` | — | An **already open** psycopg pool. The engine will not open it. |
| `loop` | `Optional[asyncio.AbstractEventLoop]` | `None` | The loop the pool is driven by. Required if you intend to call any synchronous method. |

Use this when your application already manages its own pool — a web framework's lifespan hook, for example.

:::caution
An engine created with `loop=None` can only be used through the `a`-prefixed methods. Every synchronous method raises `Exception("Engine was initialized without a background loop and cannot call sync methods.")`, and that includes the synchronous store built on it. Pass the running loop, or use [`from_connection_string()`](#from_connection_string).
:::

## Creating the table {#creating-the-table}

### `init_vectorstore_table()` / `ainit_vectorstore_table()` {#init_vectorstore_table}

```python
engine.init_vectorstore_table(table_name, vector_size, *, schema_name="public", ...)
```

Creates the table a vector store binds to, and optionally its inverted index.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `table_name` | `str` | — | Table to create. |
| `vector_size` | `int` | — | Embedding dimension `N`; the column is declared `FLOAT[N]`. |
| `schema_name` | `str` | `"public"` | Schema for the table, its index and its text search dictionary. |
| `content_column` | `str` | `"content"` | Column holding `Document.page_content`. Declared `TEXT NOT NULL`. |
| `embedding_column` | `str` | `"embedding"` | Column holding the vector. Declared `FLOAT[N] NOT NULL`. |
| `metadata_columns` | `Optional[list[Column \| ColumnDict]]` | `None` | Typed columns broken out of the metadata dict. See [Metadata columns vs the JSON column](#metadata-columns). |
| `metadata_json_column` | `str` | `"langchain_metadata"` | Column holding the remaining metadata as `JSON`. |
| `id_column` | `str \| Column \| ColumnDict` | `"langchain_id"` | Primary key. A plain string means type `UUID` — see [Choosing the id column type](#id-column). |
| `overwrite_existing` | `bool` | `False` | `DROP TABLE IF EXISTS` first, then recreate. Destroys existing data. |
| `if_not_exists` | `bool` | `False` | Emit `CREATE TABLE IF NOT EXISTS` (and `CREATE INDEX IF NOT EXISTS`), making the call idempotent. |
| `store_metadata` | `bool` | `True` | When `False`, no JSON metadata column is created. |
| `vector_index` | `Optional[BaseIndex]` | `None` | Build the ANN index in the same call, e.g. `IVFIndex()`. |
| `hybrid_index_config` | `Optional[HybridIndexConfig]` | `None` | Build the combined full-text + vector index (and its dictionary) in the same call. Build-time only — how queries fuse is set on the store, see [Hybrid Search](./hybrid-search.md). |
| `metadata_index` | `Optional[MetadataIndexConfig]` | `None` | Which metadata columns and JSON sub-fields join the index. |

Only `table_name` and `vector_size` are positional; everything else is keyword-only.

```python
from langchain_serenedb import Column, IVFIndex

engine.init_vectorstore_table(
    "my_docs",
    vector_size=768,
    metadata_columns=[
        Column("category", "TEXT", nullable=False),
        Column("year", "INTEGER"),
    ],
    vector_index=IVFIndex(),
)
```

### `Column` {#column}

A dataclass describing one typed column.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `name` | `str` | — | Column name. Raises `ValueError("Column name must be type string")` otherwise. |
| `data_type` | `str` | — | SQL type, emitted verbatim into the `CREATE TABLE`. Raises `ValueError("Column data_type must be type string")` otherwise. |
| `nullable` | `bool` | `True` | `False` appends `NOT NULL`. |

The `data_type` string is not validated against SereneDB's type list — an unknown type surfaces the database's error at `CREATE TABLE` time.

### `ColumnDict` {#columndict}

A `TypedDict` accepted anywhere a `Column` is, for callers that would rather pass a plain mapping. All three keys are required:

```python
engine.init_vectorstore_table(
    "my_docs",
    768,
    metadata_columns=[{"name": "category", "data_type": "TEXT", "nullable": False}],
)
```

Each key is type-checked at runtime and raises `TypeError("The 'name' field must be a string.")`, `TypeError("The 'data_type' field must be a string.")` or `TypeError("The 'nullable' field must be a boolean.")`.

## Table shape {#table-shape}

The example above generates:

```sql
CREATE TABLE "public"."my_docs"(
  "langchain_id" UUID PRIMARY KEY,
  "content" TEXT NOT NULL,
  "embedding" FLOAT[768] NOT NULL,
  "category" TEXT NOT NULL,
  "year" INTEGER,
  "langchain_metadata" JSON
);
```

On write, any metadata key matching a typed column is pulled out of the dict and bound to that column; whatever is left is serialized into the JSON column. A metadata key with no matching column and no JSON column is dropped. On read the JSON column is loaded first and the typed columns are layered on top, so `Document.metadata` comes back as one flat dict regardless of how the values were stored. `Document.id` is the id column rendered as a string.

### Choosing the id column type {#id-column}

Passing `id_column` as a plain string names the column and declares it `UUID PRIMARY KEY`. Generated ids are UUID4 strings, so the default works as long as you let the store assign them.

:::caution
If you supply your own ids and they are not UUIDs, declare the column accordingly:

```python
from langchain_serenedb import Column

engine.init_vectorstore_table(
    "my_docs", 768, id_column=Column("langchain_id", "VARCHAR", nullable=False)
)
```

A `UUID` column rejects arbitrary strings, so `add_texts(..., ids=["doc-1"])` fails against the default table.
:::

### Metadata columns vs the JSON column {#metadata-columns}

Declaring a metadata key as a typed column buys you a real SQL type (so range comparisons behave like numbers or dates, not strings) and a column the inverted index can cover verbatim. Everything not declared still round-trips through the JSON column, and can still be filtered — see [How a field resolves](./filtering.md#field-resolution).

`store_metadata=False` omits the JSON column entirely. Then only declared metadata columns exist, `metadata_index.json_fields` raises `ValueError`, and a filter on an undeclared key is emitted as a bare column reference that the database rejects.

### Creating indexes with the table {#index-at-create-time}

An index is built in the same call when either `vector_index` or `hybrid_index_config` is given:

| Arguments | Result |
| :--- | :--- |
| Neither | Table only. Vector search falls back to an exact scan. |
| `vector_index=IVFIndex(...)` | Table plus a vector-only inverted index on the embedding column. |
| `hybrid_index_config=HybridIndexConfig()` | `CREATE TEXT SEARCH DICTIONARY`, then one combined inverted index over the content **and** embedding columns. The vector operator class defaults to `IVFIndex()`. |
| Both | The combined index, using the `vector_index` you passed for the vector operator class. |

To create the table without an index, simply omit both — and to remove an index later, call [`drop_vector_index()`](./indexes.md#drop_vector_index) on the store.

The index is always named `<table_name>langchainvectorindex` and is not configurable; see [One index per collection](./indexes.md#one-index). `metadata_index` selects which metadata joins it, which is what makes metadata filters index-covered — see [Metadata index configuration](./indexes.md#metadata-index).

:::note
For a large bulk load, SereneDB trains better IVF clusters if the index is created *after* the data is in place. Create the table alone, load, then call [`apply_vector_index()`](./indexes.md#apply_vector_index). Creating the index up front is the convenient choice for incremental workloads.
:::

`overwrite_existing=True` drops and recreates the table. `if_not_exists=True` instead makes the call idempotent: the table and index are created only when absent, so re-running keeps existing data. The two are mutually exclusive and raise `ValueError` together.

:::caution
`if_not_exists=True` does **not** reconcile an existing table's shape. If a table with that name already exists, its columns are left exactly as they are, whatever you passed. Use the store's factory ([`create_sync()`](./vector-store.md#create_sync)) to validate that the columns are what you expect.
:::

## Publishing writes {#eventual-consistency}

### `refresh_table()` / `arefresh_table()` {#refresh_table}

```python
engine.refresh_table(table_name, *, schema_name="public")
```

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `table_name` | `str` | — | Table whose pending writes should be published. |
| `schema_name` | `str` | `"public"` | Schema the table lives in. |

SereneDB's inverted index is **eventually consistent**: rows written since the last refresh are invisible to full-text queries and to vector queries routed through the index. This method issues `VACUUM (REFRESH_TABLE)` on the table, publishing them immediately. See [Visibility and the refresh model](../../sql/indexes/inverted/maintenance.md#visibility-and-the-refresh-model) for the underlying mechanics, including the background `refresh_interval` that publishes writes on its own.

You rarely need to call this by hand: the store refreshes after every write. It becomes your responsibility when the store was created with `sync_load=False`, which is the recommended setting for bulk loads:

```python
store = SereneDBVectorStore.create_sync(engine, embeddings, "my_docs", sync_load=False)
store.add_texts(big_batch)          # no refresh per batch
engine.refresh_table("my_docs")     # publish once, at the end
```

## Dropping and closing {#lifecycle}

### `drop_table()` / `adrop_table()` {#drop_table}

```python
engine.drop_table(table_name, *, schema_name="public")
```

Issues `DROP TABLE IF EXISTS`, which also removes the table's index. It does not drop the text search dictionary created for hybrid search.

### `close()` / `aclose()` {#close}

```python
engine.close()
```

Disposes of the connection pool. The shared background loop and its thread are process-wide and are not shut down — they are daemon threads and do not keep the interpreter alive.

## See also

- [Vector Store](./vector-store.md) — building a store on top of the table
- [Indexes and Tuning](./indexes.md) — building the index after the fact
- [Inverted Index](../../sql/indexes/inverted/index.md) · [Maintenance & Introspection](../../sql/indexes/inverted/maintenance.md)
- [FAQ](./faq.md)
