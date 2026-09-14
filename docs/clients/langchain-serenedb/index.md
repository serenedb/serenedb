---
title: LangChain
sidebar_label: Overview
description: LangChain vector store integration for SereneDB — install, quickstart and API reference.
split: page
---

# LangChain

[`langchain-serenedb`](https://github.com/serenedb/langchain-serenedb) is a [LangChain](https://docs.langchain.com/oss/python/langchain/overview) vector store backed by SereneDB. SereneDB speaks the PostgreSQL wire protocol, so the package connects with [psycopg 3](https://www.psycopg.org/psycopg3/) and maps the `VectorStore` contract onto SereneDB's native features: fixed-size `FLOAT[N]` vector columns, the [inverted index](../../sql/indexes/inverted/index.md) with an `ivf` operator class for approximate nearest neighbor search, and BM25 relevance scoring for the lexical half of a hybrid query.

## What it maps onto SereneDB {#mapping}

| LangChain concept | SereneDB feature | Reference |
| :--- | :--- | :--- |
| Embedding column | `FLOAT[N]` array column | [Array](../../sql/data_types/array.md) |
| Distance metrics | `<->`, `<=>`, `<#>`, `<+>` and their named functions | [Vector functions](../../sql/functions/vector.md) |
| ANN index | `CREATE INDEX ... USING inverted (embedding ivf (metric = '...'))` | [Vector search](../../sql/indexes/inverted/vector-search.md) |
| Hybrid retrieval | One inverted index over the content **and** embedding columns, scored with `BM25()` | [Hybrid search](../../sql/indexes/inverted/hybrid-search.md) |
| Metadata | A `JSON` column, plus optional typed columns | [Indexes and Tuning](./indexes.md#metadata-index) |
| Metadata filters | Index-covered predicates, including `@@` full-text matches | [Metadata Filtering](./filtering.md) |
| Write visibility | `VACUUM (REFRESH_TABLE)` | [Maintenance](../../sql/indexes/inverted/maintenance.md) |

## Install {#install}

```sh
pip install langchain-serenedb
```

The distribution is named `langchain-serenedb`; the import name is `langchain_serenedb`.

| Requirement | Version |
| :--- | :--- |
| Python | `>= 3.10` |
| `langchain-core` | `>= 1.2.11, < 2.0` |
| `psycopg[binary]` | `>= 3, < 4` |
| `psycopg-pool` | `>= 3.2.1, < 4` |
| `numpy` | `>= 1.21, < 3` |

You also need a running SereneDB server. Note the port: SereneDB listens on **7890** by default, not PostgreSQL's 5432.

## Quickstart {#quickstart}

```python
from langchain_core.embeddings import DeterministicFakeEmbedding

from langchain_serenedb import IVFIndex, SereneDBEngine, SereneDBVectorStore

embeddings = DeterministicFakeEmbedding(size=768)

engine = SereneDBEngine.from_connection_string(
    "host=127.0.0.1 port=7890 user=postgres dbname=postgres"
)

# Create the table and its IVF index in one call, so vector search is accelerated
# from the start. All the DDL is generated for you.
engine.init_vectorstore_table("my_docs", 768, vector_index=IVFIndex())

store = SereneDBVectorStore.create_sync(engine, embeddings, "my_docs")

store.add_texts(
    ["SereneDB indexes vectors with IVF.", "BM25 ranks full-text matches."],
    metadatas=[{"topic": "vector"}, {"topic": "text"}],
)

for doc in store.similarity_search("how are vectors indexed?", k=2):
    print(doc.page_content, doc.metadata)

engine.close()
```

This gives you a `my_docs` table holding both documents — each with its text, a 768-dimensional embedding and its metadata dict — with the embeddings indexed for approximate nearest neighbor search ranked by cosine similarity. `similarity_search` returns the nearest documents with their metadata attached.
`DeterministicFakeEmbedding` keeps the example runnable without an API key, though it hashes text rather than modelling meaning, so the ranking it produces is arbitrary. In a real application, swap it for any [LangChain embeddings model](https://docs.langchain.com/oss/python/integrations/embeddings) and set `vector_size` to that model's dimension.

## Where to go next {#next}

| Page | Contents |
| :--- | :--- |
| [Engine and Tables](./engine.md) | `SereneDBEngine`, `Column`, `ColumnDict` — connecting, creating the table, publishing writes |
| [Vector Store](./vector-store.md) | `SereneDBVectorStore` — adding documents, searching, scores, retrievers |
| [Metadata Filtering](./filtering.md) | The `filter` dict: comparison, set, pattern, logical and full-text operators |
| [Hybrid Search](./hybrid-search.md) | `HybridIndexConfig`, `HybridSearchConfig`, `FusionStrategy` — fusing BM25 and vector rankings in one query |
| [Indexes and Tuning](./indexes.md) | `DistanceStrategy`, `IVFIndex`, `IVFQueryOptions`, `MetadataIndexConfig`, `MetadataColumnIndex`, `JsonFieldIndex` |
| [Async API](./async.md) | `AsyncSereneDBVectorStore` — the async core, the event loop model, async-only methods |
| [FAQ](./faq.md) | Common problems and their fixes, indexed by symptom |

## Package version {#version}

```python
import langchain_serenedb

print(langchain_serenedb.__version__)
```

`__version__` is read from the installed distribution metadata, so it is the empty string when the package is imported from a source tree that was never installed.

The package passes LangChain's standard `VectorStoreIntegrationTests` compliance suite for both the synchronous and the asynchronous store.

For plain SQL access from Python without LangChain, see [Python](../python.md). For the index that powers all of this, see [Inverted Index](../../sql/indexes/inverted/index.md).
