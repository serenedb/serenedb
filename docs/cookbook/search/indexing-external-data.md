---
title: Indexing External Data
sidebar_position: 32
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Indexing External Data

SereneDB can create inverted indexes over external files — Parquet, CSV or ORC files on local disk or S3 — enabling full-text search, phrase queries, fuzzy matching and relevance ranking without importing data into the database. You expose the files through a view over a reader function, then index the view.

## Expose the files through a view

Define a view backed by a remote file:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_001" />

Local files work the same way:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_002" />

## Create a text search dictionary

Define how text columns should be tokenized:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_003" />

## Build an inverted index

Create the index over the view. Columns without a dictionary are indexed as-is (exact matching); columns with a dictionary get full-text processing:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_004" />

## Query the index

Once built, query the index by name — just like any other inverted index:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_005" />

## Rebuild the index

An external-data index is a static snapshot of the files at build time. When the underlying files change, rebuild the index:

<SqlLogicTest id="cookbook/search/indexing-external-data/example_006" />

## Use cases

- **Log analysis** — search through terabytes of Parquet log files on S3 without loading them into the database.
- **Data lake search** — add full-text search to your existing data lake files.
- **Archival queries** — index historical data that rarely changes but needs to be searchable.
- **Hybrid pipelines** — combine external data with local tables in the same query.

## See also

- [Working with Parquet](../file_formats/parquet_import.md)
- [CREATE INDEX](../../sql/statements/create_index/index.md)
- [CREATE TEXT SEARCH DICTIONARY](../../sql/statements/create_text_search_dictionary/index.md)
- [COPY](../../sql/statements/copy/index.md)
