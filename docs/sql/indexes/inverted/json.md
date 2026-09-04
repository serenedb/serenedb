---
title: Indexing JSON
sidebar_position: 4
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Semi-structured JSON documents are indexed and searched the same way as relational columns — by putting their sub-fields into an [inverted index](./index.md). The only real decision is the storage type: keep the data as raw `JSON` text, or convert it to the shredded [`VARIANT`](../../data_types/variant.md) type.

<DocCallout type="tip">

**Use the shredded [`VARIANT`](../../data_types/variant.md) type — it is the fastest way to store, search and return JSON.** Raw `JSON` is stored as text and re-parsed on every access; `VARIANT` is **shredded** at write time into a columnar, per-field, self-describing layout, so sub-field extraction reads a single column instead of parsing a string. Reach for raw-`JSON`-text indexing only when you cannot change the column type.

</DocCallout>

## Converting JSON to `VARIANT`

Parse JSON text into a shredded `VARIANT` with a two-step cast — `::JSON` parses, `::VARIANT` shreds:

```sql
'{"host": "web-01", "msg": "disk error"}'::JSON::VARIANT
```

Cast through `JSON` deliberately: a bare `'…'::VARIANT` on a string stores the text as a single string *scalar*, not a structured object, so sub-fields cannot be addressed. Extract a sub-field by name with the `['field']` subscript and cast it to the type you need (`['msg']::VARCHAR`).

## Indexing a `VARIANT` sub-field

You do not index a `VARIANT` column as a whole — index each **sub-field extraction expression** you want to search, cast to a concrete type. The query repeats the same extraction, and any other sub-field still comes back without re-parsing because each is stored as its own column:

The cast is **required, and it carries meaning** — it is not boilerplate. A subscript like `doc['msg']` is itself a `VARIANT` (the type travels with the value), and a `VARIANT` cannot be an index key. Casting to `VARCHAR`, `INTEGER`, `DATE`, … both makes the value indexable *and* chooses how it is indexed — text is analyzed for full-text search, numbers and dates get range matching. The same cast must appear in the query so the planner matches it to the index.


<SqlLogicTest id="sql/indexes/inverted/json/example_001" />

<DocCallout type="info">

A bare `VARIANT` column cannot be an index key (`USING inverted (doc)` raises *"unsupported type VARIANT"*). Index typed sub-field extractions as shown here, or — only if you must search the serialized text — cast the whole value with `(doc::VARCHAR)`.

</DocCallout>

## How each sub-field type is indexed

The **cast type** of the extraction decides how the sub-field behaves in the index — exactly as if that type were a top-level column. One index can mix all of them:

| Sub-field | Extraction expression | Behaviour |
| :--- | :--- | :--- |
| Text | `(doc['title']::VARCHAR) dict` | Analyzed [full-text search](./full-text-search.md) — `@@ 'term'`, phrases, fuzzy |
| Number | `(doc['priority']::INTEGER)` | [Range matching](./full-text-search.md#range-queries) — `ts_between`, `ts_ge`, … |
| Date / timestamp | `(doc['ts']::DATE)` | Range matching on the temporal value |
| Boolean / identifier | `(doc['active']::BOOLEAN)`, `(doc['sku']::VARCHAR)` | Verbatim (exact) match when no dictionary is attached |
| Array | `(doc['tags']::VARCHAR[]) dict` | Any-element match — a row matches if **any** element matches (see [Indexing arrays](./modeling.md#indexing-arrays)) |
| Nested | `(doc['nested']['region']::VARCHAR)` | Address any depth by chaining subscripts |

A sub-field that is absent from a row yields `NULL` and is simply not indexed for that row — documents need not share a uniform shape.

## Nested fields

Reach into a nested object by chaining subscripts — `doc['attrs']['brand']` — and cast the leaf to its type. Any depth works, and the nested extraction is indexed exactly like a top-level one. Here `attrs.brand` is indexed and searched as analyzed text:

<SqlLogicTest id="sql/indexes/inverted/json/example_006" />

## Array fields

Cast an array sub-field to a typed array (`::VARCHAR[]`) and it is indexed **element by element**: a row matches if **any** element matches. Combine with [`ts_all`](../../functions/search/full-text.md#ts_all) to require *every* value, or [`ts_any`](../../functions/search/full-text.md#ts_any) for "at least N of". Using the same `catalog_idx` from above:

<SqlLogicTest id="sql/indexes/inverted/json/example_007" />

## Assigning a tokenizer per sub-field

Each extraction takes its **own** [dictionary](./text-analysis.md) in the opclass position, so different sub-fields can be analyzed differently in the same index — a stemming dictionary on a description, lower-casing on a title, and a verbatim (no-dictionary) identifier — while numeric and temporal sub-fields get range matching automatically:

<SqlLogicTest id="sql/indexes/inverted/json/example_002" />

## Returning a payload without searching it

To keep a `VARIANT` only for *retrieval*, `INCLUDE` the column so it is returned straight from the index's columnstore alongside a text column you do search — no extraction needed:

<SqlLogicTest id="sql/indexes/inverted/json/example_003" />

## Indexing raw JSON text

When the column must stay `JSON`, index a [JSON extraction expression](../../../data_import_and_export/json/overview.md) such as `doc ->> 'host'` with a dictionary, and repeat the same extraction in the query. This works, but every match re-parses the JSON text and pays the [whitespace/round-trip pitfalls](../../../data_import_and_export/json/caveats.md) that `VARIANT` avoids:

<SqlLogicTest id="sql/indexes/inverted/json/example_004" />

## Indexing a `JSON` column as `VARIANT`

You do not have to migrate the column to `VARIANT` to get shredded, typed sub-field indexing — **cast to `VARIANT` inside the index expression**. The column stays `JSON` (a JSON-typed column casts straight with `::VARIANT`; a `VARCHAR` holding JSON needs `::JSON::VARIANT` first), while the index extracts typed sub-fields just as a real `VARIANT` column would:

<SqlLogicTest id="sql/indexes/inverted/json/example_005" />

This is the best of both worlds when you cannot change the schema: the stored bytes remain JSON, but each indexed sub-field is shredded and typed. The cast happens once per row at index-build time, not on every search. As always, the query must repeat the exact indexed expression — including the `::VARIANT` cast.

## See also

- [What to Index](./modeling.md) — columns, expressions, arrays and sizing
- [Text Analysis](./text-analysis.md) — dictionaries and tokenizers
- [Full-Text Search](./full-text-search.md) · [`VARIANT` type](../../data_types/variant.md) · [JSON overview](../../../data_import_and_export/json/overview.md)
