---
title: Metadata Filtering
sidebar_position: 3
split: headings
---

# Metadata Filtering

Every search method and [`delete()`](./vector-store.md#delete) accepts a `filter` dict that is translated into a SereneDB `WHERE` clause. Alongside LangChain's standard comparison, set and logical operators, SereneDB contributes a set of **full-text operators** that compile to inverted-index `@@` matches, so a metadata predicate can be a prefix, regular-expression, fuzzy, n-gram or phrase match.

```python
store.similarity_search(
    "quarterly results",
    k=5,
    filter={"$and": [{"category": "finance"}, {"year": {"$gte": 2024}}]},
)
```

Values are always bound as query parameters, never interpolated, so filter values cannot inject SQL.

## Filter structure {#structure}

- A bare value is shorthand for `$eq`: `{"category": "science"}` means `{"category": {"$eq": "science"}}`.
- An operator dict must contain **exactly one** key. Two operators on one field raise; use `$and` instead.
- Several top-level fields are ANDed: `{"a": 1, "b": 2}` becomes `a = 1 AND b = 2`.
- Only `$and`, `$or` and `$not` may appear as a top-level key, and only when they are the *sole* key.
- A field name must be a Python identifier, or a dotted path whose every part is one. `{"attrs.brand": "acme"}` is valid; a name starting with `$` is rejected as an operator in field position.
- An empty filter dict produces no `WHERE` clause.

## How a field resolves {#field-resolution}

The same field name compiles to three different SQL expressions depending on how the store is configured. This matters because only some of them can be served by the index.

| Case | Emitted SQL | Notes |
| :--- | :--- | :--- |
| A declared metadata column | `"category" = %(p)s` | Real SQL type, so comparisons are typed. Index-covered when the column joins the [metadata index](./indexes.md#metadata-index). |
| A JSON field declared in `metadata_index.json_fields` | `("langchain_metadata"->>'year')::INTEGER = %(p)s` | The expression is byte-identical to the index entry, so the predicate pushes into the index scan. |
| Any other key | `(langchain_metadata->>'year')::INTEGER = %(p)s` | Same shape, but the cast is inferred from the Python value rather than declared. Not index-covered. |

For an undeclared key the SQL type comes from the value's Python type:

| Python type | SQL cast |
| :--- | :--- |
| `str` | `TEXT` (no cast emitted) |
| `int` | `INTEGER` |
| `float` | `FLOAT` |
| `bool` | `BOOLEAN` |
| `datetime.date` | `DATE` |
| `datetime.datetime` | `TIMESTAMP` |
| `datetime.time` | `TIME` |

For a list value the type is taken from the first element. Any other Python type raises `ValueError("Unsupported type: <type>")`. The `$exists` operator never casts, since it only tests for null.

The JSON extraction is always parenthesized before the comparison, because `->` and `->>` are low-precedence operators in SereneDB. Dotted paths walk with `->` and finish with `->>`: `"attrs.brand"` becomes `(langchain_metadata->'attrs'->>'brand')`.

:::caution
Declaring a JSON field in `metadata_index.json_fields` fixes its type for *all* filters on that field. An undeclared field, by contrast, is cast per call — so `{"year": "2024"}` and `{"year": 2024}` compile to different SQL against the same data. Declare the fields you filter on.
:::

:::caution
With `store_metadata=False` there is no JSON column, so an unknown key is emitted as a bare column reference and the database rejects the query. Declare every filterable key as a metadata column in that configuration.
:::

## Comparison operators {#comparison}

| Operator | SQL | Example |
| :--- | :--- | :--- |
| `$eq` | `=` | `{"category": {"$eq": "science"}}` |
| `$ne` | `!=` | `{"category": {"$ne": "draft"}}` |
| `$lt` | `<` | `{"year": {"$lt": 2020}}` |
| `$lte` | `<=` | `{"year": {"$lte": 2020}}` |
| `$gt` | `>` | `{"score": {"$gt": 0.5}}` |
| `$gte` | `>=` | `{"score": {"$gte": 0.5}}` |

## Sets, ranges and existence {#sets}

### `$in` and `$nin` {#in}

```python
filter={"category": {"$in": ["animal", "science"]}}
filter={"category": {"$nin": ["draft"]}}
```

Expands to `IN (...)` / `NOT IN (...)` with one bound parameter per element. SereneDB does not support `= ANY(:array)` or `<> ALL(:array)` in a comparison like this, so the list is unrolled into individual placeholders rather than bound as one array.

:::note
That limitation is specific to the comparison operators. Arrays are fine as function arguments, which is how [`$match`](#match) works — it emits `column @@ ts_any(ARRAY[...])`. The two are worth telling apart:

| | `$in` | `$match` |
| :--- | :--- | :--- |
| Compares | The stored value | The column's indexed terms |
| Field requirement | Any metadata column or JSON key | Must be [FTS-filterable](#full-text) |
| Value types | `str`, `int`, `float` | Text tokens |
| N-of-M matching | No | Yes, via `min_match` |

Reach for `$match` when you want "contains any of these terms" on an analyzed column, or need `min_match`. It is not a shortcut for a long `$in` list: `ts_any` receives an array *literal* built from one placeholder per token, so the statement is the same size either way.
:::

| Rule | Behavior |
| :--- | :--- |
| Element types | Only `str`, `int` and `float`. A `bool` — or anything else — raises `NotImplementedError`. |
| Empty list with `$in` | Compiles to `(1 = 0)` — matches nothing. |
| Empty list with `$nin` | Compiles to `(1 = 1)` — matches everything. |

### `$between` {#between}

```python
filter={"year": {"$between": [2020, 2024]}}
```

A two-element sequence, compiled to `BETWEEN low AND high` (inclusive on both ends).

### `$exists` {#exists}

```python
filter={"reviewer": {"$exists": True}}
```

Compiles to `IS NOT NULL` for `True` and `IS NULL` for `False`. The value must be a real `bool`; anything else raises `ValueError`. No cast is applied to the extracted JSON value, since the test is only for presence.

## Pattern operators {#patterns}

| Operator | SQL | Example |
| :--- | :--- | :--- |
| `$like` | `LIKE` | `{"title": {"$like": "Intro%"}}` |
| `$ilike` | `ILIKE` | `{"title": {"$ilike": "intro%"}}` |

These are ordinary SQL string predicates, not index matches — they are evaluated row by row. For an index-served prefix match use [`$startswith`](#startswith) instead.

## Logical operators {#logical}

`$and` and `$or` take a list of filter dicts:

```python
filter={"$or": [{"category": "science"}, {"year": {"$gte": 2024}}]}
filter={"$and": [{"category": "science"}, {"year": {"$gte": 2024}}]}
```

A single-element list collapses to that element. An empty list raises.

`$not` accepts either a dict, negating it, or a list, negating each element and ANDing the results:

```python
filter={"$not": {"category": "draft"}}                      # NOT (category = 'draft')
filter={"$not": [{"category": "draft"}, {"year": 2019}]}    # NOT (...) AND NOT (...)
```

## Full-text operators {#full-text}

These six operators compile to `<expression> @@ ts_*(...)` — an inverted-index term match rather than a row-by-row comparison. They give you prefix, regular-expression, fuzzy, set, n-gram and phrase matching on metadata.

SereneDB evaluates `@@` only against an inverted-indexed term, so the filter translator checks up front that the field qualifies and raises otherwise, instead of letting the database fail cryptically. A field qualifies when it is either:

- a metadata column that joins the index — the default (`metadata_index` unset, or its `columns` left as `None`) indexes every declared metadata column, and an explicit `columns` list narrows that; or
- a JSON sub-field declared in `metadata_index.json_fields` **with `data_type="TEXT"`**. A non-`TEXT` declared field carries a `::TYPE` cast, which is not a text term.

Anything else — an unindexed column, an undeclared JSON key — raises:

```text
Operator $startswith requires 'tag' to be an inverted-indexed metadata column or a
TEXT JSON field declared in metadata_index.json_fields: full-text operators use the
`@@` match, which SereneDB evaluates only against an indexed text term. Add it to the
store's metadata index (or drop the operator).
```

Unlike the plain operators, these have no exact-scan fallback.

:::caution
Passing the index check is necessary but not sufficient. Whether an operator actually matches depends on the column's [text search dictionary](../../sql/indexes/inverted/text-analysis.md). A column indexed *verbatim* (no dictionary) is a single whole-value token, so `$startswith` and `$regex` match against the entire value, while `$phrase` and `$ngram` need an analyzed column — and `$ngram` specifically needs an n-gram dictionary. Attach one with [`MetadataColumnIndex(name, dictionary=...)`](./indexes.md#metadatacolumnindex), remembering that doing so defeats plain-equality pushdown on that column.
:::

The document text itself is not a metadata field, so full-text search over page content goes through [hybrid search](./hybrid-search.md), not this filter.

### `$startswith` {#startswith}

```python
filter={"category": {"$startswith": "sci"}}
```

Prefix match against indexed terms, via [`ts_starts_with`](../../sql/functions/search/full-text.md#ts_starts_with). Against a verbatim `category` column holding `sci-fi`, `science`, `drama`, this matches `sci-fi` and `science`.

### `$regex` {#regex}

```python
filter={"category": {"$regex": "dram."}}
```

Regular-expression match against indexed terms, via [`ts_regexp`](../../sql/functions/search/full-text.md#ts_regexp).

### `$fuzzy` {#fuzzy}

```python
filter={"category": {"$fuzzy": "scifi"}}
```

Typo-tolerant match with an automatically chosen edit distance, via [`ts_levenshtein`](../../sql/functions/search/full-text.md#ts_levenshtein). Matches `sci-fi` for the query `scifi`. The number of dictionary terms a fuzzy predicate expands to is capped by the `sdb_levenshtein_max_terms` [session setting](../../sql/indexes/inverted/maintenance.md#session-settings).

### `$match` {#match}

Match any of several tokens, or at least N of them, via [`ts_any`](../../sql/functions/search/full-text.md#ts_any).

| Value form | Compiles to |
| :--- | :--- |
| `["drama", "comedy"]` | `ts_any(ARRAY['drama', 'comedy'])` — matches either |
| `{"tokens": ["drama", "comedy"], "min_match": 2}` | `ts_any(ARRAY[...], 2)` — matches at least two |

```python
filter={"category": {"$match": ["drama", "comedy"]}}
filter={"tags": {"$match": {"tokens": ["a", "b", "c"], "min_match": 2}}}
```

The token list must be non-empty, and `min_match` must be a real `int` — it is emitted as a SQL integer literal rather than a bound parameter, which is why it is validated strictly.

### `$ngram` {#ngram}

N-gram similarity match, via [`ts_ngram`](../../sql/functions/search/full-text.md#ts_ngram).

| Value form | Compiles to |
| :--- | :--- |
| `"hello"` | `ts_ngram('hello')` |
| `{"text": "hello", "threshold": 0.3}` | `ts_ngram('hello', 0.3)` |

`threshold` is the minimum similarity, a number in `[0, 1]`. The column must be indexed with an n-gram dictionary carrying `frequency = true` and `position = true`; the client cannot enforce that, so create the dictionary yourself:

```sql
CREATE TEXT SEARCH DICTIONARY "public"."ngram_dict"
  (template = 'ngram', mingram = 2, maxgram = 3, frequency = true, position = true);
```

```python
from langchain_serenedb import MetadataColumnIndex, MetadataIndexConfig

metadata_index = MetadataIndexConfig(
    columns=[MetadataColumnIndex("title", dictionary="ngram_dict")]
)
```

### `$phrase` {#phrase}

Ordered-adjacent phrase match with optional gaps and a proximity budget, via [`ts_phrase`](../../sql/functions/search/full-text.md#ts_phrase).

| Value form | Compiles to | Meaning |
| :--- | :--- | :--- |
| `"quick fox"` | `ts_phrase('quick fox')` | The two tokens adjacent, in order |
| `["quick", 1, "fox"]` | `ts_phrase('quick', 1, 'fox')` | Exactly one token between them |
| `["fox", [0, 2], "dog"]` | `ts_phrase('fox', [0, 2], 'dog')` | Between zero and two tokens between them |
| `{"text": "quick fox", "slop": 1}` | `ts_phrase('quick fox', slop := 1)` | Adjacent, with a one-token proximity budget |
| `{"text": ["quick", 1, "fox"], "slop": 1}` | `ts_phrase('quick', 1, 'fox', slop := 1)` | Integer gap plus a budget |

```python
filter={"title": {"$phrase": "quick fox"}}
filter={"title": {"$phrase": {"text": "quick fox", "slop": 1}}}   # admits "quick brown fox"
```

A list form alternates text segments with gaps, so it must have **odd** length and both start and end with a segment. An integer gap is an exact token distance; a `[min, max]` pair is an interval. See [Phrase and proximity search](../../cookbook/search/phrase-and-proximity-search.md) for what `slop` does.

Validation rules:

| Rule | Error |
| :--- | :--- |
| Neither a string, a list, nor a `text`/`slop` mapping | `$phrase expects a string, a [segment, gap, segment, ...] list, or {'text': <str\|list>, 'slop': int}.` |
| Even-length list | `$phrase list must alternate segment/gap and both start and end with a text segment (odd length).` |
| Empty or non-string segment | `$phrase segments must be non-empty strings.` |
| Negative integer gap | `$phrase gap must be a non-negative integer.` |
| Malformed interval | `$phrase interval gap must be [min, max] with 0 <= min <= max.` |
| Gap that is neither | `$phrase gap must be an int or a [min, max] pair of ints.` |
| Non-integer or negative `slop` | `$phrase 'slop' must be a non-negative integer.` |
| `slop` together with an interval gap | `$phrase 'slop' is incompatible with an interval [min, max] gap.` |

Phrase matching needs positional information, so the column's dictionary must carry `position = true`.

## Making filters index-covered {#pushdown}

A filter on metadata that the inverted index covers is evaluated during the index scan; a filter on anything else is a post-filter over the rows the scan returns. Which is which comes down to [`MetadataIndexConfig`](./indexes.md#metadata-index):

- By default **all** declared metadata columns are indexed verbatim, and no JSON fields are.
- A verbatim column (no dictionary) supports `=`, `IN` and range pushdown. Attaching a dictionary analyzes the column for full-text instead, which enables the `@@` operators but defeats plain-equality pushdown.
- JSON sub-fields must be declared explicitly in `json_fields` to be indexed at all.

`metadata_index` is read on two sides. [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) and [`apply_vector_index()`](./indexes.md#apply_vector_index) use it to build the index entries — the **writer** side. The store uses it to build query expressions — the **reader** side, where it is consulted as a per-field lookup, never compared as a whole.

That means the store's copy can be a **subset**: declare only the fields you actually filter on. What matters is that a field you *do* declare on the reader side carries the same `field` and `data_type` the index was built with, so the two expressions match and the predicate can push into the index scan.

```python
# Writer: index everything worth indexing.
write_index = MetadataIndexConfig(
    json_fields=[JsonFieldIndex("tag", "TEXT"), JsonFieldIndex("year", "INTEGER")]
)
engine.init_vectorstore_table("my_docs", 768, metadata_index=write_index)

# Reader: this service only ever filters on `tag`, so declaring it alone is fine.
store = SereneDBVectorStore.create_sync(
    engine,
    embeddings,
    "my_docs",
    metadata_index=MetadataIndexConfig(json_fields=[JsonFieldIndex("tag", "TEXT")]),
)
```

Omitting a field on the reader side has two consequences, and only the second is an error:

- A plain filter on it still returns correct rows — it just compiles to the generic inferred-cast expression rather than the indexed one, so it may run as a post-filter.
- A [full-text operator](#full-text) on it **raises**. The FTS gate is a lookup against the reader's own declaration, so an undeclared JSON field is treated as not indexed regardless of what the index actually contains.

The same holds for `columns`: on the reader side it does nothing but gate the full-text operators, and leaving it `None` permits them on every metadata column. Plain filters key off the table's real columns, not this list.

:::caution
Declaring a JSON field with a **different** `data_type` on the two sides is the case to avoid. `TEXT` is indexed as a bare `(md ->> 'k')` term while every other type carries a `::TYPE` cast, so a disagreement means the query expression is not the indexed one. Indexing the bare extraction and casting only in the query is worse than losing pushdown: SereneDB can still push the predicate down and then compare raw string tokens against the cast bound, which returns wrong rows. Declare a field with the type you intend to query it as.
:::

Indexing metadata never changes which rows match — provided the declared types agree, it is purely an optimization.

## Operator reference {#operators}

| Operator | Argument | SQL | Index-covered |
| :--- | :--- | :--- | :--- |
| `$eq` | scalar | `= %(p)s` | Verbatim-indexed column or declared JSON field |
| `$ne` | scalar | `!= %(p)s` | No |
| `$lt` `$lte` `$gt` `$gte` | scalar | `<` `<=` `>` `>=` | Verbatim-indexed column or declared JSON field |
| `$in` | `list[str \| int \| float]` | `IN (...)` | Verbatim-indexed column or declared JSON field |
| `$nin` | `list[str \| int \| float]` | `NOT IN (...)` | No |
| `$between` | 2-element sequence | `BETWEEN ... AND ...` | Verbatim-indexed column or declared JSON field |
| `$exists` | `bool` | `IS NULL` / `IS NOT NULL` | No |
| `$like` | `str` | `LIKE %(p)s` | No |
| `$ilike` | `str` | `ILIKE %(p)s` | No |
| `$and` `$or` | `list[dict]` | `AND` / `OR` | Per operand |
| `$not` | `dict` or `list[dict]` | `NOT` | No |
| `$startswith` | `str` | `@@ ts_starts_with(...)` | Required |
| `$regex` | `str` | `@@ ts_regexp(...)` | Required |
| `$fuzzy` | `str` | `@@ ts_levenshtein(...)` | Required |
| `$match` | `list` or `tokens`/`min_match` mapping | `@@ ts_any(...)` | Required |
| `$ngram` | `str` or `text`/`threshold` mapping | `@@ ts_ngram(...)` | Required (n-gram dictionary) |
| `$phrase` | `str`, list, or `text`/`slop` mapping | `@@ ts_phrase(...)` | Required (`position = true`) |

:::note
The operator sets themselves — `SUPPORTED_OPERATORS`, `COMPARISONS_TO_NATIVE`, `TEXT_OPERATORS`, `SPECIAL_CASED_OPERATORS`, `LOGICAL_OPERATORS`, `FTS_OPERATORS`, `FTS_UNARY_FUNCTIONS` and `PYTHON_TO_SDB_TYPE_MAP` — are importable from `langchain_serenedb.async_vectorstore`, but they are not part of the public API and may change without notice.
:::

## Errors {#errors}

| Message | Cause |
| :--- | :--- |
| `Invalid type: Expected a dictionary but got type: <type>` | `filter` is not a dict. |
| `Invalid filter condition. Expected $and, $or or $not but got: <key>` | Another `$`-operator used at the top level. |
| `Invalid filter condition. Expected a field but got: <key>` | A `$`-operator alongside other top-level keys. |
| `Invalid filter condition. Expected a field but got an operator: <field>` | A field name starting with `$`. |
| `Invalid field name: <field>. Expected a valid identifier.` | The field name, or a part of a dotted path, is not an identifier. |
| `Invalid operator: <op>. Expected one of {...}` | Unknown operator. |
| `Invalid filter condition. Expected a value which is a dictionary with a single key ...` | More than one operator in one dict. |
| `Expected a list, but got <type> for value: ...` | `$and` / `$or` given a non-list. |
| `Invalid filter condition. Expected a dictionary or a list but got: <type>` | `$not` given something else. |
| `Unsupported type: <type>` | A JSON comparison value whose Python type has no SQL mapping. |
| `Unsupported type: <type> for value: <value>` | A `bool` or other disallowed element inside `$in` / `$nin`. |
| `Expected a boolean value for $exists operator, but got: <value>` | `$exists` given a non-bool. |
| `Operator <op> requires '<field>' to be an inverted-indexed metadata column ...` | A full-text operator on a field the index does not cover. |

## See also

- [Vector Store](./vector-store.md) — the methods that take `filter`
- [Indexes and Tuning](./indexes.md#metadata-index) — making filters index-covered
- [Full-Text Search Functions](../../sql/functions/search/full-text.md) · [Text Analysis](../../sql/indexes/inverted/text-analysis.md)
- [FAQ](./faq.md)
