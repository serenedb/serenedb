---
title: "minhash"
split: headings
---

# minhash

`minhash` reduces a list of tokens to a compact [MinHash](https://en.wikipedia.org/wiki/MinHash) signature that approximates the *set* of those tokens. Two texts with similar token sets produce overlapping signatures, so comparing signatures estimates their Jaccard similarity cheaply — the basis for approximate deduplication and near-duplicate detection across large collections.

There is no `minhash` dictionary template. `TEMPLATE = 'minhash'` is rejected with `Invalid type of text search dictionary`, and `NUMHASHES` is not an option of any template. What replaced it is a scalar function, `minhash(tokens, num_hashes)`, which returns the signature as a `LIST(BLOB)`. Signatures are built by calling that function — inside an indexed expression, or inside the `EXPRESSION` of a [`sql`](../sql.md) dictionary — and matched as ordinary keyword terms. Signature components are opaque hash values, not readable words: you never search them by hand; you compare whole signatures.

## Options

The function takes two positional arguments and returns `LIST(BLOB)`:

| Option | Type | Default | Description |
|---|---|---|---|
| `tokens` | `LIST(VARCHAR)` or `LIST(BLOB)` | **required** | The token set to summarize. A NULL list yields NULL; NULL elements inside the list are skipped |
| `num_hashes` | `INTEGER` | **required** | Signature width. Must be a foldable constant and `>= 1` |

`num_hashes` is read when the query is bound, so it fails there: `minhash: num_hashes must be a constant` for a non-foldable argument, `minhash: num_hashes must not be NULL`, and `minhash: num_hashes must be >= 1, got <n>`.

## Tokenization

Every element of `tokens` is hashed to a 64-bit value, and the function keeps the `num_hashes` smallest distinct hashes — that set is the signature. Each element of the returned list is one such hash written as 8 bytes, little-endian. Which hashes come out depends only on which distinct tokens the input holds, not on their order and not on how often each one appears, so the signature summarizes the token *set*: two documents that share most of their words share most of their signature components. The list is shorter than `num_hashes` when the input holds fewer distinct tokens, and the order of its elements carries no meaning. An empty list, or a list whose elements are all NULL, yields an empty signature; only a NULL list yields NULL.

The components are opaque hash bytes with no readable meaning, so do not match on an individual component — compare full signatures. How much of two signatures overlaps estimates the Jaccard similarity of their token sets, with an expected error of about `1 / sqrt(num_hashes)`. Raising `num_hashes` sharpens that estimate at the cost of a larger index.

The function itself does not analyze text: splitting, case folding and every other normalization happen in the expression that builds the list handed to it. Both sides of a query must build that list the same way, or the signatures will not line up — hashing the query text without the `lower()` the index applied yields a different signature and matches nothing.

## Examples

### Signature index over an expression

An expression index stores the signature; the query hashes its own text the same way and matches the components as raw keyword terms through [`ts_tokenize`](../../../functions/search/full-text.md#ts_tokenize):

```sql
CREATE TABLE mh_docs(id INTEGER, body TEXT);

CREATE INDEX mh_idx ON mh_docs USING inverted(
    id,
    (minhash(regexp_split_to_array(lower(body), '\W+'), 16))
);

SELECT id FROM mh_idx
WHERE minhash(regexp_split_to_array(lower(body), '\W+'), 16)
      @@ ts_any(ts_tokenize(minhash(regexp_split_to_array(lower('Quick Brown'), '\W+'), 16), 'keyword'), 2);
```

The second argument of [`ts_any`](../../../functions/search/full-text.md#ts_any) is how many signature components must match, so it is the similarity threshold; [`ts_all`](../../../functions/search/full-text.md#ts_all) requires all of them. A `min_match` above the number of components the query produced is clamped to that number, so it means "all of them". The query expression must be the indexed one: the same text hashed with a different `num_hashes` is a different expression, and the query fails with `@@ requires an inverted-indexed column on one side`.

An indexed expression is evaluated without a client context, so it must be context-free — `ts_lexize` inside one is rejected with `Cannot use ts_lexize in this context`.

### Hashing inside a sql dictionary

A `sql` dictionary moves the hashing into the column's analyzer, so the query side needs no `minhash()` call — the dictionary hashes the query text too:

```sql
CREATE TEXT SEARCH DICTIONARY mh_sql(
    template = 'sql',
    expression = 'minhash(regexp_split_to_array(lower(input), ''\W+''), 16)'
);

CREATE INDEX mh_sql_idx ON mh_docs USING inverted(id, body mh_sql);

SELECT id FROM mh_sql_idx WHERE body @@ ts_any(ts_tokenize(['quick brown']), 2);
```

In this shape `min_match` counts produced signature components only in the `ts_tokenize` form; a plain list counts its own elements instead, so `ts_any(['quick brown'], 2)` fails with `ts_any min_match (2) exceeds number of arguments (1)`. The `sql` template supports the `FREQUENCY`, `POSITION` and `NORM` index features, but not `OFFSET`.

## See also

- [`sql`](../sql.md) — template that evaluates the `minhash()` expression as the column's analyzer
- [`keyword`](../keyword.md) — template that keeps a value verbatim; `ts_tokenize(list, 'keyword')` reuses the name to take each element verbatim, without looking a dictionary up in the catalog
- [Full-Text Search Functions](../../../functions/search/full-text.md) — `ts_any`, `ts_all` and `ts_tokenize` reference
- [CREATE TEXT SEARCH DICTIONARY](../index.md)
