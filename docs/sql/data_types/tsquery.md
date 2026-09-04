---
title: TSQUERY
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`TSQUERY` is the type of a **full-text query expression** evaluated against an [inverted index](../indexes/inverted/index.md) with the [`@@` operator](../indexes/inverted/full-text-search.md). It is the right-hand operand of every full-text predicate: `column @@ tsquery`.

<DocCallout type="attention">

Despite the shared name, SereneDB's `TSQUERY` is **not** the PostgreSQL `tsquery` type. It drives SereneDB's inverted-index search, not PostgreSQL `tsvector` matching. The familiar PostgreSQL constructor names (`to_tsquery`, `plainto_tsquery`, …) are provided for compatibility but build SereneDB queries.

</DocCallout>

## Producing a `TSQUERY`

A `TSQUERY` is rarely written as a literal. You normally build one of three ways:

- **A bare string**, analyzed by the column's [text search dictionary](../statements/create_text_search_dictionary/index.md). Multi-token input matches any token (`OR` semantics).
- **A constructor function** such as [`ts_phrase`](../functions/search/full-text.md#ts_phrase), `ts_levenshtein`, `ts_between` or `to_tsquery`. See the [function reference](../functions/search/full-text.md).
- **A cast** that changes how a string is interpreted: `'text'::tokenize('dictionary')` analyzes with a named dictionary (`'keyword'` for an exact, un-analyzed token), and `query::boost(factor)` scales its score contribution.

<SqlLogicTest id="sql/data_types/tsquery/example_001" />

## Composing queries

`TSQUERY` values compose with a small set of operators, so a complex search is assembled from simple parts. They fall into three groups:

| Group | Operators |
|---|---|
| [Boolean](#boolean-operators) | `\|\|` (OR), `&&` (AND), `!!` (NOT) |
| [Proximity](#proximity) | `##` (phrase / gap) |
| [Boost](#boost) | `^` (score weight) |

A string operand is analyzed by the column's dictionary before it is matched; the [`::tokenize` cast](#controlling-analysis) overrides that analysis.

### Boolean operators {#boolean-operators}

`||` matches **either** sub-query:

<SqlLogicTest id="sql/data_types/tsquery/example_002" />

`&&` requires **both** sub-queries:

<SqlLogicTest id="sql/data_types/tsquery/example_003" />

`!!` is unary **NOT**; combine it with `&&` to exclude matches — here "quick but not brown":

<SqlLogicTest id="sql/data_types/tsquery/example_007" />

### Proximity {#proximity}

`##` requires the operands as an adjacent **phrase**. `a ## N ## b` allows an exact `N`-token gap and `a ## [min, max] ## b` a gap range:

<SqlLogicTest id="sql/data_types/tsquery/example_004" />

### Boost {#boost}

`^` scales a sub-query's contribution to the relevance score without changing which rows match — here doubling the weight of the `quick` clause:

<SqlLogicTest id="sql/data_types/tsquery/example_006" />

### Controlling analysis {#controlling-analysis}

A string operand is normally analyzed by the column's dictionary. The `::tokenize` cast overrides that — `::tokenize('keyword')` forces an exact, un-analyzed token match:

<SqlLogicTest id="sql/data_types/tsquery/example_005" />

## Relationship to `VARCHAR`

`TSQUERY` is reinterpret-compatible with `VARCHAR`, so a string flows into a `TSQUERY` position automatically (it is then analyzed as described above). A `TSQUERY` only has meaning inside an `@@` predicate against an indexed column; it is not a general-purpose stored type.

## See also

- [Full-Text Search](../indexes/inverted/full-text-search.md) — building and running queries
- [Full-Text Search Functions](../functions/search/full-text.md) — every constructor and operator
- [Inverted Index](../indexes/inverted/index.md) · [CREATE TEXT SEARCH DICTIONARY](../statements/create_text_search_dictionary/index.md)
