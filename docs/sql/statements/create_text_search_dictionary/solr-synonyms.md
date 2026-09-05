---
title: "solr_synonyms"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# solr_synonyms

The `solr_synonyms` template expands tokens through a synonyms map written in [Apache Solr](https://solr.apache.org/) synonyms-file format, supplied inline via the required `SYNONYMS` option. It rewrites each input term to the set of terms it is equivalent to, so a search for one word also finds documents written with any of its synonyms.

Each line of the map is a rule. A comma-separated list of terms forms a **bidirectional** equivalence class — any term in the list expands to all of them. The arrow form `lhs => rhs` defines a **one-way** mapping — the left side rewrites to the right and never the reverse. Input that matches no rule passes through unchanged.

Use it — usually inside a [`pipeline`](./pipeline/index.md) after a tokenizer — to broaden recall without bloating the index, since the expansion happens at analysis time on both the indexed text and the query.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `SYNONYMS` | string | **required** | Inline Solr-format synonyms: one rule per line; comma-separated terms are bidirectional, `lhs => rhs` maps left to right |

## Tokenization

A bidirectional class such as `car, automobile, auto` makes the three terms interchangeable: any one of them expands to all three (returned in sorted order), so a query for `auto` matches text that said `car`. A one-way rule such as `laptop => notebook` rewrites only in the stated direction — `laptop` becomes `notebook`, but `notebook` is left alone. A term that matches no rule is emitted as-is.

| Input | Synonyms map | Tokens |
|---|---|---|
| `car` | `car, automobile, auto` | `{auto,automobile,car}` |
| `automobile` | `car, automobile, auto` | `{auto,automobile,car}` |
| `laptop` | `laptop => notebook` | `{notebook}` |
| `keyboard` | *(no matching rule)* | `{keyboard}` |

The map below combines a bidirectional class with a one-way rule:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/solr-synonyms/example_001" />

Any member of the class expands to the whole class:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/solr-synonyms/example_002" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/solr-synonyms/example_003" />

The left side of a one-way rule rewrites to its right side:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/solr-synonyms/example_004" />

A term that matches no rule passes through unchanged:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/solr-synonyms/example_005" />

## See also

- [`wordnet_synonyms`](./wordnet-synonyms.md) — WordNet-format synonyms
- [`pipeline`](./pipeline/index.md) — chain a tokenizer ahead of the synonym filter
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
