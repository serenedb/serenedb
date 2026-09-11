---
title: "solr_synonyms"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# solr_synonyms

The `solr_synonyms` template expands tokens through a synonyms map written in [Apache Solr](https://solr.apache.org/) synonyms-file format, supplied inline via the required `SYNONYMS` option. It rewrites the value it receives to the set of terms that value is equivalent to, so a search for one word also finds documents written with any of its synonyms.

Each line of the map is a rule. A comma-separated list of terms forms a **bidirectional** equivalence class — any term in the list expands to all of them. The arrow form `lhs => rhs` defines a **one-way** mapping — the left side rewrites to the right and never the reverse. Input that matches no rule passes through unchanged.

Empty lines and lines whose first character is `#` are skipped, so the map can carry comments. Every term is stripped of the ASCII whitespace around it, and a trailing carriage return is dropped from each line, so a CRLF-formatted map parses as written. If the same key appears on more than one line, the last line wins — the outputs of the two lines are not merged.

Use it — usually inside a [`pipeline`](./pipeline/index.md) after a tokenizer — to broaden recall, since the expansion happens at analysis time on both the indexed text and the query. On its own the template looks up the whole input value, so a bare `solr_synonyms` dictionary only expands input that equals a key in full.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `SYNONYMS` | string — multi-line Solr synonyms-file content | **required** | Inline Solr-format synonyms: one rule per line; comma-separated terms are bidirectional, `lhs => rhs` maps left to right |

`SYNONYMS` is the only option this template takes, and it has no default: omitting it fails with `required parameter "synonyms" was not found`. Inside a [`pipeline`](./pipeline/index.md) it is spelled `STEP⟨N⟩_SYNONYMS`, and a dictionary built with [`copy_from`](./copy-from.md) inherits the source dictionary's map instead. All four index feature flags — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are supported here, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

A bidirectional class such as `car, automobile, auto` makes the three terms interchangeable: any one of them expands to all three (returned in sorted order), so a query for `auto` matches text that said `car`. A one-way rule such as `laptop => notebook` rewrites only in the stated direction — `laptop` becomes `notebook`, but `notebook` is left alone. A term that matches no rule is emitted as-is.

Lookup is byte-exact and covers the whole input value — or, inside a [`pipeline`](./pipeline/index.md), each token the preceding stage hands over. Nothing is folded first: there is no case conversion and no Unicode normalization, so a map holding `car` leaves `Car` untouched. Put a lowercasing stage ([`norm`](./norm.md) or [`text`](./text.md)) ahead of it when you want case-insensitive expansion. Multi-token phrase matching is not implemented either, so a key containing a space, such as `i pod`, only matches when the whole input reaching the lookup is exactly `i pod` — a tokenizer ahead of it in a pipeline has already cut that input in two.

On a hit the rule's whole output list is emitted, one token per entry, sorted and with duplicates removed — the list was sorted and deduplicated while the map was parsed. On a miss the input is emitted verbatim as one token, so an empty value yields one empty token. Every token that comes out of one lookup shares a single position, so the expansion is a stacked synonym set — that is what lets a phrase query match through any member of the class. The offsets are those of the input that was looked up: for a bare dictionary the whole value, start `0` and end the value's length in bytes; inside a [`pipeline`](./pipeline/index.md) the offsets the preceding stage recorded for the token it handed over.

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

A malformed map is rejected when the dictionary is created. An empty term — a leading, trailing or doubled comma, an empty side of `=>`, or a line holding nothing but whitespace — fails with `solr_synonyms: failed to parse synonyms: Failed parse line N`, and a line carrying more than one `=>` fails with `solr_synonyms: failed to parse synonyms: More than one explicit mapping specified on the line N`, where `N` counts lines from 1.

## See also

- [`wordnet_synonyms`](./wordnet-synonyms.md) — WordNet-format synonyms
- [`pipeline`](./pipeline/index.md) — chain a tokenizer ahead of the synonym filter
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
