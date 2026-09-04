---
title: "wordnet_synonyms"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# wordnet_synonyms

The `wordnet_synonyms` template expands tokens using a [WordNet](https://wordnet.princeton.edu/) Prolog synonyms database supplied inline via the required `SYNONYMS` option. Where [`solr_synonyms`](./solr-synonyms.md) rewrites a word to its sibling words, this template rewrites each word to the **synset id(s)** it belongs to — a numeric concept identifier shared by all words of the same sense.

Each record has the form `s(synset_id, w_num, 'word', ss_type, sense_number, tag_count).` and assigns one word to one synset. Words that appear under the same `synset_id` are synonyms, so they all map to that id and meet in the index even though the surface words differ. A word that appears in several synsets maps to all of their ids. A word in no record produces no tokens.

Like `solr_synonyms`, it is typically used inside a [`pipeline`](./pipeline/index.md) to broaden recall to related words.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `SYNONYMS` | string | **required** | Inline WordNet Prolog database: one `s(...)` record per line |

## Tokenization

Given records that place `fast`, `quick` and `swift` under synset `100000001`, each of those words is rewritten to `{100000001}`. Because the indexed text and the query are analyzed the same way, a search for `quick` reduces to `100000001` and so matches a document that contained `fast`. Words placed under a different synset map to that synset's id, and a word the database never mentions yields an empty token set.

| Input | Records | Tokens |
|---|---|---|
| `fast` | `s(100000001,1,'fast',v,1,0).` | `{100000001}` |
| `quick` | `s(100000001,2,'quick',v,1,0).` | `{100000001}` |
| `keyboard` | *(no record)* | `{}` |

The database below defines two synsets — a verb sense and a noun sense:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wordnet-synonyms/example_001" />

Words sharing a synset map to its id, so synonyms meet under the same token:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wordnet-synonyms/example_002" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wordnet-synonyms/example_003" />

A word the database never mentions produces no tokens:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wordnet-synonyms/example_004" />

## See also

- [`solr_synonyms`](./solr-synonyms.md) — Solr-format synonyms
- [`pipeline`](./pipeline/index.md) — chain a tokenizer ahead of the synonym filter
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
