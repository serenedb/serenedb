---
title: "wordnet_synonyms"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# wordnet_synonyms

The `wordnet_synonyms` template expands tokens using a [WordNet](https://wordnet.princeton.edu/) Prolog synonyms database supplied inline via the required `SYNONYMS` option. Where [`solr_synonyms`](./solr-synonyms.md) rewrites a word to its sibling words, this template rewrites each word to the **synset id(s)** it belongs to — the concept identifier shared by all words of the same sense, taken verbatim from the record's first field.

Each record has the form `s(synset_id,w_num,'word',ss_type,sense_number,tag_count).` and assigns one word to one synset. Words that appear under the same `synset_id` are synonyms, so they all map to that id and meet in the index even though the surface words differ. A word that appears in several synsets maps to all of their ids, sorted lexicographically and deduplicated, one token per distinct id. A word in no record produces no tokens at all.

Like `solr_synonyms`, it is typically used inside a [`pipeline`](./pipeline/index.md) to broaden recall to related words.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `SYNONYMS` | string — multi-line WordNet Prolog database content | **required** | Inline WordNet Prolog database: one `s(...)` record per line |

`SYNONYMS` is the only option this template takes, and it has no default: omitting it fails with `required parameter "synonyms" was not found`. Inside a [`pipeline`](./pipeline/index.md) it is spelled `STEP⟨N⟩_SYNONYMS`, and a dictionary built with [`copy_from`](./copy-from.md) inherits the source dictionary's database instead. All four index feature flags — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are supported here, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

Given records that place `fast`, `quick` and `swift` under synset `100000001`, each of those words is rewritten to `{100000001}`. Because the indexed text and the query are analyzed the same way, a search for `quick` reduces to `100000001` and so matches a document that contained `fast`. Words placed under a different synset map to that synset's id, and a word the database never mentions yields an empty token set.

Lookup is byte-exact on the whole input value — or, inside a [`pipeline`](./pipeline/index.md), on each token the preceding stage hands over. No case folding, accent folding or Unicode normalization is applied, so `Fast` does not match a record written for `fast`. The ids of one value land on consecutive positions rather than on one shared position, so the expansion is not a stacked synonym set the way [`solr_synonyms`](./solr-synonyms.md) is. The offsets on every emitted id are those of the input that was looked up: for a bare dictionary the whole value, start `0` and end the value's length in bytes; inside a pipeline the offsets the preceding stage recorded for the token it handed over.

| Input | Records | Tokens |
|---|---|---|
| `fast` | `s(100000001,1,'fast',v,1,0).` | `{100000001}` |
| `quick` | `s(100000001,2,'quick',v,1,0).` | `{100000001}` |
| `keyboard` | *(no record)* | `{}` |

The database is split on newlines. Blank lines are skipped and a trailing carriage return is dropped, so a CRLF-formatted file parses. Every other line must be exactly one record: it starts with `s(`, ends with `).`, contains no further `)`, and holds four to six comma-separated fields. Fields are not whitespace-tolerant — `s(1, 1, 'x', n, 1, 0).` is rejected, because its third field is then ` 'x'` rather than `'x'`. Beyond the field count only the third field is checked, so a space in the first field is not rejected: it becomes part of the emitted id. The third field is the word and must be wrapped in single quotes around at least one character, so an empty pair of quotes is rejected. A doubled `''` inside the word stands for one apostrophe. A comma inside the quoted word is still read as a field separator, so a word containing a comma cannot be written. There is no comment syntax, so a `#` line is an error too. A line the parser rejects fails the statement with `wordnet_synonyms: failed to parse synonyms: Failed parse line N`.

The whole database is passed as a SQL string literal, so each `'` of a record is doubled once more at that level: the record text `'fast'` is written `''fast''` in the DDL.

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
