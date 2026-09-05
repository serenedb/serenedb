---
title: "text"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# text

The `text` template is the general-purpose word tokenizer and the one to reach for first for natural-language search. It splits input into words on Unicode boundaries and then, under the control of an ICU `LOCALE`, optionally folds case, strips accent marks, applies Snowball stemming and removes stop words.

Stemming maps inflected forms to a common root — `running`, `runs` and `ran` all index as `run` — so a query matches a document even when the surface forms differ. Because the same dictionary analyzes both the indexed text and the query, the search term is reduced the same way, so the two always meet. Stop words can be supplied inline with `STOPWORDS` or loaded from a file with `STOPWORDSPATH`, and accent folding (`ACCENT`) lets `café` match `cafe`.

Enable the `FREQUENCY` and `POSITION` [feature flags](./index.md#feature-flags) on the indexed column when you need relevance ranking or phrase and proximity search, respectively.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | — | ICU locale (e.g., `'en_US.UTF-8'`, `'fr'`, `'de'`) |
| `CASE` | string | `'none'` | Case conversion: `'none'`, `'lower'`, `'upper'` |
| `STEMMING` | boolean | `true` | Apply word stemming |
| `ACCENT` | boolean | `true` | Preserve accent marks |
| `STOPWORDS` | string list | — | Inline stop words (e.g., `'"the","a","an"'`) |
| `STOPWORDSPATH` | string | — | Path to a stopwords file |
| `MINGRAM` | integer | `2` | Edge n-gram minimum length |
| `MAXGRAM` | integer | `3` | Edge n-gram maximum length |
| `PRESERVEORIGINAL` | boolean | `false` | Emit original token alongside n-grams |

## Tokenization

The `text` template splits input on Unicode word boundaries, then applies the normalization steps you enable: case folding (`CASE`), accent folding (`ACCENT = false`), Snowball stemming (`STEMMING`) and stop-word removal (`STOPWORDS`). With stemming on, inflected forms collapse to a shared root so a query meets a document even when the surface forms differ. Setting `MINGRAM`/`MAXGRAM` adds edge n-grams — prefix-anchored fragments of each word — which is what powers as-you-type autocomplete.

| Input | Options | Tokens |
|---|---|---|
| `The runners were running quickly` | `CASE = 'lower'`, `STEMMING = true` | `{the,runner,were,run,quick}` |
| `The Runners Café` | `CASE = 'none'`, `STEMMING = false`, `ACCENT = true` | `{The,Runners,Café}` |
| `The cat is a hunter` | `STOPWORDS = '"the","a","an","is"'` | `{cat,hunter}` |
| `Search` | `MINGRAM = 2`, `MAXGRAM = 4`, `PRESERVEORIGINAL = true` | `{se,sea,sear,search}` |

Stemming reduces `runners` to `runner` and `running` to `run`, so both index under a shared root; `quickly` becomes `quick`. Note that stop words are only removed when `STOPWORDS` is set — by default common words like `the` are kept. Use `ts_lexize` to preview the exact token stream for any configuration:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_005" />

With `CASE = 'none'` and `STEMMING = false` the words keep their original form and casing, and accent marks survive because `ACCENT = true`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_006" />

Supplying `STOPWORDS` drops the listed words from the stream:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_007" />

Setting `MINGRAM`/`MAXGRAM` emits prefix-anchored edge n-grams of each word, so a partial query like `sea` matches `Search` — the basis for autocomplete:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_008" />

## Examples

### Basic English dictionary

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_001" />

### No stemming, case-sensitive

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_002" />

### With edge n-grams for autocomplete

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_003" />

### With inline stopwords

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_004" />

## See also

- [`keyword`](./keyword.md) — emit the whole input as one verbatim token
- [`ngram`](./ngram.md) — character n-grams for fuzzy and substring matching
- [`stem`](./stem.md) — stemming only, without word splitting
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
- [CREATE INDEX](../create_index/index.md)
