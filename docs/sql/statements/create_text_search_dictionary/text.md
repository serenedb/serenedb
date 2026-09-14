---
title: "text"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# text

The `text` template is the general-purpose word tokenizer and the one to reach for first for natural-language search. It splits input into words on Unicode word boundaries and then, under the control of an ICU `LOCALE`, folds case, strips accent marks, removes stop words and applies Snowball stemming. Stemming is on by default; case folding, accent stripping and stop-word removal are not.

`LOCALE` belongs on every `text` dictionary. It selects the word-breaking rules, the case mapping and the Snowball stemmer language. An absent or empty value fails at `CREATE` with `text: invalid locale`, and a string ICU cannot parse fails earlier with `Invalid locale "<value>" for option "locale"`.

Stemming maps inflected forms to a common root — `running` and `runs` both index as `run` — so a query matches a document even when the surface forms differ. Snowball strips suffixes algorithmically instead of looking words up, so an irregular form is left alone: `ran` stays `ran`. Because the same dictionary analyzes both the indexed text and the query, the search term is reduced the same way, so the two always meet. Stop words can be supplied inline with `STOPWORDS` or loaded from a path with `STOPWORDSPATH`, and `ACCENT = false` folds accent marks so `café` matches `cafe`.

All four [feature flags](./index.md#feature-flags) are supported. Enable `FREQUENCY` and `POSITION` on the indexed column when you need relevance ranking or phrase and proximity search, respectively.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | ICU locale (e.g., `'en_US.UTF-8'`, `'fr'`, `'de'`) |
| `CASE` | string | `'none'` | Case conversion, using full ICU case mapping for `LOCALE`: `'none'`, `'lower'`, `'upper'` |
| `STEMMING` | boolean | `true` | Apply Snowball stemming for the `LOCALE` language; words pass through unstemmed when no stemmer exists for it |
| `ACCENT` | boolean | `true` | Preserve accent marks; `false` strips them |
| `STOPWORDS` | string | `''` | Inline stop words, a comma-separated list of double-quoted words (e.g., `'"the","a","an"'`) |
| `STOPWORDSPATH` | string | `''` | Path to a stop-word file, or to a directory whose `<path>/<language>` files are all loaded; each line contributes the text up to its first whitespace |
| `MINGRAM` | integer | `2` | Edge n-gram minimum length in codepoints |
| `MAXGRAM` | integer | `3` | Edge n-gram maximum length in codepoints |
| `PRESERVEORIGINAL` | boolean | `false` | Emit the whole word alongside its edge n-grams |

`CASE` matches its value case-insensitively, so `'Lower'` also works; anything outside the three listed values fails with `invalid value in "case" parameter`. A boolean option written without a value means `true`.

## Tokenization

A value that contains non-ASCII bytes is split by the ICU word break iterator for `LOCALE`; pure-ASCII input is split by a built-in UAX#29 scanner instead, unless the locale is Turkish or Lithuanian. Whitespace and punctuation segments are discarded, and so is any segment holding neither a letter nor a digit. Each surviving word is then transformed in a fixed order: NFC normalization, `CASE` conversion, accent stripping when `ACCENT = false`, the stop-word test, and Snowball stemming when `STEMMING = true`. Because the stop-word test runs before stemming, a stop-word list has to spell its words as they look after case folding and accent stripping, not after stemming.

The emitted term is the transformed word, so with case folding, accent stripping or stemming it is not a substring of the input. Offsets still point at the source: each token carries the byte range of the word it came from. Every word takes one index position, counting from 1, and dropped stop words leave no gap. Empty input yields no tokens.

| Input | Options | Tokens |
|---|---|---|
| `The runners were running quickly` | `CASE = 'lower'`, `STEMMING = true` | `{the,runner,were,run,quick}` |
| `The Runners Café` | `CASE = 'none'`, `STEMMING = false`, `ACCENT = true` | `{The,Runners,Café}` |
| `The cat is a hunter` | `CASE = 'lower'`, `STOPWORDS = '"the","a","an","is"'` | `{cat,hunter}` |
| `Search` | `CASE = 'lower'`, `MINGRAM = 2`, `MAXGRAM = 4`, `PRESERVEORIGINAL = true` | `{se,sea,sear,search}` |

Stemming reduces `runners` to `runner`, `running` to `run` and `quickly` to `quick`; because Snowball only strips suffixes, `runners` and `running` do not meet at a common term. Stop words are removed only when `STOPWORDS` or `STOPWORDSPATH` is written — no word list is loaded otherwise, so common words like `the` are kept by default. A `STOPWORDSPATH` that does not exist is rejected at `CREATE` with `File "<path>" referenced by option "stopwordspath" does not exist`, a path that exists but yields no readable word list fails with `text: failed to load stopwords from the configured path`, and a malformed `STOPWORDS` list fails with `Invalid format of list of words(should be comma-separated and quoted)`. Use `ts_lexize` to preview the exact token stream for any configuration:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_005" />

With `CASE = 'none'` and `STEMMING = false` the words keep their original form and casing, and accent marks survive because `ACCENT = true`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_006" />

Supplying `STOPWORDS` drops the listed words from the stream:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_007" />

Edge n-grams stay off until at least one of `MINGRAM`, `MAXGRAM` and `PRESERVEORIGINAL` is written; the defaults in the table alone produce none. Once the mode is on, each word emits prefix-anchored fragments of its transformed form, measured in codepoints: the first is `MINGRAM` long, then one token per additional codepoint up to `MAXGRAM`. All fragments of a word share that word's single position, and each one's byte range starts at the source word and spans the fragment. A word shorter than `MINGRAM` emits nothing unless `PRESERVEORIGINAL = true`, which also appends the whole word after the longest fragment. `MINGRAM` above `MAXGRAM` is rejected at `CREATE` with `text: min_gram must not exceed max_gram`.

A partial query like `sea` then matches `Search` — the basis for autocomplete:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_008" />

## Examples

### Basic English dictionary

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_001" />

### No stemming, case-sensitive, accents stripped

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_002" />

### With edge n-grams for autocomplete

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_003" />

### With inline stopwords

<SqlLogicTest id="sql/statements/create_text_search_dictionary/text/example_004" />

## See also

- [`keyword`](./keyword.md) — emit the whole input as one verbatim token
- [`ngram`](./ngram.md) — character n-grams for fuzzy and substring matching
- [`stem`](./stem.md) — stemming only, without word splitting
- [`icu_text`](./icu_text.md) — ICU word or sentence segmentation with verbatim tokens, including dictionary segmentation for Chinese, Japanese and Thai
- [`segmentation`](./segmentation.md) — UAX#29 segmentation with no locale and no ICU
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
- [CREATE INDEX](../create_index/index.md)
