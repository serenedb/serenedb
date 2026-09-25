---
title: "split_text"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# split_text

The `split_text` template splits text into words with the language-agnostic word-boundary algorithm of [Unicode Standard Annex #29 (Unicode Text Segmentation)](https://www.unicode.org/reports/tr29/) and emits each word as a token, optionally case-folded. It is the first stage of most natural-language dictionaries: chain [`normalize_tokens`](./normalize_tokens.md), [`remove_stopwords`](./remove_stopwords.md), [`stem_words`](./stem_words.md) and [`generate_ngrams`](./generate_ngrams.md) after it to fold accents, drop stop words, stem and build edge n-grams — see [Building a language dictionary](#building-a-language-dictionary).

Boundaries come from the Unicode properties of the characters themselves rather than from whitespace or a per-language dictionary, so one dictionary works across scripts, and the algorithm runs in-process with no locale and no dictionary. That also bounds what it can do: a script that UAX#29 does not join breaks at every character, so Han text is emitted one character per token. For languages that do not separate words with spaces — Chinese, Japanese, Thai — use [`split_text_icu`](./split_text_icu.md), which segments with the ICU break iterator for a locale. For ASCII text where words are already space-separated, a [`split_text_csv`](./split_text_csv.md) split is simpler and faster.

**As a function:** `split_text(value, case := 'none', break := 'alpha')` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/function_form" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `CASE` | string | `'none'` | Case conversion applied to each emitted token: `'none'`, `'lower'`, `'upper'` |
| `BREAK` | string | `'alpha'` | Unit of segmentation and which segments are kept. Word segments, filtered: `'alpha'` (segments holding a letter or a digit), `'graphic'` (segments holding a non-whitespace, non-control character), `'all'` (every segment, whitespace runs included). Larger units, every segment kept: `'sentence'`, `'line'`, `'paragraph'`. A smaller unit, whitespace dropped: `'grapheme'` (one user-perceived character per token) |

Positional arguments bind in this order, so `split_text('lower')` is `split_text(case := 'lower')`. `CASE` and `BREAK` accept their values case-insensitively, so `'Lower'` and `'ALPHA'` also work; anything outside the listed values fails with `invalid value in "case" parameter` or `invalid value in "break" parameter`, and any other option — `LOCALE`, `STEMMING`, `MIN_GRAM` — fails with `split_text(): unknown option "<name>"`, because those jobs belong to the stages chained after `split_text`.

## Tokenization

The seven `BREAK` values fall into three families.

`alpha` (the default), `graphic` and `all` all cut the value at every UAX#29 word boundary and differ only in which segments are emitted. `alpha` keeps a segment that contains at least one letter or digit — Unicode primary category `L` or `N` — and so discards punctuation and whitespace entirely. `graphic` keeps a segment that contains at least one character that is neither whitespace nor an ASCII control character, which additionally keeps each punctuation mark as its own token. `all` keeps every segment, so a run of consecutive spaces is one token and each punctuation character is a token of its own.

`sentence`, `line` and `paragraph` change the unit of segmentation instead of filtering. No accept test applies: every segment is emitted, with leading and trailing bytes up to and including the space character trimmed off, and a segment that trims away to nothing is dropped. `sentence` emits UAX#29 sentences. `line` emits one token per line, breaking on LF, VT, FF, CR, CRLF (counted as a single break), U+0085 NEXT LINE, U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR. `paragraph` breaks only on a run of two or more of those breaks — a blank line — or on a single U+2029, so a lone line break stays inside the token.

`grapheme` goes the other way and emits one token per UAX#29 extended grapheme cluster — what a reader sees as one character. A letter with its combining marks, an emoji with its skin-tone modifier or a ZWJ sequence, a flag and a Hangul syllable written as separate jamo each stay one token, and CRLF counts as one cluster. Clusters made only of whitespace are dropped; every other cluster is kept, punctuation included. It suits per-character matching, for example in scripts that do not separate words with spaces. For a boundary inside words, such as splitting on every character that is not a letter or a digit, use [`split_by_non_alpha`](./split_by_non_alpha.md).

Each emitted token gets one index position, in input order, and its offsets are its byte range in the value. All four [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) are supported: the tokenizer records offsets, so `OFFSET` is available alongside `FREQUENCY`, `POSITION` and `NORM`. `CASE` is applied last, and only to the segments that are kept. `'none'` emits the source bytes exactly. `'lower'` and `'upper'` apply simple per-codepoint case mapping; there is no locale option here, so the mapping is locale-independent and context-free — no Turkish dotless `i`, no Greek final-sigma context. Each codepoint maps to exactly one codepoint, so `'upper'` leaves `ß` unchanged instead of expanding it to `SS`; the mapped codepoint can still encode to a different number of bytes, so a converted token is not always as long as its source. For locale-aware casing, leave `CASE` at `'none'` and chain a [`normalize_tokens`](./normalize_tokens.md) stage with the locale instead. Bytes that are not valid UTF-8 are copied through unchanged rather than rejected, and empty input yields no tokens.

The following table shows how the input `The Quick fox-trot.` is tokenized under each of the three word modes:

| `BREAK` | Tokens |
|---|---|
| `alpha` | `The`, `Quick`, `fox`, `trot` |
| `graphic` | `The`, `Quick`, `fox`, `-`, `trot`, `.` |
| `all` | `The`, ` `, `Quick`, ` `, `fox`, `-`, `trot`, `.` |

Because boundaries come from Unicode properties, the same dictionary splits ASCII words exactly where you would expect, and it needs no per-language configuration — but it also finds no boundary that Unicode does not mark.

This dictionary keeps the segments holding a letter or a digit and lowercases each one:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_001" />

### Graphic segments

`BREAK = 'graphic'` keeps punctuation as separate tokens:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_002" />

### All segments

`BREAK = 'all'` emits every segment, whitespace included, and here uppercases the result:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_003" />

### Sentences

`BREAK = 'sentence'` emits one token per sentence, trimmed of surrounding whitespace and with its terminating punctuation intact:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_004" />

### Graphemes

`BREAK = 'grapheme'` emits one token per user-perceived character: the thumbs-up with its skin tone and the two-letter flag stay whole, and the space is dropped:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/grapheme" />

## Building a language dictionary

`split_text` only splits and folds case. Everything else a language needs is a stage chained after it with `|`, each one re-analyzing the tokens of the one before, so the dictionary spells out its analysis in order:

```sql
CREATE TEXT SEARCH DICTIONARY english AS
    split_text(case := 'lower')
    | normalize_tokens('en_US.UTF-8', accent := false)
    | remove_stopwords(['the', 'a', 'an', 'is'])
    | stem_words('en_US.UTF-8')
    WITH (frequency, position);
```

- [`normalize_tokens`](./normalize_tokens.md) applies NFC normalization and, with `accent := false`, strips accent marks so `café` matches `cafe`; with a locale and `case := 'lower'` it also does locale-aware case folding, for Turkish, Greek or Lithuanian text.
- [`remove_stopwords`](./remove_stopwords.md) drops the listed words, matching them byte-for-byte, so the list is spelled the way the tokens look after the stages before it — lowercased, accents stripped, not yet stemmed. `remove_stopwords(stopwords_path := '/path')` loads the list from a file or a directory of files instead.
- [`stem_words`](./stem_words.md) replaces each word with its Snowball stem for the locale's language, so `running` and `runs` both index as `run`; a language without a stemmer passes through.
- [`generate_ngrams`](./generate_ngrams.md) with `mode := 'only_prefix'` turns each word into its prefix-anchored edge n-grams, the basis for autocomplete.

The order matters: fold before you filter, filter before you stem, and build n-grams last. A word takes one index position whatever the later stages do to it, dropped stop words leave no gap, and every token keeps the byte range of the source word, so offsets still point at the value.

An English dictionary that lowercases and stems reduces `runners` to `runner`, `running` to `run` and `quickly` to `quick`; because Snowball only strips suffixes, `runners` and `running` do not meet at a common term:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_005" />

Without case folding or stemming the words keep their form, and `normalize_tokens` with `accent := false` strips the accent from `Café`:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_006" />

A `remove_stopwords` stage between the folding and the stemming drops the listed words from the stream:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_007" />

Edge n-grams: each word emits prefix-anchored fragments measured in codepoints, from `MIN_GRAM` up to `MAX_GRAM` long, all at the word's position, and `preserve_original := true` adds the whole word when it is longer than `MAX_GRAM`. A partial query like `sea` then matches `Search`:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text/example_008" />

### Coming from the former `split_text` template

Earlier releases had a fused `split_text` template whose options selected these stages internally. Its dictionaries are written as chains now, stage for stage:

| Former option | Stage |
|---|---|
| `LOCALE = 'L'` with word splitting | `split_text()` — or `split_text_icu('L')` for Chinese, Japanese, Thai, Khmer, Lao and Burmese, where the split needs a dictionary |
| `CASE = 'lower'` / `'upper'` | `split_text(case := 'lower')`; for Turkish, Azerbaijani, Lithuanian and Greek `normalize_tokens('L', case := 'lower')` instead |
| `ACCENT = false` | `normalize_tokens('L', accent := false)` |
| `STOPWORDS = [...]` | `remove_stopwords([...])`, after `normalize_tokens` and before `stem_words` |
| `STOPWORDS_PATH = 'P'` | `remove_stopwords(stopwords_path := 'P')` |
| `STEMMING = true` (the former default) | `stem_words('L')` |
| `MIN_GRAM`, `MAX_GRAM`, `PRESERVE_ORIGINAL` | `generate_ngrams(min, max, preserve_original := ..., mode := 'only_prefix')` as the last stage |

So the former `split_text('en_US.UTF-8', case := 'lower', accent := false)` is `split_text(case := 'lower') | normalize_tokens('en_US.UTF-8', accent := false) | stem_words('en_US.UTF-8')`, and `split_text('en_US.UTF-8', case := 'none', stemming := false, accent := true)` is plain `split_text()`.

## See also

- [`split_text_icu`](./split_text_icu.md) — locale-aware segmentation for scripts UAX#29 leaves unjoined
- [`normalize_tokens`](./normalize_tokens.md) — case folding with a locale, NFC normalization and accent stripping
- [`remove_stopwords`](./remove_stopwords.md) — drop stop words from the stream
- [`stem_words`](./stem_words.md) — Snowball stemming for a locale's language
- [`generate_ngrams`](./generate_ngrams.md) — character n-grams, including the prefix-anchored edge n-grams
- [`split_text_csv`](./split_text_csv.md) — split space-separated text on a literal character
- [`keyword`](../../../statements/create_text_search_dictionary/keyword.md) — emit the whole input as one verbatim token
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
- [CREATE INDEX](../../../statements/create_index/index.md)
