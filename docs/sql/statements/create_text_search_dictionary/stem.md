---
title: "stem"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# stem

The `stem` template replaces its input with the [Snowball](https://snowballstem.org/) stem of that word for the language of the configured `LOCALE`, and does nothing else — it does not split text into words. Stemming lets different inflections of a word match one another: with `LOCALE = 'en'`, `running` and `runs` both index as `run`, so a query for one form retrieves documents written with the other. Snowball strips suffixes algorithmically, so `runners` stems to `runner` rather than to `run`, and `ran` — an irregular past form with no suffix to strip — stays `ran`.

One token goes in and exactly one token comes out. The stem replaces the original, which is not emitted alongside it, and no token is ever dropped: when the stemmer leaves a word as it is, or has no algorithm for the configured language, the token is the input unchanged.

On its own `stem` treats its whole input as a single token, so it is meant to receive pre-tokenized input. Place it as a stage inside a [`pipeline`](./pipeline/index.md), after a word splitter such as [`delimiter`](./delimiter.md), [`segmentation`](./segmentation.md) or [`text`](./text.md) with `STEMMING = false`. Reach for it when you want to control where stemming happens in a custom pipeline rather than the all-in-one behavior of [`text`](./text.md), which already stems.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | ICU locale whose language selects the Snowball stemmer |

`LOCALE` is the only option this template takes, and it has no usable default. Omitting it, or passing an empty string, leaves the locale unset and `CREATE` fails with `stem: invalid locale`; a string ICU cannot parse fails earlier with `Invalid locale "<value>" for option "locale"`. Only the language subtag reaches the stemmer, so `'en'`, `'en_US'` and `'en_US.UTF-8'` all select English, and the rest of the locale — region, encoding, keywords — is ignored. Inside a [`pipeline`](./pipeline/index.md) the option is spelled `STEP⟨N⟩_LOCALE`, and a dictionary built with [`copy_from`](./copy-from.md) inherits its source's locale. Any other tokenizer option — `CASE`, `ACCENT`, `STOPWORDS` — fails with `option "<name>" is not applicable in this context`. The [feature flags](./index.md#feature-flags) are dictionary-level options, and all four of `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` are supported here.

A stemmer is available for these languages: Arabic, Armenian, Basque, Catalan, Czech, Danish, Dutch, English, Esperanto, Estonian, Finnish, French, German, Greek, Hindi, Hungarian, Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Persian, Polish, Portuguese, Romanian, Russian, Serbian, Sesotho, Spanish, Swedish, Tamil, Turkish and Yiddish. A locale whose language is not in that list — `'zh'`, or `'C'` — is still accepted at `CREATE` time; the dictionary then passes every token through unchanged, with no error and nothing filtered.

## Tokenization

The template stems whatever token it receives. Applied to a single word it returns that word's stem. Several words separated by spaces are not split: the whole value is handed to the stemmer as if it were one word, so one token comes out rather than one per word — which is why `stem` is normally fed pre-tokenized input from a pipeline.

| Input | LOCALE | Output tokens |
|---|---|---|
| `running` | `en` | `run` |
| `running` | `zh` (no stemmer) | `running` |

On a standalone dictionary the token sits at the first position, and its offsets cover the whole input value rather than the shorter stem: the input `running` yields the token `run` with start offset `0` and end offset `7`. An empty value still yields one token, which is empty.

`stem` does not fold case, strip accents or normalize Unicode, and it does not keep the unstemmed form next to the stem. For case-insensitive stems, lowercase upstream with a [`norm`](./norm.md) step configured `CASE = 'lower'`; [`text`](./text.md) takes the same option, and neither template folds case by default. The input is expected to be UTF-8 and nothing validates it.

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stem/example_001" />

### Stemming each word of a phrase

To stem every word in a phrase, split first and stem second. A [`pipeline`](./pipeline/index.md) that runs [`delimiter`](./delimiter.md) then `stem` reduces each token independently:

| Input | Pipeline | Output tokens |
|---|---|---|
| `running runners ran` | `delimiter` (space) → `stem` (`en`) | `run`, `runner`, `ran` |

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stem/example_002" />

## See also

- [text](./text.md) — all-in-one tokenizer that stems as one of its built-in stages
- [pipeline](./pipeline/index.md) — chain a tokenizer before `stem`
- [norm](./norm.md) — fold case before stemming, for case-insensitive stems
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
