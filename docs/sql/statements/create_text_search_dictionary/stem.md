---
title: "stem"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# stem

The `stem` template reduces each input token to its morphological root with the [Snowball](https://snowballstem.org/) stemmer for the configured `LOCALE`, and does nothing else — it does not split text into words. Stemming lets different inflections of a word match one another: `connection`, `connected` and `connecting` all collapse to `connect`, so a query for one form retrieves documents written with another.

On its own `stem` treats its whole input as a single token, so it is meant to receive pre-tokenized input. Place it as a stage inside a [`pipeline`](./pipeline/index.md), after a word splitter such as [`text`](./text.md) or [`delimiter`](./delimiter.md). Reach for it when you want to control where stemming happens in a custom pipeline rather than the all-in-one behavior of [`text`](./text.md), which already stems.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | — | ICU locale (e.g., `'en'`, `'de'`) — selects the Snowball language |

## Tokenization

The template stems whatever token it receives. Applied to a single word it returns that word's root. When the input is several words separated by spaces, `stem` sees them as one token and leaves it unchanged — which is why it is normally fed pre-tokenized input from a pipeline.

| Input | LOCALE | Output tokens |
|---|---|---|
| `running` | `en` | `run` |
| `connections` | `en` | `connect` |
| `running runners ran` | `en` | `running runners ran` (single untouched token) |

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
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
