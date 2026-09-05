---
title: "stopwords"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# stopwords

The `stopwords` template removes the words listed in `STOPWORDS` from the token stream rather than producing tokens of its own. Dropping very common words (`the`, `a`, `is`) shrinks the index and keeps high-frequency terms from dominating relevance scores.

Because it is a filter, it operates on the output of an earlier tokenizer and is therefore used as a stage inside a [`pipeline`](./pipeline/index.md), after a template such as [`text`](./text.md) or [`segmentation`](./segmentation.md). Set `HEX = true` when the stop words are supplied as hex-encoded byte strings. Note that [`text`](./text.md) can filter stop words on its own through its `STOPWORDS` option — this template is for applying the same filtering within a custom pipeline.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `STOPWORDS` | string list | — | Stop words (e.g., `'"the","a","an"'`) |
| `HEX` | boolean | `false` | Treat stop words as hex-encoded strings |

## Tokenization

`stopwords` compares each token it receives against the list and drops the ones that match, passing everything else through unchanged. A token that is itself a stop word is removed, leaving no output; a token that is not in the list survives.

| Input | STOPWORDS | Output tokens |
|---|---|---|
| `the` | `"the","a","an","is"` | _(empty — removed)_ |
| `cat` | `"the","a","an","is"` | `cat` |

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_001" />

### Filtering inside a pipeline

In practice `stopwords` follows a tokenizer. A [`pipeline`](./pipeline/index.md) that splits on spaces and then filters drops the common words from a phrase while keeping the rest:

| Input | Pipeline | Output tokens |
|---|---|---|
| `the cat is a animal` | `delimiter` (space) → `stopwords` | `cat`, `animal` |

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_002" />

### Hex-encoded stopwords

With `HEX = true` the stop words are decoded from hex before matching, so `616263` filters the token `abc`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_003" />

## See also

- [text](./text.md) — tokenizer with a built-in `STOPWORDS` option
- [pipeline](./pipeline/index.md) — chain a tokenizer before `stopwords`
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
