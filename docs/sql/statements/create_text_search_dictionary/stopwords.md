---
title: "stopwords"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# stopwords

The `stopwords` template removes the words listed in `STOPWORDS` from the token stream rather than producing tokens of its own. Dropping very common words (`the`, `a`, `is`) shrinks the index and keeps high-frequency terms from dominating relevance scores.

Because it is a filter, it is normally used on the output of an earlier tokenizer, as a stage inside a [`pipeline`](./pipeline/index.md) after a template such as [`text`](./text.md) or [`segmentation`](./segmentation.md). Set `HEX = true` when the stop words are supplied as hex-encoded byte strings. Note that [`text`](./text.md) can filter stop words on its own through its `STOPWORDS` option — this template is for applying the same filtering within a custom pipeline. There is no path option here: `STOPWORDSPATH` belongs to [`text`](./text.md) alone, and writing it on a `stopwords` dictionary fails with `option "stopwordspath" is not applicable in this context`.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `STOPWORDS` | string | `''` | Words to drop, a comma-separated list of double-quoted words, e.g. `'"the","a","an"'`. Each element is whitespace-stripped and must be wrapped in double quotes, otherwise `Invalid format of list of words(should be comma-separated and quoted)` is raised; empty entries — `""`, or nothing at all between two commas — are skipped. The value is split on commas before the quotes are removed, so a stop word cannot itself contain a comma — use `HEX` for that. An empty list filters nothing |
| `HEX` | boolean | `false` | Hex-decode every entry of `STOPWORDS` before it goes into the set, which lets a stop word hold arbitrary bytes. Hex digits of either case, an even number of them; an entry that is not valid hex fails with `invalid hex stopword` |

## Tokenization

`stopwords` compares each token it receives against the list and drops the ones that match, passing everything else through unchanged. Matching is byte-exact: the incoming token is compared as it arrives, with no case folding, normalization, accent folding or whitespace trimming, so a list containing `the` does not remove `The`. Put a case-folding stage before it — [`norm`](./norm.md) or [`text`](./text.md) with `CASE = 'lower'` — when you want case-insensitive filtering. A surviving token keeps both its value and its offsets.

On its own the template treats the whole input value as a single token, so the result is either that one token or no tokens at all.

| Input | STOPWORDS | Output tokens |
|---|---|---|
| `the` | `"the","a","an","is"` | _(empty — removed)_ |
| `cat` | `"the","a","an","is"` | `cat` |

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_001" />

`STOPWORDS` is optional. Omitted, or given a value with no entries, the set is empty and every token passes through, so the dictionary filters nothing.

### Filtering inside a pipeline

In practice `stopwords` follows a tokenizer. A [`pipeline`](./pipeline/index.md) that splits on spaces and then filters drops the common words from a phrase while keeping the rest. A dropped token leaves no position gap: the surviving tokens are renumbered consecutively.

| Input | Pipeline | Output tokens |
|---|---|---|
| `the cat is a animal` | `delimiter` (space) → `stopwords` | `cat`, `animal` |

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_002" />

### Hex-encoded stopwords

With `HEX = true` the stop words are decoded from hex before matching, so `616263` filters the token `abc`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/stopwords/example_003" />

`HEX` changes only how the list is read, never how a token is read: the decoded bytes are compared against the incoming token as-is. Duplicate entries are harmless, since the list becomes a set, and a token that is not valid UTF-8 is matched like any other byte string.

## See also

- [text](./text.md) — tokenizer with a built-in `STOPWORDS` option, and the only template that loads stop words from a file
- [pipeline](./pipeline/index.md) — chain a tokenizer before `stopwords`
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
