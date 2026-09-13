---
title: "classify_text"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# classify_text

The `classify_text` template runs a pre-trained text-classification model over the input and emits its predicted category labels as tokens. This lets you index documents by an inferred property — topic, language, sentiment — and then query them by label like any other term, without storing the label yourself.

At index and query time the analyzer hands the value to a [fastText](https://fasttext.cc/) supervised model loaded from `MODELLOCATION` and turns the model's predictions into terms. `TOPK` caps how many of the highest-scoring labels are emitted and `THRESHOLD` drops any whose probability falls below the given score.

The model file is **required** and must exist on the server host when the dictionary is created; a file the loader rejects fails with `failed to load fasttext model from: <path>, error: <...>` and no dictionary is created. It also has to be a supervised model: a model trained any other way still loads, but classifying a value with it fails with fastText's `Model needs to be supervised for prediction!`. Dictionaries that name the same `MODELLOCATION` share one in-memory copy of the model.

Classification and [`find_nearest_words`](./nearest-neighbors.md) are the two model-backed templates — use `classify_text` to tag a document with predicted categories, and `find_nearest_words` to expand it with semantically related terms.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MODELLOCATION` | string — a path on the server host | **required** | Path to the fastText model file. Its existence is checked when the dictionary is created, so a path that is not there fails with `File "<path>" referenced by option "modellocation" does not exist`; omitting the option altogether fails with `classify_text: empty model location` |
| `TOPK` | integer | `1` | Maximum number of highest-scoring labels emitted per value. Must be greater than zero, otherwise `classify_text: top_k must be positive` |
| `THRESHOLD` | double | `0.0` | Minimum probability a label must reach to be emitted. Must lie in `[0.0, 1.0]`, otherwise `invalid value in "threshold" parameter. Should be in [0, 1]`. The default keeps every label the model returns |

## Tokenization

The value goes to the model as one line. The model's own reader splits it on ASCII whitespace only — space, tab, carriage return, vertical tab, form feed and NUL — and looks each word up in the model's dictionary; a word the model does not know contributes nothing. A newline is where that line ends: the reader stops at the first `\n` in the value, so anything after it is never classified. Nothing is folded on the way in: no case conversion, no accent folding, no Unicode normalization, so the input bytes reach the model unchanged.

The emitted tokens are the model's label strings, copied verbatim from the model file, which means the label prefix (`__label__`, unless the model was trained with another one) is part of the term. Labels arrive ordered by descending probability, capped at `TOPK`, and a label is kept only when its probability is at least `THRESHOLD`, so one value yields between zero and `TOPK` tokens. The input text itself is never emitted.

All labels of one value share a single position: they form one stacked set rather than a sequence, so a phrase or proximity query cannot order them. Each label also carries the offsets of the whole value — start `0`, end the length of the value in bytes — because a predicted label has no span of its own in the text.

An empty value, or a value in which the model recognizes no word, produces no tokens at all. All four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted with this template, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Examples

Point `MODELLOCATION` at a trained fastText classifier and choose how many labels to keep. This dictionary emits at most three labels per value and drops any whose probability is below `0.5`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/classification/example_001" />

With the dictionary attached to a text column in a `USING inverted` index, each document is indexed under its predicted labels, so a query for a label matches every document the model assigned it. Which labels those are is the model's business alone: the model file fixes both the label set and the prefix carried by every term.

## See also

- [nearest_neighbors](./nearest-neighbors.md) — expand text with semantically related terms
- [Full-Text Search](../../indexes/inverted/full-text-search.md)
- [`classify_text()`](../../functions/search/tokenizers.md#classify_text) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
