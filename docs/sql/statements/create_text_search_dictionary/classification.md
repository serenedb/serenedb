---
title: "classification"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# classification

The `classification` template runs a pre-trained text-classification model over the input and emits its predicted category labels as tokens. This lets you index documents by an inferred property — topic, language, sentiment — and then query them by label like any other term, without storing the label yourself.

## How it works

At index and query time the analyzer feeds the input text to a [fastText](https://fasttext.cc/) supervised model loaded from `modellocation` and turns the model's predictions into terms. For example a cooking-topic model might turn _"How do I bake a chocolate cake?"_ into the labels `__label__cake`, `__label__chocolate` and `__label__baking`. `topk` caps how many of the highest-scoring labels are emitted and `threshold` drops any whose confidence falls below the given score.

The model file is **required** and must be reachable from the server process at the path given in `modellocation`; the dictionary cannot be created without a loadable model. Classification and [`nearest_neighbors`](./nearest-neighbors.md) are the two model-backed templates — use `classification` to tag a document with predicted categories, and `nearest_neighbors` to expand it with semantically related terms.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `modellocation` | string | **required** | Path to the fastText model file, reachable from the server |
| `topk` | integer | `1` | Number of top-scoring labels to emit |
| `threshold` | double | `0.0` | Minimum confidence score to keep a label (0.0–1.0) |

## Usage

Point `modellocation` at a trained fastText classifier and choose how many labels to keep:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/classification/example_001" />

With the dictionary attached to a text column in a `USING inverted` index, each document is indexed under its predicted labels, so a query for a label matches every document the model assigned it. Predicted labels are model-specific; the example above uses a small cooking-topic model and emits labels such as `__label__cake`.

## See also

- [nearest_neighbors](./nearest-neighbors.md) — expand text with semantically related terms
- [Full-Text Search](../../indexes/inverted/full-text-search.md)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
