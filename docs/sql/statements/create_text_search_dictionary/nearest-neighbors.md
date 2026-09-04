---
title: "nearest_neighbors"
sidebar_label: Nearest Neighbors
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# nearest_neighbors

The `nearest_neighbors` template uses a pre-trained embedding model to expand each input token with the terms whose vectors lie closest to it. In effect it enriches the text with semantically related words, so a document indexed through this template can be found by synonyms and near-synonyms it never literally contained — a recall-oriented complement to exact full-text matching.

## How it works

For every token in the input the analyzer asks a [fastText](https://fasttext.cc/) embedding model loaded from `modellocation` for its `topk` nearest neighbors and emits those neighbor words as additional terms. Applied at index time it broadens what a document can match; applied to the query it broadens what the query reaches. For example a cooking model might expand _"cake"_ into related terms such as `three-tiered` and `wham`.

The model file is **required** and must be reachable from the server process at the path given in `modellocation`; the dictionary cannot be created without a loadable model. Where [`classification`](./classification.md) tags a document with predicted category labels, `nearest_neighbors` instead grows its vocabulary with related terms.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `modellocation` | string | **required** | Path to the fastText model file, reachable from the server |
| `topk` | integer | `1` | Number of nearest neighbors to emit per input token |

## Usage

Point `modellocation` at a trained fastText model and choose how many neighbors to add per token:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/nearest-neighbors/example_001" />

Attached to a text column in a `USING inverted` index, the dictionary indexes each document under both its own tokens and their nearest neighbors, widening recall. The emitted neighbors depend on the model; the example above uses a small cooking model.

## See also

- [classification](./classification.md) — tag text with predicted category labels
- [Full-Text Search](../../indexes/inverted/full-text-search.md)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
