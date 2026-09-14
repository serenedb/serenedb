---
title: "nearest_neighbors"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# nearest_neighbors

The `nearest_neighbors` template replaces each word of its input that the model knows with the words whose vectors lie closest to it in a pre-trained [fastText](https://fasttext.cc/) model. A document indexed through this template can be found by related words it never literally contained — a recall-oriented complement to exact full-text matching. The document's own words are not indexed as such: fastText leaves a word out of its own neighbor list, and this template emits nothing but neighbors, so an input word reaches the index only where the model lists it as a neighbor of another word in the same value.

## How it works

The model is loaded from `MODELLOCATION` when the dictionary is created, and dictionaries naming the same path share one in-memory copy of it. For each word of a value that the model recognizes the analyzer asks for that word's `TOPK` nearest words by cosine similarity and emits them as terms. Applied at index time this decides what a document can match; applied to the query it decides what the query reaches. For example the small cooking model used in the example below turns _"salt"_ into `homogenized` and `teach`.

The model file is **required** and must be reachable from the server process at the path given in `MODELLOCATION`; the dictionary cannot be created without a loadable model, and a file the loader rejects fails with `failed to load fasttext model from: <path>, error: <...>`. Neighbors are looked up per whole word, so the model's dictionary has to hold plain words only: a model carrying character n-grams — `minn` `3` and `maxn` `6`, what `skipgram` and `cbow` train with by default — or word n-grams (`wordNgrams` above `1`) is not supported. Training with fastText's `supervised` mode produces such a model, because that mode sets `minn` and `maxn` to `0`; the cooking model used in the example below is one, and it is the same kind of model [`classification`](./classification.md) needs. Where `classification` tags a document with predicted category labels, `nearest_neighbors` indexes it under related words instead of its own.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MODELLOCATION` | string — a path on the server host | **required** | Path to the fastText model file |
| `TOPK` | integer | `1` | Number of nearest neighbors emitted per recognized word |

`MODELLOCATION` has no usable default. The path is checked on the server process when the statement runs: a path that is not there — an empty string included — fails with `File "<path>" referenced by option "modellocation" does not exist`, and omitting the option altogether fails with `nearest_neighbors: empty model location`. `TOPK` must be greater than zero — `0` or a negative value fails with `nearest_neighbors: top_k must be positive`. A dictionary built with [`copy_from`](./copy-from.md) inherits both options, and either can be respelled there. Any other option — `THRESHOLD`, for instance, which belongs to [`classification`](./classification.md) — fails with `option "<name>" is not applicable in this context`. The index feature flags are dictionary-level options and all four of `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` are supported here, as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

The value is split into words by the model's own reader, which breaks on ASCII whitespace only — space, tab, carriage return, vertical tab, form feed and NUL. A newline ends the reader's line as well as the word before it: the first `\n` in a value becomes fastText's end-of-sentence marker `</s>`, and the words after it are never looked up. That marker is itself a dictionary entry in a model trained on multi-line text, so it is looked up like a word and contributes its own neighbors. Each word the model knows is looked up in its word vectors, and its `TOPK` closest words are emitted in order of descending similarity. A word the model does not know contributes nothing, and neither does a word whose neighbor list comes back empty. An empty value yields no tokens at all.

All neighbors of one input word share a single position, and the position counter advances only for input words that produced at least one neighbor. Every token carries the offsets of the whole input value — start `0`, end the value's length in bytes — rather than the offsets of the word it came from. No case folding, accent folding or Unicode normalization is applied; the input bytes reach the model unchanged.

Which neighbors come out is entirely the model's business, and they are raw model vocabulary, so they can carry punctuation. With the small cooking model used in the example below:

| Input | TOPK | Tokens |
|---|---|---|
| `salt` | `1` | `homogenized` at position 1 |
| `salt` | `2` | `homogenized`, `teach`, both at position 1 |
| `salt oil` | `2` | `homogenized`, `teach` at position 1; `tube"`, `"breather` at position 2 |

## Examples

Point `MODELLOCATION` at a trained fastText model and choose how many neighbors to emit per word:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/nearest-neighbors/example_001" />

Attached to a text column in a `USING inverted` index, the dictionary indexes each document under the nearest neighbors of its words, widening recall.

## See also

- [classification](./classification.md) — tag text with predicted category labels
- [solr_synonyms](./solr-synonyms.md) — expand terms through a synonyms map you write yourself
- [Full-Text Search](../../indexes/inverted/full-text-search.md)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
