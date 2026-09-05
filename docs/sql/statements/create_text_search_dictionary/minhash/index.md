---
title: "minhash"
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# minhash

The `minhash` template wraps a nested tokenizer and replaces its tokens with a compact [MinHash](https://en.wikipedia.org/wiki/MinHash) signature that approximates the *set* of tokens. Two texts with similar token sets produce overlapping signatures, so comparing signatures estimates their Jaccard similarity cheaply — the basis for approximate deduplication and near-duplicate detection across large collections.

The nested analyzer that produces the underlying tokens is configured through `TOKENIZER_TEMPLATE` and its `TOKENIZER_`-prefixed options, while `NUMHASHES` sets how many hash functions form the signature, trading index size for accuracy. Unlike the word-emitting templates, the output tokens are opaque hash values, not readable words — you never search them by hand; you compare whole signatures.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `TOKENIZER_TEMPLATE` | string | **required** | Template for the nested analyzer |
| `TOKENIZER_*` | — | — | Options for the nested analyzer, prefixed with `TOKENIZER_` |
| `NUMHASHES` | integer | `1` | Number of hash functions in the signature |

## Tokenization

The nested analyzer turns the input into a set of tokens, then `minhash` reduces that set to `NUMHASHES` hash values — the signature. The signature does not depend on token order or duplicates, only on which distinct tokens are present, so two documents that share most of their words share most of their signature components. The output is therefore **not human-readable**: each token is an opaque hash like `c1vB9RXiIbY`, and the values are specific to the engine build, so do not match on individual components — compare full signatures instead.

With a `text` nested analyzer and `NUMHASHES = 4`, three documents analyze roughly as follows (signatures shown are illustrative — the exact bytes are engine-specific):

| Input | Signature (4 hashes) | Shared with A |
|---|---|---|
| **A** `the quick brown fox jumps over the lazy dog` | `{c1vB9RXiIbY, q9VZS3VMEoY, U9QEhWO/5Dw, Y9at6wPcrAk}` | — |
| **B** `the quick brown fox jumps over the very lazy dog` | `{q9VZS3VMEoY, gEr3V+sGyUU, U9QEhWO/5Dw, Y9at6wPcrAk}` | 3 of 4 |
| **C** `completely unrelated words here today` | `{S8NxTSLm0N0, K+vCUulME9s, ScE3enAjqb8, 0lvqjdgsEcc}` | 0 of 4 |

Inserting one word (A → B) leaves three of the four hashes unchanged, signalling a near-duplicate, whereas the unrelated document C shares none. The signature is also deterministic: the same text always yields the same hashes, which is what makes overlap a usable similarity estimate. Raising `NUMHASHES` sharpens that estimate at the cost of a larger index.

You can inspect a signature with `ts_lexize`, though the values themselves carry no readable meaning:

```sql
SELECT ts_lexize('your_minhash_dict', 'the quick brown fox');
```

## Examples

### MinHash with delimiter analyzer

<SqlLogicTest id="sql/statements/create_text_search_dictionary/minhash/index/example_001" />

### MinHash with text analyzer

<SqlLogicTest id="sql/statements/create_text_search_dictionary/minhash/index/example_002" />

## See also

- [`pipeline`](../pipeline/index.md) — chain analyzers in sequence
- [`union`](../union.md) — run analyzers in parallel and merge their tokens
- [CREATE TEXT SEARCH DICTIONARY](../../create_text_search_dictionary/index.md)

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
