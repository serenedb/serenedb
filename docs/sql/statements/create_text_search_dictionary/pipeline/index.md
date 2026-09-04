---
title: "pipeline"
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# pipeline

The `pipeline` template composes several analyzers into one dictionary, feeding the output of each step as the input to the next. This builds behavior no single template offers — for example, split a field on a delimiter and then apply full [`text`](../text.md) analysis (case folding, stemming, stopwords) to each resulting piece.

Each step names its own template and options, with every option prefixed by the step's position, numbered from `1`: `STEP1_TEMPLATE` and its `STEP1_…` options, then `STEP2_TEMPLATE` and so on. Steps run strictly in order, so a tokenizer that splits text must come before filters like [`stopwords`](../stopwords.md) or [`stem`](../stem.md) that refine the tokens it produces. Where [`union`](../union.md) runs members in parallel and merges their output, `pipeline` chains them so each step transforms the previous step's tokens.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `STEP⟨N⟩_TEMPLATE` | string | **required** | Template of the Nth step (numbered from 1) |
| `STEP⟨N⟩_*` | — | — | Options for the Nth step, prefixed with `STEP⟨N⟩_` |

A step may itself be a `pipeline`; nest by chaining the prefixes, e.g. `STEP2_STEP1_TEMPLATE`.

## Tokenization

Each step consumes the tokens emitted by the one before it. A first step of [`delimiter`](../delimiter.md) on `,` splits `RED,Green,BLUE` into three tokens, then a [`norm`](../norm.md) second step lowercases each, giving `{red,green,blue}`. Swap the second step for [`text`](../text.md) with stemming and the same split feeds a stemmer, so `Cats,RUNNING` becomes `{cat,run}` — a split-then-analyze behavior no single template provides.

| Input | Steps | Tokens |
|---|---|---|
| `RED,Green,BLUE` | `delimiter` (`,`) → `norm` (`CASE = 'lower'`) | `{red,green,blue}` |
| `Cats,RUNNING` | `delimiter` (`,`) → `text` (`CASE = 'lower'`, `STEMMING = true`) | `{cat,run}` |

Split on commas, then lowercase each piece:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_005" />

Replace the second step with `text` analysis so each piece is also stemmed:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_006" />

## Examples

### Delimiter then text analysis

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_001" />

### Three-step pipeline with stopwords

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_002" />

### N-grams then normalization

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_003" />

### Nested pipeline

A pipeline step can itself be a pipeline. Nest with `STEP⟨N⟩_STEP⟨M⟩_`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_004" />

## See also

- [`union`](../union.md) — run analyzers in parallel and merge their tokens
- [`minhash`](../minhash/index.md) — another composition template
- [CREATE TEXT SEARCH DICTIONARY](../../create_text_search_dictionary/index.md)

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
