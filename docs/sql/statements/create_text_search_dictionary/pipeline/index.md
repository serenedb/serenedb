---
title: "pipeline"
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# pipeline

The `pipeline` template composes several analyzers into one dictionary, feeding the output of each step as the input to the next. This builds behavior no single template offers — for example, split a field on a delimiter and then apply full [`text`](../text.md) analysis (case folding, stemming, stopwords) to each resulting piece.

Each step names its own template and options, with every option prefixed by the step's position, numbered densely from `1`: `STEP1_TEMPLATE` and its `STEP1_…` options, then `STEP2_TEMPLATE` and so on. Steps run strictly in order, so a tokenizer that splits text must come before filters like [`stopwords`](../stopwords.md) or [`stem`](../stem.md) that refine the tokens it produces. Where [`union`](../union.md) runs members in parallel and merges their output, `pipeline` chains them so each step transforms the previous step's tokens.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `STEP⟨N⟩_TEMPLATE` | string | — | Template of the Nth step (numbered densely from 1); not required, and a pipeline with no step emits no tokens |
| `STEP⟨N⟩_*` | — | — | Options for the Nth step, prefixed with `STEP⟨N⟩_` |

Steps are read densely from `1` and the scan stops at the first missing `STEP⟨N⟩_TEMPLATE`, so a gap — `STEP1_*` and `STEP3_*` with no `STEP2_TEMPLATE` — leaves the higher-numbered options unconsumed and fails with `option "step3_template" not recognized`. An option the step's own template does not accept is rejected the same way, and a name that is not a template fails with `Invalid type of text search dictionary`.

A step may be any template, including another `pipeline`, a [`union`](../union.md), or [`copy_from`](../copy-from.md) to adopt a stored dictionary's configuration. Prefixes nest: a pipeline step inside a pipeline is `STEP2_STEP1_TEMPLATE`, a union step's first member is `STEP1_TOKENIZER1_TEMPLATE`, and a `copy_from` step names its source as `STEP1_FROM`. A dictionary copied from a pipeline can override a single step's options — `FROM = 'pipe_dict', STEP2_CASE = 'upper'` — and can append a step the source did not have.

Neither the step count nor `STEP1_TEMPLATE` is enforced: with no step options at all the dictionary is created and emits no tokens, and with exactly one step the pipeline is unwrapped, so the dictionary tokenizes exactly like that template used on its own. Give at least two steps for a pipeline to do anything. A [`keyword`](../keyword.md) step in any position after the first is dropped, because it passes its input through unchanged.

Two shapes are rejected when the dictionary is created. Both name the offending stage with a zero-based index that counts the steps left after dropped `keyword` steps, so it does not line up with the `STEP⟨N⟩` numbering:

- `pipeline: stage <i> expects <TYPE> input, but the preceding stage produces <TYPE>` — a step's input type must match the previous step's output. Every step takes `VARCHAR` input, so a step that emits binary terms can only be the last one: [`collation`](../collation.md), [`geopoint`](../geopoint.md) and [`geojson`](../geojson.md). A [`sql`](../sql.md) step is not covered by this check: its expression is bound after the chain is validated, so the check sees `VARCHAR` whatever the expression returns.
- `pipeline: stage <i> produces a per-document store blob, which a pipeline cannot deliver` — a step whose analyzer stores a per-document blob cannot sit in a chain. This rules out [`wildcard`](../wildcard.md) in any position, [`shingle`](../shingle.md) unless `STORETOKENS = false`, and `geojson` with a `CODING` other than `source`.

Both checks run only for a chain of two or more steps left after dropped `keyword` steps, since a single step is unwrapped into the bare template.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](../index.md#feature-flags). `OFFSET` requires every step to report offsets, because the pipeline folds that trait across the chain: one step without offsets — [`sql`](../sql.md), [`sparse_ngram`](../sparse-ngram.md), [`union`](../union.md), [`shingle`](../shingle.md), [`geopoint`](../geopoint.md) or [`geojson`](../geojson.md) — makes `OFFSET = true` fail when the dictionary is created, with `Unsupported index features are specified`.

## Tokenization

Step 1 consumes the raw value; every later step is called once per token the step before it produced, and what it emits replaces that token. A first step of [`delimiter`](../delimiter.md) on `,` splits `RED,Green,BLUE` into three tokens, then a [`norm`](../norm.md) second step lowercases each, giving `{red,green,blue}`. Swap the second step for [`text`](../text.md) with stemming and the same split feeds a stemmer, so `Cats,RUNNING` becomes `{cat,run}` — a split-then-analyze behavior no single template provides.

| Input | Steps | Tokens |
|---|---|---|
| `RED,Green,BLUE` | `delimiter` (`,`) → `norm` (`CASE = 'lower'`) | `{red,green,blue}` |
| `Cats,RUNNING` | `delimiter` (`,`) → `text` (`CASE = 'lower'`, `STEMMING = true`) | `{cat,run}` |

Split on commas, then lowercase each piece:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_005" />

Replace the second step with `text` analysis so each piece is also stemmed:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_006" />

Positions are consecutive and a dropped token leaves no hole: when a later step removes a token — a stop word, or a token it cannot handle such as an over-long collation key — the surviving tokens keep consecutive positions. A step that fans out, like [`ngram`](../ngram.md) or a synonym step, has its own positions rebased onto the parent stream. Only steps that assign positions themselves — [`classification`](../classification.md), [`nearest_neighbors`](../nearest-neighbors.md), [`solr_synonyms`](../solr-synonyms.md) and [`union`](../union.md) — keep their stacked numbering. If the first step rejects a value outright, that value yields no tokens at all and the following values are unaffected.

When offsets are available they always index the original input: each step's offsets are rebased into the span of the parent token they came from, and a token that reaches the end of its parent's text inherits the parent's end offset. The pipeline itself never transforms bytes: case folding, accent handling and Unicode normalization are each step's own business.

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
- [`copy_from`](../copy-from.md) — build a pipeline from a stored dictionary's configuration and override one step
- [CREATE TEXT SEARCH DICTIONARY](../index.md)

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
