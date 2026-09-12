---
title: "pipeline"
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# pipeline

The `pipeline` template composes several analyzers into one dictionary, feeding the output of each step as the input to the next. This builds behavior no single template offers — for example, split a field on a delimiter and then apply full [`split_text`](../text.md) analysis (case folding, stemming, stopwords) to each resulting piece.

A pipeline is a chain: each `|` feeds the tokens of the stage on its left into the stage on its right, so `split_csv(',') | normalize_tokens(case := 'lower')` splits on commas and lowercases each piece. Stages run strictly in order, so a tokenizer that splits text must come before filters like [`remove_stopwords`](../stopwords.md) or [`stem_words`](../stem.md) that refine the tokens it produces. Where [`union`](../union.md) runs members in parallel and merges their output, `pipeline` chains them so each stage transforms the previous stage's tokens.

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_007" />

A chain of one stage is that template alone, `(a | b) | c` is the same flat pipeline as `a | b | c`, and an SQL expression over `$1` joins the chain as a [`sql`](../sql.md) stage:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_008" />

## Stages

A stage may be any template, a [`union`](../union.md) list, or a bare dictionary name that runs a stored dictionary's analyzer, so `pipe_dict | remove_stopwords(['bar'])` appends a stage to a stored pipeline.

With exactly one stage the pipeline is unwrapped, so the dictionary tokenizes exactly like that template used on its own. A [`keyword`](../keyword.md) stage in any position after the first is dropped, because it passes its input through unchanged.

Two shapes are rejected when the dictionary is created. Both name the offending stage with a zero-based index that counts the stages left after dropped `keyword` stages:

- `pipeline: stage <i> expects <TYPE> input, but the preceding stage produces <TYPE>` — a step's input type must match the previous step's output. Every step takes `VARCHAR` input, so a step that emits binary terms can only be the last one: [`collate_tokens`](../collation.md), [`encode_geopoint`](../geopoint.md) and [`encode_geojson`](../geojson.md). A [`sql`](../sql.md) step is not covered by this check: its expression is bound after the chain is validated, so the check sees `VARCHAR` whatever the expression returns.
- `pipeline: stage <i> produces a per-document store blob, which a pipeline cannot deliver` — a stage whose analyzer stores a per-document blob cannot sit in a chain. This rules out [`generate_wildcard_ngrams`](../wildcard.md) in any position, [`generate_shingles`](../shingle.md) unless `storetokens := false`, and `encode_geojson` with a `coding` other than `source`.

Both checks run only for a chain of two or more stages left after dropped `keyword` stages, since a single stage is unwrapped into the bare template.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](../index.md#feature-flags). `OFFSET` requires every stage to report offsets, because the pipeline folds that trait across the chain: one stage without offsets — [`sql`](../sql.md), [`generate_sparse_ngrams`](../sparse-ngram.md), [`union`](../union.md), [`generate_shingles`](../shingle.md), [`encode_geopoint`](../geopoint.md) or [`encode_geojson`](../geojson.md) — makes `WITH (offset)` fail when the dictionary is created, with `Unsupported index features are specified`.

## Tokenization

Step 1 consumes the raw value; every later step is called once per token the step before it produced, and what it emits replaces that token. A first step of [`split_csv`](../csv.md) on `,` splits `RED,Green,BLUE` into three tokens, then a [`normalize_tokens`](../norm.md) second step lowercases each, giving `{red,green,blue}`. Swap the second step for [`split_text`](../text.md) with stemming and the same split feeds a stemmer, so `Cats,RUNNING` becomes `{cat,run}` — a split-then-analyze behavior no single template provides.

| Input | Steps | Tokens |
|---|---|---|
| `RED,Green,BLUE` | `split_csv(',') \| normalize_tokens(case := 'lower')` | `{red,green,blue}` |
| `Cats,RUNNING` | `split_csv(',') \| split_text(case := 'lower') \| stem_words('en_US.UTF-8')` | `{cat,run}` |

Split on commas, then lowercase each piece:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_005" />

Replace the second step with `split_text` analysis so each piece is also stemmed:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_006" />

Positions are consecutive and a dropped token leaves no hole: when a later step removes a token — a stop word, or a token it cannot handle such as an over-long collation key — the surviving tokens keep consecutive positions. A step that fans out, like [`generate_ngrams`](../ngram.md) or a synonym step, has its own positions rebased onto the parent stream. Only steps that assign positions themselves — [`classify_text`](../classification.md), [`find_nearest_words`](../nearest-neighbors.md), [`expand_solr_synonyms`](../solr-synonyms.md) and [`union`](../union.md) — keep their stacked numbering. If the first step rejects a value outright, that value yields no tokens at all and the following values are unaffected.

When offsets are available they always index the original input: each step's offsets are rebased into the span of the parent token they came from, and a token that reaches the end of its parent's text inherits the parent's end offset. The pipeline itself never transforms bytes: case folding, accent handling and Unicode normalization are each step's own business.

## Examples

### Delimiter then text analysis

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_001" />

### Three-step pipeline with stopwords

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_002" />

### N-grams then normalization

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_003" />

### Longer chain

Grouping with parentheses changes nothing: `(a | b) | c` is the flat chain `a | b | c`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pipeline/index/example_004" />

## See also

- [`union`](../union.md) — run analyzers in parallel and merge their tokens
- [CREATE TEXT SEARCH DICTIONARY](../index.md)

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
