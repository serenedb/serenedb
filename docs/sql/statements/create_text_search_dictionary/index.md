---
title: CREATE TEXT SEARCH DICTIONARY
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TEXT SEARCH DICTIONARY` statement defines a *text search dictionary* — the analyzer that turns raw text into the tokens stored in an [inverted index](../create_index/index.md). The dictionary controls every stage of that transformation: how text is split into tokens, how each token is normalized (case folding, accent folding, stemming) and which extra information (term positions, frequencies) is recorded for searching and ranking. The same dictionary is applied both when a column is indexed and when a full-text query runs against that column, so the data and the query are always analyzed the same way.

Every dictionary is built from a single **template**. A template implements one analysis strategy — splitting text on word boundaries, cutting on a delimiter, emitting character n-grams, filtering stop words and so on — and exposes its own set of options. Templates can also be composed: [`pipeline`](./pipeline/index.md) chains several analyzers end to end, [`union`](./union.md) merges the tokens of several analyzers, [`shingle`](./shingle.md) wraps another analyzer to emit word n-grams and [`copy_from`](./copy-from.md) derives a variant of an existing dictionary.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `TEMPLATE` | string | **required** | The analysis template the dictionary is built from, one of the [templates](#templates) below |
| `HELP` | boolean | `false` | Report the whole option tree — every template with its options, types and defaults — as an error message and create nothing |

Option names are case-insensitive, and an option written without a value means `= true`, so `(frequency, position)` is the same as `frequency = true, position = true`. The value of `TEMPLATE` is compared literally and must therefore be lower case, while the values of the enum-valued options — `CASE`, `BREAK`, `MODE`, `INPUTTYPE` and the like — are matched case-insensitively. Every other option comes either from the chosen template, documented on that template's page, or from the feature flags below.

## Examples

Create a dictionary that lower-cases its input, applies English stemming and stores the term frequencies and positions needed for relevance ranking and phrase search, then attach it to two columns with an inverted index:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_001" />

The dictionary is referenced by name in the index column list (`title english_dict`, `body english_dict`). Once the index exists, full-text queries against those columns are analyzed with the same dictionary, so a search term matches the indexed tokens even when the surface forms differ:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_002" />

Because `english_dict` stems its input, the query term `searching` is reduced to `search` and matches every row whose `body` contains a form of that word. To see exactly how a dictionary tokenizes a string — invaluable when tuning options — pass it to `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_003" />

## Templates

A dictionary must name exactly one template through the `TEMPLATE` option. The available templates are grouped below by what they do; follow a link for the options each one accepts.

### Text processing

These templates turn human language into searchable tokens.

| Template | Description |
|---|---|
| [`text`](./text.md) | Tokenize into words with stemming, stopwords and accent handling |
| [`icu_text`](./icu_text.md) | Segment text into words or sentences with ICU for a given locale |
| [`ngram`](./ngram.md) | Generate character n-grams for fuzzy and substring matching |
| [`sparse_ngram`](./sparse-ngram.md) | Generate sparse variable-length n-grams for substring search over code and logs |
| [`wildcard`](./wildcard.md) | Generate boundary-marked n-grams for wildcard and prefix matching |
| [`shingle`](./shingle.md) | Join the tokens of a nested analyzer into word n-grams for phrase search |
| [`stem`](./stem.md) | Apply stemming only |
| [`norm`](./norm.md) | Normalize case and accents without tokenization |
| [`keyword`](./keyword.md) | Emit the whole input as one verbatim token |
| [`segmentation`](./segmentation.md) | Segment text by Unicode word boundaries |

### Splitting & filtering

These templates carve structured text into tokens or refine an existing token stream.

| Template | Description |
|---|---|
| [`delimiter`](./delimiter.md) | Split on a single delimiter |
| [`multi_delimiter`](./multi-delimiter.md) | Split on multiple delimiters |
| [`split_by_non_alpha`](./split_by_non_alpha.md) | Split on every byte that is not an ASCII letter or digit |
| [`pattern`](./pattern.md) | Match or split with a regular expression |
| [`path_hierarchy`](./path-hierarchy.md) | Tokenize a path into its hierarchical prefixes |
| [`stopwords`](./stopwords.md) | Filter out stop words |
| [`collation`](./collation.md) | Produce collation keys for sorting |

### Composition

These templates build a dictionary out of other dictionaries.

| Template | Description |
|---|---|
| [`pipeline`](./pipeline/index.md) | Chain multiple analyzers in sequence |
| [`union`](./union.md) | Merge the tokens of several analyzers run in parallel |
| [`copy_from`](./copy-from.md) | Copy and override an existing dictionary |

A nested analyzer is configured through prefixed option names: `STEP1_`, `STEP2_` … for the steps of a `pipeline`, `TOKENIZER1_`, `TOKENIZER2_` … for the branches of a `union` and `TOKENIZER_` for the single nested analyzer of `wildcard` or `shingle`. Each prefix carries the nested `TEMPLATE` and that template's own options.

### Expressions

This template derives tokens by evaluating a SQL expression.

| Template | Description |
|---|---|
| [`sql`](./sql.md) | Emit the result of a DuckDB scalar expression over the input value |

### Synonyms

These templates expand a token into its synonyms so a search finds related wording.

| Template | Description |
|---|---|
| [`solr_synonyms`](./solr-synonyms.md) | Expand tokens using a Solr-format synonyms map |
| [`wordnet_synonyms`](./wordnet-synonyms.md) | Expand tokens using a WordNet synonyms database |

### Geospatial

These templates index geometries and coordinates for [geospatial search](../../indexes/inverted/geospatial-search.md).

| Template | Description |
|---|---|
| [`geojson`](./geojson.md) | Index GeoJSON geometries (points, lines, polygons) |
| [`geopoint`](./geopoint.md) | Index latitude/longitude points |

### Machine learning

These templates run a pre-trained model (for example [fastText](https://fasttext.cc/)) to emit tokens.

| Template | Description |
|---|---|
| [`classification`](./classification.md) | ML-based text classification |
| [`nearest_neighbors`](./nearest-neighbors.md) | ML-based nearest neighbor tokens |

## Feature flags

The following flags control how much information the index records about each token. They are all off by default — enable only what your queries need, since each one increases the size of the index.

| Flag | Default | Description |
|---|---|---|
| `FREQUENCY` | `false` | Store term frequency (needed for relevance scoring) |
| `POSITION` | `false` | Store term positions (needed for phrase queries); requires `FREQUENCY` |
| `NORM` | `false` | Store the field length normalization factor; requires `FREQUENCY` |
| `OFFSET` | `false` | Store the byte offsets of each token in the source value; requires `POSITION` |

Enable `FREQUENCY` when you rank results by relevance and `POSITION` when you run phrase or proximity queries. The dictionary in the example above sets both. The four flags are root options: they have no prefixed spelling for a nested analyzer, and [`copy_from`](./copy-from.md) does not inherit them from the source dictionary.

Not every template records every flag, and a dictionary that asks for a flag its template does not support is rejected when it is created. The geospatial templates record none of the four, [`wildcard`](./wildcard.md) accepts only `FREQUENCY` and `POSITION`, [`sparse_ngram`](./sparse-ngram.md) only `FREQUENCY` and `NORM`, and [`union`](./union.md), [`sql`](./sql.md) and [`shingle`](./shingle.md) accept every flag except `OFFSET`. Two further limits depend on the configured analyzer rather than on its template: `OFFSET` requires an analyzer that tracks offsets, so a [`pipeline`](./pipeline/index.md) whose steps drop them is rejected, and `NORM` cannot be combined with an analyzer that stores a per-document blob, which is what `shingle` does unless `STORETOKENS = false`.

## See also

- [CREATE INDEX](../create_index/index.md) — attach a dictionary to a column with an inverted index
- [DROP](../drop/index.md) — remove a text search dictionary

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
