---
title: CREATE TEXT SEARCH DICTIONARY
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TEXT SEARCH DICTIONARY` statement defines a *text search dictionary* — the analyzer that turns raw text into the tokens stored in an [inverted index](../create_index/index.md). The dictionary controls every stage of that transformation: how text is split into tokens, how each token is normalized (case folding, accent folding, stemming) and which extra information (term positions, frequencies) is recorded for searching and ranking. The same dictionary is applied both when a column is indexed and when a full-text query runs against that column, so the data and the query are always analyzed the same way.

Every dictionary is built from a single **template**. A template implements one analysis strategy — splitting text on word boundaries, cutting on a delimiter, emitting character n-grams, filtering stop words and so on — and exposes its own set of options. Templates can also be composed: [`pipeline`](./pipeline/index.md) chains several analyzers end to end, [`minhash`](./minhash/index.md) wraps another analyzer to emit similarity signatures and [`copy_from`](./copy-from.md) derives a variant of an existing dictionary.

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
| [`ngram`](./ngram.md) | Generate character n-grams for fuzzy and substring matching |
| [`sparse_ngram`](./sparse-ngram.md) | Generate sparse variable-length n-grams for substring search over code and logs |
| [`wildcard`](./wildcard.md) | Generate boundary-marked n-grams for wildcard and prefix matching |
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
| [`minhash`](./minhash/index.md) | Generate MinHash signatures with a nested analyzer |
| [`copy_from`](./copy-from.md) | Copy and override an existing dictionary |

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

Independently of the template, the following flags control how much information the index records about each token. They are all off by default — enable only what your queries need, since each one increases the size of the index.

| Flag | Default | Description |
|---|---|---|
| `FREQUENCY` | `false` | Store term frequency (needed for relevance scoring) |
| `POSITION` | `false` | Store term positions (needed for phrase queries) |
| `NORM` | `false` | Store the normalization factor |
| `OFFSET` | `false` | Store character offsets |

Enable `FREQUENCY` when you rank results by relevance and `POSITION` when you run phrase or proximity queries. The dictionary in the example above sets both.

## See also

- [CREATE INDEX](../create_index/index.md) — attach a dictionary to a column with an inverted index
- [DROP](../drop/index.md) — remove a text search dictionary

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
