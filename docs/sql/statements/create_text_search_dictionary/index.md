---
title: CREATE TEXT SEARCH DICTIONARY
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TEXT SEARCH DICTIONARY` statement defines a *text search dictionary* — the analyzer that turns raw text into the tokens stored in an [inverted index](../create_index/index.md). The dictionary controls every stage of that transformation: how text is split into tokens, how each token is normalized (case folding, accent folding, stemming) and which extra information (term positions, frequencies) is recorded for searching and ranking. The same dictionary is applied both when a column is indexed and when a full-text query runs against that column, so the data and the query are always analyzed the same way.

Every dictionary is built from a single **template**. A template implements one analysis strategy — splitting text on word boundaries, cutting on a delimiter, emitting character n-grams, filtering stop words and so on — and exposes its own set of options. Templates can also be composed: [`pipeline`](./pipeline/index.md) chains several analyzers end to end, [`union`](./union.md) merges the tokens of several analyzers and [`generate_shingles`](./shingle.md) wraps another analyzer to emit word n-grams.

A dictionary is written as an *analyzer expression* after `AS`: templates are function calls, stages are chained with `|`, and ordinary SQL functions can take part as stages.

## The analyzer expression

The expression after `AS` names each template as a function call and chains stages with `|`, so a pipeline reads left to right:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_004" />

```
CREATE TEXT SEARCH DICTIONARY [IF NOT EXISTS] name AS <chain> [WITH (<flag> [, ...])]

<chain> := <stage> [| <stage> ...]
<stage> := <template>(<arguments>)
         | generate_shingles(<chain>, <arguments>) | generate_wildcard_ngrams(<chain>, <arguments>)
         | [<chain>, <chain>, ...]
         | <SQL expression over $1>
         | <dictionary name>
```

- **Template stage.** `split_text(case := 'lower')` is the [`split_text`](./text.md) template with its options as arguments. Named arguments use DuckDB's `name := value` and take the option names of the template's page; positional arguments bind to the options in the order `HELP` prints them, which is also the order of the template page's option table, so `generate_ngrams(2, 3)` sets `MINGRAM` and `MAXGRAM` and `split_csv(',')` sets `DELIMITER`. A `NULL` argument leaves its option at the default. Template and option names are case-insensitive. A list-valued option takes a list: `remove_stopwords(['the', 'a'])`.
- **Chain.** `a | b | c` runs the stages in order, each one re-analyzing every token the previous one produced, and creates a [`pipeline`](./pipeline/index.md); a single stage creates that template directly. Parentheses group, and `(a | b) | c` is the same three-step pipeline as `a | b | c`.
- **Wrappers.** [`generate_shingles`](./shingle.md) and [`generate_wildcard_ngrams`](./wildcard.md) analyze the tokens of a nested analyzer, so they take a chain as their first argument: `generate_shingles(split_csv(' ') | normalize_tokens(case := 'lower'), 2, 3)`.
- **Union.** A list of chains runs all of them over the same input and merges the tokens, which is the [`union`](./union.md) template: `[keyword(), generate_ngrams(2, 2)]`.
- **SQL stages.** Any other expression that reads its input as `$1` becomes a [`sql`](./sql.md) stage: `lower($1)` in front of a tokenizer preprocesses the value, `upper($1)` behind one rewrites each token, and a list-returning function such as `string_split($1, ',')` fans a value out into tokens. The expression is written inline, so nothing needs quote doubling. `sql(<expression>)` marks a stage as SQL explicitly, and inside it no name is read as a template. A one-parameter lambda names the token instead of using `$1`: `split_csv(',') | (x -> upper(trim(x)))`; the parentheses are needed because the lambda arrow binds looser than `|`.
- **Existing dictionaries.** A bare dictionary name, optionally schema-qualified, is a stage that runs that dictionary's analyzer, so `english_dict | remove_stopwords(['run'])` builds on a stored dictionary and `english_dict` alone duplicates one. The stage copies the dictionary's configuration as it stands when the new dictionary is created, so dropping or altering the source afterwards does not affect the copy; the source's feature flags are not copied.
- **Features.** `WITH (frequency, position)` carries the [feature flags](#feature-flags) and `help`, and nothing else: every analyzer option belongs in the expression.

A template name always wins over an SQL function of the same name. At a stage position `stem_words(...)` is the template, and a template-named call inside an SQL stage is rejected rather than silently taken as a function; to call such a function, qualify it — `main.norm($1)` for a user macro named `normalize_tokens` — or wrap the stage in `sql(...)`. `|` binds tighter than `||` and the comparison operators, so an SQL stage that uses them needs parentheses: `(lower($1) || '-') | split_csv('-')`.

SQL functions as stages:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_005" />

A union of two analyzers:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_006" />

An existing dictionary as a building block:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_007" />

## The WITH clause

| Option | Type | Default | Description |
|---|---|---|---|
| `frequency`, `position`, `norm`, `offset` | boolean | `false` | The [feature flags](#feature-flags) |
| `help` | boolean | `false` | Report every template as a call signature with its options, defaults and descriptions, as an error message, and create nothing |

Names in the `WITH` clause are case-insensitive, and a flag written without a value means `= true`, so `WITH (frequency, position)` is the same as `WITH (frequency = true, position = true)`. Template names and option names in the expression are case-insensitive too, and so are the values of the enum-valued options — `case`, `break`, `mode`, `inputtype` and the like. Every analyzer option comes from the template it belongs to and is documented on that template's page.

## Examples

Create a dictionary that lower-cases its input, applies English stemming and stores the term frequencies and positions needed for relevance ranking and phrase search, then attach it to two columns with an inverted index:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_001" />

The dictionary is referenced by name in the index column list (`title english_dict`, `body english_dict`). Once the index exists, full-text queries against those columns are analyzed with the same dictionary, so a search term matches the indexed tokens even when the surface forms differ:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_002" />

Because `english_dict` stems its input, the query term `searching` is reduced to `search` and matches every row whose `body` contains a form of that word. To see exactly how a dictionary tokenizes a string — invaluable when tuning options — pass it to `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_003" />

## Templates

A dictionary is one template call, or a chain or list of them. The available templates are grouped below by what they do; follow a link for the options each one accepts.

### Text processing

These templates turn human language into searchable tokens.

| Template | Description |
|---|---|
| [`split_text`](./text.md) | Split text into words on Unicode word boundaries, optionally case-folded |
| [`split_text_icu`](./icu_text.md) | Segment text into words or sentences with ICU for a given locale |
| [`generate_ngrams`](./ngram.md) | Generate character n-grams for fuzzy and substring matching |
| [`generate_sparse_ngrams`](./sparse-ngram.md) | Generate sparse variable-length n-grams for substring search over code and logs |
| [`generate_wildcard_ngrams`](./wildcard.md) | Generate boundary-marked n-grams for wildcard and prefix matching |
| [`generate_shingles`](./shingle.md) | Join the tokens of a nested analyzer into word n-grams for phrase search |
| [`stem_words`](./stem.md) | Apply stemming only |
| [`normalize_tokens`](./norm.md) | Normalize case and accents without tokenization |
| [`keyword`](./keyword.md) | Emit the whole input as one verbatim token |

### Splitting & filtering

These templates carve structured text into tokens or refine an existing token stream.

| Template | Description |
|---|---|
| [`split_csv`](./csv.md) | Split on a single delimiter |
| [`split_by_delimiters`](./multi-delimiter.md) | Split on multiple delimiters |
| [`split_by_non_alpha`](./split_by_non_alpha.md) | Split on every byte that is not an ASCII letter or digit |
| [`split_by_pattern`](./pattern.md) | Match or split with a regular expression |
| [`expand_path`](./path-hierarchy.md) | Tokenize a path into its hierarchical prefixes |
| [`remove_stopwords`](./stopwords.md) | Filter out stop words |
| [`collate_tokens`](./collation.md) | Produce collation keys for sorting |

### Composition

These templates build a dictionary out of other dictionaries.

| Template | Description |
|---|---|
| [`pipeline`](./pipeline/index.md) | Chain multiple analyzers in sequence |
| [`union`](./union.md) | Merge the tokens of several analyzers run in parallel |

Composition is written in the expression itself: `a | b` is a pipeline, `[a, b]` a union, `generate_shingles(a, ...)` or `generate_wildcard_ngrams(a, ...)` a wrapper over the nested analyzer `a`, and a bare dictionary name copies an existing dictionary.

### Expressions

This template derives tokens by evaluating a SQL expression.

| Template | Description |
|---|---|
| [`sql`](./sql.md) | Emit the result of a DuckDB scalar expression over the input value |

### Synonyms

These templates expand a token into its synonyms so a search finds related wording.

| Template | Description |
|---|---|
| [`expand_solr_synonyms`](./solr-synonyms.md) | Expand tokens using a Solr-format synonyms map |
| [`expand_wordnet_synonyms`](./wordnet-synonyms.md) | Expand tokens using a WordNet synonyms database |

### Geospatial

These templates index geometries and coordinates for [geospatial search](../../indexes/inverted/geospatial-search.md).

| Template | Description |
|---|---|
| [`encode_geojson`](./geojson.md) | Index GeoJSON geometries (points, lines, polygons) |
| [`encode_geopoint`](./geopoint.md) | Index latitude/longitude points |

### Machine learning

These templates run a pre-trained model (for example [fastText](https://fasttext.cc/)) to emit tokens.

| Template | Description |
|---|---|
| [`classify_text`](./classification.md) | ML-based text classification |
| [`find_nearest_words`](./nearest-neighbors.md) | ML-based nearest neighbor tokens |

## Feature flags

The following flags control how much information the index records about each token. They are all off by default — enable only what your queries need, since each one increases the size of the index.

| Flag | Default | Description |
|---|---|---|
| `FREQUENCY` | `false` | Store term frequency (needed for relevance scoring) |
| `POSITION` | `false` | Store term positions (needed for phrase queries); requires `FREQUENCY` |
| `NORM` | `false` | Store the field length normalization factor; requires `FREQUENCY` |
| `OFFSET` | `false` | Store the byte offsets of each token in the source value; requires `POSITION` |

Enable `FREQUENCY` when you rank results by relevance and `POSITION` when you run phrase or proximity queries. The dictionary in the example above sets both. The four flags belong to the dictionary, not to a stage: they are written in the `WITH (...)` clause after the expression, and a stored dictionary used as a stage does not carry its own flags into the new dictionary.

Not every template records every flag, and a dictionary that asks for a flag its template does not support is rejected when it is created. The geospatial templates record none of the four, [`generate_wildcard_ngrams`](./wildcard.md) accepts only `FREQUENCY` and `POSITION`, [`generate_sparse_ngrams`](./sparse-ngram.md) only `FREQUENCY` and `NORM`, and [`union`](./union.md), [`sql`](./sql.md) and [`generate_shingles`](./shingle.md) accept every flag except `OFFSET`. Two further limits depend on the configured analyzer rather than on its template: `OFFSET` requires an analyzer that tracks offsets, so a [`pipeline`](./pipeline/index.md) whose stages drop them is rejected, and `NORM` cannot be combined with an analyzer that stores a per-document blob, which is what `generate_shingles` does unless `storetokens := false`.

## See also

- [CREATE INDEX](../create_index/index.md) — attach a dictionary to a column with an inverted index
- [DROP](../drop/index.md) — remove a text search dictionary

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
