---
title: CREATE TEXT SEARCH DICTIONARY
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TEXT SEARCH DICTIONARY` statement defines a *text search dictionary* — the analyzer that turns raw text into the tokens stored in an [inverted index](../create_index/index.md). The dictionary controls every stage of that transformation: how text is split into tokens, how each token is normalized (case folding, accent folding, stemming) and which extra information (term positions, frequencies) is recorded for searching and ranking. The same dictionary is applied both when a column is indexed and when a full-text query runs against that column, so the data and the query are always analyzed the same way.

What follows `AS` is an *analyzer expression*. Each stage of it is a call to a **template** — one analysis strategy, with its own options: split on word boundaries, cut on a delimiter, emit character n-grams, drop stop words. `|` chains stages, so every token one stage emits is re-analyzed by the next; a list merges stages over the same input; a lambda drops to plain SQL for anything no template covers. The statement compiles the expression once, builds the analyzer it describes, checks it against the requested feature flags and stores it under the given name. Nothing is analyzed yet: the name is what `CREATE INDEX` and every full-text query refer to later, and because both sides run the same stored analyzer, a document and a query term are always reduced the same way.

Every template that analyzes text is also a scalar function of the same name, and the two forms are the same tokenizer: `split_text(body, case := 'lower')` in a query produces what a dictionary created `AS split_text(case := 'lower')` produces for `body`. The per-template reference — options, defaults, errors, examples — therefore lives with those functions, under [tokenizer functions](../../functions/search/tokenizers/index.md).

## The analyzer expression

The expression after `AS` names each template as a function call and chains stages with `|`, so a pipeline reads left to right:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_004" />

```
CREATE TEXT SEARCH DICTIONARY [IF NOT EXISTS] name AS <chain> [WITH (<flag> [, ...])]

<chain> := <stage> [| <stage> ...]
<stage> := <template>(<arguments>)
         | generate_shingles(<chain>, <arguments>) | generate_wildcard_ngrams(<chain>, <arguments>)
         | [<chain>, <chain>, ...]
         | <SQL function call> | (lambda <x>: <expression over x>)
         | <dictionary name>
```

- **Template stage.** `split_text(case := 'lower')` is the [`split_text`](../../functions/search/tokenizers/split_text.md) template with its options as arguments. Named arguments use DuckDB's `name := value` and take the option names of the template's page; positional arguments bind to the options in the order `HELP` prints them, which is also the order of the template page's option table, so `generate_ngrams(2, 3)` sets `MIN_GRAM` and `MAX_GRAM` and `split_text_csv(',')` sets `DELIMITER`. A `NULL` argument leaves its option at the default. Template and option names are case-insensitive. A list-valued option takes a list, `remove_stopwords(['the', 'a'])`, a fixed-size array, or the string spelling documented on its template page.
- **Chain.** `a | b | c` runs the stages in order, each one re-analyzing every token the previous one produced, and creates a [`pipeline`](./pipeline/index.md); a single stage creates that template directly. Parentheses group, and `(a | b) | c` is the same three-step pipeline as `a | b | c`.
- **Wrappers.** [`generate_shingles`](../../functions/search/tokenizers/generate_shingles.md) and [`generate_wildcard_ngrams`](../../functions/search/tokenizers/generate_wildcard_ngrams.md) analyze the tokens of a nested analyzer, so they take a chain as their first argument: `generate_shingles(split_text_csv(' ') | normalize_tokens(case := 'lower'), 2, 3)`.
- **Union.** A list of chains runs all of them over the same input and merges the tokens, which is the [`union`](./union.md) template: `[keyword(), generate_ngrams(2, 2)]`.
- **SQL stages.** A call at a stage position takes the value as its first argument, so `lower()` in front of a tokenizer preprocesses the value, `upper()` behind one rewrites each token, and a list-returning function such as `string_split(',')` fans a value out into tokens. For anything a call cannot express — an operator, a cast, a `CASE`, or a value that is not the first argument — name the value with a lambda: `split_text_csv(',') | (lambda x: upper(trim(x)))`. The parentheses are needed whenever a stage follows the lambda, since the body would otherwise swallow the rest of the chain. A call whose name is neither a template nor a built-in function fails with `unknown stage "<name>"`, which suggests the nearest template name when there is one.
- **Existing dictionaries.** A bare dictionary name, optionally schema-qualified, is a stage that runs that dictionary's analyzer, so `english_dict | remove_stopwords(['run'])` builds on a stored dictionary and `english_dict` alone duplicates one. The stage copies the dictionary's configuration as it stands when the new dictionary is created, so dropping or altering the source afterwards does not affect the copy; the source's feature flags are not copied. A dictionary is a stored analyzer rather than a function, so its name is callable nowhere: an expression names it as a stage, and [`ts_lexize`](../../functions/search/full-text.md#ts_lexize) runs it over a value.
- **Features.** `WITH (frequency, position)` carries the [feature flags](#feature-flags) and `help`, and nothing else: every analyzer option belongs in the expression.

A template name always wins over an SQL function of the same name. At a stage position `stem_words(...)` is the template, and a template-named call inside an SQL stage is rejected rather than silently taken as a function; to call such a function, qualify it, as in `main.normalize_tokens()` for a user macro of that name. `|` binds tighter than `||` and the comparison operators, so an expression using them belongs in a lambda: `(lambda x: lower(x) || '-') | split_text_csv('-')`.

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

Names in the `WITH` clause are case-insensitive, and a flag written without a value means `= true`, so `WITH (frequency, position)` is the same as `WITH (frequency = true, position = true)`. Template names and option names in the expression are case-insensitive too, and so are the values of the enum-valued options — `case`, `break`, `mode`, `input_type` and the like. Every analyzer option comes from the template it belongs to and is documented on that template's page.

## Examples

Create a dictionary that lower-cases its input, applies English stemming and stores the term frequencies and positions needed for relevance ranking and phrase search, then attach it to two columns with an inverted index:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_001" />

The dictionary is referenced by name in the index column list (`title english_dict`, `body english_dict`). Once the index exists, full-text queries against those columns are analyzed with the same dictionary, so a search term matches the indexed tokens even when the surface forms differ:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_002" />

Because `english_dict` stems its input, the query term `searching` is reduced to `search` and matches every row whose `body` contains a form of that word. To see exactly how a dictionary tokenizes a string — invaluable when tuning options — pass it to `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/index/example_003" />

## Templates

A stage is a call to a template. Twenty-one templates analyze text, and each one is documented together with its function form under [tokenizer functions](../../functions/search/tokenizers/index.md): the splitters, the token filters, the n-gram and shingle wrappers, the synonym expanders, the geospatial encoders and the fastText models.

Four more templates exist only inside an analyzer expression, because the expression itself is how they are written:

| Template | Written as | Description |
|---|---|---|
| [`pipeline`](./pipeline/index.md) | `a \| b` | Chain analyzers, each re-analyzing the tokens of the one before |
| [`union`](./union.md) | `[a, b]` | Merge the tokens of several analyzers over the same input |
| [`keyword`](./keyword.md) | `keyword()` | Emit the whole input as one verbatim token |
| [`sql`](./sql.md) | `upper()`, `(lambda x: x \|\| '!')` | Emit the result of a scalar expression over the value |

A stage may also be the name of a stored dictionary, which copies that dictionary's analyzer as it stands.

## Feature flags

The following flags control how much information the index records about each token. They are all off by default — enable only what your queries need, since each one increases the size of the index.

| Flag | Default | Description |
|---|---|---|
| `FREQUENCY` | `false` | Store term frequency (needed for relevance scoring) |
| `POSITION` | `false` | Store term positions (needed for phrase queries); requires `FREQUENCY` |
| `NORM` | `false` | Store the field length normalization factor; requires `FREQUENCY` |
| `OFFSET` | `false` | Store the byte offsets of each token in the source value; requires `POSITION` |

Enable `FREQUENCY` when you rank results by relevance and `POSITION` when you run phrase or proximity queries. The dictionary in the example above sets both. The four flags belong to the dictionary, not to a stage: they are written in the `WITH (...)` clause after the expression, and a stored dictionary used as a stage does not carry its own flags into the new dictionary.

Not every template records every flag, and a dictionary that asks for a flag its template does not support is rejected when it is created. The geospatial templates record none of the four, [`generate_wildcard_ngrams`](../../functions/search/tokenizers/generate_wildcard_ngrams.md) accepts only `FREQUENCY` and `POSITION`, [`generate_sparse_ngrams`](../../functions/search/tokenizers/generate_sparse_ngrams.md) only `FREQUENCY` and `NORM`, and [`union`](./union.md), [`sql`](./sql.md) and [`generate_shingles`](../../functions/search/tokenizers/generate_shingles.md) accept every flag except `OFFSET`. Two further limits depend on the configured analyzer rather than on its template: `OFFSET` requires an analyzer that tracks offsets, so a [`pipeline`](./pipeline/index.md) whose stages drop them is rejected, and `NORM` cannot be combined with an analyzer that stores a per-document blob, which is what `generate_shingles` does unless `store_tokens := false`.

## See also

- [CREATE INDEX](../create_index/index.md) — attach a dictionary to a column with an inverted index
- [DROP](../drop/index.md) — remove a text search dictionary

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
