---
title: "sql"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# sql

The `sql` template derives the tokens of a value by evaluating a DuckDB scalar expression over it. The expression reads the value through the placeholder `$1` and returns either one token or a list of tokens, which turns the built-in SQL functions into an analyzer: `lower`, `trim`, `regexp_split_to_array`, `string_split`, `replace`, a `CASE`, a cast. Use it for a transformation no other template offers — a split rule a single regular expression cannot express, a token derived from a structured value, or normalization with the same function the rest of the schema uses.

The expression is written inline: any expression at a stage position that reads `$1` becomes a `sql` stage, so `AS regexp_split_to_array(lower($1), '\W+')` is this template on its own and `string_split(lower($1), ' ') | remove_stopwords(['the'])` puts it in front of a filter. `sql(<expression>)` marks the stage explicitly, and inside it no name is read as a template; `sql('<text>')` takes the expression as a string.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `EXPRESSION` | string | **required** | DuckDB scalar expression over the placeholder `$1` (`VARCHAR`). It returns `VARCHAR` or `BLOB` — one token per value — or a list of them, which is one token per element. Built-in functions only, no subqueries, no other parameters, no volatile functions |

`$1` is the only placeholder and stands for the whole value; it may appear any number of times. Nothing else is in scope, so a column reference fails, and an expression without `$1` is not a `sql` stage at all.

`CREATE TEXT SEARCH DICTIONARY` parses and binds the expression once to validate it, so a malformed or disallowed expression is rejected by the statement that creates the dictionary:

- `sql(): required option "expression" not given` — the option is missing.
- `sql: expected exactly one expression` — the value holds more than one comma-separated expression.
- `sql: <DuckDB parser message>` — the value does not parse as an expression.
- `sql: subqueries are not allowed` and `sql: parameters are not allowed` — a `SELECT` inside the expression, or a placeholder other than `$1`, such as `$2` or `$name`.
- `sql: function "<name>": only built-in functions are allowed` — the function is qualified with a catalog other than `system` or a schema other than `main`, or its name is not a scalar function, macro or aggregate of the system catalog. An unqualified name is resolved as `system.main.<name>`, which is why a user-defined macro is rejected whether it is qualified or not.
- `sql: text-search function "<name>" is not allowed in a sql tokenizer expression (it would recurse into the tokenizer)` — any function whose name starts with `ts_`, anywhere in the expression, since calling one would re-enter the tokenizer.
- `sql: volatile expressions are not allowed` — `random()::VARCHAR` and the like. The same value has to produce the same tokens every time.
- `sql: expression must return VARCHAR, BLOB, or a list of them, got <type>` — `length($1)` reports `BIGINT`, `[length($1)]` reports `BIGINT[]`.
- `sql: <DuckDB binder message>` — whatever else the binder refuses: an unknown column, a call whose argument types no overload of the function accepts, an aggregate, a window function, a lambda. The wording mentions check constraints, because the expression is bound the way a `CHECK` constraint is.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](./index.md#feature-flags). `OFFSET` is rejected with `Unsupported index features are specified: <mask>`, because an expression result carries no offsets back into the source value; a [`pipeline`](./pipeline/index.md) that contains a `sql` step loses offsets for the same reason.

## Tokenization

The return type decides the token shape. A scalar `VARCHAR` or `BLOB` result is exactly one token per value; a list result — `VARCHAR[]` or `BLOB[]` — is one token per element, emitted in list order.

Token content is verbatim. The template applies no case folding, accent folding, stemming, stopword filtering or trimming, and imposes no length limit, truncation, padding or marker bytes. Whatever bytes the expression returns become the term, so casing and accents are entirely the expression's business — `lower($1)`, `strip_accents($1)`. The original value does not survive on its own, since only the result is emitted, unless the expression keeps it as `list_value($1, lower($1))` does. An empty string the expression produces is emitted as an empty token; the template filters nothing.

Positions are dense: with `POSITION` enabled the tokens of a value are numbered consecutively from `1` in emission order, one per token, with no gaps and no stacked positions, so a phrase query matches the order the expression produced.

| Input | `EXPRESSION` | Tokens |
|---|---|---|
| `Hello, World! FOO bar` | `regexp_split_to_array(lower($1), '\W+')` | `hello`, `world`, `foo`, `bar` |
| `Foo BAR baz` | `string_split(lower($1), ' ')` | `foo`, `bar`, `baz` |
| `  hello  ` | `upper(trim($1))` | `HELLO` |
| `Ab` | `list_value(upper($1), NULL, lower($1))` | `AB`, `ab` |
| `skip` | `nullif($1, 'skip')` | _(none — the value is rejected)_ |
| `ab` | `input::BLOB` | one `BLOB` token holding the two bytes of `ab` |

The expressions above are written as they read; inside the statement each single quote is doubled.

A `NULL` result rejects the value, which then contributes no tokens at all; through `ts_lexize` this surfaces as `error while preparing tokenizer`. A `NULL` element inside a list is dropped on its own and the surrounding elements are still emitted. An empty list accepts the value and produces no tokens. `NULL` rows never reach the expression. Watch for `NULL` constants in null-propagating functions: `replace(input, NULL, 'x')` is `NULL` for every value, so every value is rejected.

A `BLOB`-returning expression types the tokens as `BLOB`, and their bytes are indexed as raw byte terms — the template performs no UTF-8 validation or conversion of its own. `ts_lexize` then returns `BLOB[]`, and the dictionary name has to be a constant: `ts_lexize` refuses a non-constant name for a dictionary that produces `BLOB` tokens.

An error raised while the expression runs — a failed cast, an invalid regular expression argument — is not caught by the template. It propagates out of the statement that was analyzing the value, whether that is `INSERT`, `CREATE INDEX` or `ts_lexize`.

As a step of a [`pipeline`](./pipeline/index.md) the expression runs once per token the previous step produced: that token becomes `$1`, and the result replaces it. A scalar result rewrites the token one-to-one, a list result fans it out.

## Examples

### A list result, one token per element

`regexp_split_to_array` splits the lowercased value on runs of non-word characters, and each element of the list becomes a token:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sql/example_001" />

### A scalar result, one token per value

A scalar expression is a normalizer: the whole value comes out as a single token, trimmed and uppercased.

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sql/example_002" />

### Inside a pipeline

A `sql` first step splits and lowercases, then a [`remove_stopwords`](./stopwords.md) second step filters the tokens it produced:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sql/example_003" />

### Expression form

The same two dictionaries written as expressions, with the SQL inline:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sql/example_004" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sql/example_005" />

## See also

- [pattern](./pattern.md) — split or extract with a regular expression, without a SQL expression
- [keyword](./keyword.md) — keep the whole value as one verbatim token
- [norm](./norm.md) — case and accent normalization as built-in options
- [pipeline](./pipeline/index.md) — chain `sql` with other analyzers
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
