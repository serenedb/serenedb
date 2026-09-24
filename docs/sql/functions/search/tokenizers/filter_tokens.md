---
title: "filter_tokens"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# filter_tokens

The `filter_tokens` template drops tokens and passes every other token through unchanged. A token survives when its length lies within `MIN_LENGTH`–`MAX_LENGTH` and, if a lambda is given, when the lambda returns `true` for it. Like [`remove_stopwords`](./remove_stopwords.md) it is a filter rather than a tokenizer, so it normally follows a tokenizer as a stage of a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md). Typical uses are capping the length of indexed terms — hashes, base64 blobs and other long runs that no one searches for — dropping one-character tokens, and dropping tokens by any rule an SQL expression can state, such as URLs or numbers.

Lengths count characters (Unicode code points), not bytes: `日本語` is 3 characters long and `straße` is 6.

**As a function:** `filter_tokens(value, min_length := 0, max_length := 0)` or `filter_tokens(tokens, lambda x: <predicate>)`. A call takes either the length options or a lambda, not both; nest two calls to combine them. The lambda form needs a list of tokens as its first argument, such as the result of another tokenizer function. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/filter_tokens/function_form" />

<SqlLogicTest id="sql/functions/search/tokenizers/filter_tokens/function_lambda" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `PREDICATE` | lambda | none | Keep only the tokens for which this lambda returns `true` |
| `MIN_LENGTH` | integer | `0` | Drop tokens shorter than this many characters |
| `MAX_LENGTH` | integer | `0` | Drop tokens longer than this many characters; `0` means no upper limit |

The lambda is written `lambda x: <expression>`: its one parameter is the token, a `VARCHAR`, and the expression must return `BOOLEAN`. `false` and `NULL` both drop the token, as in a `WHERE` clause. It is the first argument, as in `filter_tokens(lambda x: x NOT LIKE 'http%', max_length := 40)`; when the first argument is not a lambda, positional arguments bind to `MIN_LENGTH` and `MAX_LENGTH`, so `filter_tokens(2, 40)` sets the bounds. The expression follows the rules of an [SQL stage](../../../statements/create_text_search_dictionary/sql.md): built-in functions only, no subqueries, no parameters. A lambda with more than one parameter fails with `the lambda takes exactly one parameter`, one that does not use its parameter with `does not use its parameter`, and one that returns another type with `the lambda must return BOOLEAN`.

Both length bounds are inclusive. A negative value fails with `"min_length" must not be negative` (or `"max_length"`), and a non-zero `MAX_LENGTH` below `MIN_LENGTH` fails with `"max_length" must be >= "min_length"`. With every option at its default every token passes.

## Tokenization

`filter_tokens` tests each token it receives and drops the ones that fail; a surviving token keeps both its value and its offsets, because the filter never rewrites a token. A dropped token leaves no position gap: a position whose tokens are all dropped disappears and the later positions close up, while tokens that shared a position with a dropped one, such as the other n-grams starting at the same character, keep it. On its own the template treats the whole input value as a single token, so the result is that token or nothing. All four [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) are accepted, `OFFSET` included, which an [SQL stage](../../../statements/create_text_search_dictionary/sql.md) that drops tokens by returning `NULL` cannot offer.

Inside a pipeline the filter runs in place over each batch of tokens, in the same pass as the other filters that follow the same tokenizer. The length test comes first and costs about one comparison per token: the character count is only computed when the byte length leaves it ambiguous, since a token of `n` bytes has between `n / 4` and `n` characters. The lambda then runs once per batch as one vectorized SQL evaluation over the tokens that are still in it.

| Input | Pipeline | Tokens |
|---|---|---|
| `A quick brown fox jumps over the extraordinarily lazy dog` | `split_text(case := 'lower')` → `filter_tokens(min_length := 2, max_length := 8)` | `{quick,brown,fox,jumps,over,the,lazy,dog}` |
| `see https://example.com for details` | `split_text_csv(' ')` → `filter_tokens(lambda x: x NOT LIKE 'http%', max_length := 20)` | `{see,for,details}` |

## Examples

Length bounds:

<SqlLogicTest id="sql/functions/search/tokenizers/filter_tokens/example_001" />

A lambda and a length bound together:

<SqlLogicTest id="sql/functions/search/tokenizers/filter_tokens/example_002" />

## See also

- [`remove_stopwords`](./remove_stopwords.md) — drop the tokens of a word list
- [pipeline](../../../statements/create_text_search_dictionary/pipeline/index.md) — chain a tokenizer before `filter_tokens`
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
