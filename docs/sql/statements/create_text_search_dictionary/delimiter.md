---
title: "delimiter"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# delimiter

The `delimiter` template splits the input on one delimiter character and emits the pieces as tokens, with no further analysis. It is the simplest tokenizer and suits structured values whose parts are separated by a known character — comma-separated tags, slash-separated paths, dotted identifiers.

For example, with `DELIMITER = ','` the value `red,green,blue` produces the tokens `red`, `green` and `blue`. To split on more than one separator, use [`multi_delimiter`](./multi-delimiter.md); to further process each piece — lower-case it, stem it, drop stop words — chain this template into a [`pipeline`](./pipeline/index.md).

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITER` | string | **required** | Delimiter character |

## Tokenization

The template cuts the input at every occurrence of `DELIMITER` and emits the pieces between the cuts verbatim — no case folding, stemming or trimming. Adjacent or leading delimiters therefore yield empty tokens, since the piece between two cuts is itself empty.

| Input | Delimiter | Tokens |
|---|---|---|
| `red,green,blue` | `,` | `{red,green,blue}` |
| `com.example.app` | `.` | `{com,example,app}` |
| `/usr/local/bin` | `/` | `{"",usr,local,bin}` |

The third row shows the leading `/` producing an empty first token. Preview the split with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/delimiter/example_003" />

Any single character works as the delimiter — here a dot splits a reverse-DNS identifier into its components:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/delimiter/example_004" />

To further process each piece — lower-case it, stem it, drop stop words — chain this template into a [`pipeline`](./pipeline/index.md).

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/delimiter/example_001" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/delimiter/example_002" />

## See also

- [multi_delimiter](./multi-delimiter.md) — split on multiple delimiters
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
