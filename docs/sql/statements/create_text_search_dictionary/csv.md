---
title: "split_csv"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# split_csv

The `split_csv` template cuts the input at every occurrence of one delimiter and emits the pieces as tokens, with no further analysis. The delimiter is a byte string, not a regular expression: a single character, a multi-character string such as `'::'` and a multi-byte UTF-8 character all work, and matching is byte-exact. The template also honours `"` quoting, so a delimiter inside a quoted run does not cut — see [Quoted pieces](#quoted-pieces). It is the simplest tokenizer and suits structured values whose parts are separated by a known separator — comma-separated tags, slash-separated paths, dotted identifiers.

For example, with `DELIMITER = ','` the value `red,green,blue` produces the tokens `red`, `green` and `blue`. To split on more than one separator, use [`split_by_delimiters`](./multi-delimiter.md), which drops empty tokens and does no quote handling; to further process each piece — lower-case it, stem it, drop stop words — chain this template into a [`pipeline`](./pipeline/index.md).

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITER` | string | **required** | Byte string the input is cut at. It may be one character, a multi-character string or a multi-byte UTF-8 character; the empty string switches the template to per-byte splitting |

`DELIMITER` is the only option this template takes. Any value is accepted — the only validation is that the option is present. Omitting it fails with `split_csv(): required option "delimiter" not given`, and any other option — `CASE`, `MINGRAM` — fails with `split_csv(): unknown option "<name>"`.

## Tokenization

The template cuts the input at every occurrence of `DELIMITER` outside a `"`-quoted run and emits the piece between two cuts verbatim — no case folding, accent or Unicode normalization, trimming, stemming or stop-word removal. The one exception is a quoted piece, which loses its quotes as described under [Quoted pieces](#quoted-pieces). Tokens are raw bytes of the value, and the value is not validated as UTF-8. Positions are implicit and consecutive, one per token in emission order, and offsets are the `[start, end)` byte range of the piece inside the value. Offsets are recorded, so all four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted for this template.

| Input | Delimiter | Tokens |
|---|---|---|
| `red,green,blue` | `,` | `{red,green,blue}` |
| `com.example.app` | `.` | `{com,example,app}` |
| `/usr/local/bin` | `/` | `{"",usr,local,bin}` |
| `a::b::c` | `::` | `{a,b,c}` |

The third row shows the leading `/` producing an empty first token. Because the piece between two cuts can itself be empty, adjacent, leading and trailing delimiters each yield an empty token, a trailing delimiter therefore adds a final empty token, and an empty value yields exactly one empty token. `ts_lexize` returns those empty strings as list elements. The fourth row shows a multi-character delimiter: matching is byte-exact, so `::` splits only on the pair, never on a single `:`.

Preview the split with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/csv/example_003" />

Any byte string works as the delimiter — here a dot splits a reverse-DNS identifier into its components:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/csv/example_004" />

### Quoted pieces

Splitting follows RFC4180, without starting a new record on newlines. A delimiter inside a `"`-quoted run does not cut, wherever that run begins: with `DELIMITER = ','` the value `ab"c,d"e,f` yields `ab"c,d"e` and `f`. A piece that starts with `"` and whose last byte is the matching `"` is emitted unquoted, with each doubled `""` collapsed to one `"`. A piece with malformed quoting — no closing quote, an interior `"` that is not doubled, or bytes after the closing quote — is emitted verbatim, quotes included. Unquoting is decided by the first byte, so a piece that does not start with `"` is emitted as is even when it contains one. An unterminated `"` swallows the rest of the value, so the delimiters after it do not cut.

| Input | Delimiter | Tokens |
|---|---|---|
| `abc,"q,r"` | `,` | `abc`, `q,r` |
| `abc,"""def"` | `,` | `abc`, `"def` |
| `abc,"def","ghi` | `,` | `abc`, `def`, `"ghi` |
| `abc,"def",ghi"` | `,` | `abc`, `def`, `ghi"` |

Offsets cover the raw piece, so the quotes are counted even though they are not part of the token text: with `DELIMITER = ','` the value `abc,"def,"` yields `abc` at bytes 0–3 and `def,` at bytes 4–10.

`DELIMITER = '"'` turns this handling off: the quote is then an ordinary delimiter, and the value is cut at every one of them.

### Splitting per byte

`DELIMITER = ''` switches the template to per-byte splitting: every byte becomes its own token, except that a `"`-quoted run is kept together as one unquoted token. With `DELIMITER = ''` the value `abc,"def"` yields `a`, `b`, `c`, `,` and `def`, the last one covering bytes 4–9. An unterminated `"` swallows the rest of the value, and that token keeps its quote.

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/csv/example_001" />

<SqlLogicTest id="sql/statements/create_text_search_dictionary/csv/example_002" />

A comma is a valid delimiter here, unlike in [`split_by_delimiters`](./multi-delimiter.md), whose comma-separated list format cannot express one.

The template also composes: it is the usual first stage of a [`pipeline`](./pipeline/index.md) (`split_csv(',') | normalize_tokens(case := 'lower')`) or a branch of a [`union`](./union.md).

## See also

- [multi_delimiter](./multi-delimiter.md) — split on several delimiters, dropping empty tokens and without quote handling
- [path_hierarchy](./path-hierarchy.md) — split on a delimiter but emit cumulative prefixes
- [`split_csv()`](../../functions/search/tokenizers.md#split_csv) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
