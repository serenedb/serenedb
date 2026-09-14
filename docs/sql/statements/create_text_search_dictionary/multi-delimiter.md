---
title: "multi_delimiter"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# multi_delimiter

The `multi_delimiter` template cuts the input at every occurrence of any delimiter in the `DELIMITERS` list and emits the pieces as tokens. Each entry is a byte string, not a regular expression: a single character and a multi-character string such as `"foo"` both work, and matching is byte-exact. It suits fields that mix separators — for example splitting `key:value; key2:value2` on `:`, `;` and space yields the individual keys and values.

The pieces are emitted verbatim, so chain the template into a [`pipeline`](./pipeline/index.md) if you also need case folding or stemming. Beyond taking several separators, it differs from [`delimiter`](./delimiter.md) in dropping empty tokens and in doing no quote handling, so a `"` is an ordinary byte here.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITERS` | string | **required** | One string holding a comma-separated list of double-quoted delimiters (e.g., `'":", ";", " "'`). An entry may be a single character or a multi-character string. No entry may be a prefix of another |

The value is split on commas first, and each entry must then be wrapped in double quotes; whitespace around the commas is ignored. An unquoted entry fails with `Invalid format of list of words(should be comma-separated and quoted)`. Entries are taken verbatim — there is no escape processing — and empty entries (`""`) are dropped. Omitting the option fails with `required parameter "delimiters" was not found`.

No delimiter may be a prefix of another, so `'"ab", "abc"'` is rejected when the dictionary is created, with `multi_delimited: delimiters must not be prefixes of one another`. A string is a prefix of itself, so this rules out duplicates as well. Delimiters that merely share a suffix, such as `'"bc", "abc"'`, are accepted.

## Tokenization

The template scans the value left to right, cuts it at every occurrence of any delimiter in the list and emits the text between the cuts verbatim — no case folding, normalization or trimming. Tokens are raw bytes of the value, and the value is not validated as UTF-8. Matches are leftmost and non-overlapping: after a cut the scan resumes at the end of the delimiter that matched, so a delimiter that starts inside a longer one that already matched is not a separate cut. Empty pieces are never emitted, so adjacent, leading and trailing delimiters produce no token, and a value made only of delimiters produces no tokens at all. Empty input emits no tokens.

Positions are implicit and consecutive, one per emitted token. Offsets are the `[start, end)` byte range of the token inside the value, and all four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted.

| Input | Delimiters | Tokens |
|---|---|---|
| `key:value; key2:value2` | `:` `;` space | `{key,value,key2,value2}` |
| `2026-06-18 logs/app` | `/` `-` space | `{2026,06,18,logs,app}` |
| `foobarfoobazbarfoobar` | `foo` | `{bar,bazbar,bar}` |
| `..` | `.` | `{}` |

The third row shows a multi-character delimiter, the fourth a value that is nothing but delimiters and so yields no tokens.

An empty list — `DELIMITERS = '""'` or `DELIMITERS = ''` — is accepted and yields a tokenizer with no split points, so the whole value comes out as a single token.

Splitting `key:value; key2:value2` on the colon, semicolon and space separators recovers the individual keys and values in one pass. Preview the split with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/multi-delimiter/example_002" />

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/multi-delimiter/example_001" />

Because the list is split on commas before the quotes are parsed, a comma cannot be used as a delimiter here: `DELIMITERS = '","'` fails with `Invalid format of list of words(should be comma-separated and quoted)`. To split on a comma, use the [`delimiter`](./delimiter.md) template instead.

## See also

- [delimiter](./delimiter.md) — split on a single delimiter, keeping empty tokens
- [pattern](./pattern.md) — split on a regular expression
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
