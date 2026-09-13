---
title: "split_by_pattern"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# split_by_pattern

The `split_by_pattern` template tokenizes text with an [RE2](https://github.com/google/re2) regular expression.

It works in two modes selected by the `GROUP` option. In **extract** mode (`GROUP = 0` for the whole match, or `N > 0` for the Nth capture group) every match becomes a token. In **split** mode (`GROUP = -1`, the default) the pattern marks the separators and the text between matches becomes the tokens. This makes it useful both for pulling structured tokens out of free text — identifiers, codes, mentions — and for splitting on separators too complex for a fixed [`split_csv`](./csv.md).

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `PATTERN` | string | **required** | RE2 regular expression used to match (extract mode) or to mark separators (split mode). An empty string fails with `split_by_pattern: empty pattern`, and a regex RE2 cannot compile fails with `split_by_pattern: invalid regex: <RE2 message>` |
| `GROUP` | integer | `-1` | What each match contributes to the token stream: `-1` = split on each match, `0` = the whole match, `N > 0` = the Nth capture group. `N` may not exceed the number of capture groups in `PATTERN`; any other value fails with `split_by_pattern: group <n> out of range, pattern has <k> capturing groups` |

All three errors are raised by `CREATE TEXT SEARCH DICTIONARY` itself, because the statement instantiates the tokenizer to validate it. The regex is compiled with capturing disabled whenever `GROUP` is `0` or below, so for a `GROUP` below `-1` the range error reports `0 capturing groups`.

## Tokenization

Matches are leftmost and non-overlapping: after each match the scan resumes at the end of that match. Tokens are raw byte slices of the value — no case folding, accent folding or Unicode normalization is applied, so whatever the pattern delimits is what is indexed. Positions are implicit and consecutive, one per emitted token in emission order, and offsets are the `[start, end)` byte range of the slice inside the value. Offsets are recorded, so all four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted for this template. An empty value emits no tokens, in either mode.

In **split** mode the pattern describes the separators between tokens, so the tokens are the gaps. In **extract** mode the pattern describes the tokens themselves, so anything not matched is dropped — and with `GROUP = N` only the Nth parenthesized capture group of each match is kept.

In split mode the text between matches becomes the tokens, including the text before the first match and after the last one. Empty gaps are never emitted, so leading, trailing and adjacent separators produce no empty tokens: `,` on `,hello,world,` yields `hello` and `world`, and on `a,,b` it yields `a` and `b`. If the pattern never matches, the whole value comes out as one token; if it consumes the whole value, there are no tokens at all, as `x*` on `xx` shows.

With `GROUP = 0` every whole match becomes a token and the unmatched text is dropped. An empty match emits nothing. The pattern has to describe the whole token: one that matches a single character emits one token per character — `[a-z]` on `ab c` yields `a`, `b`, `c`, and so does the non-greedy `[a-z]+?` on `abc` — so use a greedy `+` such as `[a-z]+` or `\S+` to get runs.

With `GROUP = N` above zero the Nth capture group of each match becomes the token. A match whose group is empty or did not participate contributes nothing, and no remainder is emitted after the last match, so `([a-z]+)([0-9]*)` with `GROUP = 1` on `abc 123 def456ghi` yields `abc`, `def` and `ghi`.

The regex is case-sensitive unless the pattern says otherwise: `[A-Z]+` with `GROUP = 0` on `HelloWORLD` yields `H` and `WORLD`, and `(?i)` folds case. RE2 matches on UTF-8 semantics, so `\p{L}+` with `GROUP = 0` on `café naïve` yields `café` and `naïve`, while offsets and token lengths stay byte-based. Matching runs over the whole value with an advancing start position rather than over the remaining suffix, so anchors and boundary assertions see the true value boundaries: `^x` with `GROUP = 0` on `xxaxx` emits exactly one token, and `$` matches only the real end of the value. A pattern that can match the empty string advances one whole UTF-8 character per empty match, so a token never begins or ends inside a multi-byte character; in split mode that yields the text between the empty matches, so `x*` on `ab` gives `a` and `b`, and `x*` on `é` gives the single token `é`.

The table below shows the same idea from both directions, plus capture-group extraction:

| Mode | `PATTERN` | `GROUP` | Input | Tokens |
|---|---|---|---|---|
| split | `[-_.]` | `-1` | `SereneDB-2024_v1.2` | `SereneDB`, `2024`, `v1`, `2` |
| split | `\s+` | `-1` | `alpha  beta   gamma` | `alpha`, `beta`, `gamma` |
| extract | `[A-Z][A-Za-z0-9]{2,}` | `0` | `The Quick Brown fox jumps over Lazy Dog` | `The`, `Quick`, `Brown`, `Lazy`, `Dog` |
| extract | `([a-zA-Z]+)(\d+)` | `2` | `abc123def456ghi` | `123`, `456` |

### Extract every capitalized word (`GROUP = 0`)

Each whole match becomes a token; the lowercase `fox`, `jumps` and `over` are not matched and so are dropped:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_001" />

### Split on runs of whitespace (`GROUP = -1`)

Here the pattern `\s+` marks the separators and the runs of text between them are emitted:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_002" />

### Split an identifier on several delimiters (`GROUP = -1`)

A character class splits on `-`, `_` or `.` in a single pass — something the one fixed delimiter of `split_csv` cannot do:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_003" />

### Keep only a capture group (`GROUP = 2`)

With `GROUP = 2` each match emits just its second capture group — the trailing digits:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_004" />

## See also

- [delimiter](./csv.md) / [multi_delimiter](./multi-delimiter.md) — split on literal characters
- [text](./text.md) — Unicode word-boundary splitting
- [`split_by_pattern()`](../../functions/search/tokenizers.md#split_by_pattern) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
