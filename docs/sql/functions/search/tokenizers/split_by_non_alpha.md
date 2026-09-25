---
title: "split_by_non_alpha"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# split_by_non_alpha

The `split_by_non_alpha` template cuts the value into maximal runs of word characters and emits each run as one token. Every other character is a separator and is dropped. There is nothing to configure but case conversion and which characters count as word characters: no delimiter list, no pattern, no locale.

That makes it the tokenizer for mixed machine-readable text whose separators are not known in advance — log lines, identifiers, version strings, URLs, serial numbers — where anything that is not a letter or a digit should split. Underscore is a separator here, so `123_abc` yields `123` and `abc`. By default a word character is an ASCII letter or digit — `[0-9A-Za-z]` — and every byte from `0x80` up is a separator, so the default is not a general-purpose text tokenizer. `BREAK` widens the word characters: `'ascii_bytes'` keeps every non-ASCII byte inside tokens, the behaviour of ClickHouse's `splitByNonAlpha`; `'alnum'` accepts letters and digits in any script, like tantivy's default tokenizer; `'letters'` accepts letters in any script and splits on digits, like Lucene's `LetterTokenizer`; `'whitespace'` splits only at Unicode whitespace, like Lucene's `UnicodeWhitespaceTokenizer`. For Unicode word boundaries, which keep `don't` and `3.14` whole, use [`split_text`](./split_text.md); for a known separator use [`split_text_csv`](./split_text_csv.md), and for a separator too complex for a fixed string use [`split_by_pattern`](./split_by_pattern.md).

**As a function:** `split_by_non_alpha(value, case := 'none', break := 'ascii')` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `CASE` | string | `'none'` | Case conversion applied to each token: `'none'`, `'lower'`, `'upper'` |
| `BREAK` | string | `'ascii'` | Which characters form tokens: `'ascii'`, `'ascii_bytes'`, `'alnum'`, `'letters'`, or `'whitespace'` for every character that is not whitespace |

`CASE` and `BREAK` are matched case-insensitively, so `'Lower'` and `'ALNUM'` also work; anything outside the listed values fails with `invalid value in "case" parameter` or `invalid value in "break" parameter`. Any other option — `DELIMITER`, `MIN_GRAM` — fails with `split_by_non_alpha(): unknown option "<name>"`. With `BREAK = 'ascii'` a token holds nothing but ASCII letters and digits, so the conversion is plain ASCII. In the other modes a token that holds non-ASCII characters is converted with the simple per-codepoint mapping of [`split_text`](./split_text.md), so `ÜBER` lowercases to `über`; bytes that are not valid UTF-8 pass through unchanged. For locale-aware case conversion, leave `CASE` at `'none'` and chain [`normalize_tokens`](./normalize_tokens.md) after the template.

## Tokenization

The value is scanned once and cut at every character that is not a word character. Tokens are the runs between the cuts, in ascending order, non-overlapping, one token per run. Empty runs are never emitted, so leading, trailing and adjacent separators produce no empty tokens, and an empty value — or a value holding no word character at all — produces no tokens. Positions are implicit and consecutive, one per token in emission order, and offsets are the `[start, end)` byte range of the run inside the value. Offsets are recorded and are unaffected by case conversion, so all four [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted for this template, subject to their dependencies: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

Outside `whitespace` mode no rule joins across punctuation: `don't` gives `don` and `t`, `3.14` gives `3` and `14`, and `_` separates, unlike in the word class the other tokenizers use. What a word character is depends on `BREAK`:

- `ascii`, the default: `A`–`Z`, `a`–`z` and `0`–`9`. The classification is byte-based, so every byte from `0x80` up separates: a multi-byte UTF-8 letter splits the word around it, so `Straße` yields `Stra` and `e`, and text in a script that has no ASCII letters produces no tokens at all. Bytes that are not valid UTF-8 are not an error either — they are simply separators.
- `ascii_bytes`: the same bytes plus every byte from `0x80` up. Every UTF-8 sequence then stays whole, so `Straße`, `москва` and `北京123` are single tokens, and so does every non-ASCII punctuation mark, symbol or space: `foo—bar` with an em dash and `a，b` with a fullwidth comma are one token each, and a no-break space does not split either. Invalid UTF-8 bytes are part of the token they sit in.
- `alnum`: a letter, a combining mark or a digit in any script — Unicode categories `L`, `M` and `N`. `Grüße`, `北京123` and a Hindi word stay whole, while `—`, `«`, a no-break space or an emoji separate. Bytes that are not valid UTF-8 separate.
- `letters`: as `alnum` without digits, which separate: `abc123` gives `abc`, and `北京123` gives `北京`.
- `whitespace`: every character except the Unicode `White_Space` characters — space, tab, LF, VT, FF, CR, U+0085, the no-break space, U+1680, U+2000–U+200A, U+2028, U+2029, U+202F, U+205F and U+3000. Punctuation stays inside tokens, so `snake_case`, `kebab-case`, `x=1;y` and `Grüße,` are single tokens. Zero-width characters such as U+200B are not whitespace and stay inside tokens too, and bytes that are not valid UTF-8 are part of the token they sit in.

Unlike [`split_text`](./split_text.md), a run of Han characters stays one token in every mode except `ascii`. ASCII bytes are classified with SIMD; in `alnum` and `letters` only the bytes from `0x80` up are decoded, and in `whitespace` only the lead bytes of the multi-byte whitespace characters are checked. Nothing beyond the split and the optional case conversion happens: accents are not stripped, no Unicode normalization is applied, and there is no stemming and no stop-word removal.

| Input | Options | Tokens |
|---|---|---|
| `Hello, World! 123abc` | defaults | `{Hello,World,123abc}` |
| `Hello, World! 123_abc` | defaults | `{Hello,World,123,abc}` |
| `The Quick-Brown FOX 2024` | `CASE = 'lower'` | `{the,quick,brown,fox,2024}` |
| `Straße ÜBER Ab1` | `CASE = 'lower'` | `{stra,e,ber,ab1}` |
| `Straße ÜBER Ab1` | `CASE = 'lower'`, `BREAK = 'ascii_bytes'` | `{straße,über,ab1}` |
| `Grüße 北京123 foo—bar` | `BREAK = 'alnum'` | `{Grüße,北京123,foo,bar}` |
| `Grüße 北京123 foo—bar` | `BREAK = 'letters'` | `{Grüße,北京,foo,bar}` |
| `snake_case kebab-case  x=1;y` | `BREAK = 'whitespace'` | `{snake_case,kebab-case,x=1;y}` |

The second row shows the underscore splitting `123_abc` in two. The fourth row shows the ASCII-only split: `ß` and `Ü` are separators, so `Straße` becomes `stra` and `e` and `ÜBER` loses its first character, while the ASCII bytes around them are lowercased normally. The fifth row keeps the words whole and lowercases `Ü` too. The next two rows keep the words in any script but split at the em dash, and `letters` drops the digits. The last row splits only at the spaces, so the punctuation stays inside the tokens.

Offsets always point at the run in the original value, whatever `CASE` does to the token text. In the first row `Hello` covers bytes 0–5 and `123abc` covers bytes 14–20. In the fourth row, with `CASE = 'lower'`, `stra` still covers bytes 0–4, `e` covers 6–7 and `ber` covers 10–13 — the two bytes of `ß` and the two bytes of `Ü` are counted even though no token contains them.

## Examples

The template needs nothing but its name, so the shortest useful dictionary is one option long:

```sql
CREATE TEXT SEARCH DICTIONARY alnum_parts AS
    split_by_non_alpha();

SELECT ts_lexize('alnum_parts', 'Hello, World! 123_abc');
```

Adding `CASE = 'lower'` makes matching case-insensitive, and the two feature flags below let the index rank results and answer phrase queries:

```sql
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    line VARCHAR
);

CREATE TEXT SEARCH DICTIONARY alnum_lower AS
    split_by_non_alpha(case := 'lower')
    WITH (frequency, position);

CREATE INDEX idx_logs ON logs USING inverted (id, line alnum_lower);
```

The template also composes: it can be a stage of a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) (`split_by_non_alpha(case := 'lower') | stem_words('en_US.UTF-8')`) or a branch of a [`union`](../../../statements/create_text_search_dictionary/union.md).

A token is a maximal run of `[A-Za-z0-9]`; punctuation, whitespace, underscores and every non-ASCII byte separate, and `case := 'lower'` folds the ASCII letters. It is the dictionary-free equivalent of `regexp_split_to_array(text, '[^A-Za-z0-9]+')` without the regex engine:

<SqlLogicTest id="sql/functions/search/tokenizers/index/split_by_non_alpha" />

With `break := 'ascii_bytes'` only ASCII punctuation and whitespace separate, so words in any script survive whole, and so do the digits glued to them:

<SqlLogicTest id="sql/functions/search/tokenizers/split_by_non_alpha/ascii_bytes" />

The five modes on the same text:

<SqlLogicTest id="sql/functions/search/tokenizers/split_by_non_alpha/break_modes" />

## See also

- [`split_text_csv`](./split_text_csv.md) / [`split_by_delimiters`](./split_by_delimiters.md) — split on one or several known separators instead of on every non-alphanumeric byte
- [`split_by_pattern`](./split_by_pattern.md) — split on, or extract with, an RE2 regular expression
- [`split_text`](./split_text.md) — Unicode word-boundary splitting, for text that is not ASCII
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
