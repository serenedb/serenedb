---
title: "split_by_non_alpha"
split: headings
---

# split_by_non_alpha

The `split_by_non_alpha` template cuts the value into maximal runs of ASCII alphanumeric bytes — `[0-9A-Za-z]` — and emits each run as one token. Every other byte is a separator and is dropped. There is nothing to configure but case conversion: no delimiter list, no pattern, no locale.

That makes it the tokenizer for mixed machine-readable text whose separators are not known in advance — log lines, identifiers, version strings, URLs, serial numbers — where anything that is not a letter or a digit should split. Underscore is a separator here, so `123_abc` yields `123` and `abc`. It is strictly ASCII: every byte from `0x80` up is a separator, so it is not a general-purpose text tokenizer. For Unicode-aware word boundaries use [`segmentation`](./segmentation.md) or [`text`](./text.md); for a known separator use [`delimiter`](./delimiter.md), and for a separator too complex for a fixed string use [`pattern`](./pattern.md).

The same splitting is also available as a standalone function, [`ts_split_by_non_alpha`](../../functions/search/full-text.md#ts_split_by_non_alpha), which needs no dictionary in the catalog.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `CASE` | string | `'none'` | Case conversion applied to each token: `'none'`, `'lower'`, `'upper'` |

`CASE` is the only option this template takes. Its value is matched case-insensitively, so `'Lower'` also works; anything outside the three names fails with `invalid value in "case" parameter`. Any other option — `DELIMITER`, `MINGRAM` — fails with `option "<name>" is not applicable in this context`. The conversion is ASCII-only, which is exhaustive here, because a token holds nothing but ASCII letters and digits by construction.

## Tokenization

The value is scanned once and cut at every byte that is not an ASCII letter or digit. Tokens are the runs between the cuts, in ascending order, non-overlapping, one token per run. Empty runs are never emitted, so leading, trailing and adjacent separators produce no empty tokens, and an empty value — or a value holding no alphanumeric byte at all — produces no tokens. Positions are implicit and consecutive, one per token in emission order, and offsets are the `[start, end)` byte range of the run inside the value. Offsets are recorded and are unaffected by case conversion, so all four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — are accepted for this template, subject to their dependencies: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

The classification is byte-based, not codepoint-based. A letter is `A`–`Z` or `a`–`z` and a digit is `0`–`9`; `_` is a separator, unlike in the word class the other tokenizers use. Every byte from `0x80` up is a separator too, which is what makes the template ASCII-only: a multi-byte UTF-8 letter splits the word around it, so `Straße` yields `Stra` and `e`, and text in a script that has no ASCII letters produces no tokens at all. Bytes that are not valid UTF-8 are not an error either — they are simply separators.

Nothing beyond the split and the optional case conversion happens: accents are not stripped, no Unicode normalization is applied, and there is no stemming and no stop-word removal.

| Input | Options | Tokens |
|---|---|---|
| `Hello, World! 123abc` | defaults | `{Hello,World,123abc}` |
| `Hello, World! 123_abc` | defaults | `{Hello,World,123,abc}` |
| `The Quick-Brown FOX 2024` | `CASE = 'lower'` | `{the,quick,brown,fox,2024}` |
| `Straße ÜBER Ab1` | `CASE = 'lower'` | `{stra,e,ber,ab1}` |

The second row shows the underscore splitting `123_abc` in two. The fourth row shows both ASCII-only effects at once: `ß` and `Ü` are separators, so `Straße` becomes `stra` and `e` and `ÜBER` loses its first character, while the ASCII bytes around them are lowercased normally.

Offsets always point at the run in the original value, whatever `CASE` does to the token text. In the first row `Hello` covers bytes 0–5 and `123abc` covers bytes 14–20. In the fourth row, with `CASE = 'lower'`, `stra` still covers bytes 0–4, `e` covers 6–7 and `ber` covers 10–13 — the two bytes of `ß` and the two bytes of `Ü` are counted even though no token contains them.

## Examples

The template needs nothing but its name, so the shortest useful dictionary is one option long:

```sql
CREATE TEXT SEARCH DICTIONARY alnum_parts (
    template = 'split_by_non_alpha'
);

SELECT ts_lexize('alnum_parts', 'Hello, World! 123_abc');
```

Adding `CASE = 'lower'` makes matching case-insensitive, and the two feature flags below let the index rank results and answer phrase queries:

```sql
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    line VARCHAR
);

CREATE TEXT SEARCH DICTIONARY alnum_lower (
    template = 'split_by_non_alpha',
    case = 'lower',
    frequency = true,
    position = true
);

CREATE INDEX idx_logs ON logs USING inverted (id, line alnum_lower);
```

The template also nests: a [`pipeline`](./pipeline/index.md) step spells its options with a prefix (`STEP1_TEMPLATE = 'split_by_non_alpha'`, `STEP1_CASE = 'lower'`), a [`union`](./union.md) branch uses `TOKENIZER⟨N⟩_`, and [`copy_from`](./copy-from.md) inherits `CASE` from the source dictionary and lets it be overridden.

## See also

- [delimiter](./delimiter.md) / [multi_delimiter](./multi-delimiter.md) — split on one or several known separators instead of on every non-alphanumeric byte
- [pattern](./pattern.md) — split on, or extract with, an RE2 regular expression
- [segmentation](./segmentation.md) — Unicode word-boundary splitting, for text that is not ASCII
- [text](./text.md) — full text analysis with a locale, stemming and stop words
- [ts_split_by_non_alpha](../../functions/search/full-text.md#ts_split_by_non_alpha) — the same splitting as a scalar function
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
