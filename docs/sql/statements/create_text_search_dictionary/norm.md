---
title: "norm"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# norm

The `norm` template normalizes the whole input and returns it as a single token without splitting it into words. Normalization to the Unicode form named by `FORM` always happens; on top of it `CASE` folds case and `ACCENT = false` strips accent marks; the defaults do neither. Because the entire value becomes one token, it behaves like a normalized keyword: two strings match only if they are equal after normalization.

Use it for exact-match or keyword columns — tags, codes, names, enum-like values — that should still compare case-insensitively or accent-insensitively, rather than for free-text search. For per-word tokenization with comparable case and accent options, use [`text`](./text.md).

The template name is unrelated to the `NORM` [feature flag](./index.md#feature-flags), which stores per-document length factors. A `norm` dictionary accepts all four flags — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | `''` | ICU locale for case conversion; omit for locale-independent simple case |
| `CASE` | string | `'none'` | Case conversion: `'none'`, `'lower'`, `'upper'` |
| `ACCENT` | boolean | `true` | Preserve accent marks (`false` folds them away) |
| `FORM` | string | `'nfc'` | Unicode normalization form: `'nfc'`, `'nfkc'` |

`LOCALE` is optional and affects nothing but case conversion — the normalization form and accent folding are locale-independent. A value ICU cannot parse is rejected with `Invalid locale "<value>" for option "locale"`. `CASE` and `FORM` accept their values case-insensitively, so `'Lower'` and `'NFKC'` also work.

## Tokenization

`norm` always emits exactly one token per value: the input normalized per the options. Spaces and punctuation are kept verbatim — the value is never split — and nothing is added: no marker, no prefix, no suffix, and no copy of the original text. The token type is `VARCHAR`, so `ts_lexize` returns a one-element array, and the token's offsets always cover the whole value, from `0` to the value's length in bytes. An empty value yields one empty token; a `NULL` value yields no token.

Normalization to `FORM` always applies; `CASE` conversion and, when `ACCENT = false`, accent folding run on top of it. Accent folding removes Unicode nonspacing marks, so `é` becomes `e` and Cyrillic `Ё` becomes `Е`, while letters whose diacritic is not a separate combining mark — `ø`, `ł`, `đ` — pass through unchanged. Normalization itself is unconditional: even with every default (`CASE = 'none'`, `ACCENT = true`, `FORM = 'nfc'`) a decomposed input is recomposed, so `Cafe` followed by U+0301 indexes as `Café`. The table below shows how the same input transforms under different option combinations.

| Input | Options | Output token |
|---|---|---|
| `CAFÉ` | `CASE = 'lower'`, `ACCENT = false` | `cafe` |
| `CAFÉ` | defaults (`CASE = 'none'`, `ACCENT = true`) | `CAFÉ` |
| `café` | `CASE = 'upper'`, `ACCENT = true` | `CAFÉ` |
| `Cafe` + U+0301 | defaults | `Café` |
| `ﬁnancial` | `FORM = 'nfkc'` | `financial` |
| `①` | `FORM = 'nfkc'` | `1` |
| `ＦＵＬＬ` | `FORM = 'nfkc'`, `CASE = 'lower'` | `full` |
| `café 2²` | `FORM = 'nfkc'`, `CASE = 'lower'` | `café 22` |

Because two values collide only when their normalized forms are identical, a `norm` dictionary with `CASE = 'lower'` and `ACCENT = false` makes `CAFÉ`, `Café` and `cafe` all match.

### Compatibility normalization

`FORM = 'nfkc'` adds compatibility decomposition on top of composition, so characters that are only presentational variants collapse to their plain equivalents: the ligature `ﬁ` becomes `fi`, fullwidth letters become ASCII, circled and superscript digits become plain digits, a no-break space becomes an ordinary space and `㍍` becomes `メートル`. `FORM = 'nfc'` leaves all of them alone. An all-ASCII value is unaffected by either form, since ASCII is invariant under both.

### Case conversion and the locale

`CASE = 'none'` does no case work at all, whatever the locale. When case conversion is on, every locale except Turkish/Azerbaijani, Lithuanian and Greek uses simple 1:1 Unicode mappings, which is observable: `straße` with `CASE = 'upper'` becomes `STRAßE`, and `ΟΔΟΣ` with `CASE = 'lower'` becomes `οδοσ` with a non-final sigma. Turkish/Azerbaijani and Lithuanian locales always use ICU's locale-tailored full casing, so with `LOCALE = 'tr_TR'` and `CASE = 'lower'` `ISPARTA` becomes `ısparta` and `İSTANBUL` becomes `istanbul`. A Greek locale uses it for values that contain non-ASCII, so `ΟΔΟΣ` with `LOCALE = 'el'` becomes `οδος`. `İ` (U+0130) splits the same way: the simple mappings and the Turkish/Azerbaijani tailoring both lowercase it to a plain `i`, so `İstanbul` becomes `istanbul`, while a Lithuanian locale — and a Greek locale on non-ASCII input — keeps the canonical dot above, leaving an `i` followed by a combining mark that only `ACCENT = false` removes.

Malformed UTF-8 is not rejected. Values that go through ICU — case conversion on with a Turkish/Azerbaijani or Lithuanian locale, or with a Greek locale and non-ASCII content — have illegal sequences replaced by U+FFFD. Everywhere else, bytes that need no normalization or case work are emitted unchanged.

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/norm/example_001" />

### Uppercase normalization, accents preserved

Folding to upper case while keeping accent marks turns `café` into `CAFÉ`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/norm/example_002" />

## See also

- [text](./text.md) — per-word tokenization, with full ICU case mapping instead of the simple mappings
- [keyword](./keyword.md) — keeps the value as one token without normalizing it
- [collation](./collation.md) — one opaque sort-key token per value, for locale-aware ordering
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
