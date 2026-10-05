---
title: "split_text_icu"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# split_text_icu

The `split_text_icu` template segments text with the word or sentence break rules of a locale and emits the surviving segments verbatim. The rules and the segmentation dictionaries are those of ICU 78.3, built into SereneDB. It is the locale-aware counterpart of [`split_text`](./split_text.md): where that template runs one language-agnostic UAX#29 algorithm, `split_text_icu` takes the rules of `LOCALE` and the dictionaries for the languages that do not separate words with spaces — Chinese, Japanese, Thai, Lao, Khmer and Burmese. This, not `split_text`, is the template that splits `中文测试` into `中文` and `测试`.

Nothing is transformed on the way out. There is no case, accent, normalization or stemming step in this template, so every token is a substring of the input value and its bytes are exactly the source bytes. To fold case or normalize on top of the split, make `split_text_icu` the first step of a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) and add a [`normalize_tokens`](./normalize_tokens.md) step after it.

`BREAK` picks between two jobs. The three word modes — `'alpha'` (the default), `'graphic'` and `'all'` — cut the value at every word boundary and differ only in which segments are kept. `'sentence'` switches to the sentence rules and emits whole sentences instead.

All four [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) are supported — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`. The tokenizer records offsets, so `OFFSET` is available in every mode.

**As a function:** `split_text_icu(value, locale, break := 'alpha')` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/split_text_icu/function_form" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | Locale whose break rules segment the value (e.g., `'en_US.UTF-8'`, `'ja'`, `'th'`) |
| `BREAK` | string | `'alpha'` | Unit of segmentation and which segments are kept. Word segments, filtered: `'alpha'` (segments holding a letter or a digit), `'graphic'` (segments holding a non-whitespace, non-control character), `'all'` (every segment, whitespace runs included). Whole sentences, every segment kept: `'sentence'` |

`LOCALE` has no usable default. Omitting it, or passing an empty string, leaves the locale unset and `CREATE` fails with `split_text_icu: locale is required`. A value that is not a valid locale fails with `Invalid locale "<value>" for option "locale"`, see [Locales](#locales). Rules are looked up by shortening the locale step by step, and most locales end at the root rules: only `en_US_POSIX` has word rules of its own (a full stop joins digits, as in `3.14`, but not letters, and a colon never joins letters) and only `el` sentence rules of its own. With the `ss=standard` keyword — `LOCALE = 'en@ss=standard'` — the sentence mode does not break after the abbreviations of the language (`Mr.`, `etc.`, `z.B.`); abbreviation lists exist for `de`, `en`, `es`, `fr`, `it`, `pt` and `ru`.

`BREAK` matches its value case-insensitively, so `'Sentence'` and `'ALPHA'` also work; anything outside the four listed values fails with `invalid value in "break" parameter` and the hint `Token boundary detection mode: all, graphic, alpha, sentence`. These two options are all the template takes, and any other one — `CASE`, `ACCENT`, `MIN_GRAM` — fails with `split_text_icu(): unknown option "<name>"`.

## Tokenization

In the three word modes the word break rules of `LOCALE` cut the value at every word boundary, and the mode decides which of the resulting segments are emitted. `'alpha'` keeps a segment only when the rule that ends it marks a word — letters, numbers, kana or ideographs, which is where the segmentation dictionary applies — and the segment holds at least one letter or digit, so punctuation and whitespace are discarded. `'graphic'` keeps every segment holding at least one character that is neither whitespace nor an ASCII control character, which additionally keeps each punctuation mark as its own token. `'all'` keeps every segment, so a run of consecutive spaces is one token and each punctuation character is a token of its own.

`BREAK = 'sentence'` changes the unit instead of filtering. The sentence break rules of `LOCALE` split the value into UAX#29 sentences, no accept test applies, and each sentence is emitted with leading and trailing bytes up to and including the space character trimmed off; a segment that trims away to nothing is dropped.

The word modes take a shortcut on ASCII input: when `LOCALE` resolves to the root word rules, ASCII-only input is segmented by the built-in UAX#29 scanner that also backs [`split_text`](./split_text.md). A locale with word rules of its own goes through its rules, and so does any input carrying non-ASCII bytes; `BREAK = 'sentence'` always goes through the sentence rules. Both paths implement UAX#29 word boundaries, and only text that is not ASCII can need a segmentation dictionary, so the shortcut does not change which mode you should choose.

Each emitted token gets one index position, in input order, and its offsets are the token's byte range in the value — the trimmed range in sentence mode. Because no transformation runs, a token is always byte-identical to that range, casing and accent marks included. Empty input yields no tokens. A value that is not valid UTF-8 is skipped whole: it produces no tokens and no error, and the other values are unaffected.

The table below shows how each mode tokenizes, with `LOCALE = 'en_US.UTF-8'` throughout:

| Input | `BREAK` | Tokens |
|---|---|---|
| `中文测试 hello` | `alpha` (the default) | `{中文,测试,hello}` |
| `The Quick fox-trot.` | `alpha` | `{The,Quick,fox,trot}` |
| `The Quick fox-trot.` | `graphic` | `{The,Quick,fox,-,trot,.}` |
| `The Quick fox-trot.` | `all` | `{The," ",Quick," ",fox,-,trot,.}` |
| `Hello world. Second sentence!` | `sentence` | `{"Hello world.","Second sentence!"}` |

Because the tokens keep their original case, a query for `quick` does not match the indexed `Quick`: both sides run through the same dictionary, and nothing in it folds case. Add a [`normalize_tokens`](./normalize_tokens.md) step with `CASE = 'lower'` after `split_text_icu` in a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) to match case-insensitively.

## Locales

The templates that take a `LOCALE` — `split_text_icu`, [`normalize_tokens`](./normalize_tokens.md), [`collate_tokens`](./collate_tokens.md) and [`stem_words`](./stem_words.md) — accept a locale identifier of the form `language[_Script][_REGION][_VARIANT][.charset][@key=value;...]`, with `-` accepted in place of `_`: `en`, `en_US`, `en_US.UTF-8`, `sr_Latn_RS`, `de-DE`, `en@ss=standard`. The letter case of the language, script and region does not matter, and the stored identifier is canonical (`EN-us` becomes `en_US`).

`CREATE` checks the identifier and fails with `Invalid locale "<value>" for option "locale"`, naming the reason in the error detail, when the language is not a current ISO 639 code (`definitely_not_a_locale`, `C`), the region is not an ISO 3166 code or a three-digit UN M.49 area (`en_XX`), a variant is not 1 to 8 letters or digits, anything is left over after the identifier, or the value uses a BCP 47 extension (spell `en-u-ss-standard` as the keyword form `en@ss=standard`). Dictionaries created before this check keep the locale they were created with.

## Examples

### Word segmentation with dictionary support

The default `BREAK = 'alpha'` keeps the word segments and drops the space. The Han text has no spaces, so its split comes from the segmentation dictionary:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text_icu/example_001" />

### Whole sentences as tokens

`BREAK = 'sentence'` emits one token per sentence, trimmed of surrounding whitespace and with its terminating punctuation intact:

<SqlLogicTest id="sql/functions/search/tokenizers/split_text_icu/example_002" />

## See also

- [`split_text`](./split_text.md) — the same word and sentence modes with no locale and no dictionaries
- [`normalize_tokens`](./normalize_tokens.md) — normalize a value; chain it after `split_text_icu` in a pipeline to fold case
- [`split_text_csv`](./split_text_csv.md) — split space-separated text on a literal character
- [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) — compose `split_text_icu` with the filters that transform its tokens
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
