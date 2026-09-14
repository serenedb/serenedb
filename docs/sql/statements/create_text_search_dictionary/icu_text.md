---
title: "icu_text"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# icu_text

The `icu_text` template segments text with the ICU break iterator for a locale and emits the surviving segments verbatim. It is the locale-aware counterpart of [`segmentation`](./segmentation.md): where that template runs one language-agnostic UAX#29 algorithm, `icu_text` asks ICU for the rules of `LOCALE`, and ICU brings a segmentation dictionary for the languages that do not separate words with spaces — Chinese, Japanese and Thai. This, not `segmentation`, is the template that splits `中文测试` into `中文` and `测试`.

Nothing is transformed on the way out. There is no case, accent, normalization or stemming step in this template, so every token is a substring of the input value and its bytes are exactly the source bytes. To fold case or normalize on top of the split, make `icu_text` the first step of a [`pipeline`](./pipeline/index.md) and add a [`norm`](./norm.md) step after it.

`BREAK` picks between two jobs. The three word modes — `'alpha'` (the default), `'graphic'` and `'all'` — cut the value at every word boundary and differ only in which segments are kept. `'sentence'` switches to the ICU sentence iterator and emits whole sentences instead.

All four [feature flags](./index.md#feature-flags) are supported — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — as long as their dependencies hold: `OFFSET` requires `POSITION`, and `POSITION` and `NORM` require `FREQUENCY`. The tokenizer records offsets, so `OFFSET` is available in every mode.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | **required** | ICU locale whose break rules segment the value (e.g., `'en_US.UTF-8'`, `'ja'`, `'th'`) |
| `BREAK` | string | `'alpha'` | Unit of segmentation and which segments are kept. Word segments, filtered: `'alpha'` (segments holding a letter or a digit), `'graphic'` (segments holding a non-whitespace, non-control character), `'all'` (every segment, whitespace runs included). Whole sentences, every segment kept: `'sentence'` |

`LOCALE` has no usable default. Omitting it, or passing an empty string, leaves the locale unset and `CREATE` fails with `icu_text: locale is required`. A string ICU cannot parse at all fails earlier with `Invalid locale "<value>" for option "locale"`, and a locale ICU parses but cannot open a break iterator for fails with `icu_text: failed to create a break iterator for locale '<name>': <icu error>`. Missing break data is not such a failure: ICU falls back to the closest available rules — the root rules in the worst case — so the locale is accepted and its boundaries follow that fallback.

`BREAK` matches its value case-insensitively, so `'Sentence'` and `'ALPHA'` also work; anything outside the four listed values fails with `invalid value in "break" parameter` and the hint `Token boundary detection mode: all, graphic, alpha, sentence`. These two options are all the template takes, and any other one — `CASE`, `ACCENT`, `MINGRAM` — fails with `option "<name>" is not applicable in this context`. Inside a [`pipeline`](./pipeline/index.md) they are spelled `STEP⟨N⟩_LOCALE` and `STEP⟨N⟩_BREAK`.

## Tokenization

In the three word modes the ICU word break iterator for `LOCALE` cuts the value at every word boundary, and the mode decides which of the resulting segments are emitted. `'alpha'` keeps a segment only when ICU reports a word rule status for it — letters, numbers, kana or ideographs, which is where the segmentation dictionary applies — and the segment holds at least one letter or digit, so punctuation and whitespace are discarded. `'graphic'` keeps every segment holding at least one character that is neither whitespace nor an ASCII control character, which additionally keeps each punctuation mark as its own token. `'all'` keeps every segment, so a run of consecutive spaces is one token and each punctuation character is a token of its own.

`BREAK = 'sentence'` changes the unit instead of filtering. The ICU sentence break iterator for `LOCALE` splits the value into UAX#29 sentences, no accept test applies, and each sentence is emitted with leading and trailing bytes up to and including the space character trimmed off; a segment that trims away to nothing is dropped.

The word modes take a shortcut on ASCII input: when `LOCALE` resolves to break rules ICU does not tailor, ASCII-only input is segmented by the built-in UAX#29 scanner that also backs [`segmentation`](./segmentation.md) rather than by ICU. A locale with tailored rules goes through ICU, and so does any input carrying non-ASCII bytes; `BREAK = 'sentence'` always goes through ICU. Both paths implement UAX#29 word boundaries, and only text that is not ASCII can need a segmentation dictionary, so the shortcut does not change which mode you should choose.

Each emitted token gets one index position, in input order, and its offsets are the token's byte range in the value — the trimmed range in sentence mode. Because no transformation runs, a token is always byte-identical to that range, casing and accent marks included. Empty input yields no tokens. A value whose UTF-8 cannot be converted — malformed input — is skipped whole: it produces no tokens and no error, and the other values are unaffected.

The table below shows how each mode tokenizes, with `LOCALE = 'en_US.UTF-8'` throughout:

| Input | `BREAK` | Tokens |
|---|---|---|
| `中文测试 hello` | `alpha` (the default) | `{中文,测试,hello}` |
| `The Quick fox-trot.` | `alpha` | `{The,Quick,fox,trot}` |
| `The Quick fox-trot.` | `graphic` | `{The,Quick,fox,-,trot,.}` |
| `The Quick fox-trot.` | `all` | `{The," ",Quick," ",fox,-,trot,.}` |
| `Hello world. Second sentence!` | `sentence` | `{"Hello world.","Second sentence!"}` |

Because the tokens keep their original case, a query for `quick` does not match the indexed `Quick`: both sides run through the same dictionary, and nothing in it folds case. Add a [`norm`](./norm.md) step with `CASE = 'lower'` after `icu_text` in a [`pipeline`](./pipeline/index.md) to match case-insensitively.

## Examples

### Word segmentation with dictionary support

The default `BREAK = 'alpha'` keeps the word segments and drops the space. The Han text has no spaces, so its split comes from ICU's segmentation dictionary:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/icu_text/example_001" />

### Whole sentences as tokens

`BREAK = 'sentence'` emits one token per sentence, trimmed of surrounding whitespace and with its terminating punctuation intact:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/icu_text/example_002" />

## See also

- [`segmentation`](./segmentation.md) — the same word and sentence modes with no locale and no ICU
- [`text`](./text.md) — word splitting plus case folding, accent stripping, stopwords and stemming
- [`norm`](./norm.md) — normalize a value; chain it after `icu_text` in a pipeline to fold case
- [`delimiter`](./delimiter.md) — split space-separated text on a literal character
- [`pipeline`](./pipeline/index.md) — compose `icu_text` with the filters that transform its tokens
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
