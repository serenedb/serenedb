---
title: "segmentation"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# segmentation

The `segmentation` template splits text into tokens using the language-agnostic word-boundary algorithm defined by [Unicode Standard Annex #29 (Unicode Text Segmentation)](https://www.unicode.org/reports/tr29/).

It derives boundaries from the Unicode properties of the characters themselves rather than from whitespace or any per-language dictionary, so a single dictionary works across scripts. That makes it the right choice for languages that do not separate words with spaces — such as Chinese, Japanese and Thai — where a plain [`delimiter`](./delimiter.md) split would treat a whole sentence as a single token. For ASCII text where words are already space-separated, a `delimiter` split is simpler and faster.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `CASE` | string | `'none'` | Case conversion applied to each token: `'none'`, `'lower'`, `'upper'` |
| `BREAK` | string | `'alpha'` | Which boundaries produce tokens: `'alpha'` (alphabetic and numeric runs only), `'graphic'` (visible characters including punctuation), `'all'` (a token at every boundary, including whitespace) |

## Tokenization

The `BREAK` mode decides which Unicode word boundaries are kept. `alpha` (the default) emits only the runs of letters and digits and discards punctuation and whitespace entirely. `graphic` additionally keeps each visible punctuation mark as its own token. `all` emits every segment, including the whitespace runs between words. `CASE` is then applied to each emitted token.

The following table shows how the input `The Quick fox-trot.` is tokenized under each `BREAK` mode:

| `BREAK` | Tokens |
|---|---|
| `alpha` | `The`, `Quick`, `fox`, `trot` |
| `graphic` | `The`, `Quick`, `fox`, `-`, `trot`, `.` |
| `all` | `The`, ` `, `Quick`, ` `, `fox`, `-`, `trot`, `.` |

Because boundaries come from Unicode properties, the same dictionary also segments scripts that do not use spaces, while ASCII words are split exactly where you would expect.

This dictionary lowercases each alphabetic/numeric run:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_001" />

### Graphic boundaries

`BREAK = 'graphic'` keeps punctuation as separate tokens:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_003" />

### All boundaries

`BREAK = 'all'` emits a token at every boundary, whitespace included, and here uppercases the result:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_002" />

## See also

- [delimiter](./delimiter.md) — split space-separated text on a literal character
- [text](./text.md) — full linguistic pipeline (case folding, stemming, stopwords)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
