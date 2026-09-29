---
title: "strip_html"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# strip_html

The `strip_html` template removes HTML markup from the value, decodes character references such as `&amp;` and emits the text between the markup as tokens. It does not split words: each token is a run of text between two pieces of markup, trimmed of surrounding whitespace. The template therefore belongs at the front of a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) whose next stage — usually [`split_text`](./split_text.md) — cuts the runs into words.

It is a best-effort extractor for indexing web pages, e-mail bodies and other HTML-like text, not a validating parser. It never fails on malformed markup: an unterminated tag runs to the end of the value, and anything that cannot open markup or form a character reference is kept as text.

**As a function:** `strip_html(value, join_inline_tags := false)` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/strip_html/function_form" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `JOIN_INLINE_TAGS` | boolean | `false` | Keep a word whole across inline tags such as `<b>`, `<i>` and `<span>` instead of splitting the text at them |

Any other option fails with `strip_html(): unknown option "<name>"`.

## Tokenization

The value is scanned once, left to right, with these rules for markup:

- A `<` opens markup when a letter, `/`, `!` or `?` follows it, as in the HTML tokenizer. Any other `<` is text, so `a < b` survives.
- A tag runs to the next `>` outside a quoted attribute value. A value quoted with `"` or `'` right after an `=` may hold a `>`, so `<a title="1 > 2">` ends at its last `>`; a quote anywhere else is not special. A tag with no `>`, or with a quoted value that is never closed, swallows the rest of the value.
- A comment, `<!--` to `-->`, is skipped. The empty forms `<!-->` and `<!--->` end at their own `>`.
- `<script>` and `<style>` are dropped together with their content, up to the matching `</script>` or `</style>`. Tag names match case-insensitively, the closing tag may carry whitespace before its `>`, and a CDATA section inside the content is skipped whole. A namespaced name such as `<script:x>` is an ordinary tag, and a self-closing `<script/>` has no content.
- The content of `<![CDATA[` … `]]>` is text and becomes its own token verbatim: character references inside it are not decoded.

Each run of text between markup becomes one token once ASCII whitespace is trimmed from both ends; runs that are empty or all whitespace produce nothing. By default markup always separates, so a word broken by an inline tag, `Hel<b>lo</b>`, comes out as the two tokens `Hel` and `lo`.

Text that passes through unchanged is emitted as slices of the value, so the offsets of its tokens point into the original markup. All four [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags) are accepted, `OFFSET` included, and a pipeline that starts with `strip_html` keeps highlighting exact: the offsets of the words that a later stage cuts out of a run are carried back to the original value.

### Character references

References are decoded to UTF-8 the way a browser decodes them in text, following the HTML standard:

- **Named references:** all 2231 names the HTML standard defines, such as `&amp;`, `&eacute;`, `&check;` and `&NotEqualTilde;`, which stands for two code points. Names are case-sensitive, so `&Eacute;` is `É`.
- **Without the `;`:** the 106 legacy names such as `&amp`, `&copy` and `&nbsp` are also decoded, taking the longest legacy name that fits, so `&notit;` reads as `¬it;`.
- **Numeric references:** decimal (`&#233;`) or hexadecimal (`&#xE9;`), with or without the closing `;`. `&#0;`, surrogates and values past `U+10FFFF` become `U+FFFD`, and `&#128;` to `&#159;` are read as Windows-1252, so `&#150;` is an en dash.

Anything else after an `&`, such as `&unknown;` or a lone `&`, stays as written.

A reference that decodes to a space — `&nbsp;`, `&ensp;`, `&emsp;`, `&thinsp;`, `&NewLine;`, or a numeric reference to ASCII whitespace or another Unicode space — separates the text like whitespace. Every other reference is decoded in place, and the word that holds it, delimited by whitespace, becomes a token of its own: `Fish &amp; chips` gives `{Fish,&,chips}`. That token's text is the decoded word, and its offsets span the word in the original value, references included, so `café` from `caf&eacute;` covers all 11 bytes. The text around the word stays a slice with exact offsets. When a later stage cuts such a word apart, as `split_text` does with `tom&amp;jerry`, a piece that starts or ends inside the word takes its offsets from the decoded text, so offsets are exact at the edges of the word and approximate inside it.

### Inline tags

With `JOIN_INLINE_TAGS = true` the tags of the inline elements `a`, `abbr`, `acronym`, `b`, `bdi`, `bdo`, `big`, `cite`, `code`, `data`, `del`, `dfn`, `em`, `font`, `i`, `ins`, `kbd`, `mark`, `q`, `s`, `samp`, `small`, `span`, `strike`, `strong`, `sub`, `sup`, `time`, `tt`, `u`, `var` and `wbr` no longer separate text. They are dropped and the text on both sides runs on, so `Hel<b>lo</b>` gives the single token `Hello`. Whitespace still separates, so `<b>Bold</b> text` gives `Bold` and `text` as before. A word joined across tags is one token whose offsets run from its first character to its last, tags included: `Hello` covers bytes 0 to 8 of `Hel<b>lo</b>`. Every other tag — `p`, `div`, `br`, `td`, `li` and the rest — and every comment, CDATA section, `script` and `style` element still separates.

| Input | Options | Tokens |
|---|---|---|
| `<p>Hello <b>World</b></p>` | defaults | `{Hello,World}` |
| `<style>p {}</style><p>Text</p><script>x()</script>` | defaults | `{Text}` |
| `a < b and c > d` | defaults | `{"a < b and c > d"}` |
| `<![CDATA[raw <data>]]>` | defaults | `{"raw <data>"}` |
| `<a title="1 > 2">link</a>` | defaults | `{link}` |
| `Fish &amp; chips` | defaults | `{Fish,&,chips}` |
| `caf&eacute;&nbsp;cr&egrave;me` | defaults | `{café,crème}` |
| `Hel<b>lo</b> world` | defaults | `{Hel,lo,world}` |
| `Hel<b>lo</b> world` | `JOIN_INLINE_TAGS = true` | `{Hello,world}` |

## Examples

A dictionary that strips the markup and then splits and lowercases the words:

<SqlLogicTest id="sql/functions/search/tokenizers/strip_html/example_001" />

Character references are decoded before the words are cut, and with `join_inline_tags := true` a word broken by an inline tag stays whole:

<SqlLogicTest id="sql/functions/search/tokenizers/strip_html/join_inline_tags" />

## See also

- [`split_text`](./split_text.md) — the word splitter that usually follows `strip_html`
- [pipeline](../../../statements/create_text_search_dictionary/pipeline/index.md) — chain `strip_html` with other stages
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
