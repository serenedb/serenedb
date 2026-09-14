---
title: "shingle"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# shingle

The `shingle` template joins the tokens of a nested analyzer into word n-grams — shingles — so a multi-word sequence becomes a single term. It wraps another tokenizer, takes the base tokens that tokenizer produces, and emits the concatenation of every window of `MINGRAM` to `MAXGRAM` consecutive tokens. With the default `MINGRAM` and `MAXGRAM` of 2, `quick brown fox` yields the bigram terms `quick brown` and `brown fox` alongside the three words themselves, so a two-word sequence is matched by one term lookup instead of by combining two.

Where [`ngram`](./ngram.md) cuts a token into character fragments, `shingle` builds terms above the token level, and the nested tokenizer decides what a token is. `FREQUENCY` and `POSITION` are the interesting feature flags here; `OFFSET` is not supported, because a shingle term spans several stretches of the source value.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MINGRAM` | integer | `2` | Minimum shingle size, in base tokens. Between `2` and `16` |
| `MAXGRAM` | integer | `2` | Maximum shingle size, in base tokens. Between `2` and `16`, and at least `MINGRAM` |
| `OUTPUTUNIGRAMS` | boolean | `true` | Also index each base token on its own, alongside the shingles |
| `OUTPUTUNIGRAMSIFNOSHINGLES` | boolean | `false` | Index the unigrams only when the value produced fewer than `MINGRAM` base tokens |
| `STORETOKENS` | boolean | `true` | Store the value's base token stream in a synthetic column, so a phrase longer than `MAXGRAM` can be verified against it |
| `FREQUENTWORDS` | string | `''` | Comma-separated, individually double-quoted words, as in `'"the","of"'`. When non-empty, only `MINGRAM`-wide shingles stay dense and wider widths are indexed only for windows containing one of these words |
| `FILLERTOKEN` | string | `'_'` | Placeholder written into the stored token stream for every position no token occupies. Never indexed as a term |
| `TOKENIZER_TEMPLATE` | string | **required** | Template of the nested tokenizer that produces the base tokens |
| `TOKENIZER_*` | — | — | Options for the nested tokenizer, each prefixed with `TOKENIZER_` |

Both sizes are validated, not clamped: a value outside 2–16 is rejected with `"mingram" must be between 2 and 16` (likewise for `maxgram`), and `MAXGRAM` below `MINGRAM` with `"maxgram" must be >= "mingram"`. `FREQUENTWORDS` needs the quoted list form — anything else fails with `Invalid format of list of words(should be comma-separated and quoted)` — and it cannot be combined with `STORETOKENS = false`, which is rejected with `"storetokens" = false cannot be combined with "frequentwords"`. `FILLERTOKEN` must not contain the byte `0xFF`, the separator the template joins tokens with. An empty `FILLERTOKEN` falls back to `_`.

`TOKENIZER_TEMPLATE` is required for a new dictionary — omitting it gives `required parameter "template" was not found`, naming the bare option rather than the prefixed spelling — but [`copy_from`](./copy-from.md) inherits the whole nested tokenizer, and every option above, from the source dictionary. Nesting is recursive: a nested tokenizer that itself wraps a child spells that child's options `TOKENIZER_TOKENIZER_*`.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](./index.md#feature-flags), with `POSITION` and `NORM` each requiring `FREQUENCY`. `OFFSET` is rejected at `CREATE TEXT SEARCH DICTIONARY` time with `Unsupported index features are specified: <mask>`. `NORM` additionally conflicts with the stored token stream and fails with `the 'norm' feature cannot be combined with an analyzer that stores a per-document blob`, so a dictionary that wants `NORM` has to set `STORETOKENS = false`; the two share one synthetic column.

## Tokenization

The nested tokenizer runs first and produces the base token stream, with a position per token. For each base token, in stream order, the template emits that token as a unigram — only when `OUTPUTUNIGRAMS` is true — and then the concatenation of the token with its followers, once for every width from `MINGRAM` up to the widest width still available at that token, narrowest first.

Positions are 1-based. A nested tokenizer that reports dense positions — the usual case, a [`pipeline`](./pipeline/index.md) included as long as none of its steps assigns positions of its own — has the tokens it delivers numbered `1`, `2`, `3` … in delivery order. A step that drops tokens, a [stop word](./stopwords.md) filter for instance, therefore leaves no hole behind: the surviving neighbours are adjacent and do form a shingle. A nested tokenizer that assigns its own positions can instead stack several tokens on one position, as [`nearest_neighbors`](./nearest-neighbors.md) does with the neighbours of one source word.

The available width is capped three ways: by `MAXGRAM`, by the end of the value, and by the run of tokens whose positions rise by exactly one, so a window spans neither a repeated nor a skipped position. Windows therefore shrink toward the tail of the value: with `MINGRAM = MAXGRAM = 2` the three words of `quick brown fox` give five terms, because the last word starts no window. Tokens inside a shingle are joined by a single `0xFF` byte, which no option changes. That byte is never valid UTF-8, so a multi-token term is best treated as binary: assert on the shape of the stream, as `array_length(ts_lexize(…), 1)` does, rather than on the bytes of a term.

Writing the separator as `⟨S⟩`, and with a nested tokenizer that splits on spaces:

| Input | Options | Tokens |
|---|---|---|
| `quick brown fox` | `MINGRAM = 2`, `MAXGRAM = 2` | `{quick, quick⟨S⟩brown, brown, brown⟨S⟩fox, fox}` |
| `a b c` | `MINGRAM = 2`, `MAXGRAM = 2`, `OUTPUTUNIGRAMS = false` | `{a⟨S⟩b, b⟨S⟩c}` |
| `a b c d` | `MINGRAM = 2`, `MAXGRAM = 3` | `{a, a⟨S⟩b, a⟨S⟩b⟨S⟩c, b, b⟨S⟩c, b⟨S⟩c⟨S⟩d, c, c⟨S⟩d, d}` |
| `lonely` | `MINGRAM = 2`, `MAXGRAM = 2` | `{lonely}` |
| `solo` | `MINGRAM = 2`, `OUTPUTUNIGRAMS = false` | `{}` |
| `solo` | `MINGRAM = 2`, `OUTPUTUNIGRAMS = false`, `OUTPUTUNIGRAMSIFNOSHINGLES = true` | `{solo}` |
| `the quick brown` | `MINGRAM = 2`, `MAXGRAM = 3`, `FREQUENTWORDS = '"the","of"'` | `{the, the⟨S⟩quick, the⟨S⟩quick⟨S⟩brown, quick, quick⟨S⟩brown, brown}` |
| `quick the brown` | `MINGRAM = 2`, `MAXGRAM = 2`, nested tokenizer drops `the` | `{quick, quick⟨S⟩brown, brown}` |

Every term produced from one window carries the position of that window's first base token, so a unigram and the shingles that start with it all share a position. In the last row the two surviving words arrive at positions 1 and 2, so the bigram is built straight across the word the nested tokenizer dropped. No offsets are produced at all, which is why the `OFFSET` flag is rejected.

A base token that cannot start a window of at least `MINGRAM` tokens — the tail of the value, or a token whose next neighbour is not one position further on — is emitted as a unigram only if `OUTPUTUNIGRAMS` is true, or if `OUTPUTUNIGRAMSIFNOSHINGLES` is true and the whole value produced fewer than `MINGRAM` base tokens. That second condition is per value, not per window, as the two `solo` rows show. With `OUTPUTUNIGRAMS = false` the last `MINGRAM - 1` tokens of a value therefore contribute no term of their own; they are still carried inside the shingles that start earlier.

`FREQUENTWORDS` makes the widths above `MINGRAM` adaptive. Width `MINGRAM` is always emitted, and a wider width only when at least one token of that particular window is byte-identical to a listed word. An intermediate width can therefore be skipped: with `MINGRAM = 2`, `MAXGRAM = 4` and `FREQUENTWORDS = '"the"'`, the token `alpha` of `alpha beta gamma the delta` yields the 2-token and the 4-token window but not the 3-token one, because only the widest of the three reaches `the`. The comparison is against the tokens the nested tokenizer emits, so it is case-sensitive and must be spelled the way that tokenizer spells them — a lowercase list if the base lowercases. Setting `FREQUENTWORDS` also turns `OUTPUTUNIGRAMS` on, overriding an explicit `false`.

With `STORETOKENS` at its default the template additionally stores the value's base token stream, writing `FILLERTOKEN` once for every position between two consecutive tokens that no token occupies. A dense position stream, the usual case and the one a token-dropping pipeline still produces, leaves nothing missing, so no filler is written. The filler lives only in that stored stream and is never a term. `STORETOKENS = false` drops the stream, and is required for the `NORM` feature.

A value whose base stream holds fewer than `MINGRAM` tokens produces no shingles, only the unigrams described above; a value the nested tokenizer produces no tokens for produces no terms; and a value the nested tokenizer fails on contributes nothing. The shingle layer is byte-oriented and copies token bytes verbatim, so case folding, accent handling, Unicode normalization and tolerance of invalid UTF-8 are entirely the nested tokenizer's business.

## Examples

Create a bigram dictionary over a space-delimited base and check the shape of the stream. The `0xFF` separator is not worth printing, so the example counts the terms and looks for one of the unigrams:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/shingle/example_001" />

### Adaptive width with frequent words

With `MINGRAM = 2`, `MAXGRAM = 3` and `FREQUENTWORDS`, a window that contains a listed word escalates to the trigram while an all-rare window keeps only its bigram, so `the quick brown` gives six terms where the all-rare `quick brown fox` gives five:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/shingle/example_002" />

## See also

- [`wildcard`](./wildcard.md) — the other template that wraps a single `TOKENIZER_`-prefixed child
- [`ngram`](./ngram.md) — character n-grams, one level below the token
- [`pipeline`](./pipeline/index.md) — chain analyzers to build the base token stream
- [`delimiter`](./delimiter.md) — a minimal base tokenizer that cuts on one separator
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
