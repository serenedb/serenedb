---
title: "generate_shingles"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# generate_shingles

The `generate_shingles` template joins the tokens of a nested analyzer into word n-grams — shingles — so a multi-word sequence becomes a single term. It wraps another tokenizer, takes the base tokens that tokenizer produces, and emits the concatenation of every window of `MIN_GRAM` to `MAX_GRAM` consecutive tokens. With the default `MIN_GRAM` and `MAX_GRAM` of 2, `quick brown fox` yields the bigram terms `quick brown` and `brown fox` alongside the three words themselves, so a two-word sequence is matched by one term lookup instead of by combining two.

Where [`generate_ngrams`](./generate_ngrams.md) cuts a token into character fragments, `generate_shingles` builds terms above the token level, and the nested tokenizer decides what a token is. `FREQUENCY` and `POSITION` are the interesting feature flags here; `OFFSET` is not supported, because a shingle term spans several stretches of the source value.

In the [expression form](../../../statements/create_text_search_dictionary/index.md#expression-form) the nested analyzer is the first argument and may be a chain, and the remaining arguments are the options below in order: `generate_shingles(split_text_csv(' ') | normalize_tokens(case := 'lower'), 2, 2)` sets `MIN_GRAM` and `MAX_GRAM`.

**As a function:** `generate_shingles(value, min_gram := 2, max_gram := 2, output_unigrams := true, fallback_unigrams := false, store_tokens := true, frequent_words := [], filler_token := '', token_separator := ' ')` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/generate_shingles/function_form" />

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MIN_GRAM` | integer | `2` | Minimum shingle size, in base tokens. Between `2` and `16` |
| `MAX_GRAM` | integer | `2` | Maximum shingle size, in base tokens. Between `2` and `16`, and at least `MIN_GRAM` |
| `OUTPUT_UNIGRAMS` | boolean | `true` | Also index each base token on its own, alongside the shingles |
| `FALLBACK_UNIGRAMS` | boolean | `false` | Index the unigrams only when the value produced fewer than `MIN_GRAM` base tokens |
| `STORE_TOKENS` | boolean | `true` | Store the value's base token stream in a synthetic column, so a phrase longer than `MAX_GRAM` can be verified against it |
| `FREQUENT_WORDS` | string | `''` | A list of words, `['the', 'of']`, or one string of comma-separated, individually double-quoted words, as in `'"the","of"'`. When non-empty, only `MIN_GRAM`-wide shingles stay dense and wider widths are indexed only for windows containing one of these words |
| `FILLER_TOKEN` | string | `'_'` | Placeholder written into the stored token stream for every position no token occupies. Never indexed as a term |
| `TOKEN_SEPARATOR` | string | `' '` | Written between the words of a shingle. An empty value concatenates them |

Both sizes are validated, not clamped: a value outside 2–16 is rejected with `"min_gram" must be between 2 and 16` (likewise for `max_gram`), and `MAX_GRAM` below `MIN_GRAM` with `"max_gram" must be >= "min_gram"`. A `FREQUENT_WORDS` string needs the quoted list form — anything else fails with `Invalid format of list of words(should be comma-separated and quoted)` — and the option cannot be combined with `STORE_TOKENS = false`, which is rejected with `"store_tokens" = false cannot be combined with "frequent_words"`. An empty `FILLER_TOKEN` falls back to `_`, while an empty `TOKEN_SEPARATOR` means what it says: the words of a shingle are run together.

The nested analyzer is the first argument and is required — `generate_shingles(2, 3)` fails with `generate_shingles() requires a nested analyzer as its first argument`. Nesting is recursive: the first argument may itself be a chain or a wrapper.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags), with `POSITION` and `NORM` each requiring `FREQUENCY`. `OFFSET` is rejected at `CREATE TEXT SEARCH DICTIONARY` time with `Unsupported index features are specified: <mask>`. `NORM` additionally conflicts with the stored token stream and fails with `the 'norm' feature cannot be combined with an analyzer that stores a per-document blob`, so a dictionary that wants `NORM` has to set `STORE_TOKENS = false`; the two share one synthetic column.

## Tokenization

The nested tokenizer runs first and produces the base token stream, with a position per token. For each base token, in stream order, the template emits that token as a unigram — only when `OUTPUT_UNIGRAMS` is true — and then the concatenation of the token with its followers, once for every width from `MIN_GRAM` up to the widest width still available at that token, narrowest first.

Positions are 1-based. A nested tokenizer that reports dense positions — the usual case, a [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) included as long as none of its steps assigns positions of its own — has the tokens it delivers numbered `1`, `2`, `3` … in delivery order. A step that drops tokens, a [stop word](./remove_stopwords.md) filter for instance, therefore leaves no hole behind: the surviving neighbours are adjacent and do form a shingle. A nested tokenizer that assigns its own positions can instead stack several tokens on one position, as [`find_nearest_words`](./find_nearest_words.md) does with the neighbours of one source word.

The available width is capped three ways: by `MAX_GRAM`, by the end of the value, and by the run of tokens whose positions rise by exactly one, so a window spans neither a repeated nor a skipped position. Windows therefore shrink toward the tail of the value: with `MIN_GRAM = MAX_GRAM = 2` the three words of `quick brown fox` give five terms, because the last word starts no window. Tokens inside a shingle are joined by `TOKEN_SEPARATOR`, a space unless you say otherwise, so a shingle term reads as the words themselves. Setting it to `''` runs the words together, which is what a language without spaces wants; setting it to a character your tokens cannot contain keeps a shingle term unambiguous, at the cost of readable terms.

The separator is what keeps a shingle distinguishable from a base token that happens to look like one, and nothing splits a term back apart: a shingle is opaque bytes to everything downstream. With the default space, a base token that itself contains a space collides with the shingle of the two words around it, and with `TOKEN_SEPARATOR = ''` the collisions are wider still — `ab` + `c`, `a` + `bc` and the single token `abc` all index as `abc`. The effect is broader recall, never an error, and it is the same trade Lucene makes.

With a nested tokenizer that splits on spaces:

| Input | Options | Tokens |
|---|---|---|
| `quick brown fox` | `MIN_GRAM = 2`, `MAX_GRAM = 2` | `{quick, quick brown, brown, brown fox, fox}` |
| `a b c` | `MIN_GRAM = 2`, `MAX_GRAM = 2`, `OUTPUT_UNIGRAMS = false` | `{a b, b c}` |
| `a b c d` | `MIN_GRAM = 2`, `MAX_GRAM = 3` | `{a, a b, a b c, b, b c, b c d, c, c d, d}` |
| `a b c` | `MIN_GRAM = 2`, `MAX_GRAM = 2`, `TOKEN_SEPARATOR = ''` | `{a, ab, b, bc, c}` |
| `lonely` | `MIN_GRAM = 2`, `MAX_GRAM = 2` | `{lonely}` |
| `solo` | `MIN_GRAM = 2`, `OUTPUT_UNIGRAMS = false` | `{}` |
| `solo` | `MIN_GRAM = 2`, `OUTPUT_UNIGRAMS = false`, `FALLBACK_UNIGRAMS = true` | `{solo}` |
| `the quick brown` | `MIN_GRAM = 2`, `MAX_GRAM = 3`, `FREQUENT_WORDS = '"the","of"'` | `{the, the⟨S⟩quick, the⟨S⟩quick⟨S⟩brown, quick, quick⟨S⟩brown, brown}` |
| `quick the brown` | `MIN_GRAM = 2`, `MAX_GRAM = 2`, nested tokenizer drops `the` | `{quick, quick⟨S⟩brown, brown}` |

Every term produced from one window carries the position of that window's first base token, so a unigram and the shingles that start with it all share a position. In the last row the two surviving words arrive at positions 1 and 2, so the bigram is built straight across the word the nested tokenizer dropped. No offsets are produced at all, which is why the `OFFSET` flag is rejected.

A base token that cannot start a window of at least `MIN_GRAM` tokens — the tail of the value, or a token whose next neighbour is not one position further on — is emitted as a unigram only if `OUTPUT_UNIGRAMS` is true, or if `FALLBACK_UNIGRAMS` is true and the whole value produced fewer than `MIN_GRAM` base tokens. That second condition is per value, not per window, as the two `solo` rows show. With `OUTPUT_UNIGRAMS = false` the last `MIN_GRAM - 1` tokens of a value therefore contribute no term of their own; they are still carried inside the shingles that start earlier.

`FREQUENT_WORDS` makes the widths above `MIN_GRAM` adaptive. Width `MIN_GRAM` is always emitted, and a wider width only when at least one token of that particular window is byte-identical to a listed word. An intermediate width can therefore be skipped: with `MIN_GRAM = 2`, `MAX_GRAM = 4` and `FREQUENT_WORDS = '"the"'`, the token `alpha` of `alpha beta gamma the delta` yields the 2-token and the 4-token window but not the 3-token one, because only the widest of the three reaches `the`. The comparison is against the tokens the nested tokenizer emits, so it is case-sensitive and must be spelled the way that tokenizer spells them — a lowercase list if the base lowercases. Setting `FREQUENT_WORDS` also turns `OUTPUT_UNIGRAMS` on, overriding an explicit `false`.

With `STORE_TOKENS` at its default the template additionally stores the value's base token stream, writing `FILLER_TOKEN` once for every position between two consecutive tokens that no token occupies. A dense position stream, the usual case and the one a token-dropping pipeline still produces, leaves nothing missing, so no filler is written. The filler lives only in that stored stream and is never a term. `STORE_TOKENS = false` drops the stream, and is required for the `NORM` feature.

A value whose base stream holds fewer than `MIN_GRAM` tokens produces no shingles, only the unigrams described above; a value the nested tokenizer produces no tokens for produces no terms; and a value the nested tokenizer fails on contributes nothing. The shingle layer is byte-oriented and copies token bytes verbatim, so case folding, accent handling, Unicode normalization and tolerance of invalid UTF-8 are entirely the nested tokenizer's business.

## Examples

Create a bigram dictionary over a space-delimited base. Every window of two words becomes one term, and the words themselves stay indexed alongside them:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_shingles/example_001" />

### Adaptive width with frequent words

With `MIN_GRAM = 2`, `MAX_GRAM = 3` and `FREQUENT_WORDS`, a window that contains a listed word escalates to the trigram while an all-rare window keeps only its bigram, so `the quick brown` gives six terms where the all-rare `quick brown fox` gives five:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_shingles/example_002" />

### Expression form

The bigram dictionary over a lowercasing chain, written as an expression:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_shingles/example_003" />

## See also

- [`generate_wildcard_ngrams`](./generate_wildcard_ngrams.md) — the other template that wraps a single nested analyzer
- [`generate_ngrams`](./generate_ngrams.md) — character n-grams, one level below the token
- [`pipeline`](../../../statements/create_text_search_dictionary/pipeline/index.md) — chain analyzers to build the base token stream
- [`split_text_csv`](./split_text_csv.md) — a minimal base tokenizer that cuts on one separator
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
