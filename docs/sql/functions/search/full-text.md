---
title: Full-Text Search Functions
sidebar_label: Full-Text
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

The [`@@`](#match-operator) match operator and the [`TSQUERY`](../../data_types/tsquery.md) constructors, operators and [PostgreSQL-compatible parsers](#postgresql-compatible-parsers) used to build full-text queries against an [inverted index](../../indexes/inverted/index.md). Every example below shares the dataset created in [Setup](#setup); see [Full-Text Search](../../indexes/inverted/full-text-search.md) for a task-oriented guide.

## Setup {#setup}

<details>
<summary>The examples on this page share one dataset. Expand to see the schema and sample data.</summary>

<SqlLogicTest id="sql/functions/full_text_search/setup" />

</details>

## Match Operator {#match-operator}

| Function | Description |
| :--- | :--- |
| [`column @@ tsquery`](#column--tsquery) | Match predicate: rows where the indexed `column` satisfies the query. |

#### `column @@ tsquery` {#column--tsquery}

Filters to rows where the indexed `column` satisfies a [`TSQUERY`](../../data_types/tsquery.md).

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | any indexed column | — | A column covered by an [inverted index](../../indexes/inverted/index.md). |
| `tsquery` | `TSQUERY` | — | The query to test against the column. A bare string literal is accepted and tokenized by the column's [dictionary](../../statements/create_text_search_dictionary/index.md). |

**How it works.** `@@` is the single entry point that turns a `TSQUERY` into an inverted-index scan. The whole expression on the right of `@@` is claimed by the index at bind time and evaluated by the index scan, not row by row. The operator is **commutative** — `tsquery @@ column` is identical. Although it is typed `BOOLEAN`, it is only valid in a `WHERE` clause against an inverted-indexed column; using it as a standalone expression (for example in the `SELECT` list) raises an error, so it is not a general-purpose boolean. Likewise the `TSQUERY` constructors below only have meaning *inside* an `@@` match — evaluating one on its own raises an error.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_phrase('fox')` | `1`, `2` | Both bodies contain `fox`. |
| `body @@ 'fox'` | `1`, `2` | A bare string is a valid `TSQUERY`. |
| `body @@ ts_phrase('unicorn')` | *(none)* | No body contains `unicorn`. |

<SqlLogicTest id="sql/functions/full_text_search/column--tsquery" />

## TSQUERY Constructors {#tsquery-constructors}

Each returns a [`TSQUERY`](../../data_types/tsquery.md). A bare string literal is also a valid `TSQUERY` — it is tokenized by the column's [dictionary](../../statements/create_text_search_dictionary/index.md) (multi-token input uses `OR` semantics).

| Function | Description |
| :--- | :--- |
| [`ts_phrase(text[, gap, text, ...])`](#ts_phrase) | Match the tokens of `text` as a phrase, with optional gaps. |
| [`ts_tokenize(text[, dictionary])`](#ts_tokenize) | Tokenize `text` into a query, optionally with a named dictionary. |
| [`ts_like(pattern)`](#ts_like) | Match tokens against a SQL `LIKE` pattern. |
| [`ts_starts_with(prefix)`](#ts_starts_with) | Match tokens beginning with `prefix`. |
| [`ts_regexp(pattern[, syntax])`](#ts_regexp) | Match tokens against a regular expression. |
| [`ts_levenshtein(text[, distance[, transpositions[, prefix]]])`](#ts_levenshtein) | Match tokens within an edit distance of `text`. |
| [`ts_ngram(text[, threshold])`](#ts_ngram) | Match by n-gram similarity. |
| [`ts_between(min, max, min_incl, max_incl)`](#ts_between) | Range match between two bounds. |
| [`ts_lt(value)`](#ts_lt) | Match values less than `value`. |
| [`ts_le(value)`](#ts_le) | Match values less than or equal to `value`. |
| [`ts_gt(value)`](#ts_gt) | Match values greater than `value`. |
| [`ts_ge(value)`](#ts_ge) | Match values greater than or equal to `value`. |
| [`ts_any(list[, min_match])`](#ts_any) | OR over a list; at least `min_match` must match. |
| [`ts_all(list)`](#ts_all) | AND over a list. |
| [`ts_compound(must, must_not, should[, min_should_match])`](#ts_compound) | Boolean query (the [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) analog). |

#### `ts_phrase(text[, gap, text, ...][, slop := N])` {#ts_phrase}

Match a run of tokens in their indexed order, optionally separated by token gaps.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` or `BLOB` | — | A phrase segment. It is tokenized by the column's dictionary; multiple tokens within one segment must be strictly adjacent. |
| `gap` | `INTEGER` or `INTEGER[]` | `0` between successive segments | Number of tokens allowed *between* the two surrounding segments. An integer `N` means exactly `N` tokens between; a two-element array `[min, max]` allows a range. `0` means adjacent. |
| `text, ...` | `VARCHAR`/`BLOB` | — | Further segments, each preceded by its own `gap`. |
| `slop` | `INTEGER` (named) | `0` | Budget of position moves allowed when lining the query up with the document. Must be `>= 0`; incompatible with `[min, max]` interval gaps. |

**How it works.** `ts_phrase` matches positions, so the column's dictionary must have `position` enabled. The tokens of each `text` segment must appear adjacent and in order; the optional `gap` arguments control how far apart consecutive segments may sit. The gap counts the tokens *between* the two segments — `0` is immediate adjacency, `2` means exactly two intervening tokens. A `[min, max]` array accepts any gap in that inclusive range. Without `slop`, order is always preserved: `ts_phrase('a', 0, 'b')` does not match `b a`.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `ts_phrase('quick brown fox')` | `1` | Three adjacent tokens in order; only `id 1` has `brown`. |
| `ts_phrase('quick', 1, 'fox')` | `1`, `2` | Exactly one token (`brown`/`red`) sits between `quick` and `fox`. |
| `ts_phrase('quick', 3, 'over')` | `1`, `2` | `brown/red fox jumps` are the three tokens between `quick` and `over`. |
| `ts_phrase('fox', [0, 2], 'dog')` | *(none)* | `jumps over lazy` are three tokens apart — outside the `0..2` range. |

<SqlLogicTest id="sql/functions/full_text_search/ts_phrase" />

The gap form expresses proximity directly. To require `quick` within three tokens *before* `over`:

<SqlLogicTest id="sql/functions/full_text_search/ts_phrase_gap" />

**Slop.** `slop := N` spends a budget of `N` position moves to line the query up with the document, shared across the whole phrase: `ts_phrase('quick fox', slop := 1)` matches `quick brown fox`, because shifting `fox` one position costs one unit. An intervening token costs 1 per token and a transposed adjacent pair costs 2, so `slop := 2` also matches `fox quick`. `slop := 0` is an exact phrase.

When a `gap` is declared, the budget counts deviation from *that* gap rather than from adjacency: `ts_phrase('quick', 1, 'fox', slop := 1)` accepts `quick fox` and `quick a b fox` — one step either side of the declared single-token gap. Interval gaps already express a range, so `ts_phrase('a', [1, 3], 'b', slop := 2)` is an error.

`(...)::slop(N)` applies the same budget as a modifier to an already-built phrase, including one from [`phraseto_tsquery`](#phraseto_tsquery). Lucene's `"..."~N` reaches it through [`to_tsquery`](#to_tsquery). The forms are mutually exclusive: specifying slop twice on one phrase is an error, and `::slop` on a non-phrase query (or on a `##` part) is rejected.

See [Phrase and Proximity Search](../../../cookbook/search/phrase-and-proximity-search.md#proximity-search-with-slop) for worked examples.

#### `ts_tokenize(text[, dictionary])` {#ts_tokenize}

Analyze `text` into a query using a chosen dictionary, overriding the column's default analysis.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR`/`BLOB` (or a `LIST` of them) | — | The text to tokenize. A list yields a `LIST(TSQUERY)`, one per element, for use inside [`ts_any`](#ts_any) / [`ts_all`](#ts_all). |
| `dictionary` | `VARCHAR` | the `@@` column's dictionary | Name of the [text-search dictionary](../../statements/create_text_search_dictionary/index.md) to analyze with. The special value `'keyword'` bypasses analysis and treats `text` as a single raw token. |

**How it works.** The one-argument form analyzes `text` with the same dictionary as the column it is matched against, so the query and the index agree on casing, stemming and stop-words. Naming a dictionary forces a specific analyzer — useful when you want, say, exact `'keyword'` matching against a column that is otherwise stemmed. Multi-token output is combined with `OR`.

**`::tokenize` cast.** The cast `'text'::tokenize('dictionary')` is exactly equivalent to `ts_tokenize('text', 'dictionary')` and reads naturally inline — for example `WHERE body @@ 'Running'::tokenize('exact')`. The cast **requires** a dictionary name (use `'keyword'` to bypass analysis); there is no no-argument cast form.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_tokenize('quick', 'keyword')` | `1`, `2` | Raw token `quick` matched verbatim. |
| `body @@ ts_tokenize('quick grey')` | `1`, `2`, `3` | Two tokens combined with `OR`. |
| `body @@ ts_tokenize('QUICK', 'keyword')` | *(none)* | `'keyword'` skips lower-casing, so `QUICK` ≠ indexed `quick`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_tokenize" />

#### `ts_like(pattern)` {#ts_like}

Match indexed tokens against a SQL `LIKE` pattern.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `pattern` | `VARCHAR`/`BLOB` | — | A `LIKE` pattern. `%` matches any run of characters (including none), `_` matches exactly one character. The pattern is matched against terms as they are stored in the index. |

**How it works.** `ts_like` is applied to the *indexed* term form, not the raw text, so it is not tokenized further — match it against a column whose dictionary stores whole values (such as a `keyword`-style category column). It is the inverted-index analogue of SQL `LIKE`: a row matches when any of its indexed tokens satisfies the pattern.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `category @@ ts_like('sci%')` | `1`, `2` | `sci-fi` starts with `sci`. |
| `category @@ ts_like('dram_')` | `3` | `drama` is `dram` plus exactly one character. |
| `category @@ ts_like('%edy')` | `4` | `comedy` ends with `edy`. |
| `category @@ ts_like('thriller')` | *(none)* | No category equals `thriller`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_like" />

#### `ts_starts_with(prefix)` {#ts_starts_with}

Match any indexed token beginning with `prefix`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `prefix` | `VARCHAR`/`BLOB` | — | The literal leading substring a term must start with. Matched against the indexed term form, with no further tokenization. |

**How it works.** A row matches when any of its indexed tokens begins with `prefix`. This is the classic autocomplete primitive — `ts_starts_with('app')` matches `apple`, `application`, `app`. It is equivalent to `ts_like(prefix || '%')` but expresses intent more clearly and is the building block behind the `a*` syntax of [`to_tsquery`](#to_tsquery).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_starts_with('turt')` | `3` | `turtle` begins with `turt`. |
| `body @@ ts_starts_with('qu')` | `1`, `2` | `quick` begins with `qu`. |
| `body @@ ts_starts_with('zzz')` | *(none)* | No token starts with `zzz`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_starts_with" />

#### `ts_regexp(pattern[, syntax])` {#ts_regexp}

Match indexed tokens against a regular expression.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `pattern` | `VARCHAR`/`BLOB` | — | The regular expression. It must match a whole indexed term (anchored implicitly, not a substring search). |
| `syntax` | `VARCHAR` | `'perl'` | Dialect: `'perl'` (RE2 / Perl-compatible) or `'posix'` (POSIX ERE). Case-insensitive. |

**How it works.** The pattern is applied to each indexed term and a row matches when any term matches. Because terms are stored in their analyzed form (lower-cased by the dictionary in our setup), write the pattern against that form — `ts_regexp('QUICK')` finds nothing, but the inline flag `ts_regexp('(?i)QUICK')` does (in `'perl'` mode). The `'posix'` dialect is handy for bracket-class patterns such as `gr[ae]y`.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_regexp('qu.*ck')` | `1`, `2` | `quick` matches the Perl pattern. |
| `body @@ ts_regexp('gr[ae]y', 'posix')` | `3` | `grey` matches the POSIX class. |
| `body @@ ts_regexp('(?i)QUICK')` | `1`, `2` | The inline `(?i)` flag makes the match case-insensitive. |
| `body @@ ts_regexp('z.*')` | *(none)* | No term begins with `z`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_regexp" />

Use the `'posix'` dialect for a POSIX ERE bracket expression:

<SqlLogicTest id="sql/functions/full_text_search/ts_regexp_posix" />

#### `ts_levenshtein(text[, distance[, transpositions[, prefix]]])` {#ts_levenshtein}

Fuzzy match: find tokens within a bounded edit distance of `text` — the standard tolerance for typos.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR`/`BLOB` | — | The term to match approximately. When `prefix` is given, this is only the *suffix* that is fuzzy-matched (see below). |
| `distance` | `INTEGER` | auto by length: `0` for ≤ 2 chars, `1` for 3–5, `2` for ≥ 6 | Maximum edit distance (insertions, deletions, substitutions). Allowed range `0`–`4` (capped at `3` when `transpositions` is `true`). |
| `transpositions` | `BOOLEAN` | `true` | When `true`, swapping two adjacent characters counts as **one** edit (Damerau–Levenshtein); when `false`, it counts as two. |
| `prefix` | `VARCHAR` | `''` (empty) | A literal leading substring that must match **exactly**. The term actually matched is `prefix \|\| text`; only the `text` portion spends the edit budget. |

**How it works.** A term matches when its edit distance from the query is at most `distance`. The knobs around that rule:

- **Auto distance.** The one-argument form picks the distance from the query length: `0` for two characters or fewer, `1` for three to five, `2` from six up. Short queries tolerate fewer edits, which keeps them from drifting into unrelated tokens.
- **Transpositions.** On by default, so a single adjacent-character swap costs one edit instead of two: `quikc` reaches `quick` at distance 1, where strict Levenshtein needs distance 2.
- **Prefix.** Anchors an exact leading substring and fuzzy-matches only the rest, which both narrows the candidate set and speeds the scan. `ts_levenshtein('X', 1, true, 'quic')` requires the literal `quic`, then allows one edit on `X`, reaching `quick`.
- **Expansion cap.** [`sdb_levenshtein_max_terms`](../../indexes/inverted/maintenance.md#session-settings) (default `64`) bounds how many dictionary terms the predicate expands to. The terms closest to the query survive; the rest neither match nor score. Set it to `0` to match every term within the edit distance, or narrow the candidate set with `prefix`. The cap applies per index segment, so a wide predicate can match more terms while they sit in separate segments than after a merge.
- **Term enumeration.** A predicate on the column that a [`ts_dict_*`](./term-dictionary.md) query enumerates is exempt from the cap, because there the terms are the result rather than a means to one. Other predicates in the same query keep it, and since enumeration only sees matching documents, capping one of those narrows the returned terms too.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_levenshtein('quikc', 2)` | `1`, `2` | `quikc` → `quick` is within 2 edits. |
| `body @@ ts_levenshtein('quikc', 1, true)` | `1`, `2` | The `kc`↔`ck` transposition is a single edit. |
| `body @@ ts_levenshtein('quikc', 1, false)` | *(none)* | Without transpositions, `quikc` → `quick` is 2 edits. |
| `body @@ ts_levenshtein('X', 1, true, 'quic')` | `1`, `2` | Exact prefix `quic` plus one edit (`X` → `k`) reaches `quick`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_levenshtein" />

Combine a fixed `prefix` with a small fuzzy suffix to keep typo-tolerance fast and focused — only terms beginning with `quic` are considered:

<SqlLogicTest id="sql/functions/full_text_search/ts_levenshtein_prefix" />

#### `ts_ngram(text[, threshold])` {#ts_ngram}

Match by n-gram similarity — fuzzy matching that scores on shared character sequences rather than edit distance.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR`/`BLOB` | — | The term to match approximately. |
| `threshold` | `DOUBLE` | `0.7` | Minimum similarity, in `0.0`–`1.0`. A term matches when the fraction of n-grams it shares with `text` is at least `threshold`. Lower values widen the match (higher recall); higher values tighten it (higher precision). |

**How it works.** This requires a column tokenized with an [n-gram dictionary](../../statements/create_text_search_dictionary/ngram.md) (our `bigram` dictionary splits `hello` into `he`, `el`, `ll`, `lo`). The query string is split the same way and a term matches when enough of its n-grams overlap. Because it compares sub-sequences, n-gram similarity tolerates insertions, deletions and reorderings and is well suited to short strings and approximate matching where edit distance is too rigid. `1.0` demands an exact n-gram set; `0.3` is permissive.

| Query | Matches `id`, `title` | Why |
| :--- | :--- | :--- |
| `title @@ ts_ngram('hello')` | `1` `hello` | At the default `0.7`, only `hello` shares enough bigrams. |
| `title @@ ts_ngram('hello', 0.3)` | `1` `hello`, `2` `help`, `4` `held` | The lower threshold also admits `help` and `held`. |
| `title @@ ts_ngram('hello', 0.99)` | `1` `hello` | Near-exact n-gram overlap required. |
| `title @@ ts_ngram('zzzz', 0.3)` | *(none)* | No title shares bigrams with `zzzz`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_ngram" />

> N-gram similarity is recall-oriented, and `threshold` is its only bound on candidate terms: there is no expansion cap here, unlike [`ts_levenshtein`](#ts_levenshtein). Very low thresholds on large vocabularies can be broad; raise `threshold` to tighten results.

#### `ts_between(min, max, min_incl, max_incl)` {#ts_between}

Range match between two bounds, with explicit control over each end's inclusivity.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `min` | any (matches the column) | — *(required)* | Lower bound, or `NULL` for unbounded below. |
| `max` | any (matches the column) | — *(required)* | Upper bound, or `NULL` for unbounded above. |
| `min_incl` | `BOOLEAN` | — *(required)* | `true` includes `min` (`>=`), `false` excludes it (`>`). |
| `max_incl` | `BOOLEAN` | — *(required)* | `true` includes `max` (`<=`), `false` excludes it (`<`). |

**How it works.** `ts_between` performs a range scan over a numeric, temporal or verbatim-text (`keyword`-analyzed) column. Unlike `BETWEEN` in plain SQL, both inclusivity flags are explicit and **required** — there is no default. Either bound may be `NULL` to make that side unbounded; both `NULL` matches every indexed value. (For a one-sided range you can also use the shorthand [`ts_lt`](#ts_lt)/[`ts_le`](#ts_le)/[`ts_gt`](#ts_gt)/[`ts_ge`](#ts_ge), which take a single non-`NULL` bound.)

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `id @@ ts_between(2, 3, true, true)` | `2`, `3` | Closed interval `[2, 3]`. |
| `id @@ ts_between(2, 3, false, false)` | *(none)* | Open interval `(2, 3)` contains no integer. |
| `id @@ ts_between(NULL, 2, true, true)` | `1`, `2` | Unbounded below, up to and including `2`. |
| `id @@ ts_between(3, NULL, true, true)` | `3`, `4` | From `3` (inclusive) upward, unbounded above. |

<SqlLogicTest id="sql/functions/full_text_search/ts_between" />

A `NULL` bound makes one side unbounded — here, everything up to and including `id = 2`:

<SqlLogicTest id="sql/functions/full_text_search/ts_between_unbounded" />

#### `ts_lt(value)` {#ts_lt}

Match values strictly less than `value` — the one-sided shorthand for `ts_between(NULL, value, ?, false)`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `value` | any (matches the column) | — | Exclusive upper bound. Must be non-`NULL` (use [`ts_between`](#ts_between) for an unbounded side). |

**How it works.** Matches every indexed value `v` with `v < value`. Inclusivity is fixed by the function name: `ts_lt` is strict (`<`).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `id @@ ts_lt(3)` | `1`, `2` | Values strictly below `3`. |
| `id @@ ts_lt(1)` | *(none)* | Nothing is below the minimum `id`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_lt" />

#### `ts_le(value)` {#ts_le}

Match values less than or equal to `value`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `value` | any (matches the column) | — | Inclusive upper bound. Must be non-`NULL`. |

**How it works.** Matches every indexed value `v` with `v <= value`; `value` itself is included.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `id @@ ts_le(2)` | `1`, `2` | Values up to and including `2`. |
| `id @@ ts_le(0)` | *(none)* | Nothing is at or below `0`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_le" />

#### `ts_gt(value)` {#ts_gt}

Match values strictly greater than `value`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `value` | any (matches the column) | — | Exclusive lower bound. Must be non-`NULL`. |

**How it works.** Matches every indexed value `v` with `v > value`.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `id @@ ts_gt(3)` | `4` | Values strictly above `3`. |
| `id @@ ts_gt(4)` | *(none)* | Nothing is above the maximum `id`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_gt" />

#### `ts_ge(value)` {#ts_ge}

Match values greater than or equal to `value`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `value` | any (matches the column) | — | Inclusive lower bound. Must be non-`NULL`. |

**How it works.** Matches every indexed value `v` with `v >= value`; `value` itself is included.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `id @@ ts_ge(3)` | `3`, `4` | Values from `3` upward. |
| `id @@ ts_ge(5)` | *(none)* | Nothing is at or above `5`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_ge" />

#### `ts_any(list[, min_match])` {#ts_any}

OR over a list of sub-queries, with an optional "match at least N" threshold.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `list` | `LIST(TSQUERY)` (bare strings allowed) | — | The alternatives. Each element is a `TSQUERY`; a plain string is tokenized by the column dictionary. |
| `min_match` | `INTEGER` | `1` | How many alternatives a row must satisfy. Must be between `1` and the list length. `1` is a plain `OR`; raising it demands more of the alternatives. |

**How it works.** `ts_any` is a disjunction with a tunable floor. At `min_match = 1` it is a straight `OR` — match any alternative. Raising `min_match` turns it into an "N of M" query: with three alternatives and `min_match = 2`, a row must contain at least two of them. This is the equivalent of Elasticsearch's `minimum_should_match` (integer form) and the `terms_set` query. SereneDB takes an integer count only — it does not accept percentage or negative `minimum_should_match` formats, nor a per-document min-match field.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_any(['quick', 'grey'])` | `1`, `2`, `3` | Any one of the terms is enough. |
| `body @@ ts_any(['quick', 'grey', 'red'], 2)` | `2` | Only `id 2` (`quick` + `red`) contains two of the three. |
| `body @@ ts_any(['unicorn', 'dragon'])` | *(none)* | Neither term appears. |

<SqlLogicTest id="sql/functions/full_text_search/ts_any" />

Set `min_match` above `1` for an "N of M" query — here, rows containing at least two of the three terms:

<SqlLogicTest id="sql/functions/full_text_search/ts_any_min_match" />

#### `ts_all(list)` {#ts_all}

AND over a list of sub-queries — every element must match.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `list` | `LIST(TSQUERY)` (bare strings allowed) | — | The conjuncts. A row matches only when it satisfies *all* of them. |

**How it works.** `ts_all` is the conjunction (`AND`) of every element — equivalent to chaining the elements with [`&&`](#a--b-and), or to `ts_any(list, len(list))`. Use it to require that several tokens or sub-queries all appear in the same row.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ts_all([ts_phrase('quick'), ts_phrase('brown')])` | `1` | Only `id 1` has both `quick` and `brown`. |
| `body @@ ts_all([ts_phrase('quick'), ts_phrase('red')])` | `2` | Only `id 2` has both `quick` and `red`. |
| `body @@ ts_all([ts_phrase('quick'), ts_phrase('grey')])` | *(none)* | No row has both `quick` and `grey`. |

<SqlLogicTest id="sql/functions/full_text_search/ts_all" />

#### `ts_compound(must, must_not, should[, min_should_match])` {#ts_compound}

Boolean query combining required, forbidden and optional clauses in one call — the SereneDB analog of the Elasticsearch [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) query.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `must` | `TSQUERY`, `LIST(TSQUERY)` or `NULL` | — | Clauses that must all match (`AND`). `NULL` is an empty bucket. |
| `must_not` | `TSQUERY`, `LIST(TSQUERY)` or `NULL` | — | Clauses that must not match (exclusion). `NULL` is an empty bucket. |
| `should` | `TSQUERY`, `LIST(TSQUERY)` or `NULL` | — | Optional clauses; at least `min_should_match` of them must match. `NULL` is an empty bucket. |
| `min_should_match` | `INTEGER` | `1` | How many `should` clauses are required. Must be between `1` and the number of `should` clauses; supplying it with no `should` clauses is an error. |

**How it works.** `ts_compound` mirrors Elasticsearch's [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) query: `must` ≈ `AND`, `must_not` ≈ negation, `should` ≈ a tunable `OR`. A row matches when every `must` clause matches, no `must_not` clause matches, and at least `min_should_match` of the `should` clauses match. Each bucket accepts a single `TSQUERY`, a list of them, or `NULL` for "no clauses here". This packs the work of [`&&`](#a--b-and), [`!!`](#-a-not) and [`ts_any`](#ts_any) into one expression.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `ts_compound(ts_phrase('quick'), ts_phrase('grey'), [ts_phrase('lazy')])` | `1`, `2` | Must have `quick`, must not have `grey`, should have `lazy`. |
| `ts_compound(ts_phrase('fox'), NULL, [ts_phrase('brown'), ts_phrase('jumps'), ts_phrase('lazy')], 2)` | `1`, `2` | Both fox rows have at least two of `brown`/`jumps`/`lazy`. |
| `ts_compound(ts_phrase('fox'), NULL, [ts_phrase('brown'), ts_phrase('jumps'), ts_phrase('lazy')], 3)` | `1` | Only `id 1` has all three optional terms. |

<SqlLogicTest id="sql/functions/full_text_search/ts_compound" />

Tighten the `should` bucket with `min_should_match` — require a fox row that also has all three optional terms `brown`, `jumps` and `lazy`:

<SqlLogicTest id="sql/functions/full_text_search/ts_compound_min_should" />

## TSQUERY Operators {#tsquery-operators}

| Operator | Description |
| :--- | :--- |
| [`a \|\| b`](#a--b-or) | OR — match either sub-query. |
| [`a && b`](#a--b-and) | AND — match both sub-queries. |
| [`!! a`](#-a-not) | NOT — exclude matches of `a` (within a conjunction). |
| [`a ## b`](#a--b-phrase) | Phrase adjacency, with optional gap or gap range. |
| [`a ^ factor`](#a--factor-boost) | Boost `a`'s contribution to the relevance score. |

#### `a || b` {#a--b-or}

Disjunction (`OR`) of two sub-queries.

| Operand | Type | Meaning |
| :--- | :--- | :--- |
| `a`, `b` | `TSQUERY` | Sub-queries. A row matches when *either* matches. |

**How it works.** `||` combines two `TSQUERY` values into one that matches if at least one side does. It is the pairwise form of [`ts_any`](#ts_any); chain it (`a || b || c`) for more alternatives.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ (ts_phrase('brown') \|\| ts_phrase('grey'))` | `1`, `3` | `brown` is in `id 1`, `grey` in `id 3`. |
| `body @@ (ts_phrase('unicorn') \|\| ts_phrase('grey'))` | `3` | Only the `grey` side matches. |

<SqlLogicTest id="sql/functions/full_text_search/a--b-or" />

#### `a && b` {#a--b-and}

Conjunction (`AND`) of two sub-queries.

| Operand | Type | Meaning |
| :--- | :--- | :--- |
| `a`, `b` | `TSQUERY` | Sub-queries. A row matches only when *both* match. |

**How it works.** `&&` matches only rows satisfying both operands — the pairwise form of [`ts_all`](#ts_all). It pairs naturally with [`!!`](#-a-not) to express "has X but not Y".

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ (ts_phrase('quick') && ts_phrase('brown'))` | `1` | Only `id 1` has both. |
| `body @@ (ts_phrase('quick') && ts_phrase('grey'))` | *(none)* | No row has both. |

<SqlLogicTest id="sql/functions/full_text_search/a--b-and" />

#### `!! a` {#-a-not}

Unary negation (`NOT`) — excludes the matches of `a`.

| Operand | Type | Meaning |
| :--- | :--- | :--- |
| `a` | `TSQUERY` | The sub-query to negate. |

**How it works.** `!!a` is a hard exclusion (Elasticsearch `must_not`): it removes rows that match `a`. It is meaningful only inside a conjunction — `something && !!a` means "matches `something` but not `a`". A standalone negation has no positive clause to filter, so always combine it. SereneDB has no *soft* down-weighting query (Elasticsearch's `boosting` with a `negative_boost`); use [`^`](#a--factor-boost) to raise a positive clause instead.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ (ts_phrase('quick') && !!ts_phrase('brown'))` | `2` | Has `quick`, excludes the `brown` row. |
| `body @@ (ts_phrase('fox') && !!ts_phrase('red'))` | `1` | Has `fox`, excludes the `red` row. |

<SqlLogicTest id="sql/functions/full_text_search/-a-not" />

#### `a ## b` {#a--b-phrase}

Ordered proximity: require the sub-queries to appear close together, in order.

| Operand | Type | Meaning |
| :--- | :--- | :--- |
| `a`, `b` | `TSQUERY` or `VARCHAR` | The two ends of the phrase. |
| gap (between) | `INTEGER` or `INTEGER[]` | Optional. `a ## b` is strict adjacency; `a ## N ## b` requires exactly `N` tokens between; `a ## [min, max] ## b` allows a gap range. |

**How it works.** `##` is an **ordered** proximity operator: `a` must precede `b`. The integer counts the tokens *between* the two ends — `a ## b` (no integer) and `a ## 0 ## b` both mean immediate adjacency, `a ## 2 ## b` means exactly two intervening tokens. Order matters: `'quick' ## 'brown'` matches `quick brown` but `'brown' ## 'quick'` does not.

> The integer in `##` counts the tokens *between* the operands (`0` = adjacent). The [`tsquery_phrase`](#tsquery_phrase) function and PostgreSQL's `<->` use the opposite convention, where `distance = 1` means adjacent. See [`tsquery_phrase`](#tsquery_phrase).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ ('quick' ## 0 ## 'brown')` | `1` | `quick brown` are adjacent in order. |
| `body @@ ('brown' ## 0 ## 'quick')` | *(none)* | Reversed order — `##` is ordered. |
| `body @@ ('quick' ## 1 ## 'fox')` | `1`, `2` | Exactly one token (`brown`/`red`) between them. |
| `body @@ ('fox' ## 3 ## 'dog')` | `1`, `2` | `jumps over lazy` are the three tokens between. |

<SqlLogicTest id="sql/functions/full_text_search/a--b-phrase" />

#### `a ^ factor` {#a--factor-boost}

Boost: scale a sub-query's contribution to the relevance score.

| Operand | Type | Meaning |
| :--- | :--- | :--- |
| `a` | `TSQUERY` | The sub-query to boost. |
| `factor` | `DOUBLE` (≥ 0) | Multiplier applied to `a`'s score contribution. `> 1` raises it, `< 1` lowers it, `0` zeroes it. |

**How it works.** `^` reweights a clause for [relevance scoring](./scoring.md) without changing *which* rows match — the result set is identical, only the order differs once you `ORDER BY` a score. The factor **multiplies that clause's contribution to the score**: with `^ 3.0`, a row that matches the boosted clause earns three times its normal [`BM25`](./scoring.md) weight from it (the worked example below verifies the boosted rows score exactly 3× the unboosted). `factor > 1` promotes a clause, `0 < factor < 1` demotes it and `0` neutralizes its score contribution (the clause still matches). Factors compose multiplicatively when nested — `(a ^ 2) ^ 3` weights `a` by 6. Typical use is favouring one alternative in a disjunction, e.g. `title_match ^ 3 || body_match`. The factor must be non-negative, and the cast form `a::boost(factor)` is equivalent.

| Query | Matches `id` | Effect |
| :--- | :--- | :--- |
| `body @@ (ts_phrase('fox') \|\| ts_phrase('quick') ^ 2.0)` | `1`, `2` | Same rows as the un-boosted `OR`; `quick` matches simply score higher. |

<SqlLogicTest id="sql/functions/full_text_search/a--factor-boost" />

To see the effect, boost one alternative and order by [`BM25`](./scoring.md):

<SqlLogicTest id="sql/functions/full_text_search/a--factor-boost-score" />

## PostgreSQL-Compatible Parsers {#postgresql-compatible-parsers}

Each accepts a single string and returns a `TSQUERY`. These are SereneDB inverted-index queries, not PostgreSQL `tsvector`/`tsquery`.

| Function | Description |
| :--- | :--- |
| [`to_tsquery(text)`](#to_tsquery) | Parse a Lucene-style query (`AND`/`OR`, `+`/`-`, `*`, `~`, phrases, grouping, boost). |
| [`plainto_tsquery(text)`](#plainto_tsquery) | Tokenize and `AND` the terms. |
| [`phraseto_tsquery(text)`](#phraseto_tsquery) | Treat `text` as a phrase. |
| [`websearch_to_tsquery(text)`](#websearch_to_tsquery) | Web-search syntax: quoted substrings are phrases, `OR` separates alternatives. |
| [`tsquery_phrase(a, b[, distance])`](#tsquery_phrase) | Function form of the `##` phrase operator. |

#### `to_tsquery(text)` {#to_tsquery}

Parse a single Lucene-style query string into a `TSQUERY`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` | — | A Lucene / Elasticsearch [`query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax)-style expression (see the grammar below). Parsing is **strict**: a malformed expression raises an error. For lenient, user-facing input use [`websearch_to_tsquery`](#websearch_to_tsquery). |

**How it works.** `to_tsquery` runs the full query parser — the richest of the parsers. Beyond the `AND` / `OR` / `NOT` boolean keywords it supports the common operators of the [Lucene classic query-parser grammar](https://lucene.apache.org/core/9_10_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package.description):

| Syntax | Meaning |
| :--- | :--- |
| `a AND b` · `a OR b` · `a NOT b` | Boolean conjunction / disjunction / exclusion. |
| `+a` | `a` is required. |
| `-a` | `a` is excluded. |
| `a*` | Prefix (wildcard) match. |
| `a~N` | Fuzzy match within edit distance `N`. |
| `"a b"` | Phrase match. |
| `"a b"~N` | Proximity phrase: `a` and `b` within `N` positions. |
| `(a b)` | Grouping. |
| `a^N` | Boost `a`'s relevance contribution by factor `N`. |

These combine freely. Despite the PostgreSQL-compatible name, this builds a SereneDB inverted-index query, not a PostgreSQL `tsquery`; the queries operate on the single column on the left of `@@` (there is no `field:term` scoping).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ to_tsquery('quick AND brown')` | `1` | Both terms required. |
| `body @@ to_tsquery('+fox -red')` | `1` | Has `fox`, excludes `red`. |
| `body @@ to_tsquery('qui*')` | `1`, `2` | Prefix `qui` matches `quick`. |
| `body @@ to_tsquery('quikc~2')` | `1`, `2` | Fuzzy match within 2 edits of `quick`. |
| `body @@ to_tsquery('"quick brown"~3')` | `1` | `quick` and `brown` within three positions. |
| `body @@ to_tsquery('xyzzy')` | *(none)* | Term not present. |

<SqlLogicTest id="sql/functions/full_text_search/to_tsquery" />

The `+` (required) and `-` (excluded) operators, grouping and the others combine freely — for example, match rows containing `fox` but not `red`:

<SqlLogicTest id="sql/functions/full_text_search/to_tsquery_lucene" />

Wildcard, fuzzy and proximity operators compose too — a prefix on one term plus a fuzzy match on another:

<SqlLogicTest id="sql/functions/full_text_search/to_tsquery_advanced" />

#### `plainto_tsquery(text)` {#plainto_tsquery}

Tokenize `text` and combine the terms with `AND`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` | — | Free text. It is tokenized by the column dictionary and the resulting terms are joined with `AND`. Operators are *not* interpreted — `+`, `-`, quotes and `*` are treated as ordinary characters. |

**How it works.** `plainto_tsquery` is the "all words must appear" parser: it splits `text` into terms and requires every term, with no order constraint. It is the conjunctive counterpart to a bare string literal (which uses `OR`).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ plainto_tsquery('quick brown')` | `1` | Both `quick` and `brown` required. |
| `body @@ plainto_tsquery('quick fox')` | `1`, `2` | Both rows have `quick` and `fox`. |
| `body @@ plainto_tsquery('quick grey')` | *(none)* | No row has both. |

<SqlLogicTest id="sql/functions/full_text_search/plainto_tsquery" />

#### `phraseto_tsquery(text)` {#phraseto_tsquery}

Treat `text` as an exact ordered phrase.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` | — | Free text tokenized and matched as a contiguous, ordered phrase. |

**How it works.** `phraseto_tsquery` tokenizes `text` and requires the tokens to appear adjacent and in order — equivalent to [`ts_phrase`](#ts_phrase) for a single string. Use it when word order matters.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ phraseto_tsquery('over lazy dog')` | `1`, `2` | The three words are adjacent and in order in both. |
| `body @@ phraseto_tsquery('dog lazy over')` | *(none)* | Same words, wrong order. |

<SqlLogicTest id="sql/functions/full_text_search/phraseto_tsquery" />

#### `websearch_to_tsquery(text)` {#websearch_to_tsquery}

Parse forgiving, web-search-bar syntax into a `TSQUERY`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` | — | A search-engine-style string. Quoted substrings become phrases, the `OR` keyword separates alternatives, a leading `-` excludes a term and unquoted words are otherwise combined with `AND`. |

**How it works.** This is the parser for untrusted, user-facing input: unlike [`to_tsquery`](#to_tsquery) it never raises on malformed syntax — stray operators are simply treated as text. It recognizes `"quoted phrases"`, the literal `OR` keyword, and a leading `-` for exclusion; everything else is `AND`-ed.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ websearch_to_tsquery('"quick brown" OR "grey turtle"')` | `1`, `3` | Either phrase. |
| `body @@ websearch_to_tsquery('fox -red')` | `1` | Has `fox`, excludes `red`. |
| `body @@ websearch_to_tsquery('quick grey')` | *(none)* | Unquoted words are `AND`-ed; no row has both. |

<SqlLogicTest id="sql/functions/full_text_search/websearch_to_tsquery" />

#### `tsquery_phrase(a, b[, distance])` {#tsquery_phrase}

Function form of a two-term proximity phrase, using PostgreSQL's distance convention.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `a` | `TSQUERY` | — | First (left) sub-query. |
| `b` | `TSQUERY` | — | Second (right) sub-query, required to follow `a`. |
| `distance` | `INTEGER` | `1` (adjacent) | Number of lexemes between the start of `a` and the start of `b`, PostgreSQL-style: `1` means **adjacent**, `2` means one token in between. Must be `≥ 1`. |

**How it works.** `tsquery_phrase` is the function spelling of an ordered proximity phrase, matching PostgreSQL's `<N>` / `tsquery_phrase` semantics where `distance = 1` is adjacency. This is the **opposite** counting convention from the [`##`](#a--b-phrase) operator, where the integer counts the tokens *between* the operands (`0` = adjacent). Pick whichever reads more clearly — they target the same positions.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `body @@ tsquery_phrase('quick'::TSQUERY, 'brown'::TSQUERY)` | `1` | Default `distance = 1`: `quick brown` adjacent. |
| `body @@ tsquery_phrase('quick'::TSQUERY, 'fox'::TSQUERY, 2)` | `1`, `2` | `distance = 2`: one token (`brown`/`red`) between. |
| `body @@ tsquery_phrase('quick'::TSQUERY, 'fox'::TSQUERY, 1)` | *(none)* | `distance = 1` demands adjacency, but a token sits between them. |

<SqlLogicTest id="sql/functions/full_text_search/tsquery_phrase" />

## Convenience Predicates {#convenience-predicates}

Sugar that rewrites to `@@` at bind time. Each takes the indexed column as its first argument and returns `BOOLEAN`.

| Function | Description |
| :--- | :--- |
| [`phrase_matches(column, text[, text, ...])`](#phrase_matches) | Equivalent to `column @@ ts_phrase(...)`. |
| [`ngram_matches(column, text[, threshold])`](#ngram_matches) | Equivalent to `column @@ ts_ngram(...)`. |
| [`levenshtein_matches(column, text, distance[, transpositions[, prefix]])`](#levenshtein_matches) | Equivalent to `column @@ ts_levenshtein(...)`. |
| [`has_all_tokens(column, list)`](#has_all_tokens) | Equivalent to `column @@ ts_all(list)`. |
| [`has_any_tokens(column, list[, min_match])`](#has_any_tokens) | Equivalent to `column @@ ts_any(list[, min_match])`. |

#### `phrase_matches(column, text[, text, ...])` {#phrase_matches}

Phrase-match sugar for `column @@ ts_phrase(text, ...)`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | any indexed column | — | The inverted-indexed column to match. |
| `text` | `VARCHAR`/`BLOB` | — | A phrase segment, as in [`ts_phrase`](#ts_phrase). |
| `text, ...` | `INTEGER`/`INTEGER[]` + `VARCHAR` | — | Optional `gap, text` pairs, identical to `ts_phrase`'s gap grammar. |

**How it works.** `phrase_matches(column, ...)` rewrites at bind time to `column @@ ts_phrase(...)`, inheriting the full gap grammar. It reads as a predicate, which is convenient when the `@@` form feels verbose.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `phrase_matches(body, 'quick brown')` | `1` | Adjacent phrase, only in `id 1`. |
| `phrase_matches(body, 'quick', 3, 'over')` | `1`, `2` | Gap form: three tokens between `quick` and `over`. |
| `phrase_matches(body, 'brown grey')` | *(none)* | Those words never appear adjacent. |

<SqlLogicTest id="sql/functions/full_text_search/phrase_matches" />

#### `ngram_matches(column, text[, threshold])` {#ngram_matches}

N-gram-match sugar for `column @@ ts_ngram(text[, threshold])`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | n-gram-indexed column | — | The column to match. |
| `text` | `VARCHAR`/`BLOB` | — | Term to match by similarity. |
| `threshold` | `DOUBLE` | `0.7` | Minimum n-gram similarity, as in [`ts_ngram`](#ts_ngram). |

**How it works.** Rewrites to `column @@ ts_ngram(text[, threshold])`; see [`ts_ngram`](#ts_ngram) for the similarity model.

| Query | Matches `id`, `title` | Why |
| :--- | :--- | :--- |
| `ngram_matches(title, 'hello', 0.3)` | `1` `hello`, `2` `help`, `4` `held` | All share enough bigrams at `0.3`. |
| `ngram_matches(title, 'hello')` | `1` `hello` | At the default `0.7`, only `hello` qualifies. |

<SqlLogicTest id="sql/functions/full_text_search/ngram_matches" />

#### `levenshtein_matches(column, text, distance[, transpositions[, prefix]])` {#levenshtein_matches}

Fuzzy-match sugar for `column @@ ts_levenshtein(...)`.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | any indexed column | — | The column to match. |
| `text` | `VARCHAR`/`BLOB` | — | Term to match approximately. |
| `distance` | `INTEGER` | — *(required here)* | Maximum edit distance. Unlike [`ts_levenshtein`](#ts_levenshtein), the predicate has no auto-distance form, so `distance` is required. |
| `transpositions` | `BOOLEAN` | `true` | Whether an adjacent-character swap counts as one edit. |
| `prefix` | `VARCHAR` | `''` | Exact leading substring; only the suffix spends the edit budget. |

**How it works.** Rewrites to `column @@ ts_levenshtein(text, distance[, transpositions[, prefix]])`. See [`ts_levenshtein`](#ts_levenshtein) for the edit-distance and prefix semantics.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `levenshtein_matches(body, 'quikc', 2)` | `1`, `2` | Within 2 edits of `quick`. |
| `levenshtein_matches(body, 'quikc', 1, false)` | *(none)* | Without transpositions, `quikc` → `quick` is 2 edits. |

<SqlLogicTest id="sql/functions/full_text_search/levenshtein_matches" />

#### `has_all_tokens(column, list)` {#has_all_tokens}

True when every token in `list` is present.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | any indexed column | — | The column to match. |
| `list` | `LIST(VARCHAR)` | — | Tokens that must *all* appear. Each element is tokenized by the column dictionary. |

**How it works.** Tokenizes each element and requires all of them — sugar for `column @@ ts_all(ts_tokenize(list))`. Use it for "contains every one of these words" filters.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `has_all_tokens(body, ['quick', 'brown'])` | `1` | Only `id 1` has both. |
| `has_all_tokens(body, ['quick', 'fox'])` | `1`, `2` | Both have `quick` and `fox`. |
| `has_all_tokens(body, ['quick', 'grey'])` | *(none)* | No row has both. |

<SqlLogicTest id="sql/functions/full_text_search/has_all_tokens" />

#### `has_any_tokens(column, list[, min_match])` {#has_any_tokens}

True when at least `min_match` tokens in `list` are present.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `column` | any indexed column | — | The column to match. |
| `list` | `LIST(VARCHAR)` (or a single `VARCHAR`) | — | Candidate tokens. |
| `min_match` | `INTEGER` | `1` | Minimum number of tokens that must appear. `1` is a plain `OR`. |

**How it works.** Tokenizes the candidates and requires at least `min_match` of them — sugar for `column @@ ts_any(ts_tokenize(list)[, min_match])`. Raise `min_match` for an "N of M" filter, exactly as in [`ts_any`](#ts_any).

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `has_any_tokens(body, ['grey', 'red'])` | `2`, `3` | `red` in `id 2`, `grey` in `id 3`. |
| `has_any_tokens(body, ['quick', 'grey', 'red'], 2)` | `2` | Only `id 2` has two of the three. |
| `has_any_tokens(body, ['unicorn', 'dragon'])` | *(none)* | Neither appears. |

<SqlLogicTest id="sql/functions/full_text_search/has_any_tokens" />

## NULL Checks {#null-checks}

There is no dedicated TSQUERY constructor for nullness — the plain SQL operators do the job and the index claims them.

#### `column IS NULL` / `column IS NOT NULL` {#is-null}

Match rows by the nullness of an indexed column.

**How it works.** Every indexed column carries a null-marker term in the index; `IS NULL` compiles to a lookup of that term and `IS NOT NULL` to its negation, so both run as posting-list reads — no `INCLUDE` storage or table probe needed. On columns the index does not cover, the same predicates simply run row-level; the syntax is identical either way. In the [setup](#setup), `dual` has one `NULL` row.

| Query | Matches `id` | Why |
| :--- | :--- | :--- |
| `dual IS NULL` | `2` | `id 2` is the only row whose `dual` is `NULL`. |
| `dual IS NOT NULL` | `1`, `3`, `4` | Every other row has a value. |

<SqlLogicTest id="sql/functions/full_text_search/is-null" />

<SqlLogicTest id="sql/functions/full_text_search/is-not-null" />

## Utility Functions {#utility-functions}

| Function | Description |
| :--- | :--- |
| [`ts_lexize(dictionary, text)`](#ts_lexize) | Return the tokens a dictionary produces for `text`. |
| [`ts_split_by_non_alpha(text [, to_lower])`](#ts_split_by_non_alpha) | Split `text` on runs of non-alphanumeric characters. |

#### `ts_lexize(dictionary, text)` {#ts_lexize}

Return the tokens a dictionary produces for `text` — the tool for inspecting analysis.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `dictionary` | `VARCHAR` | — | Name of an existing [text-search dictionary](../../statements/create_text_search_dictionary/index.md). It must exist in the catalog (`'keyword'` is not a real dictionary here). |
| `text` | `VARCHAR` or `LIST(VARCHAR)` | — | The text to analyze. A list analyzes each element and concatenates the results. |

**How it works.** `ts_lexize` is the only function on this page that runs on its own (not inside `@@`): it applies a named dictionary's analysis pipeline — lower-casing, stemming, stop-word removal, n-gram splitting — and returns the resulting lexemes as a `LIST(VARCHAR)`. Use it to see exactly how a query string or a document will be tokenized when [tuning an index](../../indexes/inverted/text-analysis.md): if your search misses, lexize both the query and the source text and compare.

| Input | Tokens | Why |
| :--- | :--- | :--- |
| `ts_lexize('en', 'Quick BROWN')` | `{quick, brown}` | The `en` text dictionary lower-cases and splits on whitespace. |
| `ts_lexize('bigram', 'help')` | `{he, el, lp}` | The `bigram` n-gram dictionary emits overlapping 2-grams. |

<SqlLogicTest id="sql/functions/full_text_search/ts_lexize" />

Lexize against an n-gram dictionary to see how a term is split for [`ts_ngram`](#ts_ngram) matching:

<SqlLogicTest id="sql/functions/full_text_search/ts_lexize_ngram" />

#### `ts_split_by_non_alpha(text [, to_lower])` {#ts_split_by_non_alpha}

Split `text` on runs of non-alphanumeric characters and return the alphanumeric runs as a `LIST(VARCHAR)`. Unlike [`ts_lexize`](#ts_lexize), it needs no dictionary — it is a self-contained scalar function.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `text` | `VARCHAR` | — | The string to split. `NULL` yields `NULL`. |
| `to_lower` | `BOOLEAN` | `false` | ASCII-lowercase each emitted token. |

A token is a maximal run of `[A-Za-z0-9]`; every other character — punctuation, whitespace, underscores, and any non-ASCII byte — is a separator, and empty tokens are never emitted. This is the fast, dictionary-free equivalent of `regexp_split_to_array(text, '[^A-Za-z0-9]+')` (or `regexp_split_to_array(lower(text), '[^a-z0-9]+')` with `to_lower => true`), without the regex engine.

| Input | `to_lower` | Result |
| :--- | :--- | :--- |
| `ts_split_by_non_alpha('Hello, World! 123_abc')` | `false` | `{Hello,World,123,abc}` |
| `ts_split_by_non_alpha('The Quick-Brown FOX 2024', true)` | `true` | `{the,quick,brown,fox,2024}` |

<SqlLogicTest id="sql/functions/full_text_search/ts_split_by_non_alpha" />

## Coming from Elasticsearch {#mapping}

The functions on this page cover most of the Elasticsearch / OpenSearch query DSL. This table maps each DSL query to its SereneDB equivalent (the left column links to the Elasticsearch reference).

| Elasticsearch / OpenSearch query | SereneDB |
| :--- | :--- |
| [`match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) (analyzed terms, `OR`) | bare string, or [`ts_tokenize`](#ts_tokenize) |
| [`match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) with `operator: and` | [`plainto_tsquery`](#plainto_tsquery) |
| [`match_phrase`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html) | [`ts_phrase`](#ts_phrase), [`phraseto_tsquery`](#phraseto_tsquery) |
| [`match_phrase`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html) with `slop` | [`ts_phrase`](#ts_phrase) with `slop := N` or `::slop(N)`; same semantics |
| [`term`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html) / [`terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html) | token literal, [`has_any_tokens`](#has_any_tokens) |
| [`terms_set`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-set-query.html) (match N of M) | [`ts_any`](#ts_any) with `min_match` |
| [`prefix`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html) | [`ts_starts_with`](#ts_starts_with) |
| [`wildcard`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html) | [`ts_like`](#ts_like) or [`ts_regexp`](#ts_regexp) |
| [`regexp`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html) | [`ts_regexp`](#ts_regexp) |
| [`fuzzy`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) | [`ts_levenshtein`](#ts_levenshtein) |
| [`range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html) | [`ts_between`](#ts_between), [`ts_lt`](#ts_lt)/[`ts_le`](#ts_le)/[`ts_gt`](#ts_gt)/[`ts_ge`](#ts_ge) |
| [`exists`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html) | Plain SQL [`IS NOT NULL` / `IS NULL`](#is-null) — the index claims both on indexed columns. |
| [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) (`must`/`must_not`/`should`) | [`ts_compound`](#ts_compound), or [`&&`](#a--b-and)/[`!!`](#-a-not)/[`ts_any`](#ts_any) |
| [`query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html) / [`simple_query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html) | [`to_tsquery`](#to_tsquery) (strict), [`websearch_to_tsquery`](#websearch_to_tsquery) (lenient) |

### Notable differences

Elasticsearch features without a direct SereneDB equivalent, and what to use instead:

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| [`minimum_should_match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-minimum-should-match.html) percentage / negative / combination forms | integer count only ([`ts_any`](#ts_any), [`ts_compound`](#ts_compound)) |
| [`fuzziness: AUTO`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) | one-argument [`ts_levenshtein`](#ts_levenshtein) auto-picks a distance by term length |
| `max_expansions` (fuzzy / prefix expansion cap) | fuzzy: [`sdb_levenshtein_max_terms`](../../indexes/inverted/maintenance.md#session-settings) (session-level, per segment, default `64`); prefix: no cap |
| [`multi_match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html) / `combined_fields` / `field:term` scoping | single-column `@@`; compose multiple predicates with `OR` |
| [`constant_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html) | none; `ORDER BY` a literal, or `raw_boost` (see [Ranking](../../indexes/inverted/ranking.md)) |
| [`boosting`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html) (`negative_boost`) | none; raise a clause with [`^`](#a--factor-boost) or exclude with [`!!`](#-a-not) |
| [`match_phrase_prefix`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html) / `match_bool_prefix` | combine [`ts_phrase`](#ts_phrase) with [`ts_starts_with`](#ts_starts_with) |
| [`more_like_this`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html) | none; use vector similarity ([Vector Search](../../indexes/inverted/vector-search.md)) |

## See also

- [Relevance Scoring](./scoring.md) · [Highlighting](./highlighting.md) · [Vector Distance](../vector.md) · [Geospatial](./geo.md)
- [Inverted Index](../../indexes/inverted/index.md) · [Full-Text Search](../../indexes/inverted/full-text-search.md)
- [`tsquery` data type](../../data_types/tsquery.md)
- [CREATE TEXT SEARCH DICTIONARY](../../statements/create_text_search_dictionary/index.md)
