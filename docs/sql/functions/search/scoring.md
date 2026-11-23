---
title: Relevance Scoring
sidebar_label: Scoring
sidebar_position: 5
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Scorer Functions {#scorer-functions}

Scorer functions compute a relevance score for each row matched by a full-text search predicate, measuring how well the row matches the `@@` query in the same index scan. Each takes the index `tableoid` as its first argument and returns a `FLOAT`.

The score is an ordinary value — use it wherever you need one: project it in the `SELECT` list, rank with it in `ORDER BY <scorer> DESC`, filter on it in a `WHERE` comparison or fold it into a larger expression (for example, blending `BM25` with business signals). Ranking with `ORDER BY` is the most common use, but it is not required.

| Function | Description |
| :--- | :--- |
| [`BM25(tableoid[, k1, b])`](#bm25) | Okapi BM25 relevance score — the recommended default. |
| [`TFIDF(tableoid[, with_norms])`](#tfidf) | Classic TF-IDF score. |
| [`lm_jm(tableoid[, lambda])`](#lm_jm) | Language model, Jelinek-Mercer smoothing. |
| [`lm_dirichlet(tableoid[, mu])`](#lm_dirichlet) | Language model, Dirichlet smoothing. |
| [`indri_dirichlet(tableoid[, mu])`](#indri_dirichlet) | Indri-style Dirichlet smoothing. |
| [`dfi(tableoid[, measure])`](#dfi) | Divergence-from-independence score (parameter-free). |
| [`raw_tf(tableoid)`](#raw_tf) | Raw term frequency. |
| [`raw_boost(tableoid)`](#raw_boost) | Raw query boost factor. |
| [`raw_dl(tableoid)`](#raw_dl) | Raw document length. |

To see how a row's score was computed, use [`ts_explain(tableoid)`](#ts_explain).

## Quick start

Filter with `@@` to select matching rows, then rank them with a scorer in `ORDER BY ... DESC`. Add the primary key as a final sort key so ties resolve deterministically:

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM docs_idx
WHERE body @@ ts_phrase('fox')
ORDER BY score DESC, id;
```

Every scorer follows this shape — swap `BM25` for any function in the table above.

## Requirements {#requirements}

A scorer runs on any indexed column, but what it can tell apart depends on the column's feature flags:

| Feature flag on the column | Read by | Without it |
| :--- | :--- | :--- |
| `frequency` | every scorer except `raw_boost`, `idf`, `constant` and `BM25` with `k1 = 0` | Every row that matches a term scores the same for that term, as if the term occurred once. |
| `norm` | `BM25` with `b` above `0` (the default), `TFIDF` with `with_norms`, `lm_jm`, `lm_dirichlet`, `indri_dirichlet`, `dfi` and `raw_dl` | Every row counts as one token long, so the score ignores the column's length. |

Set both flags on the text search dictionary used by the indexed column; `norm` requires `frequency`:

```sql
-- term frequency, to rank by how often a term occurs
-- document length norms, to normalize by the column's length
CREATE TEXT SEARCH DICTIONARY scored_en AS
    split_text() | stem_words('en_US.UTF-8')
    WITH (frequency, position, norm);
```

See [token positions and feature flags](../../indexes/inverted/text-analysis.md#token-positions-and-feature-flags) for the full list.

:::note One scorer per index per query
A single index scan can apply only one scorer function. Two different scorers over the same index in one `SELECT` raise `Only one scorer function is allowed per inverted index`. To compute several scores for the same rows, combine the per-scorer queries with `UNION`.
:::

## Choosing a scorer

| If you want… | Use |
| :--- | :--- |
| A robust general-purpose default | **`BM25`** — start here; tune `k1` and `b` only if needed |
| A simple classic baseline / minimal tuning | **`TFIDF`** |
| Language-model ranking, short keyword queries | **`lm_dirichlet`** (length-aware smoothing) |
| Language-model ranking, longer / verbose queries | **`lm_jm`** (linear smoothing) |
| Indri / Lemur-compatible scores | **`indri_dirichlet`** |
| Good ranking with **no parameters to tune** | **`dfi`** |
| Raw signals to build your own score | **`raw_tf`**, **`raw_boost`**, **`raw_dl`** |

In practice **BM25 is the right default** for almost all full-text ranking — it models term-frequency *saturation* (extra occurrences of a term add ever less) and document-length normalization, which plain TF-IDF does not. Reach for the language-model scorers (`lm_*`) when you want probabilistic query-likelihood ranking, `dfi` when you can't tune parameters and the `raw_*` features when you compose a custom relevance expression (for example mixing BM25 with business signals).

How they differ, simple to advanced:

- **TF-IDF** rewards a term that is frequent in the document and rare across the collection. Its term-frequency weight has no ceiling (SereneDB uses the square root of the count), so a few very frequent terms can dominate.
- **BM25** is TF-IDF with two refinements: term-frequency *saturation* (`k1`) so the 10th occurrence counts far less than the 1st, and *length normalization* (`b`) so long documents are not unfairly favored for containing a term more times.
- **Language models** (`lm_*`) flip the question: instead of weighting terms, they estimate the probability that the document's word distribution would *generate* the query, smoothing each document with the whole-collection distribution so unseen terms do not zero out the score. They tend to track BM25 closely while exposing a single, interpretable smoothing knob.
- **DFI** is *parameter-free*: it scores a term by how far its observed frequency diverges from what statistical independence would predict, so there is nothing to tune at all.

## Scorers

A phrase or an n-gram match is scored with how often the whole match occurs in the row. `BM25`, `TFIDF` and `idf` add up the idf of its terms; `lm_jm`, `lm_dirichlet`, `indri_dirichlet` and `dfi` add up one score per term, each from that term's share of the column.

### `BM25(tableoid[, k1, b])` {#bm25}

**Signature.** `BM25(tableoid) -> FLOAT`, `BM25(tableoid, k1, b) -> FLOAT`.
**Captures.** Term-frequency saturation plus document-length normalization — the best all-round relevance signal.

The [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance score.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `k1` | `FLOAT` | `1.2` | Term-frequency saturation. Higher = extra occurrences keep mattering; lower = they saturate sooner. |
| `b` | `FLOAT` | `0.75` | Document-length normalization, in `[0, 1]`. `b = 0` disables it (a.k.a. BM15); `b = 1` fully normalizes by length. |

**How it works.** A term contributes more when it appears often in a document and rarely across the collection, but each extra occurrence adds ever less (controlled by `k1`), and the contribution is scaled down for long documents (controlled by `b`). It is the standard relevance model for full-text search — start here and tune only if results need it.

<SqlLogicTest id="sql/functions/full_text_search/bm25" />

Passing `k1` and `b` explicitly tunes the score. With `b = 0` (no length normalization) the two shorter documents are no longer penalized relative to each other and tie, where the default ranked the shorter one higher:

<SqlLogicTest id="sql/functions/full_text_search/bm25_params" />

### `TFIDF(tableoid[, with_norms])` {#tfidf}

**Signature.** `TFIDF(tableoid) -> FLOAT`, `TFIDF(tableoid, with_norms) -> FLOAT`.
**Captures.** Term frequency weighted by inverse document frequency — a simple, classic baseline.

The classic [tf–idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) (term-frequency × inverse-document-frequency) score.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `with_norms` | `BOOLEAN` | `false` | Apply document-length normalization. Requires the `norm` flag on the column to have an effect. |

**How it works.** Each matched term contributes `√tf × idf`: more occurrences in the row and rarer occurrence across the collection both raise the score. It has no term-frequency saturation, so a handful of very frequent terms can dominate — prefer BM25 unless you specifically want this baseline.

<SqlLogicTest id="sql/functions/full_text_search/tfidf" />

With `with_norms = true` the score is divided down for longer documents, which reorders nothing here but compresses the values:

<SqlLogicTest id="sql/functions/full_text_search/tfidf_norms" />

### `lm_jm(tableoid[, lambda])` {#lm_jm}

**Signature.** `lm_jm(tableoid) -> FLOAT`, `lm_jm(tableoid, lambda) -> FLOAT`.
**Captures.** Query-likelihood under a language model with fixed linear smoothing — suited to longer queries.

[Query-likelihood](https://en.wikipedia.org/wiki/Query_likelihood_model) language-model score with Jelinek-Mercer (linear) smoothing.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `lambda` | `FLOAT` | `0.1` | Smoothing weight in `(0, 1]`: each document's term probabilities are mixed with the collection's by `lambda`. Smaller favors precision on short queries; larger suits longer, verbose queries. |

**How it works.** The model estimates the probability that the document would generate the query, mixing the document's own term distribution with the collection-wide distribution by a *fixed* fraction `lambda`. Reads the `norm` flag: without it every row counts as one token long.

<SqlLogicTest id="sql/functions/full_text_search/lm_jm" />

### `lm_dirichlet(tableoid[, mu])` {#lm_dirichlet}

**Signature.** `lm_dirichlet(tableoid) -> FLOAT`, `lm_dirichlet(tableoid, mu) -> FLOAT`.
**Captures.** Query-likelihood with length-adaptive smoothing — usually the best language model for short keyword queries.

[Query-likelihood](https://en.wikipedia.org/wiki/Query_likelihood_model) language-model score with Dirichlet smoothing.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `mu` | `FLOAT` | `2000` | Dirichlet prior. Larger = more smoothing (the collection prior dominates); set it near your average document length. |

**How it works.** Like `lm_jm`, but the smoothing strength adapts to document length via the prior `mu` — short documents are smoothed proportionally more, which generally ranks short keyword queries better. Reads the `norm` flag: without it every row counts as one token long.

<SqlLogicTest id="sql/functions/full_text_search/lm_dirichlet" />

### `indri_dirichlet(tableoid[, mu])` {#indri_dirichlet}

**Signature.** `indri_dirichlet(tableoid) -> FLOAT`, `indri_dirichlet(tableoid, mu) -> FLOAT`.
**Captures.** Dirichlet-smoothed query-likelihood in the log domain, matching the Indri / Lemur search engine.

The [Indri](https://en.wikipedia.org/wiki/Lemur_Project)/Lemur variant of Dirichlet smoothing, without the score-floor clamp.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `mu` | `FLOAT` | `2000` | Dirichlet prior, as in `lm_dirichlet`. |

**How it works.** Same smoothing as `lm_dirichlet` but scores are returned in the log domain (typically negative) and the low-score floor clamp is omitted, so values match those produced by the Indri search engine. Use it when you need Indri-comparable scores. Reads the `norm` flag: without it every row counts as one token long.

<SqlLogicTest id="sql/functions/full_text_search/indri_dirichlet" />

### `dfi(tableoid[, measure])` {#dfi}

**Signature.** `dfi(tableoid) -> FLOAT`, `dfi(tableoid, measure) -> FLOAT`.
**Captures.** How far a term's frequency in a document diverges from statistical independence — with **nothing to tune**.

[Divergence-from-independence](https://scholar.google.com/scholar?q=A+nonparametric+term+weighting+method+for+information+retrieval+based+on+measuring+the+divergence+from+independence) term weighting.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `measure` | `VARCHAR` | `'standardized'` | Divergence statistic. One of `'standardized'`, `'saturated'`, `'chi_squared'`. |

**How it works.** For each term the model computes the frequency expected under independence, then scores by how far the observed frequency diverges from it — the `measure` selects which divergence statistic to use. It is *parameter-free* (no `k1`, `b`, `lambda` or `mu`), making it a strong choice when you can't or don't want to tune. Reads the `norm` flag: without it every row counts as one token long.

<SqlLogicTest id="sql/functions/full_text_search/dfi" />

The `'saturated'` and `'chi_squared'` measures rank the same documents here but produce different score magnitudes:

<SqlLogicTest id="sql/functions/full_text_search/dfi_saturated" />

<SqlLogicTest id="sql/functions/full_text_search/dfi_chi_squared" />

### `raw_tf(tableoid)` {#raw_tf}

**Signature.** `raw_tf(tableoid) -> FLOAT`. No parameters.
**Captures.** The raw count of matched-term occurrences in each row — a building block, not a ranking model.

Raw term frequency of the matched terms in each row. Use it inside a custom relevance expression; it does not normalize for length or rarity.

<SqlLogicTest id="sql/functions/full_text_search/raw_tf" />

### `raw_boost(tableoid)` {#raw_boost}

**Signature.** `raw_boost(tableoid) -> FLOAT`. No parameters.
**Captures.** The query-time boost factor that applied to each match (see [Boosting](#boosting) below).

Raw query boost contribution for each row. With no `^` boost in the query every match returns `1` — equivalent to a *constant score* (see [cross-engine notes](#cross-engine-notes)). When a clause is boosted with `^ f`, the matched rows carry that factor `f`.

<SqlLogicTest id="sql/functions/full_text_search/raw_boost" />

### `raw_dl(tableoid)` {#raw_dl}

**Signature.** `raw_dl(tableoid) -> FLOAT`. No parameters.
**Captures.** The length of the matched column for each row — the normalization input that `BM25(b>0)` and the `lm_*`/`dfi` scorers use internally.

Raw document length of the matched column: the number of token positions when the column records `POSITION`, so tokens stacked at one position, such as synonyms, count once, and the number of tokens otherwise. See [feature flags](../../statements/create_text_search_dictionary/index.md#feature-flags).

<SqlLogicTest id="sql/functions/full_text_search/raw_dl" />

## Boosting {#boosting}

The `^` operator multiplies a query clause's contribution to the score, so you can weight some clauses above others. The factor flows straight through every scorer: boosting `ts_phrase('fox') ^ 3.0` multiplies each matched row's BM25 score by exactly `3`.

<SqlLogicTest id="sql/functions/full_text_search/bm25_boost" />

`raw_boost` exposes the applied factor directly. See [Relevance ranking → Boosting](../../indexes/inverted/ranking.md#boosting) for boosting across multiple columns.

## Pattern and range matches {#patterns}

[`ts_starts_with`](./full-text.md#ts_starts_with), [`ts_like`](./full-text.md#ts_like), [`ts_regexp`](./full-text.md#ts_regexp), [`ts_between`](./full-text.md#ts_between) and its one-sided forms match many terms. Such a match is an `OR` of the terms it matches that keeps the best of them, and it scores a constant: a matching row gets `1` times the match's [boost](#boosting) once, however many of its terms the pattern matches. Under `OR` and `AND` the match is one clause, so a row that matches two of them scores `2`. Here `dog` and `day` both start with `d`, and row 5 holds both:

<SqlLogicTest id="sql/functions/full_text_search/pattern_constant_score" />

To rank by the matched terms instead, put [`::score`](#score-modifier) on the match itself. Every matching term then scores with that scorer, and the row gets the best of them:

<SqlLogicTest id="sql/functions/full_text_search/pattern_term_score" />

[`::merge('sum')`](#merge) on the match adds its terms up instead, as an `OR` of those terms would. With a scorer, row 5 then scores both `dog` and `day`; with the constant, a row scores it once per matching term:

<SqlLogicTest id="sql/functions/full_text_search/pattern_term_sum" />

`::score` with a scorer that needs term statistics, put on a group, scores the group's other terms and leaves its pattern and range matches at their constant. `::score('constant(N)')` replaces the constant with `N`, on the match or on a group around it.

[`ts_levenshtein`](./full-text.md#ts_levenshtein) is scored by default: each matched term scores with the `ORDER BY` scorer times its similarity to the query (with `starts_with := true`, the similarity of its closest beginning), and the row gets the best of them. `::score('constant(N)')` makes it score `N` once, without the similarity, and `::merge('sum')` adds its terms up.

Matches on numeric, network address and geographic columns always score a constant once: `::score('constant(N)')` changes its value, while a scorer that needs term statistics and `::merge('sum')` are errors.

## Per-node score control {#per-node}

Two modifiers change how a *part* of a query scores, while the rest of it stays on the `ORDER BY` scorer. Both are [`TSQUERY`](../../data_types/tsquery.md) modifiers, like [`::boost`](#boosting) and `::slop`, so they travel with the value: they apply to a parenthesized predicate, to a group built by [`ts_any`](./full-text.md#ts_any) or [`ts_all`](./full-text.md#ts_all), and to a bound parameter.

### `::merge(policy)` {#merge}

Sets how a boolean node combines what its branches scored, or how a [pattern match](#patterns) combines the terms it matched.

| Policy | Effect |
| :--- | :--- |
| `'sum'` | Add every matching branch's contribution. This is the behaviour of a boolean node without the modifier. |
| `'max'` | Take the best matching branch and ignore the others. This is the behaviour of a pattern match without the modifier. |
| `'default'` | The node's own behaviour. |

The name is read case-insensitively. Any other name is an error rather than a silent fallback.

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM   docs_idx
WHERE  (body @@ 'fox' OR body @@ 'cat')::merge('max');
```

Under the default `sum`, a document containing both `fox` and `cat` collects both contributions and outranks a document that matched one branch strongly. Under `max` it is scored once, on its best branch.

**A `max` starts at 0**, the score of a branch a row doesn't match: a group under `max` takes the best of 0 and its branches, so with [`indri_dirichlet`](#indri_dirichlet), whose scores are negative, it scores 0.

**The synonym case.** This is what makes `max` worth reaching for. Expanding a user's `car` into `car OR automobile` creates two branches that mean one thing, so summing them rewards a document for happening to use both spellings:

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM   docs_idx
WHERE  body @@ ts_any(['car', 'automobile'])::merge('max');
```

Synonyms that the column's dictionary produces itself, with [`expand_solr_synonyms`](./tokenizers/expand_solr_synonyms.md) or [`expand_wordnet_synonyms`](./tokenizers/expand_wordnet_synonyms.md), already score this way: the tokens a query word expands to at one position form one group merged with `max`, in bare strings, `ts_tokenize`, `plainto_tsquery`, `websearch_to_tsquery`, `to_tsquery`, `ts_any`/`ts_all` over `ts_tokenize`, and a one-word `ts_phrase`.

**It binds to its own node.** Nesting is therefore meaningful. With `((a OR b)::merge('max') OR c)` the inner group takes the best of `a` and `b` and the outer group still adds `c` on top. Move the modifier to the outer group and the best single branch of all three wins.

**It requires a boolean group or a pattern match.** A [pattern match](#patterns) combines the terms it matches; a single term has nothing to combine, so `(body @@ 'fox')::merge('max')` is an error.

`EXPLAIN` prints `Merge: max` on the node that carries the policy. Same-field branches fuse into one `Terms` node, which carries the policy of the `Or` it replaced.

### `::score(scorer)` {#score-modifier}

Scores the wrapped subtree with a scorer of its own. Everything outside it stays on the `ORDER BY` scorer, so one scan can mix signals:

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM   docs_idx
WHERE  body @@ 'fox' OR (body @@ 'cat')::score('constant(1)');
```

The `fox` branch stays on BM25 and the `cat` branch scores a flat `1`. Any [scorer](#scorers) may be named.

`::score(NULL)` excludes a subtree from scoring altogether. It still selects rows, contributes nothing to the score and requests no index features, so its postings are read without frequency or norms:

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM   docs_idx
WHERE  body @@ 'fox' OR (body @@ 'cat')::score(NULL);
```

Rows that match only through the unscored branch come back with `0`, and the rest score exactly as if that branch were not in the query. This is the equivalent of an Elasticsearch `filter` clause.

:::note Zero is a real score
An unscored branch makes `0` a legitimate score, which matters for `ORDER BY <scorer> DESC LIMIT k`. The top-k collector admits zero-scoring hits, so those rows are returned rather than dropped.
:::

A negation keeps every row except its matches, and each row it keeps scores what the query's scorer gives a row that matches every document: `0` for `BM25` in `NOT body @@ 'fox'`. A scorer inside the negation belongs to the matches it excludes, so `NOT body @@ 'fox'::score('constant(1)')` scores the same. A scorer given to the negation itself, as in `(NOT body @@ 'fox')::score('constant(2)')`, scores its constant on every row the negation keeps.

### Combining the two {#combining}

The modifiers compose in either order, and `::score` wraps the group while `::merge` applies to the node whose branches actually combine:

```sql
-- a constant score that stays constant: a document matching both
-- branches scores 42, not 84
WHERE (body @@ 'fox' OR body @@ 'cat')::score('constant(42)')::merge('max')
```

A per-branch scorer competes on its own terms under a group policy, so a `constant(10)` branch survives a `max` against a BM25 sibling. An unscored branch has nothing to compare, so the other branch wins by default.

## Explaining scores {#ts_explain}

**Signature.** `ts_explain(tableoid) -> VARIANT`.

`ts_explain` shows how a row got its score: which clauses of the query matched the row, what each one contributed and the inputs of the scorer's formula. It explains the scorer function the query uses on the same index, so the query needs one, such as `BM25(idx.tableoid)` in the select list or in `ORDER BY`:

<SqlLogicTest id="sql/functions/full_text_search/ts_explain" />

The explanation is a tree of objects, and clients receive it as `json`. Every object has a `value`; the other keys appear only where they apply:

| Key | Meaning |
| :--- | :--- |
| `value` | The object's contribution: the row's score at the top, a factor of an expression or a count below it. |
| `expression` | How `value` is computed from `inputs`, or `sum` / `max` for clauses that combine, following the query's [`::merge`](#merge) policy. |
| `inputs` | The values `expression` reads, keyed by name. |
| `clauses` | The clauses a `sum` or `max` combines. Optional clauses that the row does not match are left out. |
| `parts` | The terms of a phrase or an n-gram match: the idf of each term, which its `idf` adds up, or with `lm_jm`, `lm_dirichlet`, `indri_dirichlet` and `dfi`, the score of each term, which the clause adds up. |
| `column`, `term` | The column and the term a clause matched; `terms` lists several terms of one clause. |
| `phrase`, `slop` | A phrase's words in order: `null` for each word the phrase skips, `{"min": m, "max": n}` for a gap of `m` to `n` words, and an array for a position several words can fill, such as the words a prefix in the phrase expanded to. `slop` is the slop of a sloppy phrase. |
| `scorer` | The scorer function of a clause, such as `bm25(k1=1.2, b=0.75)`. |
| `scored` | `false` for a clause excluded from scoring with [`::score(NULL)`](#score-modifier): it matched and added nothing. |
| `note` | Why a clause scores `0`, or only its boost, without an expression; on an input, how it was counted, such as a `dl` of `1` on a column without the [`norm`](#requirements) flag. |

Read one part with dots and 1-based array indexes, and cast it where a plain value is needed:

<SqlLogicTest id="sql/functions/full_text_search/ts_explain_paths" />

The names in `inputs` are:

| Input | Meaning |
| :--- | :--- |
| `boost` | The query's weight for the clause, when it is not `1`. |
| `idf` | How rare the term is, from `df`, the number of rows containing it, and `N`, the number of rows with the column. |
| `tf` | How much the term's occurrences count, from `freq`, its occurrences in the row, the BM25 parameters `k1` and `b`, the row's length `dl` and the column's average length `avgdl`, computed as `L` / `N` from the column's total length `L`. |
| `norm` | TF-IDF's length normalization, `1 / sqrt(dl)`. |
| `p` | The term's share of all text in the column, `(ttf + 1) / (L + 1)`, from its occurrences `ttf` over all rows; with `mu` or `lambda`, the smoothing parameter of a language model. |
| `expected` | For DFI, the term's occurrences in the row if they were spread evenly, `p * dl`. |
| `value` | The score of `constant(value)`. |
| `count`, `scale` | For a sloppy phrase or an n-gram similarity match, `freq` is computed as `count * scale`: the matches in the row weighted by how well they match. A scorer without `freq` multiplies by `scale` directly. |
| `scale`, `score` | For an n-gram similarity match that needs every n-gram of the query the column contains, the clause scores `scale * score`: the scorer's `score`, weighted by `scale`, the share of the query's n-grams the column contains. |
| `constant` | In a `sum` or `max`, the score of clauses every row matches. |

- The top `value` is the row's score, exactly as the scorer function returns it for that row.
- Each word a prefix, pattern, range or fuzzy match expanded to is a clause of its own, with its `term`. A numeric range names only the `column`, as does a term in a column indexed without positions when another term of the query occurs in exactly the same rows, as often.
- `df`, `N` and the column's average length count the whole index, as the scorer does. A fuzzy match scores every word it matched with the same statistics, so its `df` is the most rows any one of those words occurs in.
- Each value is computed separately, so the values of `clauses` can differ from their total in the last digit.

`ts_explain` is computed for every row the scan returns and costs about as much as running the query again for that row, so request it together with a `LIMIT`. A query without a scorer function or a vector distance on the index raises an error.

**Elasticsearch's layout.** `ts_explain_json(explanation) -> JSON` turns a `ts_explain` result into the layout Elasticsearch and Lucene use, the one the [Elasticsearch-compatible API](../../../clients/elasticsearch-api.md#explaining-scores) returns: every node has a `value`, a `description` that says in words what the value is and how it was computed, and its `details`:

<SqlLogicTest id="sql/functions/full_text_search/ts_explain_json" />

The `details` of a node list its `clauses`, `parts` and `inputs`, with the inputs in the order of their names. A phrase is written as Lucene writes it, such as `body:"quick ? fox"~1`: `?` for a skipped word, `?{m,n}` for a gap of `m` to `n` words, `(bread brown)` for a position several words can fill and `~` with the slop of a sloppy phrase.

### Vector distances {#ts_explain-vector}

A query that ranks or filters by a distance to an indexed [vector column](../../indexes/inverted/vector-search.md) explains that distance: the function and its formula, how the row was found, and the parameters of the search:

<SqlLogicTest id="sql/indexes/inverted/vector-search/ts_explain" />

The top `value` is the distance the query returned for the row. `scorer` names the distance function, `expression` computes it from the column and the query vector, and `note` says how the row was found, such as `found by an ivf search over sq8 codes`. Its `inputs` are:

| Input | Meaning |
| :--- | :--- |
| `exact` | The distance computed from the row's stored vector. A search over quantized codes can return a different value when the row was not re-scored. Shown for an `ivf` or `hnsw` search. |
| `dims` | The number of dimensions of the vectors. |
| `k` | How many nearest rows the search returns, for a query with `ORDER BY` and `LIMIT`. |
| `radius` | The distance the search compares with, for a range search. |
| `nprobe` | The clusters an `ivf` search scans in each segment, when [`sdb_ivf_search_nprobe`](../../indexes/inverted/maintenance.md#session-settings) sets them. |
| `ef_search` | The candidates an `hnsw` search keeps, from [`sdb_hnsw_ef_search`](../../indexes/inverted/maintenance.md#session-settings) or its default. |
| `rescore_width` | For a quantized index, how far a candidate's code score may be off and still be read again at full precision: [`sdb_ann_rescore_width`](../../indexes/inverted/maintenance.md#session-settings) or the quantizer's default. |

A distance computed from every row's stored vector, without `ORDER BY ... LIMIT` and without a radius, says so in its `note` and has only `dims`. A row whose vector is `NULL` gets a `null` value and a `note`.

A query that uses both a scorer function and a vector distance on the same index explains the scorer's score; the distance is then computed exactly from each row's stored vector, without a vector search. To explain both, compute each in its own subquery, as [reciprocal rank fusion](../../../cookbook/search/reciprocal-rank-fusion.md) does.

## Top-K and WAND pruning {#top-k}

The common shape `ORDER BY <scorer>(idx.tableoid) DESC LIMIT k` returns the best `k` matches. Creating the table with the [`optimize_top_k` table option](../../statements/create_table/index.md#table-options) enables **WAND** pruning, which skips candidates that provably cannot reach the top `k`:

```sql
CREATE TABLE docs (
    id INTEGER PRIMARY KEY,
    body VARCHAR
) WITH (optimize_top_k = 'bm25(1.2, 0.75)');

CREATE INDEX docs_idx ON docs
    USING inverted (id, body scored_en);
```

Pruning engages only when the `ORDER BY` scorer can use the score bounds stored for the `optimize_top_k` scorer and the filter is a single term, an `OR` of terms that any one term satisfies, an `AND` of terms or of such `OR`s (the latter when enough rows match), terms with `AND NOT` exclusions, one required term with optional scoring terms (`+fox quick`), or a phrase without slop. Such an `OR` may also hold [pattern matches](#patterns) that score their constant, the default, as in `'climate'::tsquery || ts_starts_with('clim')`. An `OR` prunes only when the scores of its terms add up: one that keeps the best score, such as a fuzzy match that expands to several terms, a [pattern match](#patterns) given a scorer, or [`::merge('max')`](#merge), does not. Otherwise the query still runs correctly, just without the optimization. `EXPLAIN` shows `Top: k, optimized` on the scan when the scorer qualifies, whatever the filter. See [Relevance ranking → Top-K queries and WAND pruning](../../indexes/inverted/ranking.md#top-k-queries-and-wand-pruning) for the full conditions, including which scorers share score bounds.

## Cross-engine notes {#cross-engine-notes}

If you are coming from Elasticsearch or OpenSearch, here is how their relevance-tuning concepts map onto SereneDB (the left column links to the Elasticsearch reference):

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| `_score` (implicit relevance) | any scorer over `tableoid`, e.g. `BM25(idx.tableoid)` |
| [`boosting`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html) query / per-clause `"^2"` | the `^` operator: `ts_phrase('fox') ^ 2.0` ([Boosting](#boosting)) |
| [`constant_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html) | `raw_boost(idx.tableoid)` returns `1` for every match when no `^` boost is applied; or `ORDER BY` a literal |
| [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) `weight` | fold the scorer into an arithmetic expression, e.g. `BM25(idx.tableoid) * 2` |
| [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) `field_value_factor` | blend the scorer with a column in the `SELECT` / `ORDER BY` expression |
| [`dis_max`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html) | `::merge('max')` on the disjunction ([Per-node score control](#merge)) |
| `bool` [`filter`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) clause | `::score(NULL)` on the clause ([Per-node score control](#score-modifier)) |
| Top-K acceleration | `optimize_top_k` + WAND pruning ([Top-K](#top-k)) |
| Tie-breaking | extra `ORDER BY` columns, typically the primary key |
| [Reciprocal Rank Fusion](https://www.elastic.co/guide/en/elasticsearch/reference/current/rrf.html) | [Reciprocal Rank Fusion](../../../cookbook/search/reciprocal-rank-fusion.md) |

SereneDB has no single `function_score`-style query type. Because a scorer is just a `FLOAT`-valued expression, you compose the same effects directly in SQL — multiply, add, threshold in `WHERE`, or blend with table columns — rather than through a dedicated DSL.

## See also

- [Full-Text Search Functions](./full-text.md) — operators, constructors, parsers
- [Inverted Index](../../indexes/inverted/index.md) · [Relevance ranking guide](../../indexes/inverted/ranking.md)
- [Text Analysis](../../indexes/inverted/text-analysis.md#token-positions-and-feature-flags) — the `frequency` and `norm` flags scorers read
- [BM25/TFIDF Ranking recipe](../../../cookbook/search/ranking.md) — parameter tuning
