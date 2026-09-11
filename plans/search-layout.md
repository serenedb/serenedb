# `search/` — file and namespace structure

Scope: placement and names only. Deduplication is a separate discussion, after
this lands.

## 1. The model

Five kinds of thing live in `search/`.

**A filter** is what a caller builds. Declarative, segment-independent.
Rewriting one filter into a cheaper equivalent is part of the same job, so the
optimizer belongs with them.

**A query** is a filter prepared against one segment: terms resolved to
cookies, stats collected, estimates known. `term_query` and its `term_state`
are one thing at two moments. A `QueryBuilder` is what produces a query, so it
is a query-side type wherever it is written today.

**A root API** is what the caller asked for, and it changes the algorithm:
how many match, which documents, which with their scores, the best k, where in
the document.

**A node API** is how one clause is consulted by the expression above it:
`lead` (give me your next document), `probe` (do you have this one), `fill`
(fill me a window of yours). A clause never knows which root is asking.

**A scorer** turns a match into a number.

Everything else is **detail** — and `detail` means exactly *common to
`search/`*, nothing narrower. Reading a posting list, folding a window,
evaluating a phrase's positions, pricing a plan. It is the largest directory
because that is where the engine is.

## 2. Rules

> Code goes in the API that owns the **concept**, not the API that calls it.
> If more than one API could ask for it, it is `detail`.

Conceptual, not an include graph — an include graph of a wrong layout only
re-derives the wrong layout. Evaluating a phrase is `detail`, because `count`
needs to know a phrase matched and the offsets root needs to know where; both
ask the same machinery. Producing the offsets *answer* is that root's.

> There is exactly one `detail`, and it is `irs::detail`. No
> `irs::<api>::detail`.

> A directory is a namespace. Plural where it holds a family (`filters`,
> `queries`, `scorers`), singular where it is one concept (`detail`, and each
> API, which is named for its protocol).

## 3. Target tree

```
search/
  filters/     the filters, the Filter/QueryBuilder abstraction, and the optimizer
  queries/     the prepared form: each query with its state
  scorers/     the scorers, and the machinery that runs them

  count/ docs/ hits/ top/ offsets/        root APIs   irs::count ...
  lead/ probe/ fill/                      node APIs   irs::lead ...

  detail/      common to search/, flat                irs::detail
```

Nothing stays directly in `search/`.

## 4. The two names you asked about

**`scored/` → `hits/`.** The five roots answer *what do you want back*, and
they should all be nouns: a count, documents, hits, the top, offsets.
`scored` is the odd one — an adjective, and it says how the answer was made
rather than what it is. A *hit* is a matched document with its score, which is
exactly this root's output and nothing else's; `docs` is the same stream
without scores. That reads as a pair: `irs::docs` yields ids, `irs::hits`
yields ids and scores, `irs::top` yields the best hits. It also removes the
`scored/` versus `scores/` one-letter trap for free.

`docs/` I would keep. It is already a noun and already precise.

**`scores/` → `scorers/`.** It holds bm25, tfidf and friends, plus the code
that runs them. `scorers` names the family, which matches `filters/` and
`queries/`. `score/` would be singular for a directory holding fourteen of
them.

**`Advance` → `Next`.** 30 declarations, 1,851 occurrences across libs, server
and tests. Mechanical and compiler-checked.

## 5. What moves

### 5.1 Undo the `offsets/` mistake

Eight phrase and ngram headers I moved into `offsets/` are phrase and ngram
*evaluation* — slots that walk positions and decide adjacency, and the
builders that assemble them. Every API evaluates phrases; only one reports
where. Back to `detail/`: `phrase_of`, `ngram_of`, `wildcard_ngram_of`,
`phrase_fixed_slots`, `phrase_variadic_slots`, `phrase_variadic_pos`,
`ngram_slots`, `ngram_all_slots`.

`slop_phrase.hpp`, `ngram_matcher.hpp`, `phrase_iterator.hpp`,
`detail/conjunction_leaves.hpp` and `detail/node_of.hpp` are the same kind of
thing and are `detail/` too.

### 5.2 `top/detail/` → `irs::top`

Five files under `irs::top::detail`. There is one `detail` and this is not it.

### 5.3 `detail/score/` → `scorers/`

Four headers and four TUs that run a scorer over a window, a conjunction, a
probe. They belong with the scorers, along with `score_function.{hpp,cpp}`,
and `score_args` / `score_policy` / `score_provider` / `scored_context` /
`all_docs_score` if reading each agrees it is scoring rather than planning.

### 5.4 The 80 loose root files

- 34 `*_filter.*` → `filters/`, with `filter.{hpp,cpp}` and the 9 files of
  `optimizer/` (2,000 lines) joining them.
- 13 `*_query.*` and the 5 `states/` files → `queries/`, each state beside its
  query. `states/` disappears.
- `QueryBuilder` subclasses currently written inside filter files —
  `nested_filter.cpp`, `all_filter.hpp`, `wildcard_ngram_filter.hpp` — are
  extracted into their own files under `queries/`.
- `score_function.*` → `scorers/`.
- The rest — collectors, arenas, heaps, estimates, term iteration, visitors —
  is common to `search/`, so it is `detail/`, each placed by what it is.

### 5.5 Namespaces

`scorers/` and the former `states/` declare bare `irs` today; three files
declare bare `scored`, six declare bare `detail`. Each takes its directory's
namespace.

## 6. Order

1. Undo the `offsets/` move; place the five offsets-adjacent files in
   `detail/`.
2. `top/detail/` → `irs::top`.
3. `detail/score/` and `score_function` → `scorers/`; `scores/` → `scorers/`.
4. `filters/` (with the optimizer), `queries/` (with the states and the
   extracted QueryBuilders).
5. The remaining root files into `detail/`.
6. Namespaces aligned to directories.
7. `scored/` → `hits/`.
8. `Advance` → `Next`.

Each step is one commit. Every step changes no generated code, so the gate is
five parity sets byte-identical over 1,573 queries, with instructions as a
spot check.

## 7. Not in this plan

- **Deduplication** — roughly 2,700 lines across the boolean families and the
  lead/probe/fill triplets. Discussed after this structure is right.
- **Class names** beyond `Advance`.
- **The pruned roots**, Part 1.5. **Min-match pruning**, which stays deleted.
- **The cost models**, which move directory and are not otherwise touched.

## 8. What went wrong before

I executed the issue's bullets instead of thinking about what the code is.
That put eight shared headers into another API's directory and renamed
`common/` to `detail/` without changing what the name means. Then I tried to
justify placement by measuring the include graph, which only re-derives the
mistakes already in the tree.
