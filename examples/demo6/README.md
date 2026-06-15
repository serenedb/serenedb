# demo6 -- Code search + statistics over self-sufficient indexes

Search ~45k accepted solutions like `grep`, rank ~15k task statements and
editorials with BM25 and compute per-problem runtime/memory statistics --
all inside SereneDB, with **no tables**:
the corpora are views over Hugging Face parquet, and after the index build no
query touches the parquet again.

## The pattern

```
read_parquet(source A) --> view A (reshape: renames, casts, CASE, ids)  \
read_parquet(source B) --> view B (a completely different schema)        +--> UNION ALL mega view
read_json_auto(...)    --> view C (works for JSON too)                  /          |
                                                                  CREATE INDEX ... USING inverted(...)
                                                                  INCLUDE (every retrieved column)
```

Each source keeps its own reshaping SQL; the mega view glues them into one
relation; the inverted index lists every column as a key or INCLUDE (analyzed
columns appear in both lists). The result is **self-sufficient**: search,
retrieval and aggregation are all pure index reads -- delete the network and
the queries below still run.

Two corpora are built this way (see `bootstrap.sql`):

| Index | Sources | Rows |
|---|---|---|
| `tasks_idx` | [open-r1/codeforces](https://huggingface.co/datasets/open-r1/codeforces) (CC-BY-4.0) + [deepmind/code_contests](https://huggingface.co/datasets/deepmind/code_contests) (CC-BY-4.0, non-Codeforces judges) | ~15k problems |
| `solutions_idx` | [open-r1/codeforces-submissions](https://huggingface.co/datasets/open-r1/codeforces-submissions) `selected_accepted` (CC-BY-4.0) | ~45k accepted submissions with judge time/memory |

Two details that matter at scale:

* **Natural ids.** The solutions view uses `submission_id` as the id instead
  of `row_number() OVER ()` -- a global window function would serialize the
  whole pipeline into one thread, while a window-free view feeds the parallel
  index sink on all cores (17x on an 11.5M-row build).
* **Measurements as INCLUDE columns.** `time_ms` and `memory_kb` ride along
  in the index, so statistics are index-only aggregations.

## The tokenizer

`template = 'sparse_ngram'` implements the n-gram selection scheme used by
GitHub code search ([danlark1/sparse_ngrams](https://github.com/danlark1/sparse_ngrams)).
A monotonic stack over bigram hashes selects a sparse set of variable-length
ngrams (3 bytes and up):

| Mode | Used at | Emits | Guarantee |
|---|---|---|---|
| `covering = false` | index time | <= 2n-2 grams per document | every substring of the document is covered |
| `covering = true` | query time | <= n-2 grams per query | all emitted grams are present in the index grams of any string containing the query |

So substring search is a *conjunction*: tokenize the query with the covering
dictionary and require every gram to match --

```sql
SELECT count(*)
FROM solutions_idx
WHERE code @@ ts_all(ts_tokenize(ARRAY['sys.setrecursionlimit('], 'code_grams_q'))
  AND code LIKE '%sys.setrecursionlimit(%';   -- exactness post-filter
```

The index does the heavy lifting; `LIKE` verifies the few candidates. Swap
`ts_all` for `ts_any(..., k)` and the same machinery does fuzzy
shape-of-the-code search.

## Run

```bash
# one-time: dictionaries, per-source views, mega views, both indexes
psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

# the demo queries
psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
```

## The queries

1. **Substring grep** over all solutions, index-accelerated and exact.
2. **Code idiom -> tasks**: problems solved with `lru_cache`, joining the two
   indexes.
3. **BM25 over statements** across both task sources: "shortest path", ranked.
4. **Fuzzy code search**: any 3 covering grams of a canonical binary-search
   loop; BM25 ranks nearer shapes higher.
5. **Per-language statistics of one problem** -- count / best / median time,
   memory, shortest solution -- aggregated from INCLUDE columns only.
6. **Leaderboard**: fastest accepted solutions of a problem.
7. **Corpus-wide statistics**: problems with the highest median runtime.

Plus a baseline at the end: the same predicate as Q1 as a postings-free scan
over the stored column, to show what the gram conjunction saves.
