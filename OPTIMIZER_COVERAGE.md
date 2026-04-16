## Optimizer coverage

What the rule-driven planner produces today, by query shape. For each
pattern: the strategy name visible in EXPLAIN, the claim info attached
to bind_data, and the rule / path that handles it.

Two rules run (in this order, registered in
[server/connector/duckdb_storage_extension.cpp](server/connector/duckdb_storage_extension.cpp)):

1. `iresearch_plan` — detects iresearch-only patterns first because
   they can't be served by the rocksdb layer (`sdb_phrase`,
   distance, scoring, offsets, …). Skipped under
   `LOGICAL_DELETE` / `LOGICAL_UPDATE` / `LOGICAL_MERGE_INTO` subtrees
   (eventual-consistency guard).
2. `rocksdb_plan` — runs on any scan the iresearch rule didn't claim.
   Picks the best candidate across PK and all secondary indexes.

The rules only produce the **scan strategy + bind data**; execution
is still the full-table stub for every specialised strategy. The
distinct strategy names and claim-summary output let EXPLAIN show
what the rules decided, which is the optimisation-stage contract.

Schema used in every example below:

```sql
CREATE TEXT SEARCH DICTIONARY test_english(
    template = 'text', locale = 'en_US.UTF-8', case = 'none',
    stemming = false, accent = false, frequency = true, position = true
);

-- Single-col PK + secondary indexes
CREATE TABLE t1 (pk INT PRIMARY KEY, x INT, y INT, z INT);
CREATE INDEX sk_x  ON t1(x);
CREATE INDEX sk_xy ON t1(x, y);

-- Composite PK
CREATE TABLE t2 (a INT, b INT, v INT, PRIMARY KEY (a, b));

-- Inverted index with text + HNSW vector
CREATE TABLE docs (id INT PRIMARY KEY, body TEXT, title TEXT, emb FLOAT[2]);
CREATE INDEX iv ON docs USING inverted(body test_english, title test_english, emb hnsw);
```

---

### rocksdb_plan — primary key

| SQL | Strategy | Claim |
|---|---|---|
| `WHERE pk = 2` | `pk_point` | `Filter: (pk=2)` |
| `WHERE pk IN (1, 2, 3)` | `pk_point` | `Filter: (pk=1), (pk=2), (pk=3)` |
| `WHERE pk < 3` | `pk_range` | `Filter: {pk=(-inf, 3)}` |
| `WHERE pk BETWEEN 1 AND 2` | `pk_range` | `Filter: {pk=[1, 2]}` |

Composite PK (`PRIMARY KEY (a, b)`):

| SQL | Strategy | Claim |
|---|---|---|
| `WHERE a = 1 AND b = 2` | `pk_point` | `Filter: (a=1, b=2)` |
| `WHERE a = 1` | `pk_range` | `Filter: {a=1}` (prefix-equality, trailing range open) |
| `WHERE a = 1 AND b < 2` | `pk_range` | `Filter: {a=1, b=(-inf, 2)}` |

Extraction goes through [rocksdb_filter.cpp](server/connector/rocksdb_filter.cpp)
→ `ExtractAndRewriteFilterExpr` → sorted `ResolvedPoint` / disjoint
`ResolvedRange` list.

### rocksdb_plan — secondary key

`t1` has `sk_x(x)` and `sk_xy(x, y)`:

| SQL | Strategy | Claim | Chose |
|---|---|---|---|
| `WHERE x = 10` | `sk_point` | `Filter: (x=10)` | `sk_x` (single-col cover) |
| `WHERE x = 10 AND y = 100` | `sk_point` | `Filter: (x=10, y=100)` | `sk_xy` (composite cover; fewer remaining filters than `sk_x`) |
| `WHERE x < 20` | `sk_range` | `Filter: {x=(-inf, 20)}` | `sk_x` |
| `WHERE x = 10 OR x = 20` | `sk_point` | (OR merged into two points via sweep) | `sk_x` |

### rocksdb_plan — cost-based selection

When PK and one-or-more SKs can both be claimed, the rule picks:

1. `Points > Ranges` (point lookups are cheaper).
2. Fewer remaining-filter leaves wins (more predicates claimed).
3. PK beats SK on a tie (PK drives the scan without an extra MultiGet).

| SQL | Strategy | Claim |
|---|---|---|
| `WHERE pk = 2 AND x = 10` | `pk_point` (PK wins on tiebreak) | `Filter: (pk=2)` |
| `WHERE y = 100` (no index on y) | `full_table` | – |

### rocksdb_plan — `FROM index_name`

`SereneDBIndexScanEntry` routes `FROM sk_index` into a dedicated
default (`CreateFullSkScanFunction`) that the rocksdb rule can
specialise:

| SQL | Strategy |
|---|---|
| `SELECT * FROM sk_x` | `SERENEDB_FULL_SK`, `Strategy: (empty — SecondaryIndexScan)` |
| `SELECT * FROM sk_x WHERE x = 10` | `sk_point`, `Filter: (x=10)` |
| `SELECT * FROM sk_x WHERE pk = 1` | `SERENEDB_FULL_SK` (rule won't fire — `pk` isn't part of `sk_x`'s columns, so the designated-index path has no claim) |

---

### iresearch_plan — boolean filter (case 1 / 2)

Built via [search_filter_builder.cpp](server/connector/search_filter_builder.cpp)
with per-expression partial claim + `irs::And::PopBack()` rollback.
Filter tree rendered for EXPLAIN through
[search_filter_printer.cpp](server/connector/search_filter_printer.cpp)'s
`irs::ToStringDemangled` (column ids → names).

| SQL | Strategy | Claim |
|---|---|---|
| `WHERE sdb_phrase(body, 'pudge')` | `iresearch_search` | `Filter: AND[PHRASE[body(string) = <Term:pudge(0, 0); >]]` |
| `WHERE sdb_phrase(body, 'pudge') AND id > 0` | `iresearch_search` | iresearch claims phrase; `(id > 0)` stays on `LogicalFilter` above the scan |
| `WHERE sdb_term_eq(body, 'pudge')` | `iresearch_search` | AND[TERM[…]] |

### iresearch_plan — scoring (case 1 / 2 + scorer)

`sdb_score(tableoid)` in a projection → `SearchScan.scorer` +
`scorer_description = "bm25(k1=1.2, b=0.75)"`. A `LIMIT k` (or
`LogicalTopN` with descending order) upstream → `score_top_k = k`:

| SQL | Strategy | Claim |
|---|---|---|
| `SELECT id, sdb_score(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge')` | `fulltext_score` | `Score: bm25(k1=1.2, b=0.75)` |
| `SELECT id, sdb_score(tableoid) FROM docs WHERE sdb_phrase(body, 'pudge') LIMIT 5` | `fulltext_topk` | `Score: bm25(k1=1.2, b=0.75)`, `TopK: 5` |

### iresearch_plan — offsets (case 1 / 2 + offsets)

`sdb_offsets(col)` in a projection — single column-ref argument, the
rule identifies the scan via the ref's `binding.table_index`:

| SQL | Strategy | Claim |
|---|---|---|
| `SELECT id, sdb_offsets(body) FROM docs WHERE sdb_phrase(body, 'pudge')` | `iresearch_search+offsets` | `Offsets: body` |
| `SELECT id, sdb_offsets(body), sdb_offsets(title) FROM docs WHERE sdb_phrase(body, 'pudge')` | `iresearch_search+offsets` | `Offsets: body, title` (dedup per column id) |

### iresearch_plan — all the add-ons together

```
SELECT id, sdb_score(tableoid), sdb_offsets(body), sdb_offsets(title)
FROM   docs
WHERE  sdb_phrase(body, 'pudge')
LIMIT  3
```

Produces:

```
Strategy: fulltext_topk+offsets
Filter:   AND[PHRASE[body(string) = <Term:pudge(0, 0); >]]
Score:    bm25(k1=1.2, b=0.75)
TopK:     3
Offsets:  body, title
```

### iresearch_plan — ANN (case 3 / 4)

Vector-distance scans are standalone (not layered on the boolean
filter). Vector column must be declared with the `hnsw` op-class in
`CREATE INDEX ... USING inverted(..., vec hnsw)`.

| SQL | Strategy | Claim |
|---|---|---|
| `ORDER BY sdb_l2_distance(emb, [...]) LIMIT k` (over table or index) | `ann_topk` | `TopK: k`, `Dims: N` |
| `WHERE sdb_l2_distance(emb, [...]) < r` | `ann_range` | `Radius: r`, `Dims: N` |

`FROM i` against an HNSW-covered inverted index routes through
`SereneDBIndexScanEntry` just like the text-search case.

### iresearch_plan — mutation guard

Under `DELETE` / `UPDATE` / `MERGE INTO`, iresearch never claims — the
inverted index is eventually consistent, so DML must read straight
from the rocksdb layer:

```
DELETE FROM docs WHERE sdb_phrase(body, 'pudge')
```

produces `FILTER + SERENEDB_SCAN(full_table)` (rocksdb-side pushdown
still applies; iresearch sits out).

---

## Interaction between the two rules

- `iresearch_plan` runs first and only claims when iresearch-specific
  predicates are present (`sdb_phrase`, `sdb_term_eq`,
  `sdb_l2_distance`, `sdb_score`, `sdb_offsets`, …).
- `rocksdb_plan` runs second and only considers scans whose
  `scan_source` is still `FullTableScan` or `SecondaryIndexScan`.
  Iresearch-claimed scans (variants `SearchScan` / `ANNScan` /
  `RangeSearchScan`) are skipped.
- Mixed queries partial-claim both sides: iresearch takes what it can,
  rocksdb takes what's left. Since the iresearch rule visits
  `LogicalFilter` (not the Get), surviving predicates stay on the
  filter and the rocksdb rule may still fire on the Get if the scan
  was already flipped away from `FullTableScan`… no, the rocksdb rule
  only runs on `FullTableScan` / `SecondaryIndexScan` scans, so once
  iresearch flips to `SearchScan`, rocksdb leaves the scan alone and
  any rocksdb-side predicates stay on the surrounding
  `LogicalFilter` for DuckDB to evaluate after the scan.

---

## EXPLAIN strategy → function name map

The function name in EXPLAIN's top-left reflects which specialised
scan the rule swapped in:

| strategy | function name |
|---|---|
| `full_table` | `SERENEDB_SCAN` |
| `pk_point` | `SERENEDB_PK_POINT` |
| `pk_range` | `SERENEDB_PK_RANGE` |
| `sk_point` | `SERENEDB_SK_POINT` |
| `sk_range` | `SERENEDB_SK_RANGE` |
| `secondary_index` (FROM sk_idx) | `SERENEDB_FULL_SK` |
| `iresearch_search` (+`+offsets`) | `SERENEDB_IRESEARCH_SEARCH` |
| `fulltext_score` (+`+offsets`) | `SERENEDB_IRESEARCH_SEARCH` |
| `fulltext_topk` (+`+offsets`) | `SERENEDB_IRESEARCH_SEARCH` |
| `ann_topk` | `SERENEDB_ANN_TOPK` |
| `ann_range` | `SERENEDB_ANN_RANGE` |
| (inverted default) | `SERENEDB_FULL_IRESEARCH` |

Scoring / offsets / topk variants share one function
(`CreateIresearchSearchScanFunction`) and differ by bind_data flags
that `ScanSourceKindName` dispatches on.

---

## Safety invariants

- **Rules never drop predicates the runtime can't yet enforce.** The
  specialised scans (`pk_point`, `pk_range`, `sk_point`, `sk_range`)
  still dispatch to the full-table loop at runtime, so the rule
  keeps the matching `LogicalFilter` expressions in place — otherwise
  DML (`UPDATE`, `DELETE`) would mis-affect rows. Removing those
  predicates will happen as part of Phase 5f / Phase 9 once the
  executors land.
- **iresearch DOES drop claimed predicates.** The SearchScan runtime
  is already correct for `sdb_phrase` / `sdb_term_eq`, so the
  iresearch rule removes matched expressions from the enclosing
  `LogicalFilter` (and drops the filter node if it becomes empty).
  ANN topk drops the `LogicalTopN`; ANN range drops the matched
  distance comparison.
- **Mutation subtrees (`DELETE` / `UPDATE` / `MERGE INTO`) never get
  iresearch-rewritten.** Rocksdb rules still apply in those subtrees.
