# Test classification -- group by type, decide what runs when

Goal: a **simple, coarse** scheme. Group tests by type, give each type a run
policy (PR / nightly / manual), and gate a *few* expensive types on a coarse
change-category derived from a **validated** dependency graph. Deliberately
**not** precise: no separate docs-only / cmake-only / connector-only cases. The
one worthwhile split is **iresearch** (a large, isolatable suite).

---

## 1. Validated dependency graph

Two views matter, and they disagree -- which is the whole lesson:

- **Link graph** (`cmake --graphviz`) shows *link* edges between targets. Useful
  for the third_party / duckdb / iresearch layering.
- **Include graph** (`#include` of headers) shows *compile/rebuild* deps. This is
  what actually decides "a change to X forces Y to recompile" -- and inside
  `server/` it is **dense and cyclic**, which the link graph hides because those
  subsystems compile into shared targets (no link edge needed).

`A -> B` = A depends on B (B upstream; changing B rebuilds A).

```
third_party leaves  (abseil, boost, folly, faiss, re2, lz4, icu, s2, yaclib,
                     openssl, zlib, simdjson, nghttp2, llhttp, immer, date, fmt ...)
       ▲
  duckdb_static   ── includes ALL duckdb extensions linked in:
       ▲              postgres_scanner, httpfs, iceberg, avro, parquet, json,
       │              tpch, tpcds, core_functions, autocomplete
       ├────────────────────────────┐
 iresearch-static              serene-base libs  (serene_base, serene, serene_geo,
 (-> duckdb_static,                  ▲             fuerte, build_id; + vpack*, rocksdb*)
  serene_geo, text,                 │
  stemmer, streamvbyte...)            │
       ▲                            │
       └────────────┬───────────────┘
                    ▲
        ┌──────────────────────────────────────────────────┐
        │  server/  -- ONE densely-coupled unit               │
        │  connector, catalog, pg, storage_engine, query,    │
        │  search, database, network, auth, metrics, utils.  │
        │  They #include each other freely (e.g. connector   │
        │  pulls pg/, catalog/, storage_engine/, search/,    │
        │  query/). No clean internal layering -- treat as a  │
        │  blob. (compiles into serene_connector/sereneserver)│
        └──────────────────────────────────────────────────┘
                    ▲
        ┌───────────┼───────────────────────┐
   serened     serenedb-tests_basics    serenedb-tests_connector
  (+duckdb_shell)  (-> full server)       (-> full server)

  isolated leaf test bins (do NOT pull the server):
    iresearch-tests -> iresearch-static (and thus duckdb_static)
    fuerte-tests    -> fuerte
```

`*` = being dropped (rocksdb, vpack) -- those edges disappear soon; see
`project_components_being_dropped`.

### Corrections vs the remembered graph

1. **No separate "duckdb-modules" layer.** The duckdb extensions
   (postgres_scanner, httpfs, iceberg, avro, parquet, json...) are statically
   linked **into `duckdb_static`**. Duckdb + its modules are one compilation
   unit; the "duckdb <=> duckdb-modules" question resolves to "treat as one."
2. **iresearch depends on duckdb_static** (confirmed -- the LogicalType work).
   So duckdb is upstream of iresearch; a duckdb bump invalidates iresearch too.
   You cannot test iresearch in isolation from a duckdb change.
3. **`server/` is one densely-coupled blob -- do NOT sub-layer it.** The link
   graph *looks* like `connector -> ... -> sereneserver`, but that is an artifact
   of link edges. By **includes**, `server/connector` depends on the rest of
   server: `pg/` (~100 includes: errcodes, connection_context, sql_exception),
   `catalog/` (~150: catalog, table, inverted_index, mangling...),
   `storage_engine/`, `search/`, `query/`. Connector *is* server code and
   depends on server code. There is no clean "connector is a layer below the
   rest of server" -- that earlier claim was wrong (it trusted the link graph).
   -> This **validates the coarse classifier**: any `server/*` change invalidates
   all of server, so there is no point separating connector / pg / catalog /
   storage as categories.
4. **The connector is still the most valuable *test target*** (it's where
   duckdb<->iresearch integration bugs live) -- but that's about where to *aim*
   tests, not a build layer we can isolate.
5. **The connector/basics gtest binaries link the full server**
   (`serene_tests_{connector,basics}_deps -> sereneserver`). They are *not* cheap
   leaf units -- they rebuild on almost any change. Only **`iresearch-tests`** and
   **`fuerte-tests`** are genuinely isolated.
6. The dependable chain is therefore:
   `third_party -> duckdb(+modules) -> {iresearch, serene-base libs} ->
   server-blob (incl. connector) -> serened`.

> **Method note:** the link graph alone is misleading for rebuild/test scoping.
> When re-validating, check **includes** for cross-`server/` coupling, not just
> `cmake --graphviz` link edges.

---

## 2. Test types (catalog)

| # | Type | Binary / runner | Covers | Linked to (what invalidates it) |
|---|---|---|---|---|
| 1 | gtest iresearch | `iresearch-tests` | iresearch storage/search lib | iresearch, duckdb |
| 2 | gtest basics | `serenedb-tests_basics` | libs base utils | full `sereneserver` (≈ any change) |
| 3 | gtest connector | `serenedb-tests_connector` | DuckDB<->iresearch connector | connector, server, iresearch, duckdb |
| 4 | gtest fuerte | `fuerte-tests` | fuerte transport | fuerte only (isolated) |
| 5 | sqllogic -- ours | `run.sh` (any/sdb/pg) | end-to-end DB behaviour on our surface | serened (≈ any change) |
| 6 | sqllogic -- sqlite | `run.sh` (any/pg/sqlite) | DuckDB SQL semantics | serened; low novel value |
| 7 | recovery | `run_recovery_tests.sh` | crash/fault-injection + restart | serened |
| 8 | drivers | `tests/drivers/run.sh` | PG wire across 11 langs + sqlsmith | serened (pg layer) |
| 9 | extension tests | duckdb `unittest` | postgres_scanner/avro/httpfs/iceberg | the matching extension submodule |
| 10 | iresearch-load | `iresearch-load-tests` | search load (postings/score/WAND/MaxScore) | iresearch |
| 11 | microbench | `serenedb-bench-micro-*` | scoped perf | the function under test |

(vpack-tests, vpack_fuzzer, rocksdb -- dropped, omitted.)

---

## 3. Change categories (coarse classifier)

Path-based, computed at trigger time. Kept deliberately small. Superset rules
apply (an upstream change implies the downstream ones).

| Category | Path trigger | Implies |
|---|---|---|
| **duckdb** | `third_party/duckdb` submodule | iresearch + extension + core (everything) |
| **extension** | `third_party/duckdb_{postgres,avro,httpfs,iceberg}` | core (+ rebuilds duckdb_static) |
| **iresearch** | `libs/iresearch`, `third_party/iresearch.build` | core |
| **core** (default) | anything else (server/connector/libs/general) | -- |

Notes:
- We do **not** separate docs-only / cmake-only / connector-only -- they all land
  in **core** and run the default set. Cheap enough; the human decides whether to
  even trigger a docs PR.
- Because `iresearch -> duckdb`, the **iresearch** test set must also fire when
  **duckdb** changed (superset).
- Everything ultimately rebuilds `serened`, so the *integration* tests can't be
  skipped by category -- the categories only gate the **extra** heavy suites
  (iresearch-tests, searchbench, extension tests).

---

## 4. Run policy -- what runs when

| Test type | PR (triggered, fast tier) | Nightly | Manual mark |
|---|---|---|---|
| gtest iresearch (1) | **only if `iresearch`/`duckdb`** | always | -- |
| gtest basics (2) | always | always | -- |
| gtest connector (3) | always | always | -- |
| gtest fuerte (4) | always (cheap, isolated) | always | -- |
| sqllogic ours (5) | always | always | -- |
| sqllogic sqlite (6) | **never** (moved out) | always | -- |
| recovery (7) | smoke subset | full | -- |
| drivers (8) | smoke (one psycopg path) | full + sqlsmith | -- |
| extension (9) | **only if `extension`/`duckdb`** | always | -- |
| searchbench (10) | no | yes (also feeds perf) | -- |
| microbench (11) | no | no | perf runs |

Plus: the PR fast tier runs **our** suites under a sanitizer where the budget
allows (see plan §P0.3), affordable precisely because sqlite (6) left the PR set.

---

## 5. Implementation notes

- Classifier: `dorny/paths-filter` (or a small script) emitting the four
  booleans in §3, consumed by `build-manual.yml` / `build-reusable.yml` to
  toggle the gated suites.
- Keep the regeneration command handy to re-validate the graph after structural
  changes: `cmake --graphviz=graph.dot build/` then inspect edges for
  `serened / serene_connector / iresearch-static / duckdb_static`.
- Re-validate when rocksdb/vpack are removed (edges 1,3 lose those deps) and if
  `libs/iresearch` is unified/renamed.
