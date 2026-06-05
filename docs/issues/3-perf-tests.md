# Issue 3 -- Performance tests: where to start, targets, how & when

**Scope:** per-change perf comparison with history, ClickHouse-grade signal at
DuckDB-grade effort. **Decided:** runs **manual-mark only**, on a **dedicated
cloud instance**, history **dogfooded into SereneDB**.

---

## Current state
- ~20 Google-Benchmark microbenches in `tests/bench/micro` -- **never run in CI**.
- searchbench (wiki_small) runs but is **not tracked** across commits.
- No baseline, no regression detection, no flamegraphs, no history.

## Where to start (smallest useful thing first)
1. **Wire the existing microbench suite into a base-vs-PR comparison** (DuckDB's
   `scripts/regression/test_runner.py` model): build merge-base + PR in `bench`
   preset, run each bench **5x**, geomean, thresholds **10% + absolute floor**,
   post a step-summary/PR-comment table. This is mostly porting a known script.
2. Add **searchbench as the `search-query` group** (postings iteration / score
   compute / WAND/MaxScore pruning -- the workload that's currently a useless
   default PR gate becomes a tracked metric).
3. Add a few **end-to-end queries** hitting iresearch (ingest + search) as the
   `e2e` group.

## Targets (benchmark groups -- for review, see plan §7)
Tentative axes, pick when a perf run is marked:
- `micro` -- existing `tests/bench/micro`
- `storage-write` -- ingest / checkpoint / WAL
- `search-query` -- FTS / filters / scoring / pruning (ex-searchbench)
- `connector-throughput` -- DuckDB<->iresearch vector/column paths
- `e2e` -- end-to-end (TPC-H-ish + search)

Open: per-group marking (human picks group) vs always-run-all-on-mark.

## How & when to run
- **Trigger:** manual mark only (a `run-perf` checkbox/label). Human decides --
  connector/storage rewrite -> run; most PRs -> skip. No auto per-path/per-commit.
- **Where:** dedicated cloud instance (AWS/GCS, from credits) + own disk; fixed
  instance type (bare-metal/dedicated, not burstable); governor `performance`,
  controlled turbo/SMT, isolated CPUs, single job at a time, stop-when-idle.
  **Without this pinning the numbers are noise -- it's a prerequisite.**
- **Comparison:** PR vs merge-base, 5 runs, geomean, %+absolute thresholds.

## History & dashboard (dogfood SereneDB)
- Store every run keyed by commit **in SereneDB itself**: schema
  `(commit, parent, ts, arch, bench_group, bench, query, metric, value,
  threshold)`. Grafana / simple dashboard for per-query per-commit trend.
- **Sparse by design:** only marked PRs + their bases land in the store -> it's a
  per-change comparison tool, not a dense timeline. (Open: opt-in periodic
  `main` baseline run to anchor a continuous timeline?)
- Keep a **CSV fallback** so a SereneDB outage never blocks a perf run.

## Flamegraph on regression
- When a query regresses, `perf record` the repro -> attach flamegraph SVG to the
  comment (we already keep a `perf` preset + sample `perf.svg`).

## Open questions
- Canonical query watchlist per group (which queries are "the" regression set)?
- Benchmark groups vs run-all-on-mark?
- Continuous `main` timeline yes/no?

## Done when
- Marking a PR produces a base-vs-PR table (+ flamegraph on regression), and the
  result is queryable from the SereneDB-backed dashboard.
