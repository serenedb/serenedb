---
title: Experimental substring search
split: page
---

# Experimental substring search

`SET sdb_enable_fm_index = true` lets substring searches on an inverted index with a `keyword()` dictionary use an FM-index, a compressed full-text index over the distinct values of the column. The setting defaults to false and needs a server built with `-DSDB_FM_INDEX=ON`. Rare substrings get faster; the price is memory and a slow first query.

The build option `SDB_FM_BACKEND` picks one of four FM-index configurations: `rrr8` (default), `rrr32`, `plain8` or `plain32`. `rrr` stores the index in compressed bitvectors and `plain` in uncompressed ones. The number is the sampling interval used to locate matches: 8 is faster and larger than 32.

## Results

The data is a 1M-row sample of the ClickBench `hits` table. Each row of the first table is one way to answer the same three queries: the existing keyword index (the baseline), the keyword index with each FM-index configuration, the existing 3-gram index, and no index at all. Times are warm medians in milliseconds, with the interquartile range in brackets.

| Search method | Q21, ms [IQR] | Q22, ms [IQR] | Q23, ms [IQR] | Speedup over the same run's baseline, Q21 / Q22 / Q23 |
| --- | ---: | ---: | ---: | ---: |
| Keyword index (baseline, `rrr8` run) | 37.71 [0.84] | 37.65 [1.17] | 42.80 [0.70] | - |
| Keyword index + FM-index `rrr8` | 2.71 [0.20] | 2.87 [0.26] | 1.97 [0.59] | 13.9x / 13.1x / 21.8x |
| Keyword index + FM-index `rrr32` | 5.07 [0.67] | 5.57 [0.23] | 3.44 [0.43] | 7.6x / 7.0x / 12.9x |
| Keyword index + FM-index `plain8` | 1.93 [0.23] | 2.24 [0.34] | 1.39 [0.20] | 18.8x / 16.4x / 30.0x |
| Keyword index + FM-index `plain32` | 3.36 [0.55] | 3.25 [0.43] | 2.14 [0.37] | 11.1x / 11.4x / 19.7x |
| No index (Parquet scan) | 91.95 [0.68] | 77.98 [2.49] | 69.10 [4.02] | - |
| 3-gram index | 2.48 [0.14] | 0.69 [0.06] | 1.49 [0.12] | - |

The baseline row comes from the `rrr8` run; each speedup compares a configuration with the baseline measured in its own run. `rrr32` is the smallest configuration and `plain8` the fastest; `plain32` is larger and slower than `rrr8`. The 3-gram index is close to `rrr8` on Q21 and Q23 and four times faster on Q22; it takes 2.5 times the disk space of the keyword index.

## Costs

FM-index memory is held in addition to the keyword index and is not written to disk. The first query on a column that qualifies builds the FM-index for that column, so it takes seconds; RSS is about 0.5 GB before that build and peaks during it. Index build is the time of `CREATE INDEX`, a single observation. The sample holds 280.44 MB of `URL` and `Title` values, duplicates included; the FM-index adds 28.0% of that with `rrr8`, 12.6% with `rrr32`, 66.8% with `plain8` and 51.4% with `plain32`.

| Search method | Index on disk, MB | Index build, s | FM-index memory, MB | First URL / Title query, s | RSS before queries, MB | Peak RSS, MB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Keyword index + FM-index `rrr8` | 113.1 | 0.70 | 78.6 | 12.1 / 2.8 | 487.4 | 2066.2 |
| Keyword index + FM-index `rrr32` | 113.1 | 0.78 | 35.4 | 13.5 / 2.5 | 501.0 | 1791.8 |
| Keyword index + FM-index `plain8` | 113.1 | 0.68 | 187.4 | 12.1 / 2.4 | 476.3 | 2074.7 |
| Keyword index + FM-index `plain32` | 113.1 | 0.68 | 144.3 | 12.0 / 2.4 | 504.7 | 2036.6 |
| No index (Parquet scan) | 0.0 | - | - | - | 278.0 | 371.6 |
| 3-gram index | 282.6 | 5.15 | - | - | 1097.9 | 1164.1 |

## Queries

Every search method runs the same queries over a view on the sample. With an index, `URL` and `Title` are indexed as whole values:

```sql
CREATE TEXT SEARCH DICTIONARY fm_bench_dict AS keyword();

CREATE INDEX hits_idx ON hits USING inverted (
    id,
    URL fm_bench_dict,
    Title fm_bench_dict,
    SearchPhrase
) WITH (compaction_interval = 0);
```

The 3-gram index uses `generate_wildcard_ngrams(keyword(), 3)` as its dictionary instead. Q21-Q23 on the keyword and 3-gram indexes are:

```sql
SELECT count(*)
FROM hits_idx
WHERE URL @@ ts_like('%google%');

SELECT count(*)
FROM hits_idx
WHERE URL @@ ts_like('%google%')
  AND SearchPhrase <> '';

SELECT count(*)
FROM hits_idx
WHERE Title @@ ts_like('%Google%')
  AND URL NOT LIKE '%.google.%'
  AND SearchPhrase <> '';
```

Without an index the same queries read `hits` and use `LIKE '%google%'` in place of `@@ ts_like('%google%')`. They are count versions of the [ClickBench predicates](https://redirect.github.com/ClickHouse/ClickBench/blob/a101cf89a316d3fffd77f54df065c67d07123681/clickhouse/queries.sql#L21-L24), not a ClickBench run. The other queries have the same shape: the ID queries select `id` with `ORDER BY id` instead of `count(*)`, and the rest count rows for one pattern on `URL`.

The table below covers all 15 queries with `rrr8`. The baseline is measured twice, before the FM-index is built and after, and the two agree within 5% in this run. Controls are queries where no gain is expected: frequent substrings fall back to the baseline search, and prefix and exact matches do not use the FM-index.

| Query (`rrr8`) | Baseline before preparation, ms | Baseline, ms [IQR] | With FM-index, ms [IQR] | Speedup | Baseline change after preparation |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q21: count, URL contains `google` | 37.15 | 37.71 [0.84] | 2.71 [0.20] | 13.89x | +1.5% |
| Q22: Q21 with nonempty SearchPhrase | 37.90 | 37.65 [1.17] | 2.87 [0.26] | 13.10x | -0.7% |
| Q23: count, Title contains `Google`, URL lacks `.google.`, nonempty SearchPhrase | 42.37 | 42.80 [0.70] | 1.97 [0.59] | 21.76x | +1.0% |
| IDs, URL contains `google` | 88.05 | 90.00 [2.92] | 54.83 [0.47] | 1.64x | +2.2% |
| IDs, Title contains `Google` | 47.49 | 48.35 [0.68] | 44.17 [0.44] | 1.09x | +1.8% |
| Count, URL contains `wiki` | 36.89 | 37.40 [1.08] | 4.08 [0.34] | 9.17x | +1.4% |
| Count, URL contains `blog` | 38.52 | 38.31 [1.13] | 9.02 [0.84] | 4.25x | -0.5% |
| Count, URL contains `games` | 39.42 | 39.47 [1.36] | 10.27 [0.52] | 3.84x | +0.1% |
| Count, URL contains an absent string | 40.11 | 40.38 [0.51] | 0.67 [0.20] | 60.52x | +0.7% |
| Control: count, URL contains `yandex` | 61.79 | 62.08 [2.60] | 61.90 [0.97] | 1.00x | +0.5% |
| Control: count, URL contains `com` | 67.79 | 67.84 [1.18] | 68.03 [2.24] | 1.00x | +0.1% |
| Control: count, URL contains `http` | 130.32 | 131.97 [1.18] | 132.27 [3.10] | 1.00x | +1.3% |
| Control: count, URL starts with `http` | 30.71 | 29.36 [0.98] | 30.11 [0.86] | 0.98x | -4.4% |
| Control: count, URL ends with `.html` | 12.47 | 12.24 [0.38] | 12.02 [0.30] | 1.02x | -1.8% |
| Control: count, URL equals one value | 0.35 | 0.33 [0.03] | 0.41 [0.03] | 0.81x | -4.7% |

## Limitations

- The FM-index is built inside the first query that qualifies, and other queries on that column can wait for it. A restart or a compaction means building it again.
- Memory used while building is not limited by the memory settings. If the finished FM-index does not fit the memory limit, the column keeps using the baseline search.
- Turning the setting off does not free an FM-index that is already built.
- Only rare substrings gain. A substring with more occurrences than one per 128 distinct values, or spread over more than a quarter of the dictionary, uses the baseline search.
- Queries that return rows gain less than counts, because fetching the rows from Parquet takes most of their time.
- Controls differ by a few percent between off and on. The largest differences are prefix with `rrr8`, 2.6% slower with the setting on, and suffix with `plain32`, 6.2% slower with a spread wider than the difference. Exact match differs by up to 0.08 ms, slower in one configuration and faster in three.
- Timings are from one laptop, pinned to one core, with a warm file cache and one segment. Cold-cache, multi-segment and concurrent behavior are not measured.

## Method

The server is built with the `perf` preset (Clang 23, static linking, jemalloc) from main `53504ce03` plus this change, once per configuration. The machine is an Intel Core i7-13620H laptop under WSL2 with 11.7 GiB RAM. Each run uses one query thread and pins the server to CPU 0 after startup.

Every result is checked against the no-index result of the same query.

Per configuration, in one server process: each query runs seven times with the setting off, and the last six give the baseline before preparation; each query runs once with the setting on, which builds the FM-index and gives the first-query time; after 60 idle seconds each query runs in six alternating off/on pairs, which give the baseline and FM-index medians. The 3-gram and no-index rows come from separate servers and are medians of six runs after one warm-up.

Four runs were repeated and are not in the tables. In them the Q21-Q23 baseline changed by 12% to 170% between its two measurements, and in two of them the no-index scan was 1.7 and 3.4 times slower than in the retained run. Their cause cannot be established from the recorded data. A separate control reproduced a large slowdown in both keyword search and a Parquet scan under deliberate Windows CPU load, without building an FM-index; both recovered after the load stopped. With the laptop otherwise idle, preparing the FM-index did not reproduce that slowdown in either Q21 count. These controls demonstrate sensitivity to host activity, but do not identify the cause of the excluded runs.

Run on an idle machine and, under WSL2, monitor Windows activity as well as Linux: pinning the server and checking Linux load alone do not ensure stable timings. Both `run` and `report` exit unsuccessfully if any Q21-Q23 baseline median changes by more than 15% between the before-preparation and paired measurements. The JSON and printed tables are retained for diagnosis; there is no automatic retry. Investigate a flagged run before using its timings. Passing this check does not rule out contention that affects both measurements equally.

`rrr8`: binary SHA-256 `f5cff9e3d5c209c90197e3d2c34f6ec7409ae1d17c655d054479143eaeb59c6d`; one-minute load average 0.46 before, 0.40 after.

`rrr32`: binary SHA-256 `7fe37edbc4c355d2de3c86a05cc89fcbb522913b55a49acd5809243b43b6f9fa`; one-minute load average 0.12 before, 0.31 after.

`plain8`: binary SHA-256 `1fd6fd95d463feebc733b22e18e99f9fffbc155becd822b0b7b9e3ca4b60aafd`; one-minute load average 0.21 before, 0.24 after.

`plain32`: binary SHA-256 `5e88a46a2c1c46ad76e9a43aba7d8c1b2f6377d8b16bdd097c1ca6f4042d8839`; one-minute load average 0.21 before, 0.35 after.

## Reproduction

The runner needs Linux, Python 3.11+, PyArrow, psycopg, curl and ss. Run it on an idle machine, after the builds have finished.

```bash
set -euo pipefail
bash scripts/setup_fm_index.sh
mkdir -p scripts/perf/results/fm-index/bin
for backend in rrr8 rrr32 plain8 plain32; do
  cmake --preset perf -B build_fm_perf \
    -DSDB_FM_INDEX=ON -DSDB_FM_BACKEND="$backend" \
    -DSDB_SDSL_SOURCE_DIR="$PWD/build_fm_deps/sdsl-lite"
  cmake --build build_fm_perf --target serened -j 4
  cp build_fm_perf/bin/serened "scripts/perf/results/fm-index/bin/serened-${backend}"
done
python3 scripts/perf/fm_index.py prepare \
  --output scripts/perf/results/fm-index/hits-1m.parquet
for backend in rrr8 rrr32 plain8 plain32; do
  extra=()
  if [ "$backend" = rrr8 ]; then extra+=(--references); fi
  python3 scripts/perf/fm_index.py run --backend "$backend" \
    --binary "scripts/perf/results/fm-index/bin/serened-${backend}" \
    --data scripts/perf/results/fm-index/hits-1m.parquet \
    --output "scripts/perf/results/fm-index/${backend}.json" \
    --server-cpu 0 "${extra[@]}"
done
python3 scripts/perf/fm_index.py report \
  scripts/perf/results/fm-index/{rrr8,rrr32,plain8,plain32}.json
```

`prepare` downloads 200,000-row prefixes of five row groups of the source file; the sample's SHA-256 is `9593b129bb4c2717b0c67f6b1bc64ea39d8dfd5be74471a96c9f640487e36806`. `run` saves every timing, the query plans, the binary's SHA-256 and the load averages, and refuses to overwrite a report. `report` prints the three tables from the saved reports without starting a server.
