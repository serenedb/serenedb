---
title: Experimental substring search
split: page
---

# Experimental substring search

The experimental substring build targets case-sensitive searches within complete string values, including `LIKE '%google%'` and `ts_like('%google%')` on a `keyword()` dictionary.
Patterns with additional wildcards retain their exact matching semantics.
Ordinary builds use the existing search behavior.

## Build

Prepare the optional dependency and build the comparison binaries with the measured settings:

```bash
bash scripts/setup_fm_index.sh
cmake --preset perf -B build_fm \
  -DSDB_STATIC=OFF -DSDB_ALLOC=SYS -DSDB_DEBUG_INFO=STACK \
  -DSDB_LTO=OFF -DSDB_EMBEDDED_DOCS=OFF -DSDB_FM_INDEX=OFF
cmake --build build_fm --target serened -j 4
mkdir -p build_fm_baseline/bin
cp build_fm/bin/serened build_fm_baseline/bin/serened
cp build_fm/CMakeCache.txt build_fm_baseline/CMakeCache.txt
cmake -S . -B build_fm -DSDB_FM_INDEX=ON \
  -DSDB_SDSL_SOURCE_DIR="$PWD/build_fm_deps/sdsl-lite"
cmake --build build_fm --target serened -j 4
```

The first qualifying query on a field pays an additional preparation cost and increases resident memory.
The cached representation is released when the field's reader is released.
A restart repeats the preparation cost.
New or compacted data can incur preparation again.
The experiment adds no persistent index files.
Dense matches can use the existing search path.

The `fm_index_serialized_bytes` row of `sdb_metrics` reports the resident experimental representation: SDSL serialized sizes, compressed key bytes, key offsets, and the key decoder.
It is a representation-size measurement, not the process's resident set or peak construction memory.

## ClickBench substring comparison

The comparison uses the substring predicates from [ClickBench Q21–Q24](https://redirect.github.com/ClickHouse/ClickBench/blob/a101cf89a316d3fffd77f54df065c67d07123681/clickhouse/queries.sql#L21-L24) on the original `hits` data.
It extracts matching-row counts and complete matching-row ID lists; it does not measure the original queries' grouping, ranking, or full-row projection.
Matching-row counts differ from substring-occurrence counts when a value contains a substring more than once.
Additional controls cover a dense substring, an absent substring, and a prefix search.

Download the measured sample containing only the required columns:

```bash
python3 scripts/perf/fm_index.py prepare --rows 1000000 --groups 5 \
  --output scripts/perf/results/fm-index/hits-1m.parquet
```

The downloader requires `pyarrow` and `curl`.
It fetches selected column chunks with HTTP range requests and records the source row count, selected rows and columns, downloaded bytes, and sample SHA-256.
The sample takes prefixes of five evenly spaced row groups; it is not a random sample or evidence for the full dataset's performance.
Omitting `--groups` selects a prefix of the source file.

Build the same revision with the experiment disabled and preserve that binary as `build_fm_baseline/bin/serened`.
Run the comparison with `psycopg` installed, after builds have finished and the machine is otherwise idle:

```bash
python3 scripts/perf/fm_index.py run \
  --baseline build_fm_baseline/bin/serened \
  --candidate build_fm/bin/serened \
  --data scripts/perf/results/fm-index/hits-1m.parquet \
  --output scripts/perf/results/fm-index/results.json \
  --tries 7 --threads 2
```

Each configuration starts a fresh server and data directory.
The configurations are a scan, a complete-value keyword index, the experimental keyword index, and a complete-value 3-gram index.
Indexed queries use `@@ ts_like(...)` to request the same complete-value substring predicate through each index.
All configurations must produce identical counts and matching sample-row IDs.

The report retains query plans, every request-to-result timing, process CPU time and page-fault deltas, index build time, on-disk index bytes, database-directory bytes, process resident and peak resident memory, and the experimental representation size.
The first execution is reported separately from the remaining executions.
Process CPU time has the resolution of the operating system's process clock.
Operating-system caches are not cleared, so the first execution is not a cold-cache measurement.
The source Parquet file is shared and must be included once in every configuration's storage total.

The experimental keyword index adds memory to the ordinary keyword index.
A space advantage must therefore be established against an alternative such as the 3-gram index, while also reporting the keyword comparison and process memory.

## Measurements

These measurements used base revision `4c715787fd10325ce1ae5a2540b10d4c1dcecd42` with uncommitted experimental changes, on 28 October 2026.
The machine was WSL2 Linux on an Intel Core i7-13620H, with 16 logical CPUs and 7.6 GiB of memory; builds and tests had finished before timing began.
Both binaries used Clang 23.1.5 and the `perf` preset with `SDB_STATIC=OFF`, `SDB_ALLOC=SYS`, `SDB_DEBUG_INFO=STACK`, `SDB_LTO=OFF`, and `SDB_EMBEDDED_DOCS=OFF`.
The experiment was disabled for the scan, keyword, and 3-gram configurations and enabled for the FM configuration.

The source contains 99,997,497 rows.
The measured sample contains 200,000 rows from each of row groups 0, 56, 112, 168, and 225, with SHA-256 `9593b129bb4c2717b0c67f6b1bc64ea39d8dfd5be74471a96c9f640487e36806`.
The client used PyArrow 26.0.0 and psycopg 3.3.6, with automatic prepared statements disabled.
Each query ran seven times; the table reports the median of the six executions following the first, in milliseconds, with two query threads.

| Predicate and operation | Matching rows | Scan | Keyword | Keyword + FM | 3-gram |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q21 URL count | 787 | 53.423 | 10.980 | 5.708 | 21.242 |
| Q22 URL count, nonempty search phrase | 1 | 45.230 | 12.453 | 5.056 | 0.688 |
| Q23 combined title count | 100 | 38.991 | 14.634 | 3.387 | 15.779 |
| Q24 URL matching sample IDs | 787 | 51.866 | 34.708 | 31.676 | 134.427 |
| Title matching sample IDs, title predicate only | 1,074 | 34.809 | 25.296 | 24.114 | 63.015 |
| Dense substring `%http%`, count | 999,709 | 49.606 | 141.280 | 195.701 | 85.408 |
| Absent substring, count | 0 | 52.395 | 11.932 | 0.599 | 0.613 |
| Prefix `http%`, count | 999,709 | 47.513 | 42.191 | 72.852 | 78.651 |

All counts and complete, sorted matching-ID lists agreed across the four configurations.
The three adapted count queries were 1.9–4.3 times faster than keyword; the two matching-ID queries improved by approximately 5–10%.
The matching-ID plans include native Parquet materialization, so their timings cover more than candidate-term lookup or posting-ID enumeration.
The 3-gram index was faster for Q22 and for the dense control.

| Configuration | Index disk, MiB | Additional FM representation, MiB | Process RSS after queries, MiB | Peak process RSS, MiB |
| --- | ---: | ---: | ---: | ---: |
| Scan | 0.00 | 0.00 | 351.21 | 376.29 |
| Keyword | 108.73 | 0.00 | 439.70 | 737.02 |
| Keyword + FM | 108.73 | 113.06 | 657.98 | 1,572.45 |
| 3-gram | 266.52 | 0.00 | 389.00 | 1,351.42 |

Every configuration also references the same 41.05 MiB sample Parquet file.
FM reduced index disk by 59.2% relative to 3-grams and increased resident memory.
Adding the keyword disk representation and the additional FM representation gives 221.79 MiB, compared with 266.52 MiB for 3-grams; this 16.8% representation-size difference combines different storage media and does not imply lower process memory.
Relative to keyword alone, index disk was unchanged and process RSS increased by 218.28 MiB.

Creating the keyword and FM configurations' ordinary index took approximately 0.42 seconds; creating the 3-gram index took 3.13 seconds.
The FM configuration's first qualifying URL query took 39.14 seconds, and its first qualifying title query took 7.50 seconds.
These are total query latencies including preparation, reported separately from the warm medians.

The dense and prefix controls had large outliers with two threads, including in the keyword baseline.
An additional instrumented single-thread build/query configuration retained the count-query improvements and produced these medians:

| Operation | Keyword, ms | Keyword + FM, ms |
| --- | ---: | ---: |
| Q21 count | 11.292 | 5.464 |
| Q22 count | 11.029 | 4.731 |
| Q23 combined count | 14.380 | 3.033 |
| URL matching IDs | 64.642 | 61.574 |
| Title matching IDs | 47.615 | 48.788 |
| Dense count | 123.795 | 127.101 |
| Absent count | 11.067 | 0.580 |
| Prefix count | 37.310 | 45.408 |

The single-thread dense and prefix samples had no major page faults and identical query plans between keyword and FM.
The thread setting applied to both index construction and queries, and the follow-up added process-counter reads; it does not isolate query scheduling alone.
Prefix search retained a penalty of approximately 22%; these measurements establish neither a universal speedup nor the cause of that remaining penalty.
Full query plans, individual timings, result hashes, binary hashes, and memory measurements are retained in `docs/benchmarks/fm-index-results.json` and `docs/benchmarks/fm-index-single-thread.json`.
The single-thread report also includes process CPU-time and page-fault measurements.

## SearchBench OpenTelemetry comparison

This comparison adapts [SearchBench Q01 and Q25–Q27](https://redirect.github.com/serenedb/searchbench/blob/3c8e3928619adc0e0b58c48df89dc8a68b4cfc1b/serenedb/queries.sql) to a search-backed table containing `id` and `Body`.
The public corpus contains synthetic logs generated by the OpenTelemetry demo application; it is not production telemetry.
The sample takes the first 100,000 rows in timestamp-sorted `part_000.parquet`, matching SearchBench's smoke selection, and contains 31,114 distinct lowercase ASCII-alphanumeric tokens.
These measurements cover a chronological prefix and selected predicates, not the billion-row source part or the full SearchBench suite.

Native and FM configurations retain SearchBench's `split_by_non_alpha(case := 'lower', break := 'ascii')` analyzer and `frequency`, `norm`, and `position` features.
The scalar scan evaluates the same token predicates.
The auxiliary 3-gram configuration retains tokenization and omits unsupported `norm`; its exact-term control uses `ts_like('error')` without a wildcard.
Only counts and complete sample-row IDs are measured, without ranking.

Prepare and run with the same binaries and Python dependencies as the ClickBench comparison:

```bash
python3 scripts/perf/fm_index_otel.py prepare --rows 100000 \
  --output scripts/perf/results/fm-index/otel/otel-100k.parquet
python3 scripts/perf/fm_index_otel.py run \
  --baseline build_fm_baseline/bin/serened \
  --candidate build_fm/bin/serened \
  --data scripts/perf/results/fm-index/otel/otel-100k.parquet \
  --output scripts/perf/results/fm-index/otel/results-100k.json \
  --tries 7 --threads 1
```

Preparation rejects an existing sample; use a fresh output location for another download.
The measured sample's SHA-256 is `409286d2efad41659afe59a1c945f42b2cf444c01f17dc117c77638fd51409b8`.
The run used the same machine and build settings, with one thread and no concurrent builds or tests; CPU idle was 99.0% before the run.
All 168 executions matched an independent ASCII-token oracle, including complete matching-ID hashes.
Each timing below is the median of six executions following the first, in milliseconds; operating-system caches were not cleared.

| Predicate and operation | Matching rows | Scalar scan | Native | Native + FM | 3-gram |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q27 `%nnec%`, count | 3,465 | 30.8898 | 0.7493 | 0.6284 | 0.5335 |
| Q27 `%nnec%`, complete IDs | 3,465 | 31.8876 | 1.8132 | 1.0175 | 1.0043 |
| Q25 `conn%`, count | 3,764 | 27.1039 | 0.4735 | 0.4814 | 0.5874 |
| Q26 `%tion`, count | 11,292 | 27.9522 | 0.6333 | 0.6621 | 0.7316 |
| Q01 `error`, count | 9,083 | 20.5312 | 0.4056 | 0.4039 | 0.8390 |
| Absent substring, count | 0 | 27.2747 | 0.7755 | 0.5391 | 0.4506 |

The Q27 count median improved by 1.19 times, saving 0.121 milliseconds.
The complete-ID median improved by 1.78 times, but native observations ranged from 1.33 to 3.54 milliseconds, making that ratio noisy.
Prefix, suffix, and exact-term controls showed no useful benefit.
The first FM Q27 query took 135.05 milliseconds versus 1.18 milliseconds for native, including preparation.

| Resource | Native | Native + FM | 3-gram |
| --- | ---: | ---: | ---: |
| Ingest, index, and refresh, seconds | 0.12734 | 0.14430 | 0.31188 |
| Index disk, bytes | 5,292,786 | 5,292,786 | 17,815,385 |
| Database directory, bytes | 5,570,352 | 5,570,352 | 18,093,004 |
| Additional FM representation, bytes | 0 | 619,618 | 0 |
| Process RSS after queries, bytes | 287,916,032 | 288,317,440 | 279,597,056 |
| Peak process RSS, bytes | 293,130,240 | 293,400,576 | 335,990,784 |

Every configuration also uses the same 913,588-byte source sample.
Index disk includes stored data in the search-backed table, so it is not an isolated dictionary-size measurement and differs in scope from the ClickBench view comparison.
The 3-gram configuration used 3.37 times the index disk and was faster than FM on Q27 count.
FM added a resident representation while leaving native index disk unchanged; process RSS was higher than both native and 3-grams.
The complete timings, plans, hashes, source provenance, and resource measurements are retained in `docs/benchmarks/fm-index-otel.json`.

## Engineering comparison

A separate 100,000-row sample from the same five source row groups was used to compare configurations before the million-row evaluation.
Each row below is a measured SQL run with seven executions and two query threads; latency is the median of the six executions following the first.

| FM configuration | URL count, ms | Combined title count, ms | FM representation, MiB |
| --- | ---: | ---: | ---: |
| RRR wavelet tree, SA-order samples, CSA key reconstruction | 27.738 | 16.241 | 5.56 |
| RRR wavelet tree, SA-order samples, FSST-compressed keys | 1.665 | 1.800 | 14.43 |
| RRR wavelet tree, text-order samples, FSST-compressed keys | 1.363 | 1.321 | 14.62 |

Compressed candidate keys trade additional memory for faster positive matches.
Text-order sampling further reduced lookup time with a small representation-size increase.
The selected configuration uses sampling distance 32; sparse inverse-SA samples avoid paying for unused CSA key reconstruction.
An isolated comparison also tested a plain wavelet bitvector and per-key LZ4 compression; the measured decoder gains did not justify their larger representations on these sample fields.
The complete SQL timing and space records for the three configurations are retained in `docs/benchmarks/fm-index-engineering.json`.
