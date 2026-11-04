# Lucene vs SereneDB vs PostgreSQL index benchmark

Compares the SereneDB inverted index with Lucene and with PostgreSQL's native indexes on `INET`/`CIDR` columns, on `INTEGER` and `BIGINT` columns against Lucene's BKD points, and on `VARCHAR` as a reference. Each engine loads the same generated data; the benchmark measures ingestion until searchable, index size on disk and the median latency of `count(*)` queries, and checks that every engine returns the same count.

## Prerequisites

- docker and `curl`. The `eclipse-temurin:21.0.12.1_1-jdk` and `postgres:18.6` images are pulled on first use; the Lucene 10.5.2 and pgjdbc 42.7.14 jars are downloaded from Maven Central into the data directory and checked against their SHA-1.
- serened from the current tree with the `bench` preset: `ninja -C build_bench serened`.

## Commands

```bash
scripts/perf/lucene_compare/run.sh prepare                       # once: generate the datasets
scripts/perf/lucene_compare/run.sh bench [inet|int|string|all]   # every run reads them
```

`bench` starts a fresh serened (free port, temporary data directory) and a fresh PostgreSQL container per type and stops both at the end. The benchmark JVM (javac, Lucene and the pg-wire client) and PostgreSQL run in docker with `--network host`, as the calling user, with every file (datasets, Lucene index, PGDATA, serened data) in the data directory on the host disk; serened runs natively. The report is printed and saved as `reports/<timestamp>-<type>.md`, with `reports/latest.md` pointing at the last one; every `EXPLAIN` is saved next to it as `.plans.txt`, the serened logs in `<timestamp>-logs/`.

| Variable | Default | Meaning |
|---|---|---|
| `SERENEDB_LUCENE_BENCH_DATA` | `~/.cache/serenedb-lucene-bench` | datasets, jars, reports |
| `SERENED` | `build_bench/bin/serened` | binary under test |
| `PG_IMAGE` | `postgres:18.6` | PostgreSQL image |
| `JAVA_IMAGE` | `eclipse-temurin:21.0.12.1_1-jdk` | JDK image for the benchmark JVM |
| `BENCH_HOSTS`, `BENCH_NETWORKS`, `BENCH_INTS`, `BENCH_STAMPS`, `BENCH_LOWCARD`, `BENCH_STRINGS` | 2M, 1M, 2M, 2M, 2M, 2M | rows, read by `prepare` |
| `BENCH_LUCENE_THREADS` | 8 | Lucene indexing threads |
| `BENCH_MERGE` | unset | merge both engines to this many segments before querying (Lucene `forceMerge`, SereneDB `VACUUM (COMPACT_TABLE)`); the merge counts as ingest |
| `BENCH_CPUS` | unset | `taskset` list for serened and `--cpuset-cpus` for the JVM and PostgreSQL containers |
| `BENCH_TIMEOUT`, `BENCH_JAVA_HEAP` | 900, `8g` | seconds the benchmark JVM may run per type, its `-Xmx`; raise both for tens of millions of rows |

## Data

Seeded, so `prepare` always writes the same files.

- `hosts`: 90% IPv4 (half in `10.0.0.0/8`, half anywhere), 10% IPv6 in `2001:db8::/32`, no netmask.
- `networks`: `CIDR` blocks, 90% IPv4 with masks from `/8` to `/32` weighted towards `/16`-`/24`, 10% IPv6 `/32` to `/64`.
- `ints`: uniform in `[0, 1000000)`.
- `stamps`: millisecond timestamps rising with the row number, 0-19 ms apart, each up to 2 s late: nearly sorted, nearly unique.
- `lowcard`: 1000 distinct values, Zipf-distributed (the most frequent value holds 13% of the rows).
- `strings`: 8 random lowercase letters.

`bench int` runs `ints`, `stamps` and `lowcard`. Each gets an exact match, `IN` lists of 10 and 1000 sampled values, ranges centred on the median that hold 0.001% to 90% of the rows, and a range holding every row.

## Engines

- **Lucene**, in-process in the benchmark JVM: `InetAddressPoint` for hosts, `InetAddressRange` for networks, `IntPoint`, `LongPoint`, `StringField`; query cache off. Each query is timed twice: `IndexSearcher.count`, and a "visit" that collects every matching document into a counting collector, so Lucene builds the document set the way a query that needs the rows would. Ingestion is parse + `addDocument` on N threads + `commit` + opening the reader.
- **SereneDB**: `CREATE INDEX ... USING inverted (col)` on an empty table, then `COPY` and `VACUUM (REFRESH_TABLE)`; queries read the index (`FROM <index>`).
- **PostgreSQL**: `COPY`, `VACUUM (ANALYZE)` (reported, not counted), then `CREATE INDEX`. Hosts and networks get a B-tree plus GiST `inet_ops`, then the GiST index is replaced with SP-GiST `inet_ops` and every query runs again. `INTEGER` gets a B-tree, `VARCHAR` a B-tree on a `COLLATE "C"` column, so `LIKE 'abc%'` and ranges can use it. The plan column says which index PostgreSQL chose; it may prefer a seq scan.

SereneDB and PostgreSQL run over pg-wire with the same client: pgjdbc, one connection, one server-side prepared `SELECT count(*)`, the same number of runs per query for all three engines. The report states the `SELECT 1` round trip of both servers, the floor every SQL latency includes and Lucene does not.

## Query mapping

| SQL predicate | Lucene query |
|---|---|
| `ip = a`, `ip IN (...)` | `InetAddressPoint.newExactQuery`, `newSetQuery` |
| `ip < a`, `ip BETWEEN a AND b` | `InetAddressPoint.newRangeQuery` (IPv4 values sort before IPv6 in both) |
| `ip <<= 'N/b'` | `InetAddressPoint.newPrefixQuery` |
| `net >>= 'a'` | `InetAddressRange.newContainsQuery(a, a)` |
| `net <<= 'N/b'` | `InetAddressRange.newWithinQuery` |
| `net = 'N/b'` | within AND contains |
| `net << 'N/b'` | within AND NOT contains |
| `net >> 'N/b'` | contains AND NOT within |
| `net && 'N/b'` | `InetAddressRange.newIntersectsQuery` (two CIDR blocks overlap only when one contains the other) |
| `v = x`, `v IN`, `v BETWEEN`, `v >= x` | `IntPoint`/`LongPoint` `newRangeQuery`, `newSetQuery` |
| `s = x`, `s IN`, `s LIKE 'p%'`, `s >= a AND s < b` | `TermQuery`, `TermInSetQuery`, `PrefixQuery`, `TermRangeQuery` |

The reference count of every query comes from PostgreSQL with index scans turned off; a cell that differs from it is marked **MISMATCH**.

## Caveats

- The machine is shared with CI runners: the report records the load average, CPU idle and the busiest processes before and after each type. Compare runs only at similar load.
- Lucene answers `count` from index statistics where it can (term document frequencies, BKD cell counts), so its `count` numbers are lower bounds of a search that visits documents; the `visit` column is that search.
- Lucene indexes on several threads and, without `BENCH_MERGE`, is not force-merged; the report lists both engines' segment counts.
