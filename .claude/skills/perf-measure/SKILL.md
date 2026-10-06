---
name: perf-measure
description: Measure, benchmark or profile SereneDB on shared dev machines - which build to time, load checks, A/B methodology, perf with frame pointers, micro vs end-to-end numbers. Use before timing, benchmarking, A/B comparing or profiling anything.
---

# Measuring performance

Benchmarks and load-generating runs happen only when the user asks for them.

## Before any number

1. Dev boxes are shared. Check the machine:
   `uptime; ps -eo user,pcpu,comm --sort=-pcpu | head -15`.
   Don't measure while anyone's build, test suite or benchmark runs (pinning to other cores doesn't isolate L3, memory bandwidth or SMT siblings). Note the load next to every result; discard numbers that overlapped.
2. Time and profile `build_perf` (preset `perf`: RelWithDebInfo `-O3` with frame pointers, static, no `SDB_DEV`). `build_bench` (Release) compiles with `-fomit-frame-pointer`, so `perf record -g` call graphs are broken there. Never time a Debug or `SDB_DEV` build (`lldb`, `clangd` presets): dev asserts make some paths tens of times slower.
3. Prove the binary contains the change: fresh relink in the build log, binary mtime, or `nm` for a new symbol.
4. Keep `git commit`, `pre-commit`, `scripts/format_duckdb.sh` (clang-format in docker) and pushes out of measurement windows.

## A/B

- Baseline = main built the same way. Save main's binaries before the tree moves.
- Interleave runs (ABBA), don't run all of A then all of B: sequential runs drift and invent trends. Compare ratios within one harness run.
- Cold-cache probes of two binaries on the same data dir are biased: the second inherits a warm drive. Give each its own data copy or use ABBA through the harness.
- On Zen 5 machines, rebuilding identical code moves tight loops by around 5% (sometimes much more) from code placement alone. For small effects compare instructions and IPC (`perf stat -r 5`), not single wall-clock runs.
- Microbenchmarks explain, end-to-end numbers decide. Work is done when no meaningful end-to-end row is slower than main.
- Benchmark tables have no PRIMARY KEY unless the subject is the key: PK maintenance (ART) pollutes the measurement.
- Measure with default settings; debug knobs (`sdb_scan_split`, ...) are not baselines.
- Wire benchmark (`scripts/perf/bench_wire_old_vs_new.sh`): `PERF_SDB_DOCKER=0` for A/B of our own changes; the docker default is for cross-database comparisons.

## Micro benchmarks

One binary holds them all: `ninja -C build_perf serenedb-bench-micro`, then `build_perf/bin/serenedb-bench-micro <name> [--benchmark_filter=...]`. Register new ones with `add_bench(<name>)` in `tests/bench/micro/CMakeLists.txt`.

## Profiling

- `perf record -g` (frame pointers), never `--call-graph dwarf`. Read the existing profile at instruction level before adding instrumentation: `perf annotate -i perf.data --stdio -l --symbol='<demangled>'`.
- `perf stat -e task-clock,context-switches,instructions,cycles` separates CPU work from blocking.
- Released images (`serenedb/serenedb:latest`) have no frame pointers: compare self time only (`perf report --no-children -g none`).
- Samples attributed to `[JIT]`: symbolize leaf IPs from `perf script -F ip` with `llvm-symbolizer --obj=<binary>`.
- Keep perf's build-id cache off: it copies every sampled multi-GB binary into `~/.debug` and once reached 86 GB. The dev boxes disable it in `/etc/perfconfig`; elsewhere pass `-N` to `perf record`.
- Codegen of an inline function under real flags: add a temporary `[[gnu::noinline]]` extern probe calling it in an existing `.cpp`, build normally, `objdump -dC --no-show-raw-insn <that>.cpp.o`.
