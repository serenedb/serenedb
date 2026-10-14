---
paths:
  - "server/**"
  - "iresearch/**"
  - "tests/**"
  - "examples/**"
  - "third_party/**"
---

# C++

Three kinds of code, with these rules in common: our own (`server/`, `iresearch/`, `tests/`, `examples/` and the in-tree parts of `third_party/`: [cpp-serenedb.md](cpp-serenedb.md)), the DuckDB family ([cpp-duckdb.md](cpp-duckdb.md)) and the other submodules ([cpp-third-party.md](cpp-third-party.md)).

- C++26 everywhere, forks included (the LLVM runtimes excepted): use the standard's facilities directly, never test the language version.
- Fix a problem where it is. Every submodule is ours to patch, so a dependency's bug or gap is fixed in the dependency, never worked around in its callers.
- Dependencies use each other (our libc++ inside DuckDB, DuckDB's ICU inside our RE2): reuse the copy we have instead of adding another.
- Our libc++ defaults `std::map`/`std::set` to `std::less<>`: their lookups (`find`, `contains`, `count`, `lower_bound`, `upper_bound`, `equal_range`) take the key you have, never a temporary; `operator[]`, `at`, `try_emplace` and `erase` still need a `key_type`.
- Formatting comes from each code's own tooling, never by hand: pre-commit for our code, `scripts/duckdb_family.sh` for the DuckDB family, a submodule's own `.clang-format` where it has one.
- No comments ([CLAUDE.md](../../CLAUDE.md)).
- Generated files change only through their generator.
- No forward declarations to avoid an include: include the header that defines the type.
- A hook, virtual function, field, setting or mode arrives with the code that uses it; a setting nothing reads anymore is deleted.
- Test code isn't refactored unasked: what it covers matters, its style much less.

## Performance

- No unnecessary copy or allocation; none at all in hot paths.
- Refactors are free at runtime, an absent feature costs nothing, and test-only instrumentation exists only in debug builds.
- Engine behaviour never depends on measured durations or wall-clock freshness: use exact, structural signals.
- Microbenchmarks explain, end-to-end numbers decide: nothing gets slower than main on a meaningful case, and a benchmark is never tuned for at the expense of what it doesn't measure.
- Measure with the `perf` preset (frame pointers, which `bench` lacks), on a quiet machine: the dev machines are shared with CI runners and other developers, so check `top -b -n 2 -d 1 -o %CPU` before each run, never measure while a build or test runs, and discard numbers that overlapped other load.
