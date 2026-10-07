# Notes for AI agents

## Rules

- You don't allowed to write any comments until you explictly asked to write them.
If you noticed outdated comment or make some comment outdated, just remove it until explicitly asked to keep it.

- Don't use python/sed for normal code editing/reading, only for real scripting changes

- Read `CONTRIBUTING.md` -- it covers build, tests, branches, commits, PRs, and
C++ style. This file only flags traps that aren't in there.

- A user-visible feature is not finished until `docs/` describes it. Write the
page in the same change, following the **Documentation** section of
`CONTRIBUTING.md`, and say in your summary which page you added or updated.

## Formatting

serenedb's own code and the DuckDB family are never formatted by hand: run the
tools below and commit what they produce. Other submodules have no formatter
set up; there, match the surrounding code by hand, and never run a formatter
over their files.

- serenedb's own code: `.clang-format` through pre-commit (see `CONTRIBUTING.md`).
- The DuckDB family (the duckdb submodules, `database-connector`,
  `duckdb_clickhouse`): `scripts/duckdb_family.sh`, which runs DuckDB's own
  format.py, generators and Makefile targets with their pinned tools; `--help`
  describes every mode. Never run a generator, format.py or clang-format there
  by hand.
  - `format` before every commit, and `format --check --range <a>..<b> <dir>`
    before pushing a series: every commit, merges included, formatted on its
    own.
  - `regen` builds the duckdb fork's final `regen:` commit with DuckDB's
    generators in DuckDB's order; `regen --check` proves it is current.

## Before writing tests

- Sqllogic: read a sibling `.test` first. Control directives, retry patterns,
  connection naming, and the right subtree (`any/sdb/pg/recovery`) are set by
  example, not docs.
- gtest / microbench: mirror an existing one in the same `tests/<area>/` subtree.

## Local smoke server

Pick a free `<port>`. You own it for the session -- when you're done, kill the
process so the next contributor / agent doesn't inherit a stale serened.

```bash
# Backgrounded server logs land in the file you redirect to (>/tmp/sdb.log
# 2>&1 &). DuckDB's LogManager writes to in-memory storage by default; query
# `SELECT * FROM duckdb_logs()` or the `sdb_log` catalog view from psql.
./build/bin/serened ./build_data --listen='postgres://0.0.0.0:<port>'

# Default database is `postgres` (serenedb has no separate default).
psql -h 127.0.0.1 -p <port> -U postgres -d postgres

# when finished:
kill -9 $(lsof -t -i:<port>) 2>/dev/null
rm -rf build_data    # only if you don't need the datadir afterwards
```
