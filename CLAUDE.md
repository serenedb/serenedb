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

serenedb's own code and the duckdb submodules are never formatted by hand: run
the tools below and commit what they produce. Other submodules have no
formatter set up; there, match the surrounding code by hand, and never run a
formatter over their files.

- serenedb's own code: `.clang-format` through pre-commit (see `CONTRIBUTING.md`).
- The DuckDB fork, from `third_party/duckdb`, in this order:
  1. `./scripts/parser/build_grammar.sh` -- the PEG grammar and transformer;
     `make generate-files` does not regenerate the parser, so this goes first.
  2. `make generate-files` -- settings, serialization, enum_util, functions,
     metrics and storage info, then formats the tree.

  Commit regenerated artifacts separately from the change that caused them
  (`regen: ...`), as upstream does.
- Then, from the repo root, `./scripts/format_duckdb.sh` -- clang-format 11.0.1
  (in docker) over the changed files of every duckdb submodule and
  `duckdb_clickhouse`, each with its own `.clang-format`.

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
