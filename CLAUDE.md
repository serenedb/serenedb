# Notes for AI agents

Read `CONTRIBUTING.md` -- it covers build, tests, branches, commits, PRs, and
C++ style. This file only flags traps that aren't in there.

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
./build/bin/serened ./build_data --server_endpoint='pgsql+tcp://0.0.0.0:<port>'

# Default database is `postgres` (serenedb has no separate default).
psql -h 127.0.0.1 -p <port> -U postgres -d postgres

# when finished:
kill -9 $(lsof -t -i:<port>) 2>/dev/null
rm -rf build_data    # only if you don't need the datadir afterwards
```
