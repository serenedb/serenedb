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
./build/bin/serened ./build_data --server.endpoint='pgsql+tcp://0.0.0.0:<port>'
psql -h 127.0.0.1 -p <port> -U postgres
# when finished:
kill -9 $(lsof -t -i:<port>) 2>/dev/null
rm -rf build_data    # only if you don't need the datadir afterwards
```
