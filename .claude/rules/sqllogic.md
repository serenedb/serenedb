---
paths:
  - "tests/sqllogic/**/*.test"
  - "tests/sqllogic/**/*.test_slow"
---

# Writing sqllogic tests

Read a sibling `.test` in the same directory first and match its style.

- Write bare `query`, without type letters. Expected results start with the column-name header line.
- A multiline `statement error` block ends only at two empty lines: with one, it swallows the next record. Query results end at the first empty line.
- Errors use the block form, matched as exact text:

  ```
  statement error
  DROP TABLE missing_t;
  ----
  db error: ERROR: <exact message>
  ```

  `<slt:ignore>` wildcards a volatile tail (a "Did you mean" list, oids, generated names); use it sparingly, since an ignored tail asserts nothing. The one exception to exact text is `any/pg/` when PostgreSQL and SereneDB word an error differently: a one-line `statement error <regex>` that matches both, or a `skipif`/`onlyif` pair.
- Fill expectations with `run.sh ... --override` against a live server, then rerun without it and review the diff.
- `connection <name>` applies to the next record only; repeat it before every record that needs that connection.
- Retries: `statement ok retry 10 backoff 200ms`, `query ok retry 10 backoff 200ms`.
- Results must not depend on execution order: `ORDER BY` every multi-row result and round floating-point aggregates.
- Each file runs in its own database, created per run under a unique name (`${__DATABASE__}`), but secrets, ATTACH aliases, roles, servers and databases are server-global, and other files and the other wire engine run against the same server at the same time: suffix their names with the database, `<name>_${__DATABASE__}`, and name attached files the same way, `${__TEST_DIR__}/<name>_${__DATABASE__}.db` (DuckDB names an attached database after the file's basename).
- Server-wide listings (`duckdb_tables()`, `duckdb_databases()`, `pg_database`, ...) also show other files' objects: filter them, e.g. `WHERE database_name = current_database()`.
- Files go under `${__TEST_DIR__}`, never a literal `/tmp` (pre-commit `check-no-tmp-in-sqllogic`). Any `${...}` needs `control substitution on` before its first use (pre-commit `check-substitution-directive`).
- Outside `recovery/`, every file shares one suite server: a `SET GLOBAL` must not change another test's result and is undone with `RESET GLOBAL` at the end of the file. A test that needs more goes under `recovery/`.

## Races

Races are testable in sqllogic, so a concurrency bug still gets a test:

- `connection <name>` before a record runs it on that named session.
- `statement async ok`, `statement async error`, `query async` and `system async ok` run the record in the background. On a named connection, async records keep that connection's order and overlap other connections. Without a connection, each async record gets a fresh one.
- `wait` blocks until every background record has finished; a later synchronous record on a busy named connection waits for that connection only.
- Records are dispatched in file order, and dispatching a record on a busy named connection first waits for that connection. To keep a step in flight while others run, put the steps that should overlap on different connections right after it, then `wait`.
- `control max-async-connections N` caps parallel background records (default 10); `control always-async on` makes every record async.
- There are no loops: widen a race window by repeating records.
- `tests/sqllogic/pg/simple/async.test` is a minimal example.
