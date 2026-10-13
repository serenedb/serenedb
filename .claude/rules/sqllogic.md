---
paths:
  - "tests/sqllogic/**/*.test"
  - "tests/sqllogic/**/*.test_slow"
---

# Writing sqllogic tests

- Write bare `query`, without type letters. Expected results start with the column-name header line.
- A multiline `statement error` or `query error` block ends only at two empty lines: with one, it swallows the next record. Query results end at the first empty line.
- New error records use the block form, matched as exact text, even where siblings use the one-line `statement error <regex>`, which accepts any message containing the regex:

  ```
  statement error
  DROP TABLE missing_t;
  ----
  db error: ERROR: <exact message>
  ```

  `<slt:ignore>` wildcards a volatile part (a "Did you mean" list, oids, generated names); use it sparingly, since an ignored part asserts nothing. The one exception to exact text is `any/pg/` when PostgreSQL and SereneDB word an error differently: a one-line `statement error <regex>` that matches both, or a `skipif`/`onlyif` pair.
- Fill expectations with `run.sh ... --override` against a live server, then rerun without it and review the diff.
- `connection <name>` runs the next record on that named session, and only that record: repeat it before every record that needs it.
- Retries: `statement ok retry 10 backoff 200ms`, `query ok retry 10 backoff 200ms`.
- Results must not depend on execution order: `ORDER BY` every multi-row result and round floating-point aggregates.
- Each file runs in its own database, created per run under a unique name (`${__DATABASE__}`), but secrets, ATTACH aliases, roles, servers and databases are server-global: other files run against the same server at the same time, and the other wire engine reruns the file afterwards. Suffix their names with the database, `<name>_${__DATABASE__}`; for an ATTACH that is the alias (`AS <name>_${__DATABASE__}`), or the file name when there's no `AS`, since DuckDB then names the database after the file's basename.
- Server-wide listings (`duckdb_tables()`, `duckdb_databases()`, `pg_database`, ...) also show other files' objects: filter them, e.g. `WHERE database_name = current_database()`.
- Files go under `${__TEST_DIR__}`, never a literal `/tmp` (pre-commit `check-no-tmp-in-sqllogic`). Any `${...}` needs `control substitution on` before its first use (pre-commit `check-substitution-directive`).
- Outside `recovery/`, never `SET GLOBAL`: the value reaches other files' sessions before any reset. A test that needs it goes under `recovery/`, where each file gets its own serened.
- A test that needs a service carries the service's suffix and `.test_slow`: `_s3`, `_azure`, `_iceberg`, `_ollama`, `_kev`, `_pgscan`, `_pgscan_tls`, `_chscan` (`<name>_pgscan.test_slow`). `run.sh` starts the service from the name, and `--fast` skips the file.

## Races

Races are testable in sqllogic, so a concurrency bug still gets a test:

- `statement async ok`, `statement async error`, `query async` and `system async ok` run the record in the background. On a named connection, async records keep that connection's order and overlap other connections. Without a connection, each async record gets a fresh one.
- `wait` blocks until every background record has finished; a later synchronous record on a busy named connection waits for that connection only.
- Records are dispatched in file order, and dispatching a record on a busy named connection first waits for that connection. To keep a step in flight while others run, put the steps that should overlap on different connections right after it, then `wait`.
- `control max-async-connections N` caps parallel background records (default 10); `control always-async on` makes every record async.
- There are no loops: widen a race window by repeating records.
- `tests/sqllogic/pg/simple/async.test` is a minimal example.
