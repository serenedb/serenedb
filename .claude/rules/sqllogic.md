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

## Fault points

Fault points force what a plain test can't reach: a rare interleaving, an error or a crash at an exact spot, a corner case. Use them for such tests, and add one to the code where none sits at the spot you need.

- In C++ (`iresearch/utils/debugging.hpp`): `SDB_IF_FAILURE("<name>") { ... }` runs its block when the point is armed, usually `THROW_SQL_ERROR(ERR_MSG("intentional debug error"))` or `SDB_IMMEDIATE_ABORT()`; `SDB_WAIT_ON_FAILURE("<name>")` holds every thread that reaches it until the point is disarmed; `SDB_PARK_ONCE_ON_FAILURE("<name>")` parks the first thread that reaches it until the test releases it.
- A test arms a point with `SET sdb_faults = '<name>'` and disarms it with `SET sdb_faults = '-<name>'`; `RESET sdb_faults` clears all. Arming an armed point or disarming an unarmed one is an error. Only a superuser can set them, and an armed point fires in every session.
- Only under `recovery/`, where each file gets its own serened, and only in builds with fault injection (the `lldb`, `clangd` and sanitizer presets, not `bench` or `perf`). Pre-commit `check-fault-points` also requires every point a test arms to exist in the code, and every point in the code to be used by some test.
- A race, step by step: arm a park point; start the statement on its own connection with `statement async ok`; wait until it is parked with `statement ok retry 40 backoff 250ms` on `SET sdb_faults = '-<name>:parked'`, which fails until the thread parks; run the competing statements on other connections; release the parked thread with `SET sdb_faults = '<name>:release'` (a wait point: disarm it) and `wait`. Release it from another connection than the parked one, or the runner deadlocks. `tests/sqllogic/recovery/inverted_index_checkpoint_during_refresh.test` does exactly this.
