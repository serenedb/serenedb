---
paths:
  - "tests/sqllogic/**/*.test"
  - "tests/sqllogic/**/*.test_slow"
---

# Writing sqllogic tests

- Bare `query`, no type letters; expected results start with the column-name header line.
- A multiline `statement error`/`query error` block ends only at two empty lines (with one, it swallows the next record); query results end at the first empty line.
- New errors use the block form, matched as exact text (`db error: ERROR: <message>` after `----`), even where siblings use the one-line regex form. `<slt:ignore>` wildcards a volatile part, sparingly. In `any/pg/`, where PostgreSQL and SereneDB word an error differently, a one-line regex matching both or a `skipif`/`onlyif` pair.
- Fill expectations with `scripts/ci/test.py --override --test '<unit>'` only for the file you are writing, then check every result against PostgreSQL or the spec, never against what the build returns. A changed record you didn't write is a failure to investigate.
- `connection <name>` applies to the next record only: repeat it before each.
- Results never depend on execution order: `ORDER BY` every multi-row result, round floating-point aggregates. Where `ORDER BY` can't apply (`RETURNING`, `EXECUTE`), `rowsort` after one `?` per column (`query ?? rowsort`): a bare `query rowsort` reads `rowsort` as the column types and does not sort.
- Outside `recovery/`, files share one server with other files running at the same time, and the second wire engine reruns each file. Anything server-wide (`SET GLOBAL`, fault points, a restart) therefore goes under `recovery/`, where each file gets its own server.
- Each file runs in its own database (`${__DATABASE__}`), but secrets, ATTACH aliases, roles, servers and databases are server-global: give them names no other file uses (a prefix of the file's own, or `<name>_${__DATABASE__}`; a file's two wire engines never run at once), and filter server-wide listings (`WHERE database_name = current_database()`).
- Files go under `${__TEST_DIR__}`, never `/tmp` (`check-no-tmp-in-sqllogic`); `${...}` needs `control substitution on` first (`check-substitution-directive`).
- A test that needs a service is `.test_slow` with the service's suffix (`_s3`, `_azure`, `_iceberg`, `_ollama`, `_kev`, `_pgscan`, `_pgscan_tls`, `_chscan`): `test.py` starts the service from the name.
- `.test_slow` means a non-standard run: a service, TLS, a generated fixture. A test that is only slow is `.test`; split it rather than rename it.

## Races

A concurrency bug still gets a test:

- `statement async ok`/`error`, `query async` and `system async ok` run in the background, in order per named connection; `wait` waits for all of them. A synchronous record on a busy connection waits for that connection.
- To keep a step in flight, put the overlapping steps on other connections right after it, then `wait`. No loops: a race test parks a statement on a fault point rather than repeating records. Example: `tests/sqllogic/pg/simple/async.test`.

## Fault points

- In C++, ours and DuckDB's alike (`duckdb/common/fault_points.hpp`): `SDB_IF_FAILURE("<name>") { ... }` runs its block when armed; `SDB_WAIT_ON_FAILURE("<name>")` holds threads until disarmed; `SDB_PARK_ONCE_ON_FAILURE("<name>")` parks the first thread until released. Add one where none sits at the spot a test needs.
- Armed with `SET sdb_faults = '<name>'`, disarmed with `'-<name>'`, cleared with `RESET sdb_faults`, which also releases every parked thread; superuser only, only in builds with fault injection (not `bench`/`perf`); `check-fault-points` requires every armed point to exist and every point to be used.
- A test crashes serened only through a fault point whose block calls `SDB_IMMEDIATE_ABORT()`, or stops it cleanly (`shutdown_on_packet`). Any other exit, such as a signal, an assertion or a sanitizer abort, fails the recovery test, even when the test passes after the restart.
- A race step by step: arm a park point, start the statement with `statement async ok` on its own connection, wait for the park with `statement ok retry 40 backoff 250ms` on `SET sdb_faults = '-<name>:parked'`, run the competing statements on other connections, release with `SET sdb_faults = '<name>:release'` from another connection, then `wait` (`recovery/inverted_index_checkpoint_during_refresh.test`).
- A parked thread keeps its locks: a competing statement that needs one (a checkpoint's exclusive lock, a commit's WAL sync lane) runs async, and the release comes once `pg_stat_activity` shows it active. Never park while holding the transaction lock every statement takes when it starts: the test's own release would wait on it.
