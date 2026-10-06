---
paths:
  - "tests/sqllogic/**"
---

# Writing sqllogic tests

Read a sibling `.test` in the same directory first; match its style.

## Where a test goes

- `any/pg/`: behaviour PostgreSQL and SereneDB share. It runs through the symlinks `sdb/pg/any` and `pg/any`, never directly, and must pass on both engines. Validate new SQL behaviour on real PostgreSQL (`tests/sqllogic/run_pg_tests.sh --host <h> --single-port <p>`, or docker `postgres:18`), then pin it. Outcomes must match; error text may differ (use a `skipif`/`onlyif` pair or an inline regex that matches both messages).
- `sdb/`: SereneDB-only syntax and features.
- `recovery/`: crash and restart tests; each file gets its own serened. `SET sdb_faults = ...` is allowed only here (pre-commit `check-fault-points`). Every other file shares one suite server: a `SET GLOBAL` there must not change another test's result and is reset with `RESET GLOBAL` at the end of the file; a test that needs more belongs here.

## Records

- Write bare `query`, without type letters.
- Expected `query` results start with the column-name header line.
- Separate records with two blank lines. One blank line lets an expected block swallow the next record.
- Errors use the block form, matched as exact text. The one exception is `any/pg/` when PostgreSQL and SereneDB word an error differently: a one-line `statement error <regex>` that matches both, or a `skipif`/`onlyif` pair.

  ```
  statement error
  DROP TABLE missing_t;
  ----
  db error: ERROR: <exact message>


  ```

  `<slt:ignore>` wildcards a volatile tail (a "Did you mean" list, oids, generated names). Use it sparingly: an ignored tail asserts nothing.
- Fill expectations with `run.sh ... --override` against a live server, then rerun without it and review the diff. The override keeps `<slt:ignore>` markers by aligning them with the new output and falls back to the raw output when it can't: check that each marker survived. In recovery tests, check post-crash records too: one captured while the server restarts reads "Connection refused".
- `connection <name>` applies to the next record only. Repeat it before every record that needs that connection.
- Retries: `statement ok retry 10 backoff 200ms`, `query ok retry 10 backoff 200ms`.
- A statement parked on a failpoint with `statement async` must be released (`SET sdb_faults = '-X'`) from a different connection, or the runner deadlocks.
- Exact float results of order-dependent aggregates (`covar_pop`, `corr`, `regr_*`, anything over DISTINCT) flake between environments: round them.

## Names, files and substitution

- Each file gets its own database, but secrets (`CREATE SECRET`), ATTACH aliases, roles and databases are server-global and both wire engines run against one server: name them after the test file, guard with `DROP ... IF EXISTS`, and build ATTACH paths as `${__TEST_DIR__}/${__RUN_ID__}_<name>`.
- Files go under `${__TEST_DIR__}`, never a literal `/tmp` (pre-commit `check-no-tmp-in-sqllogic`). Any `${...}` needs `control substitution on` before its first use (pre-commit `check-substitution-directive`).
- SQL bodies substitute `${VAR}` and `$VAR`; `system` commands substitute only `$VAR` (follow it with a non-word character); expected blocks substitute nothing.
- Docs examples live in `sdb/pg/site_docs/**` and `recovery/site_docs/**` behind `# DOCS_TEST: <name>` markers; the docs site embeds them by id. A file without a `# DOCS_TEST_BODY` line yields no examples.
