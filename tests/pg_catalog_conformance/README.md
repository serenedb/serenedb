# pg_catalog bench

Gate 0 of `PG_CATALOG_DESIGN.md` (section 7). The conformance checks live in
one sqllogic file, `tests/sqllogic/any/pg/system/catalog_conformance.test`,
which runs on SereneDB in the sqllogic suites and on PostgreSQL 18.3 in the
validate-pg job. This directory holds the latency bench that times the
queries of that file.

## The conformance test

The file creates its objects (schemas `app` and `public`, enums, a composite
type, sequences, tables with keys, checks, defaults and `serial`, indexes,
views, a SQL function, comments, grants to the role `conf_reader`), then
checks:

- every column of every `pg_catalog` and `information_schema` relation:
  name, order, `format_type`, `attnotnull`;
- the catalog rows describing those objects in `pg_namespace`, `pg_class`,
  `pg_attribute`, `pg_type` (user and builtin), `pg_index`, `pg_constraint`,
  `pg_attrdef`, `pg_description`, `pg_depend`, `pg_shdepend`, `pg_sequence`,
  `pg_enum`, `pg_rewrite`, `pg_trigger`, `pg_proc`, `pg_database`, `pg_roles`,
  `pg_am`, `pg_range`, `pg_inherits` and 19 `information_schema` views;
- `pg_get_*def`, `pg_get_expr`, `format_type`, `*_is_visible`, `reg*` input
  and output, `to_reg*`, descriptions, `pg_get_serial_sequence` and
  privileges, for every object and for fixed builtin cases;
- the queries of psql (`\d`, `\d+`, `\dt`, `\dv`, `\di`, `\ds`, `\dT`, `\dn`,
  `\df`, `\dp`), pgjdbc, Npgsql, psycopg, asyncpg, tokio-postgres,
  postgres.js, SQLAlchemy, Django, Rails, dbt, Metabase, pgAdmin, DBeaver and
  serene-ui, wrapped in an outer query that turns oids into names and orders
  the rows;
- the DDL SereneDB does not support yet (domains, `jsonb`, identity columns,
  `ON DELETE CASCADE`, PL/pgSQL triggers, row-level security, comments on
  schemas and constraints, foreign data wrappers), run in schema `pgonly`
  after all shared checks, and the catalog rows those objects produce.

No query prints the oid of a user object. Every statement and query runs on
both servers with one expected result, except where SereneDB differs: there
the shared query leaves out the differing rows or columns, a
`skipif serenedb` block keeps PostgreSQL's result for them, and the
`onlyif serenedb` block right after it runs the same SQL and holds
SereneDB's current result or error. The differences decided in section 5.9
(`sdb_*` relations, the `secondary`/`inverted`/`iresearch` access methods,
the bootstrap superuser oid, `pg_toast`, builtin objects without
descriptions) are left out of the shared queries and checked on their own,
with the same pairs or with `onlyif serenedb` queries for SereneDB-only
objects.

## Bench

```bash
python3 tests/pg_catalog_conformance/bench.py \
    --target sdb=127.0.0.1:7861 --target pg=127.0.0.1:55433 --server-comm pg=postgres \
    --tables 1000,10000 --json /tmp/bench.json
```

Needs python3 with `psycopg` (3.x). Servers are reached over the wire only.

For each size it creates (once; reused later, `--regenerate` rebuilds)
database `catbench_<n>` on every target: `n` tables in schema `catbench`,
each with a primary key, 8 columns, two defaults and a secondary index; a
comment on every 10th table, a view on every 5th, a sequence on every 20th.
The schema is not on the search path and none of the test's queries filters
on it, so it grows the catalogs without growing their answers.

Then it reads the sqllogic file (`--test`, the conformance test by default):

- the statements before the first query and between queries run once as
  setup, after the statements after the last query ran once to clear
  leftovers of an interrupted run; the latter run again at the end;
- every query block is timed; `skipif`/`onlyif` are honoured for the engine
  `version()` reports (`serenedb` or `postgres`) and `pg-wire-simple`, and a
  skipped query shows as `SKIP`;
- queries are named `L<line> <first catalog read>`; `--filter REGEX` matches
  that name or the SQL.

- 5 interleaved rounds (`--rounds`); targets alternate and the order flips
  every round. One run executes a query back to back for about
  `--budget-ms` (400) wall time, never fewer than `--min-iters` (3) times,
  over the simple query protocol. The table shows the median (min-max) of the
  per-run mean latency in ms.
- CPU quietness: each run records the busy jiffies of the whole box
  (`/proc/stat`) minus the server's (the pid listening on the target port, plus
  processes named by `--server-comm name=comm`, needed for a docker
  PostgreSQL whose listener is `docker-proxy`) minus the bench client's. A run
  whose foreign load exceeds `--max-foreign-cores` (2.0) is marked `*` and
  the size is reported as not quiet; rerun it then.
- EXPLAIN ANALYZE: for every timed query, every `SYSTEM_TABLE_SCAN` in
  `EXPLAIN (ANALYZE, FORMAT JSON)` must emit at most
  `max(--explain-floor, --explain-factor x result rows)` rows (1000 and 4 by
  default), i.e. in proportion to the answer rather than to the catalog. Each
  scan is listed with its projections and pushed filters.

Queries that fail on a target are listed as errors and excluded from timing.
