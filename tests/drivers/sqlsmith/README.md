# sqlsmith

[SQLsmith](https://github.com/anse1/sqlsmith) is a grammar-based PostgreSQL
fuzzer. It introspects `pg_catalog` to discover the schema, then generates
random SELECT queries against the discovered relations. We use it as an
external PG-wire client to look for crashes, assertion failures, and
sanitizer hits in serened.

## Install sqlsmith

Debian/Ubuntu:

```
sudo apt install sqlsmith
```

Build from source:

```
git clone https://github.com/anse1/sqlsmith.git
cd sqlsmith
autoreconf -i
./configure
make
sudo make install
```

Build deps: C++11 compiler + libpqxx.

## Run it

In one terminal, start serened with auth off and PG wire on `localhost:5432`:

```
ninja serened
./build/bin/serened /tmp/datadir \
    --server_endpoints pgsql+tcp://0.0.0.0:5432 \
    2>/tmp/serened.log
```

In another terminal:

```
bash tests/drivers/sqlsmith/run.sh
```

The script loads `seed.sql` via `psql`, then runs sqlsmith for ~60 seconds
(2000 queries) and reports the symbol-stream tally.

For sanitizer signal, build serened with ASan/UBSan and point the harness
at the server log so it can scan for sanitizer reports after the run:

```
SDB_SERVER_LOG=/tmp/serened.log bash tests/drivers/sqlsmith/run.sh
```

## Tunables

| Env var | Default | Meaning |
|---|---|---|
| `SDB_FUZZ_QUERIES` | `2000` | Queries per sqlsmith instance (`--max-queries`). 2000 ≈ 60s on a quiet box. |
| `SDB_FUZZ_PARALLEL` | `1` | Number of concurrent sqlsmith instances. Each gets a distinct seed (base SEED + i). Total queries = `QUERIES * PARALLEL`. |
| `SDB_FUZZ_SEED` | random | Fix the RNG base seed to repro. Instance i uses `SEED + i`. |
| `SDB_FUZZ_LOG_TO` | unset | libpq DSN of a separate PG instance for sqlsmith's structured query+error log (`--log-to`). When set, the harness prints a top-20 error histogram after the run. See "Bucketing errors" below. |
| `SDB_SERVER_LOG` | unset | Path to serened's stderr log; if set, scanned for sanitizer/assert hits |
| `SDB_DRV_HOST` | `localhost` | PG host |
| `SDB_DRV_PORT` | `5432` | PG port |
| `SDB_DRV_DATABASE` | `postgres` | Database name |
| `SDB_DRV_USER` | `postgres` | PG user |
| `SDB_DRV_JUNIT` | unset | If set, emit `tests-drivers-sqlsmith-junit.xml` here |

## Interpreting the output

sqlsmith `--verbose` prints one character per query to stderr:

| Symbol | Meaning | Treated as |
|---|---|---|
| `.` | Query executed successfully | normal |
| `S` | Server returned a syntax error | noise (PG-syntax we don't yet support) |
| `e` | Server returned a runtime error | noise (semantic mismatches, type errors) |
| `t` | Query timed out | noise |
| `C` | Connection was broken mid-query | informational -- see below |

A `C` symbol alone does **not** mean the server crashed: it can also fire
when the server tears a single session on a non-fatal error path (timeout,
per-query OOM, etc.) while the process keeps running. The script
distinguishes the two by probing the server after the run and only fails
if **either** the server log shows a sanitizer / assertion / fatal signal
**or** the server stops accepting new TCP connections. Standalone `C`
symbols are reported in the summary line as `conn_drops=N` for triage
but do not fail the run on their own.

The run also fails if sqlsmith exited non-zero with zero successful queries
-- that typically means a catalog-introspection problem in `pg_catalog`,
fixable upstream of this harness.

## Bucketing errors

The symbol stream tells you *how many* queries errored but not *which kinds*
of error. To get a top-N histogram of error classes (SQLSTATE + first 100 chars
of the message), point sqlsmith at a side-channel logging DB:

```
# 1. Start a postgres sidecar
docker run -d --name sqlsmith-log -p 5433:5432 \
    -e POSTGRES_PASSWORD=x postgres:16
sleep 3

# 2. Apply sqlsmith's log schema
curl -s https://raw.githubusercontent.com/anse1/sqlsmith/master/log.sql | \
    PGPASSWORD=x psql -h localhost -p 5433 -U postgres -d postgres

# 3. Run the harness with SDB_FUZZ_LOG_TO set
SDB_FUZZ_LOG_TO="host=localhost port=5433 user=postgres password=x dbname=postgres" \
SDB_FUZZ_PARALLEL=4 SDB_DRV_PORT=7897 \
    bash tests/drivers/sqlsmith/run.sh
```

The harness automatically queries the log DB at the end of the run and prints
the top 20 distinct (sqlstate, message) pairs by frequency. That tells you
which features/functions are eating the most fuzz budget.

## Reproducing a finding

When the script fails, it prints a one-line repro command that pins the
seed. Re-run it against the same serened build to reproduce:

```
sqlsmith --target="host=localhost port=5432 dbname=postgres user=postgres" \
    --max-queries=2000 --seed=<seed> --verbose
```

The full sqlsmith log is preserved under either `$SDB_DRV_JUNIT` (if
running under `tests/drivers/run.sh`) or a `mktemp -d` path printed at
failure time.

## What's not covered

- This harness only generates SELECT statements (sqlsmith's PG target is
  read-only). DDL/DML fuzzing belongs to later phases (SQLancer).
- Logic bugs (wrong-but-not-crashing answers) require oracle-based
  fuzzing -- that's Phase 4 of the plan, not this script.
- Parser-level UB inside `Prepare()` / `ExtractStatements()` and wire-level
  framing bugs are better caught by in-tree libFuzzer harnesses (Phases 2
  and 3).
