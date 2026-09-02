# Catalog stress suite

Sustained parallel DDL/DML churn against a `serened` of its own, with a
model-based consistency oracle, a hang detector, and seeded repro.

It exists because `#930` ("Split catalog and data") replaced the hand-rolled
catalog with a port onto DuckDB's `CatalogEntry` / `CatalogSet` /
`DependencyManager` / `WriteAheadLog`, and changed DDL concurrency control to
optimistic MVCC: there is no `ACCESS EXCLUSIVE` lock, and the second writer of an
entry is refused with `40001` on the spot. Existing coverage of that is almost
entirely sequential -- the concurrent cases are a handful of hand-unrolled
sqllogic files, because the vendored `sqllogictest-rs` runner has no loop, no
randomization, no duration bound and no per-record timeout.

## Running it

```bash
tests/stress/run.sh --profile smoke
tests/stress/run.sh --profile soak --scenario ddl_churn --seconds 900
tests/stress/run.sh --scenario serial_churn --workers 2 --seed 12345
tests/stress/run.sh --profile break-everything          # all of it at once
```

### Scenarios

| scenario | what it churns |
|---|---|
| `tables_only` | tables, views, ART indexes, DML -- no sequences, the one shape that survives longest |
| `ddl_churn` | the broad DDL mix incl. sequences and SERIAL |
| `ddl_dml_race` | DDL against DML on the same tables |
| `dependency_churn` | views and indexes over tables, cascade drops |
| `serial_churn` | `SERIAL` tables and sequences -- the fastest route to the catalog-commit wedge |
| `name_reuse` | drop-then-recreate on fixed names: the ABA / identity test |
| `shared_arena` | every worker on 8 shared names -- the optimistic-conflict path |
| `cancel_bait` | slow `CREATE TABLE AS` for protocol-level cancellation to land on |
| `iceberg_views` | text search dictionaries, views over `iceberg_scan`, inverted indexes over those views, search, `REINDEX`, `VACUUM (REFRESH_INDEX)` |
| `foreign_servers` | `CREATE SERVER ... postgres_fdw` pointed at this serened's own port |
| `server_race` | several workers creating, dropping and *using* the same few servers |
| `attach_churn` | `CREATE`/`DROP DATABASE`, `ATTACH`/`DETACH` of plain duckdb files, cross-database reads and writes |
| `break_everything` | all of the above interleaved, 30 op kinds |

### Profiles

`smoke`, `soak`, `soak-tsan`, `scale`, `cancel`, `iceberg`, `remote`, `attach`,
`server-race`, `compaction-probe`, `wedge-probe`, `break-everything`.

`break-everything` arms every chaos knob at once -- injected catalog-window crashes,
a graceful `SIGTERM` restart, an index-build park, protocol cancellations and a forced
catalog-log compaction. A clean run there is the surprising outcome.

`iceberg_views` needs `resources/tests/iceberg`, which
`scripts/ensure_iceberg_fixture.sh` generates (it wants docker). Without it the suite
says so and skips rather than failing.

`BUILD_DIR` / `SERENED` pick the binary, exactly as in `tests/network/run.sh`.
Every flag has an `SDB_STRESS_*` environment equivalent, so CI can drive it
through the `docker.env` block.

Fault-dependent profiles need a build with `-DSDB_FAULT_INJECTION=On` (on for
`dev` and the sanitizer presets, **off** for `perf`). The suite prints
`fault_injection=` in its preflight line and skips those scenarios when it is off.

## What it asserts

Four properties, in the order they fail loudest:

1. **It does not crash.** The server log is scanned for the sanitizer / assertion /
   fatal-signal regex shared with `tests/drivers/sqlsmith/run.sh`.
2. **It does not hang.** A watchdog probes `SELECT 1` on a *fresh* connection --
   a wedge takes the whole process down, not just the busy sessions -- and on two
   consecutive failures samples `/proc/<pid>/task/*` twice to prove every thread
   is frozen, with a `wchan` histogram. This needs no debugger and no ptrace,
   which matters because `ptrace_scope=1` blocks attaching to a sibling process.
   `statement_timeout` is **not** used: the GUC self-describes as "accepted for
   compatibility but not currently enforced", so every deadline here is
   client-side.
3. **It stays consistent.** At each quiesce the harness's own model of what should
   exist is diffed against `pg_class` + `pg_description` + `sdb_catalog_sets()`,
   together with dependency-edge integrity, oid non-reuse, and an orphan-artifact
   walk of the datadir.
4. **It holds the load.** Committed ops, retries and the per-label error histogram
   are reported. The budget metric is *committed* ops, never ops/s: a conflict
   refusal is cheaper than a real commit, so an ops/s floor reads a conflict storm
   as healthy load.

## The model, and why it is a candidate set

A naive model produces a false positive on the first restart and the suite loses
its credibility. Each op resolves to an `Outcome`; three of them are *ambiguous*
(`UNKNOWN_CRASH`, `UNKNOWN_CANCEL`, `UNKNOWN_TIMEOUT`), and an ambiguous op leaves
its key holding **both** the before and after state. That is not hedging: the two
catalog crash windows differ exactly there. A crash before the catalog log is
flushed means the statement never happened; a crash after the flush and before the
data commit is a *delayed* commit, not a lost one.

After a restart every ambiguous key must resolve to a member of its own candidate
set. Resolution to a third state is the finding
(`ambiguous_resolved_to_third_state`).

Object identity is the content token, not the name: every created object carries a
unique token written with `COMMENT ON` and read back through `pg_description`. That
is what lifts the oracle from "an object with that name exists" to "it is the right
object", and it makes drop-then-recreate distinguishable from survival.

Only keys a worker *owns* are modelled. Shared-arena keys are registered as shared
and excluded from existence assertions, because with several workers racing on one
name the harness cannot know the order and any existence claim would be a guess.
Global invariants (edge integrity, oid reuse, ghost entries) still cover them.

## Oracle facts that are easy to get wrong

All four were verified against a running server, and all four would otherwise
produce false positives:

- `sdb_catalog_sets().visible` is **hardcoded true** at every row site, and each
  walk is transaction-filtered so an invisible entry is not emitted at all. The
  doc comment above the function claims otherwise. The oracle never depends on it;
  it asserts `bool_and(visible)` only as a tripwire that the contract changed.
- `entry_type` is human-readable: `Table`, `View`, `Index`, `Sequence`, `Schema`,
  `Role`, `Database`, `Dependency` -- not the C++ enum spelling.
- **An index occupies two slots.** It appears as both `Index` and `Table` with the
  same oid. The snapshot drops the `Table` row for any name that also has an
  `Index` row, or every index reads as a ghost table.
- **Implicit `*_pkey` indexes are in `pg_class` but not in `sdb_catalog_sets()`.**
  The oracle only reasons about names matching its own generator
  (`^s<tag>_w\d+_(t|q|v|i)\d+$`), so engine-derived names never trip it.

Entry rows come from the session's default database only, while `Dependency` rows
come from every attachment. One result set, two scopes. The consequence is concrete:
with more than one database in play an edge into another one looks dangling, so the
dependency-edge check is **skipped** once `pg_database` holds more than one row rather
than made to lie. Cross-database invariants need one connection per database.

Two more kinds have no token channel at all: `COMMENT ON TEXT SEARCH DICTIONARY` and
`COMMENT ON SERVER` are `42601`, and `COMMENT ON DATABASE` is `0A000`. Those three are
modelled for presence only, and their identity is proven by oid instead -- the oracle
requires the oid to be stable within one incarnation, which is the same ABA test the
token gives every other kind. A foreign server is also invisible to `pg_class` and
carries an empty `schema_name`.

`DROP VIEW` and `DROP TABLE` **silently** take every inverted index over them, with no
`CASCADE` asked for and no warning, so the generator cascades those in its own model --
without that, every later op against the index reports a vanished private key.
`DROP TEXT SEARCH DICTIONARY` is the opposite: refused with `2BP01` while any index
references it, and its `CASCADE` form is a syntax error, so a tokenizer is only
droppable once its indexes are gone.

Search-index artifact reclamation is deferred to a background pool after the deciding
commit, so mid-run a just-dropped index legitimately still owns its directory. The
orphan-artifact walk therefore only runs where boot's sweep is the actual contract:
after a restart.

## Fault injection

`SET sdb_faults` does **not validate names**: it inserts any string, so a typo
arms nothing, silently. It also throws `22023` on a double arm. Both are handled
by `faults.py`: every fault name is a module constant checked against the actual
`SDB_IF_FAILURE` / `SDB_WAIT_ON_FAILURE` literals in `server/` and `libs/` by
`test_fault_catalog.py`, and arming goes through one refcounted broker on a single
admin connection.

Fault points are process-global and survive a failed run, so the suite resets them
on entry and uses one server per run, never reused. A park fault
(`SDB_WAIT_ON_FAILURE`) blocks with **no timeout**, so a harness killed by a job
timeout with one armed would leave threads parked forever.

Two park faults only fire when progress reporting is active
(`pause_create_index_mid_build`, `pause_vacuum_mid_walk`). Over pg-wire that is
always true, so they park a client-driven build; they do **not** park an internal
or background one. `pause_create_index_between_batches` lives in `IndexWriter`'s
commit-on-flush path, shared with the background refresh and compaction tasks, so
arming it can park the whole background pool.

The constants must stay flat `NAME_FAULT = "literal"` assignments in `faults.py`:
`scripts/check_fault_points.py` harvests them with that exact regex, and only from
files that already mention `sdb_faults`. A dict of names would not be seen.

## Layout

| file | what it holds |
|---|---|
| `main.py` | preflight, scenario run, quiesce, oracle, verdict, artifacts |
| `config.py` | profiles: smoke / soak / soak-tsan / wedge-probe |
| `scenarios.py` | op weights + the per-worker object state machine |
| `ops.py` | one builder per DDL family |
| `model.py` | the candidate-set model and its collapse algebra |
| `classify.py` | the closed error table; no default-expected branch |
| `snapshot.py` | the oracle's SQL and the datadir walk |
| `oracle.py` | invariants as pure functions of (models, snapshot) |
| `watchdog.py` | liveness probe, stall detection, wedge classification |
| `quiesce.py` | the drain protocol |
| `faults.py` | fault-name constants and the refcounted broker |
| `capture.py`, `journal.py`, `junit.py` | artifacts, the op log, the CI report |

`tests/harness/python/{serened,procutil}.py` holds the process lifecycle, shared
with `tests/network/hba_mask_test.py`.

## Triage

Every failing run writes to `--outdir`:

- `summary.txt` -- the one-paste repro: the exact command with the seed, the
  findings, and the last journal records.
- `report.json` -- meta, watchdog verdict, every finding.
- `ops.jsonl` -- every op with its outcome, sqlstate, label and attempt count.
- `threads.txt` -- the two `/proc` thread samples, on a wedge.
- `sanitizer-hits.txt` -- matching server-log lines, when there are any.
- `datadirs/` -- the datadir, moved rather than deleted, on any non-clean verdict.

A run is reproducible in its op *stream* (a pure function of seed and worker id),
not in its *schedule*. A finding replays only to the extent its scenario used park
faults to pin the interleaving.

## Known state on `main`

`tests/stress` currently fails on `main` by design: concurrent DDL churn wedges
the server. See `resources/suppressions/tsan.txt:33-51`, which describes the
lock-order inversions as **"NOT proven harmless ... Suppressed, NOT fixed"**, and
`origin/fix/catalog-commit-deadlock`, which fixes them and is not merged. With the
repo's own suppression file loaded a ThreadSanitizer run reports nothing, so the
suite gets its own suppression file for the TSAN leg.
