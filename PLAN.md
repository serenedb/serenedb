# Rewrite serenedb's catalog on top of duckdb's catalog

Brief for a fresh session. You are starting over: the current
`server/catalog/` is deleted and written again, this time as a thin PostgreSQL
layer on duckdb's own catalog. This document is self-contained -- everything
you are allowed to know about the previous implementation is in here.

## Hard rules

- **Do not write comments. None, anywhere** -- server code, duckdb patches,
  tests, headers, CMake. Delete outdated comments in code you touch. If you
  believe a comment is genuinely needed, ask the owner first.
- **Do not look at the previous implementation.** That means: the current
  contents of `server/catalog/` beyond what deleting it requires, the duckdb
  fork commits after the pinned baseline (below), any docs or git history
  describing the old catalog. It is a bad implementation being discarded; if
  you stumble into it, close it. The spec is the test suites and
  PostgreSQL 18, nothing else.
  **Two exceptions, and only these:**
  1. the existing **tokenizer** integration (the catalog kind behind
     `CREATE TEXT SEARCH DICTIONARY`, its entry code, and the command/function
     layer around it) -- you may read it and reuse it as the reference
     implementation for an added kind;
  2. the existing **system tables / system views** code -- the pg_catalog and
     information_schema projections (the fork's default generators and the
     server-side system functions and views) -- you may read it and reuse it.
- **Plan first.** Deliver a short design document and get the owner's
  approval before writing implementation code. Same for any patch to duckdb.

## Where you stand

- Repo: `~/serenedb`, branch off `main`. Read `CLAUDE.md` and
  `CONTRIBUTING.md` first; they cover build, tests, commits, PRs and C++
  style. This file only adds what is specific to this task.
- duckdb is a fork under `third_party/duckdb` (submodule). It is pinned to
  **`76c226cde3b74279522b566efbd04148dc883484`** ("regen: parser, settings,
  serialization and function artifacts") -- the last commit *before* the
  catalog-split patchset. The two commits after it (`5166587daa`,
  `1b7d500cb0`) are that patchset and its regen; they are reverted and you
  must not check them out, diff against them, or read them. Verify the pin
  first: at your baseline the submodule contains **no**
  `Catalog::WriteCatalogChange`, no `TransactionManager::CatalogLog`, no
  `CatalogEntry::duck_managed`, no `CatalogPermissions`, no
  `host_table_provider`, no by-id entry lookups. If it does, stop and ask.
- What the fork DOES carry at the baseline, predating the split -- use it:
  `Identifier` and case-exact name handling, the pg_catalog default
  generators (views, functions, types, schemas), PostgreSQL `search_path`
  semantics, **triggers** (full kind: `TRIGGER_ENTRY`, entry class, CREATE
  TRIGGER plan), `DATABASE_ENTRY` for attached databases, parser support for
  `CREATE SERVER` and the role/GRANT statements, the remote-catalog surface
  for linked databases, the durable-horizon group commit.
- `server/catalog/` is ~20.6k lines today and is deleted whole. Its
  consumers, by directory: `connector` (50 files), `pg` (37), `search` (11),
  `query` (5), `network` (4), `auth` (3), `storage_engine` (2),
  `rest_server` (1). They are rewired to your new surface; that is part of
  the job. About 16 files outside `server/catalog/` (9 in `pg/`, 6 in
  `connector/`, 1 in `query/`) additionally call duckdb fork API that does
  not exist at your baseline; expect them to be part of the rewrite, not
  just re-pointed.

## The task, in one paragraph

Pretend serenedb never had a catalog. duckdb has one: entries, sets, MVCC
versioning, dependencies, binder, DDL, storage, WAL, checkpoints, attached
databases, and it already implements transactions. Use **all of it, as is**
-- if duckdb has a mechanism, serenedb uses duckdb's, not a re-implementation,
not a wrapper that re-derives what duckdb already computed. Add only what
PostgreSQL needs and duckdb lacks, and add it *as an extension of duckdb's
catalog*, one pattern for every added kind. Build the **tokenizer**
(`CREATE TEXT SEARCH DICTIONARY`) first and make it the reference
implementation the other serenedb-only kinds copy -- the existing tokenizer
code is the one piece of the old implementation you are allowed to read and
reuse (Hard rules). PostgreSQL-specific semantics and RBAC are explicitly
out of scope for the reference implementation: both come at the end, as
phases of their own.

## Phases -- incremental, test-driven

Four phases, one working loop inside each: pick the next test group, make
it pass, re-run everything already green, then move on. The tree compiles at
every step, the set of passing tests only grows, and there is no big-bang
integration at the end.

1. **Reuse + our kinds** -- the reference implementation: duckdb's catalog
   as-is, the added serenedb kinds behaving exactly like duckdb's own
   entities (transactional, versioned, rollback-clean -- see the pattern
   below), consumers rewired. Durability is whatever stock duckdb already
   gives; cluster-wide kinds may stay memory-only until phase 2. duckdb's
   behavior and duckdb's errors are acceptable everywhere. Gate: the
   phase-1 suites in the test table.
2. **The cluster catalog WAL** -- the two-WAL architecture (see the WAL
   section): the global catalog log becomes the durable home of every
   definition, the per-database WALs keep only data, boot order and crash
   reconciliation land. Gate: the recovery suite.
3. **PostgreSQL semantics** -- what PG does differently from duckdb, driven
   suite by suite: name rules and namespaces, oid exposure in the system
   catalogs, conformance, site_docs, the deferred phase-1 tests.
4. **RBAC** -- owners, ACLs, grants, checks; its own design round.

## Extending duckdb's catalog: the pattern

A serenedb-only kind is: a new enumerator in duckdb's `CatalogType`, an entry
class deriving from duckdb's, stored in a duckdb `CatalogSet`, created,
altered, dropped, versioned and dependency-tracked by duckdb's own machinery.
Nothing else -- no side registries, no shadow maps, no parallel lifecycle.

Everything you add must have the same properties duckdb's own entities have,
and it gets them **by construction** -- by going through the same machinery,
never through code that imitates it. A role is transactional exactly like a
table: an uncommitted one is invisible to other transactions, a rollback
removes it without a trace, an alter makes a new version, concurrent writers
get a write-write conflict, and it replays. Copy how duckdb adds an entity
-- entry class, set, create/drop/alter infos -- by analogue, for every kind.

At your baseline, `CatalogType` already has `DATABASE_ENTRY` and
`TRIGGER_ENTRY` (triggers are fully implemented -- reuse). What you add:

- `TOKENIZER_ENTRY` -- the reference kind, built first; the existing
  tokenizer code is readable and reusable (Hard rules).
- `ROLE_ENTRY` -- cluster-wide.
- `FOREIGN_SERVER_ENTRY` -- the parser side (`CREATE SERVER`) already exists;
  the catalog kind does not.

`DATABASE_ENTRY` exists only as duckdb's in-memory attached-database entry;
making databases durable cluster-wide objects is yours to design.

## What duckdb already has -- use it 100%

Schemas; tables, views, indexes (ART), sequences, types, scalar and table
macros; triggers; `CatalogSet` with MVCC version chains and write-write
conflict detection; `DependencyManager` with RESTRICT/CASCADE; the binder and
planner for every CREATE/ALTER/DROP duckdb supports; `DataTable` storage, the
per-database WAL, checkpoints, `ATTACH`/`DETACH`, `DatabaseManager`, storage
extensions; `TransactionManager`/`MetaTransaction` -- transactions are
duckdb's, full stop; default generators (pg_catalog content is already
produced this way); comments; `duckdb_*()` introspection functions; the
`CREATE INDEX` physical plan; sequences with WAL-logged values.

If you find yourself writing any of these, stop.

## What serenedb adds in phase 1 -- you design and add

The tests define the exact behavior. This is a list of *concerns*, not of
classes:

- **Stable object identity.** An object's id never changes across renames or
  alters and is never reused after a drop; the catalog log, dependencies and
  the on-disk artifacts of inverted indexes hang off it. Exposing it as
  PostgreSQL oids in the system catalogs is the PostgreSQL-semantics phase.
- **Databases** as durable, cluster-wide objects (see next section).
- **Roles** as durable, cluster-wide objects (see next section). Privilege
  *enforcement* is the deferred RBAC phase, not this.
- **The cluster-wide catalog log** (see the WAL section) -- built as
  phase 2, once the kinds exist and the basics are green.
- **Tokenizers** (`CREATE TEXT SEARCH DICTIONARY`) -- the reference kind.
- **Inverted indexes and search tables** -- catalog objects whose data lives
  in iresearch (`server/search`, `server/storage_engine`); the catalog owns
  their lifecycle: create/drop artifacts, crash-safe cleanup, DML
  maintenance, rebuild at recovery. duckdb already has a pluggable index
  framework -- `IndexTypeSet` (register a named index type), `BoundIndex` /
  `UnboundIndex`, `CREATE INDEX ... USING <type>`, WAL replay of named index
  types -- build the inverted index on it; propose a fork patch only for a
  proven gap, with approval.
- **Foreign servers** (`CREATE SERVER`) -- durable, re-attached at boot; an
  unreachable remote must not abort startup.
- **Errors: duckdb's are acceptable.** When an operation's *behavior* matches
  PostgreSQL (fails where PG fails, succeeds where PG succeeds) and only the
  error code or wording differs, do not write error-translation code: keep
  duckdb's error and rewrite the test to expect it. Only a behavioral
  difference justifies code.
## Deferred to phase 3: PostgreSQL semantics

Phase 1 takes duckdb's behavior wholesale, plus what the baseline fork
already gives for free (`Identifier` case handling, `search_path`, the
pg_catalog generators). What PostgreSQL does differently comes afterwards,
test-driven:

- **Name semantics.** Unquoted identifiers fold at parse and match exactly;
  one relation namespace per schema shared by tables, views, sequences and
  indexes (index names are schema-level, not per-table);
  `database.schema.object` resolution; `search_path` with `$user`;
  `pg_catalog`/`information_schema` always present and read-only.
- **System catalogs** (`pg_class`, `pg_namespace`, `pg_attribute`,
  `pg_depend`, `pg_roles`, `pg_database`, `pg_index`, `pg_constraint`, ...
  and `information_schema`) **derived** from the live catalog state -- never
  a second bookkeeping. The fork's default generators are the mechanism for
  the static content; extend them for the new kinds. The existing
  system-table/system-view code is readable and reusable (Hard rules) --
  port it onto the new catalog rather than rewriting it.

## The global architecture: two kinds of WAL

This is how the system works and the shape you keep -- built as **phase 2**.
In phase 1, stock duckdb durability stands in; the implementation behind
this architecture is yours to design.

**One global catalog WAL, and one data WAL per database:**

```
<datadir>/
  engine_catalog/catalog.wal      the cluster catalog log -- ONE per instance
  engine_duckdb/<id>.db           one duckdb database file per serenedb database
  engine_duckdb/<id>.db.wal       that database's own duckdb WAL
  engine_search/<db>/...          iresearch artifacts (inverted indexes, search tables)
  pg_hba.conf                     auth rules -- a plain file, never catalog state
```

- **The catalog WAL** is the only durable home of every *definition*: roles,
  databases, schemas, tables, views, indexes, sequences, types, functions,
  tokenizers, foreign servers -- plus the small non-entry state that must
  survive with them (the id allocation horizon, sequence counter values). It
  currently reuses duckdb's own `WriteAheadLog` format and serializer over a
  serenedb path; keep reusing duckdb's WAL machinery rather than inventing a
  format. The owner intends this log to become the replication (raft) log of
  a future cluster: an append that can be *refused* must surface as an
  ordinary aborted transaction, and nothing may assume the log is local
  forever.
- **Each database's data WAL** is stock duckdb: it holds the rows (inserts,
  deletes, updates) and duckdb's storage-side records, and is folded by
  duckdb checkpoints. Definitions are never durable here -- a definition
  record in a data WAL would be a second source of truth.
- **Ordering invariant:** within a commit, the catalog WAL is made durable
  before the database's own commit is. After a crash the catalog can be
  *ahead* of the rows, never behind; ahead is repairable, because storage is
  (re)built from definitions at boot.
- **Boot order:** replay the catalog WAL completely first; then open each
  database's file and replay its data WAL against the already-complete
  catalog; then reconcile -- build storage the definitions promise but the
  files lack, and sweep on-disk leftovers (database files, search
  directories, counters) whose ids no committed definition names. Ids are
  never reused, so "unreferenced id" is a complete definition of garbage.
- One transaction may combine per-database DDL with cluster-wide changes
  (`CREATE TABLE` + `CREATE ROLE`; `CREATE DATABASE`); commit and rollback
  are atomic across both. duckdb's one-writable-database-per-transaction rule
  exists because a commit cannot span two logs -- resolve this without
  breaking duckdb's invariants.
- Crash-safety bar: committed statements survive `kill -9` at any instant; a
  rolled-back or crashed-out statement leaves nothing that boot cannot
  account for. The recovery suite (`tests/sqllogic/recovery/`, ~220 tests)
  kills and restarts servers mid-test and injects faults via
  `SET sdb_faults` (allowed only under `recovery/`;
  `scripts/check_fault_points.py` enforces it). Read a few before designing:
  they are the durability spec.

**Databases:** PostgreSQL semantics -- a connection is to exactly one
database; no cross-database references; `CREATE`/`DROP`/`ALTER DATABASE`;
`DROP DATABASE` removes everything the database owned, including its file and
search artifacts; the default database `postgres` exists from first boot.
Every database has `public`, `pg_catalog`, `information_schema`. Each
database is its own duckdb file (above); how attach/detach and lifetimes work
is your design, built on duckdb's attach machinery.

**Roles:** cluster-wide objects: `CREATE`/`ALTER`/`DROP ROLE`, attributes
(SUPERUSER, LOGIN, CREATEDB, ...), membership (`GRANT role TO role`),
SCRAM password verifiers (login must work; `POSTGRES_PASSWORD` seeds the
root role `postgres` on first boot), `VALID UNTIL`, connection limit,
per-role `ALTER ROLE ... SET`. In this reference implementation roles carry
**no privileges and nothing checks any**: no owners, no ACLs, no
GRANT-on-object, no permission errors. That entire dimension is the RBAC
phase at the end.

## RBAC: deferred to the end

The reference implementation contains **no RBAC code**. Ownership, ACLs,
`GRANT`/`REVOKE` on objects, `ALTER DEFAULT PRIVILEGES`, `has_*_privilege`,
permission checks at bind -- none of it. `server/auth/` and the RBAC command
and optimizer code go out of the build with the old catalog and come back
redesigned in the final phase. Defer the RBAC suites with them:
`tests/sqllogic/any/pg/rbac/` (256 tests), `tests/sqllogic/sdb/pg/rbac/`
(24), and the `rbac_*` recovery tests are the acceptance gate of that last
phase, not of the rewrite. Design nothing that makes them unreachable --
stable ids and roles-as-objects are exactly what grants will attach to.

## The spec: tests

Compile checks any time. Test runs are the working loop: run the group you
are making pass as often as you need, and re-run the already-green groups
before moving on -- that cadence is pre-agreed, no need to ask. *Read* the
suites before designing -- they are the acceptance criteria. Behavior is
never weakened to pass; the stated exceptions are error wording (see
"Errors" above) and recorded deferrals to the PostgreSQL-semantics phase
(below):

| Phase | Suite | Tests | What it pins |
|---|---|---|---|
| 1 | `sdb/pg/simple`, `any/pg/simple`, `sdb/pg/dml`, `any/pg/txn` | ~130 | basics: tables, DML, transactions |
| 1 | `sdb/pg/ddl/`, `any/pg/ddl/` | ~70 | DDL surface, cascades |
| 1 | `sdb/pg/index/` | 218 | inverted indexes, search tables |
| 2 | `tests/sqllogic/recovery/` (minus `rbac_*`) | ~210 | durability, crash windows, restart, orphan cleanup |
| 3 | `sdb/pg/system/`, `any/pg/system/`, conformance | ~37 | pg_catalog / information_schema, oid exposure |
| 3 | `sdb/pg/site_docs/` | 322 | everything the documentation promises |
| 3 | the remaining small suites (`settings`, `temporal`, `explain`, ...) | ~30 | odds and ends |
| 4 | `any/pg/rbac/`, `sdb/pg/rbac/`, `rbac_*` recovery | ~290 | owners, grants, ACLs |

Also `tests/duckdb/` (duckdb's own suites, run by CI) must stay green.
Unit tests under `tests/server/basics/catalog_*` test the old serialization
and go with it; write new gtests only for what sqllogic cannot reach. Where
tests are silent, duckdb's behavior stands until the PostgreSQL-semantics
phase; PostgreSQL 18 is the reference from there on.

Sqllogic conventions (bare `query`, control directives, connection naming,
retry patterns) are set by example: read a sibling `.test` before writing
one. RBAC-flavored statements inside non-RBAC tests (a stray `GRANT`) parse
and succeed as no-ops until the RBAC phase; if a non-RBAC test *asserts* a
permission error, flag it to the owner instead of implementing checks early.
A phase-1 test that turns out to hinge on PostgreSQL-only semantics moves to
the PostgreSQL-semantics phase the same way: recorded in a list for the
owner, never silently skipped.

## Suggested order of work

1. **Baseline.** Confirm the submodule pin. Build duckdb alone. Inventory
   what each consumer directory needs from the catalog (the *questions they
   ask*, not the functions they call today).
2. **Design document** (short; a new markdown file at the repo root): the
   duckdb extension points you will use; the added kinds and the one pattern
   they follow (tokenizer as the worked example); identity; databases, roles
   and the catalog log against the architecture above; the new public
   surface for consumers; which fork patches, if any, and why; the on-disk
   layout. **Stop and get approval.**
3. **Delete `server/catalog/`** (and the RBAC code paths) on the branch and
   bring the build back in layers, each compiling before the next:
   duckdb-native kinds -> stable identity -> tokenizers (the reference kind)
   -> roles, databases -> inverted indexes, search tables, foreign servers.
4. **Rewire consumers**; delete every adapter you were tempted to keep
   "temporarily".
5. **Phase-1 tests**, group by group, keeping earlier groups green:
   simple/dml/txn -> ddl -> index.
6. **Phase 2 -- the cluster catalog WAL**: definitions move into the global
   log; boot order and crash reconciliation; gate = the recovery suite
   (`tests/sqllogic/run_recovery_tests.sh`, not `run.sh`).
7. **Phase 3 -- PostgreSQL semantics**, suite by suite: system ->
   conformance -> site_docs -> the deferred phase-1 tests.
8. **Phase 4 -- RBAC**: design (approval again), implement
   owners/ACLs/checks, green the RBAC suites.
9. **Measure and report**: lines deleted vs added, server and fork; each
   fork patch with a one-sentence justification; open items.

## Settled decisions

- **No on-disk compatibility.** The new catalog need not open a datadir
  written by today's binary; a fresh datadir is fine, and the old golden
  serialization fixtures die with the old code.
- **Incremental and test-driven**: the loop in "Phases" is the cadence --
  no need to ask before test runs.
- **PostgreSQL semantics come at the end**, like RBAC: phase 1 accepts
  duckdb behavior wholesale.
- **Inverted indexes ride duckdb's own index framework** (`IndexTypeSet` et
  al., above); fork hooks only for a proven gap, approval first.
- **Error differences are not defects** (see "Errors" above).
- **The result is small.** Reuse means the new server-side catalog is a
  fraction of the ~20.6k lines it replaces; a piece that grows large is
  re-implementing duckdb -- stop and reconsider.

## Traps (conceptual)

- duckdb allows one writable attached database per transaction; PostgreSQL
  statements routinely touch cluster-wide and per-database state together.
  Your design must resolve this without breaking duckdb's invariants.
- Anything keyed by *name* breaks under RENAME; PostgreSQL semantics are by
  oid. Dependencies and storage references included.
- An error raised by duckdb where PostgreSQL words it differently is not a
  defect: keep duckdb's error, rewrite the test (see "Errors" above).
- duckdb's restrictions are acceptable in phase 1, but do not *design them
  in*: the PostgreSQL-semantics phase must be able to lift them without
  another rewrite (schema rename; index names moving to the schema's
  relation namespace).
- The `public` schema, `pg_catalog` and `information_schema` exist in every
  database from the moment it exists; the two static ones have fixed oids
  and nobody owns them.
- Search artifacts and any per-database files are outside duckdb's
  transactions; a crash between "decided" and "cleaned up" must be
  reconcilable at boot from the catalog alone.
- Sequences: `nextval` must never hand out a value twice across a crash and
  must not serialize on the catalog.
- Concurrency of DDL vs DML: DDL on an object must not block unrelated DML;
  the recovery and index suites include mid-build DML.
- Do not read the old implementation to "see how it handled X". If the tests
  do not say, duckdb's behavior answers until the PostgreSQL-semantics phase,
  PostgreSQL 18 from there on.
