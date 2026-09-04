# Design: the new SereneDB catalog (`server/catalog1/`)

Design document for the rewrite described in `PLAN.md`. Written against duckdb
submodule pin `76c226cde3b74279522b566efbd04148dc883484`, verified present in
the working tree. Scope of this document: **phase 1** (reuse + our kinds), with
the phase-2/3/4 seams named so they can be built without another rewrite.

The new code lands in `server/catalog1/` so the tree keeps building while the
old `server/catalog/` is still present; the folder is renamed to
`server/catalog/` when the old one is deleted.

---

## 0. Baseline facts this design rests on

Verified in the pinned tree, not assumed:

| Fact | Where |
|---|---|
| `CatalogType` has 26 enumerators, `TRIGGER_ENTRY = 11` is the last kind before a gap at 12–24 | `src/include/duckdb/common/enums/catalog_type.hpp:18-53` |
| `TOKENIZER_ENTRY` / `ROLE_ENTRY` / `FOREIGN_SERVER_ENTRY` **do not exist** | grep, 0 hits |
| `CatalogEntry::oid` exists, is `idx_t`, comes from `DatabaseManager::NextOid()` (atomic, seeded 20000), is **preserved across ALTER** and is **never serialized** | `catalog_entry.hpp:43`, `catalog_entry.cpp:17-19`, `catalog_set.cpp:364-365` |
| `CatalogSet` stores `identifier_tree_t<unique_ptr<CatalogEntry>>`; MVCC is a `child`/`parent` version chain; conflicts throw `TransactionException("Catalog write-write conflict on create/alter with \"%s\"")` | `catalog_set.hpp:45-50`, `catalog_set.cpp` |
| Dependencies are keyed by **mangled name** (`type\0schema\0name`), never by id; they are not persisted but rebuilt from `CreateInfo::dependencies` | `dependency_manager.cpp:33-45`, `wal_write_state.cpp:156` |
| `TRIGGER_ENTRY`'s `CatalogSet` lives on `DuckTableEntry`, not on the schema — a `CatalogSet` may live anywhere | `duck_table_entry.hpp:124` |
| `TransactionManager::ForwardWrites()` already exists and exempts a database from the one-writable-database rule | `transaction_manager.hpp:56`, `meta_transaction.cpp:308` |
| Stock `DuckTransactionManager` dereferences `db.GetStorageManager()` unconditionally on the read-write commit path; `GetStorageManager()` throws when `storage` is null | verified — **a storage-less non-system attachment cannot commit** |
| `CREATE DATABASE x` is already rewritten by the grammar to `ATTACH '' AS x (TYPE serenedb)`, `DROP DATABASE x` to `DETACH x` | `transform_create_database.cpp:7-35` |
| `WriteAheadLog`'s ctor takes `(StorageManager &, path, ...)` — the path is free, the `StorageManager &` is not; `WriteAheadLogSerializer`/`Deserializer` are private to their `.cpp` | `write_ahead_log.hpp:49-51` |
| duckdb hard-casts a database's `Catalog` to `DuckCatalog` at four points on the WAL/checkpoint path | `write_ahead_log.cpp:143,163`, checkpoint path |
| `CREATE TEXT SEARCH DICTIONARY` / `CREATE SERVER` / the role+GRANT surface already parse, but dead-end as `PragmaStatement`s, not `CreateInfo`s | `create_text_search_dictionary.gram`, `transform_create_*.cpp` |
| A custom index type gets `create_instance` + `create_plan`; the default `CREATE INDEX` plan hard-casts to `DuckTableEntry`/`DuckIndexEntry`, so a custom kind must set `create_plan` | `plan_create_index.cpp`, `physical_create_index.cpp` |

Two consequences worth stating up front, because they kill otherwise-obvious designs:

- **The cluster catalog cannot be a storage-less attachment.** It must own a
  real `StorageManager` (its own file + WAL) or it cannot commit.
- **`SereneDBCatalog` must stay `Cast<DuckCatalog>`-able.** It derives from
  `DuckCatalog`, never from `Catalog` directly.

---

## 1. Shape

```
                       DatabaseInstance
                              │
        ┌─────────────────────┼──────────────────────────┐
        │                     │                          │
  AttachedDatabase      AttachedDatabase           AttachedDatabase
   "system" (duckdb)     __cluster__                 postgres, …
        │                (TYPE serenedb_cluster)     (TYPE serenedb)
        │                     │                          │
   DuckCatalog          ClusterCatalog             SereneDBCatalog
                        : SereneDBCatalog          : DuckCatalog
                              │                          │
                    CatalogSet roles                CatalogSet schemas
                    CatalogSet databases                  │
                                                   SereneDBSchemaEntry
                                                   : DuckSchemaEntry
                                                     + CatalogSet tokenizers
                                                     + CatalogSet foreign_servers
```

Every box is duckdb machinery. serenedb contributes three entry classes, two
catalog subclasses, one schema subclass and an id allocator — nothing else.

### Placement of each kind

| Kind | `CatalogSet` lives on | Scope | Rationale |
|---|---|---|---|
| `TOKENIZER_ENTRY` | `SereneDBSchemaEntry` | schema | PG's `pg_ts_dict` is schema-qualified; index opclass resolution looks the dictionary up in the *target table's schema* |
| `FOREIGN_SERVER_ENTRY` | `SereneDBCatalog` | database | PG's `pg_foreign_server` is a per-database, non-schema-qualified catalog |
| `ROLE_ENTRY` | `ClusterCatalog` | cluster | PG roles are shared |
| `DATABASE_ENTRY` | `ClusterCatalog` | cluster | this is the durable database list; duckdb's `AttachedDatabase`-as-`DATABASE_ENTRY` stays the *runtime* attachment |

`DATABASE_ENTRY` is deliberately **two things**: duckdb keeps owning the live
attachment (`AttachedDatabase`, in `DatabaseManager`'s plain map), while the
cluster catalog owns the durable *definition*. The definition is authoritative;
the attachment is derived from it at boot and on demand.

---

## 2. Stable object identity

### The requirement

An id that never changes across rename/alter, is never reused after a drop,
survives restart, and is what dependencies, the catalog log and the on-disk
search artifacts hang off.

### The design

duckdb already gives two thirds of this: `CatalogEntry::oid` is stable across
ALTER by construction (`catalog_set.cpp:364-365` copies it onto the new
version). What it lacks is persistence and non-reuse.

So: **serenedb owns the number, duckdb keeps carrying it.**

```
ObjectId  ── a strong 64-bit id (server/catalog1/object_id.h)
              reserved low range for static objects (pg_catalog,
              information_schema, the root role, the system views)
              allocated ids start at id::kFirstDynamic

IdAllocator ── monotonic counter with a persisted horizon.
               Hands out ids one at a time; persists a horizon
               `allocated + kHorizonStep` so a crash burns at most
               kHorizonStep ids and never reissues one.
               Same trick as sequences, same reason.
```

The id reaches the entry by riding on `CreateInfo`:

- `CreateInfo` gains one field, `idx_t catalog_oid = 0`, serialized under a new
  field id. This is **fork patch #2** (§7).
- `InCatalogEntry`'s construction path uses `info.catalog_oid` when non-zero,
  else falls back to `DatabaseManager::NextOid()`. Every existing kind therefore
  keeps working untouched.
- `CatalogEntry::GetInfo()` writes `oid` back into the info, so checkpoint and
  WAL round-trip it for free — no separate id table, no shadow map.

This is the smallest patch that satisfies "the catalog log, dependencies and the
on-disk artifacts hang off it" without a side registry. Everything else
(`pg_class.oid` stability, oid→entry lookup) falls out of it.

**Reverse lookup by id** (`LookupEntryById`) is a genuine gap — duckdb has no
by-id index anywhere. It is served by scanning, not by a second map: the system
catalog's callers are already whole-catalog enumerations
(`Visit<T>`, `VisitTableEntries`, …), and a by-id probe walks the same sets. A
cache is a phase-3 optimisation, and it must be derived state, never a second
source of truth.

**Non-reuse across a drop** is what makes the boot-time orphan sweep sound:
"no committed definition names this id" is then a complete definition of
garbage, exactly as `PLAN.md` requires.

---

## 3. The pattern for an added kind

One pattern, applied identically to all three added kinds. The template is
duckdb's own `TRIGGER_ENTRY`, which was added the same way and is the only
worked example in the tree. For a kind `X`:

**In the fork (patch #1, §7):**
1. `CatalogType::X_ENTRY = <n>` in `catalog_type.hpp` (new values only, never a
   renumber — the enum is on-disk format).
2. An arm in `CatalogTypeToString` and `CatalogTypeFromString`.
3. An entry + a bumped count in `enum_util.cpp`'s `CatalogType` array.
4. An arm in `EntryToString` (`dependency_manager.cpp:381`) — its `default:`
   throws `InternalException`, so a kind that ever appears in a dependency error
   *must* be listed.

**In `server/catalog1/` (no fork change):**
5. `CreateXInfo : duckdb::CreateInfo` — the definition, `Copy()`, `Serialize()`,
   `Deserialize()`, `ToString()`.
6. `XCatalogEntry : duckdb::StandardEntry` (schema-scoped) or
   `duckdb::InCatalogEntry` (catalog-scoped) with `Copy()`, `GetInfo()`,
   `ToSQL()`, and — where the kind supports it — `AlterEntry()`.
7. A `CatalogSet` member on whichever entry owns the scope, plus a
   `GetCatalogSet(CatalogType)` arm.
8. `CreateX` / `DropX` / `ScanX` on that owner, going straight to
   `CatalogSet::CreateEntry` / `DropEntry` / `Scan`. No bespoke lifecycle.

That is the whole pattern. What the kind gets **by construction**, because it
went through `CatalogSet`: MVCC visibility, write-write conflict detection,
rollback that leaves no trace, dependency tracking, `OnCreateConflict` handling,
similar-name suggestions.

### Why the DDL entry point stays a pragma in phase 1

`CREATE TEXT SEARCH DICTIONARY` and `CREATE SERVER` already parse in the fork,
but the grammar lowers them to `PragmaStatement`, not `CreateStatement`. Moving
them onto `CreateStatement` means editing the PEG grammar, `grammar_types.yml`,
regenerating four artifacts, and adding arms to `Binder::Bind(CreateStatement&)`
and `PhysicalDrop` — a fork patch per kind, for no phase-1 behavioural gain.

Phase 1 therefore keeps the pragma entry point and has the handler build the
`CreateXInfo` and call the catalog. The kind is a *real* catalog kind either
way — MVCC, rollback and dependencies all come from `CatalogSet`, not from the
statement class. Promoting the statements is a phase-3 item (it is what buys
`IF NOT EXISTS`/`OR REPLACE` from the binder), and it does not invalidate
anything built here.

---

## 4. The reference kind: tokenizer

Built first, and the other two copy it verbatim.

A tokenizer is durably two values: an `irs::analysis::TokenizerConfig` (a
move-only 23-arm variant, cloned via `irs::analysis::Clone`) and a
`sdb::search::Features` bitmask. In memory it additionally holds a pooled set of
built `irs::analysis::Analyzer` instances.

```
CreateTokenizerInfo : duckdb::CreateInfo
    ObjectId                        id
    irs::analysis::TokenizerConfig  config
    sdb::search::Features           features

TokenizerCatalogEntry : duckdb::StandardEntry
    static constexpr CatalogType Type = CatalogType::TOKENIZER_ENTRY
    ObjectId Id() const
    const TokenizerConfig& Config() const
    Features GetFeatures() const
    TokenizerWrapper Acquire() const          // pooled analyzer, RAII return
```

Three properties carried over from the existing integration because they are
load-bearing, not incidental:

- **The analyzer pool is unversioned.** A reader that resolved a tokenizer keeps
  tokenizing with that instance. This is safe precisely because the entry is
  MVCC — the reader holds a pinned version.
- **`template = 'copy_from'` is a CREATE-time snapshot, not a reference.** It
  copies the source config; it creates **no** dependency edge. Keep it that way:
  a dependency here would change `DROP` behaviour the index suite pins.
- **Lookup is by name for DDL/queries, by id for index storage.** Each inverted
  index column persists `text_dictionary = ObjectId`; resolution during WAL
  replay happens with no `ClientContext`, so the by-id path must work off a
  committed-only transaction (`CatalogTransaction::GetSystemTransaction`).

The tokenizer→index dependency (which makes `DROP TEXT SEARCH DICTIONARY` fail
while an index uses it, pinned by the index suite) is a normal
`LogicalDependencyList` entry on the index's `CreateInfo`. duckdb's
`DependencyManager` produces the exact refusal text the tests assert.

---

## 5. Roles and databases

### Roles

`RoleCatalogEntry : InCatalogEntry`, in `ClusterCatalog`'s `roles` set.
Attributes, taken from what consumers actually read (24 `FindRole` sites, the
`auth::RoleGraph`, and the `pg_authid`/`pg_roles` projections):

```
ObjectId id; Identifier name;
RoleOption options;              // Superuser|Inherit|CreateRole|CreateDb|Login|Replication|BypassRls
int32_t   conn_limit;            // kNoConnLimit
int64_t   valid_until;           // kNoValidUntil
string    password;              // SCRAM-SHA-256$<iters>:<salt>$<stored>:<server>  or md5<32hex>
vector<Membership> member_of;    // {role, admin_option, inherit_option, set_option}
vector<string>     config;       // "name=value"
```

Deliberately **absent in phase 1**: owner, ACL, `DefaultAcls`. Those are RBAC
and belong to phase 4. Role *existence* and the *membership graph* are not
deferrable — login and `pg_auth_members` need them now — but nothing checks a
privilege.

Login reads roles with a **null `ClientContext`**, before any transaction
exists (`network/server.cpp:126`, `pg/connection_context.cpp:39`,
`pg_wire_session.cpp:888`). So the read surface must offer a committed-only,
sessionless path: `CatalogTransaction::GetSystemTransaction(db)`.

`auth::RoleGeneration` is a process-global counter the role DDL must bump on
commit, so the flattened role graph cache invalidates. That is the one piece of
non-catalog state role DDL touches.

### Databases

`DatabaseCatalogEntry : InCatalogEntry` in `ClusterCatalog`'s `databases` set:
`{ ObjectId id, Identifier name, ObjectId public_schema_id }`. Every other
`pg_database` column is a constant today and stays one.

Lifecycle rides duckdb's attach machinery, because the grammar already routes
there:

- `CREATE DATABASE x` → `ATTACH '' AS x (TYPE serenedb)` → our
  `StorageExtension("serenedb")::attach` → allocate `ObjectId`, create the
  `DatabaseCatalogEntry` in the cluster catalog, rewrite `info.path` to
  `<datadir>/engine_duckdb/<id>.db`, return a `SereneDBCatalog`. Because the
  returned catalog `IsDuckCatalog()`, duckdb still builds a real
  `SingleFileStorageManager` over that path — we get the file, the WAL, the
  checkpoints for free.
- `DROP DATABASE x` → `DETACH x` → `Catalog::OnDetach` drops the
  `DatabaseCatalogEntry` and schedules artifact removal.

Two things the baseline does *not* give and that the design must supply:

1. **The attach is not MVCC** — `DatabaseManager` holds a plain map, and
   `DETACH` is not transactional at all. Phase 1 accepts this (duckdb's
   behaviour stands); the durable definition in the cluster catalog *is*
   transactional, so a rolled-back `CREATE DATABASE` leaves no definition even
   though the file may survive. The boot sweep reclaims the file: its id is
   named by no committed definition.
2. **No double-open protection for a non-`duckdb` `db_type`** — but duckdb takes
   an `fcntl` write lock on the `.db` file itself, so a second *process* is
   still refused.

The `postgres` database and the `postgres` superuser role must exist before
`Server::StartListeners()`; both are seeded on first boot, the role's verifier
from `POSTGRES_PASSWORD`. `id::kRootUser` stays 1000000.

---

## 6. The two-WAL architecture (phase 2 seam)

Phase 1 uses stock duckdb durability and lets cluster kinds be memory-only, as
`PLAN.md` allows. The design below is what phase 2 builds; phase 1 must not
foreclose it.

```
<datadir>/
  engine_catalog/catalog.wal    the cluster catalog log — ONE per instance
  engine_duckdb/<id>.db(.wal)   one duckdb database + its data WAL
  engine_search/<db>/…          iresearch artifacts
  pg_hba.conf                   a plain file, never catalog state
```

**How the cluster log reuses duckdb's WAL machinery.** `WriteAheadLog` needs a
`StorageManager &`, and `WriteAheadLogSerializer`/`Deserializer` are private to
their translation units — so a hand-rolled record writer is not available
without a large fork patch. The cheap, honest route is therefore: the cluster
catalog **is an attached database** (`__cluster__`, `TYPE serenedb_cluster`)
with a real `StorageManager` pointed at `engine_catalog/`. Its WAL *is*
duckdb's WAL, its records *are* duckdb catalog records, its replay *is*
duckdb's replay. No new format, no new serializer, no fork patch.

**Crossing the one-writable-database rule -- UNRESOLVED.** duckdb exempts a
database whose `TransactionManager::ForwardWrites()` returns true, and the
exemption exists at the baseline. But its meaning is literal: *"forwards its
writes to another database, so it never occupies the single-writable-db slot"*.
A cluster catalog with its own storage and its own WAL does not forward
anything, so returning true there would not resolve the rule -- it would
disable the check while `CREATE TABLE t; CREATE ROLE r;` committed to two
independent WALs with two independent fsyncs and no atomicity. That is exactly
the invariant PLAN.md asks the design to preserve, not to switch off.

Two honest options, to be decided in phase 2 rather than pre-empted here:
(a) the cluster genuinely forwards -- its records are appended to the
per-database WAL of whichever database the transaction is writing, and
`ForwardWrites()` is then true and true in substance; or (b) the cluster keeps
its own log and the ordering invariant below carries the weight, in which case
the exemption is the wrong mechanism and duckdb needs a different seam.

**Ordering.** Within a commit the cluster log is made durable before the
per-database commit. `MetaTransaction::Commit` fans out in reverse first-touch
order and each `DuckTransactionManager::CommitTransaction` is independently
durable when it returns, so ordering is achieved by controlling touch order,
not by a new protocol. After a crash the catalog can be ahead of the rows,
never behind — and ahead is repairable because storage is rebuilt from
definitions.

**Boot order.** Replay the cluster log completely → open each database file and
replay its data WAL against the already-complete catalog → reconcile: build
storage the definitions promise but the files lack, and sweep artifacts whose id
no committed definition names.

**The refusable append.** The owner intends this log to become a raft log. So
the append path returns a failure that surfaces as an ordinary aborted
transaction (`TransactionException`), never an assert or a crash, and nothing
assumes the log is local.

---

## 7. Fork patches

Every patch, with its justification. Nothing here is speculative — each is
required by a compile error or a `throw` in duckdb's own `default:` arm.

| # | Patch | Files | Why |
|---|---|---|---|
| 1 | Three `CatalogType` enumerators (`TOKENIZER_ENTRY = 12`, `ROLE_ENTRY = 13`, `FOREIGN_SERVER_ENTRY = 14`) plus their `CatalogTypeToString`/`FromString`/`enum_util`/`EntryToString` arms | `catalog_type.hpp`, `catalog_type.cpp`, `enum_util.cpp`, `dependency_manager.cpp` | `PLAN.md` defines an added kind *as* a new enumerator. `EntryToString`'s `default:` throws, so a kind appearing in a dependency error must be listed. |
| 2 | `CreateInfo::catalog_oid` + one serialized field, honoured by the `CatalogEntry` ctor and written back by `GetInfo()` | `create_info.hpp/.cpp`, `catalog_entry.cpp` | Stable identity. Without it no id survives a restart and the boot sweep has no definition of garbage. Smallest possible alternative to a side registry. |

Patches explicitly **not** taken in phase 1, and why:

- *Promoting the tokenizer/server/role statements from `PragmaStatement` to
  `CreateStatement`* — no phase-1 behavioural gain (§3), large regen surface.
- *Checkpoint/WAL arms for the added kinds* — phase 2 puts definitions in the
  cluster log; adding per-kind arms to the per-database checkpoint now would
  build the second source of truth phase 2 exists to remove.
- *Any inverted-index hook* — the index framework's `create_plan` escape hatch
  plus `IndexStorageInfo::options` covers what is needed. `PLAN.md` requires a
  proven gap and approval; nothing is proven yet.

---

## 8. Public surface for consumers

Consumers ask a small, stable set of questions. The surface is free functions
over duckdb types, not a facade object — anything that re-derives what duckdb
already computed is out.

```
// identity
ObjectId  IdOf(const duckdb::CatalogEntry&);
optional_ptr<CatalogEntry> LookupEntryById(ClientContext*, ObjectId);

// resolution — a null ClientContext means "committed, sessionless"
optional_ptr<RoleCatalogEntry>          FindRole(ClientContext*, string_view);
optional_ptr<DatabaseCatalogEntry>      FindDatabase(ClientContext*, string_view);
optional_ptr<TokenizerCatalogEntry>     FindTokenizer(ClientContext&, QualifiedName);
optional_ptr<ForeignServerCatalogEntry> FindForeignServer(ClientContext&, string_view);

// enumeration — every system-catalog projection is one of these
void VisitRoles(ClientContext*, callback);
void VisitDatabases(ClientContext*, callback);
void VisitTokenizers(ClientContext&, callback);
void VisitForeignServers(ClientContext&, callback);

// DDL
optional_ptr<CatalogEntry> CreateTokenizer(ClientContext&, CreateTokenizerInfo&);
optional_ptr<CatalogEntry> CreateRole(ClientContext&, CreateRoleInfo&);
optional_ptr<CatalogEntry> CreateForeignServer(ClientContext&, CreateForeignServerInfo&);
void DropEntryObject(ClientContext&, CatalogType, QualifiedName, bool cascade, bool missing_ok);
```

Two non-functional contracts the system-catalog layer states repeatedly and that
must survive the rewrite:

1. A projection must **never re-enter the `CatalogSet` it is walking** — the
   lock is not recursive.
2. An entry version reached by a set scan stays **pinned for the transaction**
   (rows may borrow `string_view`s into it), whereas anything read through
   `GetInfo()` is pinned only for the callback and must be copied.

---

## 9. What is deliberately not built

- **RBAC** — no owners, no ACLs, no `GRANT`, no permission checks. `GRANT` in a
  non-RBAC test parses and no-ops. Phase 4.
- **PostgreSQL name semantics** — phase 1 takes duckdb's case-insensitive
  `Identifier` comparison and duckdb's per-kind namespaces. The parser already
  folds unquoted identifiers (`preserve_identifier_case`), which is most of what
  the phase-1 suites need. One relation namespace per schema, schema-level index
  names, and case-exact matching are phase 3 — the design must let them land
  without another rewrite, which is why nothing keys off a per-kind namespace.
- **Error translation** — duckdb's errors stand. Where behaviour matches PG and
  only wording differs, the test is rewritten, not the code.

---

## 10. Open questions for the owner

1. **Fork patch #2 (`CreateInfo::catalog_oid`)** — this is the one design choice
   with no alternative that stays inside "no side registries". Confirm it is
   acceptable before it is relied on.
2. **`FOREIGN_SERVER_ENTRY` scope** — designed here as database-scoped
   (PostgreSQL's `pg_foreign_server` is a per-database catalog). If serenedb
   intends servers to be cluster-wide (they back linked databases), the set
   moves to `ClusterCatalog` and everything else is unchanged.
3. **Cluster catalog naming** — `__cluster__` as the attached database name;
   it must be hidden from `SHOW DATABASES` (`AttachVisibility::HIDDEN` exists).
4. **Phase-1 durability of roles** — `PLAN.md` permits memory-only until
   phase 2. Confirm the recovery suite is not expected to pass before phase 2.
