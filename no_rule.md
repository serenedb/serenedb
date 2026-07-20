# Inline relation/column RBAC in the binder — delete the post-bind rule

Status: **DESIGN + FALSE-START DOCUMENTED. Reverted to the working post-bind rule.**
Attempted 2026-07-16; blocked on the store-orphan facade problem (see §Blocker). Pick up later.

## Goal

Today table/column privilege enforcement is a detached post-bind pass. The binder collects an
opaque carrier (`StatementProperties.access_requirements`, one `AccessRequirement{table, who, verb,
read/write}` per base relation) and, after the plan is fully bound, `CollectAndEnforce`
(`server/connector/optimizer/rbac.cpp:140`, installed as `db.config.access_check_function`, invoked
once at `third_party/duckdb/src/main/client_context.cpp:520`) re-resolves each carrier entry back to
a `catalog::Object` and throws 42501.

That indirection is a recurring bug source: the rule re-interprets identities the binder recorded,
and every duckdb bump shifts them (the store-orphan remap, the RETURNING global-state share, the
projection-translation layer all exist to paper over it). Move the check **into** the binder as two
injected hooks that run while the entry is in hand — completing the migration already done for
functions/types (checked at catalog lookup) and DDL (checked inside the catalog).

## Design (Option A — two hooks, NO finalize pass) — this is the agreed design

Add two C function-pointer hooks to `DBConfig` (like the existing `access_check_function`,
`config.hpp:203`), null for stock duckdb, installed by serened:

```cpp
// true  => table-authorized (skip this verb's column checks)
// false => column-gated (each touched column must clear rbac_check_column)
// throws 42501 => total denial (no table grant AND no column grant for the verb),
//                 or schema-USAGE / view / system-relation denial
bool (*rbac_check_table)(ClientContext&, const CatalogEntry& table,
                         const CatalogEntry* who, AccessVerb verb);
// no-op if the verb was table-authorized; else throws 42501 if this column lacks the grant
void (*rbac_check_column)(ClientContext&, const CatalogEntry& table,
                          const CatalogEntry* who, idx_t logical_col, AccessVerb verb);
```

- `check_table` at the `RecordAccess` site (table-ref / DML target). Owns the ordering the old rule
  had (rbac.cpp:215-248): system-schema → schema-USAGE-first → view SELECT → table `Can(verb)`; if
  no table grant, `CanAnyColumn(verb)` decides throw-now (total denial) vs return-column-gated.
- `check_column` at the `RecordRead` site (a resolved column) and in the DML write-column loop.
  No-op when the verb is table-authorized; else per-column `CanColumns`, throw on miss.
- **No finalize/third hook.** Zero-column reads (`count(*)`, `SELECT 1`, `WHERE false`, `EXISTS`,
  whole-row `update_is_del_and_insert`) are decided by `check_table`'s `CanAnyColumn`: no grant on
  any column ⇒ deny at the table hook; some column grant ⇒ allow, the (zero) column checks pass.
  This is exactly PG (`enf_zero_column_count`: one column grant makes `count(*)` succeed).
- **Timing is safe**: `BindColumn` runs before `RecordRead`, so an unknown column already threw
  42703; the column hook only sees resolved columns. Invariant "42703 before 42501" holds for free.
- The binder keeps a per-(table_index, verb) "table-authorized" bit (a small `unordered_set<idx_t>`
  keyed by `(table_index<<8)|verb` on `GlobalBinderState`) so the column hook can no-op.
- `who` fold (RULE R) stays as `EffectiveDefiner()` (`binder.cpp:229`): innermost enclosing
  VIEW_BINDER — definer ⇒ view owner, invoker/none ⇒ caller. Functions (EXECUTE) / types (USAGE)
  are NOT in this path — they check the caller at catalog lookup (`duckdb_schema_entry.cpp:139`).
  Do not touch.

### Write path (the part that bit us — get it right)

Reads charge at `RecordRead`. Writes do NOT go through `RecordRead` — they're a bind-site column
set. So charge at both entry points, and preserve:
1. **Per-verb, not per-relation.** One UPDATE = UPDATE on SET cols + SELECT on WHERE/SET-RHS/
   RETURNING cols on the same relation ⇒ the table-authorized bit is per-(relation, verb).
2. **Written columns charged in a loop at the DML site.** INSERT `named_column_map`
   (bind_insert.cpp:729) is already `LogicalIndex` — use directly. UPDATE `update->columns`
   (bind_update.cpp:322) is **PhysicalIndex** — convert:
   `table.GetColumns().GetColumn(phys).Logical().index` (ColumnList has both overloads).
3. **`update_is_del_and_insert` ⇒ empty write set** ⇒ total-denial already caught by
   `check_table`'s `CanAnyColumn(UPDATE)`. No special code.
4. MERGE: `check_table` once per verb bit present (UPDATE/DELETE/INSERT), like del-and-insert.

## Blocker (why it was reverted) — the store-orphan facade problem

Phase-0 run of the hooks-only binary scored **270/284** on `any/pg/rbac`; the 14 failures were all
DML write-verb tests, one root cause:

**A serened table is two duckdb objects.** The *facade* (`SereneDBTableEntry`, carries owner +
GRANTs + column ACLs; what `SereneDBRelation()` resolves) and the hidden *store table*
(`__sdb_store.<name>`, holds the rows, has NO acl). A `SELECT` hands the binder the **facade** —
hooks work. A **DML** builds its plan on the storage scan, so `bind_delete/update/insert` obtain the
target via `get.GetTable()` which returns the **store table** (`bind_delete.cpp:27`,
`table_ptr = get.GetTable()`). `SereneDBRelation(store_table)` → null → the hook read null as "RBAC
doesn't apply → allow", so the write verb was never checked (e.g. `DELETE` with no DELETE grant was
allowed).

Root cause pinpointed: `SereneDBTableEntry::GetScanFunction` (`duckdb_table_entry.cpp:110`) —
- Search-engine tables set `data->table_entry = this` (facade) ⇒ `get.GetTable()` = facade, hooks OK.
- Regular tables delegate to `ResolveStoreEntry(context).GetScanFunction(...)` (line 132-133) and do
  NOT re-stamp `table_entry`, so it stays the **store** entry.

The old post-bind rule dodged this by collecting ALL tables into a list and remapping the orphan
store entry back to the facade by composed name (`catalog::StoreTableName`, rbac.cpp:164-192). An
inline hook sees one entry at a time and has no list to match against.

### Two fixes (decide before resuming)

- **(preferred) Re-stamp at the scan source.** In `SereneDBTableEntry::GetScanFunction`, regular-table
  path, after delegating: if the bind data is a `TableScanBindData`, set `tb->table_entry = this`
  (mirrors the Search path). Then `get.GetTable()` returns the facade everywhere and the store-orphan
  remap disappears for good. **Risk to verify first:** `SereneDBScanBindData::table_entry`
  (duckdb_table_function.h:229) is also read by display-name / virtual-column (`GetVirtualColumns`,
  `GetRowIdColumns`) / tableoid paths (duckdb_table_function.cpp:277,307,320,710) — confirm they
  tolerate the facade before changing it.
- **(fallback) Keep the remap, moved into the serened hook.** When `SereneDBRelation(entry)` is null
  and `IsStoreEntry(entry)`, resolve the facade from the store name via the snapshot. Store entry
  `name` is the composed `db.schema.table` — recover (db,schema,table) and call
  `snapshot->GetRelation(NoAccessCheck(), db_id, schema, name)`. Fork stays clean; the workaround the
  plan wanted gone survives. (Note: composed name can't be naively split on `.` — names may contain
  dots; the old rule sidestepped this by matching against facades composed the SAME way from the same
  statement, which an inline hook can't do — so this fallback needs a real split-safe resolver.)

## Files touched by the (reverted) attempt — the exact wiring for next time

Fork (`third_party/duckdb`):
- `src/include/duckdb/main/config.hpp` — the two hook pointers (+ `#include enums/statement_type.hpp`
  for `AccessVerb`, + `class CatalogEntry;` fwd-decl).
- `src/include/duckdb/planner/binder.hpp` — `GlobalBinderState::rbac_table_authorized` set +
  `CheckTableAccess`/`CheckColumnAccess` decls.
- `src/planner/binder.cpp` — `CheckTableAccess`/`CheckColumnAccess` bodies (call the hooks via
  `DBConfig::GetConfig(context)`, gate on non-null, use `EffectiveDefiner().get()` for `who`); call
  `CheckColumnAccess(SELECT)` inside `RecordRead` after the logical-index translation.
- `src/planner/binder/tableref/bind_basetableref.cpp` — `CheckTableAccess(SELECT)` at both the
  TABLE_ENTRY scan (line ~250) and the view-self SELECT (line ~301).
- `bind_insert.cpp:729` — `CheckTableAccess(INSERT)` + `CheckColumnAccess(INSERT)` per
  `named_column_map` (logical).
- `bind_update.cpp:322` — `CheckTableAccess(UPDATE)` + `CheckColumnAccess(UPDATE)` per
  `update->columns` (PHYSICAL → convert to logical).
- `bind_delete.cpp:85` — `CheckTableAccess(DELETE|TRUNCATE)`.
- `bind_merge_into.cpp:312` — `CheckTableAccess` per verb bit.

Serened (`server/`):
- `server/connector/optimizer/rbac.cpp` — `CheckTableHook`/`CheckColumnHook` bodies reusing
  `SereneDBRelation`, `EffectiveRole`, `AsAclMode`, `ClosureFor`, `RoleClosure::Can/CanColumns/
  CanAnyColumn`; register on `db.config.rbac_check_table/column` in `RegisterRbacAccessCheck`.
  **This is where the store-orphan fallback resolver would live if not fixing at the source.**

## Migration (unchanged plan)

- **Phase 0**: land hooks + serened bodies. (The inline hooks THROW during bind, so they cannot be
  literally parallel-run against the old rule for assert-equal — the first throw wins. Instead:
  register the hooks as the live path, keep `CollectAndEnforce` compiled-but-unregistered, and use
  the 331-test suite + PG oracle as the differential oracle. That is what the reverted attempt did.)
- **Phase 1**: delete `CollectAndEnforce`, `access_check_function`, the `AccessRequirement` carrier
  + `access_requirements` vector, the store-orphan remap; keep `RecordRead`'s translation logic in
  the hook path.
- **Phase 2**: fold `who` into the hook arg; drop dead `SereneDBClientState`-gated no-op branch.

## Verification (for next time)

- Build clean + relink (config.hpp touch forces the wide rebuild; check the Linking line).
- Full suite `--label serenedb`, isolated per file (fresh datadir per file, ownership pollution
  otherwise): `any/pg/rbac` (284) + `sdb/pg/rbac` (21) + `any/pg/system` (12) + `sdb/pg/system` (14)
  = 331.
- The 14 that failed the first attempt (all store-orphan write-verb) — must go green:
  cp_column_employees_roles, cp_returning_column, enf_dml_select_when_read, enf_iud_privilege_matrix,
  enf_update_select_when_read, idem_accumulation_enforcement, iso_enforcement, prep_statement_rbac,
  ret_returning_matrix, wide_multigroup_union, xmb_diamond_union, xog_g_dml_selectonly_grantee,
  xog_h_union_three_sources (+1).
- Oracle cross-check the divergence-guarded tests (postgres:18.3): enf_view_column_through,
  prep_catalog_version_rebind, enf_schema_usage, agg_column_enforcement, enf_zero_column_count,
  enf_merge_privilege, ret_returning_matrix.
- Hand spot-check via psql (non-super `password=` role): count(*) with column-only grant;
  `SELECT secret FROM definer_view` = 42703 not 42501; cross-join unused-table denial; and the write
  matrix — blind DELETE (DELETE only) allowed but `DELETE ... WHERE col=1` needs SELECT; constant
  `UPDATE SET c=1` allowed but `SET c=c+1`/WHERE/RETURNING need SELECT; `UPDATE ... RETURNING`
  (del-and-insert) authorizes via UPDATE; `INSERT ... RETURNING` INSERT-only denied (no leak/crash).

Full invariant list (36 items, each tied to its test) is in the earlier planning artifacts; the
write matrix (invariants 29-33) is the part the first attempt broke.
