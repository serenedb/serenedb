# Plan: unified RBAC access-check placement (PG record-then-enforce model)

## Goal

Replace the scattered / mis-placed operator access checks (column SELECT via a
fork binder hook, DML actions via four post-optimize `Plan*` hooks, plus a
rejected `sdb_write_target_bind_depth` hack) with **one mechanism**: a per-statement
"required access" record, populated at bind time and enforced once on the
fully-bound, un-optimized plan — the PostgreSQL `RTEPermissionInfo` model.

DDL + catalog `Get*` checks stay in the catalog (unchanged). Only
SELECT / column-SELECT / DELETE / UPDATE / INSERT operator checks move.

## BUGS & DIVERGENCES from PostgreSQL (verified; the actionable index)

Every item below was confirmed against the live PG19 oracle (and ClickHouse where
noted). "Current SereneDB" = behavior at the start of this work. Severity:
SECURITY = wrongly grants access PG denies (data exposure); OVER = wrongly denies
access PG allows (usability, not a hole); DIVERGENCE = different but defensible.

B1 -- VIEW DEFINER-RIGHTS NOT IMPLEMENTED  [OVER, but real correctness bug]
  PG: reads through a view run with the VIEW OWNER's rights. A role granted SELECT
  on view v only (nothing on the base table) CAN `SELECT * FROM v`; direct
  `SELECT secret FROM base` is denied.
  SereneDB: `SELECT * FROM v` -> DENIED "permission denied for table base" -- charges
  the invoker for the base table through the view (invoker-rights, not owner-rights).
  Cause: column hook CheckColumnReadAccess (table_binding.cpp:279) fires for view-body
  columns with NO inside_view gating; inside_view exists only on the
  CheckCatalogReadAccess path which SereneDB does not override.
  Fix: the redesign records reads with the bind-time inside_view flag and skips the
  invoker check for view-body reads. REQUIRES the bind-time recorder (a pure plan
  walk loses inside_view once the view is inlined). See "VIEW DEFINER-RIGHTS" section.

B2 -- ZERO-COLUMN READS NOT ENFORCED  [SECURITY -- minor: leaks row count/existence]
  PG: `SELECT count(*) FROM t`, `SELECT 1 FROM t`, `WHERE EXISTS(SELECT 1 FROM t)`,
  and an unreferenced cross-joined table all require SELECT (any-column) -> DENIED
  without a grant.
  SereneDB: ALLOWED (the per-column hook never fires when no column is referenced).
  Currently masked in tests by probing with SELECT * instead of count(*).
  Fix: the walk records a relation as read even with zero referenced columns; empty
  column set -> any-column SELECT (PG ACLMASK_ANY), already in HasColumnPrivilege.
  ClickHouse contrast: CH agrees with PG here (count(*) on no-grant table DENIED).

B3 -- SCAN-ERASED READS NOT ENFORCED (was, under the old post-optimize idea) [SECURITY]
  PG: `SELECT secret FROM t WHERE false` (and provably-empty joins) -> DENIED, even
  though PG erases the scan (EXPLAIN: Result/One-Time Filter:false, Replaces Scan).
  Risk: any post-optimize, scan-based SELECT check silently SKIPS when the optimizer
  rewrites the query to EMPTY_RESULT -> a forbidden read succeeds.
  Fix: enforce PRE-optimize (the walk runs before EMPTY_RESULT_PULLUP / scan erasure),
  recording from column references, decoupled from the surviving plan -- exactly PG's
  record-at-analyze model. (This is why post-optimize Plan* is wrong for SELECT.)

B4 -- PRUNED COLUMNS / ELIMINATED JOINS  [SECURITY if checked post-prune]
  PG: a column the optimizer prunes (e.g. secret in `SELECT av FROM (SELECT av,secret
  FROM pa)`) is STILL required -> DENIED. A table whose join is ELIMINATED
  (`SELECT pa.av FROM pa LEFT JOIN pb`, pb unused -> PG drops pb to a bare Seq Scan)
  is STILL required -> DENIED "permission denied for table pb".
  Risk: checking the post-optimize plan misses pruned columns / eliminated relations.
  Fix: pre-optimize walk records every referenced column / relation (before
  UNUSED_COLUMNS / JOIN_ELIMINATION). DuckDB is less aggressive than PG (does not
  eliminate the join) but relying on that is fragile -- pre-optimize is the robust seam.
  ClickHouse contrast: CH is LAXER (allows pruned columns) -- PG is the strict target.

B5 -- DELETE ACTION PRIVILEGE BYPASSED BY SELECT-ONLY ROLE  [SECURITY] (already fixed
  earlier this session, keep as regression coverage)
  PG: SELECT-only role -> DELETE denied; DELETE-only role -> `DELETE WHERE false` ok,
  `DELETE WHERE secret=...` denied (predicate read needs SELECT).
  SereneDB (before fix): a SELECT grant let DELETE through, because the DELETE target
  bound to the STORE table and bypassed SereneDBCatalog::PlanDelete (bind_delete lost
  the facade re-resolution block that bind_update had). FIXED by restoring the
  Catalog::GetEntry re-resolution in bind_delete.cpp. The redesign folds the DELETE
  action check into the walk; keep the SELECT-only-cannot-DELETE test.

B6 -- PREDICATE-ONLY COLUMN REFERENCE  [SECURITY -- subtle]
  PG: a column named ONLY in a (even tautological/foldable) predicate, never
  projected, still requires SELECT. With SELECT(id) but not secret:
  `WHERE secret != secret`, `WHERE secret = secret`, `WHERE secret IS NULL OR secret
  IS NOT NULL`, `WHERE length(secret) >= 0` -> ALL DENIED. (`WHERE false` / `1=0`,
  which name no column, are ALLOWED.) PG does NOT fold `secret != secret` to false.
  Risk (FEARED): a projection-only check would miss filter-only columns.
  RESOLVED (verified): filter column refs bind through the SAME GetColumnBinding path
  (bind_columnref_expression.cpp:111 -> TableBinding::Bind -> GetColumnBinding), so a
  predicate-only column DOES land in LogicalGet.column_ids at the pre-optimize seam
  (live EXPLAIN: the un-optimized C6 plan keeps FILTER (secret != secret) over the
  scan). So reading column_ids at the seam covers C6; the BoundColumnRefExpression walk
  over all expression trees remains the safe superset. See "REUSE THE BINDER'S COLUMN
  COLLECTION".

B7 -- PREPARED-AFTER-GRANT: EAGER-AT-PREPARE vs PG-LAZY  [DIVERGENCE -- accepted]
  PG: `PREPARE pc AS SELECT secret` SUCCEEDS (defers the check); EXECUTE denied;
  GRANT SELECT(secret); EXECUTE the SAME stmt -> ALLOWED (no re-prepare).
  SereneDB: PQprepare of a forbidden stmt is DENIED AT PREPARE TIME (RBAC runs in the
  binder at parse/bind), so the stmt never exists and the grant-then-allow transition
  is unreachable. SereneDB checks EAGERLY at prepare; PG LAZILY at execute.
  Decision: ACCEPTED divergence (failing sooner is safe, no data exposure). NOT a
  security bug. Pending the field survey (B7-survey) to confirm eager is common.

B8 -- PREPARED-AFTER-REVOKE: NO GAP, but the re-check is INCIDENTAL  [robustness]
  PG: prepared stmt re-executed after REVOKE -> DENIED (re-checked at exec-start).
  SereneDB: ALSO denies -- VERIFIED via protocol-level prepare+execute+revoke (psycopg2,
  two sessions; SereneDB result byte-for-byte == PG19). But it works INCIDENTALLY, and
  NOT via catalog_version as earlier believed: SereneDB does NOT override
  GetCatalogVersion, so the base returns {} (invalid) -> CheckCatalogIdentity returns
  false -> PreparedStatementData::RequireRebind votes DO-NOT-rebind. The real driver is
  DuckDB's BUILT-IN ClientContextState, whose OnExecutePrepared returns
  ATTEMPT_TO_REBIND *unconditionally* (client_context.cpp:180-182). client_context.cpp:
  662-677 ORs the two votes, so EVERY prepared EXECUTE rebinds -> the binder re-runs ->
  the RBAC binder check re-fires against current grants. VERIFIED: a REVOKE on the
  prepared table denies the next EXECUTE; a re-GRANT makes the SAME prepared stmt
  succeed again WITHOUT re-preparing (verdict recomputed each execute, not baked); and
  an ALTER on an UNRELATED table ALSO triggers the rebind (because the rebind is
  unconditional, not version-gated -- this falsified the earlier "unrelated DDL doesn't
  re-check" guess).
  Risk (latent, not a live bug): the re-check rides on DuckDB's unconditional
  ATTEMPT_TO_REBIND. If upstream ever makes that conditional, a recorder-only binder
  (post-redesign) would record-without-checking and the re-check would vanish ->
  prepared-after-REVOKE hole. Hardening (recommended): make enforcement explicit at
  OnExecutePrepared (the SereneDB state already overrides it, client_state.cpp:261) so
  the re-check does not depend on rebind behavior at all.

B9 -- UNUSED CTE  [NOT A DIVERGENCE -- corrected by measurement]
  PG (and ClickHouse): `WITH unused AS (SELECT * FROM pb) SELECT av FROM pa` -> ALLOWED
  (a fully-unreferenced CTE is not charged).
  EARLIER FEAR (now FALSIFIED): "at pre-optimize the dead CTE's LogicalGet(pb) is still
  in the bound plan, so the walk would record pb and DENY -> stricter than PG."
  MEASURED TRUTH (live EXPLAIN explain_output='all', 2026-06): an UNREFERENCED CTE is
  dropped at BIND time, NOT by the optimizer -- bind_cte_node.cpp binds CTEs lazily and
  only when IsReferenced(), so a dead CTE never enters the bound plan at all. The
  un-optimized logical plan for C5 contains ONLY `SEQ_SCAN pa`; pb is absent BEFORE any
  optimizer runs. So the pre-optimize walk never sees pb -> ALLOWS C5 -> MATCHES PG with
  ZERO special-casing. Contrast C4 (unused LEFT JOIN): pb's SEQ_SCAN IS present in the
  un-optimized plan (a present join is not dropped at bind), so the walk records pb and
  DENIES -> also matches PG. The bind-vs-optimize distinction is what separates them.
  CONCLUSION: C5 is NOT an accepted divergence; it is PG-correct for free. The current
  build already ALLOWS C5 (verified by the test agent) for exactly this reason. The
  divergence-test was repurposed to LOCK the allow-behavior (div_unused_cte_denied.test
  asserts ALLOW with a comment that a future strict walk must NOT regress it).

B10 -- COLUMN-LEVEL UPDATE + RETURNING/index (del-and-insert): OVER-STRICT  [divergence]
  Verified on PG19 oracle: with `GRANT UPDATE(amt), SELECT(id)`, PG ALLOWS
  `UPDATE t SET amt=5 WHERE id=1 RETURNING id`. SereneDB DENIES it.
  CAUSE: RETURNING (and indexed tables) trigger DuckDB's del-and-insert UPDATE
  rewrite, where LogicalUpdate.columns lists ALL columns (genuine SET targets PLUS
  constraint/index columns appended by BindExtraColumns, 1:1 with .expressions, so the
  SET prefix is not separable post-bind). The walk therefore falls back to a TABLE-WIDE
  UPDATE requirement (set_cols empty -> empty-span RequireColumnAccess), which a
  column-level UPDATE(amt) role fails. SAFE (over-strict, denies a legit query; NOT a
  hole). Only triggers for column-level UPDATE grants + del-and-insert mode (RETURNING
  or an indexed table). Proper fix needs the genuine SET-target column set captured at
  BIND (where set_info.columns is available) -- a future fork hook. Tests: ret_returning
  _matrix.test uses table-level UPDATE and documents this; div note here. Accepted.

B11 -- PREPARED INSERT re-check under SIMPLE protocol: LAXER  [divergence, narrow]
  Verified vs PG19: PG re-checks INSERT on every EXECUTE (prepared INSERT re-executed
  after REVOKE INSERT -> DENIED), exactly like SELECT/UPDATE/DELETE. SereneDB matches PG
  under the EXTENDED wire protocol, but under the SIMPLE protocol an already-prepared
  INSERT keeps succeeding after REVOKE INSERT (a FRESH INSERT and a NEW PREPARE both
  correctly deny -- the gap is only the re-execute of an already-prepared INSERT, simple
  protocol only). SELECT/UPDATE/DELETE do NOT share the gap on either protocol. Real but
  narrow security gap; the INSERT prepared-execute path under simple-query does not
  re-run the action check. Documented + locked in div_prepared_eager.test. Follow-up:
  route the simple-protocol prepared-INSERT execute through the same per-execute
  enforcement as UPDATE/DELETE.

NON-BUGS confirmed PG-faithful (do not "fix"):
- Function EXECUTE checked at catalog lookup / bind, NOT re-checked per execute -- PG
  is the same (cached at expression-compile; even a fresh call after REVOKE still
  runs). STAYS in LookupEntry. See "Function EXECUTE" section.
- Sequence USAGE/Update via GetSequence(RequireAccess) at resolution time -- correct.
- DDL ownership/create checks in the catalog -- correct, untouched.

## Why this shape (verified, not assumed)

- **PG model** (confirmed in `~/projects/kal/postgres`): per-relation
  `RTEPermissionInfo {requiredPerms, selectedCols, insertedCols, updatedCols}`
  recorded at parse-analyze from column references; enforced once at executor
  start (`ExecCheckPermissions`). Attached to the range table, so it survives
  optimizer scan-erasure. Yugabyte identical; CockroachDB/ClickHouse scattered.
- **Post-optimize is wrong for reads** (verified PG19 oracle + DuckDB source):
  the optimizer rewrites `WHERE false` / empty joins to `EMPTY_RESULT`, erasing
  the `LogicalGet`. A post-optimize scan-based SELECT check silently skips ->
  under-enforces. PG still denies (perms decoupled from the plan).
- **Post-optimize is OK for the table-level ACTION priv** (adversarially
  verified): `empty_result_pullup.cpp` has no case for
  `LOGICAL_INSERT/UPDATE/DELETE/MERGE_INTO`, so the mutation operator is never
  erased. But we fold the action check into the one walk anyway, for the
  single-place goal (no semantic loss).
- **The "optimizer reads stats before the check = leak" worry is not a real
  differentiator**: PG also plans (reads `pg_statistic`) before
  `ExecCheckPermissions`; a denied query errors before emitting any plan/stats.
- **No DuckDB `RTEPermissionInfo` analogue exists** -> we add the record on the
  SereneDB side (client state), not in any DuckDB struct.
- **Referenced-column source**: two equivalent sources at the pre-optimize seam.
  (1) `BoundColumnRefExpression.binding {table_index, column_index}` — the always-safe
  superset (a full expression walk). (2) `LogicalGet.GetColumnIds()` — the SAME set the
  binder already accumulated for READ relations, readable in one place per scan (see
  "REUSE THE BINDER'S COLUMN COLLECTION"). Prefer (2) for reads (less code, reuses bind
  work); `column_ids` OVER-counts ONLY for DML write targets (index/RETURNING columns
  force-added), so for write targets take columns from the DML operator's own fields,
  not the scan's `column_ids`.

## Architecture

### The record (server side, zero DuckDB struct changes)

```
// server/connector/duckdb_access_check.h  (new)
struct RelationAccess {
  const SereneDBTableEntry* entry = nullptr;  // facade carrying _sdb_table (captured at bind)
  catalog::AclMode action = NoRights;          // Delete/Truncate/Update/Insert on the relation
  containers::FlatHashSet<column_t> selected;   // columns needing SELECT  (logical indices)
  containers::FlatHashSet<column_t> updated;    // columns needing UPDATE
  containers::FlatHashSet<column_t> inserted;   // columns needing INSERT
  bool table_read = false;                      // relation referenced as a read source (zero-col -> any-col SELECT)
  bool inside_view = false;                     // definer-rights: skip enforcement
};
class AccessRecord {
  // keyed by LogicalGet/DML TableIndex; one entry per directly-referenced relation
  containers::FlatHashMap<idx_t /*TableIndex*/, RelationAccess> by_table;
  // CheckCatalogReadAccess can't see TableIndex, so also key the facade entry by name
  // at record time and resolve TableIndex in the walk -- see "keying" below.
};
```

Lives on `SereneDBClientState` (`server/connector/duckdb_client_state.{h,cpp}`),
reset in `QueryBegin`, cleared in `QueryEnd`.

### Keying (the load-bearing detail)

`LogicalGet::GetTable()` returns the **STORE** entry, not the facade, so the walk
cannot map a `LogicalGet`/`LogicalDelete.table` to the facade via `GetTable()`.
Resolution:
- `CheckCatalogReadAccess` (bind time) **receives the facade** `SereneDBTableEntry`
  and the relation name -> capture the facade pointer there.
- `CheckColumnReadAccess` fires on the same facade entry per column ref -> records
  the column into that relation's `selected`.
- The pre-optimize walk maps a `LogicalGet`/DML op to its relation by **TableIndex**
  (`LogicalGet.GetTableIndex()`, `LogicalDelete.table_index`, etc.) and looks up
  the facade in the bind-time record — never trusting `GetTable()`.
- For the DML action: `LogicalDelete.table` / `LogicalUpdate.table` /
  `LogicalInsert.table` are also store entries, so the action is recorded against
  the same TableIndex entry the bind-time hooks populated (the target scan's
  TableIndex), whose facade is already captured.

## Populate (bind time — repurpose existing fork virtuals as recorders)

Both hooks already exist in the fork and fire at the right spots; they change
from "check now" to "record into the AccessRecord". No new fork virtuals.

1. `SereneDBClientState::CheckCatalogReadAccess(ctx, entry, inside_view)`
   (fork call site `bind_basetableref.cpp:234`, per directly-referenced relation):
   if `entry` is a `SereneDBTableEntry`, create/fetch its `RelationAccess`, set
   `entry` (facade), `table_read = true`, `inside_view`.
2. `SereneDBTableEntry::CheckColumnReadAccess(ctx, column_index)`
   (fork call site `table_binding.cpp:279`, per resolved column ref): append
   `column_index` to this relation's `selected`. This is the `markVarForSelectPriv`
   analogue — WHERE / RETURNING / SET-rhs reads all flow through here.

(Open keying question to resolve in step 1 of implementation: `CheckColumnReadAccess`
has the facade entry but not the TableIndex; `CheckCatalogReadAccess` has neither
index nor a stable key beyond the entry. Simplest robust keying: key `by_table`
by the **facade entry pointer** during recording, and in the walk map TableIndex ->
facade via the bind-captured set, intersecting on the entry. Validate during
implementation; fall back to keying by (db,schema,name) if entry-pointer identity
is not stable across the bind.)

## Enforce (once — SereneDB pre-optimize OptimizerExtension)

Register a `pre_optimize_function` (fires once on the fully-bound, un-optimized
plan, before `RunBuiltInOptimizers`, confirmed `optimizer.cpp:427`). One walk:

1. Collect read-referenced columns. PRIMARY path: for each non-write-target
   `LogicalGet`, take `get.GetColumnIds()` as that relation's `selected`
   (`by_table[get.GetTableIndex()]`) — the binder already accumulated it and the pruner
   has not run yet (see "REUSE THE BINDER'S COLUMN COLLECTION"). SAFE-SUPERSET fallback:
   a `BoundColumnRefExpression` walk adding `binding.column_index` to
   `by_table[binding.table_index].selected`. Bring-up: run BOTH and assert parity across
   the C1-C8 matrix, then drop the walk. The bind-time `CheckColumnReadAccess` hook is
   no longer a required source either way and becomes removable.
2. Classify DML operators in the same walk:
   - `LogicalDelete` -> `by_table[table_index].action = is_truncate ? Truncate : Delete`.
   - `LogicalUpdate` -> `.action = Update`; add `op.columns` (PhysicalIndex, SET
     targets) to `.updated`.
   - `LogicalInsert` -> `.action = Insert`; add the targeted columns
     (`column_index_map` non-INVALID entries) to `.inserted`.
   - `LogicalMergeInto` -> conservative `Insert|Update|Delete` union (reproduce
     current PlanMergeInto behavior; refine to per-action later).
3. Enforcement loop, per `RelationAccess` (skip if `inside_view`):
   - action: `RequirePrivilege(conn_ctx, *facade->GetSereneDBTable(), action)`.
   - `selected`: `snapshot->RequireColumnAccess(role, *table, Select, selected_cols)`
     — empty set -> any-column SELECT (PG count(*) rule, already in
     `HasColumnPrivilege`).
   - `updated`: `RequireColumnAccess(role, table, Update, updated_cols)`.
   - `inserted`: `RequireColumnAccess(role, table, Insert, inserted_cols)`.
   Throws 42501 on the first failure (same errors/text as today).

The write-target-vs-read distinction is purely structural (operator type names the
target) — no bind-depth, no `OnWriteTargetBind`, no `sdb_*` state.

## REUSE THE BINDER'S COLUMN COLLECTION (verified — simplifies Populate, removes a fork hook)

The premise behind the pre-optimize rule "before all rules": the binder ALREADY
collects the referenced-column set as a side-effect of resolution, and that set
survives onto the bound plan, so the early rule can READ IT BACK instead of
re-walking every expression. Verified end-to-end (source + live `EXPLAIN`):

WHAT THE BINDER ACCUMULATES:
- `TableBinding` holds a `vector<ColumnIndex>& bound_column_ids`
  (`table_binding.hpp:122`). `TableBinding::GetColumnBinding`
  (`table_binding.cpp:239-258`) is called for EVERY resolved column ref and, when the
  column is new, APPENDS it (`:254 column_ids.emplace_back(column_index)`). So at end
  of bind, `bound_column_ids` == the set of columns the query actually referenced.
- This is the SAME vector the `LogicalGet` carries: `LogicalGet.GetColumnIds()`.
- A WHERE / predicate-only column ref binds through the SAME path
  (`bind_columnref_expression.cpp:111` -> `BindContext::BindColumn` ->
  `TableBinding::Bind` -> `GetColumnBinding`), so `WHERE secret != secret` (C6)
  DOES land `secret` in `column_ids` even though it is never projected. VERIFIED on a
  live server: the un-optimized logical plan for C6 keeps a FILTER `(secret != secret)`
  above `SEQ_SCAN pa` (`explain_output='all'`), i.e. `secret` is present pre-optimize.

WHY THE RULE MUST RUN BEFORE ALL BUILT-INS (the ordering, confirmed `optimizer.cpp`):
- Un-anchored `pre_optimize_function` extensions run on the freshly-bound plan at
  `optimizer.cpp:427-437`, BEFORE `RunBuiltInOptimizers()` (`:439`).
- The column pruner `RemoveUnusedColumns` (OptimizerType::UNUSED_COLUMNS, `:296`)
  REWRITES `column_ids` -> `new_column_ids` dropping pruned columns
  (`remove_unused_columns.cpp:727-797`). JOIN_ELIMINATION (`:284`) and
  EMPTY_RESULT_PULLUP (`:247`) likewise run inside `RunBuiltInOptimizers`.
- Therefore AT THE SEAM `column_ids` is still the FULL referenced set (C1/C3 pruned
  columns, C6 predicate column, C4 eliminated-join scan all still present). After the
  pruner it is NOT — which is exactly why a post-optimize read would diverge from PG.

CONSEQUENCE FOR THE DESIGN (simplification):
- For READS, the early rule can collect per relation by reading `get.GetColumnIds()`
  off each `LogicalGet` instead of walking every `BoundColumnRefExpression`. Same
  result, less code, and it reuses work the binder already did (the same spirit as the
  function-EXECUTE check, which reuses the catalog lookup it already performed).
- This RESOLVES the open keying question above: a `LogicalGet` gives BOTH the
  `table_index` AND the referenced `column_ids` in one place, mapped to the facade by
  TableIndex via the bind-captured set. No need to correlate a separate
  `CheckColumnReadAccess` stream by facade-pointer.
- This makes the bind-time per-column fork hook (`table_binding.cpp:279`
  `CheckColumnReadAccess`) REMOVABLE: the early rule reconstructs `selected` from
  `column_ids`, so the binder no longer needs an RBAC-specific touch-point.

THE ONE CAVEAT (already known): `column_ids` OVER-counts for DML write targets
(row-id / RETURNING / index columns are force-added into the same vector). So:
- read-only relations (plain SELECT / scans feeding a query): `column_ids` IS the
  clean `selected` set -> reuse directly.
- write-target relations (the `LogicalDelete/Update/Insert.table`): do NOT read
  per-column SELECT off `column_ids` (it is polluted). Take the ACTION privilege
  structurally from the operator, and take UPDATE/INSERT target columns from the
  operator's own fields (`LogicalUpdate.columns`, `LogicalInsert.column_index_map`),
  not from the scan's `column_ids`.
This keeps the earlier statement "`column_ids` over-counts for DML targets" true while
using it where it is exact (reads). NET FORK CHANGE vs the Populate section above: the
column-ref expression walk and the bind-time `CheckColumnReadAccess` hook both become
optional — one early-rule pass over `LogicalGet.column_ids` + DML operator fields
covers reads, zero-col (C7/C8), and actions.

STILL TO PROVE BEFORE RELYING ON `column_ids` ALONE (do in step 2, on real bound
plans, not headers): (a) a column referenced ONLY in a correlated subquery
(`depth>0`) lands in the correct relation's `column_ids` and is reachable from the
rule's plan root. (Item (b) -- the dead-CTE case -- is now RESOLVED by measurement: a
dead CTE is dropped at BIND and never appears in the bound plan, so there is no scan to
mis-attribute; a MATERIALIZED/referenced CTE does appear and is attributed by its own
TableIndex like any scan.) If (a) fails, fall back to the `BoundColumnRefExpression`
walk for that case (the walk is the safe superset; the `column_ids` read is the
optimization). Recommended: implement the rule to read `column_ids` for reads, keep a
debug assert that it matches a `BoundColumnRefExpression` walk during bring-up, then
drop the walk once parity holds across the C1-C8 matrix.

## Fork edits (net: shrinks)

DELETE (the rejected hack + now-unused write-target machinery):
- `third_party/duckdb/src/include/duckdb/planner/binder.hpp`: remove
  `sdb_write_target_bind_depth`, `IsBindingWriteTarget()`, `SdbBindWriteTarget()`.
- `third_party/duckdb/src/planner/binder.cpp`: remove `SdbBindWriteTarget` +
  `IsBindingWriteTarget` bodies.
- `third_party/duckdb/src/planner/binder/statement/bind_update.cpp:203` and
  `bind_delete.cpp:22`: `SdbBindWriteTarget(*node.table)` -> `Bind(*node.table)`.
  KEEP the `bind_delete.cpp` `Catalog::GetEntry` re-resolution block (it makes the
  DELETE target the facade so the op is routed to SereneDBCatalog — still needed
  for the action even though the action check moves; verify whether still required
  once checks move, may also become removable).
- `third_party/duckdb/src/include/duckdb/main/client_context_state.hpp`: remove
  `OnWriteTargetBindBegin/End` virtuals.

KEEP (repurposed as recorders, no signature change):
- `TableCatalogEntry::CheckColumnReadAccess` virtual + its `table_binding.cpp`
  call + the `TableBinding::context` wiring in `bind_context.cpp`.
- `ClientContextState::CheckCatalogReadAccess` virtual + its `bind_basetableref.cpp`
  call.

(If step-1 of enforcement — the BoundColumnRefExpression walk — proves complete on
its own, `CheckColumnReadAccess` + the `context` wiring can ALSO be deleted, making
the fork even smaller. Decide after the walk is proven against the test matrix.)

## Server edits

- NEW `server/connector/duckdb_access_check.{h,cpp}`: `RelationAccess`,
  `AccessRecord`, the `BoundColumnRefExpression`/DML walk, the enforcement loop,
  `RegisterRequireQueryPrivileges(DatabaseInstance&)`.
- `server/connector/duckdb_client_state.{h,cpp}`: hold `AccessRecord` on
  `SereneDBClientState`; `QueryBegin` reset, `QueryEnd` clear; the
  `CheckCatalogReadAccess` override (recorder); repurpose `CheckColumnReadAccess`
  path to record.
- `server/connector/duckdb_table_entry.cpp`: `SereneDBTableEntry::CheckColumnReadAccess`
  records instead of enforcing (or is deleted if the walk subsumes it).
- `server/connector/duckdb_storage_extension.cpp:137` (`RegisterSereneDBOptimizers`):
  add `optimizer::RegisterRequireQueryPrivileges(db)`.
- `server/connector/duckdb_catalog.cpp`: STRIP the 4 RBAC calls — `:679` (Insert
  cols), `:739` (Delete/Truncate), `:777` (Update cols), `:899` (MergeInto union)
  — and their stale "PlanGet enforces SELECT" comments. `Plan*` keep ONLY
  physical-plan construction.
- `server/connector/optimizer/CMakeLists.txt`: add the new source.

## Test plan

Verify on BOTH SereneDB and the PG19 oracle (15544) for the `any/` cases:
- Column: `SELECT a` (granted) ok; `SELECT secret` denied; `GRANT SELECT(id,a)`
  round-trip.
- Zero-column: `count(*)` / `SELECT 1` denied without grant, ok with any-col grant
  (this is the case the old design got wrong — must now match PG).
- Scan-erased reads: `SELECT secret FROM t WHERE false` denied (the under-enforce
  bug); cross-join with an unreferenced denied table denied.
- Column referenced ONLY in a (foldable/tautological) predicate, never projected:
  with SELECT(id) but not secret, ALL of these are DENIED on PG (verified) and must
  be on SereneDB -- `WHERE secret != secret`, `WHERE secret = secret`,
  `WHERE secret IS NULL OR secret IS NOT NULL`, `WHERE length(secret) >= 0`. PG does
  NOT fold `secret != secret` to false, but even if it did the SELECT(secret)
  requirement is recorded at analyze from the Var reference, independent of
  foldability. This is THE reason the walk must collect BoundColumnRefExpression
  from ALL expression trees (filter/join/projection/RETURNING), not LogicalGet
  column_ids (secret is filtered, not projected). Contrast: `WHERE false` / `WHERE
  1=0` reference no column -> allowed with SELECT(id) alone.
- Blind writes: `DELETE/UPDATE WHERE false` need only the action priv, NOT SELECT;
  `INSERT VALUES` needs only INSERT.
- WHERE/RETURNING reads: `DELETE WHERE secret=...` needs SELECT on secret;
  `UPDATE SET a=... WHERE x` needs SELECT on x + UPDATE on a.
- Action: SELECT-only role denied DELETE/UPDATE/INSERT/TRUNCATE; ok after GRANT.
- Full suites: `tests/sqllogic/any/pg/rbac/*` (cp_*, enf_*, xog_*, xmb_*,
  grant_revoke_cascade) + `sdb/pg/rbac/*`; the `count(*)` probe in
  `grant_revoke_cascade.test` flips from divergence to PASS (revert the `SELECT *`
  weakening). Build Debug + perf; dual-run `any/` against the oracle.

## Sequencing

1. Build the record + the pre-optimize walk + registration; populate ONLY from the
   `BoundColumnRefExpression` walk + DML classification (no bind-time recorders yet).
   Strip the 4 `Plan*` checks. Run the full matrix. This proves whether the pure
   walk is complete (esp. `inside_view` / view-body reads, and the zero-column
   case which needs the relation recorded even with no column ref — may REQUIRE the
   bind-time `CheckCatalogReadAccess` recorder for the "relation referenced but no
   column" signal).
2. If the walk can't see "relation read with zero columns" or loses `inside_view`,
   add the bind-time recorders (`CheckCatalogReadAccess` for relation + inside_view;
   keep `CheckColumnReadAccess` only if needed). Re-run.
3. Delete the rejected hack (`sdb_write_target_bind_depth` etc.) + `OnWriteTargetBind*`.
   Re-run.
4. Dual-run against the PG oracle; confirm only the known pre-existing/colleague
   fails remain (`cat_authid_shadow_superuser_only`, `ddl_drop_database_ownership`,
   `enf_authorization_denied`, `div_enforcement`).

## Prune/eliminate matrix across 5 live engines + SereneDB target (all verified in Docker)

Setup (identical on every engine): role `pu` is granted ONLY `SELECT (id, av) ON pa`
-- no `pa.secret`, no `pb` at all. Every query below runs AS `pu`. C1-C8 each isolate
one optimizer behavior that could change whether an inaccessible column/table is
"charged" before the optimizer removes it.

LEGEND -- what each case tests (with the exact SQL, all run as `pu`):
- **C1** pruned subquery column: `SELECT av FROM (SELECT av, secret FROM pa) s;`
  -- the subquery names `secret`, but the outer query never uses it (prunable).
- **C2** SELECT * subquery: `SELECT s.av FROM (SELECT * FROM pa) s;`
  -- `SELECT *` pulls in `secret`; the outer query uses only `av`.
- **C3** pruned CTE column: `WITH c AS (SELECT av, secret FROM pa) SELECT av FROM c;`
  -- the CTE names `secret`; the outer query uses only `av`.
- **C4** unused join table: `SELECT pa.av FROM pa LEFT JOIN pb ON pa.id=pb.id;`
  -- `pb` is joined but none of its columns are used (join eliminable).
- **C5** dead unused CTE: `WITH unused AS (SELECT * FROM pb) SELECT av FROM pa;`
  -- the CTE scans `pb` but is never referenced by the main query (entirely dead).
- **C6** predicate-only column: `SELECT av FROM pa WHERE secret != secret;`
  -- `secret` appears ONLY in a foldable WHERE predicate, never projected.
- **C7** count(*), granted: `SELECT count(*) FROM pa;`
  -- zero columns referenced; `pu` HAS a grant on `pa`.
- **C8** count(*), no grant: `SELECT count(*) FROM pb;`
  -- zero columns referenced; `pu` has NO grant on `pb` at all.

THE EMPIRICAL TABLE (5 live engines in Docker + SereneDB target):
| Case | What it tests          | PG 17   | MariaDB 11 | MySQL 8 | ClickHouse 26 | CockroachDB | SereneDB target       |
|------|------------------------|---------|------------|---------|---------------|-------------|-----------------------|
| C1   | pruned subquery column | DENIED  | DENIED     | ALLOWED | ALLOWED       | N/A¹        | DENIED                |
| C2   | SELECT * subquery      | DENIED  | DENIED     | DENIED  | ALLOWED       | N/A¹        | DENIED                |
| C3   | pruned CTE column      | DENIED  | DENIED     | ALLOWED | ALLOWED       | N/A¹        | DENIED                |
| C4   | unused join table      | DENIED  | DENIED     | DENIED  | DENIED        | DENIED      | DENIED                |
| C5   | dead unused CTE        | ALLOWED | DENIED     | ALLOWED | ALLOWED       | DENIED      | ALLOWED² (PG-correct) |
| C6   | predicate-only column  | DENIED  | DENIED     | DENIED  | DENIED        | N/A¹        | DENIED                |
| C7   | count(*), granted      | ALLOWED | ALLOWED    | ALLOWED | ALLOWED       | ALLOWED     | ALLOWED               |
| C8   | count(*), no grant     | DENIED  | DENIED     | DENIED  | DENIED        | DENIED      | DENIED (bug B2 today) |

¹ CockroachDB has NO column-level grants (`GRANT SELECT(col)` -> 42601 syntax error),
  so the column-pruning axis (C1/C2/C3/C6) is N/A and `secret` is fully exposed.
² SereneDB ALLOWS C5, matching PG -- NOT a divergence (corrected by measurement). An
  unreferenced CTE is dropped at BIND (bind_cte_node.cpp, lazy/IsReferenced), so pb never
  enters the bound plan and the pre-optimize walk never sees it. Verified live: the
  un-optimized plan for C5 is `SEQ_SCAN pa` only. See the corrected B9 note below.

Engine versions: PostgreSQL 17.9, MariaDB 11.8.8, MySQL 8.4.10, ClickHouse 26.5.1.882,
CockroachDB v26.2.2. ALLOWED = query succeeded; DENIED = permission/access error
(PG/CRDB 42501, ClickHouse Code 497 ACCESS_DENIED, MySQL 1142 table / 1143 column).

The strict<->lax spectrum (charge REFERENCED vs charge SURVIVING columns):
- STRICT (charge what's referenced, pre-prune): **PostgreSQL + MariaDB**. Deny C1/C2/C3/
  C6 -- any mention of `secret` (projection, `*` expansion, or a foldable predicate) is
  charged before the optimizer prunes. MariaDB is the MOST conservative -- the only
  column-capable engine that also denies C5 (resolves the dead CTE name before noticing
  it is unused).
- LAX extreme: **ClickHouse**. Allows C1/C2/C3 -- expands `*` to only granted columns and
  lets pruning drop `secret` before the check. Charges only what SURVIVES into the plan.
- HYBRID: **MySQL** -- lax on column pruning (allows C1/C3) but strict on `*` (denies C2,
  via a table-level 1142 rather than column-level 1143).
- OUTLIER by capability: **CockroachDB** -- no column grants at all; strict on the table
  axis (denies C4/C5/C8).
- UNIVERSAL agreement: C4 (unused join) is ALWAYS DENIED -- no engine eliminates a present
  join for authorization; C7 ALWAYS ALLOWED (one granted column suffices for a zero-column
  aggregate); C8 ALWAYS DENIED. The only real disagreement is the pruning axis (C1-C3) and
  the dead-CTE (C5).

SereneDB targets PG-faithful, so it belongs in the STRICT camp (PG + MariaDB): record
column refs PRE-prune (the pre-optimize walk) so C1/C2/C3/C6 DENY. ONE callout: C8 is a
confirmed security gap TODAY (B2 -- every column-capable engine unanimously denies it;
SereneDB currently allows it; must-fix). C5 is NOT a divergence -- the pre-optimize walk
allows it automatically (the dead CTE is dropped at BIND, never reaches the plan), so
SereneDB matches PG for free (corrected B9).

ClickHouse checks the columns that SURVIVE analysis (RequiredSourceColumns), so a
PRUNED column is NOT charged -> laxer. PostgreSQL records every REFERENCED column at
parse-analyze and charges it regardless of pruning -> stricter. SereneDB targets
PG-faithful, so the pruned-column cases must DENY -> the design must record column
refs PRE-prune (the pre-optimize walk). A "check what survived" (CH-style, post-prune)
approach would ALLOW them and diverge from PG. The unused-CTE C5 is the exception that
proves the rule: it is ALLOWED by ALL of PG/CH/SereneDB precisely because a dead CTE is
never bound -- "referenced" means "survived binding", and a dead CTE never enters.

## Pruned columns / eliminated joins / dead CTEs (verified on PG19 oracle)

PG's record-at-analyze model means a column/table the OPTIMIZER LATER PRUNES is
STILL enforced — the requirement is decoupled from the plan:
- `SELECT av FROM (SELECT av, secret FROM pa)` (secret pruned) -> DENIED (needs
  SELECT secret). Same for `SELECT *` subquery and CTE.
- `SELECT pa.av FROM pa LEFT JOIN pb ON pa.id=pb.id` with pb unused -> PG ELIMINATES
  the pb join (EXPLAIN: bare `Seq Scan on pa`, no pb) yet STILL throws
  `permission denied for table pb`. THE proof that the check is decoupled from the
  optimized plan.
This CONFIRMS the pre-optimize seam: it runs (optimizer.cpp:427) BEFORE
JOIN_ELIMINATION (:284), UNUSED_COLUMNS (:296, column pruner), EMPTY_RESULT_PULLUP
(:247), CTE_INLINING (:193) -- so a present-but-unused join's LogicalGet (C4 pb) and
the pruned subquery/projected-CTE column refs (C1/C2/C3 secret) are ALL still present in
the bound plan the walk sees. A post-optimize walk would miss the eliminated pb. (The
one thing NOT present pre-optimize is a fully DEAD CTE -- dropped at BIND, see the
corrected unused-CTE note below -- which is exactly why C5 is allowed, matching PG.)
NOTE: DuckDB is LESS aggressive than PG -- it does NOT eliminate the LEFT JOIN pb
(physical plan keeps HASH_JOIN + SEQ_SCAN pb), so SereneDB catches pb either way;
but relying on that is fragile (UNUSED_COLUMNS pruning DOES run), so pre-optimize
remains the correct, robust seam.

UNUSED CTE -- NO divergence (corrected by measurement): `WITH unused AS (SELECT * FROM
pb) SELECT av FROM pa` -> PG ALLOWS it. The earlier claim here ("at pre-optimize the
dead CTE's LogicalGet(pb) is still in the bound plan ... a naive walk would DENY") was
WRONG. Re-measured live with explain_output='all': the UN-OPTIMIZED logical plan for
this query is `SEQ_SCAN pa` ONLY -- pb is absent BEFORE any optimizer runs. Reason:
DuckDB binds CTEs LAZILY and only when IsReferenced() (bind_cte_node.cpp), so a dead CTE
is dropped at BIND, not during optimization. Therefore the pre-optimize walk never sees
pb and ALLOWS C5 -> matches PG with no special-casing and no liveness analysis needed.
This is the opposite of C4 (unused LEFT JOIN), where pb's SEQ_SCAN IS present in the
un-optimized plan (a present join is kept at bind) and so is correctly DENIED. The
distinguishing line is bind-time presence: dead CTE absent -> allow; unused join present
-> deny; both match PG. (Test: div_unused_cte_denied.test locks the ALLOW behavior.)

## Why PG checks at executor-start, and the prepared-statement consequence (verified)

PG SEPARATES record from check. Perms are RECORDED at parse-analyze onto
Query.rteperminfos (parsenodes.h:184) and the planner only COPIES them through
unchanged to PlannedStmt.permInfos (planner.c:676) -- the optimizer never computes
or alters them. So the CHECK CONTENT is frozen pre-optimizer; only the check TIMING
is post-optimizer. PG defers the CHECK to executor-start (ExecCheckPermissions in
InitPlan, execMain.c:862) for ONE reason: plans are CACHED and re-executed
(prepared statements, plan cache), possibly after a REVOKE or under a different
role, so PG must re-verify against CURRENT grants on every execution rather than
trust a verdict baked in at plan time (plancache.c RevalidateCachedQuery/
CheckCachedPlan re-run the check on reuse). Also SECURITY DEFINER / SET ROLE: the
effective user is only known at execution time.

CLICKHOUSE CONTRAST (verified from source): CH checks access at PLAN-BUILD from the
resolved query tree (checkAccessRights, PlannerJoinTree.cpp:159, per table
expression) -- ONE phase, NOT deferred. It checks the columns that SURVIVE analysis
(the required-column set), not every textual reference -> laxer than PG on pruned
columns. CH has NO PG-style prepared statements: "prepared statements" there are
just query-PARAMETER substitution ({name:Type} -> literal at parse,
executeQuery.cpp:1347); there is no prepare-once/execute-many cached COMPILED PLAN
reused under changed grants (QueryCache caches RESULTS, not plan+perms). So every CH
execution re-parses/re-analyzes/re-plans and re-runs the access check from scratch.
THE INSIGHT: CH can safely do record-and-check in ONE plan-build phase precisely
BECAUSE it has no plan reuse -- there is never a stale cached verdict. PG defers to
executor-start ONLY to handle cached-plan reuse. So the "one pass" design is correct
IFF there is no plan-with-baked-verdict reuse. This validates option (1) below:
make SereneDB re-bind per execution (ClickHouse-like) and a single pre-optimize
record+check pass is both correct and simplest.

CONSEQUENCE FOR SERENEDB (must decide): SereneDBClientState::OnExecutePrepared
returns current_rebind (default DO_NOT_REBIND), so a prepared statement REUSES its
bound+optimized plan across EXECUTEs WITHOUT re-binding -> the pre-optimize
extension does NOT re-run per execution. So "record+check in one pre-optimize pass"
is correct for FRESH statements but has a GAP for prepared reuse: PREPARE checks at
prepare time; a later EXECUTE after a REVOKE (or under a different role) would skip
the re-check and wrongly ALLOW (PG denies). Options:
 (1) Force re-bind per execution (OnExecutePrepared -> ATTEMPT_TO_REBIND / always-
     require-rebind) so the walk re-runs every EXECUTE -> record+check collapse into
     one pass, simplest, matches PG's effect. Cost: loses prepared-plan reuse perf.
 (2) Split like PG: record perms at bind (carried with the cached plan in the
     AccessRecord), run a cheap RE-CHECK per execution against current grants from a
     separate execute-time hook reading the record. Faithful, keeps plan reuse, but
     is two mechanisms.
 RECOMMENDATION: (1) for the first cut -- prepared-statement RBAC re-check is an
 edge case, and forcing rebind is one line + obviously correct; revisit (2) if
 prepared-plan reuse perf matters. Verify current SereneDB prepared-statement
 behavior with a test: PREPARE as superuser-granted role, REVOKE, EXECUTE -> must
 deny.

## Open questions to settle during step 1 (not blockers, but decide early)

- **Record keying**: TableIndex vs facade-pointer vs (db,schema,name). The walk has
  TableIndex; the bind recorders have the facade. Pick the key that both can produce.
- **Zero-column relation**: does the `BoundColumnRefExpression` walk see a relation
  that's referenced but reads no column (`count(*)`, `FROM a,b` with b unread)?
  A `LogicalGet` with empty column refs still exists in the plan, so the walk CAN
  record the relation as `table_read=true` from the `LogicalGet` node itself
  (independent of any column ref) — likely YES, removing the need for the
  `CheckCatalogReadAccess` recorder. Confirm.
- **inside_view / definer-rights**: the only thing a pure plan walk loses (view
  bodies are inlined). If any `any/` test exercises view-body reads with a role
  lacking base-table SELECT, the bind-time `inside_view` recorder is required.
```

## Function EXECUTE (and resolution-time privs) STAY in catalog lookup -- verified PG-faithful

Question: with reads/DML moving to record-at-bind/check-per-execute, does function
EXECUTE (currently checked in LookupEntry at bind time via
GetFunction(RequireAccess(Execute))) also need the per-execute re-check?

ANSWER: NO. PG models function EXECUTE SEPARATELY from table perms and does NOT
re-check it per execution. Verified on PG19 oracle (persistent sessions):
- Table SELECT: PREPARE p AS SELECT a FROM t; EXECUTE (ok); REVOKE SELECT; EXECUTE
  -> DENIED. (RTEPermissionInfo, re-checked at exec-start.)
- Function EXECUTE: PREPARE pf AS SELECT f(41); EXECUTE (=42); REVOKE EXECUTE ON
  FUNCTION f; EXECUTE -> STILL 42 (NOT denied). And even a FRESH (non-prepared)
  SELECT f2(20) after REVOKE in the same session -> STILL allowed.
PG source: function EXECUTE is object_aclcheck(ProcedureRelationId, funcid,
ACL_EXECUTE) at EXPRESSION-COMPILE time (execExpr.c:2741/4565/4701), session/plan-
cached, NOT in ExecCheckPermissions/RTEPermissionInfo. It is a deliberate,
documented laxness (functions are called per-row; the ACL decision is cached).

CONSEQUENCE: the per-execute record/recheck mechanism is ONLY for the privileges PG
re-checks per execution -- table SELECT, column SELECT, DELETE, UPDATE, INSERT.
Function EXECUTE stays EXACTLY where it is (the LookupEntry catalog-resolution check
at bind time) -- moving it into the per-execute record would be STRICTER than PG (a
divergence). Likewise other resolution-time gates (e.g. schema/type USAGE needed to
resolve a name) are correctly catalog-lookup-timed, not per-execute. So the boundary
is: DATA-ACCESS privs (the rows) -> per-execute record; RESOLUTION/EXECUTE privs (can
you name/call this object) -> catalog lookup at bind, unchanged.

## PREPARED-STATEMENT DECISION (verified empirically — do NOT build the split)

Workflow ran genuine protocol-level tests on live PG + SereneDB. Findings:
- gap_confirmed = FALSE. SereneDB already DENIES prepared-after-REVOKE: PQprepare
  once, PQexecPrepared twice with an out-of-band REVOKE between -> exec#2 DENIED
  "permission denied for table t". The "stale cached plan serves after revoke" gap
  does NOT manifest today.
- WHY (incidental, not deliberate -- and NOT catalog_version as first believed):
  SereneDB does NOT override Catalog::GetCatalogVersion, so the base returns {} ->
  CheckCatalogIdentity sees an invalid version and returns false -> RequireRebind's
  catalog path votes NO rebind. The actual driver is DuckDB's BUILT-IN
  ClientContextState: its OnExecutePrepared returns ATTEMPT_TO_REBIND UNCONDITIONALLY
  (client_context.cpp:180-182). client_context.cpp:662-677 ORs that vote in, so EVERY
  prepared EXECUTE rebinds -> binder re-runs -> the RBAC binder check re-fires against
  current grants. Re-verified live: REVOKE denies next EXECUTE; re-GRANT revives the
  SAME prepared stmt without re-prepare; an UNRELATED ALTER ALSO rebinds (the rebind is
  unconditional, NOT version-gated -- this falsified the earlier "unrelated DDL does not
  re-check" guess).
- The pre-optimize OptimizerExtension already re-runs on every rebind (it's inside
  Optimize()), so the unified walk inherits the SAME working re-check -- no extra
  machinery needed for the common case.

Two real (smaller) items, NEITHER requiring the heavyweight split:
1. ROBUSTNESS (latent, not a live bug): the re-check rides on DuckDB's unconditional
   ATTEMPT_TO_REBIND, not on any SereneDB code. If upstream ever makes that rebind
   conditional, a recorder-only binder (post-redesign) would record-without-checking and
   the re-check would vanish. To ANCHOR it, enforce explicitly at OnExecutePrepared (the
   SereneDB state already overrides it) -- recommended hardening, cheap, removes the
   upstream dependency; still not a heavyweight split.
2. PG DIVERGENCE (real, narrow): prepared-after-GRANT. PG: PREPARE SELECT secret
   SUCCEEDS (defers), EXECUTE denied, GRANT, EXECUTE same stmt -> ALLOWED. SereneDB:
   PQprepare DENIES at prepare (RBAC runs in binder at parse/bind), so the stmt
   never exists. SereneDB checks EAGERLY at prepare; PG LAZILY at execute.

DECISION: DO NOT build the record-at-prepare / recheck-at-execute SPLIT. It is
machinery for a gap that does not exist, and the Verify agent surfaced its real
costs: the pre_optimize seam cannot see the PreparedStatementData* key (transient
park-then-rekey after Prepare() at pg_comm_task.cpp:1082), AND a raw-pointer
side-map with an ABA hazard requiring strict eviction on every statement teardown.
High risk, no correctness benefit.

ACCEPTED divergence: prepared-after-GRANT (eager-at-prepare). Rejecting a forbidden
statement at PREPARE rather than EXECUTE is defensible (fails sooner, no data
exposure). Document it; do not chase PG's lazy semantics. Add an sdb/ divergence
note/test if desired.

NET PLAN (unchanged in scope): (1) unified pre-optimize walk for
SELECT/column/DELETE/UPDATE/INSERT -- the correctness fixes + DML consolidation +
delete the sdb_write_target_bind_depth hack; re-check on prepared-execute rides the
existing rebind path (proven to work). (2) function EXECUTE + resolution privs stay
in catalog lookup (PG-faithful: PG does not re-check those per-execute either).
(3) prepared-after-GRANT = accepted documented divergence. (4) SKIP the
record-carrier/per-execute-recheck split.

## VIEW DEFINER-RIGHTS: a pre-existing BUG the redesign must FIX (verified)

PG: reads THROUGH a view run with the VIEW OWNER's rights, not the invoker's.
Verified on oracle: role vr granted SELECT on view v only (nothing on base table) ->
`SELECT * FROM v` (which reads base.secret) is ALLOWED; direct `SELECT secret FROM
base` is DENIED.

SereneDB TODAY: `SELECT * FROM v` as vr -> DENIED "permission denied for table base"
-- WRONG. It charges the invoker for the base table through the view (invoker-rights,
not definer-rights). ROOT CAUSE: the column hook CheckColumnReadAccess
(table_binding.cpp:279) fires for EVERY resolved column with NO inside_view gating;
the inside_view flag exists only on the CheckCatalogReadAccess path
(bind_basetableref.cpp:234, IsBindingCatalogDefinition), which SereneDB does not
override. So view-body column reads are charged to the invoker.

DESIGN CONSEQUENCE (decisive): the inside_view / definer-rights distinction is known
ONLY at BIND time (the binder knows it is expanding a view body); it is LOST in the
bound/optimized plan (the view is inlined into a base-table scan). Therefore:
- A PURE pre-optimize plan-walk CANNOT do views correctly -- it would see base.secret
  read and deny, diverging from PG.
- The recording MUST flow through a bind-time hook that carries inside_view: either
  thread inside_view into the column-read recording, or record via CheckCatalogReadAccess
  (which has inside_view) and skip recording reads that originate inside a view body.
- This is a PRE-EXISTING BUG to FIX, not just preserve: SereneDB currently denies
  legitimate view reads. The redesign should make view-body reads bypass the
  invoker check (definer-rights), matching PG.
ACTION: add a test (vr granted on view only -> SELECT * FROM v ALLOWED, direct base
DENIED) to the matrix; this is now a required correctness outcome, not a divergence.

## COMPLETE enforcement-site inventory: stays vs moves (so nothing is missed)

STAYS (resolution/action/DDL/oracle -- correct where they are):
- sequence.cpp: GetSequence(RequireAccess Usage/Update/Select) -- resolution-time
  (like function EXECUTE; PG does not re-check per-execute). STAYS.
- schema_entry.cpp:95: function EXECUTE via GetFunction(RequireAccess Execute). STAYS.
- duckdb_catalog.cpp Drop* (141/205/248/260/264/292/306/326) + schema_entry Create*
  (519/526) + create_index (320/328) + storage_extension OnDetach (110) +
  schema_entry ALTER-owner (833): DDL ownership/create checks IN THE CATALOG. STAY.
- system.cpp has_*_privilege (1142/1214/...): privilege ORACLE functions
  (introspection), not enforcement. STAY.
- client_state.cpp:293 RequirePrivilege (the connector helper) -- stays as the
  primitive the walk + DML call.

MOVES into the unified pre-optimize walk:
- duckdb_catalog.cpp 679/739/777/899: the 4 Plan* DML checks (Insert/Delete/Update/
  MergeInto) -> fold into the walk (action + modified/inserted cols).
- duckdb_table_entry.cpp EnforceColumnRead / CheckColumnReadAccess (63/72/140):
  the column read hook -> becomes the recorder (gated by inside_view, see view bug).

WRINKLE to handle in step 1 -- index-as-table scans:
- index_scan_entry.cpp 75/139/219: SELECT * FROM <index_name> checks SELECT on the
  BASE table at scan resolution (RequirePrivilege(Select)). These are READ checks.
  The walk must cover index-as-table scans -> verify how an *IndexScanEntry binds
  (does it produce a LogicalGet the walk sees, with the base table's TableIndex?).
  If the index-scan entry does NOT go through the normal LogicalGet/column-ref path,
  these may need to remain as their own scan-resolution check (acceptable: it is a
  read-resolution check, not optimizer-eliminable the same way). Decide in step 1.

## STILL-OPEN ITEMS BEFORE/DURING IMPLEMENTATION (the "did we discuss everything" list)
1. Prepared-statement RBAC field survey (workflow wc6i1ugy8 running) -- is eager-at-
   prepare common or is SereneDB an outlier? decides accept-divergence vs go-lazy.
2. Record keying: TableIndex vs facade-ptr vs (db,schema,name). settle step 1.
3. Unused-CTE divergence: accept+document vs handle. (both PG and CH allow; strict
   walk denies.)
4. MERGE INTO: conservative Insert|Update|Delete union vs per-action. keep
   conservative first, refine later.
5. VIEW definer-rights: RESOLVED -- pre-existing bug to FIX; bind-time inside_view
   recorder is REQUIRED (pure walk cannot do views). see view section above.
6. SET ROLE / SECURITY DEFINER role switching mid-session: per-execute role is the
   current one; covered by reading GetRoleId() at the seam. low risk, add a test.
7. Sequences on worker threads: RESOLVED -- ride catalog GetSequence(RequireAccess),
   resolution-time, NOT in the walk. STAYS.
8. index-as-table scans: see wrinkle above. settle step 1.

## PREPARED-STATEMENT RBAC SURVEY (B7 finalized) -- where SereneDB sits vs the field

Verified across engines (live: PG, ClickHouse; source: CockroachDB, YugabyteDB;
docs: MySQL, SQL Server, Oracle). Two axes per engine:
  ALLOW-DIR = a stmt the user is NOT allowed to prepare: does PREPARE fail (EAGER)
              or succeed-then-deny-at-execute, and does a later GRANT make the SAME
              prepared stmt run without re-preparing (LAZY)?
  DENY-DIR  = a REVOKE between prepare and execute: does the next EXECUTE deny?

| Engine        | ALLOW direction (PREPARE forbidden, then GRANT)        | DENY direction (REVOKE then EXECUTE) |
| PostgreSQL    | LAZY: PREPARE succeeds; GRANT -> same stmt runs        | denies (re-check at exec-start)      |
| YugabyteDB    | LAZY (inherits PG exactly)                             | denies                               |
| ClickHouse    | LAZY-effective: no cached plan; GRANT -> next run OK   | denies (ContextAccess live-invalidated) |
| MySQL/MariaDB | LAZY: priv checked at each execute (next-request rule) | denies                               |
| CockroachDB   | EAGER: PREPARE of forbidden stmt FAILS; GRANT needs    | denies (memo IsStale -> CheckDependencies |
|               | re-prepare. (memo built at prepare, optbuilder raises) | re-checks privileges every execute)  |
| SQL Server    | permission checks are a runtime concern (not in the    | recompiles / re-checks               |
|               | plan-invalidation set) -> effectively re-checked       |                                      |
| Oracle        | object-priv REVOKE invalidates the cached cursor       | denies (cursor invalidated)          |
| **SereneDB**  | **EAGER: PREPARE of forbidden stmt FAILS** (RBAC in    | **denies** (DuckDB unconditional     |
|               | binder at parse/bind); GRANT needs re-prepare          | per-execute rebind re-runs binder)   |

WHERE SERENEDB SITS: identical shape to **CockroachDB** -- EAGER on the ALLOW
direction (forbidden stmt cannot be prepared; a later GRANT needs a re-prepare),
LAZY/re-validated on the DENY direction (REVOKE -> next execute denies). Same
OBSERVABLE behavior, but DIFFERENT mechanism: CRDB re-checks via memo
IsStale->CheckDependencies (genuinely version/dependency-driven, surgical); SereneDB
re-checks because DuckDB's built-in state rebinds EVERY prepared execute
unconditionally (client_context.cpp:180-182,662-677) and the binder check re-fires --
NOT catalog_version (SereneDB doesn't override GetCatalogVersion, so that path is inert).
So SereneDB's re-check is coarser (always rebinds) and rides on an upstream default
rather than its own dependency tracking.

IS EAGER-AT-PREPARE COMMON? Yes -- it is NOT unique to SereneDB. CockroachDB is
eager-at-prepare for the allow direction; Oracle/SQL Server bind permission concerns
to compile/cursor and invalidate on change. The PG family (PG, Yugabyte) and the
no-cached-plan engines (ClickHouse, MySQL) are lazy/re-checked. So both models exist
in the field; PG-lazy is one camp, CRDB-eager-with-revalidation (= SereneDB) is the
other. SereneDB is NOT an outlier.

DECISION (B7 finalized): ACCEPT the eager-at-prepare divergence. It matches
CockroachDB exactly and is a recognized model. The DENY direction (the security-
relevant one -- a REVOKE must stop access) already works in SereneDB and must be
preserved. The only PG-faithful behavior we forgo is "GRANT un-sticks an already-
prepared forbidden statement without re-preparing", which is a usability nicety, not
a security property; CockroachDB forgoes it too. Document as an accepted divergence;
keep the prepared-after-REVOKE denial as a required regression test.
