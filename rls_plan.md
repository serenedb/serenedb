# Row-Level Security (RLS) — full implementation plan

Status: **IMPLEMENTATION STARTED (2026-07-17).** Grounded in PG 18 docs + reading the RLS source of
PostgreSQL, CockroachDB, and ClickHouse + auditing duckdb's optimizer/execution.

**AUTHORITATIVE SECTION: Part 6 (bottom).** It records the final, code-grounded decisions after the
full design discussion + two source-exploration passes. Parts 4.5–4.9 are the analysis that led
there and are kept for rationale, but where Part 6 differs, **Part 6 wins**. Chosen approach:
**double-filter, zero-fork** (early advisory copy for the optimizer + late fence copy for safety),
full USING + WITH CHECK, RLS state in a **side catalog object** (no `TableData` field — dodges the
positional serializer trap).

---

## Part 1 — What RLS is

Row-Level Security lets a table restrict **which rows** each role can see or modify, on top of the
normal table/column GRANT system. Enable it on a table, attach one or more *policies* (boolean
expressions over the row), and every query against that table is silently filtered / checked so a
role only touches the rows its policies allow. It's the mechanism behind multi-tenant "each tenant
sees only their rows" without the app adding a `WHERE tenant_id = ...` to every query.

## Part 2 — How PostgreSQL does it (authoritative, PG 18 docs)

### 2.1 Turning it on (per table, owner-only DDL)
- `ALTER TABLE t ENABLE ROW LEVEL SECURITY` — from now on **all** row access must be allowed by a
  policy. **Default-deny**: "If no policy exists for the table, a default-deny policy is used,
  meaning that no rows are visible or can be modified."
- `ALTER TABLE t DISABLE ROW LEVEL SECURITY` — policies are kept but **ignored**; all rows visible.
- `ALTER TABLE t FORCE ROW LEVEL SECURITY` — subject the **table owner** to RLS too (owners
  normally bypass). `NO FORCE` reverts.
- Two independent per-table flags: `relrowsecurity` (enabled) and `relforcerowsecurity` (forced).

### 2.2 Policies (`CREATE POLICY`)
```
CREATE POLICY name ON table
  [ AS { PERMISSIVE | RESTRICTIVE } ]     -- default PERMISSIVE
  [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]   -- default ALL
  [ TO { role | PUBLIC } [, ...] ]        -- default PUBLIC
  [ USING (bool_expr) ]                   -- which existing rows are visible/affected
  [ WITH CHECK (bool_expr) ]              -- which new/modified rows are allowed
```
Multiple policies per table (unique name per table). Managing policies + enable/force is
**table-owner-only**.

### 2.3 USING vs WITH CHECK — which applies to which command
| Command | USING (old/existing row) | WITH CHECK (new/proposed row) |
|---|---|---|
| SELECT | ✓ filters visible rows | — |
| DELETE | ✓ filters deletable rows | — |
| INSERT | — | ✓ (if WITH CHECK omitted → **defaults to USING**) |
| UPDATE | ✓ (old row must pass) | ✓ (new row must pass; omitted → defaults to USING) |

- **USING fails → row silently filtered** (not an error): "Rows for which the expression does not
  return true will not be processed."
- **WITH CHECK fails → hard error**: `ERROR: new row violates row-level security policy for table "t"`
  (SQLSTATE 42501). This distinction is essential: reads/deletes vanish rows quietly; writes that
  would create a forbidden row are rejected loudly.
- Policy expr is evaluated **before** the user's own WHERE/qual, with the querying user's privileges
  (security-definer functions can escalate). `current_user` / `current_setting(...)` are usable.

### 2.4 Combining multiple policies
Only policies whose `FOR` matches the command AND whose `TO` includes the current role apply. Then:
- **PERMISSIVE** (default): OR'd together — a row passes if ANY permissive policy allows it.
- **RESTRICTIVE**: AND'd — a row must satisfy EVERY restrictive policy.
- Final predicate = `(OR of permissive) AND (AND of restrictive)`.
- Corollary: RLS-enabled with **only restrictive** policies and no permissive one ⇒ the empty OR ⇒
  default-deny (no rows), because there's no permissive policy to admit anything.

### 2.5 Who bypasses RLS
- **Superusers** and roles with **BYPASSRLS** — always bypass.
- **Table owner** — bypasses UNLESS the table is `FORCE`d.
- **Whole-table ops** `TRUNCATE` / `REFERENCES` — not subject to RLS.
- **Referential-integrity checks** (unique/PK/FK) — always bypass RLS (data-integrity;
  covert-channel caveat noted by PG).

### 2.6 The `row_security` GUC
`SET row_security = off` does NOT bypass RLS — it makes a query **error** if any result would be
filtered by a policy (so e.g. pg_dump can be sure it isn't silently dropping rows). Default `on`.
(serenedb already parses this GUC as a no-op — needs to actually gate.)

## Part 3 — Current state in serenedb (grep-verified)

**Parses, stores nothing, enforces nothing:**
- Grammar (rbac.gram:132-150) already parses `CreatePolicyStatement`, `AlterPolicyStatement`,
  `DropPolicyStatement`, `AlterTableRowSecurityStatement` (ENABLE/DISABLE/FORCE/NO FORCE) →
  `serenedb_*` pragmas, and `common.gram:12-15` wires them into the top-level statement choice.
- `BYPASSRLS`/`NOBYPASSRLS` role attribute already parses (rbac.gram:30) and is stored
  (`duckdb_rbac_function.cpp:124,158` set `options.bypassrls`).
- `pg_policy.h` exists; `row_security` GUC parses as a no-op.

**Missing (the work):** policy storage (catalog object), the per-table relrowsecurity/
relforcerowsecurity flags, the pragma handlers, pg_policy/pg_policies population, and the entire
enforcement path (filter injection + WITH CHECK). The transform emits pragmas that are currently
**unregistered** → they hard-error today (same disease the old RLS attempt had).

---

## Part 4 — serenedb implementation plan (grounded, file:line)

Overall shape: store policies as a **new catalog object** (`RlsPolicy`), carry two per-table flags
on `Table`, register the four already-emitted-but-unregistered DDL pragmas, populate
`pg_policy`/`pg_class`, and enforce at **bind time** by rewriting a policied `BaseTableRef` into a
filtered subquery + attaching WITH CHECK as a constraint. Everything mirrors an existing pattern.

### A. Policy catalog object (`RlsPolicy`) — SAFE (new type, no datadir break)
New object type; the `ObjectType` enum is persisted but tail-appending is safe. Mirror `Sequence`.
- `server/catalog/object.h` — append `RlsPolicy` to `ObjectType` **after `Table`** (a policy loads
  after its parent table; do NOT renumber existing values — they're persisted keys).
- `server/catalog/persistence/rls_policy.h` (new) — positional `RlsPolicyData` (field order is the
  frozen wire format): `{ std::string name; ObjectId table_id; bool permissive; char command; //
  'r'/'a'/'w'/'d'/'*' matching PgPolicy::Polcmd  std::vector<ObjectId> role_ids; // empty = PUBLIC
  std::string using_expr; std::string check_expr; Permissions perm; }`.
- `server/catalog/rls_policy.{h,cpp}` (new) — `class RlsPolicy final : public Object`,
  Serialize/Deserialize/Clone mirroring `sequence.cpp:42-64`; ctor passes
  `(perm, parent=table_id, id, name, ObjectType::RlsPolicy)`.
- `server/catalog/catalog.{h,cpp}` — `GetObjectType<RlsPolicy>` branch; `CreatePolicy`/`AlterPolicy`/
  `DropPolicy`/`GetPolicies` mirroring `CreateSequence` (catalog.cpp:2419) / `DropSequence` (4506);
  `RegisterPolicies` boot loader (mirror `RegisterSequences` 5054) wired into per-schema boot
  **after `RegisterTables`**; collect policies in `CreateTableDrop` (4792) for DROP TABLE cascade.
- **Parent = the table** (like indexes), so DROP TABLE cascades to its policies naturally.
- `server/catalog/CMakeLists.txt` — add `rls_policy.cpp`.

### B. Per-table flags `relrowsecurity` / `relforcerowsecurity` — ⚠️ SERIALIZER-TRAP BLOCKER
`Table` persists via positional `basics::WriteTuple` on `TableData` with a **strict count check**
(`serializer.h:407`): appending fields breaks EVERY existing datadir. So the two flags cannot be
added for free.
- Options: **(preferred) accept a storage-format-version bump/migration** — append two bools (or one
  small enum, since `force` only matters when `enabled`) at the END of `TableData`, and touch in
  lockstep: `Table` ctor, `Serialize` (table.cpp:116), `Deserialize` (104), **and `Clone` (415)**
  (Clone copies field-by-field — miss it and the flag silently resets on every COW mutation).
  Alternative: a side "table RLS settings" object to dodge the trap (uglier; only if no format gate
  exists). **This is the one gating decision — confirm the datadir/format-version policy first.**
- Flip via `Catalog::ChangeTable` COW (catalog.cpp:3723) + a `Table::SetRowSecurity` mutator
  (mirror `SetComment` table.h:157). Read the accessors in `pg_class.cpp` GetTableData for
  `pg_class.relrowsecurity`/`relforcerowsecurity`.

### C. DDL pragma handlers (owner-only) — the 4 already-emitted pragmas
Transform already emits these (transform_rbac.cpp) but they are UNREGISTERED → hard-error today.
Add handlers to `server/connector/duckdb_rbac_function.cpp` + register in `RegisterRbacPragmas`
(308-383), with a `pg::` command-layer entry (mirror `pg/commands/rbac.h`). Owner-only (RequireObjectOwner).
- `serenedb_create_policy` — 9 args: name, table(dotted), permissive bool, cmd, roles LIST (name or
  "PUBLIC"), has_using, using_text, has_check, check_text. → `CreatePolicy`. **Store USING/CHECK as
  the TEXT** (the transformer already renders `expr->ToString()`). Default WITH CHECK omitted:
  store empty, fall back to USING at enforcement (PG rule).
- `serenedb_alter_policy` — 10 args (rename / roles / using / check; FOR & PERMISSIVE not alterable,
  matching PG). → `AlterPolicy` COW.
- `serenedb_drop_policy` — 3 args: name, table, if_exists. → `DropPolicy`.
- `serenedb_alter_table_row_security` — 2 args: table, action (ENABLE/DISABLE/FORCE/NOFORCE). →
  `ChangeTable` + `SetRowSecurity`.

### D. `pg_policy` / `pg_policies` population
`pg_policies` view (system_views.h:107) + `SystemTable<PgPolicy>` registration (system_catalog.cpp:189)
already exist and return empty only because there's no data.
- `pg_policy.h` — declare `template<> ... SystemTableSnapshot<PgPolicy>::GetTableData();` (mirror
  pg_sequence.h:44).
- `pg_policy.cpp` (new) — define it, iterating `GetPolicies(...)` → `PgPolicy` rows (mirror
  pg_sequence.cpp:33). `polcmd` from the stored char, `polqual`/`polwithcheck` from the TEXT.

### E. ENFORCEMENT — bind-time subquery rewrite (NOT the optimizer extension)
**Why bind, not optimizer:** the optimizer extension is bypassable via `disable_optimizer`
(`client_context.cpp:524`); a security filter must never be bypassable. RBAC already enforces at the
bind path for exactly this reason (`client_context.cpp:517`, the comment says so). And duckdb already
expands views into subqueries at bind (`bind_basetableref.cpp:294-345`) — that's the working template.

Hook: the `TABLE_ENTRY` branch of `Binder::Bind(BaseTableRef&)` (bind_basetableref.cpp:~270), sibling
to the view-expansion case. This is a small patch to the fork's binder (like the RBAC RecordAccess
calls already there). For a table with `relrowsecurity` on and the acting role not exempt:
1. **Bypass check** (skip rewrite entirely): `is_superuser` (`ClosureFor(role).is_superuser`), or
   `role->Has(RoleOption::BypassRls)` (role.h:60), or (owner AND not `relforcerowsecurity`) via
   `closure.Owns(*obj)`. Role/snapshot obtained as RBAC does (rbac.cpp:141-148).
2. **Collect applicable policies**: from `GetPolicies(table)`, keep those whose `command` matches the
   statement (SELECT/INSERT/UPDATE/DELETE/ALL) AND whose `role_ids` include the acting role (or
   PUBLIC/empty), honoring role membership via the closure.
3. **Build the predicate** (PG combiner): `(OR of permissive USING) AND (AND of restrictive USING)`.
   Parse each TEXT with `Parser::ParseExpressionList` (catalog.cpp:190) and combine the
   ParsedExpressions. **Default-deny**: RLS-on + zero applicable permissive policies ⇒ predicate is
   `false` (no rows).
4. **Rewrite** the `BaseTableRef` into `(SELECT * FROM t WHERE <predicate>)` as a `SubqueryRef`,
   bound with a child binder in the table's scope (mirror bind_basetableref.cpp:312-337). Binding
   then resolves the predicate's column refs + `current_setting(...)`/`current_user` automatically —
   no manual CheckBinder, no ColumnBinding surgery.

### F. WITH CHECK on INSERT/UPDATE
After `BindConstraints(table)` at `bind_insert.cpp:754` / `bind_update.cpp:278`, for each applicable
WITH CHECK policy (INSERT: check-or-USING; UPDATE: USING on old row via the E-rewrite + CHECK on new
row here), bind the TEXT with `CheckBinder` (bind_create_table.cpp:152) and append a
`BoundCheckConstraint` to `bound_constraints`. Violation raises at `data_table.cpp:945-968`. **To emit
PG's exact `new row violates row-level security policy for table "X"` (not the generic CHECK text),**
carry a custom-message flag on the synthetic constraint (or intercept before
`VerifyCheckConstraintExpression` data_table.cpp:619). Search tables need the WITH CHECK threaded
through `SereneDBTableEntry::BindUpdateConstraints` (duckdb_table_entry.cpp:177) like real CHECKs.

### G. row_security = off gate
Today it parses as a no-op. PG: `row_security=off` does NOT bypass — it makes the query **error** if
any result would be RLS-filtered. Implement: when the flag is off and step E would inject a filter on
any table, throw instead of rewriting.

### H. Test matrix (any/pg/rls oracle-verified vs postgres:18.3; sdb/pg/rls for serened-only)
- Enable/default-deny (no policy → 0 rows); DISABLE keeps policies but ignores.
- USING filters SELECT/DELETE silently; per-command FOR scoping.
- INSERT WITH CHECK reject (exact error + 42501); WITH CHECK defaults to USING when omitted.
- UPDATE: old row must pass USING AND new row must pass WITH CHECK.
- PERMISSIVE OR (two policies widen); RESTRICTIVE AND (narrows); restrictive-only ⇒ deny.
- TO role scoping + membership inheritance; PUBLIC.
- Bypass: superuser, BYPASSRLS, owner; FORCE subjects the owner; owner sees all without FORCE.
- current_user / current_setting in a policy; multi-tenant example.
- pg_policies / pg_policy columns; ALTER POLICY rename+roles+using+check; DROP POLICY; DROP TABLE cascade.
- row_security=off errors on a would-be-filtered query.
- FK/unique/PK bypass RLS (integrity checks unaffected); TRUNCATE not subject.
- Harness note: DML routes to the superuser connection which BYPASSES RLS unless FORCEd — so
  enforcement tests need non-super `password=` roles (or network tests), same gotcha as RBAC DML.

## Part 4.5 — SECURITY: is it hackable? (leakproof / covert channels)

Two distinct attacks; they have different answers.

### Attack A — leak filtered rows via a user WHERE function/operator  ⚠️ REAL, no defense today
A reader crafts `SELECT * FROM t WHERE 1/(secret-42) > 0` (or `WHERE leak(secret)`): if the engine
evaluates the user's `1/(...)` on a row the policy should hide, the division error (or the function's
side effect) reveals the hidden row's value. This is pure inference — no write needed, just observe
which queries error.
- **PG's defense = leakproof / security-barrier ordering:** *"This expression will be evaluated for
  each row prior to any conditions or functions coming from the user's query. The only exceptions
  are leakproof functions."* PG guarantees the policy predicate runs FIRST; a non-leakproof user
  predicate is never evaluated on a policy-filtered row.
- **serenedb has NONE of this.** `LEAKPROOF` only parses then is ignored
  (transform_create_macro.cpp:157); there is no security-barrier concept and NO evaluation-order
  guarantee in the optimizer (grep-confirmed). duckdb `filter_pushdown` freely reorders/merges
  conjuncts for speed. A bind-time-injected RLS `WHERE` and the user's `WHERE` become peer conjuncts
  the optimizer can reorder — **so the plain subquery rewrite does NOT stop Attack A** (the subquery
  is flattened and the filters merged).
- **This is the biggest gap in Part 4.** Options, in order of soundness:
  1. **Post-filter materialization barrier**: force the RLS filter to be evaluated (and rows
     dropped) BEFORE any user qual — e.g. mark the injected subquery as an optimization barrier the
     pushdown must not cross, or lower RLS to a `LogicalFilter` the optimizer is taught not to
     reorder user predicates below. Requires a real fork change to filter_pushdown (a
     security-barrier flag on LogicalFilter/LogicalGet that pins ordering). This is the correct
     fix; it is the bulk of a faithful RLS.
  2. **Leakproof allowlist**: only let a small set of known-leakproof operators/functions push below
     the RLS filter; everything else stays above. This is literally PG's model and the eventual
     end-state, but needs the barrier from (1) plus per-function leakproof metadata.
  3. **Conservative interim**: never push ANY user predicate below the RLS filter (treat all user
     functions as non-leakproof). Correct + safe, slower. Simplest honest first version.
  - A partial RLS that filters but leaves Attack A open is a **security illusion** — worse than no
    RLS, because users trust it. So the barrier (option 1/3) is NOT optional for a real feature; it
    must ship with enforcement, not after.

### Attack B — craft INSERT/UPDATE to skip/error past WITH CHECK and write forbidden data  ✅ defended
WITH CHECK attaches as a per-row constraint verified on the FINAL computed row at insert/update
time (data_table.cpp:945), after all user expressions have produced the row — so it cannot be
"skipped" by expression ordering, and an error thrown earlier in the statement just aborts the whole
write (nothing lands). The only way through is failing to ATTACH the constraint (a coverage bug:
must cover INSERT, UPDATE-new-row, and the search-table passthrough at duckdb_table_entry.cpp:177),
not an ordering trick. Structurally sound if attachment is complete.

### Unavoidable caveat (PG has it too, by design)
Integrity checks bypass RLS: a UNIQUE/PK/FK violation on INSERT reveals a hidden row with that key
exists ("covert channel"). PG documents this as a design caveat, not a bug; serenedb inherits it
identically. Not fixable without breaking integrity semantics.

## Part 4.6 — How real databases solve the leak problem (studied their source)

Cloned + read PostgreSQL, CockroachDB, ClickHouse RLS implementations. All three combine policies
the same PG way — `(OR of permissive) AND (all restrictive)`, with default-deny = a literal `false`
when no permissive policy matches (PG `add_security_quals` rowsecurity.c:782; CRDB
`genPolicyUsingExpr` returns `FalseSingleton` row_level_security.go:138; CH `FiltersMixer::getResult`
RowPolicyCache.cpp:42 + empty-OR default-deny :44). So the combiner (Part 4E step 3) is settled and
identical across all three. They DIVERGE on the leak defense (Attack A):

**PostgreSQL — per-qual `security_level` + `leakproof` (finest-grained, heaviest).**
Every filter gets an integer `security_level`; RLS quals get low levels (0,1,2…), user quals get
`max_barrier+1`. `order_qual_clauses()` (createplan.c:5292) sorts quals by `(security_level, cost)`
so an untrusted user qual can NEVER be evaluated before a lower-level RLS qual — UNLESS it's
`leakproof` (a per-function bit, `!contain_leaked_vars`), in which case it may run early. Also gates
index-qual promotion and EquivalenceClass derivation. Best plans, but woven through the ENTIRE
planner (RestrictInfo, EC, initsplan, createplan) — a very large port to duckdb's non-RestrictInfo
optimizer.

**CockroachDB — coarse `Barrier` operator (best structural fit for our optimizer).**
Plan shape `Barrier( Select( Scan, RLS-pred ) )`. No generic pushdown/reorder rule pattern-matches a
`Barrier`, so user quals structurally cannot be pushed below the RLS filter; four dedicated rules
re-admit ONLY leakproof exprs across it (`IsLeakproof` = volatility ⊆ {Leakproof}). Stripped at
execbuild (zero runtime cost). CRDB is a from-scratch cost-based optimizer like ours, so the CONCEPT
ports — but CRDB gets it free because its optimizer is rule-pattern-matched (a node not matching a
rule is automatically safe), whereas **duckdb's optimizer is hand-written passes**, so each pass
(FilterPushdown, FlattenDependentJoins, projection pushdown, join order, statistics) needs an
explicit "don't cross this barrier" check.

**ClickHouse — filter at the storage scan, NO leakproof machinery (simplest; the pragmatic fit).**
CH has no leakproof/security-barrier concept at all. It compiles the combined policy predicate and
injects it as the **first PREWHERE step on the scan** (`MergeTreeSelectProcessor` :207, need_filter,
ahead of the user PREWHERE) or the first `FilterStep` in the plan otherwise — so rows failing the
policy are dropped AT THE SCAN, before any user WHERE runs. The leak defense is purely structural
ordering: "the policy already removed the rows before your qual runs." SELECT-only; no WITH CHECK.

### The INDEX path — the exception that "scan-level filter" alone does NOT cover
"Attach RLS at the scan, user quals above it → safe" holds for filters ABOVE the scan, but an
**index qualifier is evaluated INSIDE the scan** (during the index traversal), before/alongside the
RLS filter. So a user `WHERE indexed_secret = 42` that becomes an index seek touches hidden rows
first → leak. This is a real hole; PG closes it at index-planning:
- `restriction_is_securely_promotable()` (restrictinfo.c:422) gates whether a clause may be used as
  an index qual: allowed ONLY if `security_level <= baserestrict_min_security` (it's at/below every
  other clause) OR it's `leakproof`. Called from `match_clause_to_indexcol` (indxpath.c:2605); a
  non-leakproof user qual over an RLS table is REFUSED as an index qual and demoted to a post-scan
  filter (evaluated after RLS drops the hidden rows). The index is still used for RLS's own quals.
- **duckdb analog:** `LogicalGet.table_filters` (pushed-down single-column comparisons applied at the
  scan/index) is the exact danger. If a user `WHERE secret=42` lands in `table_filters` on an RLS
  table, it runs during the scan — the leak. So the scan-level design MUST also gate `table_filters`
  population + index-scan qual selection.
- **v1 rule (leakproof deferred → blunt but safe):** on an RLS-enabled table, do NOT push any USER
  predicate into `table_filters` / index quals — keep them all as a `LogicalFilter` ABOVE the RLS
  filter. The RLS predicate itself may still use the index. Costs some index-seek optimization on
  RLS tables (acceptable for correctness). When leakproof classification lands later, relax to PG's
  rule: leakproof user quals may promote into the index.
- **This is the one duckdb pass beyond FilterPushdown that must be audited** — index-scan qual
  selection / `table_filters` pushdown. Add it to the enforcement verification.

### Recommendation for serenedb: ClickHouse-style scan-level filter + PG combiner + PG/CRDB WITH CHECK

serenedb, like ClickHouse, is a **scan-based engine** — duckdb's `LogicalGet` carries
`table_filters` that are evaluated AT THE SCAN, below anything above it. That gives us
ClickHouse-style leak-safety cheaply and structurally, without porting PG's `security_level`
plumbing or auditing every duckdb pass for a CRDB barrier:

- **Attach the RLS USING predicate to the scan itself** (a `LogicalFilter` immediately above the
  `LogicalGet`, or into the get where possible), built pre-optimizer at bind time. Because duckdb
  evaluates scan-level filters before higher operators, and filter-pushdown only pushes user quals
  DOWN toward the scan (never below the scan's own filter), the RLS predicate runs first on every
  row — the ClickHouse guarantee. **Verify** the one thing that could break it: that duckdb's
  FilterPushdown cannot merge a user qual to evaluate strictly-before the RLS filter on the same
  node, and that FlattenDependentJoins can't decorrelate a user subquery below it. If it can, add a
  minimal CRDB-style "no-reorder" flag on that one filter (far smaller than PG's scheme).
- **Combine** exactly as all three do: `(OR permissive USING) AND (AND restrictive USING)`,
  default-deny `false` when no permissive matches.
- **WITH CHECK** (which CH lacks) as PG/CRDB do: a synthetic per-row check constraint on
  INSERT/UPDATE (Part 4F), raising `new row violates row-level security policy for table "X"`
  (42501) — CRDB `row/errors.go:322`, PG `execMain.c:2391` confirm the exact message + code.
- **leakproof escape = SKIP for v1.** Take ClickHouse's stance: no leakproof classification, all
  user quals run after the RLS filter. Correct + safe, slightly less optimal plans (a genuinely
  leakproof cheap qual can't pre-filter). Add PG/CRDB-style leakproof gating later ONLY if plan
  quality demands it. This keeps v1 leak-safe without the biggest cost.
- **Bypass**: superuser / BYPASSRLS / owner-unless-FORCE, resolved at bind (CRDB
  `isExemptFromRLSPolicies` row_level_security.go:64 is the model; the role bits are already on our
  Role/closure per Part 4E step 1).
- **row_security=off** → refuse the query (CRDB row_level_security.go:91, PG RLS_NONE_ENV) — Part 4G.
- **Plan-cache**: an RLS plan is only valid for the user + policy-set that built it (CRDB
  `RowLevelSecurityMeta`). serenedb already re-binds on catalog-version bump (prep_catalog_version_
  rebind); confirm a policy change / SET ROLE bumps the version so cached plans don't serve stale RLS.

Net: **borrow ClickHouse's scan-level injection (cheap leak-safety on a scan engine) + everyone's
PG-identical combiner + PG/CRDB's WITH-CHECK write enforcement**, and defer PG/CRDB's leakproof
optimization as a later refinement. This is the minimal design that is actually leak-safe (not a
security illusion) and faithful to PG semantics.

## Part 4.7 — Which relations RLS applies to (tables only; views are transparent)

RLS is a **table** feature. Policies attach only to base tables; views cannot carry a policy but a
view's base-table access still respects the base tables' RLS, under a role chosen by the view kind.

- **Tables** — the whole feature. `RlsPolicy` parent = a `Table`; `CREATE POLICY ON <view/seq/...>`
  must error ("... is not a table"), matching PG.
- **Views** — cannot have a policy. A view is expanded at bind into its body; RLS fires when the
  binder reaches the **base table inside the body**, evaluated against the view's effective role:
  definer view → the **view owner's** policies; `security_invoker` view → the **querying user's**
  policies. This is EXACTLY the definer/invoker "RULE R" role that RBAC already resolves via
  `EffectiveDefiner()` (bind_basetableref) — RLS reuses it. So: acting role for the RLS predicate =
  the same "who" RBAC computes for that base-table reference. No separate RLS-on-views path.
- **Bypass** (superuser/BYPASSRLS/owner-unless-FORCE) is evaluated against that same effective role,
  so a definer view owned by the table owner bypasses naturally (unless FORCEd) — like a direct
  owner query.
- **Materialized views** (n/a — serenedb has none): PG applies base-table RLS at REFRESH time as the
  refreshing role, not per-query. **Sequences / functions / foreign tables / system catalogs** — no
  RLS.

Consequence for the design: **one injection point** — base-table `LogicalGet` scans — reusing the
definer/invoker role RBAC already threads. Views need no RLS-specific handling beyond that.

## Part 4.8 — SECURITY AUDIT of the subquery/filter approach (read the duckdb optimizer)

Audited all ~40 duckdb optimizer passes + the execution layer against the injected RLS predicate.
**Result: the naive "wrap in a subquery, let it flatten to a merged filter" is EXPLOITABLE.** The
RLS predicate MUST be an isolated fenced filter node, and ~7 passes need a barrier check. Three
confirmed row-visibility leaks + two threat-model-dependent metadata leaks:

**LEAK 1 (fatal, below the plan): runtime `AdaptiveFilter` conjunct reordering.** `PhysicalFilter`
rebuilds a filter's AND-conjuncts into one `CONJUNCTION_AND`, and `AdaptiveFilter`
(adaptive_filter.cpp:109-180, execute_conjunction.cpp:82) permutes their eval order AT RUNTIME by
measured selectivity — regardless of the logical plan. If the RLS predicate and a user predicate are
siblings in one conjunction, the user predicate can run FIRST on rows RLS rejects. **No logical fence
bit stops this.** Only fix: RLS must never be a sibling conjunct of a user expr (keep it a
single-expr `LogicalFilter` so PhysicalFilter takes the size==1 path and never conjoins with user
quals). (`CanThrow()` disables permutation, adaptive_filter.cpp:20 — a fallback pin, not the fix.)

**LEAK 2 (plan-level): REORDER_FILTER sorts conjuncts by cost** (expression_heuristics.cpp:38-71) —
a cheap user qual sorts ahead of an expensive RLS predicate within one filter node. Same class as 1,
same fix: don't share a node.

**LEAK 3 (the index case you flagged): FILTER_PUSHDOWN + JOIN_FILTER_PUSHDOWN push user quals INTO
the scan.** PushdownFilter dissolves the filter node and pushes single-column user conjuncts into
`get.table_filters` (pushdown_filter.cpp:16, pushdown_get.cpp:61/99, filter_combiner.cpp:418);
JOIN_FILTER_PUSHDOWN installs a build-side-derived dynamic min/max filter into `get.dynamic_filters`
recursing THROUGH the RLS filter (join_filter_pushdown_optimizer.cpp:153,341). Both evaluate INSIDE
the `LogicalGet`, below RLS → filtered rows touched pre-RLS.

**LEAK 4 (metadata, threat-model-dependent): STATISTICS_PROPAGATION existence oracle.** Column stats
come from the FULL table pre-RLS (propagate_get.cpp:160); a user predicate evaluated against
full-table min/max can collapse the plan to EMPTY_RESULT (propagate_filter.cpp:278) — so result
cardinality/empty-ness encodes filtered-out rows' values. Row visibility stays correct (filter-only),
but an attacker learns hidden values via plan shape. Fix only if existence oracles are in scope.

**LEAK 5 (not an optimizer pass): plan cache per role.** A cached prepared plan built for role A
(with A's policies + A as the acting role) must NOT be reused for role B / another tenant. CRDB keys
its plan cache on user+policy-set (RowLevelSecurityMeta). Verify serenedb's catalog-version rebind
fires on SET ROLE / different session, or don't cache RLS-table plans.

### Required model (supersedes "flatten the subquery")
RLS predicate = a **single-expression, isolated `LogicalFilter` tagged `rls_fence`, directly on the
`LogicalGet`**. The subquery rewrite is just the *authoring* convenience; the bound result must be
this isolated fenced node, NOT flattened into a shared filter. Then these passes need a one-line
"treat fence as opaque barrier: don't dissolve / pull / push-through / reorder-around / recurse-past"
check (agent gave exact insertion sites):
- FILTER_PULLUP (pullup_filter.cpp:13) · FILTER_PUSHDOWN (pushdown_filter.cpp:9) · JOIN_ORDER
  extraction (relation_manager.cpp:275) · JOIN_FILTER_PUSHDOWN (join_filter_pushdown_optimizer.cpp:153)
  · LATE_MATERIALIZATION · DELIMINATOR (deliminator.cpp:197) · CSE/COMMON_SUBPLAN keying.
- PhysicalFilter: ensure the RLS node stays single-expression (never conjoined) so AdaptiveFilter
  can't permute it against user quals.
- SAFE as-is (no guard): LIMIT_PUSHDOWN, EMPTY_RESULT_PULLUP, ROW_GROUP_PRUNER (for row visibility).

### Honest reframing of effort
This is NOT "one hook + one fence bit." It's one injection point + a `rls_fence` bit honored in
~7 optimizer passes + a PhysicalFilter isolation guarantee + plan-cache keying — a fork-wide
optimizer change. This is exactly the per-pass audit CockroachDB gets "for free" (rule-pattern
optimizer: a node not matching a rule is auto-safe) and duckdb (hand-written passes) does NOT. It's
the real cost of leak-safe RLS here, and skipping any one pass reopens a leak. A filter-only RLS
that misses a pass is a security ILLUSION.

## Part 4.9 — FINAL enforcement design: security_level-gated pushdown (supersedes 4E/4.6/4.8 seams)

Context: a filter-pushdown + `table_filters` engine is landing next (colleague's PR #884 branch;
#884 itself is filtered-top-k for inverted-index scans — it enforces `table_filters` INSIDE the
WAND/HNSW score collector + DocPruner zonemap). We WANT RLS to ride that pushdown. Earlier designs
(pin RLS above the scan behind a fence; or "just push RLS to the scan") are both wrong because RLS
predicates split into two kinds and safety cannot depend on WHERE a predicate lands:

- **Pushdownable RLS** — e.g. `region = 7`, or (once table_filters broadens) `region = current_setting()`.
- **Non-pushdownable RLS** — subqueries, complex boolean → stays a `LogicalFilter` above the scan.

duckdb's `table_filters` today only accepts `column <cmp> foldable-constant` (filter_combiner.cpp,
`IsFoldable()` scalar, 6 comparisons). So a realistic RLS predicate like `tenant_id =
current_setting('app.tenant')` does NOT currently push (current_setting isn't foldable) and stays
above; meanwhile a USER `WHERE indexed_col = 42` DOES push into the scan/index — the asymmetric leak.

### The design: tag every predicate with a trust level; gate pushdown on it (PG's model)
1. **Bind-time tag.** Every predicate carries `security_level`: RLS predicates = **0** (trusted),
   user predicates = **1**. (An `is_rls` bool is the minimal encoding.)
2. **The one pushdown invariant** (this IS the whole defense — position-independent):
   > A predicate may be pushed below/past another predicate on the same relation only if its
   > `security_level <= that predicate's level`, OR it is **leakproof**.
   This is PostgreSQL's `restriction_is_securely_promotable` (`level <= baserestrict_min_security ||
   leakproof`, restrictinfo.c:422, gating index-qual use at indxpath.c:2605).
3. **Consequences** — works whether or not RLS itself pushed down, because it's about the *relative*
   order of trusted vs untrusted predicates, not the scan:
   - Pushdownable RLS (level 0) → always pushes. ✅ fast RLS.
   - Non-pushdownable RLS → stays above; the invariant keeps user preds from jumping below it.
   - User pred on an RLS relation → pushes into scan/index ONLY if leakproof; else stays above RLS.

### v1 (no leakproof classifier yet) — blunt but correct
> On an RLS-enabled relation: push RLS predicates freely; do NOT push ANY user predicate into
> `table_filters`/index — user predicates stay as a `LogicalFilter` above the RLS filter.
Fast RLS, no leak; costs index acceleration on *user* filters over RLS tables only. Relax later when
a leakproof whitelist exists.

### What a pushed-down USER filter must be blocked from doing — the 3-class leak taxonomy
A pushed user filter is a bug iff it can do any of these on a pre-RLS row:
1. **Throw** (value-leak via error presence): `1/(salary-100000)`, `ssn::int`, `arr[100]`, overflow,
   regex-with-user-input. → non-total → NOT leakproof.
2. **Side effect** (existence-leak via observable effect): `log(secret)`, `nextval(seq)`, volatile
   UDFs. → NOT leakproof.
3. **Prune/score inside the index over unfiltered rows** (existence-leak via TIMING/pruning) — the
   #884-specific one: even a pure `col = 42` in the WAND/HNSW collector or DocPruner zonemap prunes
   based on rows RLS would hide, so result-emptiness/latency encodes a hidden row. PG has a weak
   form of this (index-seek timing) and TOLERATES it as out-of-scope; #884 sharpens it.
Classes 1+2 = exactly "not leakproof" (value/effect leaks — MUST block; PG blocks these). Class 3 is
a serenedb-specific decision below.

### How RLS + indexes stays safe (the PG guarantee to replicate)
An index does NOT return rows to the user — it finds candidate TIDs; the RLS filter still runs on
the fetched rows before anything is returned or any user function sees them. The only risk is a user
`WHERE` becoming an INDEX CONDITION (evaluated during the index scan, pre-RLS). PG allows that ONLY
for leakproof conditions; a non-leakproof user condition is demoted to a filter above the RLS check.
So a non-privileged user never sees hidden rows via an index. Replicate: on an RLS relation, a user
predicate may become an index/`table_filter` condition only if leakproof (v1: never).

### The one Class-3 decision (needs your call; #884-specific, beyond PG)
Value/effect leaks (1+2) are must-fix and non-negotiable. The TIMING/pruning leak (3) is a
threat-model choice for the #884 index path:
- **(a) Match PG** — accept the timing/pruning oracle; allow leakproof user filters to ride the
  index/collector on RLS tables (once leakproof exists). Best plans; a sophisticated attacker can
  infer existence by latency.
- **(b) Stricter** — on RLS relations, never let a *user* filter into the #884 index-collector /
  zonemap pruner (only above the RLS filter), even if leakproof. Closes the timing oracle; slower.
- v1 chooses (b) for free (no user filter pushes at all). Decide (a) vs (b) when the leakproof tier
  lands.

### Coordination with the pushdown PR — the one rule to give the colleague
> Tag pushable filters with a trust level (RLS=0, user=1). At every point a predicate becomes a
> `table_filter` OR a #884 index-collector/DocPruner filter, a predicate may be pushed only if
> `level <= min level of predicates remaining above it on this relation, OR leakproof`. v1: no user
> function is leakproof → on an RLS relation, only RLS predicates push; user predicates stay above.
> The gate lives at: filter_combiner `GenerateTableScanFilters` / `TryPushdownGenericExpression`
> (table_filters), and the #884 `table_filter_iterator.cpp` / score-gate (index/collector path).

### Runtime AdaptiveFilter note (still applies)
If two predicates end up co-located in one scan's filter set, duckdb's runtime AdaptiveFilter
permutes their eval order (adaptive_filter.cpp:109). In v1 this is moot (only RLS predicates push →
nothing untrusted to reorder against). When leakproof user pushdown is allowed, the leakproof
property is exactly what makes co-location reorder-safe (a leakproof pred can't leak regardless of
order) — which is why PG's escape hatch is specifically "leakproof."

## Part 5 — scope & risks

- **Attack A (leak via user WHERE / index) — SOLVED by the Part 4.9 design.** NOT a fence and NOT
  "push RLS to the scan" (both superseded). The defense is a per-predicate `security_level` tag
  (RLS=0, user=1) + one invariant enforced in the pushdown engine: never push an untrusted predicate
  below/past a lower-level (RLS) one unless it's leakproof. This is PG's proven model, fitted onto
  the pushdown PR the colleague is building — RLS predicates push freely (fast), user predicates on
  an RLS table are gated. v1 = block all user pushdown on RLS tables (correct, slightly slower). The
  work is: the trust tag at bind + the gate at the `table_filters` and #884 index-collector push
  sites (Part 4.9 colleague rule). Value/effect leaks (taxonomy classes 1+2) are must-fix; the #884
  timing/pruning oracle (class 3) is the one open decision (accept-like-PG vs stricter).
- **Biggest STORAGE gate:** the `TableData` serializer trap for the two per-table flags (Part 4B).
  Needs a format-version bump/migration decision before coding. The policy object itself is trap-free.
- **Fork patch required:** enforcement injects at `bind_basetableref.cpp` (the fork), not purely
  serened-side — same as RBAC's existing binder hooks. Acceptable per the RBAC precedent.
- **WITH CHECK custom error** needs a small fork touch to `BoundCheckConstraint`/the verify path to
  carry the RLS message (else the message diverges from PG).

---

## Part 6 — AUTHORITATIVE implementation decisions (2026-07-17, code-grounded)

Two source-exploration passes settled every open question. This supersedes earlier seams where they
differ.

### 6.1 Chosen enforcement: double-filter, ZERO fork

Not the `security_level` tag (4.9), not a fork `is_rls_fence` node. Both need fork edits and an
expression-identity mechanism. Instead:

- **Early copy (advisory, for the optimizer):** a serened `pre_optimize_function` optimizer
  extension (runs BEFORE built-in passes, `optimizer.cpp:489`) walks the plan for `LogicalGet`s on
  RLS tables and wraps each in a `LogicalFilter(USING)`. FILTER_PUSHDOWN + JOIN_ORDER then see it →
  `col=const` parts prune into `table_filters` (leak-safe by construction), CBO costs the reduction.
- **Late copy (the fence, for safety):** a second serened `optimize_function` extension (runs AFTER
  all passes, `optimizer.cpp:503`) splices a **fresh** `LogicalFilter(USING)` — rebuilt from the
  catalog — directly above each RLS-table `LogicalGet`. Its own node → its own `PhysicalFilter`
  (`plan_filter.cpp:18`) → its own `AdaptiveFilter` conjunction → a user predicate can never be
  reordered/pushed across it onto a hidden row.
- **Why zero fork:** the late copy is rebuilt from `pg_policy`, so we never search the mangled plan
  for "which expr is RLS" (the unsolved identity problem). RLS evaluated ~twice; the pushable
  constant part dedups into `table_filters` so only the non-constant part double-evaluates.
- Both extensions register in `RegisterSereneDBOptimizers` (`duckdb_storage_extension.cpp:123`)
  alongside the iresearch plan optimizer (the working template for a plan-rewriting extension).
- Binding the stored policy TEXT → bound filter: use a **`WhereBinder`** (the `bind_delete.cpp:67`
  model), NOT `CheckBinder` (which yields storage-offset `BoundReferenceExpression`s wrong for a
  plan filter). WhereBinder yields `BoundColumnRefExpression` with `ColumnBinding{table_index,col}`
  matching the scan.

### 6.2 Storage: side catalog object (NO TableData field)

- `RlsPolicy` is its own persisted `catalog::Object`, parented by table id — modeled EXACTLY on
  `SecondaryIndex` (`server/catalog/secondary_index.{h,cpp}` + `persistence/secondary_index.h`).
  Persist via `PutDefinition(table_id, ObjectType::Policy, policy_id, bytes)`. Rebuild an in-memory
  policies-by-table map on load (like `ObjectDependencies`, which is load-derived not serialized).
- New enum slot `ObjectType::Policy` in `server/catalog/object.h` **after `InvertedIndex`** (line 68)
  — objects scan in enum order; policy must load after its table.
- `PolicyData` (persistence struct): `name`, `cmd (char)`, `permissive (bool)`, `roles (vector<Oid>)`,
  `using_text (string)`, `check_text (string)`. Serialize via `basics::WriteTuple` — but it's a NEW
  struct so no positional trap (the trap is only appending to the EXISTING `TableData`).
- `relrowsecurity`/`relforcerowsecurity` per-table pair: a tiny per-table `ObjectType::RowSecurity`
  side object (parent = table id), OR fold into "a policy-enable marker exists." Chosen: dedicated
  tiny side object so DISABLE-with-policies-kept works. NO `TableData` change → existing datadirs
  load unchanged.

### 6.3 DDL wiring (scaffolding already exists through the pragma boundary)

Grammar already fully transforms into `serenedb_create_policy` / `serenedb_alter_policy` /
`serenedb_drop_policy` / `serenedb_alter_table_row_security` pragmas with all args
(`transform_rbac.cpp:406-524`). MISSING: the handlers + commands + catalog mutation.

- 4 pragma handlers in `server/connector/duckdb_rbac_function.cpp` — mirror `GrantTablePragma`
  (line 205) + `PragmaCall` registration (line 349). Arg helpers `ArgStr/ArgBool/ArgStrList`
  (lines 51-71) reused.
- 4 commands in `server/pg/commands/rbac.{h,cpp}` — mirror `GrantObject` (line 779) / `CreateRole`
  (line 207). Owner-only authz via `RequireObjectOwner` (`catalog.cpp:2095`).
- Catalog mutation: new `catalog.CreatePolicy/AlterPolicy/DropPolicy/SetRowSecurity` mirroring
  `CreateSecondaryIndex` (`catalog.h:468`) + the `ChangeTable` mutate-under-`_mutex`-then-
  `PutDefinition` pattern (`catalog.cpp:3723`).

### 6.4 WITH CHECK (writes)

- Bind WITH CHECK expr (fallback to USING per PG if no explicit WITH CHECK) at `bind_insert` /
  `bind_update`; validate the post-image (new row values); error 42501 on violation.
- Custom error message needs a small fork touch to the check-verify path to carry the RLS wording
  (else diverges from PG) — deferred; generic message acceptable for v1.

### 6.5 Enforcement principal + bypass

- Role = `EffectiveRole(caller, req.who)` (`rbac.cpp:133`): definer view → view owner, else caller
  (RULE R). Reuse `SereneDBRelation` (`rbac.cpp:92`) for facade → `catalog::Table`.
- Bypass (no filter injected): superuser, `RoleOption::BypassRls` (`role.h:60`), or table owner
  UNLESS `relforcerowsecurity`.
- Multiple policies → one expr per (table, role): PERMISSIVE OR'd, RESTRICTIVE AND'd, before
  injection (dedup).

### 6.6 Introspection

- Populate `pg_policy` (`server/pg/pg_catalog/pg_policy.h` — struct already defined) from the
  policies-by-table map. `pg_policies` view already exists (`system_views.h:107`).
- `pg_class.relrowsecurity`/`relforcerowsecurity` from the RowSecurity side object.

### 6.7 Attack coverage vs PG (the honest scope)

- **Class A (reordering: error-oracle, count-oracle, runtime-reorder, table_filters pushdown) —
  CLOSED** by the late fence (separate PhysicalFilter stage). PG parity.
- **Class B (function leaks) — CLOSED**; malicious-policy-author trusted (PG-same).
- **Class C (side channels): timing/estimates ACCEPTED (PG-same); stats views NOT gated (PG gates —
  gap to close or accept); #884 index-pruning oracle is a surface PG lacks — OPEN decision.**
- **Class D (writes): covered by WITH CHECK (6.4).** Read-portion of UPDATE/DELETE filtered by USING.

### 6.8 Build phases (task list #29–#34)

1. Catalog object (`RlsPolicy` + RowSecurity side object) — foundation. **[in progress]**
2. DDL handlers + commands.
3. Read enforcement (double-filter extensions).
4. WITH CHECK.
5. Introspection (`pg_policy` populate).
6. Tests: `any/pg/rls` (oracle vs postgres:18.3) + `sdb/pg/rls`, `--label serenedb`.
- **Effort: XL.** New catalog object (A) + format bump (B) + 4 handlers (C) + catalog view (D) +
  bind-time rewrite (E) + WITH CHECK constraint plumbing (F) + GUC gate (G) + a large oracle test
  matrix (H). Sequenceable: land A+C+D (DDL stores + introspects, still unenforced) first — that's
  independently testable via pg_policies — then B, then E/F/G enforcement, then the matrix.
