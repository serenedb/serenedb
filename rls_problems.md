# RLS: confirmed problems in the current design

> **Status (2026-07-28).** Fixed serened-side, with **zero duckdb changes** — the
> fork stays at `c2b6db94eb`: **P2a**, **P2b**, **P5**, **P9**, the multiple-permissive
> WITH CHECK crash, INSERT write-default-deny, the policy column-ambiguity bind, and
> **P1** is now pinned by a canary test. **C4/TRUNCATE** is closed fail-closed, as a
> deliberate divergence from PG (see below). Three gaps remain that cannot be closed
> without touching the fork — **G1–G3** at the end of this document. Suite: 46/46 RLS
> runs, 724 regression runs, `any/pg/rls` green against postgres:18.3.

Evidence-backed record of what is wrong with the RLS implementation as it stands
(fork baseline `c2b6db94eb` + serened `rls.cpp`). Every item below was reproduced
against a running serened; the PG-comparison items were diffed against
postgres:18.3.

---

## P1. Predicate ordering — reorderers exist, but the concrete attacks are blocked

**Status: NOT currently exploitable.** An earlier draft of this document claimed
this was a live leak; targeted attacks disproved that. The reordering machinery
below is real, but existing `CanThrow`-keyed guards happen to close the channels
we could construct. What remains is *fragility*, not a hole. Kept here because
the protection is incidental and must be pinned by tests.

### Attempted attacks, both blocked

| attack | result |
|---|---|
| `WHERE 1/(secret-1000) = 999999999` on a 200k-row table, RLS filter made deliberately unselective (50%), one *hidden* row with `secret=1000` | 0 rows, **no error** |
| side-effecting predicate: `CREATE FUNCTION leak_fn(x) RETURNS int LANGUAGE sql AS 'INSERT INTO leaked VALUES (x) RETURNING 1'`, then `WHERE leak_fn(secret) > 0` | only the caller's *own* row value (`5`) was written; hidden `1000`/`7777` never evaluated |

### Why it holds — `pushdown_get.cpp:95`

```c
// Allow pushing down filters that can throw only if there is a single expression
if (expr.CanThrow() && filters.size() > 1) {
    continue;
}
```

Because an RLS-enabled scan *always* contributes its own filter, `filters.size()
> 1` holds whenever a user qual is present, so a throwing user qual is **refused
entry to the scan** and necessarily evaluates above it — i.e. after RLS.
Confirmed by plan inspection: with `USING (lower(owner) = current_user)` the RLS
qual is pushed *into* `SEQ_SCAN` as a Column Filter while the throwing qual
remains in a separate `FILTER` node above.

For the conjunction case (both quals in one `LogicalFilter`) the same flag guards
both reorderers: `ExpressionHeuristics::ReorderExpressions` returns early
(`expression_heuristics.cpp:52`) and `AdaptiveFilter::AdaptiveFilter(expr)` sets
`disable_permutations` (`adaptive_filter.cpp:20`). Both are **upstream** duckdb
code (`git log`: `Mytherin`, `Mark`, upstream PR numbers), so this is inherited
behaviour, not fork-local.

### Residual fragility (why this entry stays open)

1. **The guard's contract is about errors, not security.** Upstream documents the
   flag as *"Whether or not this function can throw an error"*
   (`function.hpp:484`), and it is set by genuinely-erroring built-ins
   (`arithmetic.cpp`, `decimal_division.cpp`, `regexp.cpp`, `strftime.cpp`,
   `list_extract.cpp`, `length.cpp`, `error.cpp`). Upstream could legitimately
   narrow these bail-outs — e.g. "only bail when the throwing qual would move
   *earlier*" — which is correct for their purpose and would silently reopen the
   channel on a duckdb bump. **A canary test is mandatory.**
2. **Order-dependent edge.** The `filters.size() > 1` test runs inside a loop that
   *erases* fully-pushed filters (`filters.erase_at(i)`). If the RLS qual is
   processed and erased first, a later throwing qual could see `size() == 1` and
   be admitted into the scan. Not reproduced, but the guard is order-sensitive.
3. **No guard on the scan-filter permuter.** `AdaptiveFilter(const
   TableFilterSet&)` (`adaptive_filter.cpp:30`) uses `GetInitialOrder()` with no
   `CanThrow` check. Unreachable for throwing quals today (they can't get into the
   set, per above), but it is the one place with no protection at all.
4. **Timing/resource channels** are not addressed — nor are they by PostgreSQL's
   `leakproof`.

### Note on `LEAKPROOF`

Not needed. `LEAKPROOF` is parsed and discarded
(`transform_create_macro.cpp:157`) and `pg_proc.proleakproof` is hard-coded
`false` (`default_views.cpp:117`). Introducing it was considered and rejected:
serened has **no procedural language** (a `LANGUAGE` body is re-parsed as SQL,
`transform_create_macro.cpp:239`) and no user-reachable extension loading, so a
user predicate is a SQL macro over built-ins. `CanThrow() || IsVolatile()` covers
its observable channels, as the two attacks above demonstrate.

### Original finding, retained for reference

RLS is emitted as an ordinary filter, and duckdb does actively reorder quals — at
both compile time and run time:

1. **Compile-time cost sort.** `src/optimizer/expression_heuristics.cpp:38`
   `ExpressionHeuristics::ReorderExpressions()` sorts `AND` children by estimated
   cost (`sort(expression_costs...)`, line 67). Registered in the pipeline at
   `src/optimizer/optimizer.cpp:458`. A policy such as `owner = current_user`
   (function call) costs more than e.g. `1/(salary-1000) > 0` (arithmetic), so the
   user predicate is deliberately placed first.

2. **Run-time adaptive permutation of conjuncts.**
   `src/execution/expression_executor/execute_conjunction.cpp:60`
   `ExpressionExecutor::Select(BoundConjunctionExpression …)` evaluates children
   through `adaptive_filter->GetPermutation()` (lines 82, 123). Even a correct
   bind-time order is undone at execution: the executor learns selectivity and
   permutes. Ordering conjuncts at bind time is therefore *not* a fix.

3. **Run-time adaptive permutation of pushed scan filters.**
   `AdaptiveFilter` (`src/include/duckdb/execution/adaptive_filter.hpp:31`) does
   the same for table-scan filters. Pushing RLS into the scan does not escape the
   problem, because `TableFilterType::EXPRESSION_FILTER = 9` ("an arbitrary
   expression", `table_filter.hpp:32`) means arbitrary user expressions can be
   pushed into the same filter set and permuted against the RLS filter.

**Consequence.** A non-leakproof user predicate can be evaluated on rows RLS is
meant to hide, leaking their existence/content via thrown errors, side effects or
timing. Classic form: `WHERE 1/(salary-1000) > 0` — a division-by-zero error
proves a hidden row with `salary = 1000` exists.

**No barrier concept exists to prevent it.** `LEAKPROOF` is *parsed and then
discarded* — `src/parser/peg/transformer/transform_create_macro.cpp:157`
("Other decorators (volatility, strict, security, cost, rows, parallel,
leakproof) are accepted for PG-syntax…"), and `pg_proc.proleakproof` is
hard-coded `false` (`src/catalog/default/default_views.cpp:117`). PostgreSQL's
entire defence against this class is `proleakproof` plus planner rules that
refuse to sink a non-leakproof qual below a security qual. We have neither.

**`volatile` is not the fix.** `src/optimizer/pushdown/pushdown_get.cpp:79`
skips pushdown for volatile expressions, so marking the RLS predicate volatile
would *prevent* it being pushed into the scan (losing zonemap/index benefit)
while still not establishing any order relative to a user qual. Volatility
constrains *how often* an expression is evaluated, not *when* relative to other
predicates.

Two adjacent concepts that already exist and are useful inputs, but are not
sufficient: `Expression::IsVolatile()` and `Expression::CanThrow()`
(`src/include/duckdb/planner/expression.hpp:55`). Note `pushdown_get.cpp:95`
guards throwing expressions with the comment *"scan pushdown loses short-circuit
evaluation semantics"* — duckdb already knows ordering is lost here; it protects
correctness, not security.

---

## P2. Role-dependent decisions are baked into plan structure ⇒ cached-plan bypass

RLS resolves role-dependent facts at **bind time** and freezes them into the
plan. Nothing calls `SetAlwaysRequireRebind()`, so the plan is reused after the
role changes.

Three role-dependent inputs, inconsistently handled:

| input | resolved | stale after `SET ROLE`? | failure |
|---|---|---|---|
| `current_user` inside the policy body | run time (function) | no — self-corrects | — |
| policy selection (`TO`-role membership) | bind time (`PolicyAppliesTo`) | **yes** | *wrong* filter |
| bypass (superuser / BYPASSRLS / owner-unless-FORCE) | bind time (`rls.cpp:169-178`, three `return plan;`) | **yes** | ***no* filter** |

### P2a. Total bypass via bypassing-role prepare (reproduced)

The owner bypasses RLS, so their bound plan contains **no filter node at all**.

```sql
CREATE TABLE pd (owner text, v int);
INSERT INTO pd VALUES ('alice',1),('pd_bob',2),('carol',3);
CREATE ROLE pd_bob LOGIN PASSWORD 'pw';
GRANT SELECT ON pd TO pd_bob;
ALTER TABLE pd ENABLE ROW LEVEL SECURITY;
CREATE POLICY p ON pd FOR SELECT USING (owner = current_user);
```

| sequence | result |
|---|---|
| `SET ROLE pd_bob; SELECT …` | `pd_bob\|2` — correct |
| `SET ROLE pd_bob; PREPARE p; EXECUTE p` | `pd_bob\|2` — correct |
| `PREPARE p; SET ROLE pd_bob; EXECUTE p` | **`alice\|1 carol\|3 pd_bob\|2`** — RLS entirely off |

Two statements disable RLS. This is the most severe defect found.

### P2b. Wrong-visibility via stale policy selection (reproduced)

Neither role is owner or superuser, so the bypass path is not involved. Policy is
`FOR SELECT TO ps_priv USING (true)`; `ps_none` should get default-deny.

| sequence | result |
|---|---|
| `SET ROLE ps_priv; SELECT …` | `a b` — correct |
| `SET ROLE ps_none; SELECT …` | *(empty)* — correct |
| `SET ROLE ps_priv; PREPARE q; EXECUTE q;` then `SET ROLE ps_none; EXECUTE q` | **`a b`** — inherits `ps_priv`'s visibility |

Root cause is a modelling error, not a missing call: **RLS is treated as a
property of the query when it is a property of (query, role, snapshot).**
The same error causes both the redundant recomputation of P3 and the staleness
here.

---

## P3. The predicate is computed redundantly, and twice with *different semantics*

1. **Same text, two parsers, two meanings.** For `FOR ALL USING (…)` with no
   explicit `WITH CHECK` (PG falls back to USING), an UPDATE runs the same text
   through both `BindPolicyExpr` (`WhereBinder`, qual semantics, NULL = deny) and
   `BindCheckExpr` (`CheckBinder`, INTEGER, NULL = **pass**). Divergent
   preprocessing too — only the check path calls `RewriteSpecialRegisters`. Two
   implementations of one predicate, free to disagree. This is the origin of the
   NULL fail-open (P4).
2. **Once per scan site.** `rls_wrap_scan` fires per `LogicalGet`, so
   `FROM t a JOIN t b` parses and binds every policy twice.
3. **No caching at all.** Every query re-runs `GetRowSecurity` → `PolicyIds` →
   per policy `GetObject` + a full `Parser::ParseExpressionList` + bind + fold.

---

## P4. Enforcement is opt-in at N call sites ⇒ fail-open by construction

`AccessVerb` covers `SELECT | INSERT | UPDATE | DELETE | TRUNCATE`, and seven
logical operators touch a relation (`GET, INSERT, UPDATE, DELETE, MERGE_INTO,
COPY_TO_FILE, EXPORT`). Enforcement is a hook each statement binder must
*remember* to call, so any unwired path is a silent hole. Confirmed instances:

- **MERGE** skips the target-row USING (pre-image) check on `WHEN MATCHED
  UPDATE`/`DELETE` — silently mutates/deletes rows PG rejects with 42501.
- **INSERT … ON CONFLICT** leaks the existence of an RLS-hidden conflicting row
  as a `23505` PK violation (PG: `42501`); `DO NOTHING` errors instead of
  no-op'ing.
- **TRUNCATE** by a non-owner is RLS-row-filtered: reports success and leaves
  rows (PG empties unconditionally).
- `COPY` / `EXPORT` — unverified, but structurally unprotected.

Any future write path is a hole until a reviewer notices.

---

## P5. WITH CHECK is fail-open on NULL

An RLS `WITH CHECK` evaluating to NULL was treated as *satisfied*, so a row whose
policy expression is NULL (e.g. `owner = current_user` with `owner IS NULL`, or
`a > b` with `b IS NULL`) was accepted. PG evaluates WITH CHECK as a qual, where
NULL means rejected. Caused by reusing duckdb's CHECK-constraint verifier, whose
`entry.IsValid() && entry.GetValue() == 0` test skips invalid (NULL) results
(`src/storage/data_table.cpp`). Direct consequence of P3.1.

---

## P6. Policies stored as re-parsed text ⇒ orphaning, internal errors, ambiguity

`PolicyData` stores `using_text` / `check_text` as raw strings, re-parsed and
re-bound per query. Consequences:

- `ALTER TABLE DROP COLUMN` of a policy-referenced column succeeds and leaves a
  dangling qual (PG refuses via a column→policy dependency); subsequent reads
  silently return 0 rows.
- `ALTER TABLE RENAME COLUMN` does not rewrite the qual — silently changes results.
- Subqueries in `USING` create fine but fail at query time with `XX000`
  ("Cannot copy BoundSubqueryExpression" / "unknown type") — the standard
  ACL-table pattern is unusable.
- An unqualified column in a policy binds against whatever bind context exists at
  query time, so in `UPDATE … FROM src` where `src` has a same-named column the
  policy can bind to the *wrong table*.

---

## P7. Policy evaluation is charged to the caller's column privileges

A write or read is wrongly denied `42501` when a policy's USING expression reads
a column the caller lacks `SELECT` on. PG evaluates the policy internally and
does not require caller privilege on columns used only inside the policy.

---

## P8. Wrong SQLSTATE for RLS violations

RLS `WITH CHECK` violations are raised as duckdb `ConstraintException` ⇒ class
`23000` (integrity_constraint_violation). PG uses `42501`
(insufficient_privilege). Applications that branch on SQLSTATE to distinguish an
authorization denial from a data-integrity failure misclassify.

---

## P9. Policy DDL on a non-table crashes the server

`CREATE POLICY` / `ENABLE`/`FORCE ROW LEVEL SECURITY` on a **view or sequence**
aborted the process (SIGABRT, exit 134, port dies for all connections): the RLS
path unconditionally downcast the relation's dependency to `TableDependency`
(`libs/basics/down_cast.h:57`) via `Catalog::CreatePolicy` → `Snapshot::PolicyIds`
→ `GetDependency<TableDependency>`. A trivially reachable denial of service. PG
returns a clean statement error.

---

## P10. UPDATE post-image is not validated against the SELECT policy

PG folds the SELECT policy's USING into an implicit WITH CHECK — always for
UPDATE, and for INSERT/UPDATE with `RETURNING`. Without it:

- `UPDATE` can move a row *out of* the writer's own visible set (PG: `42501`).
- `INSERT … RETURNING` returns **and persists** a row the SELECT policy hides —
  simultaneously a wrongful write and a read of hidden data.

---

## Severity summary

| | problem |
|---|---|
| **critical** | P2a total bypass via cached plan |
| **high** | P4 fail-open enforcement · P5 NULL fail-open · P10 missing post-image fold · P2b stale policy selection · P9 crash/DoS |
| **medium** | P3 duplicate/divergent computation · P6 text-stored policies · P7 privilege entanglement |
| **low** | P8 SQLSTATE |
| **not exploitable, needs a canary test** | P1 predicate ordering |

The recurring theme is that RLS is **bolted onto** the query pipeline — at N
opt-in call sites, with the answer frozen at bind time — rather than being a
property of the relation enforced at a chokepoint. P2 and P4 are not bugs to
patch; they are consequences of that shape.

P1 is the exception: ordering is *already* sound, for free, because a
potentially-leaking qual cannot enter a scan that an RLS filter also occupies.
The design should preserve that property (keep the RLS qual pushable and
non-throwing) rather than add a barrier mechanism of its own.

---

# Gaps that require a duckdb fork change

Everything else was fixed with the fork untouched. These three cannot be, and the
reason is the same in all three: the WITH CHECK path rides duckdb's
CHECK-constraint verifier, and the read path's hook is verb-blind.

## G1. UPDATE/DELETE pre-image is filtered by the SELECT policy, not the command's

`rls_wrap_scan` is verb-blind: it always builds the SELECT-visibility predicate.
An UPDATE/DELETE therefore reads its rows through the *SELECT* policy, so a
standalone `FOR UPDATE` / `FOR DELETE USING (…)` policy never restricts which
rows a bare write touches. With a permissive `FOR SELECT USING (true)` and a
narrow `FOR UPDATE USING (owner = current_user)`, an `UPDATE` modifies **every**
visible row, not just the ones the UPDATE policy admits.

*Needs:* a verb argument on the hook (`0=SELECT, 1=UPDATE, 2=DELETE`) plus the
matching call sites in `bind_update` / `bind_delete`. Also note `bind_delete`
must hand the RLS hook the **facade** entry, not `get.GetTable()` (which returns
the storage table and fails the facade cast, silently disabling DELETE filtering
altogether).

## G2. Write default-deny does not apply to UPDATE

A constant deny carries no column references, and the UPDATE verifier drops any
check whose columns are absent from the SET list:

```c
// src/storage/data_table.cpp, CreateMockChunk
if (found_columns == 0) {
    // no columns were found: no need to check the constraint again
    return false;
}
if (found_columns != desired_column_ids.size()) {
    throw InternalException("Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
}
```

So a `BoolConst(false)` RLS check is **silently skipped** on UPDATE, and any
policy referencing a column outside the SET list raises an *InternalException*
instead of being evaluated against the full post-image. INSERT is unaffected (it
verifies every constraint), which is why INSERT default-deny works today.

*Needs:* `TableCatalogEntry::BindUpdateConstraints` to add the RLS constraint's
`bound_columns` to the update (it currently only walks the table's *own*
constraints, so engine-appended ones are invisible to it).

**A fork-free fix was attempted and does not work.** `SereneDBTableEntry` already
overrides `BindUpdateConstraints`, and its Search branch already performs exactly
the required loop, with a comment naming this failure mode. Adding the same loop
to the transactional branch has **no effect**, because `bind_update.cpp:322`
delegates past the facade:

```c
auto storage_table = get->GetTable();          // the STORE DuckTableEntry
if (storage_table && storage_table.get() != &table) {
    storage_table->BindUpdateConstraints(...);  // base impl -- facade override skipped
} else {
    table.BindUpdateConstraints(...);
}
```

For a transactional serened table the scan's table is the store entry, so the
base implementation runs and serened's override is never reached. (This also means
the transactional branch of serened's own override is effectively dead for this
path.) Symptom without the fix: the check is silently **skipped**, not partially
applied — a partial-column `UPDATE` on a row the policy forbids succeeds
unchecked, which is worse than the `InternalException` it replaces.

## G3. The SELECT policy is not folded into a write's post-image

PG folds the SELECT policy's USING into an implicit WITH CHECK — always for
UPDATE, and for INSERT/UPDATE carrying `RETURNING`. Verified against
postgres:18.3:

| statement | PG |
|---|---|
| `INSERT` of a row the SELECT policy hides, no RETURNING | allowed |
| same, **with** `RETURNING` | `42501`, rolled back |
| `UPDATE` moving a row out of the visible set (no RETURNING) | `42501` |
| `UPDATE` keeping it visible | allowed |
| no SELECT policy at all + `INSERT … RETURNING` | `42501` |

Without the fold, `INSERT … RETURNING` **persists and returns** a row the policy
hides, and an `UPDATE` can move a row out of the writer's own visible set.

*Needs:* G2 first (the fold references arbitrary columns), plus a flag on the
check hook telling it whether to fold — which for INSERT depends on the presence
of a `RETURNING` clause, information the hook does not currently receive.

---

# Where RLS could live without any fork hook

`rls.cpp` sits in `server/connector/optimizer/` but is the only file there that
never runs in the optimizer — every sibling acts on the bound plan, while
`rls.cpp` reaches back into the binder. That mismatch is what forces the eight
fork touchpoints. The seams below are all **serened-owned and already overridden**,
so enforcement can move without adding hooks.

| seam | file | phase | bypassable? |
|---|---|---|---|
| `SereneDBTableEntry::GetScanFunction` | `duckdb_table_entry.h:62` | every scan of a serened table | no |
| `SereneDBCatalog::PlanInsert` | `duckdb_catalog.h:68` | physical planning | no |
| `SereneDBCatalog::PlanDelete` | `duckdb_catalog.h:73` | physical planning | no |
| `SereneDBCatalog::PlanUpdate` | `duckdb_catalog.h:78` | physical planning | no |
| `SereneDBCatalog::PlanMergeInto` | `duckdb_catalog.h:83` | physical planning | no |
| `SereneDBCatalog::PlanCreateTableAs` | `duckdb_catalog.h:64` | physical planning | no |
| `access_check_function` | `rbac.cpp:270` | post-bind, pre-optimizer | no (but check-only: it gets the binder, not the plan) |

**An `OptimizerExtension` is NOT usable for RLS.** `optimizer_extensions` are
constructed inside `Optimizer` (`optimizer.cpp:64`), and `client_context.cpp` only
calls `Optimize()` `if (optimize && …)`, where `optimize` comes from the
**user-settable** `enable_optimizer`. A restricted role can run
`SET enable_optimizer=false` (accepted, no error), so anything registered as an
optimizer extension is skipped. Verified: with the optimizer disabled both ways
(`SET enable_optimizer=false` and `PRAGMA disable_optimizer`) RLS is still enforced
today — precisely *because* it lives in the binder. Moving it to an extension would
turn those two statements into a full bypass. This is the same class of bug already
fixed once for RBAC, which is why `access_check_function` is called at
`client_context.cpp:520` with the comment that it "runs before (and independent of)
the optimizer so it cannot be bypassed via disable_optimizer".

So the viable fork-free shape is: **reads enforced inside serened's own scan**
(`GetScanFunction`, which no plan manipulation can avoid) and **writes enforced in
`PlanInsert`/`PlanUpdate`/`PlanDelete`/`PlanMergeInto`**. Both are post-optimizer or
scan-internal, so `enable_optimizer` is irrelevant to them.

---

# Deliberate divergence: TRUNCATE

PostgreSQL does **not** apply RLS to TRUNCATE — verified on 18.3: a role holding
the TRUNCATE privilege empties an RLS-enabled table completely. serened now
**refuses** instead (`RlsGuardTruncate`), because the previous behaviour was a
silent no-op that reported success while leaving every row in place, and because
allowing it would let any TRUNCATE-holder destroy rows the policies protect.

This is the one intentional behavioural difference from PG in the current RLS
implementation. Reverting to PG semantics means letting TRUNCATE bypass RLS
outright; the decision is deliberately left open.
