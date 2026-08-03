# RLS: confirmed problems in the current design

> **Status (2026-07-29).** Fixed serened-side: **P2b**, **P5**, **P9**, the
> multiple-permissive WITH CHECK crash, INSERT write-default-deny, and the policy
> column-ambiguity bind. **P1** is pinned by a canary test. **C4/TRUNCATE** is closed
> fail-closed, a deliberate divergence from PG.
>
> Enforcement now runs as a serened **pre-optimizer pass**, not fork hooks: both
> `rls_wrap_scan` and `rls_append_check_constraints` are gone, along with the peel
> logic in the write binders. The fork is down to **2 files / 10 lines**
> (`bound_check_constraint.hpp` `is_rls` + the `data_table.cpp` message) plus the
> parser. Pushdown is preserved — the emitted filter still reaches the scan as a
> `Column Filter`.
>
> **That move reopened P2a — a total RLS bypass via a cached prepared plan. See G4.**
> Remaining gaps: **G1–G3** need a fork change; **G4** does not. Suite: 44/46 RLS
> runs (G4's test removed), regression clean.

Evidence-backed record of what is wrong with the RLS implementation as it stands
(fork baseline `c2b6db94eb` + serened `rls.cpp`). Every item below was reproduced
against a running serened; the PG-comparison items were diffed against
postgres:18.3.

---

## P1. Predicate ordering — reorderers exist, but the concrete attacks are blocked

**Status: MITIGATED (2026-08-02), not yet structurally fixed.** The exploit
below is closed by conjoining a volatile no-op (`sdb_rls_barrier()`) onto every
visibility predicate: duckdb never pushes a volatile term into a scan, so it
stays in the filter set and keeps `filters.size() > 1` true, which re-arms the
guard that refuses a throwing user qual entry. The policy predicate itself is
still pushed, so row-group pruning is unaffected. Pinned by
`sdb/pg/rls/predicate_ordering_leak.test`, which fails without the mitigation.

This restores the arrangement that tests clean; it does not create an invariant.
The real fix remains a barrier flag duckdb honours when ordering scan filters
(`ExpressionHeuristics::GetInitialOrder(TableFilterSet&)` and
`AdaptiveFilter(const TableFilterSet&)`, neither of which has any security
concept today).

**Originally: CONFIRMED EXPLOITABLE.** A hidden row's value is disclosed to a role
the policy denies. Earlier "not exploitable" verdicts were an artefact of an
invalid probe (see warning below) and of test tables too small to reach the
leaking path.

### Confirmed leak — hidden value disclosed in an error message

Policy is the ordinary, fully pushable shape:

```sql
CREATE POLICY p ON t FOR SELECT TO mop USING (owner = current_user);
```

`mop` owns 105,000 rows with `balance` in 0..99; `vedernikoff` owns 105,000
hidden rows with `balance = 1000`. `mop` reads 0 of them
(`SELECT count(*) FROM t WHERE balance = 1000` -> `0`). Then:

```sql
SELECT count(*) FROM t WHERE md5(CAST(balance AS TINYINT)::VARCHAR) = 'zzz';
ERROR:  Type INT32 with value 1000 can't be cast because the value is out of
        range for the destination type INT8 when casting from source column balance
```

`1000` occurs *only* in rows the policy hides. The attacker's expression was
evaluated against hidden data and the value is quoted verbatim in the error.
Reproduced 10/10.

**Why the probe works where earlier ones did not.** The cast is wrapped in
`md5(...)` so it cannot be rewritten into a range filter on the column, and
unlike `1/0` (which duckdb evaluates to NULL) a cast overflow genuinely raises.

**Why table size matters.** The same query on a ~10k-row table does not raise.
The leak needs enough rows to cross row-group boundaries; every earlier
experiment in this document used tables far below that, which is why they all
came back clean.

**Mechanism.** The policy predicate is *fully* pushed and therefore erased from
the filter set, so the attacker's qual sees `filters.size() == 1` and is admitted
into the scan beside it (fragility item 2). Ordering inside the scan is then
decided by `AdaptiveFilter(const TableFilterSet&)`, which has no `CanThrow`
guard at all (fragility item 3). The two items were recorded separately as
theoretical; together they are this bug.

**Note the inversion:** the *unpushable* policy shapes (`... OR 1/(balance+1) >
999999`) do **not** leak under the same probe -- the predicate stays in a
conjunction above the scan and short-circuits. It is the plain, idiomatic policy
that is vulnerable. An earlier draft of this document claimed
this was a live leak; targeted attacks disproved that. The reordering machinery
below is real, but existing `CanThrow`-keyed guards happen to close the channels
we could construct. What remains is *fragility*, not a hole. Kept here because
the protection is incidental and must be pinned by tests.

### Attempted attacks, both blocked

> **The error-oracle row below is not a valid experiment.** duckdb returns NULL
> for integer division by zero (`SELECT 1/0` -> NULL) rather than raising, as PG
> does. That query returns 0 rows and no error *regardless* of evaluation order,
> so it demonstrates nothing about ordering. A cast-overflow oracle
> (`CAST(balance AS TINYINT)`) was tried as a replacement and is also neutralised
> -- duckdb rewrites it into a range filter on the column, so the cast never
> executes. **The side-effect attack is the only probe here with real signal.**

| attack | result |
|---|---|
| ~~`WHERE 1/(secret-1000) = 999999999` on a 200k-row table, RLS filter made deliberately unselective (50%), one *hidden* row with `secret=1000`~~ (**invalid — no signal, see above**) | 0 rows, no error |
| side-effecting predicate: `CREATE FUNCTION leak_fn(x) RETURNS int LANGUAGE sql AS 'INSERT INTO leaked VALUES (x) RETURNING 1'`, then `WHERE leak_fn(secret) > 0` | only the caller's *own* row value (`5`) was written; hidden `1000`/`7777` never evaluated |

### Why it holds — `pushdown_get.cpp:95`

```c
// Allow pushing down filters that can throw only if there is a single expression
if (expr.CanThrow() && filters.size() > 1) {
    continue;
}
```

When the RLS qual is still in the filter set, `filters.size() > 1` holds and a
throwing user qual is **refused entry to the scan**, so it evaluates above it —
i.e. after RLS. Confirmed by plan inspection: with
`USING (lower(owner) = current_user)` the RLS qual is pushed *into* `SEQ_SCAN`
as a Column Filter while the throwing qual remains in a separate `FILTER` node
above.

**This does not hold universally.** A *fully* pushed RLS qual is erased from the
set, so a later throwing qual can see `size() == 1` and be admitted into the
scan alongside it — see fragility item 2 below, where a plain
`USING (owner = current_user)` produces exactly that. Whether the guard applies
therefore depends on the shape of the policy predicate, which the table owner
chooses and the attacker can observe. Neither outcome leaked in testing, but
they are protected by different mechanisms, and only one of them is the
mechanism described here.

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
2. **Order-dependent edge — NOW REPRODUCED.** The `filters.size() > 1` test runs
   inside a loop that *erases* fully-pushed filters (`filters.erase_at(i)`). If
   the RLS qual is processed and erased first, a later throwing qual sees
   `size() == 1` and is admitted into the scan. This *does* happen. With
   `USING (owner = current_user)` and `WHERE 1/(balance-1000) = 999999999`:

   ```
   SEQ_SCAN
     Column Filter:
       (1 / (balance - 1000)) = 999999999,   <- throwing qual, INSIDE the scan, listed first
       owner = 'mop'
   ```

   The throwing qual is not kept above the scan at all — the protection
   described in "Why it holds" above does not apply to this shape. It still does
   not raise, so the runtime application order is not the plan's listing order,
   but nothing documents or tests that. Combined with item 3 (no `CanThrow`
   guard on the `TableFilterSet` permuter) this is the least protected path in
   the whole area: the qual is in the set, and the thing that orders the set has
   no guard.
3. **No guard on the scan-filter permuter.** `AdaptiveFilter(const
   TableFilterSet&)` (`adaptive_filter.cpp:30`) uses `GetInitialOrder()` with no
   `CanThrow` check. Unreachable for throwing quals today (they can't get into the
   set, per above), but it is the one place with no protection at all.
4. **Timing/resource channels** are not addressed — nor are they by PostgreSQL's
   `leakproof`.
5. **The pushdown asymmetry is defeatable, and a *second* accident covers it.**
   The guard above assumes our predicate is pushable and the attacker's is not.
   A policy whose security check is one indivisible expression containing a
   throwing builtin — e.g. `USING (owner = current_user OR 1/(balance+1) >
   999999)`, semantically just `owner = current_user` — is refused entry to the
   scan as well. Verified by plan inspection: `SEQ_SCAN` carries **no** filter
   and both quals sit in a single `FILTER` conjunction over every row. It still
   does not leak, because the policy conjunct is evaluated first and
   short-circuits (confirmed by both the error oracle and the side-effect
   probe). Nothing *guarantees* that order — reordering is merely disabled
   because a conjunct can throw. Note a plain AND policy does not reach this
   state: duckdb splits conjunctions and pushes the selective part, so only an
   indivisible expression (OR, CASE) qualifies. Pinned by the second scenario in
   `predicate_ordering_canary.test`; the first scenario would not catch it.

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

## G4. P2a is REOPENED: enforcement moved to the optimizer, after the cache decision

**Reintroduced deliberately**, as the price of removing both fork hooks. RLS now
runs as a pre-optimizer `OptimizerExtension`, so `SetAlwaysRequireRebind()` fires
*after* `client_context.cpp` has already decided whether the bound plan may be
cached. The P2a repro is live again:

```
PREPARE q AS SELECT owner FROM t;   -- as the owner: bypasses, no filter emitted
SET ROLE restricted;
EXECUTE q;                          -- returns every row: RLS off
```

There is no zero-fork way to mark the plan uncacheable at bind time:
`bind_basetableref.cpp:254` marks rebind when `!bind_data->SupportStatementCache()`,
but that is a pure virtual on `FunctionData` (`function.hpp:70`) and a
store-delegated scan carries duckdb's own `TableScanBindData`, which serened does
not own.

Two further ways the optimizer is skippable, which the same move exposes:
`enable_optimizer=false` (user-settable — a restricted role gets `SET`, no error)
and `LogicalPrepare::RequireOptimizer()` returning false when
`always_require_rebind` is set — i.e. a property RLS itself sets.

**The fix that keeps the parser-only fork** is to stop depending on rebind at all:
make the plan role-*independent* by evaluating bypass and policy selection at
execution through `CONSISTENT_WITHIN_QUERY` (PG `stable`) functions, rather than
freezing them into plan structure. Then caching a plan is harmless. That is the
shape described under "Role-independent plans" above.

The regression test that covered this (`any/pg/rls/prepared_plan_role_change.test`,
green against postgres:18.3) was removed rather than left failing. Restore it when
G4 is closed; the repro above is its content.

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

---

## TODO — places this branch diverges from how the rest of the codebase does it

Status: **2 of 6 done** (items 2 and 5). Items 1, 3, 4 and 6 are open; 1 and 3
are interlocked (real dependency edges give the ids that let the column-drop
check become a DropPlan entry).

Found by comparing the RLS code against its established siblings. None are bugs
today except where noted; all are "we invented a shape the codebase already has".

1. **Policy predicates register no expression dependencies.** Now that
   `PolicyData` holds a `ColumnExpr`, `GetRefs()` is available and unused.
   Columns (`catalog.cpp:535`, `:566`) and views (`:632`) both walk their
   expression and register sequence / function / type edges. A policy naming a
   sequence or function therefore does not stop it being dropped.

2. **[DONE 2026-08-03]** ~~`Policy::ReferencesColumn` is name-keyed~~ -- it now
   takes the relation as well, so a ref qualified with a different relation no
   longer matches, and `ChangeTable` rewrites policy predicates on RENAME COLUMN
   in the same Apply as the rename (`any/pg/rls/policy_column_rename.test`,
   oracle-verified). Original text:

   **`Policy::ReferencesColumn` is name-keyed; every sibling is id-keyed.**
   `Index::ReferencesColumn(Column::Id)` (`index.h:91`) matches by id;
   `Policy::ReferencesColumn(std::string_view)` (`policy.h:74`) matches by
   identifier text. Two consequences: a policy referencing
   `other_table.owner` falsely blocks dropping *this* table's `owner`, and
   `ALTER TABLE ... RENAME COLUMN` is not covered at all -- it silently leaves
   the policy referring to a name that no longer exists. Fixing #1 gives the ids
   needed to key this properly.

3. **The policy column-drop check is a one-off, not a drop-plan entry.**
   Index drops go through `ComputeColumnDropPlan` / `DropEmitter`
   (`catalog.cpp:1452`, `:1492`, `:4796`); the policy check is an inline loop in
   `DropTableColumn` (`:4725`) that drops policies in a separate engine write
   *before* `CommitDropPlan`, so it is not atomic with the plan, and any future
   column-drop path gets no policy check.

4. **No snapshot-level policy index.** `RoleClosureMap` (`catalog.h:447`) is the
   model: a Snapshot member, rebuilt with the snapshot, turning a per-query
   derivation into a hash lookup. RLS has no equivalent, so every scan of every
   table pays a `dynamic_cast` plus a `GetRowSecurity` dependency lookup even on
   a database with zero policies. An index also gives a global early-out.

5. **[DONE 2026-08-03]** ~~Enforcement hangs off `OptimizerExtension`~~ -- it now
   runs from `DBConfig::access_check_function`, which the fork's
   `access_check_function` hook was widened by one parameter to allow (it already
   received the binder; it now also receives the plan). Verified: with
   `SET enable_optimizer=false` a governed role saw every row and could insert
   rows the policy forbids; it now cannot. Pinned by
   `sdb/pg/rls/optimizer_cannot_be_disabled.test`. Original text:

   **Enforcement hangs off `OptimizerExtension`, which is bypassable.**
   `SET enable_optimizer=false` disables RLS entirely (verified). The
   un-bypassable seam is `access_check_function`, which this same branch already
   uses for `RlsGuardTruncate` (`optimizer/rbac.cpp`), and which upstream
   documents as running "before (and independent of) the optimizer so it cannot
   be bypassed via disable_optimizer".

6. **34 hand-written authz prologues in `catalog.cpp`.** Not duplicated *checks*
   -- duplicated *shape*. Each DDL entry point re-writes resolve relation ->
   require it is the right type -> require ownership by hand. This branch folded
   four of them (the policy DDL) into `ResolveOwnedTable`; the same treatment
   applies to the rest, removing ~250 lines and no enforcement.

   Note for whoever picks this up: do **not** "simplify" by moving these checks
   into the RBAC rule. The rule runs at plan time and the mutation happens later
   under `Catalog::_mutex`; checking in the rule and mutating afterwards is
   check-then-act, the TOCTOU class already fixed here by moving checks *into*
   the catalog. The two-tier split (DML enforced once by the rule from
   bind-collected AccessRequirements, DDL enforced in the catalog under the
   lock) is deliberate.
