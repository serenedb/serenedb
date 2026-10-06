---
name: review
description: Review a SereneDB pull request, branch or diff for correctness, crash safety, concurrency, performance and test coverage, with findings verified against the code. Use when asked to review a PR, a branch, a commit range or the working-tree diff.
argument-hint: "[PR-number | branch | diff-spec]"
context: fork
background: false
---

# Reviewing a SereneDB change

Review target: $ARGUMENTS (nothing given: the working tree against `origin/main`). This runs in a fresh context, without the conversation that produced the change: judge the code, not the intent behind it.

High signal only: real problems in correctness, durability, concurrency, resources, performance and tests. Formatting and naming are enforced by pre-commit and `.clang-tidy`; don't spend findings on them.

## Get the change

- PR: `gh pr view <N> --repo serenedb/serenedb --json title,body,baseRefName,headRefName,files,comments` and `gh pr diff <N> --repo serenedb/serenedb`.
- Branch: `git diff $(git merge-base origin/main <branch>) <branch>`.
- Range or working tree: `git diff <range>` / `git diff HEAD`.
- Read every changed line, then the surrounding code the diff relies on. A summary of the diff is not a review.
- Submodule pointer bumps: review the fork commits too (`git -C third_party/<fork> log --oneline <old>..<new>`).

## Gates

Work through these before giving a verdict; say which ones you could not close and what evidence would.

1. **Contract.** What does the change promise (title, description, tests, docs)? A `perf:` PR promises a measured end-to-end gain; a `fix:` promises the bug can't come back, which needs a test that failed before.
2. **Impacted surface.** Follow the changed invariant through callers, sibling implementations and both wire protocols (simple and extended). For a new setting or option: SET/RESET/SHOW, defaults, persistence, docs.
3. **Failure paths.** Startup, shutdown, cancellation, exceptions halfway through, rollback, `kill -9` at every step, and replay after restart. Catalog and storage changes must survive a restart from a kept datadir and from a checkpoint (`recovery/` tests are the place to prove it).
4. **Evidence.** Each material claim maps to proof: a test that fails without the change, measurements against main for performance, a recovery test for crash behaviour.
5. **Everything else**: performance on hot paths, duplication, dead code, docs.

## Where SereneDB changes break

- Catalog, WAL and commit: DDL inside a transaction with data, rollback of a half-applied DDL, lock order between catalog sets, a guard in `Create*`/`Alter*` that rejects the internal entries boot recreates (`info.internal`, default schemas).
- MVCC and visibility: system transactions that can't see session entries, snapshots taken at the first statement, index reads that are as of the last refresh.
- Concurrency: `absl::Mutex` lock order, work that can still mutate after a guard, async work in flight at shutdown, parallel sinks writing to one connection's buffer.
- pg-wire: extended-protocol parameter types, implicit transactions per Sync, error SQLSTATE (client-facing errors use `THROW_SQL_ERROR`).
- Storage formats: new fields, removed fields and enum values against CONTRIBUTING.md "Storage compatibility" (and the team's current release policy); DuckDB-format files stay readable by DuckDB.
- Hot paths: allocations, copies, virtual calls, `Flatten`/`GetValue`, temporary keys in map lookups (see `.claude/rules/cpp.md`).
- Forks: the smallest possible diff, upstream first, regen in its own last commit (see `.claude/rules/duckdb-fork.md`).
- Tests: the right subtree (`any`/`sdb`/`pg`/`recovery`), both engines for `any/pg`, block-form errors, no `SET GLOBAL` outside `recovery/`, server-global names guarded (see `.claude/rules/sqllogic.md`).
- Docs: a user-visible change ships its `docs/` page in the same PR.

## Signal

- Verify every finding by reading the code it is about before reporting it. "main does it differently" is not a finding.
- State findings as broken invariants: "`X` promises `P`, but `Y` skips `Q`, so ...". Definite bugs are findings; plausible serious risks are "needs verification" with the check that settles them.
- Nothing unrequested: no speculative refactors, no new abstractions, no style nits.
- Answer in the conversation. Post on GitHub only when the user asks.

## Report

1. Verdict in one line.
2. Findings, most severe first: `file:line`, the broken invariant, the scenario that triggers it, the fix direction.
3. Missing tests or evidence.
4. Gates not closed and why.
