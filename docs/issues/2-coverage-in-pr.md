# Issue 2 -- Coverage visible in PRs

**Scope:** turn the coverage we already collect into something seen per-PR,
ClickHouse-style (diff coverage on changed lines), as part of the default run.
Builds on issue 1's nightly-coverage-visible step.

---

## Current state
- `CMakeLists.txt` supports `USE_COVERAGE=SourceBased` (llvm `-fprofile-instr-
  generate -fcoverage-mapping`); `scripts/prepare_coverage.py` merges `.profraw`
  -> HTML via `llvm-profdata`/`llvm-cov`.
- Runs **nightly only**, output is an **HTML artifact nobody opens**. No PR
  comment, no diff coverage, no gate.

## What ClickHouse does (model)
- Per-test coverage collected, merged; `genhtml` tree uploaded to S3.
- **Diff coverage**: compares against the base branch and prints **newly-covered
  vs newly-uncovered lines** for the PR (`generate_diff_coverage_report.sh`,
  `print_newly_covered_code.py`).
- Stored in CIDB for trend; shown via report links.

## Target
A PR comment (or check) showing **coverage of the lines this PR adds/changes** --
"X of Y new lines covered", with the uncovered ones listed. Informational, not a
hard gate (DuckDB keeps codecov informational -- right call initially).

---

## Concrete approach (copy ClickHouse, minimal infra)
1. **Instrumented build + run** of the relevant suites (unit + our sqllogic +
   recovery) with `USE_COVERAGE=SourceBased`. Run as a **separate, non-blocking
   job** (own machine) so it never gates the fast tier.
2. `llvm-cov export -format=lcov` -> feed `diff-cover` (or ClickHouse's scripts)
   against the PR merge-base -> produce changed-line coverage.
3. Post as a **PR comment / neutral check** with the summary + uncovered lines.
4. Reuse `prepare_coverage.py` for the full HTML; link it from the comment.

## Decisions to make
- **Build cost:** instrumented + full suite is slow. Mitigations: only the
  changed-category suites; run async (comment lands after the green fast tier);
  or coverage only on a label for big PRs. Pick based on issue-1 timings.
- **External vs self-hosted:** codecov (easy diff UI, external upload) vs a
  self-hosted `diff-cover` comment (no data leaves CI). Privacy call.
- **Gate or not:** start informational. Possibly later a soft floor on
  novel-surface dirs (`server/`, `libs/iresearch/`), never on generated/3p code.
- **Scope of measured code:** restrict to our code (`server/`, `libs/iresearch`,
  connector) -- exclude `third_party/` (DuckDB tests itself).

## Open questions
- Which suites' coverage counts -- unit only (fast, in-process) or include
  over-the-wire sqllogic (slower, but covers pg/connector paths that unit misses)?
- Per-test coverage (ClickHouse) is powerful but heavy -- worth it, or is merged
  whole-run coverage enough for us now?

## Done when
- Opening a PR (or a labeled run) yields a visible changed-line coverage summary,
  and the full HTML report is one click away.
