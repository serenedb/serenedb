---
paths:
  - "third_party/duckdb/**"
  - "third_party/duckdb_*/**"
  - "third_party/database-connector/**"
---

# Editing the DuckDB-family forks

`tests/duckdb/README.md` is the source of truth for branches, the update recipe and the patchset; read its "Branches" and "Traps" sections before fork work.

- First check whether upstream already fixed it (`git log -S <symbol> upstream/main -- <file>`). If so, `git cherry-pick -x` it. Syncing with upstream follows the README's update recipe, not cherry-picks.
- Keep the fork diff small: an additive, self-contained function beats a parameter threaded through core classes; never add per-object state to core classes.
- Message wording, DETAIL lines and display cosmetics never justify a fork change; update the test instead.
- No comments, and no provenance markers ("SereneDB fork:").
- Format only with clang-format 11.0.1: `./scripts/format_duckdb.sh` from the repo root, or the fork's `scripts/format.py` with the format venv's clang-format first on PATH. The system clang-format 21 produces different output.
- Generated files come only from the generators (CLAUDE.md "Formatting"); never edit them by hand.
- Commits: one self-contained concern each, conventional subject with a scope (`fix(parser): ...`); regenerated files in one `regen:` commit, last; no `#N` or `owner/repo#N` in messages.
- The superproject's submodule pointer is bumped separately, never inside a feature commit. Pre-commit `check-submodule-pointers` rejects a gitlink to a fork commit that is not on a `vYYYY.MM.DD` branch.
- `cmake` reconfigures run `git submodule update --force` on a submodule whose HEAD differs from the gitlink. Since 3d12e1a84 a dirty submodule is skipped, but a clean fork branch ahead of the gitlink is detached. Configure with `-DAUTO_UPDATE_MODULES=OFF` while working on a fork branch and check `git -C third_party/duckdb branch --show-current` after any reconfigure.
- DuckDB's own suites: `./tests/duckdb/run.sh --suite core` (skip lists in `tests/duckdb/config/<suite>.json`).
