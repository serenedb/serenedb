---
paths:
  - "third_party/duckdb/**"
  - "third_party/duckdb_*/**"
  - "third_party/database-connector/**"
---

# Editing the DuckDB-family forks

`tests/duckdb/README.md` is the source of truth for branches, the update recipe and the patchset; read its "Branches" and "Traps" sections before fork work. Regeneration, commits, the fork PR and the gitlink bump: CLAUDE.md "When you change ...".

- First check whether upstream already fixed it (`git log -S <symbol> upstream/main -- <file>`). If so, `git cherry-pick -x` it. Syncing with upstream follows the README's update recipe, not cherry-picks.
- Keep the fork diff small: an additive, self-contained function beats a parameter threaded through core classes; never add per-object state to core classes.
- Message wording, DETAIL lines and display cosmetics never justify a fork change; update the test instead.
- No comments, and no provenance markers ("SereneDB fork:").
- Format only with clang-format 11.0.1: `./scripts/format_duckdb.sh` from the repo root, or the fork's `scripts/format.py` with the format venv's clang-format first on PATH. The system clang-format 21 produces different output.
- One concern per commit, with a conventional-commit subject.
- DuckDB's own suites: `./tests/duckdb/run.sh --suite core` (skip lists in `tests/duckdb/config/<suite>.json`).
