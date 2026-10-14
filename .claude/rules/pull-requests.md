# Branching, commits, PRs

## serenedb PRs

- Branch from `main` as `<author>/<topic>`, one focused change per PR.
- PRs are squash-merged: the title becomes the commit subject and the description its body, so branch commits can say anything.
- **Title:** a conventional prefix (`feat`, `fix`, `perf`, `refactor`, `chore`, `docs`, `test`, `ci`, `build`, `style`, `misc`; ask if none fits) and the exact thing changed (`fix: jobs and text search dictionaries in nested schemas`), not `fix: bugs`.
- **Description:** why, not what. The problem and who hits it (for a fix, the failing behaviour and the root cause); the approach, and the alternative it beat when that isn't obvious; the tests; for `perf:`, end-to-end numbers against main with the build and the machine load; the `docs/` page changed; the companion fork PRs. One line per paragraph, no hard wraps.
- **Links:** our own repositories link as `owner/repo#N`, and a PR and its fork PRs link each other. Another project's issues and PRs are plain text (`duckdb/duckdb PR 123`) or a `redirect.github.com` link: GitHub notifies the target of every `owner/repo#N` and `github.com` link. Code is linked by commit SHA, never by branch.
- `pre-commit` runs on every push. `serenedb | build & test` runs when **Trigger Jobs** is ticked ([ci.md](ci.md)), by Claude only when the user asks and after the change passes locally.
- serenedb PRs are merged by a maintainer, after an approval and a green `build & test`.

## Forks

Every submodule under `third_party/` is our fork; the in-tree directories there are ordinary serenedb code.

- A fork change is a PR into the fork's current version branch (`vYYYY.MM.DD`, the one main's gitlink is on), and the serenedb PR points the gitlink at that PR's head: CI builds both together, and `check-submodule-pointers` fails until the fork PR merges, as expected.
- Fork PRs are rebase-merged, so the fork history is what reviewers keep: one concern per commit, complete across every path it spans, a conventional subject with a scope (`fix(planner): ...`), every message final.
- A fix to lines one of our fork commits introduced is a `fixup! <that commit's subject>` commit, found by blaming those lines, so the next update folds it into its commit.
- Merge only when the user says to: fast-forward the version branch to the fork PR's head, so the gitlink stays valid, then rerun the serenedb PR's `pre-commit` job. If the version branch moved since CI ran, rebase the fork PR onto it and usually the serenedb PR onto main, point the gitlink at the new head and run CI again: a green run proves only the tree it built.
- A version branch is never rewritten, except on the user's explicit order.

## Pushing

- Push a branch by name (`git push origin <branch>`), from a local branch tracking `origin/<branch>`.
- Never push `main` or a version branch, except a merge the user asked for.
- Force-push a feature branch only on purpose, knowing why its history changes, with `--force-with-lease=<branch>:<the head you started from>`: a push someone made meanwhile then fails yours instead of disappearing.
