# Branching, commits, PRs

- **Branch from `main`**, one focused change per PR.
- **Branch name:** `<author>/<topic>` (e.g. `mbkkt/fix-view-indexes-recovery`). Topic is free-form.
- **Conventional commit prefix** in the PR title: `feat:`, `fix:`, `perf:` (most common), or one of `refactor:`, `chore:`, `docs:`, `test:`, `ci:`, `build:`, `style:`, `misc:`. Don't invent new ones -- if none fit, ask.
- **Squash-merge:** the PR title is the final commit subject and the PR description is the body. Branch-internal commit messages are discarded, so they can be anything.
  - Exception: if your branch has exactly one commit and you let GitHub open the PR for you, GitHub will pre-fill the PR title and description from that commit -- so in that case keep the commit message PR-ready.
- **Pre-commit hooks** run as a PR check. You don't have to install them locally; if you want to check before pushing, run `pre-commit run --all-files`.
- **CI must pass** and one maintainer must approve before merge.
- **PR title:** name the exact thing (`fix: jobs and text search dictionaries in nested schemas`, `perf: n-gram prefilter for case-insensitive ASCII letters in ts_regexp`), not `fix: bugs` and not a sentence about the PR.
- **PR description:** why, not what -- the diff shows what. The problem and who hits it (for a fix, the failing behaviour and the root cause); the approach, and the alternative it beat when that's not obvious; the tests that cover it, and for `perf:` the end-to-end numbers against main with the build and machine load; the `docs/` page added or updated; fork changes by commit SHA. One line per paragraph, no hard wraps.
- **Other repositories:** refer to another repository's issue or PR as plain text (`serenedb/duckdb PR 89`) or link it through `redirect.github.com` instead of `github.com`, never as `#N`, `owner/repo#N` or a `github.com` URL: GitHub links all three back from the target. This holds for commit messages, PR descriptions and comments. Link code by commit SHA, not by branch.
