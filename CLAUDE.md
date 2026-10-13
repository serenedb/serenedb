# Notes for AI agents

## Rules

- Don't write comments unless explicitly asked to; the license block and
`# DOCS_TEST:` markers are required, not comments in this sense. When a comment
is outdated, or your change makes it outdated, remove it unless asked to keep
it.

- Don't use python/sed for normal code editing or reading, only for real
scripting changes.

- Read `CONTRIBUTING.md`.

- A user-visible change is not finished until `docs/` describes it. Write the
page in the same change, following `.claude/rules/docs.md`, and say in your
summary which page you added or updated.

- Code is the evidence: PR descriptions, comments, docstrings, review notes and
memory are claims. Read the implementation and its callers before relying on
them.

- Before filing an issue or writing a fix, check that it isn't already there:
search open and recently closed issues and PRs
(`gh issue list --state all --search <words>`,
`gh pr list --state all --search <words>`). The same problem already filed: add
your findings to that issue instead of opening another. A PR already fixes it:
link the PR instead of fixing it again (a stale one: ask whether to wait, build
on it or fix it here). Only related: go ahead, mention the related issue in
yours and leave a cross-reference on it.

- When you find a bug, look for the same mistake elsewhere -- the other callers,
the sibling functions and types, the same pattern in other files -- and fix the
whole family in one change, with a test for each, or file the ones that don't
belong in it.

- Never present an estimated or remembered number as measured: measure it or
say it is unknown.

- Build and run only what the task needs: the touched targets and the tests that
cover the change. Full sqllogic/recovery suites, the DuckDB suites, stress,
benchmarks and sanitizer builds only when the user asks. CI only once the
change passes locally, and only when the user asks (`.claude/rules/ci.md`).

- Repo-wide knowledge goes into the repo, not into personal memory. What every
contributor needs goes into its topic page in `.claude/rules/` (a new topic gets
a page with `paths:` for the files it covers, and a link from
`CONTRIBUTING.md`); instructions only for Claude go into this file or a skill in
`.claude/skills/`. Never copy a rule into a second place: point to it. Propose
the change in the same PR; when the user rejects a rule, save that to memory and
never propose that kind of rule again.

## Formatting

Code is never formatted by hand: serenedb's own code goes through pre-commit
(`.clang-format`), the submodules as `.claude/rules/third-party.md` says.

Before saying done, run `pre-commit run --files <changed files>`. As a git hook
it stashes unstaged changes, so in a checkout another session also edits, use
`--files`.

## Before writing tests

Read a sibling test in the same `tests/<area>/` directory first and mirror its
style.

## Local smoke server

Start serened on a free port (`ss -ltnH "sport = :<port>"` prints nothing) with
a fresh data dir (`mktemp -d`). When you're done, stop it by its pid and remove
the data dir.

```bash
./build/bin/serened <datadir> --listen='postgres://127.0.0.1:<port>' \
  >/tmp/$USER-serened-<port>.log 2>&1 & echo $!
kill -0 <pid> && psql -h 127.0.0.1 -p <port> -U postgres -d postgres -c '<sql>'
kill <pid>
rm -rf <datadir>
```

psql is refused until the server is up: retry while `kill -0 <pid>` succeeds.
Once it fails, serened has exited, and the log file says why. The server logs
to that file; `--log_level=debug` adds detail.
