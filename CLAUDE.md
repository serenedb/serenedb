# Notes for AI agents

## Rules

- You don't allowed to write any comments until you explictly asked to write them.
If you noticed outdated comment or make some comment outdated, just remove it until explicitly asked to keep it.

- Don't use python/sed for normal code editing/reading, only for real scripting changes

- Read `CONTRIBUTING.md` -- it covers build, tests, branches, commits, PRs, and
C++ style. This file only flags traps that aren't in there.

- A user-visible feature is not finished until `docs/` describes it. Write the
page in the same change, following the **Documentation** section of
`CONTRIBUTING.md`, and say in your summary which page you added or updated.

- Git belongs to the user: never commit, push, merge, rebase or switch branches
unless asked. Never undo with `git checkout/restore/reset/stash/clean` -- the
tree and the index can hold someone's uncommitted work, and staged means
reviewed. Undo your own edits with Edit.

- On a PR branch, add commits; no amend, rebase or force-push unless asked.
Push only as `git push origin <branch>:refs/heads/<branch>`, never to `main` or
a fork `v20*` branch. Create branches with
`git switch -c <branch> --no-track origin/main`.

- No AI attribution anywhere: commits, PR bodies, comments, issues.

- GitHub text never contains `owner/repo#N` or a `#N` meant for another
repository: GitHub notifies every one of them. Link code by commit SHA, not by
branch. `gh pr edit` can fail on GitHub's retired projects-classic API; edit a
PR with `gh api -X PATCH repos/serenedb/serenedb/pulls/<N> -f title=... -F body=@body.md`.

- Build and run only what the task needs: the touched targets and the tests that
cover the change. Full sqllogic/recovery suites, benchmarks and sanitizer
builds only when the user asks; CI runs the suites.

- A bug fix starts with a test that fails for the claimed reason. Never edit a
test or an expectation just to make a failure go away.

- No subagent or workflow fan-outs unless the user asks for one.

- Issues labelled `student-task` (epic #1134) are reserved for students: don't
implement one unless the user names that issue.

- C++ navigation and diagnostics come from the `clangd-lsp` plugin (enabled in
`.claude/settings.json`; `clangd` is installed on the dev machines). Prefer
the LSP tool's definitions and references over grepping for a symbol. clangd
reads `build/compile_commands.json`; with another build dir, run
`ln -s <build dir>/compile_commands.json .` in the checkout once.

- Repo-wide knowledge goes into the repo, not into personal memory, which is
per machine and invisible to the team: propose the line for this file, the
matching `.claude/rules/*.md` or the skill in `.claude/skills/` in the same PR.

## Shared machines

The dev machines are shared by several developers and their agents. Nothing
there is yours except your own processes and files.

- Prefix anything in `/tmp` with `$USER`, or keep it in the scratchpad.
- Kill only pids you started (see the smoke server below).
- Before timing anything: `uptime` and `ps -eo user,pcpu,comm --sort=-pcpu | head`.
  Never measure while a build or test runs on the box; discard overlapped numbers.
- Inside tmux, the kernel OOM-killing any child stops the whole pane, Claude
  included (tmux 3.4 scopes use `OOMPolicy=stop`). Run anything that can
  allocate without bound in its own scope:
  `systemd-run --user --scope -p MemoryMax=64G <cmd>`.
- Never pipe `ninja` into `tail`/`grep`: the pipe hides ninja's exit code.

## Formatting

serenedb's own code and the DuckDB family are never formatted by hand: run the
tools below and commit what they produce. Other submodules have no formatter
set up; there, match the surrounding code by hand, and never run a formatter
over their files.

- serenedb's own code: `.clang-format` through pre-commit (see `CONTRIBUTING.md`).
  Before saying done, run `pre-commit run --files <changed files>`: it also
  rewrites typographic Unicode (arrows, the multiplication sign, em/en dashes,
  ellipsis, curly quotes) to ASCII, rejects `/tmp` in sqllogic tests and checks
  the license header. As a git hook it stashes unstaged changes, so in a
  checkout another session also edits, use `--files`.
- The DuckDB fork, from `third_party/duckdb`, in this order:
  1. `./scripts/parser/build_grammar.sh` -- the PEG grammar and transformer;
     `make generate-files` does not regenerate the parser, so this goes first.
  2. `make generate-files` -- settings, serialization, enum_util, functions,
     metrics and storage info, then formats the tree.

  Commit regenerated artifacts separately from the change that caused them
  (`regen: ...`), as upstream does.
- Then, from the repo root, `./scripts/format_duckdb.sh` -- clang-format 11.0.1
  (in docker) over the changed files of every duckdb submodule and
  `duckdb_clickhouse`, each with its own `.clang-format`.

## Before writing tests

- Sqllogic: read a sibling `.test` first. Control directives, retry patterns,
  connection naming, and the right subtree (`any/sdb/pg/recovery`) are set by
  example, not docs.
- gtest / microbench: mirror an existing one in the same `tests/<area>/` subtree.
- When a feature goes away, its tests go too (port them when there is a
  replacement); never add tests that only assert something no longer exists.

## Local smoke server

Pick a free `<port>` (`ss -ltn | grep :<port>` prints nothing). You own it for
the session -- when you're done, kill the process so the next contributor /
agent doesn't inherit a stale serened.

```bash
# Backgrounded server logs land in the file you redirect to. DuckDB's
# LogManager writes to in-memory storage by default; query
# `SELECT * FROM duckdb_logs()` or the `sdb_log` catalog view from psql.
./build/bin/serened ./build_data_<purpose> --listen='postgres://0.0.0.0:<port>' \
  >/tmp/$USER-serened-<port>.log 2>&1 &

# A busy port or a retired flag makes serened exit; only the log says why.
# Default database is `postgres` (serenedb has no separate default). Use the
# system psql: `serened psql` is the embedded shell, not a client.
psql -h 127.0.0.1 -p <port> -U postgres -d postgres -c 'SELECT 1'

# when finished -- only the listener; `lsof -t -i:<port>` also returns psql
# clients and the sshd behind SSH/IDE tunnels:
kill $(lsof -t -iTCP:<port> -sTCP:LISTEN) 2>/dev/null
rm -rf build_data_<purpose>    # only if you don't need the datadir afterwards
```
