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

- Commits, PRs and comments may refer to an issue or PR of another repository,
but never in a form GitHub links back from: no `#N` meant for another
repository, no `owner/repo#N`, no issue or PR URL. Write it as plain text
(`serenedb/duckdb PR 89`) or inside backticks. Link code by commit SHA, not by
branch. `gh pr edit` can fail on GitHub's retired projects-classic API; edit a
PR with `gh api -X PATCH repos/serenedb/serenedb/pulls/<N> -f title=... -F body=@body.md`.

- Build and run only what the task needs: the touched targets and the tests that
cover the change. Full sqllogic/recovery suites, benchmarks and sanitizer
builds only when the user asks; CI runs the suites.

- A bug fix starts with a test that fails for the claimed reason. Never edit a
test or an expectation just to make a failure go away.

- C++ symbols are looked up with the LSP tool (`clangd-lsp` plugin), not grep:
`goToDefinition`, `findReferences`, `incomingCalls`, `goToImplementation` for
the overrides of a virtual, `hover` for types. grep stays the tool for strings,
SQL, tests, error messages and non-C++ files. clangd starts at the first LSP
call of a session and loads its index for about 5 seconds (minutes in a fresh
checkout): repeat an empty or short first answer before trusting it. No LSP
tool means the plugin is missing
(`claude plugin install clangd-lsp@claude-plugins-official`); an LSP error that
the server can't start means `clangd` itself is missing. Install what is
missing, or ask the user to. clangd reads `compile_commands.json` from the
checkout root or `build/`; with another build dir, run
`ln -s <build dir>/compile_commands.json .` once.

- Repo-wide knowledge goes into the repo, not into personal memory, which is
per machine and invisible to the team: propose the line for this file, the
matching `.claude/rules/*.md` or the skill in `.claude/skills/` in the same PR.

## Parallel work

Other work runs next to yours: the user's other Claude sessions, builds, tests
and servers, in this checkout or another one, and other developers on the
shared dev machines. Nothing is yours except the processes and files you
created.

- Uncommitted changes in the tree may belong to another session: never discard
  them wholesale (`git checkout .`, `git reset --hard`, `git stash`,
  `git clean`). Undo your own edits one by one.
- Temporary files, data dirs and ports get names nobody else picks: `mktemp -d`
  or the session's scratchpad, a free port, a data dir named after the task;
  never a fixed `/tmp/<name>`.
- Kill only pids you started, never by name or pattern: `pkill serened` also
  stops the servers of the user's other sessions.
- Before timing anything: `uptime` and `ps -eo user,pcpu,comm --sort=-pcpu | head`.
  Never measure while a build or test runs on the box; discard overlapped numbers.

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
- The DuckDB fork: from `third_party/duckdb`,
  `DUCKDB_FORMAT_SKIP_FETCH=1 make generate-files` regenerates the PEG grammar
  and transformer, settings, serialization, enum_util, functions, metrics and
  storage info, then formats the files that differ from the local `main` branch
  (without the variable it first runs `git fetch origin main:main`). Commit
  regenerated artifacts separately from the change that caused them
  (`regen: ...`), as upstream does.
- Then, from the repo root, `./scripts/format_duckdb.sh` -- clang-format 11.0.1
  (in docker) over the changed files of every duckdb submodule and
  `duckdb_clickhouse`, each with its own `.clang-format`.

## Before writing tests

- Sqllogic: read a sibling `.test` first; `.claude/rules/sqllogic.md` has the
  conventions.
- gtest / microbench: mirror an existing one in the same `tests/<area>/` subtree.
- When code goes away, its gtests go with it (port them when there is a
  replacement). A sqllogic test changes only when the behaviour it pins
  changes, such as an error that no longer happens or a feature that is now
  implemented: rewrite it to the new behaviour. Never add a test that only
  asserts that something no longer exists.

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
# Default database is `postgres` (serenedb has no separate default).
psql -h 127.0.0.1 -p <port> -U postgres -d postgres -c 'SELECT 1'

# when finished -- only the listener; `lsof -t -i:<port>` also returns psql
# clients and the sshd behind SSH/IDE tunnels:
kill $(lsof -t -iTCP:<port> -sTCP:LISTEN) 2>/dev/null
rm -rf build_data_<purpose>    # only if you don't need the datadir afterwards
```
