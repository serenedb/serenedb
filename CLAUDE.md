# Notes for AI agents

## Rules

- Don't write comments unless explicitly asked to. When a comment is outdated,
or your change makes it outdated, remove it unless asked to keep it.

- Don't use python/sed for normal code editing or reading, only for real
scripting changes.

- Read `CONTRIBUTING.md` -- it covers build, tests, branches, commits, PRs, and
C++ style. This file only flags traps that aren't in there.

- A user-visible feature is not finished until `docs/` describes it. Write the
page in the same change, following the **Documentation** section of
`CONTRIBUTING.md`, and say in your summary which page you added or updated.

- After a change, do its follow-up steps from `CONTRIBUTING.md` "When you
change ..." (regeneration, fork PRs and gitlinks, fixtures, docs tables):
nothing runs them for you.

- Before writing a commit message, PR description or GitHub comment, follow
`CONTRIBUTING.md` "Branching, commits, PRs"; it says how to refer to another
repository's issues and PRs without GitHub linking back.

- Code is the evidence: PR descriptions, comments, docstrings, review notes and
memory are claims. Read the implementation and its callers before relying on
them.

- Never present an estimated or remembered number as measured: measure it or
say it is unknown.

- Build and run only what the task needs: the touched targets and the tests that
cover the change. Full sqllogic/recovery suites, benchmarks and sanitizer
builds only when the user asks; CI runs the suites.

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

- Before writing C++, read `CONTRIBUTING.md` "C++ Code Style". Before writing a
helper, container, algorithm or utility, search abseil, `server/utils/`,
`iresearch/utils/` and DuckDB for it: never implement what already exists.

- Repo-wide knowledge goes into the repo, not into personal memory, which is
per machine and invisible to the team. What every contributor needs goes into
`CONTRIBUTING.md`; instructions only for Claude go into this file,
`.claude/rules/*.md` or a skill in `.claude/skills/`. Never copy a
`CONTRIBUTING.md` rule into them: point to its section. Propose the change in
the same PR.

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
- Before timing anything, follow `CONTRIBUTING.md` "Performance" (quiet
  machine, discard overlapped numbers).

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
- The DuckDB family (the duckdb submodules, `database-connector`,
  `duckdb_clickhouse`): `scripts/duckdb_family.sh`, which runs DuckDB's own
  format.py, generators and Makefile targets with their pinned tools; `--help`
  describes every mode. Never run a generator, format.py or clang-format there
  by hand.
  - `format` before every commit, and `format --check --range <a>..<b> <dir>`
    before pushing a series: every commit, merges included, formatted on its
    own.
  - `regen` builds the duckdb fork's `regen:` commit, a DuckDB update's one or
    a pull request's own, with DuckDB's generators in DuckDB's order;
    `regen --check` proves it is current.

## Before writing tests

- Read `CONTRIBUTING.md` "Test": where a test goes, when a change needs one,
  and "Writing sqllogic tests".
- Sqllogic: read a sibling `.test` first and match its style.
- gtest / microbench: mirror an existing one in the same `tests/<area>/` subtree.

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
