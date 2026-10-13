# Notes for AI agents

## Rules

- Don't write comments unless explicitly asked to; for you this overrides
"Comments" in `.claude/rules/cpp.md`. The license block and `# DOCS_TEST:`
markers are required, not comments in this sense. When a comment is outdated, or
your change makes it outdated, remove it unless asked to keep it.

- Don't use python/sed for normal code editing or reading, only for real
scripting changes.

- Read `CONTRIBUTING.md` -- it covers the build and links the topic pages in
`.claude/rules/`, which load by themselves when you open a file they cover.
This file only flags traps that aren't in there.

- A user-visible change is not finished until `docs/` describes it. Write the
page in the same change, following `.claude/rules/docs.md`, and say in your
summary which page you added or updated.

- Code is the evidence: PR descriptions, comments, docstrings, review notes and
memory are claims. Read the implementation and its callers before relying on
them.

- Before filing an issue or writing a fix, check that it isn't already there:
search open and recently closed issues and PRs
(`gh issue list --state all --search <words>`,
`gh pr list --state all --search <words>`). The same problem already filed: add your findings to
that issue instead of opening another. A PR already fixes it: link the PR
instead of fixing it again (a stale one: ask whether to wait, build on it or
fix it here). Only related: go ahead, mention the related issue in yours and
leave a cross-reference on it. Post, file or comment on GitHub only after the
user agrees to the text.

- A bug rarely comes alone. When you find one, look for the same mistake
elsewhere -- the other callers, the sibling functions and types, the same
pattern in other files -- and fix the whole family in one change, with a test
for each, or file the ones that don't belong in it.

- Never present an estimated or remembered number as measured: measure it or
say it is unknown.

- Build and run only what the task needs: the touched targets and the tests that
cover the change. Full sqllogic/recovery suites, the DuckDB suites, stress,
benchmarks and sanitizer builds only when the user asks. CI only once the change passes locally, and only
when the user asks (`.claude/rules/ci.md`).

- C++ symbols are looked up with the LSP tool (`clangd-lsp` plugin), not grep:
`goToDefinition`, `findReferences`, `incomingCalls`, `goToImplementation` for
the overrides of a virtual, `hover` for types. grep stays the tool for strings,
SQL, tests, error messages and non-C++ files, and grep too before renaming or
removing a symbol: clangd sees one build configuration, not the `#if` branches
for other platforms. clangd starts at the first LSP
call of a session and loads its index for about 5 seconds (minutes in a fresh
checkout): repeat an empty or short first answer before trusting it. The LSP
tool is deferred: load it with ToolSearch (`select:LSP`). If ToolSearch finds
none, the plugin is missing
(`claude plugin install clangd-lsp@claude-plugins-official`); an LSP error that
the server can't start means `clangd` itself is missing. Ask the user to
install what is missing. clangd reads `build/compile_commands.json`.

- Repo-wide knowledge goes into the repo, not into personal memory, which is
per machine and invisible to the team. What every contributor needs goes into
its topic page in `.claude/rules/` (a new topic gets a page with `paths:` for
the files it covers, and a link from `CONTRIBUTING.md`); instructions only for
Claude go into this file or a skill in `.claude/skills/`. Never copy a rule
into a second place: point to it. Propose the change in the same PR; when the
user rejects a rule, save that to memory and never propose that kind of rule
again.

## Parallel work

Other work runs next to yours: the user's other Claude sessions, builds, tests
and servers, in this checkout or another one, and other developers on the
shared dev machines. Nothing is yours except the processes and files you
created.

- Uncommitted changes in the tree may belong to another session: never discard
  them wholesale (`git checkout .`, `git reset --hard`, `git stash`,
  `git clean`). Undo your own edits one by one, stage your own files by path
  (never `git add -A` or `commit -a`), and switch branches only in a checkout
  no other session works in.
- Temporary files, data dirs and ports get names nobody else picks: `mktemp -d`
  or the session's scratchpad, a free port, a data dir named after the task;
  never a fixed `/tmp/<name>`.
- Kill only pids you started, never by name or pattern: `pkill serened` also
  stops the servers of the user's other sessions.
- Before timing anything, follow "Performance" in `.claude/rules/cpp.md`
  (quiet machine, discard overlapped numbers).

## Formatting

Code is never formatted by hand: serenedb's own code goes through pre-commit
(`.clang-format`), the submodules as `.claude/rules/third-party.md` says.

Before saying done, run `pre-commit run --files <changed files>`: it also
rewrites typographic Unicode (arrows, the multiplication sign, em/en dashes,
ellipsis, curly quotes) to ASCII, rejects `/tmp` in sqllogic tests and checks
the license header. It skips `docs/` and `resources/`. As a git hook it stashes
unstaged changes, so in a checkout another session also edits, use `--files`.

## Before writing tests

Read a sibling test in the same `tests/<area>/` directory first and mirror its
style; opening it also loads the test rules from `.claude/rules/`.

## Local smoke server

Pick a free `<port>` (`ss -ltn | grep :<port>` prints nothing) and a fresh data
dir (`mktemp -d`). Note the pid the start prints: it is how you check and stop
your server, since another session can take the port between your check and the
bind. Stop it when you're done, so nobody inherits a stale serened.

```bash
# The log lands in the file you redirect to. DuckDB's LogManager writes to
# in-memory storage by default; query `SELECT * FROM duckdb_logs` (or
# `sdb_logs`) from psql.
./build/bin/serened <datadir> --listen='postgres://0.0.0.0:<port>' \
  >/tmp/$USER-serened-<port>.log 2>&1 & echo $!

# A busy port or a retired flag makes serened exit; only the log says why.
# Default database is `postgres` (serenedb has no separate default).
kill -0 <pid> && psql -h 127.0.0.1 -p <port> -U postgres -d postgres -c 'SELECT 1'

# when finished
kill <pid>
rm -rf <datadir>    # only if you don't need the datadir afterwards
```
