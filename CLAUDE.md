# Notes for AI agents

Build and launch: `CONTRIBUTING.md`. Topic rules: `.claude/rules/`, each page loads with the files it covers; `pull-requests.md` loads always.

## Working with the user

- Be straightforward and laconic, in replies and in everything you write: short, but complete.
- A requested plan or design ends the turn with the plan and a question; implement it only after an explicit yes.
- An explicit ask is never parked as a follow-up.
- A clear improvement found on the way (a bug, a speedup, duplicated code, a cleaner structure) is just done. What goes to the user is a decision: a trade-off, new scope (a dependency, a subsystem, compatibility code), a workaround instead of the real fix.
- Merges, CI runs and pushes to base branches happen only when the user asks for them ([pull-requests.md](.claude/rules/pull-requests.md)).

## Evidence

- Code is the evidence: PR descriptions, comments, docs and memory are claims. Read the implementation and its callers before relying on them.
- An estimate is said to be one; only a measurement is presented as a result.
- A bug gets a reproducer first, preferably end to end (sqllogic, recovery, driver): a unit test can drive an API in ways no caller does. It fails for the reason the fix claims, then passes.
- Understand the mechanism before fixing it, and fix the root cause in the design: a quick fix that silences the symptom (catch-all error skipping, a guard, a new lock around a race) only hides the bug.
- A bug is a pattern: look for the same mistake in the other callers, siblings and files, and fix every instance or file the ones outside the change.
- A failure found while working gets fixed, whoever caused it, never set aside as pre-existing. Ask instead when an issue or PR already covers it, or when the fix needs a decision.
- Before filing an issue or writing a fix, search issues and PRs, closed ones too (`gh issue list --state all --search ...`, `gh pr list --state all --search ...`): add to an existing issue, link an existing fix.

## Running things

- Build and run only what the task needs: the touched targets and the tests that cover the change. Full suites, sanitizer builds and benchmarks only when asked.
- Run the smallest thing that answers the question, once, with the full output in a log, and answer from the log. A noisy or flaky result is a question to answer, not a reason to rerun or run longer.
- Wrap a test run in a timeout of 2-3 times its usual duration, so a hang ends itself.
- `pre-commit run --files <changed files>` before saying done, as `SKIP=check-submodule-pointers pre-commit run --files ...` while a fork PR is unmerged (the gitlink check fails by design until then). Not as a git hook: it stashes unstaged changes, which breaks a checkout someone else is editing.

## Text

- No code comments unless asked; the license block and `# DOCS_TEST:` markers are not comments. Remove a comment your change makes outdated.
- No text describes the implementation: comments, rules, docs and memory hold motivation, rules, desired behaviour and processes. How the code works is read from the code.
- Use the tools you have for what they are for: read and edit files with the read and edit tools, never through python, sed or cat; scripts for real scripting.
- Knowledge every contributor needs goes into the repo, into the page under `.claude/rules/` it belongs to; instructions only for Claude go here. One rule lives in one place: point to it, never copy it.

## Local smoke server

Start serened on a free port (`ss -ltnH "sport = :<port>"` prints nothing) with a fresh data dir (`mktemp -d`); stop it by its pid and remove the data dir when done.

```bash
./build/bin/serened <datadir> --listen='postgres://127.0.0.1:<port>' \
  >/tmp/$USER-serened-<port>.log 2>&1 & echo $!
kill -0 <pid> && psql -h 127.0.0.1 -p <port> -U postgres -d postgres -c '<sql>'
kill <pid>
rm -rf <datadir>
```

psql is refused until the server is up: retry while `kill -0 <pid>` succeeds; once it fails, the log says why serened exited.
