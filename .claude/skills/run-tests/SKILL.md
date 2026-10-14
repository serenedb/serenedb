---
name: run-tests
description: Run SereneDB tests locally - pick the suites that cover a change and run them the way CI does - and, when the user asks, reproduce a red CI job. Use when running tests, choosing which tests to run, or looking into a CI failure.
---

# Running SereneDB tests

Read `.claude/rules/testing.md` first, and `.claude/rules/ci.md` for a CI failure.

- Build the targets you run first. Never pipe `ninja` into `tail` or `grep`: the pipe hides its exit code.
- Report what ran: the files or test names, the `[OK]`/`[FAILED]` counts, and the log of every failure.
