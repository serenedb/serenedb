---
name: run-tests
description: Run SereneDB tests locally and read a red CI run - pick the suites that cover a change, run them the way CI does, and find the failing records in CI logs. Use when running tests, choosing which tests to run, or looking into a CI failure.
---

# Running SereneDB tests

Read `.claude/rules/testing.md` (every suite's local command and its traps, which suites CI picks for a change, and how to read a failed run) before running anything.

- Build the targets you run first. Never pipe `ninja` into `tail` or `grep`: the pipe hides its exit code.
- A server you start for `run.sh` follows CLAUDE.md "Local smoke server".
- Report what ran: the test files or names, the `[OK]`/`[FAILED]` counts, and the log of every failure.
