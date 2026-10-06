---
name: pr-description
description: Write or update a SereneDB pull request title and description that becomes the squash commit - conventional prefix, why over what, linked docs and tests. Use when creating a PR, or when asked to write or fix a PR title or description.
argument-hint: "[PR-number | branch]"
---

# PR title and description

PRs are squash-merged: the title becomes the commit subject on `main` and the description becomes its body (`.github/PULL_REQUEST_TEMPLATE.md`).

## Title

`<prefix>: <what changed>`, with one of the prefixes from CONTRIBUTING.md: `feat:`, `fix:`, `perf:` (most common), or `refactor:`, `chore:`, `docs:`, `test:`, `ci:`, `build:`, `style:`, `misc:`. Don't invent prefixes; ask if none fits.

- Name the exact thing: `fix: jobs and text search dictionaries in nested schemas`, `perf: n-gram prefilter for case-insensitive ASCII letters in ts_regexp`.
- Not vague (`fix: bugs`, `improvements`), not a sentence about the PR (`This PR adds ...`).

## Body

Explain why, not what; the diff shows what.

- The problem and who hits it; for a fix, the failing behaviour and the root cause.
- The approach, and the alternative it beat when that's not obvious.
- Evidence: the tests that cover it (paths), and for `perf:` the measured end-to-end numbers against main with the build and machine load.
- User-visible changes: the `docs/` page added or updated.
- Fork changes: the fork commit by SHA, or its PR as plain text (see below).
- Plain paragraphs, one line per paragraph (no hard wraps), lists where they help.

## Never

- Another repository's issue or PR as `#N`, `owner/repo#N` or a URL: GitHub links all three back from the target. Write it as plain text (`serenedb/duckdb PR 89`) or inside backticks.
- Links to code on a branch: pin them to a commit SHA.

## Apply

Show the title and body to the user first. On approval, for an existing PR:

```bash
gh api -X PATCH repos/serenedb/serenedb/pulls/<N> -f title='<title>' -F body=@body.md --jq '.title'
```

`gh pr edit` can fail on GitHub's retired projects-classic API and leave the PR unchanged; the REST call above doesn't. Check with `gh pr view <N> --json title,body`.
