---
name: pr-description
description: Write or update a SereneDB pull request title and description that becomes the squash commit - conventional prefix, why over what, linked docs and tests. Use when creating a PR, or when asked to write or fix a PR title or description.
argument-hint: "[PR-number | branch]"
---

# PR title and description

Read CONTRIBUTING.md "Branching, commits, PRs" first: the title prefixes, squash-merge, and how to refer to other repositories. The template is `.github/PULL_REQUEST_TEMPLATE.md`.

## Title

`<prefix>: <what changed>`.

- Name the exact thing: `fix: jobs and text search dictionaries in nested schemas`, `perf: n-gram prefilter for case-insensitive ASCII letters in ts_regexp`.
- Not vague (`fix: bugs`, `improvements`), not a sentence about the PR (`This PR adds ...`).

## Body

Explain why, not what; the diff shows what.

- The problem and who hits it; for a fix, the failing behaviour and the root cause.
- The approach, and the alternative it beat when that's not obvious.
- Evidence: the tests that cover it (paths), and for `perf:` the measured end-to-end numbers against main with the build and machine load.
- User-visible changes: the `docs/` page added or updated.
- Fork changes: the fork commit by SHA, or its PR written as CONTRIBUTING.md says.
- Plain paragraphs, one line per paragraph (no hard wraps), lists where they help.

## Apply

Show the title and body to the user first. On approval, for an existing PR:

```bash
gh api -X PATCH repos/serenedb/serenedb/pulls/<N> -f title='<title>' -F body=@body.md --jq '.title'
```

`gh pr edit` can fail on GitHub's retired projects-classic API and leave the PR unchanged; the REST call above doesn't. Check with `gh pr view <N> --json title,body`.
