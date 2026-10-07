---
name: pr-description
description: Write or update a SereneDB pull request title and description that becomes the squash commit. Use when creating a PR, or when asked to write or fix a PR title or description.
argument-hint: "[PR-number | branch]"
---

# PR title and description

Write them by CONTRIBUTING.md "Branching, commits, PRs" (prefixes, title, description, references to other repositories); the template is `.github/PULL_REQUEST_TEMPLATE.md`.

Show the title and body to the user first. On approval, for an existing PR:

```bash
gh api -X PATCH repos/serenedb/serenedb/pulls/<N> -f title='<title>' -F body=@body.md --jq '.title'
```

`gh pr edit` can fail on GitHub's retired projects-classic API and leave the PR unchanged; the REST call above doesn't. Check with `gh pr view <N> --json title,body`.
