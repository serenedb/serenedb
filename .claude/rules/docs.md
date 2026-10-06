---
paths:
  - "docs/**"
---

# Writing docs pages

Read CONTRIBUTING.md "Documentation" first: frontmatter, `_category_.json` and runnable examples (`<SqlLogicTest>` embeds backed by sqllogic tests).

- Write only what a user can act on: what works, what an option does, requirements, errors, trade-offs. No engine internals, project history or comparisons with earlier behaviour.
- Describe SereneDB's behaviour on its own terms: don't cite other databases as design references or as the origin of a dataset. Other systems as data sources (connectors, ATTACH) are product features and belong in the docs.
- SQL keywords in UPPER CASE; multi-line formatted `SELECT`s rather than one-liners.
- One line per paragraph or list item; no hard wraps.
