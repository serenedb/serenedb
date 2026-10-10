---
paths:
  - "docs/**"
  - "tests/sqllogic/sdb/pg/site_docs/**"
  - "tests/sqllogic/recovery/site_docs/**"
---

# Documentation

A user-visible change lands with its documentation in the same PR. New SQL syntax, functions, settings, CLI flags, catalog objects, wire-protocol behavior -- anything a user can reach -- is undocumented until `docs/` says so, and reviewers ask for the page before approving a `feat:`. Behavior that changes gets its existing page updated in the same PR.

- **`docs/` is the source of truth**, and two consumers read it: the website, which renders it as its Docusaurus tree, and the server itself, which embeds it as the `sdb_docs` schema when built with `SDB_EMBEDDED_DOCS`.
- **Frontmatter:** every page needs `title` and `split`, where `split` is `page` (index the whole page as one unit) or `headings` (index each heading separately -- use it for long reference pages). `scripts/generate_docs.py` rejects anything else, which fails the build.
- **New folder:** add a `_category_.json` beside the pages with `label` and `position`, or the sidebar falls back to the folder name.
- **Content:** only what a user can act on -- what works, what an option does, requirements, errors, trade-offs. No engine internals, project history or comparisons with earlier behaviour.
- **SereneDB on its own terms:** don't cite other databases as design references or as the origin of a dataset. Other systems as data sources (connectors, ATTACH) are product features and belong in the docs.
- **Style:** SQL keywords in UPPER CASE, multi-line formatted `SELECT`s rather than one-liners, one line per paragraph or list item (no hard wraps).

## Documenting with runnable examples

SQL examples are backed by sqllogic tests, so an example that stops working fails CI instead of shipping.

- Put the example in a test under `tests/sqllogic/sdb/pg/site_docs/` (or `tests/sqllogic/recovery/site_docs/`) and mark its block with a `# DOCS_TEST: <name>` comment.
- Reference it from the page as `<SqlLogicTest id="<file>/<name>" />`, where `<file>` is the test's path relative to `site_docs` with the extension dropped. So `tests/sqllogic/sdb/pg/site_docs/quick-start.test` plus `# DOCS_TEST: example_003` gives `id="quick-start/example_003"`.
- Import the component once per page with `import SqlLogicTest from "@site/src/components/SqlLogicTest";`, and pass `hideResult` to render the query without its output.
- An `id` that matches no marker renders **nothing** -- no error, no warning, just a missing example. Grep for the marker after you write the tag.
