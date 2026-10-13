---
paths:
  - "docs/**"
  - "tests/sqllogic/sdb/pg/site_docs/**"
  - "tests/sqllogic/recovery/site_docs/**"
---

# Documentation

A user-visible change lands with its documentation in the same PR. New SQL syntax, functions, settings, CLI flags, catalog objects, wire-protocol behavior -- anything a user can reach -- is undocumented until `docs/` says so, and reviewers ask for the page before approving a `feat:`. Behavior that changes gets its existing page updated in the same PR.

- **`docs/` is the source of truth**, and two consumers read it: the website, which renders it as its Docusaurus tree, and the server itself, which embeds it as the `sdb_docs` schema when built with `SDB_EMBEDDED_DOCS`.
- **Frontmatter:** every page needs `split`: `page` (index the whole page as one unit) or `headings` (index each heading separately -- use it for long reference pages). `title` defaults to the file name. `scripts/generate_docs.py` fails the build on any other `split`, on a `slug` key, and on two sections with the same heading path in a `headings` page.
- **New folder:** add a `_category_.json` beside the pages with `label` and `position`, or the sidebar falls back to the folder name.
- **Content:** only what a user can observe or rely on -- what works, what an option does, requirements, errors, trade-offs. "What stays out" below lists what doesn't belong.
- **SereneDB on its own terms:** don't cite other databases as design references or as the origin of a dataset. Other systems as data sources (connectors, ATTACH) are product features and belong in the docs.
- **Style:** SQL keywords in UPPER CASE, multi-line formatted `SELECT`s rather than one-liners, one line per paragraph or list item (no hard wraps).

## What stays out

A page is user documentation, not a design note: its reader runs SereneDB and never reads its code. Describe what they type and what they get back -- syntax, options, defaults, results, errors, limits -- and leave out:

- **How it works inside:** data structures, algorithms, buffers, caches, threads, scans and code paths ("Internally, we subtract the block header size", "`@@` is the single entry point that turns a `TSQUERY` into an inverted-index scan"). Keep only the effect a user sees: which queries get faster or slower, what costs more memory or disk, what a setting trades.
- **Names from the code:** source files, classes, functions, fields, constants and internal components (`Write()`, `kCapacity`, the binder). A name belongs on a page only when the user types it or sees it: SQL, settings, function names, error messages, output columns.
- **The change's history:** "now", "no longer", "previously", "new", "after the fix", PR and issue numbers. A page says how things are.
- **Engineering notes:** how a feature was tested, what CI checks, test file paths, "the worked example below verifies ...".
- **Plans and guesses:** "currently", "not yet", "planned", "in a future release". State a limitation as it is today: "`X` is not supported".

Check every sentence: could a user observe it or rely on it, without the source? If not, delete it.

Two kinds of pages are exempt in part: `docs/releases/` describes what changed between releases, and a benchmark page may explain why a result is what it is.

## Documenting with runnable examples

SQL examples are backed by sqllogic tests, so an example that stops working fails CI instead of shipping.

- Put the example in a test under `tests/sqllogic/sdb/pg/site_docs/` (or `tests/sqllogic/recovery/site_docs/`) and mark its block with a `# DOCS_TEST: <name>` comment.
- Reference it from the page as `<SqlLogicTest id="<file>/<name>" />`, where `<file>` is the test's path relative to `site_docs` with the extension dropped. So `tests/sqllogic/sdb/pg/site_docs/quick-start.test` plus `# DOCS_TEST: example_003` gives `id="quick-start/example_003"`.
- Import the component once per page with `import SqlLogicTest from "@site/src/components/SqlLogicTest";`, and pass `hideResult` to render the query without its output.
- An `id` that matches no marker renders **nothing** on the site; the build only prints a warning. Grep for the marker after you write the tag.
