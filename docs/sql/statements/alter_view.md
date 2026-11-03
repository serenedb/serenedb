---
title: ALTER VIEW
split: headings
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `ALTER VIEW` statement changes the schema of an existing view in the catalog.

## Examples

Rename a view:

<SqlLogicTest id="sql/statements/alter_view/example_001" />

Move a view to another schema:

<SqlLogicTest id="sql/statements/alter_view/example_002" />

`SET SCHEMA` needs the `CREATE` privilege on the target schema, which must not already hold a relation of that name. A view that another view or a function uses can't be moved, the same as for `RENAME TO`. A name without a schema in the view's query is looked up in the view's own schema first, so a view that names a table of its old schema without the schema stops finding it once the view moves.

<DocCallout type="tip">
    `ALTER VIEW` changes the schema of an existing view.
</DocCallout>

<!--
 All the changes made by `ALTER VIEW` fully respect the transactional semantics, i.e., they will not be visible to other transactions until committed, and can be fully reverted through a rollback. Note that other views that rely on the view are **not** automatically updated.
-->
