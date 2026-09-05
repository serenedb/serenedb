---
title: ALTER VIEW
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `ALTER VIEW` statement changes the schema of an existing view in the catalog.

## Examples

Rename a view:

<SqlLogicTest id="sql/statements/alter_view/example_001" />

<DocCallout type="tip">
    `ALTER VIEW` changes the schema of an existing view.
</DocCallout>

<!--
 All the changes made by `ALTER VIEW` fully respect the transactional semantics, i.e., they will not be visible to other transactions until committed, and can be fully reverted through a rollback. Note that other views that rely on the view are **not** automatically updated.
-->
