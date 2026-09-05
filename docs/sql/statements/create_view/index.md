---
title: CREATE VIEW
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE VIEW` statement defines a new view in the catalog.

## Examples

Create a simple view:

<SqlLogicTest id="sql/statements/create_view/index/example_001" />

Create a view or replace it if a view with that name already exists:

<SqlLogicTest id="sql/statements/create_view/index/example_002" />

Create a view and replace the column names:

<SqlLogicTest id="sql/statements/create_view/index/example_003" />

The SQL query behind an existing view can be read from the PostgreSQL-compatible [`pg_views`](https://www.postgresql.org/docs/current/view-pg-views.html) catalog:

<SqlLogicTest id="sql/statements/create_view/index/example_004" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

`CREATE VIEW` defines a view of a query. The view is not physically materialized. Instead, the query is run every time the view is referenced in a query.

`CREATE OR REPLACE VIEW` is similar, but if a view of the same name already exists, it is replaced.

If a schema name is given then the view is created in the specified schema. Otherwise, it is created in the current schema. Temporary views exist in a special schema, so a schema name cannot be given when creating a temporary view. The name of the view must be distinct from the name of any other view or table in the same schema.
