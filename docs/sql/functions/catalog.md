---
title: Catalog Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Several functions are provided to inspect the catalogs and schemas that are configured in the database.

The following catalog functions are available:

| Function | Description |
| :------- | :---------- |
| [`current_catalog()`](#current_catalog) | Returns the name of the currently active catalog. |
| [`current_schema()`](#current_schema) | Returns the name of the currently active schema. |
| [`current_schemas(include_implicit)`](#current_schemasinclude_implicit) | Returns the list of schemas in the search path. Pass `true` to include implicit schemas. |

#### `current_catalog()`

Returns the name of the currently active catalog, which is the database the session is connected to.

<SqlLogicTest id="sql/functions/catalog/example_004" />

#### `current_schema()`

Returns the name of the currently active schema.

<SqlLogicTest id="sql/functions/catalog/example_001" />

#### `current_schemas(include_implicit)`

Returns the list of schemas in the search path. Pass `true` to include implicit schemas such as `pg_catalog`.

<SqlLogicTest id="sql/functions/catalog/example_002" />

<SqlLogicTest id="sql/functions/catalog/example_003" />
