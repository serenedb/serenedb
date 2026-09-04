---
title: CREATE INDEX
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## `CREATE INDEX`

The `CREATE INDEX` statement constructs an index on the specified column(s) of the specified table.

### Examples

Create a unique index `films_id_idx` on the column id of table `films`:

<SqlLogicTest id="sql/statements/create_index/index/example_001" />

Create index `s_idx` that allows for duplicate values on column `revenue` of table `films`:

<SqlLogicTest id="sql/statements/create_index/index/example_002" />

Create index if it does not yet exist:

<SqlLogicTest id="sql/statements/create_index/index/example_003" />

<DocCallout type="tip">
The `CREATE INDEX IF NOT EXISTS` statement does not have an “early exit” at the moment, instead, it will attempt to create the index and only check its existence before committing it to storage.
Therefore, it may run for a longer time compared to other `IF NOT EXISTS` statements, which terminate early.
</DocCallout>

Create compound index `gy_idx` on `genre` and `year` columns:

<SqlLogicTest id="sql/statements/create_index/index/example_004" />

Create an index on an expression — e.g., the sum of columns `j` and `k` from table `integers`:

<SqlLogicTest id="sql/statements/create_index/index/example_005" />

[Inverted indexes](./inverted.md) support indexed expressions too — see [CREATE INDEX … USING inverted](./inverted.md).

### Parameters

| Name         | Description                                                                                                                                                                                                                                      |
| :----------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UNIQUE`     | Causes the system to check for duplicate values in the table when the index is created (if data already exist) and each time data is added. Attempts to insert or update data that would result in duplicate entries will generate an error.     |
| `name`       | The name of the index to be created.                                                                                                                                                                                                             |
| `table`      | The name of the table to be indexed.                                                                                                                                                                                                             |
| `column`     | The name of the column to be indexed.                                                                                                                                                                                                            |
| `expression` | An expression over one or more columns of the table (see the example above). [Inverted indexes](./inverted.md) also support indexed expressions. |
| `index type` | Specified index type, see [Indexes](../../indexes/index.md). Optional.                                                                                                                                                                           |
| `option`     | Index option in the form of a Boolean true value (e.g., `is_cool`) or a key-value pair (e.g., `my_option = 2`). Optional.                                                                                                                        |

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

## Inverted Indexes

Adding `USING inverted` creates an [inverted index](../../indexes/inverted/index.md) for full-text, [vector](../../indexes/inverted/vector-search.md) and [geospatial](../../indexes/inverted/geospatial-search.md) search. It has its own grammar — column dictionaries, feature flags, `INCLUDE` columns and index options. See [CREATE INDEX … USING inverted](./inverted.md) for the full syntax reference.

## `DROP INDEX`

`DROP INDEX` drops an existing index from the database system.

### Examples

Remove the index `title_idx`:

<SqlLogicTest id="sql/statements/create_index/index/example_006" />

### Parameters

| Name        | Description                                        |
| :---------- | :------------------------------------------------- |
| `IF EXISTS` | Do not throw an error if the index does not exist. |
| `name`      | The name of an index to remove.                    |

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

## Limitations

The `CREATE INDEX` clause does not support the `OR REPLACE` modifier.
