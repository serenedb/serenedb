---
layout: docu
redirect_from:
- /docs/preview/sql/meta/information_schema
- /docs/sql/meta/information_schema
- /docs/stable/sql/meta/information_schema
title: Information Schema
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The views in the `information_schema` are SQL-standard views that describe the catalog entries of the database. These views can be filtered to obtain information about a specific column or table. SereneDB implements the complete [PostgreSQL `information_schema`](https://www.postgresql.org/docs/18/information-schema.html); the most commonly used views are documented below, and you can refer to the PostgreSQL documentation for the full set. Some standard views (such as `sequences`, `routines` and `parameters`) are present for compatibility but are not yet populated.

The following `information_schema` views are documented here:

| View | Description |
| :--- | :---------- |
| [`character_sets`](#character_sets-character-sets) | Available character sets |
| [`columns`](#columns-columns) | Columns of all tables and views |
| [`constraint_column_usage`](#constraint_column_usage-constraint-column-usage) | Columns used by a constraint |
| [`key_column_usage`](#key_column_usage-key-column-usage) | Columns constrained as keys |
| [`referential_constraints`](#referential_constraints-referential-constraints) | Foreign-key constraints |
| [`schemata`](#schemata-database-catalog-and-schema) | Catalogs and schemas |
| [`tables`](#tables-tables-and-views) | Tables and views |
| [`table_constraints`](#table_constraints-table-constraints) | Constraints defined on tables |
| [`views`](#views-views) | Views and their definitions |

## Views

Each view below describes one facet of the catalog, and you can query it like any ordinary table: join the views together, aggregate over them or filter them in a `WHERE` clause. Most views expose a consistent set of identifier columns — `table_catalog` (the database), `table_schema` and `table_name` (or the analogous `constraint_*` columns) — so you can scope results to a specific database, schema or object.

By default these views also include SereneDB's internal objects, which live in the `information_schema` and `pg_catalog` schemas. To list only your own objects, exclude those system schemas, for example `WHERE table_schema NOT IN ('information_schema', 'pg_catalog')`. See the [Examples](#examples) below for common queries.

Because these views follow the SQL standard, the same queries run unchanged against PostgreSQL, which makes them a portable choice for database tools, ORMs and migration scripts. Where you need SereneDB-specific metadata that the standard does not cover — such as internal object details or storage information — use the native [Metadata Functions](functions/duckdb_table_functions.md) instead.

Following the SQL standard, the views use a small set of domain types (`sql_identifier`, `character_data`, `cardinal_number` and `yes_or_no`) rather than SereneDB's native types. In particular, the boolean-style columns (for example `is_nullable` and `is_updatable`) return the strings `'YES'` and `'NO'` rather than `BOOLEAN` values, so compare against `'YES'`/`'NO'` when filtering on them.

To discover every view that is available in your version, query `information_schema.tables` itself:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'information_schema'
ORDER BY table_name;
```

### `character_sets`: Character Sets

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| `character_set_catalog` | Currently not implemented – always `NULL`. | `VARCHAR` | `NULL` |
| `character_set_schema` | Currently not implemented – always `NULL`. | `VARCHAR` | `NULL` |
| `character_set_name` | Name of the character set, currently implemented as showing the name of the database encoding. | `VARCHAR` | `'UTF8'` |
| `character_repertoire` | Character repertoire, showing `UCS` if the encoding is `UTF8`, else just the encoding name. | `VARCHAR` | `'UCS'` |
| `form_of_use` | Character encoding form, same as the database encoding. | `VARCHAR` | `'UTF8'` |
| `default_collate_catalog`| Name of the database containing the default collation (always the current database). | `VARCHAR` | `'my_db'` |
| `default_collate_schema` | Name of the schema containing the default collation. | `VARCHAR` | `'pg_catalog'` |
| `default_collate_name` | Name of the default collation. | `VARCHAR` | `'ucs_basic'` |

### `columns`: Columns

The view that describes the catalog information for columns is `information_schema.columns`. It lists the columns present in the database and has the following layout:

| Column | Description | Type | Example |
|:--|:---|:-|:-|
| `table_catalog` | Name of the database containing the table (always the current database). | `VARCHAR` | `'my_db'` |
| `table_schema` | Name of the schema containing the table. | `VARCHAR` | `'main'` |
| `table_name` | Name of the table. | `VARCHAR` | `'widgets'` |
| `column_name` | Name of the column. | `VARCHAR` | `'price'` |
| `ordinal_position` | Ordinal position of the column within the table (count starts at 1). | `INTEGER` | `5` |
| `column_default` | Default expression of the column. |`VARCHAR`| `1.99` |
| `is_nullable` | `YES` if the column is possibly nullable, `NO` if it is known not nullable. |`VARCHAR`| `'YES'` |
| `data_type` | Data type of the column. |`VARCHAR`| `'DECIMAL(18, 2)'` |
| `character_maximum_length` | If `data_type` identifies a character or bit string type, the declared maximum length; `NULL` for all other data types or if no maximum length was declared. |`INTEGER`| `255` |
| `character_octet_length` | If `data_type` identifies a character type, the maximum possible length in octets (bytes) of a datum; `NULL` for all other data types. The maximum octet length depends on the declared character maximum length (see above) and the character encoding. |`INTEGER`| `1073741824` |
| `numeric_precision` | If `data_type` identifies a numeric type, this column contains the (declared or implicit) precision of the type for this column. The precision indicates the number of significant digits. For all other data types, this column is `NULL`. |`INTEGER`| `18` |
| `numeric_scale` | If `data_type` identifies a numeric type, this column contains the (declared or implicit) scale of the type for this column. The scale indicates the number of digits after the decimal point. For all other data types, this column is `NULL`. |`INTEGER`| `2` |
| `datetime_precision` | If `data_type` identifies a date, time, timestamp, or interval type, this column contains the (declared or implicit) fractional seconds precision of the type for this column, that is, the number of decimal digits maintained following the decimal point in the seconds value. No fractional seconds are currently supported in SereneDB. For all other data types, this column is `NULL`. |`INTEGER`| `0` |

### `constraint_column_usage`: Constraint Column Usage

This view describes all columns in the current database that are used by some constraint. For a check constraint, this view identifies the columns that are used in the check expression. For a not-null constraint, this view identifies the column that the constraint is defined on. For a foreign key constraint, this view identifies the columns that the foreign key references. For a unique or primary key constraint, this view identifies the constrained columns.

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| `table_catalog` | Name of the database that contains the table that contains the column that is used by some constraint (always the current database) |`VARCHAR`| `'my_db'` |
| `table_schema` | Name of the schema that contains the table that contains the column that is used by some constraint |`VARCHAR`| `'main'` |
| `table_name` | Name of the table that contains the column that is used by some constraint |`VARCHAR`| `'widgets'` |
| `column_name` | Name of the column that is used by some constraint |`VARCHAR`| `'price'` |
| `constraint_catalog` | Name of the database that contains the constraint (always the current database) |`VARCHAR`| `'my_db'` |
| `constraint_schema` | Name of the schema that contains the constraint |`VARCHAR`| `'main'` |
| `constraint_name` | Name of the constraint |`VARCHAR`| `'exam_id_students_id_fkey'` |

### `key_column_usage`: Key Column Usage

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| `constraint_catalog` | Name of the database that contains the constraint (always the current database). | `VARCHAR` | `'my_db'` |
| `constraint_schema` | Name of the schema that contains the constraint. | `VARCHAR` | `'main'` |
| `constraint_name` | Name of the constraint. | `VARCHAR` | `'exams_exam_id_fkey'` |
| `table_catalog` | Name of the database that contains the table that contains the column that is restricted by this constraint (always the current database). | `VARCHAR` | `'my_db'` |
| `table_schema` | Name of the schema that contains the table that contains the column that is restricted by this constraint. | `VARCHAR` | `'main'` |
| `table_name` | Name of the table that contains the column that is restricted by this constraint. | `VARCHAR` | `'exams'` |
| `column_name` | Name of the column that is restricted by this constraint. | `VARCHAR` | `'exam_id'` |
| `ordinal_position` | Ordinal position of the column within the constraint key (count starts at 1). | `INTEGER` | `1` |
| `position_in_unique_constraint` | For a foreign-key constraint, ordinal position of the referenced column within its unique constraint (count starts at `1`); otherwise `NULL`. | `INTEGER` | `1` |

### `referential_constraints`: Referential Constraints

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| `constraint_catalog` | Name of the database containing the constraint (always the current database). | `VARCHAR` | `'my_db'` |
| `constraint_schema` | Name of the schema containing the constraint. | `VARCHAR` | `main` |
| `constraint_name` | Name of the constraint. | `VARCHAR` | `exam_id_students_id_fkey` |
| `unique_constraint_catalog` | Name of the database that contains the unique or primary key constraint that the foreign key constraint references. | `VARCHAR` | `'my_db'` |
| `unique_constraint_schema` | Name of the schema that contains the unique or primary key constraint that the foreign key constraint references. | `VARCHAR` | `'main'` |
| `unique_constraint_name` | Name of the unique or primary key constraint that the foreign key constraint references. | `VARCHAR` | `'students_id_pkey'` |
| `match_option` | Match option of the foreign key constraint. Always `NONE`. | `VARCHAR` | `NONE` |
| `update_rule` | Update rule of the foreign key constraint. Always `NO ACTION`. | `VARCHAR` | `NO ACTION` |
| `delete_rule` | Delete rule of the foreign key constraint. Always `NO ACTION`. | `VARCHAR` | `NO ACTION` |

### `schemata`: Database, Catalog and Schema

The top level catalog view is `information_schema.schemata`. It lists the catalogs and the schemas present in the database and has the following layout:

| Column | Description | Type | Example |
|:--|:---|:-|:-|
| `catalog_name` | Name of the database that the schema is contained in. | `VARCHAR` | `'my_db'` |
| `schema_name` | Name of the schema. | `VARCHAR` | `'main'` |
| `schema_owner` | Name of the owner of the schema. Not yet implemented. | `VARCHAR` | `'serenedb'` |
| `default_character_set_catalog` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |
| `default_character_set_schema` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |
| `default_character_set_name` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |
| `sql_path` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |

### `tables`: Tables and Views

The view that describes the catalog information for tables and views is `information_schema.tables`. It lists the tables present in the database and has the following layout:

| Column | Description | Type | Example |
|:--|:---|:-|:-|
| `table_catalog` | The catalog the table or view belongs to. | `VARCHAR` | `'my_db'` |
| `table_schema` | The schema the table or view belongs to. | `VARCHAR` | `'main'` |
| `table_name` | The name of the table or view. | `VARCHAR` | `'widgets'` |
| `table_type` | The type of table. One of: `BASE TABLE`, `LOCAL TEMPORARY`, `VIEW`. | `VARCHAR` | `'BASE TABLE'` |
| `self_referencing_column_name` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |
| `reference_generation` | Applies to a feature not available in SereneDB. | `VARCHAR` | `NULL` |
| `user_defined_type_catalog` | If the table is a typed table, the name of the database that contains the underlying data type (always the current database), else `NULL`. Currently unimplemented. | `VARCHAR` | `NULL` |
| `user_defined_type_schema` | If the table is a typed table, the name of the schema that contains the underlying data type, else `NULL`. Currently unimplemented. | `VARCHAR` | `NULL` |
| `user_defined_type_name` | If the table is a typed table, the name of the underlying data type, else `NULL`. Currently unimplemented. | `VARCHAR` | `NULL` |
| `is_insertable_into` | `YES` if the table is insertable into, `NO` if not (Base tables are always insertable into, views not necessarily.)| `VARCHAR` | `'YES'` |
| `is_typed` | `YES` if the table is a typed table, `NO` if not. | `VARCHAR` | `'NO'` |
| `commit_action` | Not yet implemented. | `VARCHAR` | `'NO'` |

### `table_constraints`: Table Constraints

| Column | Description | Type | Example |
|--------|-------------|------|---------|
| `constraint_catalog` | Name of the database that contains the constraint (always the current database). | `VARCHAR` | `'my_db'` |
| `constraint_schema` | Name of the schema that contains the constraint. | `VARCHAR` | `'main'` |
| `constraint_name` | Name of the constraint. | `VARCHAR` | `'exams_exam_id_fkey'` |
| `table_catalog` | Name of the database that contains the table (always the current database). | `VARCHAR` | `'my_db'` |
| `table_schema` | Name of the schema that contains the table. | `VARCHAR` | `'main'` |
| `table_name` | Name of the table. | `VARCHAR` | `'exams'` |
| `constraint_type` | Type of the constraint: `CHECK`, `FOREIGN KEY`, `PRIMARY KEY`, or `UNIQUE`. | `VARCHAR` | `'FOREIGN KEY'` |
| `is_deferrable` | `YES` if the constraint is deferrable, `NO` if not. | `VARCHAR` | `'NO'` |
| `initially_deferred` | `YES` if the constraint is deferrable and initially deferred, `NO` if not. | `VARCHAR` | `'NO'` |
| `enforced` | Always `YES`. | `VARCHAR` | `'YES'` |
| `nulls_distinct` | If the constraint is a unique constraint, then `YES` if the constraint treats `NULL`s as distinct or `NO` if it treats `NULL`s as not distinct, otherwise `NULL` for other types of constraints. | `VARCHAR` | `'YES'` |

### `views`: Views

The view that describes the views present in the database is `information_schema.views`. It has the following layout:

| Column | Description | Type | Example |
|:--|:---|:-|:-|
| `table_catalog` | Name of the database that contains the view (always the current database). | `VARCHAR` | `'my_db'` |
| `table_schema` | Name of the schema that contains the view. | `VARCHAR` | `'public'` |
| `table_name` | Name of the view. | `VARCHAR` | `'active_users'` |
| `view_definition` | The `SELECT` statement that defines the view. | `VARCHAR` | `'SELECT id, name FROM users;'` |
| `check_option` | Applies to a feature not available in SereneDB. Always `NONE`. | `VARCHAR` | `'NONE'` |
| `is_updatable` | `YES` if the view is updatable, `NO` if not. | `VARCHAR` | `'NO'` |
| `is_insertable_into` | `YES` if the view is insertable into, `NO` if not. | `VARCHAR` | `'NO'` |
| `is_trigger_updatable` | Applies to a feature not available in SereneDB. Always `NO`. | `VARCHAR` | `'NO'` |
| `is_trigger_deletable` | Applies to a feature not available in SereneDB. Always `NO`. | `VARCHAR` | `'NO'` |
| `is_trigger_insertable_into` | Applies to a feature not available in SereneDB. Always `NO`. | `VARCHAR` | `'NO'` |

## Examples

List the columns of a table, in order:

<SqlLogicTest id="sql/information_schema/example_001" />

List the tables and views in the current schema:

<SqlLogicTest id="sql/information_schema/example_002" />

Inspect the constraints defined on a table:

<SqlLogicTest id="sql/information_schema/example_003" />

Retrieve the definition of a view:

<SqlLogicTest id="sql/information_schema/example_004" />

## See Also

SereneDB also provides native introspection through the [Metadata Functions](functions/duckdb_table_functions.md) and the [Catalog Functions](functions/catalog.md). These often expose additional SereneDB-specific details and internal objects that the SQL-standard `information_schema` views omit.
