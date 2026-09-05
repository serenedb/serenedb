---
title: SQL to / from JSON
sidebar_position: 9
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB provides functions to serialize and deserialize `SELECT` statements between SQL and JSON, as well as executing JSON serialized statements.

| Function                                                                                                               | Type   | Description                                                                                                            |
| :--------------------------------------------------------------------------------------------------------------------- | :----- | :--------------------------------------------------------------------------------------------------------------------- |
| `json_deserialize_sql(json)`                                                                                           | Scalar | Deserialize one or many `json` serialized statements back to an equivalent SQL string.                                 |
| `json_execute_serialized_sql(varchar)`                                                                                 | Table  | Execute `json` serialized statements and return the resulting rows. Only one statement at a time is supported for now. |
| `json_serialize_sql(varchar, skip_default := boolean, skip_empty := boolean, skip_null := boolean, format := boolean)` | Scalar | Serialize a set of semicolon-separated (`;`) select statements to an equivalent list of `json` serialized statements.  |
| `PRAGMA json_execute_serialized_sql(varchar)`                                                                          | Pragma | Pragma version of the `json_execute_serialized_sql` function.                                                          |

The `json_serialize_sql(varchar)` function takes four optional parameters, `skip_default`, `skip_empty`, `skip_null` and `format` that can be used to control the output of the serialized statements.

If you run the `json_execute_serialized_sql(varchar)` table function inside of a transaction the serialized statements will not be able to see any transaction local changes. This is because the statements are executed in a separate query context. You can use the `PRAGMA json_execute_serialized_sql(varchar)` pragma version to execute the statements in the same query context as the pragma, although with the limitation that the serialized JSON must be provided as a constant string, i.e., you cannot do `PRAGMA json_execute_serialized_sql(json_serialize_sql(...))`.

Note that these functions do not preserve syntactic sugar such as `FROM * SELECT ...`, so a statement round-tripped through `json_deserialize_sql(json_serialize_sql(...))` may not be identical to the original statement, but should always be semantically equivalent and produce the same output.

## Examples

Simple example:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_001" />

Example with multiple statements and skip options:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_002" />

Skip the default values in the AST (e.g., `"distinct":false`):

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_003" />

Example with a syntax error:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_004" />

Example with deserialize:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_005" />

Example with deserialize and syntax sugar, which is lost during the transformation:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_006" />

Example with execute:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_007" />

Example with error:

<SqlLogicTest id="data_import_and_export/json/sql_to_and_from_json/example_008" />
