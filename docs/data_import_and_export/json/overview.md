---
title: Overview
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB supports SQL functions that are useful for reading values from existing JSON and creating new JSON data.

## About JSON

JSON is an open standard file format and data interchange format that uses human-readable text to store and transmit data objects consisting of attribute–value pairs and arrays (or other serializable values).
While it is not a very efficient format for tabular data, it is very commonly used, especially as a data interchange format.

## JSONPath and JSON Pointer Syntax

SereneDB implements multiple interfaces for JSON extraction: [JSONPath](https://goessner.net/articles/JsonPath/) and [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901). Both of them work with the arrow operator (`->`) and the `json_extract` function call.

Note that SereneDB only supports lookups in JSONPath, i.e., extracting fields with `.<key>` or array elements with `[<index>]`.
Arrays can be indexed from the back and both approaches support the wildcard `*`.
SereneDB does _not_ support the full JSONPath syntax because SQL is readily available for any further transformations.

<DocCallout type="tip">

It's best to pick either the JSONPath or the JSON Pointer syntax and use it in your entire application.

</DocCallout>

## Indexing

<DocCallout type="attention">

Following [PostgreSQL's conventions](../../compatibility/core-sql-compatibility.md), SereneDB uses 1-based indexing for its [`ARRAY`](../../sql/data_types/array.md) and [`LIST`](../../sql/data_types/list.md) data types but [0-based indexing for the JSON data type](https://www.postgresql.org/docs/17/functions-json.html#FUNCTIONS-JSON-PROCESSING).

</DocCallout>

## Examples

### Loading JSON

Read a JSON file from disk, auto-infer options:

<SqlLogicTest id="data_import_and_export/json/overview/example_001" />

Use the `read_json` function with custom options:

<SqlLogicTest id="data_import_and_export/json/overview/example_002" />

Read a JSON file from stdin, auto-infer options:

```batch
cat data/json/todos.json | serened shell -c "SELECT * FROM read_json('/dev/stdin')"
```

Read a JSON file into a table:

<SqlLogicTest id="data_import_and_export/json/overview/example_003" />

Alternatively, create a table without specifying the schema manually with a [`CREATE TABLE ... AS SELECT` clause](../../sql/statements/create_table/index.md#create-table--as-select-ctas):

<SqlLogicTest id="data_import_and_export/json/overview/example_004" />

### Writing JSON

Write the result of a query to a JSON file:

<SqlLogicTest id="data_import_and_export/json/overview/example_006" />

### JSON Data Type

Create a table with a column for storing JSON data and insert data into it:

<SqlLogicTest id="data_import_and_export/json/overview/example_007" />

### Retrieving JSON Data

Retrieve the family key's value as `JSON` with the subscript operator:

<SqlLogicTest id="data_import_and_export/json/overview/example_008" />

Extract the family key's value as `JSON` with the `->` operator:

<SqlLogicTest id="data_import_and_export/json/overview/example_009" />

Extract the family key's value as a `VARCHAR` with the `->>` operator:

<SqlLogicTest id="data_import_and_export/json/overview/example_010" />

### Keys with Special Characters

JSON object keys that contain special characters such as `[` and `.` can be accessed by passing the key name to the `->` operator:

<SqlLogicTest id="data_import_and_export/json/overview/example_011" />
