---
title: JSON Type
sidebar_position: 5
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports `json` via the `JSON` logical type. For example:

<SqlLogicTest id="data_import_and_export/json/json_type/example_001" />

Logically, the `JSON` type is similar to a `VARCHAR`, but with the restriction that it must be valid JSON.
Physically, the data is stored as a `VARCHAR`.

For example, you can't parse invalid JSON:

<SqlLogicTest id="data_import_and_export/json/json_type/example_002" />

Instead, what you probably want here is `SELECT '"quoted"'::JSON`.

Since the data is stored physically as a `VARCHAR`, whitespace is significant:

<SqlLogicTest id="data_import_and_export/json/json_type/example_003" />

Please note that whitespaces are kept in roundtrips:

<SqlLogicTest id="data_import_and_export/json/json_type/example_004" />

The order of keys in objects is significant:

<SqlLogicTest id="data_import_and_export/json/json_type/example_005" />

Duplicate keys are allowed in JSON objects:

<SqlLogicTest id="data_import_and_export/json/json_type/example_006" />

We allow any of SereneDB's types to be cast to JSON, and JSON to be cast back to any of SereneDB's types, for example, to cast `JSON` to SereneDB's `STRUCT` type, run:

<SqlLogicTest id="data_import_and_export/json/json_type/example_007" />

And back:

<SqlLogicTest id="data_import_and_export/json/json_type/example_008" />

This works for our nested types as shown in the example, but also for non-nested types:

<SqlLogicTest id="data_import_and_export/json/json_type/example_009" />

The only exception to this behavior is the cast from `VARCHAR` to `JSON`, which does not alter the data, but instead parses and validates the contents of the `VARCHAR` as JSON.
