---
layout: docu
redirect_from:
- /docs/guides/performance/schema
- /docs/preview/guides/performance/schema
- /docs/stable/guides/performance/schema
title: Schema
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Types

It is important to use the correct type for encoding columns (e.g., `BIGINT`, `DATE`, `TIMESTAMP`). While it is always possible to use string types (`VARCHAR`, etc.) to encode more specific values, this is not recommended. Strings use more space and are slower to process in operations such as filtering, join, and aggregation. When loading CSV files, you may leverage the CSV reader's auto-detection mechanism to determine the correct types for CSV inputs.

If you run in a memory-constrained environment, using smaller data types (e.g., `TINYINT`) can reduce the amount of memory and disk space required to complete a query. SereneDB's bitpacking compression means small values stored in larger data types will not take up larger sizes on disk, but they will take up more memory during processing – hence, using the most restrictive types possible when creating columns is necessary to reduce memory consumption.

### Using Temporal Types

Encode temporal data with a native temporal type such as `TIMESTAMP` or `DATE` rather than storing it as text. Native temporal types are stored more compactly and are substantially faster to filter, aggregate and extract parts from than the equivalent string representation, and the gap grows as tables get larger.

With a `TIMESTAMP` column, you can operate on the value directly with the [`extract` datetime function](../../sql/functions/timestamp.md):

<SqlLogicTest id="cookbook/performance/schema/example_001" />

Storing the same value as a `VARCHAR` forces slower and more error-prone string manipulation to obtain the same result:

<SqlLogicTest id="cookbook/performance/schema/example_002" />

<DocCallout type="bestPractice">

Store dates and timestamps using temporal types such as `DATE` and `TIMESTAMP` rather than `VARCHAR`.

</DocCallout>

### Joining on the Right Type

When a column holds numeric identifiers, define it with a numeric type such as `BIGINT` rather than `VARCHAR`. Joins, filters and aggregations on native numeric types are faster than the same operations on strings encoding the same values, because numeric comparisons are cheaper than string comparisons and the values are stored more compactly.

For example, a self-join on an identifier column performs best when both the join key and the referenced column are typed as `BIGINT`:

<SqlLogicTest id="cookbook/performance/schema/example_003" />

Defining those same columns as `VARCHAR` produces identical results but materially slower joins, and the difference becomes more pronounced as the data grows.

<DocCallout type="bestPractice">

Avoid representing numeric values as strings, especially if you intend to perform e.g., join operations on them.

</DocCallout>

## Constraints

SereneDB allows defining constraints such as `UNIQUE`, `PRIMARY KEY`, and `FOREIGN KEY`. These constraints can be beneficial for ensuring data integrity but they have a negative effect on load performance as they necessitate building indexes and performing checks. Moreover, they _very rarely improve the performance of queries_ as SereneDB does not rely on these indexes for join and aggregation operators.

<DocCallout type="bestPractice">

Do not define constraints unless your goal is to ensure data integrity.

</DocCallout>

### The Effect of Primary Keys

Primary key (and unique) constraints require SereneDB to build and maintain an index and to check uniqueness for every inserted row. During bulk loading this slows ingestion considerably, and the overhead grows with the size of the data.

When a primary key is required, loading the data first and adding the constraint afterwards is faster than loading into a table that already has the primary key defined: the index is then built once over the final data rather than incrementally row by row.

Primary keys (or indexes) only help highly selective queries, such as filtering on a single identifier. They do not speed up join and aggregation operators.

<DocCallout type="bestPractice">

For best bulk load performance, avoid primary key constraints. If they are required, define them after the bulk loading step.

</DocCallout>
