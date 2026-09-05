---
title: Combining Schemas
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<!-- markdownlint-disable MD036 -->

## Examples

Read a set of CSV files combining columns by position:

<SqlLogicTest id="data_import_and_export/multiple_files/combining_schemas/example_001" />

Read a set of CSV files combining columns by name:

<SqlLogicTest id="data_import_and_export/multiple_files/combining_schemas/example_002" />

## Combining Schemas

When reading from multiple files, we have to **combine schemas** from those files. That is because each file has its own schema that can differ from the other files. SereneDB offers two ways of unifying schemas of multiple files: **by column position** and **by column name**.

By default, SereneDB reads the schema of the first file provided, and then unifies columns in subsequent files by column position. This works correctly as long as all files have the same schema. If the schema of the files differs, you might want to use the `union_by_name` option to allow SereneDB to construct the schema by reading all of the names instead.

Below is an example of how both methods work.

## Union by Position

By default, SereneDB unifies the columns of these different files **by position**. This means that the first column in each file is combined together, as well as the second column in each file, etc. For example, consider the following two files.

<a href="/files/docs/flights1.csv" download>`flights1.csv`</a>:

```csv
FlightDate|UniqueCarrier|OriginCityName|DestCityName
1988-01-01|AA|New York, NY|Los Angeles, CA
1988-01-02|AA|New York, NY|Los Angeles, CA
```

<a href="/files/docs/flights2.csv" download>`flights2.csv`</a>:

```csv
FlightDate|UniqueCarrier|OriginCityName|DestCityName
1988-01-03|AA|New York, NY|Los Angeles, CA
```

Reading the two files at the same time will produce the following result set:

<SqlLogicTest id="data_import_and_export/multiple_files/combining_schemas/example_004" />

This is equivalent to the SQL construct [`UNION ALL`](../../sql/query_syntax/setops/index.md#union-all-bag-semantics).

## Union by Name

If you are processing multiple files that have different schemas, perhaps because columns have been added or renamed, it might be desirable to unify the columns of different files **by name** instead. This can be done by providing the `union_by_name` option. For example, consider the following two files, where `flights4.csv` has an extra column (`UniqueCarrier`).

<a href="/files/docs/flights3.csv" download>`flights3.csv`</a>:

```csv
FlightDate|OriginCityName|DestCityName
1988-01-01|New York, NY|Los Angeles, CA
1988-01-02|New York, NY|Los Angeles, CA
```

<a href="/files/docs/flights4.csv" download>`flights4.csv`</a>:

```csv
FlightDate|UniqueCarrier|OriginCityName|DestCityName
1988-01-03|AA|New York, NY|Los Angeles, CA
```

Reading these when unifying column names **by position** results in an error – as the two files have a different number of columns. When specifying the `union_by_name` option, the columns are correctly unified, and any missing values are set to `NULL`.

<SqlLogicTest id="data_import_and_export/multiple_files/combining_schemas/example_003" />

This is equivalent to the SQL construct [`UNION ALL BY NAME`](../../sql/query_syntax/setops/index.md#union-all-by-name).

<DocCallout type="tip">
Using the `union_by_name` option increases memory consumption.
</DocCallout>
