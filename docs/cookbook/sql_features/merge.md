---
layout: docu
redirect_from:
- /docs/guides/sql_features/merge
- /docs/preview/guides/sql_features/merge
- /docs/stable/guides/sql_features/merge
title: Merge Statement for SCD Type 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

This is a practical, step-by-step guide to using SereneDB’s `MERGE` statement to perform upserts and build [Slowly Changing Dimension Type 2 (SCD Type 2) tables](https://en.wikipedia.org/wiki/Slowly_changing_dimension). Type 2 SCDs let you keep full historical versions of records while clearly identifying the current version, perfect for audit trails, data warehousing, and analytical workloads. Type 2 SCDs are practical when you want to know previous values of your primary key data, when it changed and for how long it was in a particular state.

## Why Use MERGE in SereneDB?

- Single SQL statement for `INSERT`, `UPDATE`, and soft `DELETE` (upsert and expire).
- Much cleaner and faster than equivalent Python/Pandas logic.
- Full history tracking without hard deletes.
- Works directly on Parquet, CSV, databases, thanks to SereneDB's connectivity!

## Prerequisites

* Basic SQL knowledge

## Key Terminology

| Term                          | Meaning                                                                                   |
|-------------------------------|-------------------------------------------------------------------------------------------|
| **Target table**              | The main/master table you are updating (e.g., `master_employees`)                             |
| **Source table**              | The incoming/new data (e.g., `incoming_employees`)                                            |
| **MERGE INTO**                | Specifies the target table                                                                |
| **USING**                     | Specifies the source table/query                                                          |
| **ON**                        | Join condition (usually primary/business key + current flag)                             |
| **WHEN MATCHED**              | Row exists in both → typically UPDATE (or DELETE)                                         |
| **WHEN NOT MATCHED BY TARGET**| New row (insert)                                                                          |
| **WHEN NOT MATCHED BY SOURCE**| Row disappeared → soft-delete/expire old version                                          |
| **RETURNING merge_action**    | Optional: shows what happened to each row (INSERT/UPDATE/DELETE)                          |

## Build an SCD Type 2 Dimension Table

We’ll track employees and preserve history whenever their name, department, or office changes.

### Step 1: Create the Incoming (source) Table

This table represents today’s transactional data.

<SqlLogicTest id="cookbook/sql_features/merge/example_001" />

### Step 2: Create the Master (target) Table

This table represents the type 2 SCD data (i.e., transaction data with history).

<SqlLogicTest id="cookbook/sql_features/merge/example_002" />

### Step 3: Perform the Merge Statement

This statement will perform the merge, it will check for differences between the data of target and source and follow the `WHEN MATCHED` or `WHEN NOT MATCHED` logic specified.

<SqlLogicTest id="cookbook/sql_features/merge/example_003" />

### Step 4: Insert New Current Versions for Changed Records

This statement inserts the new current records into the master table. While it's possible to achieve the same result using the `MERGE` statement's `RETURNING` clause, this two-step approach is more straightforward and easier to understand.

<SqlLogicTest id="cookbook/sql_features/merge/example_004" />

### Step 5: Query The Results

The following queries can be used to examine the data resulting from the `MERGE` statement.

<SqlLogicTest id="cookbook/sql_features/merge/example_005" />

### Step 6: Examine a Single Employee

To better illustrate the concept, let's examine a single employee, to drive home the value add for type 2 SCDs.
If we select from the master table after running the merge statement and the post update insert statement, we can see the individual rows for `Alice`.

To view the original row of data that is historical: 

<SqlLogicTest id="cookbook/sql_features/merge/example_006" />

**Note**: 

- The `end date` is NOT NULL, it has the date when this employee's data was updated.
- The `is_current` is `false` indicating this is a historical record.
- The field that will change is `office`, it is currently `Office A` and will be updated to `Office B`.

To view the current row of data:

<SqlLogicTest id="cookbook/sql_features/merge/example_007" />

**Note**: 

- The `end date` is NULL, the NULL in this context indicates this is the latest record for this `employee_id`.
- The `is_current` is `true` also indicating this is a current record.
- The `office` is now `Office B`.

To view all of `Alice` data, which will contain both current and non-current rows:

<SqlLogicTest id="cookbook/sql_features/merge/example_008" />

## Common Patterns and Variations

| Use Case                          | Clause to Use                                                      |
|-----------------------------------|--------------------------------------------------------------------|
| Simple upsert (no history)        | `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED BY TARGET THEN INSERT` |
| Upsert and delete missing rows      | Add `WHEN NOT MATCHED BY SOURCE THEN DELETE`                       |
| Only insert new, never update     | Omit `WHEN MATCHED`                                                |
| Return affected rows              | Add `RETURNING merge_action, *`                                    |

## Best Practices

- Remember that `TARGET` is the master table and `SOURCE` is the incoming table or query.
- Keep end_date NULL for current rows (makes queries faster).
- Wrap `MERGE` and `INSERT` statements in a transaction when needed.
- Use a primary key or a surrogate key for uniqueness.
- Test with RETURNING first.
