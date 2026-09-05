---
title: INSERT Statements
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`INSERT` statements are the standard way of loading data into a relational database. When using `INSERT` statements, the values are supplied row-by-row. While simple, there is significant overhead involved in parsing and processing individual `INSERT` statements. This makes lots of individual row-by-row insertions very inefficient for bulk insertion.

<DocCallout type="bestPractice">
    As a rule-of-thumb, avoid using lots of individual row-by-row `INSERT` statements when inserting more than a few rows (i.e., avoid using `INSERT` statements as part of a loop). When bulk inserting data, try to maximize the amount of data that is inserted per statement.
</DocCallout>

If you must use `INSERT` statements to load data in a loop, avoid executing the statements in auto-commit mode. After every commit, the database is required to sync the changes made to disk to ensure no data is lost. In auto-commit mode every single statement will be wrapped in a separate transaction, meaning `fsync` will be called for every statement. This is typically unnecessary when bulk loading and will significantly slow down your program.

<DocCallout type="tip">
    If you absolutely must use `INSERT` statements in a loop to load data, wrap them in calls to `BEGIN TRANSACTION` and `COMMIT`.
</DocCallout>

## Syntax

An example of using `INSERT INTO` to load data in a table is as follows:

<SqlLogicTest id="data_import_and_export/insert/example_001" />

For a more detailed description together with a syntax diagram, see the [page on the `INSERT` statement](../sql/statements/insert/index.md).
