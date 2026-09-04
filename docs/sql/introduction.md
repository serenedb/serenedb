---
title: SQL Introduction
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

This page provides an overview of how to perform simple operations in SQL.
This tutorial is only intended to give you an introduction and is in no way a complete tutorial on SQL.
This tutorial is adapted from the [PostgreSQL tutorial](https://www.postgresql.org/docs/current/tutorial-sql-intro.html).

<DocCallout type="pin">
SereneDB's SQL dialect closely follows the conventions of the PostgreSQL dialect.
The few exceptions to this are listed on the [PostgreSQL compatibility page](../compatibility/core-sql-compatibility.md).
</DocCallout>

In the examples that follow, we assume that you have installed the SereneDB. See the [installation page](../installation/index.md) for information on our database.

## Concepts

SereneDB is an open-source database for real-time search analytics. Rather than being a traditional relational database management system, it combines search-engine capabilities, analytical query processing and a PostgreSQL-compatible SQL interface.

In SereneDB, data is organized into tables with named columns and rows, similar to relational databases. This makes it possible to query data using familiar SQL concepts while also supporting search-oriented workloads such as full-text search, vector search, hybrid search, filtering, aggregations and analytics over the same data.

## Creating a New Table

You can create a new table by specifying the table name, along with all column names and their types:

<SqlLogicTest id="sql/introduction/example_001" />

You can enter this into the shell with the line breaks. The command is not terminated until the semicolon.

<DocCallout type="tip">

White space (i.e., spaces, tabs and newlines) can be used freely in SQL commands. That means you can type the command aligned differently than above, or even all on one line. Two dash characters (`--`) introduce comments. Whatever follows them is ignored up to the end of the line. SQL is case-insensitive about keywords and identifiers. When returning identifiers, [their original cases are preserved](../compatibility/keywords_and_identifiers.md#rules-for-case-sensitivity).

</DocCallout>

In the SQL command, we first specify the type of command that we want to perform: `CREATE TABLE`. After that follows the parameters for the command. First, the table name, `weather`, is given. Then the column names and column types follow.

`city VARCHAR` specifies that the table has a column called `city` that is of type `VARCHAR`. `VARCHAR` specifies a data type that can store text of arbitrary length. The temperature fields are stored in an `INTEGER` type, a type that stores integer numbers (i.e., whole numbers without a decimal point). `FLOAT` columns store single precision floating-point numbers (i.e., numbers with a decimal point). `DATE` stores a date (i.e., year, month, day combination). `DATE` only stores the specific day, not a time associated with that day.

SereneDB supports the standard SQL types `INTEGER`, `SMALLINT`, `FLOAT`, `DOUBLE`, `DECIMAL`, `CHAR(n)`, `VARCHAR(n)`, `DATE`, `TIME` and `TIMESTAMP`.

The second example will store cities and their associated geographical location:

<SqlLogicTest id="sql/introduction/example_002" />

Finally, it should be mentioned that if you don't need a table any longer or want to recreate it differently you can remove it using the following command:

<SqlLogicTest id="sql/introduction/example_003" />

## Populating a Table with Rows

The insert statement is used to populate a table with rows:

<SqlLogicTest id="sql/introduction/example_004" />

<DocCallout type="tip">

Constants that are not numeric values (e.g., text and dates) must be surrounded by single quotes (`''`), as in the example. Input dates for the date type must be formatted as `'YYYY-MM-DD'`.

</DocCallout>

We can insert into the `cities` table in the same manner.

<SqlLogicTest id="sql/introduction/example_005" />

The syntax used so far requires you to remember the order of the columns. An alternative syntax allows you to list the columns explicitly:

<SqlLogicTest id="sql/introduction/example_006" />

You can list the columns in a different order if you wish or even omit some columns, e.g., if the `prcp` is unknown:

<SqlLogicTest id="sql/introduction/example_007" />

<DocCallout type="tip">
Many developers consider explicitly listing the columns better style than relying on the order implicitly.
</DocCallout>

Please enter all the commands shown above so you have some data to work with in the following sections.

Alternatively, you can use the `COPY` statement. This is faster for large amounts of data because the `COPY` command is optimized for bulk loading while allowing less flexibility than `INSERT`. An example with <a href="/files/docs/weather.csv" download>weather.csv</a> would be:

<SqlLogicTest id="sql/introduction/example_008" hideResult />

Where the file name for the source file must be available on the machine running the process. There are many other ways of loading data into SereneDB, see the [corresponding documentation section](../data_import_and_export/overview.md) for more information.

## Querying a Table

To retrieve data from a table, the table is queried. A SQL `SELECT` statement is used to do this. The statement is divided into a select list (the part that lists the columns to be returned), a table list (the part that lists the tables from which to retrieve the data) and an optional qualification (the part that specifies any restrictions). For example, to retrieve all the rows of table weather, type:

<SqlLogicTest id="sql/introduction/example_009" />

Here `*` is a shorthand for “all columns”. So the same result would be had with:

<SqlLogicTest id="sql/introduction/example_010" />

You can write expressions, not just simple column references, in the select list. For example, you can do:

<SqlLogicTest id="sql/introduction/example_011" />

Notice how the `AS` clause is used to relabel the output column. (The `AS` clause is optional.)

A query can be “qualified” by adding a `WHERE` clause that specifies which rows are wanted. The `WHERE` clause contains a Boolean (truth value) expression and only rows for which the Boolean expression is true are returned. The usual Boolean operators (`AND`, `OR` and `NOT`) are allowed in the qualification. For example, the following retrieves the weather of San Francisco on rainy days:

<SqlLogicTest id="sql/introduction/example_012" />

You can request that the results of a query be returned in sorted order:

<SqlLogicTest id="sql/introduction/example_013" />

In this example, the sort order isn't fully specified and so you might get the San Francisco rows in either order. But you'd always get the results shown above if you do:

<SqlLogicTest id="sql/introduction/example_014" />

You can request that duplicate rows be removed from the result of a query:

<SqlLogicTest id="sql/introduction/example_015" />

Here again, the result row ordering might vary. You can ensure consistent results by using `DISTINCT` and `ORDER BY` together:

<SqlLogicTest id="sql/introduction/example_016" />

## Joins between Tables

Thus far, our queries have only accessed one table at a time. Queries can access multiple tables at once, or access the same table in such a way that multiple rows of the table are being processed at the same time. A query that accesses multiple rows of the same or different tables at one time is called a join query. As an example, say you wish to list all the weather records together with the location of the associated city. To do that, we need to compare the city column of each row of the `weather` table with the name column of all rows in the `cities` table and select the pairs of rows where these values match.

This would be accomplished by the following query:

<SqlLogicTest id="sql/introduction/example_017" />

Observe two things about the result set:

-   There is no result row for the city of Hayward. This is because there is no matching entry in the `cities` table for Hayward, so the join ignores the unmatched rows in the `weather` table. We will see shortly how this can be fixed.
-   There are two columns containing the city name. This is correct because the lists of columns from the `weather` and `cities` tables are concatenated. In practice this is undesirable, though, so you will probably want to list the output columns explicitly rather than using `*`:

<SqlLogicTest id="sql/introduction/example_018" />

Since the columns all had different names, the parser automatically found which table they belong to. If there were duplicate column names in the two tables you'd need to qualify the column names to show which one you meant, as in:

<SqlLogicTest id="sql/introduction/example_019" />

It is widely considered good style to qualify all column names in a join query, so that the query won't fail if a duplicate column name is later added to one of the tables.

Join queries of the kind seen thus far can also be written in this alternative form:

<SqlLogicTest id="sql/introduction/example_020" />

This syntax is not as commonly used as the one above, but we show it here to help you understand the following topics.

Now we will figure out how we can get the Hayward records back in. What we want the query to do is to scan the `weather` table and for each row to find the matching cities row(s). If no matching row is found we want some “empty values” to be substituted for the `cities` table's columns. This kind of query is called an outer join. (The joins we have seen so far are inner joins.) The command looks like this:

<SqlLogicTest id="sql/introduction/example_021" />

This query is called a left outer join because the table mentioned on the left of the join operator will have each of its rows in the output at least once, whereas the table on the right will only have those rows output that match some row of the left table. When outputting a left-table row for which there is no right-table match, empty (null) values are substituted for the right-table columns.

## Aggregate Functions

Like most other relational database products, SereneDB supports aggregate functions. An aggregate function computes a single result from multiple input rows. For example, there are aggregates to compute the `count`, `sum`, `avg` (average), `max` (maximum) and `min` (minimum) over a set of rows.

As an example, we can find the highest low-temperature reading anywhere with:

<SqlLogicTest id="sql/introduction/example_022" />

If we wanted to know what city (or cities) that reading occurred in, we might try:

<SqlLogicTest id="sql/introduction/example_023" />

But this will not work since the aggregate max cannot be used in the `WHERE` clause.

This restriction exists because the `WHERE` clause determines which rows will be included in the aggregate calculation; so obviously it has to be evaluated before aggregate functions are computed.
However, as is often the case the query can be restated to accomplish the desired result, here by using a subquery:

<SqlLogicTest id="sql/introduction/example_024" />

This is OK because the subquery is an independent computation that computes its own aggregate separately from what is happening in the outer query.

Aggregates are also very useful in combination with `GROUP BY` clauses. For example, we can get the maximum low temperature observed in each city with:

<SqlLogicTest id="sql/introduction/example_025" />

Which gives us one output row per city. Each aggregate result is computed over the table rows matching that city. We can filter these grouped rows using `HAVING`:

<SqlLogicTest id="sql/introduction/example_026" />

which gives us the same results for only the cities that have all `temp_lo` values below 40. Finally, if we only care about cities whose names begin with `S`, we can use the `LIKE` operator:

<SqlLogicTest id="sql/introduction/example_027" hideResult/>

More information about the `LIKE` operator can be found in the [pattern matching page](./functions/pattern_matching/).

It is important to understand the interaction between aggregates and SQL's `WHERE` and `HAVING` clauses. The fundamental difference between `WHERE` and `HAVING` is this: `WHERE` selects input rows before groups and aggregates are computed (thus, it controls which rows go into the aggregate computation), whereas `HAVING` selects group rows after groups and aggregates are computed. Thus, the `WHERE` clause must not contain aggregate functions; it makes no sense to try to use an aggregate to determine which rows will be inputs to the aggregates. On the other hand, the `HAVING` clause always contains aggregate functions.

In the previous example, we can apply the city name restriction in `WHERE`, since it needs no aggregate. This is more efficient than adding the restriction to `HAVING`, because we avoid doing the grouping and aggregate calculations for all rows that fail the `WHERE` check.

## Updates

You can update existing rows using the `UPDATE` command. Suppose you discover the temperature readings are all off by 2 degrees after November 28. You can correct the data as follows:

<SqlLogicTest id="sql/introduction/example_028" />

Look at the new state of the data:

<SqlLogicTest id="sql/introduction/example_029" />

## Deletions

Rows can be removed from a table using the `DELETE` command. Suppose you are no longer interested in the weather of Hayward. Then you can do the following to delete those rows from the table:

<SqlLogicTest id="sql/introduction/example_030" />

All weather records belonging to Hayward are removed.

<SqlLogicTest id="sql/introduction/example_031" />

One should be cautious when issuing statements of the following form:

<SqlLogicTest id="sql/introduction/example_032" />

<DocCallout type="attention">
Without a qualification, `DELETE` will remove all rows from the given table, leaving it empty. The system will not request confirmation before doing this.
</DocCallout>
