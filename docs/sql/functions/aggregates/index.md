---
title: Aggregate Functions
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

## Examples

Produce a single row containing the sum of the `amount` column:

<SqlLogicTest id="sql/functions/aggregates/index/example_001" />

Produce one row per unique region, containing the sum of `amount` for each group:

<SqlLogicTest id="sql/functions/aggregates/index/example_002" />

Return only the regions that have a sum of `amount` higher than 100:

<SqlLogicTest id="sql/functions/aggregates/index/example_003" />

Return the number of unique values in the `region` column:

<SqlLogicTest id="sql/functions/aggregates/index/example_004" />

Return two values, the total sum of `amount` and the sum of `amount` minus columns where the region is `north` using the [`FILTER` clause](../../../sql/query_syntax/filter/index.md):

<SqlLogicTest id="sql/functions/aggregates/index/example_005" />

Returns a list of all regions in order of the `amount` column:

<SqlLogicTest id="sql/functions/aggregates/index/example_006" />

Returns the amount of the first sale using the `first()` aggregate function:

<SqlLogicTest id="sql/functions/aggregates/index/example_007" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

Aggregates are functions that _combine_ multiple rows into a single value. Aggregates are different from scalar functions and window functions because they change the cardinality of the result. As such, aggregates can only be used in the `SELECT` and `HAVING` clauses of a SQL query.

### `DISTINCT` Clause in Aggregate Functions

When the `DISTINCT` clause is provided, only distinct values are considered in the computation of the aggregate. This is typically used in combination with the `count` aggregate to get the number of distinct elements; but it can be used together with any aggregate function in the system.
There are some aggregates that are insensitive to duplicate values (e.g., `min` and `max`) and for them this clause is parsed and ignored.

### `ORDER BY` Clause in Aggregate Functions

An `ORDER BY` clause can be provided after the last argument of the function call. Note the lack of the comma separator before the clause.

<SqlLogicTest id="sql/functions/aggregates/index/example_008" />

This clause ensures that the values being aggregated are sorted before applying the function.
Most aggregate functions are order-insensitive, and for them this clause is parsed and discarded.
However, there are some order-sensitive aggregates that can have non-deterministic results without ordering, e.g., `first`, `last`, `list` and `string_agg` / `group_concat` / `listagg`.
These can be made deterministic by ordering the arguments.

For example:

<SqlLogicTest id="sql/functions/aggregates/order_sensitive_string_agg/example_009" />

### Handling `NULL` Values

All general aggregate functions ignore `NULL`s, except for [`list`](#listarg) ([`array_agg`](#listarg)), [`first`](#firstarg) ([`arbitrary`](#firstarg)) and [`last`](#lastarg).
To exclude `NULL`s from `list`, you can use a [`FILTER` clause](../../../sql/query_syntax/filter/index.md).
To ignore `NULL`s from `first`, you can use the [`any_value` aggregate](#any_valuearg).

All general aggregate functions except [`count`](#countarg) return `NULL` on empty groups.
In particular, [`list`](#listarg) does _not_ return an empty list, [`sum`](#sumarg) does _not_ return zero, and [`string_agg`](#string_aggarg-sep) does _not_ return an empty string in this case.

## General Aggregate Functions

The examples in this section run against the following table:

<SqlLogicTest id="sql/functions/aggregates/index/setup" />

The table below shows the available general aggregate functions.

| Function                                                                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| :--------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`any_value(arg)`](#any_valuearg)                                                              | Returns the first non-null value from `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `arbitrary(arg)`                                                                              | Returns the first value — `NULL` or not — from `arg`. Cheaper than [`any_value`](#any_valuearg) when any row will do and `NULL`s are acceptable. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). |
| [`arg_max(arg, val)`](#arg_maxarg-val)                                                         | Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                      |
| [`arg_max(arg, val, n)`](#arg_maxarg-val-n)                                                    | The generalized case of [`arg_max`](#arg_maxarg-val) for `n` values: returns a `LIST` containing the `arg` expressions for the top `n` rows ordered by `val` descending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                |
| [`arg_max_null(arg, val)`](#arg_max_nullarg-val)                                               | Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                  |
| `arg_max_nulls_last(arg, val)`                                                                 | Like [`arg_max_null`](#arg_max_nullarg-val), but rows whose `val` expression is `NULL` are also kept, and are ordered after every row with a value. Takes an optional third argument `n` for the top `n` rows. |
| [`arg_min(arg, val)`](#arg_minarg-val)                                                         | Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                      |
| [`arg_min(arg, val, n)`](#arg_minarg-val-n)                                                    | Returns a `LIST` containing the `arg` expressions for the "bottom" `n` rows ordered by `val` ascending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                 |
| [`arg_min_null(arg, val)`](#arg_min_nullarg-val)                                               | Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                  |
| `arg_min_nulls_last(arg, val)`                                                                 | Like [`arg_min_null`](#arg_min_nullarg-val), but rows whose `val` expression is `NULL` are also kept, and are ordered after every row with a value. Takes an optional third argument `n` for the bottom `n` rows. |
| [`avg(arg)`](#avgarg)                                                                          | Calculates the average of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`bit_and(arg)`](#bit_andarg)                                                                  | Returns the bitwise AND of all bits in a given expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`bit_or(arg)`](#bit_orarg)                                                                    | Returns the bitwise OR of all bits in a given expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`bit_xor(arg)`](#bit_xorarg)                                                                  | Returns the bitwise XOR of all bits in a given expression.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`bitstring_agg(arg)`](#bitstring_aggarg)                                                      | Returns a bitstring whose length corresponds to the range of the non-null (integer) values, with bits set at the location of each (distinct) value.                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`bool_and(arg)`](#bool_andarg)                                                                | Returns `true` if every input value is `true`, otherwise `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`bool_or(arg)`](#bool_orarg)                                                                  | Returns `true` if any input value is `true`, otherwise `false`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [`count()`](#count)                                                                            | Returns the number of rows.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`count(arg)`](#countarg)                                                                      | Returns the number of rows where `arg` is not `NULL`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [`countif(arg)`](#countifarg)                                                                  | Returns the number of rows where `arg` is `true`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `decimal_average(arg)`                                                                         | Averages a `DECIMAL` column with exact integer arithmetic and banker's rounding, returning a `DECIMAL` of the same width and scale as the input. Use it instead of [`avg`](#avgarg) when the result must stay exact. |
| `every(arg)`                                                                                   | Returns `TRUE` when every input value is `TRUE`. The SQL-standard spelling of [`bool_and`](#bool_andarg). |
| [`favg(arg)`](#favgarg)                                                                        | Calculates the average using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                      |
| [`first(arg)`](#firstarg)                                                                      | Returns the first value (null or non-null) from `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [`fsum(arg)`](#fsumarg)                                                                        | Calculates the sum using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                          |
| [`geometric_mean(arg)`](#geometric_meanarg)                                                    | Calculates the geometric mean of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`histogram(arg)`](#histogramarg)                                                              | Returns a `MAP` of key-value pairs representing buckets and counts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`histogram(arg, boundaries)`](#histogramarg-boundaries)                                       | Returns a `MAP` of key-value pairs representing the provided upper `boundaries` and counts of elements in the corresponding bins (left-open and right-closed partitions) of the datatype. A boundary at the largest value of the datatype is automatically added when elements larger than all provided `boundaries` appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg). Boundaries may be provided, e.g., via [`equi_width_bins`](../../../sql/functions/utility.md#equi_width_binsmin-max-bincount-nice--false). |
| [`histogram_exact(arg, elements)`](#histogram_exactarg-elements)                               | Returns a `MAP` of key-value pairs representing the requested elements and their counts. A catch-all element specific to the data-type is automatically added to count other elements when they appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg).                                                                                                                                                                                                                                                      |
| [`histogram_values(source, boundaries)`](#histogram_valuessource-col_name-technique-bin_count) | Returns the upper boundaries of the bins and their counts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `kahan_sum(arg)`                                                                               | Sums a floating-point column with Kahan compensated summation, which keeps far more precision than [`sum`](#sumarg) when the values differ widely in magnitude. |
| [`last(arg)`](#lastarg)                                                                        | Returns the last value of a column. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [`list(arg)`](#listarg)                                                                        | Returns a `LIST` containing all the values of a column. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`max(arg)`](#maxarg)                                                                          | Returns the maximum value present in `arg`. This function is [unaffected by distinctness](#distinct-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`max(arg, n)`](#maxarg-n)                                                                     | Returns a `LIST` containing the `arg` values for the "top" `n` rows ordered by `arg` descending.                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`min(arg)`](#minarg)                                                                          | Returns the minimum value present in `arg`. This function is [unaffected by distinctness](#distinct-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`min(arg, n)`](#minarg-n)                                                                     | Returns a `LIST` containing the `arg` values for the "bottom" `n` rows ordered by `arg` ascending.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| [`product(arg)`](#productarg)                                                                  | Calculates the product of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`string_agg(arg)`](#string_aggarg-sep)                                                        | Concatenates the column string values with a comma separator (`,`). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                     |
| [`string_agg(arg, sep)`](#string_aggarg-sep)                                                   | Concatenates the column string values with a separator. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| [`sum(arg)`](#sumarg)                                                                          | Calculates the sum of all non-null values in `arg` / counts `true` values when `arg` is boolean. The floating-point versions of this function are [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                        |
| [`weighted_avg(arg, weight)`](#weighted_avgarg-weight)                                         | Calculates the weighted average of all non-null values in `arg`, where each value is scaled by its corresponding `weight`. If `weight` is `NULL`, the corresponding `arg` value will be skipped. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                        |

#### `any_value(arg)`

Returns the first non-`NULL` value from `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/any_value" />

#### `arg_max(arg, val)`

Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `argmax(arg, val)`, `max_by(arg, val)`.

<SqlLogicTest id="sql/functions/aggregates/index/arg_max" />

#### `arg_max(arg, val, n)`

The generalized case of [`arg_max`](#arg_maxarg-val) for `n` values: returns a `LIST` containing the `arg` expressions for the top `n` rows ordered by `val` descending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `argmax(arg, val, n)`, `max_by(arg, val, n)`.

<SqlLogicTest id="sql/functions/aggregates/index/arg_max_n" />

#### `arg_max_null(arg, val)`

Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/arg_max_null" />

#### `arg_min(arg, val)`

Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `argmin(arg, val)`, `min_by(arg, val)`.

<SqlLogicTest id="sql/functions/aggregates/index/arg_min" />

#### `arg_min(arg, val, n)`

The generalized case of [`arg_min`](#arg_minarg-val) for `n` values: returns a `LIST` containing the `arg` expressions for the bottom `n` rows ordered by `val` ascending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `argmin(arg, val, n)`, `min_by(arg, val, n)`.

<SqlLogicTest id="sql/functions/aggregates/index/arg_min_n" />

#### `arg_min_null(arg, val)`

Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/arg_min_null" />

#### `avg(arg)`

Calculates the average of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Alias: `mean`.

<SqlLogicTest id="sql/functions/aggregates/index/avg" />

#### `bit_and(arg)`

Returns the bitwise `AND` of all bits in a given expression.

<SqlLogicTest id="sql/functions/aggregates/index/bit_and" />

#### `bit_or(arg)`

Returns the bitwise `OR` of all bits in a given expression.

<SqlLogicTest id="sql/functions/aggregates/index/bit_or" />

#### `bit_xor(arg)`

Returns the bitwise `XOR` of all bits in a given expression.

<SqlLogicTest id="sql/functions/aggregates/index/bit_xor" />

#### `bitstring_agg(arg)`

Returns a bitstring whose length corresponds to the range of the non-null (integer) values, with bits set at the location of each (distinct) value.

<SqlLogicTest id="sql/functions/aggregates/index/bitstring_agg" />

#### `bool_and(arg)`

Returns `true` if every input value is `true`, otherwise `false`.

<SqlLogicTest id="sql/functions/aggregates/index/bool_and" />

#### `bool_or(arg)`

Returns `true` if any input value is `true`, otherwise `false`.

<SqlLogicTest id="sql/functions/aggregates/index/bool_or" />

#### `count()`

Returns the number of rows. Alias: `count(*)`.

<SqlLogicTest id="sql/functions/aggregates/index/count_star" />

#### `count(arg)`

Returns the number of rows where `arg` is not `NULL`.

<SqlLogicTest id="sql/functions/aggregates/index/count" />

#### `countif(arg)`

Returns the number of rows where `arg` is `true`.

<SqlLogicTest id="sql/functions/aggregates/index/countif" />

#### `favg(arg)`

Calculates the average using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/favg" />

#### `first(arg)`

Returns the first value (null or non-null) from `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Alias: `arbitrary(A)`.

<SqlLogicTest id="sql/functions/aggregates/index/first" />

#### `fsum(arg)`

Calculates the sum using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `sumkahan`, `kahan_sum`.

<SqlLogicTest id="sql/functions/aggregates/index/fsum" />

#### `geometric_mean(arg)`

Calculates the geometric mean of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Alias: `geomean(A)`.

<SqlLogicTest id="sql/functions/aggregates/index/geometric_mean" />

#### `histogram(arg)`

Returns a `MAP` of key-value pairs representing buckets and counts.

<SqlLogicTest id="sql/functions/aggregates/index/histogram" />

#### `histogram(arg, boundaries)`

Returns a `MAP` of key-value pairs representing the provided upper `boundaries` and counts of elements in the corresponding bins (left-open and right-closed partitions) of the datatype. A boundary at the largest value of the datatype is automatically added when elements larger than all provided `boundaries` appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg). Boundaries may be provided, e.g., via [`equi_width_bins`](../../../sql/functions/utility.md#equi_width_binsmin-max-bincount-nice--false).

<SqlLogicTest id="sql/functions/aggregates/index/histogram_boundaries" />

#### `histogram_exact(arg, elements)`

Returns a `MAP` of key-value pairs representing the requested elements and their counts. A catch-all element specific to the data-type is automatically added to count other elements when they appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg).

<SqlLogicTest id="sql/functions/aggregates/index/histogram_exact" />

#### `histogram_values(source, col_name, technique, bin_count)`

<div className="docs-table-properties">

| **Description** | Returns the upper boundaries of the bins and their counts. |
| :--- | :--- |
| **Example** | `histogram_values(integers, i, bin_count := 2)` |

</div>

Note: this function is not yet supported in SereneDB.

#### `last(arg)`

Returns the last value of a column. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/last" />

#### `list(arg)`

Returns a `LIST` containing all the values of a column. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Alias: `array_agg`.

<SqlLogicTest id="sql/functions/aggregates/index/list" />

#### `max(arg)`

Returns the maximum value present in `arg`. This function is [unaffected by distinctness](#distinct-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/max" />

#### `max(arg, n)`

Returns a `LIST` containing the `arg` values for the "top" `n` rows ordered by `arg` descending.

<SqlLogicTest id="sql/functions/aggregates/index/max_n" />

#### `min(arg)`

Returns the minimum value present in `arg`. This function is [unaffected by distinctness](#distinct-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/min" />

#### `min(arg, n)`

Returns a `LIST` containing the `arg` values for the "bottom" `n` rows ordered by `arg` ascending.

<SqlLogicTest id="sql/functions/aggregates/index/min_n" />

#### `product(arg)`

Calculates the product of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/product" />

#### `string_agg(arg)`

Concatenates the column string values with a comma separator (`,`). This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `group_concat(arg)`, `listagg(arg)`.

<SqlLogicTest id="sql/functions/aggregates/index/string_agg" />

#### `string_agg(arg, sep)`

Concatenates the column string values with a separator. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Aliases: `group_concat(arg, sep)`, `listagg(arg, sep)`.

<SqlLogicTest id="sql/functions/aggregates/index/string_agg_sep" />

#### `sum(arg)`

Calculates the sum of all non-null values in `arg` / counts `true` values when `arg` is boolean. The floating-point versions of this function are [affected by ordering](#order-by-clause-in-aggregate-functions).

<SqlLogicTest id="sql/functions/aggregates/index/sum" />

#### `weighted_avg(arg, weight)`

Calculates the weighted average of all non-null values in `arg`, where each value is scaled by its corresponding `weight`. If `weight` is `NULL`, the value will be skipped. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). Alias: `wavg(arg, weight)`.

<SqlLogicTest id="sql/functions/aggregates/index/weighted_avg" />

## Approximate Aggregates

The table below shows the available approximate aggregate functions.

| Function                                              | Description                                                                                                                | Example                            |
| :---------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------- | :--------------------------------- |
| `approx_count_distinct(x)`                            | Calculates the approximate count of distinct elements using HyperLogLog.                                                   | `approx_count_distinct(A)`         |
| `approx_quantile(x, pos)`                             | Calculates the approximate quantile using T-Digest.                                                                        | `approx_quantile(A, 0.5)`          |
| `approx_top_k(arg, k)`                                | Calculates a `LIST` of the `k` approximately most frequent values of `arg` using Filtered Space-Saving.                    |                                    |
| `reservoir_quantile(x, quantile, sample_size = 8192)` | Calculates the approximate quantile using reservoir sampling, the sample size is optional and uses 8192 as a default size. | `reservoir_quantile(A, 0.5, 1024)` |

## Statistical Aggregates

The table below shows the available statistical aggregate functions.
They all ignore `NULL` values (in the case of a single input column `x`), or pairs where either input is `NULL` (in the case of two input columns `y` and `x`).

| Function                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| :--------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`corr(y, x)`](#corry-x)                       | The correlation coefficient.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`covar_pop(y, x)`](#covar_popy-x)             | The population covariance, which does not include bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`covar_samp(y, x)`](#covar_sampy-x)           | The sample covariance, which includes Bessel's bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`entropy(x)`](#entropyx)                      | The log-2 entropy of count input-values.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [`kurtosis_pop(x)`](#kurtosis_popx)            | The excess kurtosis (Fisher’s definition) without bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`kurtosis(x)`](#kurtosisx)                    | The excess kurtosis (Fisher's definition) with bias correction according to the sample size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`mad(x)`](#madx)                              | The median absolute deviation. Temporal types return a positive `INTERVAL`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [`median(x)`](#medianx)                        | The middle value of the set. For even value counts, quantitative values are averaged and ordinal values return the lower value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`mode(x)`](#modex)                            | The most frequent value. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `quantile(x, pos)`                             | The exact `pos`-quantile of `x` for `0 <= pos <= 1`, an alias of [`quantile_disc`](#quantile_discx-pos). If `pos` is a `LIST` of `FLOAT`s the result is a `LIST` of the corresponding quantiles. For an approximate answer over large inputs see [`approx_quantile`](#approximate-aggregates). |
| [`quantile_cont(x, pos)`](#quantile_contx-pos) | The interpolated `pos`-quantile of `x` for `-1 <= pos <= 1`. Returns the `pos * (n_nonnull_values - 1)`th (zero-indexed, in the specified order) value of `x` or an interpolation between the adjacent values if the index is not an integer. Values of `pos` between `-1` and `0` correspond to counting backwards from `1`. More precisely, `quantile_cont(x, -y) = quantile_cont(x, 1 - y)`. Intuitively, arranges the values of `x` as equispaced _points_ on a line, starting at 0 and ending at 1, and returns the (interpolated) value at `pos`. This is Type 7 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding interpolated quantiles. |
| [`quantile_disc(x, pos)`](#quantile_discx-pos) | The discrete `pos`-quantile of `x` for `0 <= pos <= 1`. Returns the `greatest(ceil(pos * n_nonnull_values) - 1, 0)`th (zero-indexed, in the specified order) value of `x`. Intuitively, assigns to each value of `x` an equisized _sub-interval_ (left-open and right-closed except for the initial interval) of the interval `[0, 1]`, and picks the value of the sub-interval that contains `pos`. This is Type 1 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding discrete quantiles.                                                                                                                                                        |
| [`regr_avgx(y, x)`](#regr_avgxy-x)             | The average of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`regr_avgy(y, x)`](#regr_avgyy-x)             | The average of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [`regr_count(y, x)`](#regr_county-x)           | The number of non-`NULL` pairs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`regr_intercept(y, x)`](#regr_intercepty-x)   | The intercept of the univariate linear regression line, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [`regr_r2(y, x)`](#regr_r2y-x)                 | The squared Pearson correlation coefficient between y and x. Also: The coefficient of determination in a linear regression, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [`regr_slope(y, x)`](#regr_slopey-x)           | The slope of the linear regression line, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [`regr_sxx(y, x)`](#regr_sxxy-x)               | The sample variance, which includes Bessel's bias correction, of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`regr_sxy(y, x)`](#regr_sxyy-x)               | The sample covariance, which includes Bessel's bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`regr_syy(y, x)`](#regr_syyy-x)               | The sample variance, which includes Bessel's bias correction, of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`skewness(x)`](#skewnessx)                    | The skewness.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [`sem(x)`](#semx)                              | The standard error of the mean.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [`stddev_pop(x)`](#stddev_popx)                | The population standard deviation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| [`stddev_samp(x)`](#stddev_sampx)              | The sample standard deviation.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [`var_pop(x)`](#var_popx)                      | The population variance, which does not include bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| [`var_samp(x)`](#var_sampx)                    | The sample variance, which includes Bessel's bias correction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

#### `corr(y, x)`

<div className="docs-table-properties">

| **Description** | The correlation coefficient. |
| :--- | :--- |
| **Formula** | `covar_pop(y, x) / (stddev_pop(x) * stddev_pop(y))` |

</div>

#### `covar_pop(y, x)`

<div className="docs-table-properties">

| **Description** | The population covariance, which does not include bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / regr_count(y, x)`, `covar_samp(y, x) * (1 - 1 / regr_count(y, x))` |

</div>

#### `covar_samp(y, x)`

<div className="docs-table-properties">

| **Description** | The sample covariance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / (regr_count(y, x) - 1)`, `covar_pop(y, x) / (1 - 1 / regr_count(y, x))` |
| **Alias(es)** | `regr_sxy(y, x)` |

</div>

#### `entropy(x)`

<div className="docs-table-properties">

| **Description** | The log-2 entropy of count input-values. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `kurtosis_pop(x)`

<div className="docs-table-properties">

| **Description** | The excess kurtosis (Fisher’s definition) without bias correction. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `kurtosis(x)`

<div className="docs-table-properties">

| **Description** | The excess kurtosis (Fisher's definition) with bias correction according to the sample size. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `mad(x)`

<div className="docs-table-properties">

| **Description** | The median absolute deviation. Temporal types return a positive `INTERVAL`. |
| :--- | :--- |
| **Formula** | `median(abs(x - median(x)))` |

</div>

#### `median(x)`

<div className="docs-table-properties">

| **Description** | The middle value of the set. For even value counts, quantitative values are averaged and ordinal values return the lower value. |
| :--- | :--- |
| **Formula** | `quantile_cont(x, 0.5)` |

</div>

#### `mode(x)`

<div className="docs-table-properties">

| **Description** | The most frequent value. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). |
| :--- | :--- |
| **Formula** | - |

</div>

#### `quantile_cont(x, pos)`

<div className="docs-table-properties">

| **Description** | The interpolated `pos`-quantile of `x` for `0 <= pos <= 1`. Returns the `pos * (n_nonnull_values - 1)`th (zero-indexed, in the specified order) value of `x` or an interpolation between the adjacent values if the index is not an integer. Intuitively, arranges the values of `x` as equispaced _points_ on a line, starting at 0 and ending at 1, and returns the (interpolated) value at `pos`. This is Type 7 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding interpolated quantiles. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `quantile_disc(x, pos)`

<div className="docs-table-properties">

| **Description** | The discrete `pos`-quantile of `x` for `0 <= pos <= 1`. Returns the `greatest(ceil(pos * n_nonnull_values) - 1, 0)`th (zero-indexed, in the specified order) value of `x`. Intuitively, assigns to each value of `x` an equisized _sub-interval_ (left-open and right-closed except for the initial interval) of the interval `[0, 1]`, and picks the value of the sub-interval that contains `pos`. This is Type 1 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding discrete quantiles. |
| :--- | :--- |
| **Formula** | - |
| **Alias(es)** | `quantile` |

</div>

#### `regr_avgx(y, x)`

<div className="docs-table-properties">

| **Description** | The average of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `regr_avgy(y, x)`

<div className="docs-table-properties">

| **Description** | The average of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `regr_count(y, x)`

<div className="docs-table-properties">

| **Description** | The number of non-`NULL` pairs. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `regr_intercept(y, x)`

<div className="docs-table-properties">

| **Description** | The intercept of the univariate linear regression line, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | `regr_avgy(y, x) - regr_slope(y, x) * regr_avgx(y, x)` |

</div>

#### `regr_r2(y, x)`

<div className="docs-table-properties">

| **Description** | The squared Pearson correlation coefficient between y and x. Also: The coefficient of determination in a linear regression, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `regr_slope(y, x)`

<div className="docs-table-properties">

| **Description** | Returns the slope of the linear regression line, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | `regr_sxy(y, x) / regr_sxx(y, x)` |
| **Alias(es)** | - |

</div>

#### `regr_sxx(y, x)`

<div className="docs-table-properties">

| **Description** | The sample variance, which includes Bessel's bias correction, of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `regr_sxy(y, x)`

<div className="docs-table-properties">

| **Description** | The sample covariance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / (regr_count(y, x) - 1)`, `covar_pop(y, x) / (1 - 1 / regr_count(y, x))` |
| **Alias(es)** | `covar_samp(y, x)` |

</div>

#### `regr_syy(y, x)`

<div className="docs-table-properties">

| **Description** | The sample variance, which includes Bessel's bias correction, of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `sem(x)`

<div className="docs-table-properties">

| **Description** | The standard error of the mean. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `skewness(x)`

<div className="docs-table-properties">

| **Description** | The skewness. |
| :--- | :--- |
| **Formula** | - |

</div>

#### `stddev_pop(x)`

<div className="docs-table-properties">

| **Description** | The population standard deviation. |
| :--- | :--- |
| **Formula** | `sqrt(var_pop(x))` |

</div>

#### `stddev_samp(x)`

<div className="docs-table-properties">

| **Description** | The sample standard deviation. |
| :--- | :--- |
| **Formula** | `sqrt(var_samp(x))`|
| **Alias(es)** | `stddev(x)`|

</div>

#### `var_pop(x)`

<div className="docs-table-properties">

| **Description** | The population variance, which does not include bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x^2) - sum(x)^2 / count(x)) / count(x)`, `var_samp(y, x) * (1 - 1 / count(x))` |

</div>

#### `var_samp(x)`

<div className="docs-table-properties">

| **Description** | The sample variance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x^2) - sum(x)^2 / count(x)) / (count(x) - 1)`, `var_pop(y, x) / (1 - 1 / count(x))` |
| **Alias(es)** | `variance(arg, val)` |

</div>

## Counter and Histogram Aggregates

These aggregates read counters that only grow, gauges that go up and down, and histograms of bucket counts, such as the sums, gauges and histograms OpenTelemetry SDKs export. Each takes the time of the sample (`TIMESTAMP`, `TIMESTAMP_NS` or `TIMESTAMPTZ`) as its second argument and orders the samples by it, so the order of the input rows does not matter. Of several samples with the same time, the largest counts. Samples with a `NULL` value or time, an infinite time, or a `NaN` value are ignored. A counter that goes down was restarted: the first sample after the drop counts in full as the increase since the restart.

They return `NULL` for a group with fewer than two samples (`otel_changes` and `otel_resets` return 0 for a single sample), and they don't extrapolate: the change is measured from the first to the last sample of the group, so the change between the last sample of one time bucket and the first sample of the next is counted in neither. Over a window of the current and the previous row, `otel_increase` gives the change since the previous sample:

<SqlLogicTest id="sql/functions/aggregates/index/counter_delta" />

#### `otel_increase(value, ts)`

The increase of the counter `value` from the first to the last sample, across restarts.

<SqlLogicTest id="sql/functions/aggregates/index/counters" />

#### `otel_increase(bucket_counts, ts)`

The increase of each bucket of a histogram whose `bucket_counts`, a list of per-bucket counts, only grow, as a `DOUBLE[]`. When any bucket goes down, the histogram was restarted and the sample after the drop counts in full. When the number of buckets changes, the increase up to that sample is dropped and the sample counts in full. A `NULL` element of `bucket_counts` counts as 0, and a sample with a `NaN` count is ignored.

<SqlLogicTest id="sql/functions/aggregates/index/histogram_increase" />

#### `otel_rate(value, ts)`

The per-second rate of the counter `value`: `otel_increase(value, ts)` divided by the seconds from the first to the last sample.

#### `otel_irate(value, ts)`

The per-second rate of the counter `value` between the last two samples.

#### `otel_delta(value, ts)`

The change of the gauge `value` from the first to the last sample.

#### `otel_idelta(value, ts)`

The change of the gauge `value` between the last two samples.

#### `otel_deriv(value, ts)`

The per-second slope of the gauge `value`, fitted by least squares over all samples. A `value` that never changes gives 0.

#### `otel_predict_linear(value, ts, seconds)`

The value the least-squares line through the samples of `value` reaches `seconds` after the last sample. The rows of a group must have the same `seconds`; otherwise the function fails.

#### `otel_changes(value, ts)`

The number of samples whose `value` differs from the previous sample, as a `BIGINT`.

#### `otel_resets(value, ts)`

The number of samples whose `value` is lower than the previous sample, the restarts of a counter, as a `BIGINT`.

<SqlLogicTest id="sql/functions/aggregates/index/gauges" />

#### `otel_histogram_quantile(q, bucket_counts, explicit_bounds)`

The `q`-quantile of a histogram with explicit bucket bounds, the layout OpenTelemetry uses: bucket `i` counts the values up to `explicit_bounds[i]` and the last bucket the values above the last bound, so `bucket_counts` has one element more than `explicit_bounds`, and `explicit_bounds` must be strictly ascending, without `NULL` or `NaN`. The counts of all rows of a group are added up, so the rows must have the same `q` and `explicit_bounds`; otherwise the function fails.

The result is interpolated linearly inside the bucket that holds the quantile. The first bucket starts at 0, unless its bound is 0 or less, in which case the result is that bound. A quantile in the last bucket is the last bound. A `q` below 0 gives `-Infinity` and above 1 `Infinity`. A `NULL` element of `bucket_counts` counts as 0. Rows with empty `bucket_counts` or a `NaN` count are skipped, and a group with no observations or no bounds gives `NULL`.

<SqlLogicTest id="sql/functions/aggregates/index/histogram_quantile" />

#### `otel_histogram_fraction(lower, upper, bucket_counts, explicit_bounds)`

The fraction of the observations of a histogram with explicit bucket bounds that lie between `lower` and `upper`, for example the share of requests that took at most 300 ms. Rows are added up as for `otel_histogram_quantile`, so they must have the same `lower`, `upper` and `explicit_bounds`.

Inside a bucket the observations are taken to be spread evenly, as for `otel_histogram_quantile`, and the first bucket starts at 0 when its bound is above 0. The last bucket, which has no upper bound, counts in full when `upper` is `Infinity` and not at all otherwise; a first bucket whose bound is 0 or less has no lower bound and counts in full when `lower` is `-Infinity` and not at all otherwise. A `lower` of `-Infinity` gives the fraction of observations up to `upper`. A `lower` at or above `upper` gives 0, a `NaN` bound gives `NaN`, and a group with no observations gives `NULL`.

<SqlLogicTest id="sql/functions/aggregates/index/histogram_fraction" />

#### `otel_histogram_stddev(bucket_counts, explicit_bounds)`

The estimated population standard deviation of the observations of a histogram with explicit bucket bounds. Each observation is taken to lie at the middle of its bucket: the first bucket at half its bound (at its bound when that is 0 or less), the last bucket at the last bound. Rows are added up as for `otel_histogram_quantile`. A group with no observations gives `NULL`.

#### `otel_histogram_stdvar(bucket_counts, explicit_bounds)`

The square of `otel_histogram_stddev`: the estimated population variance.

#### `otel_exponential_histogram(scale, zero_count, positive_offset, positive_bucket_counts, negative_offset, negative_bucket_counts)`

Packs the columns of an exponential histogram, the layout OpenTelemetry uses for base-2 exponential histograms, into one value for the aggregates below. Bucket `i` of the positive side counts the values above 2^(i·2^-`scale`) up to 2^((i+1)·2^-`scale`); `positive_bucket_counts[k]` is bucket `positive_offset + k`. The negative side holds the same buckets for the absolute values of negative observations, and `zero_count` counts the zeros. A `NULL` `zero_count`, offset, list or list element counts as 0 or empty; a value with a `NULL` `scale` or a `NaN` count is ignored by the aggregates, and a `scale` outside -10 to 20, or a bucket beyond the range of `DOUBLE` values at that scale, makes them fail.

#### `otel_histogram_quantile(q, histogram)`

The `q`-quantile of the exponential histograms of a group, added up. Rows at different scales are added at the coarsest of them, and at a coarser scale still when their buckets would otherwise span more than 4096 buckets on one side. Inside a bucket the result is interpolated on a logarithmic scale; a quantile among the zeros is 0. The rows of a group must have the same `q`. The other edge cases are those of `otel_histogram_quantile` for explicit bounds.

<SqlLogicTest id="sql/functions/aggregates/index/exponential_histogram" />

#### `otel_histogram_fraction(lower, upper, histogram)`

The fraction of the observations between `lower` and `upper`, interpolated on a logarithmic scale inside a bucket. Zeros count when `lower` is below 0 and `upper` at or above 0. The edge cases are those of `otel_histogram_fraction` for explicit bounds.

#### `otel_histogram_stddev(histogram)` and `otel_histogram_stdvar(histogram)`

The estimated population standard deviation and variance of the observations, each observation taken to lie at the geometric mean of its bucket's bounds and the zeros at 0.

#### `otel_increase(histogram, ts)`

The increase of each bucket of an exponential histogram whose counts only grow, as an exponential histogram value: the samples are compared at the coarsest scale among them, and when any bucket or `zero_count` goes down, the histogram was restarted and the sample after the drop counts in full. Buckets with no increase at either end are left out.

<SqlLogicTest id="sql/functions/aggregates/index/exponential_increase" />

## Ordered Set Aggregate Functions

The table below shows the available “ordered set” aggregate functions.
These functions are specified using the `WITHIN GROUP (ORDER BY sort_expression)` syntax,
and they are converted to an equivalent aggregate function that takes the ordering expression
as the first argument.

| Function                                                                                 | Equivalent                                                                      |
| :--------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------ |
| <code>mode() WITHIN GROUP (ORDER BY column [(ASC&#124;DESC)])</code>                     | <code>mode(column ORDER BY column [(ASC&#124;DESC)])</code>                     |
| <code>percentile_cont(fraction) WITHIN GROUP (ORDER BY column [(ASC&#124;DESC)])</code>  | <code>quantile_cont(column, fraction ORDER BY column [(ASC&#124;DESC)])</code>  |
| <code>percentile_cont(fractions) WITHIN GROUP (ORDER BY column [(ASC&#124;DESC)])</code> | <code>quantile_cont(column, fractions ORDER BY column [(ASC&#124;DESC)])</code> |
| <code>percentile_disc(fraction) WITHIN GROUP (ORDER BY column [(ASC&#124;DESC)])</code>  | <code>quantile_disc(column, fraction ORDER BY column [(ASC&#124;DESC)])</code>  |
| <code>percentile_disc(fractions) WITHIN GROUP (ORDER BY column [(ASC&#124;DESC)])</code> | <code>quantile_disc(column, fractions ORDER BY column [(ASC&#124;DESC)])</code> |

## Aggregate Aliases

These names are alternative spellings of the aggregates above, kept so that queries
written for PostgreSQL, DuckDB or other dialects run unchanged. Each behaves exactly like
the function it aliases.

| Function                     | Description                                                       |
| :--------------------------- | :---------------------------------------------------------------- |
| `argmax(arg, val)`           | Alias of [`arg_max(arg, val)`](#arg_maxarg-val).                  |
| `argmin(arg, val)`           | Alias of [`arg_min(arg, val)`](#arg_minarg-val).                  |
| `array_agg(arg)`             | Alias of [`list(arg)`](#listarg).                                 |
| `count_star()`               | Alias of [`count()`](#count).                                     |
| `group_concat(arg, sep)`     | Alias of [`string_agg(arg, sep)`](#string_aggarg-sep).            |
| `listagg(arg, sep)`          | Alias of [`string_agg(arg, sep)`](#string_aggarg-sep).            |
| `max_by(arg, val)`           | Alias of [`arg_max(arg, val)`](#arg_maxarg-val).                  |
| `mean(arg)`                  | Alias of [`avg(arg)`](#avgarg).                                   |
| `min_by(arg, val)`           | Alias of [`arg_min(arg, val)`](#arg_minarg-val).                  |
| `stddev(x)`                  | Alias of [`stddev_samp(x)`](#stddev_sampx).                       |
| `sumkahan(arg)`              | Alias of `kahan_sum(arg)`.                                        |
| `variance(x)`                | Alias of [`var_samp(x)`](#var_sampx).                             |

## Miscellaneous Aggregate Functions

| Function     | Description                                                                                                                                                                                                                                                                                          | Alias           |
| :----------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------- |
| `grouping()` | For queries with `GROUP BY` and either [`ROLLUP` or `GROUPING SETS`](../../../sql/query_syntax/grouping_sets/index.md#identifying-grouping-sets-with-grouping_id): Returns an integer identifying which of the argument expressions were used to group on to create the current super-aggregate row. | `grouping_id()` |
