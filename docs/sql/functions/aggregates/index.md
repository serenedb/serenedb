---
title: Aggregate Functions
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
| [`arg_max(arg, val)`](#arg_maxarg-val)                                                         | Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                      |
| [`arg_max(arg, val, n)`](#arg_maxarg-val-n)                                                    | The generalized case of [`arg_max`](#arg_maxarg-val) for `n` values: returns a `LIST` containing the `arg` expressions for the top `n` rows ordered by `val` descending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                |
| [`arg_max_null(arg, val)`](#arg_max_nullarg-val)                                               | Finds the row with the maximum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                  |
| [`arg_min(arg, val)`](#arg_minarg-val)                                                         | Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the value of the `arg` or `val` expression is `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                      |
| [`arg_min(arg, val, n)`](#arg_minarg-val-n)                                                    | Returns a `LIST` containing the `arg` expressions for the "bottom" `n` rows ordered by `val` ascending. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                 |
| [`arg_min_null(arg, val)`](#arg_min_nullarg-val)                                               | Finds the row with the minimum `val` and calculates the `arg` expression at that row. Rows where the `val` expression evaluates to `NULL` are ignored. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                  |
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
| [`favg(arg)`](#favgarg)                                                                        | Calculates the average using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                      |
| [`first(arg)`](#firstarg)                                                                      | Returns the first value (null or non-null) from `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [`fsum(arg)`](#fsumarg)                                                                        | Calculates the sum using a more accurate floating point summation (Kahan Sum). This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                          |
| [`geometric_mean(arg)`](#geometric_meanarg)                                                    | Calculates the geometric mean of all non-null values in `arg`. This function is [affected by ordering](#order-by-clause-in-aggregate-functions).                                                                                                                                                                                                                                                                                                                                                                                                          |
| [`histogram(arg)`](#histogramarg)                                                              | Returns a `MAP` of key-value pairs representing buckets and counts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [`histogram(arg, boundaries)`](#histogramarg-boundaries)                                       | Returns a `MAP` of key-value pairs representing the provided upper `boundaries` and counts of elements in the corresponding bins (left-open and right-closed partitions) of the datatype. A boundary at the largest value of the datatype is automatically added when elements larger than all provided `boundaries` appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg). Boundaries may be provided, e.g., via [`equi_width_bins`](../../../sql/functions/utility.md#equi_width_binsmin-max-bincount-nice--false). |
| [`histogram_exact(arg, elements)`](#histogram_exactarg-elements)                               | Returns a `MAP` of key-value pairs representing the requested elements and their counts. A catch-all element specific to the data-type is automatically added to count other elements when they appear, see [`is_histogram_other_bin`](../../../sql/functions/utility.md#is_histogram_other_binarg).                                                                                                                                                                                                                                                      |
| [`histogram_values(source, boundaries)`](#histogram_valuessource-col_name-technique-bin_count) | Returns the upper boundaries of the bins and their counts.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
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

<div class="nostroke_table"></div>

| **Description** | Returns the upper boundaries of the bins and their counts. |
| :--- | :--- |
| **Example** | `histogram_values(integers, i, bin_count := 2)` |

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

<div class="nostroke_table"></div>

| **Description** | The correlation coefficient. |
| :--- | :--- |
| **Formula** | `covar_pop(y, x) / (stddev_pop(x) * stddev_pop(y))` |

#### `covar_pop(y, x)`

<div class="nostroke_table"></div>

| **Description** | The population covariance, which does not include bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / regr_count(y, x)`, `covar_samp(y, x) * (1 - 1 / regr_count(y, x))` |

#### `covar_samp(y, x)`

<div class="nostroke_table"></div>

| **Description** | The sample covariance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / (regr_count(y, x) - 1)`, `covar_pop(y, x) / (1 - 1 / regr_count(y, x))` |
| **Alias(es)** | `regr_sxy(y, x)` |

#### `entropy(x)`

<div class="nostroke_table"></div>

| **Description** | The log-2 entropy of count input-values. |
| :--- | :--- |
| **Formula** | - |

#### `kurtosis_pop(x)`

<div class="nostroke_table"></div>

| **Description** | The excess kurtosis (Fisher’s definition) without bias correction. |
| :--- | :--- |
| **Formula** | - |

#### `kurtosis(x)`

<div class="nostroke_table"></div>

| **Description** | The excess kurtosis (Fisher's definition) with bias correction according to the sample size. |
| :--- | :--- |
| **Formula** | - |

#### `mad(x)`

<div class="nostroke_table"></div>

| **Description** | The median absolute deviation. Temporal types return a positive `INTERVAL`. |
| :--- | :--- |
| **Formula** | `median(abs(x - median(x)))` |

#### `median(x)`

<div class="nostroke_table"></div>

| **Description** | The middle value of the set. For even value counts, quantitative values are averaged and ordinal values return the lower value. |
| :--- | :--- |
| **Formula** | `quantile_cont(x, 0.5)` |

#### `mode(x)`

<div class="nostroke_table"></div>

| **Description** | The most frequent value. This function is [affected by ordering](#order-by-clause-in-aggregate-functions). |
| :--- | :--- |
| **Formula** | - |

#### `quantile_cont(x, pos)`

<div class="nostroke_table"></div>

| **Description** | The interpolated `pos`-quantile of `x` for `0 <= pos <= 1`. Returns the `pos * (n_nonnull_values - 1)`th (zero-indexed, in the specified order) value of `x` or an interpolation between the adjacent values if the index is not an integer. Intuitively, arranges the values of `x` as equispaced _points_ on a line, starting at 0 and ending at 1, and returns the (interpolated) value at `pos`. This is Type 7 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding interpolated quantiles. |
| :--- | :--- |
| **Formula** | - |

#### `quantile_disc(x, pos)`

<div class="nostroke_table"></div>

| **Description** | The discrete `pos`-quantile of `x` for `0 <= pos <= 1`. Returns the `greatest(ceil(pos * n_nonnull_values) - 1, 0)`th (zero-indexed, in the specified order) value of `x`. Intuitively, assigns to each value of `x` an equisized _sub-interval_ (left-open and right-closed except for the initial interval) of the interval `[0, 1]`, and picks the value of the sub-interval that contains `pos`. This is Type 1 in Hyndman & Fan (1996). If `pos` is a `LIST` of `FLOAT`s, then the result is a `LIST` of the corresponding discrete quantiles. |
| :--- | :--- |
| **Formula** | - |
| **Alias(es)** | `quantile` |

#### `regr_avgx(y, x)`

<div class="nostroke_table"></div>

| **Description** | The average of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

#### `regr_avgy(y, x)`

<div class="nostroke_table"></div>

| **Description** | The average of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

#### `regr_count(y, x)`

<div class="nostroke_table"></div>

| **Description** | The number of non-`NULL` pairs. |
| :--- | :--- |
| **Formula** | - |

#### `regr_intercept(y, x)`

<div class="nostroke_table"></div>

| **Description** | The intercept of the univariate linear regression line, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | `regr_avgy(y, x) - regr_slope(y, x) * regr_avgx(y, x)` |

#### `regr_r2(y, x)`

<div class="nostroke_table"></div>

| **Description** | The squared Pearson correlation coefficient between y and x. Also: The coefficient of determination in a linear regression, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

#### `regr_slope(y, x)`

<div class="nostroke_table"></div>

| **Description** | Returns the slope of the linear regression line, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | `regr_sxy(y, x) / regr_sxx(y, x)` |
| **Alias(es)** | - |

#### `regr_sxx(y, x)`

<div class="nostroke_table"></div>

| **Description** | The sample variance, which includes Bessel's bias correction, of the independent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

#### `regr_sxy(y, x)`

<div class="nostroke_table"></div>

| **Description** | The sample covariance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x*y) - sum(x) * sum(y) / regr_count(y, x)) / (regr_count(y, x) - 1)`, `covar_pop(y, x) / (1 - 1 / regr_count(y, x))` |
| **Alias(es)** | `covar_samp(y, x)` |

#### `regr_syy(y, x)`

<div class="nostroke_table"></div>

| **Description** | The sample variance, which includes Bessel's bias correction, of the dependent variable for non-`NULL` pairs, where x is the independent variable and y is the dependent variable. |
| :--- | :--- |
| **Formula** | - |

#### `sem(x)`

<div class="nostroke_table"></div>

| **Description** | The standard error of the mean. |
| :--- | :--- |
| **Formula** | - |

#### `skewness(x)`

<div class="nostroke_table"></div>

| **Description** | The skewness. |
| :--- | :--- |
| **Formula** | - |

#### `stddev_pop(x)`

<div class="nostroke_table"></div>

| **Description** | The population standard deviation. |
| :--- | :--- |
| **Formula** | `sqrt(var_pop(x))` |

#### `stddev_samp(x)`

<div class="nostroke_table"></div>

| **Description** | The sample standard deviation. |
| :--- | :--- |
| **Formula** | `sqrt(var_samp(x))`|
| **Alias(es)** | `stddev(x)`|

#### `var_pop(x)`

<div class="nostroke_table"></div>

| **Description** | The population variance, which does not include bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x^2) - sum(x)^2 / count(x)) / count(x)`, `var_samp(y, x) * (1 - 1 / count(x))` |

#### `var_samp(x)`

<div class="nostroke_table"></div>

| **Description** | The sample variance, which includes Bessel's bias correction. |
| :--- | :--- |
| **Formula** | `(sum(x^2) - sum(x)^2 / count(x)) / (count(x) - 1)`, `var_pop(y, x) / (1 - 1 / count(x))` |
| **Alias(es)** | `variance(arg, val)` |

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

## Miscellaneous Aggregate Functions

| Function     | Description                                                                                                                                                                                                                                                                                          | Alias           |
| :----------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------- |
| `grouping()` | For queries with `GROUP BY` and either [`ROLLUP` or `GROUPING SETS`](../../../sql/query_syntax/grouping_sets/index.md#identifying-grouping-sets-with-grouping_id): Returns an integer identifying which of the argument expressions were used to group on to create the current super-aggregate row. | `grouping_id()` |
