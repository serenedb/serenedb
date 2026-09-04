---
title: Unnesting
sidebar_position: 10
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Examples

Unnest a list, generating 3 rows (1, 2, 3):

<SqlLogicTest id="sql/query_syntax/unnest/example_001" />

Unnesting a struct, generating two columns (a, b):

<SqlLogicTest id="sql/query_syntax/unnest/example_002" />

Recursive unnest of a list of structs:

<SqlLogicTest id="sql/query_syntax/unnest/example_003" />

Limit depth of recursive unnest using `max_depth`:

<SqlLogicTest id="sql/query_syntax/unnest/example_004" />

The `unnest` special function is used to unnest lists or structs by one level. The function can be used as a regular scalar function, but only in the `SELECT` clause. Invoking `unnest` with the `recursive` parameter will unnest lists and structs of multiple levels. The depth of unnesting can be limited using the `max_depth` parameter (which assumes `recursive` unnesting by default).

### Unnesting Lists

Unnest a list, generating 3 rows (1, 2, 3):

<SqlLogicTest id="sql/query_syntax/unnest/example_005" />

Unnest a list, generating 3 rows ((1, 10), (2, 10), (3, 10)):

<SqlLogicTest id="sql/query_syntax/unnest/example_006" />

Unnest two lists of different sizes, generating 3 rows ((1, 10), (2, 11), (3, NULL)):

<SqlLogicTest id="sql/query_syntax/unnest/example_007" />

Unnest a list column from a subquery:

<SqlLogicTest id="sql/query_syntax/unnest/example_008" />

Empty result:

<SqlLogicTest id="sql/query_syntax/unnest/example_009" />

Empty result:

<SqlLogicTest id="sql/query_syntax/unnest/example_010" />

Using `unnest` on a list emits one row per list entry. Regular scalar expressions in the same `SELECT` clause are repeated for every emitted row. When multiple lists are unnested in the same `SELECT` clause, the lists are unnested side-by-side. If one list is longer than the other, the shorter list is padded with `NULL` values.

Empty and `NULL` lists both unnest to zero rows.

### Unnesting Structs

Unnesting a struct, generating two columns (a, b):

<SqlLogicTest id="sql/query_syntax/unnest/example_011" />

Unnesting a struct, generating two columns (a, b):

<SqlLogicTest id="sql/query_syntax/unnest/example_012" />

`unnest` on a struct will emit one column per entry in the struct.

### Recursive Unnest

Unnesting a list of lists recursively, generating 5 rows (1, 2, 3, 4, 5):

<SqlLogicTest id="sql/query_syntax/unnest/example_013" />

Unnesting a list of structs recursively, generating two rows of two columns (a, b):

<SqlLogicTest id="sql/query_syntax/unnest/example_014" />

Unnesting a struct, generating two columns (a, b):

<SqlLogicTest id="sql/query_syntax/unnest/example_015" />

Calling `unnest` with the `recursive` setting will fully unnest lists, followed by fully unnesting structs. This can be useful to fully flatten columns that contain lists within lists, or lists of structs. Note that lists _within_ structs are not unnested.

### Setting the Maximum Depth of Unnesting

The `max_depth` parameter allows limiting the maximum depth of recursive unnesting (which is assumed by default and does not have to be specified separately).
For example, unnesting to `max_depth` of 2 yields the following:

<SqlLogicTest id="sql/query_syntax/unnest/example_016" />

Meanwhile, unnesting to `max_depth` of 3 results in:

<SqlLogicTest id="sql/query_syntax/unnest/example_017" />

### Keeping Track of List Entry Positions

To keep track of each entry's position within the original list, `unnest` may be combined with [`generate_subscripts`](../functions/list.md#generate_subscriptsarr-dim):

<SqlLogicTest id="sql/query_syntax/unnest/example_018" />

### Keep Column Names When Recursively Unnesting

The `keep_parent_names` parameter can be used to retain the parent column names when recursively unnesting a named struct. For example, unnesting the following query with `keep_parent_names` enabled:

<SqlLogicTest id="sql/query_syntax/unnest/example_019" />

In this case, the field names are preserved, showing the path to the innermost value. This is particularly useful when working with complex nested data structures, as it maintains the structure and naming convention of the original data. The parameter can also be used in conjunction with the `max_depth` parameter, allowing more control and enabling more precise management of nested structures.
