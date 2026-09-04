---
title: CREATE MACRO
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `CREATE MACRO` statement can create a scalar or table macro (function) in the catalog.

For a scalar macro, `CREATE MACRO` is followed by the name of the macro, and optionally parameters within a set of parentheses. The keyword `AS` is next, followed by the text of the macro. By design, a scalar macro may only return a single value.
For a table macro, the syntax is similar to a scalar macro except `AS` is replaced with `AS TABLE`. A table macro may return a table of arbitrary size and shape.

<DocCallout type="tip">
If a `MACRO` is temporary, it is only usable within the same database connection and is deleted when the connection is closed.
</DocCallout>

## Examples

### Scalar Macros

Create a macro that adds two expressions (`a` and `b`):

<SqlLogicTest id="sql/statements/create_macro/index/example_001" />

Create a macro, replacing possible existing definitions:

<SqlLogicTest id="sql/statements/create_macro/index/example_002" />

Create a macro if it does not already exist, else do nothing:

<SqlLogicTest id="sql/statements/create_macro/index/example_003" />

Create a macro for a `CASE` expression:

<SqlLogicTest id="sql/statements/create_macro/index/example_004" />

Create a macro that does a subquery:

<SqlLogicTest id="sql/statements/create_macro/index/example_005" />

Macros are schema-dependent, and have an alias, `FUNCTION`:

<SqlLogicTest id="sql/statements/create_macro/index/example_006" />

Create a macro with a default parameter:

<SqlLogicTest id="sql/statements/create_macro/index/example_007" />

Create a macro `arr_append` (with a functionality equivalent to `array_append`):

<SqlLogicTest id="sql/statements/create_macro/index/example_008" />

Create a macro with a typed parameter:

<SqlLogicTest id="sql/statements/create_macro/index/example_009" />

### Table Macros

Create a table macro without parameters:

<SqlLogicTest id="sql/statements/create_macro/index/example_010" />

Create a table macro with parameters (that can be of any type):

<SqlLogicTest id="sql/statements/create_macro/index/example_011" />

Create a table macro that returns multiple rows. It will be replaced if it already exists, and it is temporary (will be automatically deleted when the connection ends):

<SqlLogicTest id="sql/statements/create_macro/index/example_012" />

Pass an argument as a list:

<SqlLogicTest id="sql/statements/create_macro/index/example_013" />

An example for how to use the `get_users` table macro is the following:

<SqlLogicTest id="sql/statements/create_macro/index/example_014" />

To define macros on arbitrary tables, use the [`query_table` function](../../../cookbook/sql_features/query_and_query_table_functions.md). For example, the following macro computes a column-wise checksum on a table:

<SqlLogicTest id="sql/statements/create_macro/index/example_015" />

## Overloading

It is possible to overload a macro based on the types or the number of its parameters; this works for both scalar and table macros.

By providing overloads we can have both `add_x(a, b)` and `add_x(a, b, c)` with different function bodies.

<SqlLogicTest id="sql/statements/create_macro/index/example_016" />

<SqlLogicTest id="sql/statements/create_macro/index/example_017" />

<SqlLogicTest id="sql/statements/create_macro/index/example_018" />

<SqlLogicTest id="sql/statements/create_macro/index/example_019" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

Macros allow you to create shortcuts for combinations of expressions.

<SqlLogicTest id="sql/statements/create_macro/index/example_020" />

This works:

<SqlLogicTest id="sql/statements/create_macro/index/example_021" />

Usage example:

<SqlLogicTest id="sql/statements/create_macro/index/example_022" />

However, this fails:

<SqlLogicTest id="sql/statements/create_macro/index/example_023" />

Macros can have default parameters.

`b` is a default parameter:

<SqlLogicTest id="sql/statements/create_macro/index/example_024" />

The following will result in 42:

<SqlLogicTest id="sql/statements/create_macro/index/example_025" />

The order of named parameters does not matter:

<SqlLogicTest id="sql/statements/create_macro/index/example_026" />

<SqlLogicTest id="sql/statements/create_macro/index/example_027" />

When macros are used, they are expanded (i.e., replaced with the original expression), and the parameters within the expanded expression are replaced with the supplied arguments. Step by step:

The `add` macro we defined above is used in a query:

<SqlLogicTest id="sql/statements/create_macro/index/example_028" />

Internally, `add` is replaced with its definition of `a + b`:

<SqlLogicTest id="sql/statements/create_macro/index/example_029" />

Then, the parameters are replaced by the supplied arguments:

<SqlLogicTest id="sql/statements/create_macro/index/example_030" />

## Limitations

### Using Subquery Macros

Table macros as well as scalar macros defined using scalar subqueries cannot be used in the arguments of table functions. SereneDB will return an error:

<SqlLogicTest id="sql/statements/create_macro/index/example_035" />

### Overloads

Overloads for macro functions have to be set at creation, it is not possible to define a macro by the same name twice without first removing the first definition.

### Recursive Functions

Defining recursive functions is not supported. A recursive macro expands until it exceeds the expression-depth limit (`max_expression_depth`, `1000` by default). For example, the following macro – supposed to compute the *n*th number of the Fibonacci sequence – fails (the example lowers `max_expression_depth` so the failure is immediate):

<SqlLogicTest id="sql/statements/create_macro/index/example_031" />

### Function Chaining on the First Function

Macros support the dot operator for function chaining on the first function.
For example, the following macro uses the `lower` function in the conventional form:

<SqlLogicTest id="sql/statements/create_macro/index/example_032" />

Rewriting `lower(s)` to use function chaining works as well:

<SqlLogicTest id="sql/statements/create_macro/index/example_033" />

### Viewing the List of Macros and Table Macros

You can use the following query to display the list of macros and table macros:

<SqlLogicTest id="sql/statements/create_macro/index/example_034" />
