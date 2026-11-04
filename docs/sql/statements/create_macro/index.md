---
title: CREATE MACRO
split: headings
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

An argument that is a constant expression takes the place of its parameter as its value, so it can stand where a constant is required, such as in `LIMIT`. Any other argument that the query reads more than once, other than a column or a parameter, is evaluated once for each call.

### SQL Functions

`CREATE FUNCTION` also takes PostgreSQL's form of an SQL function: typed parameters, a `RETURNS` clause, `LANGUAGE sql` and the body as a string. Any language other than `sql` is not supported. The function returns its `RETURNS` type, reached from the body's type the way PostgreSQL assigns a value to a column of that type. As in PostgreSQL, a type modifier of the result, such as the precision and scale of `numeric(4,2)`, is not applied:

<SqlLogicTest id="sql/statements/create_macro/index/example_036" />

In the body, a parameter is a value of the call: it can stand next to an aggregate, in a `HAVING` clause, in a `WITH` query or in a nested subquery, and an aggregate over a parameter aggregates the rows of the query it is written in:

<SqlLogicTest id="sql/statements/create_macro/index/example_040" />

Inside a query of the body, a column of that query, or of a query of the body around it, takes precedence over a parameter of the same name. The parameter is then reached by qualifying it with the name of the function, as `function_name.parameter`, which also works anywhere else in the body. A parameter of a composite type gives its fields as `parameter.field`:

<SqlLogicTest id="sql/statements/create_macro/index/example_044" />

`STRICT`, or `RETURNS NULL ON NULL INPUT`, makes the function return `NULL` without running its body when any argument is `NULL`, and a set-returning function return no rows. `CALLED ON NULL INPUT`, the default, runs the body:

<SqlLogicTest id="sql/statements/create_macro/index/example_045" />

A parameter takes a default after `DEFAULT` or `=`, as well as after `:=`. `IMMUTABLE`, `STABLE` and `VOLATILE`, volatile by default, are shown in `pg_proc.provolatile` and in the function's definition; what a call evaluates follows its body.

A function runs with the privileges of the role that calls it, also when a view calls it: the tables its body reads and the functions it calls are checked for that role, and `current_user` returns it. `SECURITY DEFINER` runs the body with the privileges of the function's owner instead, and `current_user` in the body returns the owner, while the arguments are still evaluated with the caller's privileges. The body of a `SECURITY DEFINER` function cannot change the session's role: `set_config('role', ...)` and `set_config('session_authorization', ...)` there fail with `cannot set parameter "role" within security-definer function`. `SECURITY INVOKER` is the default, and `pg_proc.prosecdef` shows which one a function uses. `SECURITY DEFINER` does not make a call slower: either way the body is optimized together with the query that calls it. A `SECURITY DEFINER` function cannot be called as a window function. Creating a function checks no privileges on the tables its body reads or writes and the functions it calls: the role the body runs as needs them when it runs. It does need `USAGE` on the function's language, `sql`, which every role has until it is [revoked from `PUBLIC`](../../../security/privileges.md#available-privileges).

The body resolves the names it does not qualify through the `search_path` of the session that calls it. `SET search_path = schema, ...` (or `TO`) gives the function a path of its own, which the body uses instead, also when the function is created and when a view calls it; `SET search_path FROM CURRENT` takes the creating session's path. A `SECURITY DEFINER` function should set one, so that a caller cannot put objects of its own in the way of the names the body uses. `pg_proc.proconfig` shows the setting and `pg_get_functiondef` prints it. A function can set only `search_path`: `SET` of any other parameter fails with `SET of parameter "..." on a function is not supported`. A function that sets `search_path` cannot be called as a window function.

A function that `RETURNS void` runs its body to the end, every row of a query body included, and returns `NULL`. A function whose body modifies data cannot be called for each row of a query:

<SqlLogicTest id="sql/statements/create_macro/index/example_047" />

Without `SETOF`, the function returns the first row of the body, also when its result calls a set-returning function such as `unnest`. `RETURNS SETOF <type>` returns every row of the body. Called in `FROM`, the function's column is named after the function:

<SqlLogicTest id="sql/statements/create_macro/index/example_037" />

`RETURNS TABLE` names and types the columns:

<SqlLogicTest id="sql/statements/create_macro/index/example_038" />

An `OUT` parameter is a column of the result and an `INOUT` parameter is both an argument and a column. A function with one output returns that type, one with several returns a record of them, and `RETURNS SETOF` with outputs returns every row. A `RETURNS` clause next to outputs must name that type: the output's type for one output, `record` for several. Called in `FROM`, a record function returns its outputs as columns:

<SqlLogicTest id="sql/statements/create_macro/index/example_039" />

## Overloading

It is possible to overload a macro based on the types or the number of its parameters; this works for both scalar and table macros.

`ALTER FUNCTION`, `ALTER PROCEDURE` and `ALTER ROUTINE` rename a function with `RENAME TO` and change its owner with `OWNER TO`, for the overload the argument list after the name names, or for the function's only overload when the list is left out; with several overloads and no list, the statement fails with `function name "..." is not unique`. A renamed overload joins the overloads of its new name, and `RENAME TO` fails with `function name(...) already exists in schema "..."` when the new name has an overload with the same arguments. The only overload of a name renamed to an unused name keeps its `oid`; an overload renamed away from other overloads of its name, or to a name that has overloads, gets a new `oid`.

They also change `SECURITY DEFINER` or `SECURITY INVOKER`, `IMMUTABLE`, `STABLE` or `VOLATILE`, and `SET search_path` (`RESET search_path` or `RESET ALL` removes it), for the overload the argument list names, or for the function's only overload when the list is left out; with several overloads and no list, the statement fails with `function name "..." is not unique`. Only the function's owner or a superuser may do it. `COST`, `ROWS`, `PARALLEL` and `LEAKPROOF` are accepted and ignored, and `STRICT` or `CALLED ON NULL INPUT` may only restate what the function already is.

```sql
ALTER FUNCTION add_tax(numeric) SECURITY DEFINER STABLE;
```

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

The `add_pair` macro we defined above is used in a query:

<SqlLogicTest id="sql/statements/create_macro/index/example_028" />

Internally, `add_pair` is replaced with its definition of `a + b`:

<SqlLogicTest id="sql/statements/create_macro/index/example_029" />

Then, the parameters are replaced by the supplied arguments:

<SqlLogicTest id="sql/statements/create_macro/index/example_030" />

An argument that the expression uses more than once is evaluated once for each row, and only for the rows where the expression reads it, such as the taken branch of a `CASE`, including inside a lambda, where it is evaluated only for the elements that read it:

<SqlLogicTest id="sql/statements/create_macro/index/example_041" />

A volatile argument such as `random()` or `nextval('s')` is no exception: the expression sees one value of it for each row:

<SqlLogicTest id="sql/statements/create_macro/index/example_043" />

A macro used as a predicate in `WHERE` or in a join condition is planned like the expression it expands to, so it can filter a table scan, use an index or make a hash join.

In a macro whose body contains a subquery, an aggregate over a parameter aggregates the rows of the subquery it is written in:

<SqlLogicTest id="sql/statements/create_macro/index/example_042" />

An argument is evaluated in the calling query: its names are the calling query's columns, an aggregate or window function in it is computed over the rows of the calling query, and a nested call of a macro with a subquery is evaluated once for each row of the calling query, even when the subquery returns no rows. Only an argument that names something the calling query does not have is replaced as written, so that the body can resolve it, such as a column of the table the body reads with `query_table`.

A macro or function whose body is a query or contains a subquery runs that query for each call, so a volatile argument, or a volatile function in the body, gives every row its own value:

<SqlLogicTest id="sql/statements/create_macro/index/example_046" />

## Limitations

### Using Subquery Macros

Table macros as well as scalar macros defined using scalar subqueries cannot be used in the arguments of table functions. SereneDB will return an error:

<SqlLogicTest id="sql/statements/create_macro/index/example_035" />

A scalar subquery made of a single expression, with no `FROM` or other clause, such as `(SELECT 5)`, expands to that expression, so a macro defined with it has no such limitation, unless the expression calls an aggregate, window or set-returning function or contains a subquery. The same limitation keeps a macro or function whose body is a query, such as one that reads a table, out of `CHECK` constraints, `DEFAULT` values, generated columns and index expressions.


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
