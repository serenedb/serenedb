---
layout: docu
redirect_from:
- /docs/guides/meta/explain
- /docs/preview/guides/meta/explain
- /docs/stable/guides/meta/explain
title: 'EXPLAIN: Inspect Query Plans'
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<SqlLogicTest id="cookbook/meta/explain/example_001" />

The `EXPLAIN` statement displays the physical plan, i.e., the query plan that will get executed,
and is enabled by prepending the query with `EXPLAIN`.
The physical plan is a tree of operators that are executed in a specific order to produce the result of the query.
To generate an efficient physical plan, the query optimizer transforms the existing physical plan into a better physical plan.

To demonstrate, see the below example:

<SqlLogicTest id="cookbook/meta/explain/example_002" />

Note that the query is not actually executed – therefore, we can only see the estimated cardinality (shown as the `~N rows` line) for each operator, which is calculated by using the statistics of the base tables and applying heuristics for each operator.

Table scan operators display the fully qualified table name including catalog and schema (e.g., `memory.myschema.mytable`).

## Additional Explain Settings

The `EXPLAIN` statement supports additional settings that can be used to control the output. These settings are controlled via the `explain_output` pragma. The following values are available:

`physical_only` is the default setting. It shows only the physical plan.

<SqlLogicTest id="cookbook/meta/explain/example_003" />

`optimized_only` shows only the optimized plan.

<SqlLogicTest id="cookbook/meta/explain/example_004" />

`all` shows both the physical and optimized plans.

<SqlLogicTest id="cookbook/meta/explain/example_005" />

## See Also

For more information, see the ["Profiling" page](../performance/profiling.md).
