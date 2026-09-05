---
title: Caveats
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Equality Comparison

Equality and ordering comparisons on `JSON` values are based on their raw physical text, not on their logical content. Two `JSON` values with identical logical content are treated as different whenever their text differs in whitespace, number formatting, or object key order.

The following query shows that `JSON` comparison is purely textual, so logically equivalent values are treated as not equal when their physical text differs. It also shows the subscript operator (`[...]`) extracting an array element by position and an object field by key:

<SqlLogicTest id="data_import_and_export/json/caveats/example_001" />
