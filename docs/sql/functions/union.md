---
title: Union Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

| Name                                                     | Description                                                                                                         |
| :------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| [`union.tag`](#uniontag)                                 | Dot notation serves as an alias for `union_extract`.                                                                |
| [`union_extract(union, 'tag')`](#union_extractunion-tag) | Extract the value with the named tags from the union. `NULL` if the tag is not currently selected.                  |
| [`union_value(tag := any)`](#union_valuetag--any)        | Create a single member `UNION` containing the argument value. The tag of the value will be the bound variable name. |
| [`union_tag(union)`](#union_tagunion)                    | Retrieve the currently selected tag of the union as an [Enum](../../sql/data_types/enum.md).                        |

#### `union.tag`

Dot notation serves as an alias for `union_extract`.

<SqlLogicTest id="sql/functions/union/uniontag" />

#### `union_extract(union, 'tag')`

Extract the value with the named tags from the union. `NULL` if the tag is not currently selected.

<SqlLogicTest id="sql/functions/union/union_extract" />

#### `union_value(tag := any)`

<div class="nostroke_table"></div>

| **Description** | Create a single member `UNION` containing the argument value. The tag of the value will be the bound variable name. |
| :--- | :--- |
| **Example** | `union_value(k := 'hello')` |
| **Result** | `'hello'::UNION(k VARCHAR)` |

#### `union_tag(union)`

Retrieve the currently selected tag of the union as an [Enum](../../sql/data_types/enum.md).

<SqlLogicTest id="sql/functions/union/union_tag" />
