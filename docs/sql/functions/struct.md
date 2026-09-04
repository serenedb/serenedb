---
title: Struct Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

| Name                                                                         | Description                                                                                                              |
| :--------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------- |
| [`struct.entry`](#structentry)                                               | Dot notation that serves as an alias for `struct_extract` from named `STRUCT`s.                                          |
| [`struct[entry]`](#structentry)                                              | Bracket notation that serves as an alias for `struct_extract` from named `STRUCT`s.                                      |
| [`struct[idx]`](#structidx)                                                  | Bracket notation that serves as an alias for `struct_extract` from unnamed `STRUCT`s (tuples), using an index (1-based). |
| [`row(any, ...)`](#rowany-)                                                  | Create an unnamed `STRUCT` (tuple) containing the argument values.                                                       |
| [`struct_concat(structs...)`](#struct_concatstructs)                         | Merge the multiple `structs` into a single `STRUCT`.                                                                     |
| [`struct_contains(struct, entry)`](#struct_containsstruct-entry)             | Check if the `STRUCT` contains the specified entry.                                                                      |
| [`struct_extract(struct, 'entry')`](#struct_extractstruct-entry)             | Extract the named entry from the `STRUCT`.                                                                               |
| [`struct_extract(struct, idx)`](#struct_extractstruct-idx)                   | Extract the entry from an unnamed `STRUCT` (tuple) using an index (1-based).                                             |
| [`struct_extract_at(struct, idx)`](#struct_extract_atstruct-idx)             | Extract the entry from a `STRUCT` (tuple) using an index (1-based).                                                      |
| [`struct_insert(struct, name := any, ...)`](#struct_insertstruct-name--any-) | Add field(s) to an existing `STRUCT`.                                                                                    |
| [`struct_pack(name := any, ...)`](#struct_packname--any-)                    | Create a `STRUCT` containing the argument values. The entry name will be the bound variable name.                        |
| [`struct_position(struct, entry)`](#struct_positionstruct-entry)             | Return the index of the entry within the `STRUCT` (1-based), or `NULL` if not found.                                     |
| [`struct_update(struct, name := any, ...)`](#struct_updatestruct-name--any-) | Add or update field(s) of an existing `STRUCT`.                                                                          |
| [`struct_values(struct)`](#struct_valuesstruct)                              | Return the values of a `STRUCT` as an unnamed `STRUCT` (tuple).                                                          |

#### `struct.entry`

Dot notation that serves as an alias for `struct_extract` from named `STRUCT`s.

<SqlLogicTest id="sql/functions/struct/structentry" />

#### `struct[entry]`

Bracket notation that serves as an alias for `struct_extract` from named `STRUCT`s.

<SqlLogicTest id="sql/functions/struct/structentry_bracket" />

#### `struct[idx]`

Bracket notation that serves as an alias for `struct_extract` from unnamed `STRUCT`s (tuples), using an index (1-based).

<SqlLogicTest id="sql/functions/struct/structidx" />

#### `row(any, ...)`

Create an unnamed `STRUCT` (tuple) containing the argument values.

<SqlLogicTest id="sql/functions/struct/rowany" />

#### `struct_concat(structs...)`

Merge the multiple `structs` into a single `STRUCT`.

<SqlLogicTest id="sql/functions/struct/struct_concat" />

#### `struct_contains(struct, entry)`

Check if the `STRUCT` contains the specified entry. Alias: `struct_has`.

<SqlLogicTest id="sql/functions/struct/struct_contains" />

#### `struct_extract(struct, 'entry')`

Extract the named entry from the `STRUCT`.

<SqlLogicTest id="sql/functions/struct/struct_extract_entry" />

#### `struct_extract(struct, idx)`

Extract the entry from an unnamed `STRUCT` (tuple) using an index (1-based).

<SqlLogicTest id="sql/functions/struct/struct_extract_idx" />

#### `struct_extract_at(struct, idx)`

Extract the entry from a `STRUCT` (tuple) using an index (1-based).

<SqlLogicTest id="sql/functions/struct/struct_extract_at" />

#### `struct_insert(struct, name := any, ...)`

Add field(s) to an existing `STRUCT`.

<SqlLogicTest id="sql/functions/struct/struct_insert" />

#### `struct_pack(name := any, ...)`

Create a `STRUCT` containing the argument values. The entry name will be the bound variable name.

<SqlLogicTest id="sql/functions/struct/struct_pack" />

#### `struct_position(struct, entry)`

Return the index of the entry within the `STRUCT` (1-based), or `NULL` if not found. Alias: `struct_indexof`.

<SqlLogicTest id="sql/functions/struct/struct_position" />

#### `struct_update(struct, name := any, ...)`

Add or update field(s) of an existing `STRUCT`.

<SqlLogicTest id="sql/functions/struct/struct_update" />

#### `struct_values(struct)`

Return the values of a `STRUCT` as an unnamed `STRUCT` (tuple).

<SqlLogicTest id="sql/functions/struct/struct_values" />
