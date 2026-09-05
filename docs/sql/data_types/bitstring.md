---
title: Bitstring
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name        | Aliases | Description                          |
| :---------- | :------ | :----------------------------------- |
| `BITSTRING` | `BIT`   | Variable-length strings of 1s and 0s |

Bitstrings are strings of 1s and 0s. The bit type data is of variable length. A bitstring value requires 1 byte for each group of 8 bits, plus a fixed amount to store some metadata.

By default bitstrings will not be padded with zeroes.
Bitstrings can be very large, having the same size restrictions as `BLOB`s.

## Creating a Bitstring

A string encoding a bitstring can be cast to a `BITSTRING`:

<SqlLogicTest id="sql/data_types/bitstring/example_001" />

Creating a `BITSTRING` with a predefined length is possible with the `bitstring` function. The resulting bitstring will be left-padded with zeroes.

<SqlLogicTest id="sql/data_types/bitstring/example_002" />

Numeric values (integer and float values) can also be converted to a `BITSTRING` via casting. For example:

<SqlLogicTest id="sql/data_types/bitstring/example_003" />

## Functions

See [Bitstring Functions](../../sql/functions/bitstring.md).
