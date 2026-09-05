---
title: Bitstring Functions
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`BITSTRING`](../../sql/data_types/bitstring.md) values.
Bitstrings must be of equal length when performing the bitwise operands AND, OR and XOR. When bit shifting, the original length of the string is preserved.

## Bitstring Operators

The table below shows the available mathematical operators for `BIT` type.

<!-- markdownlint-disable MD056 -->

| Operator | Description         | Example                                   |             Result |
| :------- | :------------------ | :---------------------------------------- | -----------------: |
| `&`      | Bitwise AND         | `'10101'::BITSTRING & '10001'::BITSTRING` |            `10001` |
| <code>&#124;</code> | Bitwise OR | <code>'1011'::BITSTRING &#124; '0001'::BITSTRING</code> | `1011` |
| `xor`    | Bitwise XOR         | `xor('101'::BITSTRING, '001'::BITSTRING)` |              `100` |
| `~`      | Bitwise NOT         | `~('101'::BITSTRING)`                     |              `010` |
| `<<`     | Bitwise shift left  | `'1001011'::BITSTRING << 3`               |          `1011000` |
| `>>`     | Bitwise shift right | `'1001011'::BITSTRING >> 3`               |          `0001001` |

<!-- markdownlint-enable MD056 -->

## Bitstring Functions

The table below shows the available scalar functions for `BIT` type.

| Name                                                                        | Description                                                                                                                              |
| :-------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| [`bit_count(bitstring)`](#bit_countbitstring)                               | Returns the number of set bits in the bitstring.                                                                                         |
| [`bit_length(bitstring)`](#bit_lengthbitstring)                             | Returns the number of bits in the bitstring.                                                                                             |
| [`bit_position(substring, bitstring)`](#bit_positionsubstring-bitstring)    | Returns first starting index of the specified substring within bits, or zero if it's not present. The first (leftmost) bit is indexed 1. |
| [`bitstring(bitstring, length)`](#bitstringbitstring-length)                | Returns a bitstring of determined length.                                                                                                |
| [`get_bit(bitstring, index)`](#get_bitbitstring-index)                      | Extracts the nth bit from bitstring; the first (leftmost) bit is indexed 0.                                                              |
| [`length(bitstring)`](#lengthbitstring)                                     | Alias for `bit_length`.                                                                                                                  |
| [`octet_length(bitstring)`](#octet_lengthbitstring)                         | Returns the number of bytes in the bitstring.                                                                                            |
| [`set_bit(bitstring, index, new_value)`](#set_bitbitstring-index-new_value) | Sets the nth bit in bitstring to newvalue; the first (leftmost) bit is indexed 0. Returns a new bitstring.                               |

#### `bit_count(bitstring)`

Returns the number of set bits in the bitstring.

<SqlLogicTest id="sql/functions/bitstring/bit_count" />

#### `bit_length(bitstring)`

Returns the number of bits in the bitstring.

<SqlLogicTest id="sql/functions/bitstring/bit_length" />

#### `bit_position(substring, bitstring)`

Returns first starting index of the specified substring within bits, or zero if it's not present. The first (leftmost) bit is indexed 1.

<SqlLogicTest id="sql/functions/bitstring/bit_position" />

#### `bitstring(bitstring, length)`

Returns a bitstring of determined length.

<SqlLogicTest id="sql/functions/bitstring/bitstring" />

#### `get_bit(bitstring, index)`

Extracts the nth bit from bitstring; the first (leftmost) bit is indexed 0.

<SqlLogicTest id="sql/functions/bitstring/get_bit" />

#### `length(bitstring)`

Alias for `bit_length`.

<SqlLogicTest id="sql/functions/bitstring/length" />

#### `octet_length(bitstring)`

Returns the number of bytes in the bitstring.

<SqlLogicTest id="sql/functions/bitstring/octet_length" />

#### `set_bit(bitstring, index, new_value)`

Sets the nth bit in bitstring to newvalue; the first (leftmost) bit is indexed 0. Returns a new bitstring.

<SqlLogicTest id="sql/functions/bitstring/set_bit" />

## Bitstring Aggregate Functions

These aggregate functions are available for `BIT` type.

| Name                                                        | Description                                                                                                                                                                     |
| :---------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`bit_and(arg)`](#bit_andarg)                               | Returns the bitwise AND operation performed on all bitstrings in a given expression.                                                                                            |
| [`bit_or(arg)`](#bit_orarg)                                 | Returns the bitwise OR operation performed on all bitstrings in a given expression.                                                                                             |
| [`bit_xor(arg)`](#bit_xorarg)                               | Returns the bitwise XOR operation performed on all bitstrings in a given expression.                                                                                            |
| [`bitstring_agg(arg)`](#bitstring_aggarg)                   | Returns a bitstring with bits set for each distinct position defined in `arg`.                                                                                                  |
| [`bitstring_agg(arg, min, max)`](#bitstring_aggarg-min-max) | Returns a bitstring with bits set for each distinct position defined in `arg`. All positions must be within the range [`min`, `max`] or an `Out of Range Error` will be thrown. |

The examples below use a table `bits` with a `BITSTRING` column `A` holding `10101`, `11001` and `00110`, and a table `nums` with an `INTEGER` column `A` holding `1`, `3`, `5` and `8`.

<SqlLogicTest id="sql/functions/bitstring/setup" />

#### `bit_and(arg)`

Returns the bitwise AND operation performed on all bitstrings in a given expression.

<SqlLogicTest id="sql/functions/bitstring/bit_and" />

#### `bit_or(arg)`

Returns the bitwise OR operation performed on all bitstrings in a given expression.

<SqlLogicTest id="sql/functions/bitstring/bit_or" />

#### `bit_xor(arg)`

Returns the bitwise XOR operation performed on all bitstrings in a given expression.

<SqlLogicTest id="sql/functions/bitstring/bit_xor" />

#### `bitstring_agg(arg)`

The `bitstring_agg` function takes any integer type as input and returns a bitstring with bits set for each distinct value. The left-most bit represents the smallest value in the column and the right-most bit the maximum value. If possible, the min and max are retrieved from the column statistics. Otherwise, it is also possible to provide the min and max values.

<SqlLogicTest id="sql/functions/bitstring/bitstring_agg" />

<DocCallout type="tip">
The combination of `bit_count` and `bitstring_agg` can be used as an alternative to `count(DISTINCT ...)`, with possible performance improvements in cases of low cardinality and dense values.
</DocCallout>

#### `bitstring_agg(arg, min, max)`

Returns a bitstring with bits set for each distinct position defined in `arg`. All positions must be within the range [`min`, `max`] or an `Out of Range Error` will be thrown.

<SqlLogicTest id="sql/functions/bitstring/bitstring_agg_min_max" />
