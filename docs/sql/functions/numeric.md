---
title: Numeric Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

## Numeric Operators

The table below shows the available mathematical operators for [numeric types](../../sql/data_types/numeric.md).

<!-- markdownlint-disable MD056 -->

| Operator | Description               | Example    | Result |
| -------- | ------------------------- | ---------- | ------ |
| `+`      | Addition                  | `2 + 3`    | `5`    |
| `-`      | Subtraction               | `2 - 3`    | `-1`   |
| `*`      | Multiplication            | `2 * 3`    | `6`    |
| `/`      | Float division            | `5 / 2`    | `2.5`  |
| `//`     | Division                  | `5 // 2`   | `2`    |
| `%`      | Modulo (remainder)        | `5 % 4`    | `1`    |
| `**`     | Exponent                  | `3 ** 4`   | `81`   |
| `^`      | Exponent (alias for `**`) | `3 ^ 4`    | `81`   |
| `&`      | Bitwise AND               | `91 & 15`  | `11`   |
| <code>&#124;</code> | Bitwise OR | <code>32 &#124; 3</code> | `35` |
| `<<`     | Bitwise shift left        | `1 << 4`   | `16`   |
| `>>`     | Bitwise shift right       | `8 >> 2`   | `2`    |
| `~`      | Bitwise negation          | `~15`      | `-16`  |
| `!`      | Factorial of `x`          | `4!`       | `24`   |

<!-- markdownlint-enable MD056 -->

### Division and Modulo Operators

There are two division operators: `/` and `//`.
They are equivalent when at least one of the operands is a `FLOAT` or a `DOUBLE`.
When both operands are integers, `/` performs floating points division (`5 / 2 = 2.5`) while `//` performs integer division (`5 // 2 = 2`).

### Supported Types

The modulo, bitwise, negation, and factorial operators work only on integral data types,
whereas the others are available for all numeric data types.

## Numeric Functions

The table below shows the available mathematical functions.

| Name                                                                   | Description                                                                                                                                                                                    |
| :--------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`@(x)`](#x)                                                           | Absolute value. Parentheses are optional if `x` is a column name.                                                                                                                              |
| [`abs(x)`](#absx)                                                      | Absolute value.                                                                                                                                                                                |
| [`acos(x)`](#acosx)                                                    | Computes the inverse cosine of `x`.                                                                                                                                                            |
| [`acosh(x)`](#acoshx)                                                  | Computes the inverse hyperbolic cosine of `x`.                                                                                                                                                 |
| [`add(x, y)`](#addx-y)                                                 | Alias for `x + y`.                                                                                                                                                                             |
| [`asin(x)`](#asinx)                                                    | Computes the inverse sine of `x`.                                                                                                                                                              |
| [`asinh(x)`](#asinhx)                                                  | Computes the inverse hyperbolic sine of `x`.                                                                                                                                                   |
| [`atan(x)`](#atanx)                                                    | Computes the inverse tangent of `x`.                                                                                                                                                           |
| [`atanh(x)`](#atanhx)                                                  | Computes the inverse hyperbolic tangent of `x`.                                                                                                                                                |
| [`atan2(y, x)`](#atan2y-x)                                             | Computes the inverse tangent of `(y, x)`.                                                                                                                                                      |
| [`bit_count(x)`](#bit_countx)                                          | Returns the number of bits that are set.                                                                                                                                                       |
| [`cbrt(x)`](#cbrtx)                                                    | Returns the cube root of the number.                                                                                                                                                           |
| [`ceil(x)`](#ceilx)                                                    | Rounds the number up.                                                                                                                                                                          |
| [`ceiling(x)`](#ceilingx)                                              | Rounds the number up. Alias of `ceil`.                                                                                                                                                         |
| [`cos(x)`](#cosx)                                                      | Computes the cosine of `x`.                                                                                                                                                                    |
| [`cot(x)`](#cotx)                                                      | Computes the cotangent of `x`.                                                                                                                                                                 |
| [`degrees(x)`](#degreesx)                                              | Converts radians to degrees.                                                                                                                                                                   |
| [`divide(x, y)`](#dividex-y)                                           | Alias for `x // y`.                                                                                                                                                                            |
| [`even(x)`](#evenx)                                                    | Round to next even number by rounding away from zero.                                                                                                                                          |
| [`exp(x)`](#expx)                                                      | Computes `e ** x`.                                                                                                                                                                             |
| [`factorial(x)`](#factorialx)                                          | See the `!` operator. Computes the product of the current integer and all integers below it.                                                                                                   |
| [`fdiv(x, y)`](#fdivx-y)                                               | Performs integer division (`x // y`) but returns a `DOUBLE` value.                                                                                                                             |
| [`floor(x)`](#floorx)                                                  | Rounds the number down.                                                                                                                                                                        |
| [`fmod(x, y)`](#fmodx-y)                                               | Calculates the modulo value. Always returns a `DOUBLE` value.                                                                                                                                  |
| [`gamma(x)`](#gammax)                                                  | Interpolation of the factorial of `x - 1`. Fractional inputs are allowed.                                                                                                                      |
| [`gcd(x, y)`](#gcdx-y)                                                 | Computes the greatest common divisor of `x` and `y`.                                                                                                                                           |
| [`greatest_common_divisor(x, y)`](#greatest_common_divisorx-y)         | Computes the greatest common divisor of `x` and `y`.                                                                                                                                           |
| [`greatest(x1, x2, ...)`](#greatestx1-x2-)                             | Selects the largest value.                                                                                                                                                                     |
| [`isfinite(x)`](#isfinitex)                                            | Returns true if the floating point value is finite, false otherwise.                                                                                                                           |
| [`isinf(x)`](#isinfx)                                                  | Returns true if the floating point value is infinite, false otherwise.                                                                                                                         |
| [`isnan(x)`](#isnanx)                                                  | Returns true if the floating point value is not a number, false otherwise.                                                                                                                     |
| [`lcm(x, y)`](#lcmx-y)                                                 | Computes the least common multiple of `x` and `y`.                                                                                                                                             |
| [`least_common_multiple(x, y)`](#least_common_multiplex-y)             | Computes the least common multiple of `x` and `y`.                                                                                                                                             |
| [`least(x1, x2, ...)`](#leastx1-x2-)                                   | Selects the smallest value.                                                                                                                                                                    |
| [`lgamma(x)`](#lgammax)                                                | Computes the log of the `gamma` function.                                                                                                                                                      |
| [`ln(x)`](#lnx)                                                        | Computes the natural logarithm of `x`.                                                                                                                                                         |
| [`log(x)`](#logx)                                                      | Computes the base-10 logarithm of `x`.                                                                                                                                                         |
| [`log10(x)`](#log10x)                                                  | Alias of `log`. Computes the base-10 logarithm of `x`.                                                                                                                                         |
| [`log2(x)`](#log2x)                                                    | Computes the base-2 log of `x`.                                                                                                                                                                |
| [`multiply(x, y)`](#multiplyx-y)                                       | Alias for `x * y`.                                                                                                                                                                             |
| [`nextafter(x, y)`](#nextafterx-y)                                     | Return the next floating point value after `x` in the direction of `y`.                                                                                                                        |
| [`pi()`](#pi)                                                          | Returns the value of pi.                                                                                                                                                                       |
| [`pow(x, y)`](#powx-y)                                                 | Computes `x` to the power of `y`.                                                                                                                                                              |
| [`power(x, y)`](#powerx-y)                                             | Alias of `pow`. Computes `x` to the power of `y`.                                                                                                                                              |
| [`radians(x)`](#radiansx)                                              | Converts degrees to radians.                                                                                                                                                                   |
| [`random()`](#random)                                                  | Returns a random number `x` in the range `0.0 <= x < 1.0`.                                                                                                                                     |
| [`round_even(v NUMERIC, s INTEGER)`](#round_evenv-numeric-s-integer)   | Alias of `roundbankers(v, s)`. Round to `s` decimal places using the [_rounding half to even_ rule](https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even). Values `s < 0` are allowed. |
| [`roundbankers(v NUMERIC, s INTEGER)`](#round_evenv-numeric-s-integer) | Alias of `round_even(v, s)`. Round to `s` decimal places using the [_rounding half to even_ rule](https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even). Values `s < 0` are allowed.   |
| [`round(v NUMERIC, s INTEGER)`](#roundv-numeric-s-integer)             | Round to `s` decimal places. Values `s < 0` are allowed.                                                                                                                                       |
| [`setseed(x)`](#setseedx)                                              | Sets the seed to be used for the random function.                                                                                                                                              |
| [`sign(x)`](#signx)                                                    | Returns the sign of `x` as -1, 0 or 1.                                                                                                                                                         |
| [`signbit(x)`](#signbitx)                                              | Returns whether the signbit is set or not.                                                                                                                                                     |
| [`sin(x)`](#sinx)                                                      | Computes the sin of `x`.                                                                                                                                                                       |
| [`sqrt(x)`](#sqrtx)                                                    | Returns the square root of the number.                                                                                                                                                         |
| [`subtract(x, y)`](#subtractx-y)                                       | Alias for `x - y`.                                                                                                                                                                             |
| [`tan(x)`](#tanx)                                                      | Computes the tangent of `x`.                                                                                                                                                                   |
| [`trunc(x)`](#truncx)                                                  | Truncates the number.                                                                                                                                                                          |
| [`xor(x, y)`](#xorx-y)                                                 | Bitwise XOR.                                                                                                                                                                                   |

#### `@(x)`

<div class="nostroke_table"></div>

| **Description** | Absolute value. Parentheses are optional if `x` is a column name. |
| :--- | :--- |
| **Example** | `@(-17.4)` |
| **Result** | `17.4` |
| **Alias** | `abs` |

#### `abs(x)`

Absolute value. Alias: `@`.

<SqlLogicTest id="sql/functions/numeric/abs" />

#### `acos(x)`

Computes the inverse cosine of `x`.

<SqlLogicTest id="sql/functions/numeric/acos" />

#### `acosh(x)`

Computes the inverse hyperbolic cosine of `x`.

<SqlLogicTest id="sql/functions/numeric/acosh" />

#### `add(x, y)`

Alias for `x + y`.

<SqlLogicTest id="sql/functions/numeric/add" />

#### `asin(x)`

Computes the inverse sine of `x`.

<SqlLogicTest id="sql/functions/numeric/asin" />

#### `asinh(x)`

Computes the inverse hyperbolic sine of `x`.

<SqlLogicTest id="sql/functions/numeric/asinh" />

#### `atan(x)`

Computes the inverse tangent of `x`.

<SqlLogicTest id="sql/functions/numeric/atan" />

#### `atanh(x)`

Computes the inverse hyperbolic tangent of `x`.

<SqlLogicTest id="sql/functions/numeric/atanh" />

#### `atan2(y, x)`

Computes the inverse tangent of `(y, x)`.

<SqlLogicTest id="sql/functions/numeric/atan2" />

#### `bit_count(x)`

Returns the number of bits that are set.

<SqlLogicTest id="sql/functions/numeric/bit_count" />

#### `cbrt(x)`

Returns the cube root of the number.

<SqlLogicTest id="sql/functions/numeric/cbrt" />

#### `ceil(x)`

Rounds the number up.

<SqlLogicTest id="sql/functions/numeric/ceil" />

#### `ceiling(x)`

Rounds the number up. Alias of `ceil`.

<SqlLogicTest id="sql/functions/numeric/ceiling" />

#### `cos(x)`

Computes the cosine of `x`.

<SqlLogicTest id="sql/functions/numeric/cos" />

#### `cot(x)`

Computes the cotangent of `x`.

<SqlLogicTest id="sql/functions/numeric/cot" />

#### `degrees(x)`

Converts radians to degrees.

<SqlLogicTest id="sql/functions/numeric/degrees" />

#### `divide(x, y)`

Alias for `x // y`.

<SqlLogicTest id="sql/functions/numeric/divide" />

#### `even(x)`

Round to next even number by rounding away from zero.

<SqlLogicTest id="sql/functions/numeric/even" />

#### `exp(x)`

Computes `e ** x`.

<SqlLogicTest id="sql/functions/numeric/exp" />

#### `factorial(x)`

See the `!` operator. Computes the product of the current integer and all integers below it.

<SqlLogicTest id="sql/functions/numeric/factorial" />

#### `fdiv(x, y)`

Performs integer division (`x // y`) but returns a `DOUBLE` value.

<SqlLogicTest id="sql/functions/numeric/fdiv" />

#### `floor(x)`

Rounds the number down.

<SqlLogicTest id="sql/functions/numeric/floor" />

#### `fmod(x, y)`

Calculates the modulo value. Always returns a `DOUBLE` value.

<SqlLogicTest id="sql/functions/numeric/fmod" />

#### `gamma(x)`

Interpolation of the factorial of `x - 1`. Fractional inputs are allowed.

<SqlLogicTest id="sql/functions/numeric/gamma" />

#### `gcd(x, y)`

Computes the greatest common divisor of `x` and `y`.

<SqlLogicTest id="sql/functions/numeric/gcd" />

#### `greatest_common_divisor(x, y)`

Computes the greatest common divisor of `x` and `y`.

<SqlLogicTest id="sql/functions/numeric/greatest_common_divisor" />

#### `greatest(x1, x2, ...)`

Selects the largest value.

<SqlLogicTest id="sql/functions/numeric/greatest" />

#### `isfinite(x)`

Returns true if the floating point value is finite, false otherwise.

<SqlLogicTest id="sql/functions/numeric/isfinite" />

#### `isinf(x)`

Returns true if the floating point value is infinite, false otherwise.

<SqlLogicTest id="sql/functions/numeric/isinf" />

#### `isnan(x)`

Returns true if the floating point value is not a number, false otherwise.

<SqlLogicTest id="sql/functions/numeric/isnan" />

#### `lcm(x, y)`

Computes the least common multiple of `x` and `y`.

<SqlLogicTest id="sql/functions/numeric/lcm" />

#### `least_common_multiple(x, y)`

Computes the least common multiple of `x` and `y`.

<SqlLogicTest id="sql/functions/numeric/least_common_multiple" />

#### `least(x1, x2, ...)`

Selects the smallest value.

<SqlLogicTest id="sql/functions/numeric/least" />

#### `lgamma(x)`

Computes the log of the `gamma` function.

<SqlLogicTest id="sql/functions/numeric/lgamma" />

#### `ln(x)`

Computes the natural logarithm of `x`.

<SqlLogicTest id="sql/functions/numeric/ln" />

#### `log(x)`

Computes the base-10 logarithm of `x`.

<SqlLogicTest id="sql/functions/numeric/log" />

#### `log10(x)`

Alias of `log`. Computes the base-10 logarithm of `x`.

<SqlLogicTest id="sql/functions/numeric/log10" />

#### `log2(x)`

Computes the base-2 log of `x`.

<SqlLogicTest id="sql/functions/numeric/log2" />

#### `multiply(x, y)`

Alias for `x * y`.

<SqlLogicTest id="sql/functions/numeric/multiply" />

#### `nextafter(x, y)`

Return the next floating point value after `x` in the direction of `y`.

<SqlLogicTest id="sql/functions/numeric/nextafter" />

#### `pi()`

Returns the value of pi.

<SqlLogicTest id="sql/functions/numeric/pi" />

#### `pow(x, y)`

Computes `x` to the power of `y`.

<SqlLogicTest id="sql/functions/numeric/pow" />

#### `power(x, y)`

Alias of `pow`. Computes `x` to the power of `y`.

<SqlLogicTest id="sql/functions/numeric/power" />

#### `radians(x)`

Converts degrees to radians.

<SqlLogicTest id="sql/functions/numeric/radians" />

#### `random()`

<div class="nostroke_table"></div>

| **Description** | Returns a random number `x` in the range `0.0 <= x < 1.0`. |
| :--- | :--- |
| **Example** | `random()` |
| **Result** | various |

#### `round_even(v NUMERIC, s INTEGER)`

Alias of `roundbankers(v, s)`. Round to `s` decimal places using the [_rounding half to even_ rule](https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even). Values `s < 0` are allowed.

<SqlLogicTest id="sql/functions/numeric/round_even" />

#### `roundbankers(v NUMERIC, s INTEGER)`

Alias of `round_even(v, s)`. Round to `s` decimal places using the [_rounding half to even_ rule](https://en.wikipedia.org/wiki/Rounding#Rounding_half_to_even). Values `s < 0` are allowed.

<SqlLogicTest id="sql/functions/numeric/roundbankers" />

#### `round(v NUMERIC, s INTEGER)`

Round to `s` decimal places. Values `s < 0` are allowed.

<SqlLogicTest id="sql/functions/numeric/round" />

#### `setseed(x)`

<div class="nostroke_table"></div>

| **Description** | Sets the seed to be used for the random function. |
| :--- | :--- |
| **Example** | `setseed(0.42)` |

#### `sign(x)`

Returns the sign of `x` as -1, 0 or 1.

<SqlLogicTest id="sql/functions/numeric/sign" />

#### `signbit(x)`

Returns whether the signbit is set or not.

<SqlLogicTest id="sql/functions/numeric/signbit" />

#### `sin(x)`

Computes the sin of `x`.

<SqlLogicTest id="sql/functions/numeric/sin" />

#### `sqrt(x)`

Returns the square root of the number.

<SqlLogicTest id="sql/functions/numeric/sqrt" />

#### `subtract(x, y)`

Alias for `x - y`.

<SqlLogicTest id="sql/functions/numeric/subtract" />

#### `tan(x)`

Computes the tangent of `x`.

<SqlLogicTest id="sql/functions/numeric/tan" />

#### `trunc(x)`

Truncates the number.

<SqlLogicTest id="sql/functions/numeric/trunc" />

#### `xor(x, y)`

Bitwise XOR.

<SqlLogicTest id="sql/functions/numeric/xor" />
