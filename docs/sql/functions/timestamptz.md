---
title: Timestamp with Time Zone Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`TIMESTAMP WITH TIME ZONE`
(or `TIMESTAMPTZ`) values](../../sql/data_types/timestamp.md). See also the related [`TIMESTAMP` functions](../../sql/functions/timestamp.md).

In the examples below, the current time zone is presumed to be `America/Los_Angeles`
using the Gregorian calendar.

## Built-In Timestamp with Time Zone Functions

The table below shows the available scalar functions for `TIMESTAMPTZ` values.
Since these functions do not involve binning or display,
they are always available.

| Name                                                                     | Description                                                                |
| :----------------------------------------------------------------------- | :------------------------------------------------------------------------- |
| `current_timestamp`                                | Current date and time (start of current transaction).                      |
| `get_current_timestamp()`                      | Current date and time (start of current transaction).                      |
| [`greatest(timestamptz, timestamptz)`](#greatesttimestamptz-timestamptz) | The later of two timestamps.                                               |
| [`isfinite(timestamptz)`](#isfinitetimestamptz)                          | Returns true if the timestamp with time zone is finite, false otherwise.   |
| [`isinf(timestamptz)`](#isinftimestamptz)                                | Returns true if the timestamp with time zone is infinite, false otherwise. |
| [`least(timestamptz, timestamptz)`](#leasttimestamptz-timestamptz)       | The earlier of two timestamps.                                             |
| `now()`                                                          | Current date and time (start of current transaction).                      |
| [`timetz_byte_comparable(timetz)`](#timetz_byte_comparabletimetz)        | Converts a `TIME WITH TIME ZONE` to a `UBIGINT` sort key.                  |
| [`to_timestamp(double)`](#to_timestampdouble)                            | Converts seconds since the epoch to a timestamp with time zone.            |
| `transaction_timestamp()`                      | Current date and time (start of current transaction).                      |

#### `current_timestamp`

<div class="nostroke_table"></div>

| **Description** | Current date and time (start of current transaction). |
| :--- | :--- |
| **Example** | `current_timestamp` |
| **Result** | `2022-10-08 12:44:46.122-07` |

#### `get_current_timestamp()`

<div class="nostroke_table"></div>

| **Description** | Current date and time (start of current transaction). |
| :--- | :--- |
| **Example** | `get_current_timestamp()` |
| **Result** | `2022-10-08 12:44:46.122-07` |

#### `greatest(timestamptz, timestamptz)`

The later of two timestamps.

<SqlLogicTest id="sql/functions/timestamptz/greatest" />

#### `isfinite(timestamptz)`

Returns true if the timestamp with time zone is finite, false otherwise.

<SqlLogicTest id="sql/functions/timestamptz/isfinite" />

#### `isinf(timestamptz)`

Returns true if the timestamp with time zone is infinite, false otherwise.

<SqlLogicTest id="sql/functions/timestamptz/isinf" />

#### `least(timestamptz, timestamptz)`

The earlier of two timestamps.

<SqlLogicTest id="sql/functions/timestamptz/least" />

#### `now()`

<div class="nostroke_table"></div>

| **Description** | Current date and time (start of current transaction). |
| :--- | :--- |
| **Example** | `now()` |
| **Result** | `2022-10-08 12:44:46.122-07` |

#### `timetz_byte_comparable(timetz)`

Converts a `TIME WITH TIME ZONE` to a `UBIGINT` sort key.

<SqlLogicTest id="sql/functions/timestamptz/timetz_byte_comparable" />

#### `to_timestamp(double)`

Converts seconds since the epoch to a timestamp with time zone.

<SqlLogicTest id="sql/functions/timestamptz/to_timestamp" />

#### `transaction_timestamp()`

<div class="nostroke_table"></div>

| **Description** | Current date and time (start of current transaction). |
| :--- | :--- |
| **Example** | `transaction_timestamp()` |
| **Result** | `2022-10-08 12:44:46.122-07` |

## Timestamp with Time Zone Strings

`TIMESTAMPTZ` values are cast to and from strings using offset notation.
This will let you specify an instant correctly without access to time zone information.
For portability, `TIMESTAMPTZ` values will always be displayed using GMT offsets:

<SqlLogicTest id="sql/functions/timestamptz/example_001" />

Named time zone parsing (such as parsing a time zone name from a string and casting it to a representation in the local time zone) relies on ICU time zone support.

## ICU Timestamp with Time Zone Operators

The table below shows the available mathematical operators for `TIMESTAMP WITH TIME ZONE` values. These operators rely on ICU time zone support.

| Operator | Description                   | Example                                               | Result                |
| :------- | :---------------------------- | :---------------------------------------------------- | :-------------------- |
| `+`      | addition of an `INTERVAL`     | `TIMESTAMPTZ '1992-03-22 01:02:03' + INTERVAL 5 DAY`  | `1992-03-27 01:02:03` |
| `-`      | subtraction of `TIMESTAMPTZ`s | `TIMESTAMPTZ '1992-03-27' - TIMESTAMPTZ '1992-03-22'` | `5 days`              |
| `-`      | subtraction of an `INTERVAL`  | `TIMESTAMPTZ '1992-03-27 01:02:03' - INTERVAL 5 DAY`  | `1992-03-22 01:02:03` |

Adding to or subtracting from [infinite values](../../sql/data_types/timestamp.md#special-values) produces the same infinite value.

Addition and subtraction of intervals uses the [ICU Calendar add function](https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/classicu_1_1Calendar.html#aa6e19a88ca2225eddcbbe82313c9c095).
For positive intervals (forwards in time) the fields are incremented from least to most significant.
For negative intervals (backwards in time) the fields are decremented from most to least significant.
This produces the same results as Postgres, but does not match some [more recent calendar RFCs](https://www.rfc-editor.org/rfc/rfc5545).

## ICU Timestamp with Time Zone Functions

The table below shows the ICU scalar functions for `TIMESTAMP WITH TIME ZONE` values. These functions rely on ICU time zone support.

| Name                                                                                                                                            | Description                                                                                                                                                                                                                                                                                                                                                          |
| :---------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `age(timestamptz, timestamptz)`                                                                                  | Subtract arguments, resulting in the time difference between the two timestamps.                                                                                                                                                                                                                                                                                     |
| `age(timestamptz)`                                                                                                           | Subtract from current_date.                                                                                                                                                                                                                                                                                                                                          |
| `date_diff(part, starttimestamptz, endtimestamptz)`                                           | The number of [`part`](../../sql/functions/datepart.md) boundaries between `starttimestamptz` and `endtimestamptz` inclusive of the larger timestamp and exclusive of the smaller timestamp.                                                                                                                                                                         |
| `date_part([part, ...], timestamp)`                                                                              | Get the listed [subfields](../../sql/functions/datepart.md) as a `struct`. The list must be constant.                                                                                                                                                                                                                                                                |
| `date_part(part, timestamp)`                                                                                      | Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`).                                                                                                                                                                                                                                                                                           |
| `date_sub(part, starttimestamptz, endtimestamptz)`                                             | The signed length of the interval between `starttimestamptz` and `endtimestamptz`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md).                                                                                                                                                                                                        |
| `date_trunc(part, timestamptz)`                                                                                  | Truncate to specified [precision](../../sql/functions/datepart.md).                                                                                                                                                                                                                                                                                                  |
| [`epoch_ns(timestamptz)`](#epoch_nstimestamptz)                                                                                                 | Converts a timestamptz to nanoseconds since the epoch.                                                                                                                                                                                                                                                                                                               |
| [`epoch_us(timestamptz)`](#epoch_ustimestamptz)                                                                                                 | Converts a timestamptz to microseconds since the epoch.                                                                                                                                                                                                                                                                                                              |
| `extract(field FROM timestamptz)`                                                                             | Get [subfield](../../sql/functions/datepart.md) from a `TIMESTAMP WITH TIME ZONE`.                                                                                                                                                                                                                                                                                   |
| `last_day(timestamptz)`                                                                                                 | The last day of the month.                                                                                                                                                                                                                                                                                                                                           |
| `make_timestamptz(bigint, bigint, bigint, bigint, bigint, double, string)` | The `TIMESTAMP WITH TIME ZONE` for the given parts and time zone.                                                                                                                                                                                                                                                                                                    |
| `make_timestamptz(bigint, bigint, bigint, bigint, bigint, double)`                | The `TIMESTAMP WITH TIME ZONE` for the given parts in the current time zone.                                                                                                                                                                                                                                                                                         |
| `make_timestamptz(microseconds)`                                                                               | The `TIMESTAMP WITH TIME ZONE` for the given µs since the epoch.                                                                                                                                                                                                                                                                                                     |
| [`strftime(timestamptz, format)`](#strftimetimestamptz-format)                                                                                  | Converts a `TIMESTAMP WITH TIME ZONE` value to string according to the [format string](../../sql/functions/dateformat.md#format-specifiers).                                                                                                                                                                                                                         |
| [`strptime(text, format)`](#strptimetext-format)                                                                                                | Parses string to a `TIMESTAMP WITH TIME ZONE` if a `%Z` element is present in the format, otherwise to a `TIMESTAMP`, according to the [format string](../../sql/functions/dateformat.md#format-specifiers).                                                                                                                                                                                                            |
| `time_bucket(bucket_width, timestamptz[, offset])`                                               | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at `2000-01-01 00:00:00+00:00[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00+00:00[ + offset]`. Note that `2000-01-03` is a Monday.                                                                                                |
| `time_bucket(bucket_width, timestamptz[, origin])`                                               | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01 00:00:00+00:00` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00+00:00`. Note that `2000-01-03` is a Monday.                                                                            |
| `time_bucket(bucket_width, timestamptz[, timezone])`                                             | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01 00:00:00` in the provided `timezone` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00` in the provided `timezone`. The default timezone is `'UTC'`. Note that `2000-01-03` is a Monday. |

#### `age(timestamptz, timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Subtract arguments, resulting in the time difference between the two timestamps. |
| :--- | :--- |
| **Example** | `age(TIMESTAMPTZ '2001-04-10', TIMESTAMPTZ '1992-09-20')` |
| **Result** | `8 years 6 months 20 days` |

#### `age(timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Subtract from current_date. |
| :--- | :--- |
| **Example** | `age(TIMESTAMP '1992-09-20')` |
| **Result** | `29 years 1 month 27 days 12:39:00.844` |

#### `date_diff(part, starttimestamptz, endtimestamptz)`

<div class="nostroke_table"></div>

| **Description** | The signed number of [`part`](../../sql/functions/datepart.md) boundaries between `starttimestamptz` and `endtimestamptz`, inclusive of the larger timestamp and exclusive of the smaller timestamp. |
| :--- | :--- |
| **Example** | `date_diff('hour', TIMESTAMPTZ '1992-09-30 23:59:59', TIMESTAMPTZ '1992-10-01 01:58:00')` |
| **Result** | `2` |

#### `date_part([part, ...], timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Get the listed [subfields](../../sql/functions/datepart.md) as a `struct`. The list must be constant. |
| :--- | :--- |
| **Example** | `date_part(['year', 'month', 'day'], TIMESTAMPTZ '1992-09-20 20:38:40-07')` |
| **Result** | `{year: 1992, month: 9, day: 20}` |

#### `date_part(part, timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Get [subfield](../../sql/functions/datepart.md) (equivalent to _extract_). |
| :--- | :--- |
| **Example** | `date_part('minute', TIMESTAMPTZ '1992-09-20 20:38:40')` |
| **Result** | `38` |

#### `date_sub(part, starttimestamptz, endtimestamptz)`

<div class="nostroke_table"></div>

| **Description** | The signed length of the interval between `starttimestamptz` and `endtimestamptz`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md). |
| :--- | :--- |
| **Example** | `date_sub('hour', TIMESTAMPTZ '1992-09-30 23:59:59', TIMESTAMPTZ '1992-10-01 01:58:00')` |
| **Result** | `1` |

#### `date_trunc(part, timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Truncate to specified [precision](../../sql/functions/datepart.md). |
| :--- | :--- |
| **Example** | `date_trunc('hour', TIMESTAMPTZ '1992-09-20 20:38:40')` |
| **Result** | `1992-09-20 20:00:00` |

#### `epoch_ns(timestamptz)`

Converts a timestamptz to nanoseconds since the epoch.

<SqlLogicTest id="sql/functions/timestamptz/epoch_ns" />

#### `epoch_us(timestamptz)`

Converts a timestamptz to microseconds since the epoch.

<SqlLogicTest id="sql/functions/timestamptz/epoch_us" />

#### `extract(field FROM timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Get [subfield](../../sql/functions/datepart.md) from a `TIMESTAMP WITH TIME ZONE`. |
| :--- | :--- |
| **Example** | `extract('hour' FROM TIMESTAMPTZ '1992-09-20 20:38:48')` |
| **Result** | `20` |

#### `last_day(timestamptz)`

<div class="nostroke_table"></div>

| **Description** | The last day of the month. |
| :--- | :--- |
| **Example** | `last_day(TIMESTAMPTZ '1992-03-22 01:02:03.1234')` |
| **Result** | `1992-03-31` |

#### `make_timestamptz(bigint, bigint, bigint, bigint, bigint, double, string)`

<div class="nostroke_table"></div>

| **Description** | The `TIMESTAMP WITH TIME ZONE` for the given parts and time zone. |
| :--- | :--- |
| **Example** | `make_timestamptz(1992, 9, 20, 15, 34, 27.123456, 'CET')` |
| **Result** | `1992-09-20 06:34:27.123456-07` |

#### `make_timestamptz(bigint, bigint, bigint, bigint, bigint, double)`

<div class="nostroke_table"></div>

| **Description** | The `TIMESTAMP WITH TIME ZONE` for the given parts in the current time zone. |
| :--- | :--- |
| **Example** | `make_timestamptz(1992, 9, 20, 13, 34, 27.123456)` |
| **Result** | `1992-09-20 13:34:27.123456-07` |

#### `make_timestamptz(microseconds)`

<div class="nostroke_table"></div>

| **Description** | The `TIMESTAMP WITH TIME ZONE` for the given µs since the epoch. |
| :--- | :--- |
| **Example** | `make_timestamptz(1667810584123456)` |
| **Result** | `2022-11-07 16:43:04.123456-08` |

#### `strftime(timestamptz, format)`

Converts a `TIMESTAMP WITH TIME ZONE` value to string according to the [format string](../../sql/functions/dateformat.md#format-specifiers).

<SqlLogicTest id="sql/functions/timestamptz/strftime" />

#### `strptime(text, format)`

Parses string to a `TIMESTAMP` according to the [format string](../../sql/functions/dateformat.md#format-specifiers). When the format contains a `%Z` element the result is a `TIMESTAMP WITH TIME ZONE` instead.

<SqlLogicTest id="sql/functions/timestamptz/strptime" />

#### `time_bucket(bucket_width, timestamptz[, offset])`

<div class="nostroke_table"></div>

| **Description** | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at `2000-01-01 00:00:00+00:00[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00+00:00[ + offset]`. Note that `2000-01-03` is a Monday. |
| :--- | :--- |
| **Example** | `time_bucket(INTERVAL '10 minutes', TIMESTAMPTZ '1992-04-20 15:26:00-07', INTERVAL '5 minutes')` |
| **Result** | `1992-04-20 15:25:00-07` |

#### `time_bucket(bucket_width, timestamptz[, origin])`

<div class="nostroke_table"></div>

| **Description** | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01 00:00:00+00:00` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00+00:00`. Note that `2000-01-03` is a Monday. |
| :--- | :--- |
| **Example** | `time_bucket(INTERVAL '2 weeks', TIMESTAMPTZ '1992-04-20 15:26:00-07', TIMESTAMPTZ '1992-04-01 00:00:00-07')` |
| **Result** | `1992-04-15 00:00:00-07` |

#### `time_bucket(bucket_width, timestamptz[, timezone])`

<div class="nostroke_table"></div>

| **Description** | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01 00:00:00` in the provided `timezone` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00` in the provided `timezone`. The default timezone is `'UTC'`. Note that `2000-01-03` is a Monday. |
| :--- | :--- |
| **Example** | `time_bucket(INTERVAL '2 days', TIMESTAMPTZ '1992-04-20 15:26:00-07', 'Europe/Berlin')` |
| **Result** | `1992-04-19 15:00:00-07` (=`1992-04-20 00:00:00 Europe/Berlin`) |

There are also dedicated extraction functions to get the [subfields](../../sql/functions/datepart.md).

## ICU Timestamp Table Functions

The table below shows the available table functions for `TIMESTAMP WITH TIME ZONE` types.

| Name                                                                                                      | Description                                                                                                                                                   |
| :-------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `generate_series(timestamptz, timestamptz, interval)` | Generate a table of timestamps in the closed range (including both the starting timestamp and the ending timestamp), stepping by the interval.                |
| `range(timestamptz, timestamptz, interval)`                     | Generate a table of timestamps in the half open range (including the starting timestamp, but stopping before the ending timestamp), stepping by the interval. |

<DocCallout type="tip">

Infinite values are not allowed as table function bounds.

</DocCallout>

#### `generate_series(timestamptz, timestamptz, interval)`

<div class="nostroke_table"></div>

| **Description** | Generate a table of timestamps in the closed range (including both the starting timestamp and the ending timestamp), stepping by the interval. |
| :--- | :--- |
| **Example** | `generate_series(TIMESTAMPTZ '2001-04-10', TIMESTAMPTZ '2001-04-11', INTERVAL 30 MINUTE)` |

#### `range(timestamptz, timestamptz, interval)`

<div class="nostroke_table"></div>

| **Description** | Generate a table of timestamps in the half open range (including the starting timestamp, but stopping before the ending timestamp), stepping by the interval. |
| :--- | :--- |
| **Example** | `range(TIMESTAMPTZ '2001-04-10', TIMESTAMPTZ '2001-04-11', INTERVAL 30 MINUTE)` |

## ICU Timestamp Without Time Zone Functions

The table below shows the ICU scalar functions that operate on plain `TIMESTAMP` values. These functions rely on ICU time zone support.
These functions assume that the `TIMESTAMP` is a “local timestamp”.

A local timestamp is effectively a way of encoding the part values from a time zone into a single value.
They should be used with caution because the produced values can contain gaps and ambiguities thanks to daylight savings time.
Often the same functionality can be implemented more reliably using the `struct` variant of the `date_part` function.

| Name                                                       | Description                                                                                                                                                                 |
| :--------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `current_localtime()`                | Returns a `TIME` whose GMT bin values correspond to local time in the current time zone.                                                                                    |
| `current_localtimestamp()`      | Returns a `TIMESTAMP` whose GMT bin values correspond to local date and time in the current time zone.                                                                      |
| `localtime`                                  | Synonym for the `current_localtime()` function call.                                                                                                                        |
| `localtimestamp`                        | Synonym for the `current_localtimestamp()` function call.                                                                                                                   |
| `timezone(text, timestamp)`     | Use the [date parts](../../sql/functions/datepart.md) of the timestamp in GMT to construct a timestamp in the given time zone. Effectively, the argument is a “local” time. |
| `timezone(text, timestamptz)` | Use the [date parts](../../sql/functions/datepart.md) of the timestamp in the given time zone to construct a timestamp. Effectively, the result is a “local” time.          |

#### `current_localtime()`

<div class="nostroke_table"></div>

| **Description** | Returns a `TIME` whose GMT bin values correspond to local time in the current time zone. |
| :--- | :--- |
| **Example** | `current_localtime()` |
| **Result** | `08:47:56.497` |

#### `current_localtimestamp()`

<div class="nostroke_table"></div>

| **Description** | Returns a `TIMESTAMP` whose GMT bin values correspond to local date and time in the current time zone. |
| :--- | :--- |
| **Example** | `current_localtimestamp()` |
| **Result** | `2022-12-17 08:47:56.497` |

#### `localtime`

<div class="nostroke_table"></div>

| **Description** | Synonym for the `current_localtime()` function call. |
| :--- | :--- |
| **Example** | `localtime` |
| **Result** | `08:47:56.497` |

#### `localtimestamp`

<div class="nostroke_table"></div>

| **Description** | Synonym for the `current_localtimestamp()` function call. |
| :--- | :--- |
| **Example** | `localtimestamp` |
| **Result** | `2022-12-17 08:47:56.497` |

#### `timezone(text, timestamp)`

<div class="nostroke_table"></div>

| **Description** | Use the [date parts](../../sql/functions/datepart.md) of the timestamp in GMT to construct a timestamp in the given time zone. Effectively, the argument is a “local” time. |
| :--- | :--- |
| **Example** | `timezone('America/Denver', TIMESTAMP '2001-02-16 20:38:40')` |
| **Result** | `2001-02-16 19:38:40-08` |

#### `timezone(text, timestamptz)`

<div class="nostroke_table"></div>

| **Description** | Use the [date parts](../../sql/functions/datepart.md) of the timestamp in the given time zone to construct a timestamp. Effectively, the result is a “local” time. |
| :--- | :--- |
| **Example** | `timezone('America/Denver', TIMESTAMPTZ '2001-02-16 20:38:40-05')` |
| **Result** | `2001-02-16 18:38:40` |

## At Time Zone

The `AT TIME ZONE` syntax is syntactic sugar for the (two argument) `timezone` function listed above. Like that function, it relies on ICU time zone support:

<SqlLogicTest id="sql/functions/timestamptz/example_003" />

The `TIMESTAMP WITH TIME ZONE` spelling of the input type is also not accepted by the parser in this build:

<SqlLogicTest id="sql/functions/timestamptz/example_004" />

Numeric timezones are not allowed either:

<SqlLogicTest id="sql/functions/timestamptz/example_005" />

## Infinities

Functions applied to infinite dates will either return the same infinite dates
(e.g., `greatest`) or `NULL` (e.g., `date_part`) depending on what “makes sense”.
In general, if the function needs to examine the parts of the infinite temporal value,
the result will be `NULL`.

## Calendars

ICU also supports [non-Gregorian calendars](../../sql/data_types/timestamp.md#calendar-support).
If such a calendar is current, then the display and binning operations will use that calendar.

### Daylight Saving Time (DST) Transitions

Adding calendar intervals such as `INTERVAL '1 day'` to a
`TIMESTAMPTZ` uses the ICU time zone operators described above.
The interval is added in terms of the calendar fields, so the result
adjusts around a daylight saving time transition rather than simply
adding a fixed number of hours:

<SqlLogicTest id="sql/functions/timestamptz/example_006" />
