---
title: Timestamp Functions
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`TIMESTAMP` values](../../sql/data_types/timestamp.md).
See also the related [`TIMESTAMPTZ` functions](../../sql/functions/timestamptz.md).

## Timestamp Operators

The table below shows the available mathematical operators for `TIMESTAMP` types.

| Operator | Description                  | Example                                            | Result                |
| :------- | :--------------------------- | :------------------------------------------------- | :-------------------- |
| `+`      | addition of an `INTERVAL`    | `TIMESTAMP '1992-03-22 01:02:03' + INTERVAL 5 DAY` | `1992-03-27 01:02:03` |
| `-`      | subtraction of `TIMESTAMP`s  | `TIMESTAMP '1992-03-27' - TIMESTAMP '1992-03-22'`  | `5 days`              |
| `-`      | subtraction of an `INTERVAL` | `TIMESTAMP '1992-03-27 01:02:03' - INTERVAL 5 DAY` | `1992-03-22 01:02:03` |

Adding to or subtracting from [infinite values](../../sql/data_types/timestamp.md#special-values) produces the same infinite value.

## Scalar Timestamp Functions

The table below shows the available scalar functions for `TIMESTAMP` values.

| Name                                                                                                                         | Description                                                                                                                                                                                                                                                                 |
| :--------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`age(timestamp, timestamp)`](#agetimestamp-timestamp)                                                                       | Subtract arguments, resulting in the time difference between the two timestamps.                                                                                                                                                                                            |
| `age(timestamp)`                                                                                            | Subtract from current_date.                                                                                                                                                                                                                                                 |
| `ago(interval)`                                                                                              | Subtracts an interval from the current timestamp.                                                                                                                                                                                                                           |
| [`century(timestamp)`](#centurytimestamp)                                                                                    | Extracts the century of a timestamp.                                                                                                                                                                                                                                        |
| `current_localtimestamp()`                                                                        | Returns the current timestamp (at the start of the transaction).                                                                                                                                                                                                            |
| [`date_diff(part, starttimestamp, endtimestamp)`](#date_diffpart-starttimestamp-endtimestamp)                                | The number of [`part`](../../sql/functions/datepart.md) boundaries between `starttimestamp` and `endtimestamp`, inclusive of the larger timestamp and exclusive of the smaller timestamp.                                                                                   |
| [`date_part([part, ...], timestamp)`](#date_partpart--timestamp)                                                             | Get the listed [subfields](../../sql/functions/datepart.md) as a `struct`. The list must be constant.                                                                                                                                                                       |
| [`date_part(part, timestamp)`](#date_partpart-timestamp)                                                                     | Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`).                                                                                                                                                                                                  |
| [`date_sub(part, starttimestamp, endtimestamp)`](#date_subpart-starttimestamp-endtimestamp)                                  | The signed length of the interval between `starttimestamp` and `endtimestamp`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md).                                                                                                                   |
| [`date_trunc(part, timestamp)`](#date_truncpart-timestamp)                                                                   | Truncate to specified [precision](../../sql/functions/datepart.md).                                                                                                                                                                                                         |
| [`dayname(timestamp)`](#daynametimestamp)                                                                                    | The (English) name of the weekday.                                                                                                                                                                                                                                          |
| [`epoch_ms(timestamp)`](#epoch_mstimestamp)                                                                                  | Returns the total number of milliseconds since the epoch.                                                                                                                                                                                                                   |
| [`epoch_ns(timestamp)`](#epoch_nstimestamp)                                                                                  | Returns the total number of nanoseconds since the epoch.                                                                                                                                                                                                                    |
| [`epoch_us(timestamp)`](#epoch_ustimestamp)                                                                                  | Returns the total number of microseconds since the epoch.                                                                                                                                                                                                                   |
| [`epoch(timestamp)`](#epochtimestamp)                                                                                        | Returns the total number of seconds since the epoch.                                                                                                                                                                                                                        |
| [`extract(field FROM timestamp)`](#extractfield-from-timestamp)                                                              | Get [subfield](../../sql/functions/datepart.md) from a timestamp.                                                                                                                                                                                                           |
| [`greatest(timestamp, timestamp)`](#greatesttimestamp-timestamp)                                                             | The later of two timestamps.                                                                                                                                                                                                                                                |
| [`isfinite(timestamp)`](#isfinitetimestamp)                                                                                  | Returns true if the timestamp is finite, false otherwise.                                                                                                                                                                                                                   |
| [`isinf(timestamp)`](#isinftimestamp)                                                                                        | Returns true if the timestamp is infinite, false otherwise.                                                                                                                                                                                                                 |
| [`julian(timestamp)`](#juliantimestamp)                                                                                      | Extract the Julian Day number from a timestamp.                                                                                                                                                                                                                             |
| [`last_day(timestamp)`](#last_daytimestamp)                                                                                  | The last day of the month.                                                                                                                                                                                                                                                  |
| [`least(timestamp, timestamp)`](#leasttimestamp-timestamp)                                                                   | The earlier of two timestamps.                                                                                                                                                                                                                                              |
| [`make_timestamp(bigint, bigint, bigint, bigint, bigint, double)`](#make_timestampbigint-bigint-bigint-bigint-bigint-double) | The timestamp for the given parts.                                                                                                                                                                                                                                          |
| [`make_timestamp(microseconds)`](#make_timestampmicroseconds)                                                                | Converts microseconds since the epoch to a timestamp.                                                                                                                                                                                                                       |
| [`make_timestamp_ms(milliseconds)`](#make_timestamp_msmilliseconds)                                                          | Converts milliseconds since the epoch to a timestamp.                                                                                                                                                                                                                       |
| [`make_timestamp_ns(nanoseconds)`](#make_timestamp_nsnanoseconds)                                                            | Converts nanoseconds since the epoch to a timestamp.                                                                                                                                                                                                                        |
| [`monthname(timestamp)`](#monthnametimestamp)                                                                                | The (English) name of the month.                                                                                                                                                                                                                                            |
| [`strftime(timestamp, format)`](#strftimetimestamp-format)                                                                   | Converts timestamp to string according to the [format string](../../sql/functions/dateformat.md#format-specifiers).                                                                                                                                                         |
| [`strptime(text, format-list)`](#strptimetext-format-list)                                                                   | Converts the string `text` to timestamp applying the [format strings](../../sql/functions/dateformat.md) in the list until one succeeds. Throws an error on failure. To return `NULL` on failure, use [`try_strptime`](#try_strptimetext-format-list).                      |
| [`strptime(text, format)`](#strptimetext-format)                                                                             | Converts the string `text` to timestamp according to the [format string](../../sql/functions/dateformat.md#format-specifiers). Throws an error on failure. To return `NULL` on failure, use [`try_strptime`](#try_strptimetext-format).                                     |
| [`time_bucket(bucket_width, timestamp[, offset])`](#time_bucketbucket_width-timestamp-offset)                                | Truncate `timestamp` to a grid of width `bucket_width`. The grid is anchored at `2000-01-01 00:00:00[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00[ + offset]`. Note that `2000-01-03` is a Monday.                     |
| [`time_bucket(bucket_width, timestamp[, origin])`](#time_bucketbucket_width-timestamp-origin)                                | Truncate `timestamp` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01 00:00:00` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00`. Note that `2000-01-03` is a Monday. |
| [`try_strptime(text, format-list)`](#try_strptimetext-format-list)                                                           | Converts the string `text` to timestamp applying the [format strings](../../sql/functions/dateformat.md) in the list until one succeeds. Returns `NULL` on failure.                                                                                                         |
| [`try_strptime(text, format)`](#try_strptimetext-format)                                                                     | Converts the string `text` to timestamp according to the [format string](../../sql/functions/dateformat.md#format-specifiers). Returns `NULL` on failure.                                                                                                                   |

There are also dedicated extraction functions to get the [subfields](../../sql/functions/datepart.md).

Functions applied to infinite dates will either return the same infinite dates
(e.g., `greatest`) or `NULL` (e.g., `date_part`) depending on what “makes sense”.
In general, if the function needs to examine the parts of the infinite date, the result will be `NULL`.

#### `age(timestamp, timestamp)`

Subtract arguments, resulting in the time difference between the two timestamps.

<SqlLogicTest id="sql/functions/timestamp/age_two" />

#### `age(timestamp)`

<div class="nostroke_table"></div>

| **Description** | Subtract from current_date. |
| :--- | :--- |
| **Example** | `age(TIMESTAMP '1992-09-20')` |
| **Result** | `29 years 1 month 27 days 12:39:00.844` |

#### `ago(interval)`

<div class="nostroke_table"></div>

| **Description** | Subtracts an interval from the current timestamp, returning a timestamp in the past. Equivalent to `current_timestamp - interval`. |
| :--- | :--- |
| **Example** | `ago(INTERVAL 1 HOUR)` |
| **Result** | `2024-11-30 12:28:48.895` (if current time is `2024-11-30 13:28:48.895`) |

#### `century(timestamp)`

Extracts the century of a timestamp.

<SqlLogicTest id="sql/functions/timestamp/century" />

#### `current_localtimestamp()`

<div class="nostroke_table"></div>

| **Description** | Returns the current timestamp with time zone (at the start of the transaction). |
| :--- | :--- |
| **Example** | `current_localtimestamp()` |
| **Result** | `2024-11-30 13:28:48.895` |

#### `date_diff(part, starttimestamp, endtimestamp)`

The signed number of [`part`](../../sql/functions/datepart.md) boundaries between `starttimestamp` and `endtimestamp`, inclusive of the larger timestamp and exclusive of the smaller timestamp.

<SqlLogicTest id="sql/functions/timestamp/date_diff" />

#### `date_part([part, ...], timestamp)`

Get the listed [subfields](../../sql/functions/datepart.md) as a `struct`. The list must be constant.

<SqlLogicTest id="sql/functions/timestamp/date_part_list" />

#### `date_part(part, timestamp)`

Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`).

<SqlLogicTest id="sql/functions/timestamp/date_part" />

#### `date_sub(part, starttimestamp, endtimestamp)`

The signed length of the interval between `starttimestamp` and `endtimestamp`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md).

<SqlLogicTest id="sql/functions/timestamp/date_sub" />

#### `date_trunc(part, timestamp)`

Truncate to specified [precision](../../sql/functions/datepart.md).

<SqlLogicTest id="sql/functions/timestamp/date_trunc" />

#### `dayname(timestamp)`

The (English) name of the weekday.

<SqlLogicTest id="sql/functions/timestamp/dayname" />

#### `epoch_ms(timestamp)`

Returns the total number of milliseconds since the epoch.

<SqlLogicTest id="sql/functions/timestamp/epoch_ms" />

#### `epoch_ns(timestamp)`

Returns the total number of nanoseconds since the epoch.

<SqlLogicTest id="sql/functions/timestamp/epoch_ns" />

#### `epoch_us(timestamp)`

Returns the total number of microseconds since the epoch.

<SqlLogicTest id="sql/functions/timestamp/epoch_us" />

#### `epoch(timestamp)`

Returns the total number of seconds since the epoch.

<SqlLogicTest id="sql/functions/timestamp/epoch" />

#### `extract(field FROM timestamp)`

Get [subfield](../../sql/functions/datepart.md) from a timestamp.

<SqlLogicTest id="sql/functions/timestamp/extract" />

#### `greatest(timestamp, timestamp)`

The later of two timestamps.

<SqlLogicTest id="sql/functions/timestamp/greatest" />

#### `isfinite(timestamp)`

Returns true if the timestamp is finite, false otherwise.

<SqlLogicTest id="sql/functions/timestamp/isfinite" />

#### `isinf(timestamp)`

Returns true if the timestamp is infinite, false otherwise.

<SqlLogicTest id="sql/functions/timestamp/isinf" />

#### `julian(timestamp)`

Extract the Julian Day number from a timestamp.

<SqlLogicTest id="sql/functions/timestamp/julian" />

#### `last_day(timestamp)`

The last day of the month.

<SqlLogicTest id="sql/functions/timestamp/last_day" />

#### `least(timestamp, timestamp)`

The earlier of two timestamps.

<SqlLogicTest id="sql/functions/timestamp/least" />

#### `make_timestamp(bigint, bigint, bigint, bigint, bigint, double)`

The timestamp for the given parts.

<SqlLogicTest id="sql/functions/timestamp/make_timestamp_parts" />

#### `make_timestamp(microseconds)`

Converts microseconds since the epoch to a timestamp.

<SqlLogicTest id="sql/functions/timestamp/make_timestamp_us" />

#### `make_timestamp_ms(milliseconds)`

Converts milliseconds since the epoch to a timestamp.

<SqlLogicTest id="sql/functions/timestamp/make_timestamp_ms" />

#### `make_timestamp_ns(nanoseconds)`

Converts nanoseconds since the epoch to a timestamp.

<SqlLogicTest id="sql/functions/timestamp/make_timestamp_ns" />

#### `monthname(timestamp)`

The (English) name of the month.

<SqlLogicTest id="sql/functions/timestamp/monthname" />

#### `strftime(timestamp, format)`

Converts timestamp to string according to the [format string](../../sql/functions/dateformat.md#format-specifiers).

<SqlLogicTest id="sql/functions/timestamp/strftime" />

#### `strptime(text, format-list)`

Converts the string `text` to timestamp applying the [format strings](../../sql/functions/dateformat.md) in the list until one succeeds. Throws an error on failure. To return `NULL` on failure, use [`try_strptime`](#try_strptimetext-format-list).

<SqlLogicTest id="sql/functions/timestamp/strptime_list" />

#### `strptime(text, format)`

Converts the string `text` to timestamp according to the [format string](../../sql/functions/dateformat.md#format-specifiers). Throws an error on failure. To return `NULL` on failure, use [`try_strptime`](#try_strptimetext-format).

<SqlLogicTest id="sql/functions/timestamp/strptime" />

#### `time_bucket(bucket_width, timestamp[, offset])`

Truncate `timestamp` to a grid of width `bucket_width`. The grid includes `2000-01-01 00:00:00[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00[ + offset]`. Note that `2000-01-03` is a Monday.

<SqlLogicTest id="sql/functions/timestamp/time_bucket_offset" />

#### `time_bucket(bucket_width, timestamp[, origin])`

Truncate `timestamp` to a grid of width `bucket_width`. The grid includes the `origin` timestamp, which defaults to `2000-01-01 00:00:00` when `bucket_width` is a number of months or coarser units, else `2000-01-03 00:00:00`. Note that `2000-01-03` is a Monday.

<SqlLogicTest id="sql/functions/timestamp/time_bucket_origin" />

#### `try_strptime(text, format-list)`

Converts the string `text` to timestamp applying the [format strings](../../sql/functions/dateformat.md) in the list until one succeeds. Returns `NULL` on failure.

<SqlLogicTest id="sql/functions/timestamp/try_strptime_list" />

#### `try_strptime(text, format)`

Converts the string `text` to timestamp according to the [format string](../../sql/functions/dateformat.md#format-specifiers). Returns `NULL` on failure.

<SqlLogicTest id="sql/functions/timestamp/try_strptime" />

## Timestamp Table Functions

The table below shows the available table functions for `TIMESTAMP` types.

| Name                                                                                              | Description                                                                      |
| :------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------- |
| [`generate_series(timestamp, timestamp, interval)`](#generate_seriestimestamp-timestamp-interval) | Generate a table of timestamps in the closed range, stepping by the interval.    |
| [`range(timestamp, timestamp, interval)`](#rangetimestamp-timestamp-interval)                     | Generate a table of timestamps in the half open range, stepping by the interval. |

<DocCallout type="tip">

Infinite values are not allowed as table function bounds.

</DocCallout>

#### `generate_series(timestamp, timestamp, interval)`

Generate a table of timestamps in the closed range, stepping by the interval.

<SqlLogicTest id="sql/functions/timestamp/generate_series" />

#### `range(timestamp, timestamp, interval)`

Generate a table of timestamps in the half open range, stepping by the interval.

<SqlLogicTest id="sql/functions/timestamp/range" />
