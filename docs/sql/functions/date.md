---
title: Date Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`DATE`](../../sql/data_types/date.md) values.

## Date Operators

The table below shows the available mathematical operators for `DATE` types.

| Operator | Description                          | Example                                                                                                       | Result                                          |
| :------- | :----------------------------------- | :------------------------------------------------------------------------------------------------------------ | :---------------------------------------------- |
| `+`      | addition of days (integers)          | `DATE '1992-03-22' + 5`{:.language-sql .highlight}                                                            | `1992-03-27`                                    |
| `+`      | addition of AN `INTERVAL`            | `DATE '1992-03-22' + INTERVAL 5 DAY`{:.language-sql .highlight}                                               | `1992-03-27 00:00:00`                           |
| `+`      | addition of a variable `INTERVAL`    | `SELECT DATE '1992-03-22' + INTERVAL (d.days) DAY FROM (VALUES (5), (11)) d(days)`{:.language-sql .highlight} | `1992-03-27 00:00:00` and `1992-04-02 00:00:00` |
| `-`      | subtraction of `DATE`s               | `DATE '1992-03-27' - DATE '1992-03-22'`{:.language-sql .highlight}                                            | `5`                                             |
| `-`      | subtraction of an `INTERVAL`         | `DATE '1992-03-27' - INTERVAL 5 DAY`{:.language-sql .highlight}                                               | `1992-03-22 00:00:00`                           |
| `-`      | subtraction of a variable `INTERVAL` | `SELECT DATE '1992-03-27' - INTERVAL (d.days) DAY FROM (VALUES (5), (11)) d(days)`{:.language-sql .highlight} | `1992-03-22 00:00:00` and `1992-03-16 00:00:00` |

Adding to or subtracting from [infinite values](../../sql/data_types/date.md#special-values) produces the same infinite value.

## Date Functions

The table below shows the available functions for `DATE` types.
Dates can also be manipulated with the [timestamp functions](../../sql/functions/timestamp.md) through type promotion.

| Name                                                                                | Description                                                                                                                                                                                                                                                 |
| :---------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`date_add(date, interval)`](#date_adddate-interval)                                | Add the interval to the date and return a `DATETIME` value.                                                                                                                                                                                                 |
| [`date_diff(part, startdate, enddate)`](#date_diffpart-startdate-enddate)           | The number of [`part`](../../sql/functions/datepart.md) boundaries between `startdate` and `enddate`, inclusive of the larger date and exclusive of the smaller date.                                                                                       |
| [`date_part(part, date)`](#date_partpart-date)                                      | Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`).                                                                                                                                                                                  |
| [`date_sub(part, startdate, enddate)`](#date_subpart-startdate-enddate)             | The signed length of the interval between `startdate` and `enddate`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md).                                                                                                             |
| [`date_trunc(part, date)`](#date_truncpart-date)                                    | Truncate to specified [precision](../../sql/functions/datepart.md).                                                                                                                                                                                         |
| [`dayname(date)`](#daynamedate)                                                     | The (English) name of the weekday.                                                                                                                                                                                                                          |
| [`days_in_month(date)`](#days_in_monthdate)                                         | The number of days in the month of the given date.                                                                                                                                                                                                          |
| [`extract(part from date)`](#extractpart-from-date)                                 | Get [subfield](../../sql/functions/datepart.md) from a date.                                                                                                                                                                                                |
| [`greatest(date, date)`](#greatestdate-date)                                        | The later of two dates.                                                                                                                                                                                                                                     |
| [`isfinite(date)`](#isfinitedate)                                                   | Returns true if the date is finite, false otherwise.                                                                                                                                                                                                        |
| [`isinf(date)`](#isinfdate)                                                         | Returns true if the date is infinite, false otherwise.                                                                                                                                                                                                      |
| [`julian(date)`](#juliandate)                                                       | Extract the Julian Day number from a date.                                                                                                                                                                                                                  |
| [`last_day(date)`](#last_daydate)                                                   | The last day of the corresponding month in the date.                                                                                                                                                                                                        |
| [`least(date, date)`](#leastdate-date)                                              | The earlier of two dates.                                                                                                                                                                                                                                   |
| [`make_date(year, month, day)`](#make_dateyear-month-day)                           | The date for the given parts.                                                                                                                                                                                                                               |
| [`monthname(date)`](#monthnamedate)                                                 | The (English) name of the month.                                                                                                                                                                                                                            |
| [`strftime(date, format)`](#strftimedate-format)                                    | Converts a date to a string according to the [format string](../../sql/functions/dateformat.md).                                                                                                                                                            |
| [`time_bucket(bucket_width, date[, offset])`](#time_bucketbucket_width-date-offset) | Truncate `date` to a grid of width `bucket_width`. The grid is anchored at `2000-01-01[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03[ + offset]`. Note that `2000-01-03` is a Monday.                            |
| [`time_bucket(bucket_width, date[, origin])`](#time_bucketbucket_width-date-origin) | Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01` when `bucket_width` is a number of months or coarser units, else `2000-01-03`. Note that `2000-01-03` is a Monday. |
| `today()`                                                                 | Current date (start of current transaction) in the local time zone.                                                                                                                                                                                         |

#### `date_add(date, interval)`

Add the interval to the date and return a `DATETIME` value.

<SqlLogicTest id="sql/functions/date/date_add" />

#### `date_diff(part, startdate, enddate)`

The number of [`part`](../../sql/functions/datepart.md) boundaries between `startdate` and `enddate`, inclusive of the larger date and exclusive of the smaller date. Alias: `datediff`.

<SqlLogicTest id="sql/functions/date/date_diff" />

#### `date_part(part, date)`

Get the [subfield](../../sql/functions/datepart.md) (equivalent to `extract`). Alias: `datepart`.

<SqlLogicTest id="sql/functions/date/date_part" />

#### `date_sub(part, startdate, enddate)`

The signed length of the interval between `startdate` and `enddate`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md). Alias: `datesub`.

<SqlLogicTest id="sql/functions/date/date_sub" />

#### `date_trunc(part, date)`

Truncate to specified [precision](../../sql/functions/datepart.md). Always returns a `TIMESTAMP`, even when the input is a `DATE`. Alias: `datetrunc`.

<SqlLogicTest id="sql/functions/date/date_trunc" />

#### `dayname(date)`

The (English) name of the weekday.

<SqlLogicTest id="sql/functions/date/dayname" />

#### `days_in_month(date)`

The number of days in the month of the given date.

<SqlLogicTest id="sql/functions/date/days_in_month" />

#### `extract(part from date)`

Get [subfield](../../sql/functions/datepart.md) from a date.

<SqlLogicTest id="sql/functions/date/extract" />

#### `greatest(date, date)`

The later of two dates.

<SqlLogicTest id="sql/functions/date/greatest" />

#### `isfinite(date)`

Returns `true` if the date is finite, false otherwise.

<SqlLogicTest id="sql/functions/date/isfinite" />

#### `isinf(date)`

Returns `true` if the date is infinite, false otherwise.

<SqlLogicTest id="sql/functions/date/isinf" />

#### `julian(date)`

Extract the Julian Day number from a date.

<SqlLogicTest id="sql/functions/date/julian" />

#### `last_day(date)`

The last day of the corresponding month in the date.

<SqlLogicTest id="sql/functions/date/last_day" />

#### `least(date, date)`

The earlier of two dates.

<SqlLogicTest id="sql/functions/date/least" />

#### `make_date(year, month, day)`

The date for the given parts.

<SqlLogicTest id="sql/functions/date/make_date" />

#### `monthname(date)`

The (English) name of the month.

<SqlLogicTest id="sql/functions/date/monthname" />

#### `strftime(date, format)`

Converts a date to a string according to the [format string](../../sql/functions/dateformat.md).

<SqlLogicTest id="sql/functions/date/strftime" />

#### `time_bucket(bucket_width, date[, offset])`

Truncate `date` to a grid of width `bucket_width`. The grid is anchored at `2000-01-01[ + offset]` when `bucket_width` is a number of months or coarser units, else `2000-01-03[ + offset]`. Note that `2000-01-03` is a Monday.

<SqlLogicTest id="sql/functions/date/time_bucket_offset" />

#### `time_bucket(bucket_width, date[, origin])`

Truncate `timestamptz` to a grid of width `bucket_width`. The grid is anchored at the `origin` timestamp, which defaults to `2000-01-01` when `bucket_width` is a number of months or coarser units, else `2000-01-03`. Note that `2000-01-03` is a Monday.

<SqlLogicTest id="sql/functions/date/time_bucket_origin" />

#### `today()`

<div class="nostroke_table"></div>

| **Description** | Current date (start of current transaction) in the local time zone. |
| :--- | :--- |
| **Example** | `today()` |
| **Result** | `2022-10-08` |
| **Alias** | `current_date` (no parentheses necessary) |

## Date Part Extraction Functions

There are also dedicated extraction functions to get the [subfields](../../sql/functions/datepart.md#part-functions).
A few examples include extracting the day from a date, or the day of the week from a date.

Functions applied to infinite dates will either return the same infinite dates
(e.g., `greatest`) or `NULL` (e.g., `date_part`) depending on what “makes sense”.
In general, if the function needs to examine the parts of the infinite date, the result will be `NULL`.
