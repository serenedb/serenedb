---
title: Interval Functions
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`INTERVAL`](../../sql/data_types/interval.md) values.

## Interval Operators

The table below shows the available mathematical operators for `INTERVAL` types.

| Operator | Description                    | Example                                            | Result                |
| :------- | :----------------------------- | :------------------------------------------------- | :-------------------- |
| `+`      | Addition of an `INTERVAL`      | `INTERVAL 1 HOUR + INTERVAL 5 HOUR`                | `INTERVAL 6 HOUR`     |
| `+`      | Addition to a `DATE`           | `DATE '1992-03-22' + INTERVAL 5 DAY`               | `1992-03-27 00:00:00` |
| `+`      | Addition to a `TIMESTAMP`      | `TIMESTAMP '1992-03-22 01:02:03' + INTERVAL 5 DAY` | `1992-03-27 01:02:03` |
| `+`      | Addition to a `TIME`           | `TIME '01:02:03' + INTERVAL 5 HOUR`                | `06:02:03`            |
| `-`      | Subtraction of an `INTERVAL`   | `INTERVAL 5 HOUR - INTERVAL 1 HOUR`                | `INTERVAL 4 HOUR`     |
| `-`      | Subtraction from a `DATE`      | `DATE '1992-03-27' - INTERVAL 5 DAY`               | `1992-03-22`          |
| `-`      | Subtraction from a `TIMESTAMP` | `TIMESTAMP '1992-03-27 01:02:03' - INTERVAL 5 DAY` | `1992-03-22 01:02:03` |
| `-`      | Subtraction from a `TIME`      | `TIME '06:02:03' - INTERVAL 5 HOUR`                | `01:02:03`            |

## Interval Functions

The table below shows the available scalar functions for `INTERVAL` types.

| Name                                                        | Description                                                                                                                                                                                               |
| :---------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`date_part(part, interval)`](#date_partpart-interval)      | Extract [datepart component](../../sql/functions/datepart.md) (equivalent to `extract`). See [`INTERVAL`](../../sql/data_types/interval.md) for the sometimes surprising rules governing this extraction. |
| [`datepart(part, interval)`](#datepartpart-interval)        | Alias of `date_part`.                                                                                                                                                                                     |
| [`extract(part FROM interval)`](#extractpart-from-interval) | Alias of `date_part`.                                                                                                                                                                                     |
| [`epoch(interval)`](#epochinterval)                         | Get total number of seconds, as double precision floating point number, in interval.                                                                                                                      |
| [`to_centuries(integer)`](#to_centuriesinteger)             | Construct a century interval.                                                                                                                                                                             |
| [`to_days(integer)`](#to_daysinteger)                       | Construct a day interval.                                                                                                                                                                                 |
| [`to_decades(integer)`](#to_decadesinteger)                 | Construct a decade interval.                                                                                                                                                                              |
| [`to_hours(integer)`](#to_hoursinteger)                     | Construct an hour interval.                                                                                                                                                                               |
| [`to_microseconds(integer)`](#to_microsecondsinteger)       | Construct a microsecond interval.                                                                                                                                                                         |
| [`to_millennia(integer)`](#to_millenniainteger)             | Construct a millennium interval.                                                                                                                                                                          |
| [`to_milliseconds(integer)`](#to_millisecondsinteger)       | Construct a millisecond interval.                                                                                                                                                                         |
| [`to_minutes(integer)`](#to_minutesinteger)                 | Construct a minute interval.                                                                                                                                                                              |
| [`to_months(integer)`](#to_monthsinteger)                   | Construct a month interval.                                                                                                                                                                               |
| [`to_quarters(integer`)](#to_quartersinteger)               | Construct an interval of `integer` quarters.                                                                                                                                                              |
| [`to_seconds(integer)`](#to_secondsinteger)                 | Construct a second interval.                                                                                                                                                                              |
| [`to_weeks(integer)`](#to_weeksinteger)                     | Construct a week interval.                                                                                                                                                                                |
| [`to_years(integer)`](#to_yearsinteger)                     | Construct a year interval.                                                                                                                                                                                |

<DocCallout type="tip">
Only the documented [date part components](../../sql/functions/datepart.md) are defined for intervals.
</DocCallout>

#### `date_part(part, interval)`

Extract [datepart component](../../sql/functions/datepart.md) (equivalent to `extract`). See [`INTERVAL`](../../sql/data_types/interval.md) for the sometimes surprising rules governing this extraction.

<SqlLogicTest id="sql/functions/interval/date_part" />

#### `datepart(part, interval)`

Alias of `date_part`.

<SqlLogicTest id="sql/functions/interval/datepart" />

#### `extract(part FROM interval)`

Alias of `date_part`.

<SqlLogicTest id="sql/functions/interval/extract" />

#### `epoch(interval)`

Get total number of seconds, as double precision floating point number, in interval.

<SqlLogicTest id="sql/functions/interval/epoch" />

#### `to_centuries(integer)`

Construct a century interval.

<SqlLogicTest id="sql/functions/interval/to_centuries" />

#### `to_days(integer)`

Construct a day interval.

<SqlLogicTest id="sql/functions/interval/to_days" />

#### `to_decades(integer)`

Construct a decade interval.

<SqlLogicTest id="sql/functions/interval/to_decades" />

#### `to_hours(integer)`

Construct an hour interval.

<SqlLogicTest id="sql/functions/interval/to_hours" />

#### `to_microseconds(integer)`

Construct a microsecond interval.

<SqlLogicTest id="sql/functions/interval/to_microseconds" />

#### `to_millennia(integer)`

Construct a millennium interval.

<SqlLogicTest id="sql/functions/interval/to_millennia" />

#### `to_milliseconds(integer)`

Construct a millisecond interval.

<SqlLogicTest id="sql/functions/interval/to_milliseconds" />

#### `to_minutes(integer)`

Construct a minute interval.

<SqlLogicTest id="sql/functions/interval/to_minutes" />

#### `to_months(integer)`

Construct a month interval.

<SqlLogicTest id="sql/functions/interval/to_months" />

#### `to_quarters(integer)`

Construct an interval of `integer` quarters.

<SqlLogicTest id="sql/functions/interval/to_quarters" />

#### `to_seconds(integer)`

Construct a second interval.

<SqlLogicTest id="sql/functions/interval/to_seconds" />

#### `to_weeks(integer)`

Construct a week interval.

<SqlLogicTest id="sql/functions/interval/to_weeks" />

#### `to_years(integer)`

Construct a year interval.

<SqlLogicTest id="sql/functions/interval/to_years" />
