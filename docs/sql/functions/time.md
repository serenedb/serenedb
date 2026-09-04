---
title: Time Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`TIME` values](../../sql/data_types/time.md).

## Time Operators

The table below shows the available mathematical operators for `TIME` types.

| Operator | Description                  | Example                             | Result     |
| :------- | :--------------------------- | :---------------------------------- | :--------- |
| `+`      | addition of an `INTERVAL`    | `TIME '01:02:03' + INTERVAL 5 HOUR` | `06:02:03` |
| `-`      | subtraction of an `INTERVAL` | `TIME '06:02:03' - INTERVAL 5 HOUR` | `01:02:03` |

## Time Functions

The table below shows the available scalar functions for `TIME` types.

| Name                                                                      | Description                                                                                                                                                           |
| :------------------------------------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`date_diff(part, starttime, endtime)`](#date_diffpart-starttime-endtime) | The number of [`part`](../../sql/functions/datepart.md) boundaries between `starttime` and `endtime`, inclusive of the larger time and exclusive of the smaller time. |
| [`date_part(part, time)`](#date_partpart-time)                            | Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`).                                                                                            |
| [`date_sub(part, starttime, endtime)`](#date_subpart-starttime-endtime)   | The signed length of the interval between `starttime` and `endtime`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md).                       |
| [`extract(part FROM time)`](#extractpart-from-time)                       | Get subfield from a time.                                                                                                                                             |
| `get_current_time()`                                 | Current time (start of current transaction).                                                                                                                          |
| [`make_time(bigint, bigint, double)`](#make_timebigint-bigint-double)     | The time for the given parts.                                                                                                                                         |

The only [date parts](../../sql/functions/datepart.md) that are defined for times are `epoch`, `hours`, `minutes`, `seconds`, `milliseconds` and `microseconds`.

#### `date_diff(part, starttime, endtime)`

The number of [`part`](../../sql/functions/datepart.md) boundaries between `starttime` and `endtime`, inclusive of the larger time and exclusive of the smaller time. Alias: `datediff`.

<SqlLogicTest id="sql/functions/time/date_diff" />

#### `date_part(part, time)`

Get [subfield](../../sql/functions/datepart.md) (equivalent to `extract`). Alias: `datepart`.

<SqlLogicTest id="sql/functions/time/date_part" />

#### `date_sub(part, starttime, endtime)`

The signed length of the interval between `starttime` and `endtime`, truncated to whole multiples of [`part`](../../sql/functions/datepart.md). Alias: `datesub`.

<SqlLogicTest id="sql/functions/time/date_sub" />

#### `extract(part FROM time)`

Get subfield from a time.

<SqlLogicTest id="sql/functions/time/extract" />

#### `get_current_time()`

<div class="nostroke_table"></div>

| **Description** | Current time (start of current transaction) in the local time zone as `TIMETZ`. |
| :--- | :--- |
| **Example** | `get_current_time()` |
| **Result** | `06:09:59.988+2` |
| **Alias** | `current_time` (no parentheses necessary) |

#### `make_time(bigint, bigint, double)`

The time for the given parts.

<SqlLogicTest id="sql/functions/time/make_time" />
