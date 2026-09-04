---
title: Date Part Functions
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

The `date_part`, `date_trunc` and `date_diff` functions can be used to extract or manipulate parts of temporal types such as [`TIMESTAMP`](../../sql/data_types/timestamp.md), [`TIMESTAMPTZ`](../../sql/data_types/timestamp.md), [`DATE`](../../sql/data_types/date.md) and [`INTERVAL`](../../sql/data_types/interval.md).

The parts to be extracted or manipulated are specified by one of the strings in the tables below.
The example column provides the corresponding parts of the timestamp `2021-08-03 11:59:44.123456`.
Only the entries of the first table can be extracted from `INTERVAL`s or used to construct them.

<DocCallout type="tip">
Except for `julian` and `epoch`, which return `DOUBLE`s, all parts are extracted as integers. Since there are no infinite integer values in SereneDB, `NULL`s are returned for infinite timestamps.
</DocCallout>

## Part Specifiers Usable as Date Part Specifiers and in Intervals

| Specifier      | Description               | Synonyms                                                    |    Example |
| :------------- | :------------------------ | :---------------------------------------------------------- | ---------: |
| `century`      | Gregorian century         | `cent`, `centuries`, `c`                                    |       `21` |
| `day`          | Gregorian day             | `days`, `d`, `dayofmonth`                                   |        `3` |
| `decade`       | Gregorian decade          | `dec`, `decades`, `decs`                                    |      `202` |
| `hour`         | Hours                     | `hr`, `hours`, `hrs`, `h`                                   |       `11` |
| `microseconds` | Sub-minute microseconds   | `microsecond`, `us`, `usec`, `usecs`, `usecond`, `useconds` | `44123456` |
| `millennium`   | Gregorian millennium      | `mil`, `millenniums`, `millenia`, `mils`, `millenium`       |        `3` |
| `milliseconds` | Sub-minute milliseconds   | `millisecond`, `ms`, `msec`, `msecs`, `msecond`, `mseconds` |    `44123` |
| `minute`       | Minutes                   | `min`, `minutes`, `mins`, `m`                               |       `59` |
| `month`        | Gregorian month           | `mon`, `months`, `mons`                                     |        `8` |
| `quarter`      | Quarter of the year (1-4) | `quarters`                                                  |        `3` |
| `second`       | Seconds                   | `sec`, `seconds`, `secs`, `s`                               |       `44` |
| `year`         | Gregorian year            | `yr`, `y`, `years`, `yrs`                                   |     `2021` |

## Part Specifiers Only Usable as Date Part Specifiers

| Specifier         | Description                                                   | Synonyms         |              Example |
| :---------------- | :------------------------------------------------------------ | :--------------- | -------------------: |
| `dayofweek`       | Day of the week (Sunday = 0, Saturday = 6)                    | `weekday`, `dow` |                  `2` |
| `dayofyear`       | Day of the year (1-365/366)                                   | `doy`            |                `215` |
| `epoch`           | Seconds since 1970-01-01                                      |                  |  `1627991984.123456` |
| `era`             | Gregorian era (CE/AD, BCE/BC)                                 |                  |                  `1` |
| `isodow`          | ISO day of the week (Monday = 1, Sunday = 7)                  |                  |                  `2` |
| `isoyear`         | ISO Year number (Starts on Monday of week containing Jan 4th) |                  |               `2021` |
| `julian`          | Julian Day number.                                            |                  | `2459430.4998162435` |
| `timezone_hour`   | Time zone offset hour portion                                 |                  |                  `0` |
| `timezone_minute` | Time zone offset minute portion                               |                  |                  `0` |
| `timezone`        | Time zone offset in seconds                                   |                  |                  `0` |
| `week`            | Week number                                                   | `weeks`, `w`     |                 `31` |
| `yearweek`        | ISO year and week number in `YYYYWW` format                   |                  |             `202131` |

## Part Functions

There are dedicated extraction functions to get certain subfields:

| Name                                            | Description                                                                  |
| :---------------------------------------------- | :--------------------------------------------------------------------------- |
| [`century(date)`](#centurydate)                 | Century.                                                                     |
| [`day(date)`](#daydate)                         | Day.                                                                         |
| [`dayofmonth(date)`](#dayofmonthdate)           | Day (synonym).                                                               |
| [`dayofweek(date)`](#dayofweekdate)             | Numeric weekday (Sunday = 0, Saturday = 6).                                  |
| [`dayofyear(date)`](#dayofyeardate)             | Day of the year (starts from 1, i.e., January 1 = 1).                        |
| [`decade(date)`](#decadedate)                   | Decade (year / 10).                                                          |
| [`epoch(date)`](#epochdate)                     | Seconds since 1970-01-01.                                                    |
| [`era(date)`](#eradate)                         | Calendar era.                                                                |
| [`hour(date)`](#hourdate)                       | Hours.                                                                       |
| [`isodow(date)`](#isodowdate)                   | Numeric ISO weekday (Monday = 1, Sunday = 7).                                |
| [`isoyear(date)`](#isoyeardate)                 | ISO Year number (Starts on Monday of week containing Jan 4th).               |
| [`julian(date)`](#juliandate)                   | `DOUBLE` Julian Day number.                                                  |
| [`microsecond(date)`](#microseconddate)         | Sub-minute microseconds.                                                     |
| [`millennium(date)`](#millenniumdate)           | Millennium.                                                                  |
| [`millisecond(date)`](#milliseconddate)         | Sub-minute milliseconds.                                                     |
| [`minute(date)`](#minutedate)                   | Minutes.                                                                     |
| [`month(date)`](#monthdate)                     | Month.                                                                       |
| [`quarter(date)`](#quarterdate)                 | Quarter.                                                                     |
| [`second(date)`](#seconddate)                   | Seconds.                                                                     |
| [`timezone_hour(date)`](#timezone_hourdate)     | Time zone offset hour portion.                                               |
| [`timezone_minute(date)`](#timezone_minutedate) | Time zone offset minutes portion.                                            |
| [`timezone(date)`](#timezonedate)               | Time zone offset in seconds.                                                 |
| [`week(date)`](#weekdate)                       | ISO Week.                                                                    |
| [`weekday(date)`](#weekdaydate)                 | Numeric weekday synonym (Sunday = 0, Saturday = 6).                          |
| [`weekofyear(date)`](#weekofyeardate)           | ISO Week (synonym).                                                          |
| [`year(date)`](#yeardate)                       | Year.                                                                        |
| [`yearweek(date)`](#yearweekdate)               | `BIGINT` of combined ISO Year number and 2-digit version of ISO Week number. |

#### `century(date)`

Century.

<SqlLogicTest id="sql/functions/datepart/century" />

#### `day(date)`

Day.

<SqlLogicTest id="sql/functions/datepart/day" />

#### `dayofmonth(date)`

Day (synonym).

<SqlLogicTest id="sql/functions/datepart/dayofmonth" />

#### `dayofweek(date)`

Numeric weekday (Sunday = 0, Saturday = 6).

<SqlLogicTest id="sql/functions/datepart/dayofweek" />

#### `dayofyear(date)`

Day of the year (starts from 1, i.e., January 1 = 1).

<SqlLogicTest id="sql/functions/datepart/dayofyear" />

#### `decade(date)`

Decade (year / 10).

<SqlLogicTest id="sql/functions/datepart/decade" />

#### `epoch(date)`

Seconds since 1970-01-01.

<SqlLogicTest id="sql/functions/datepart/epoch" />

#### `era(date)`

Calendar era.

<SqlLogicTest id="sql/functions/datepart/era" />

#### `hour(date)`

Hours.

<SqlLogicTest id="sql/functions/datepart/hour" />

#### `isodow(date)`

Numeric ISO weekday (Monday = 1, Sunday = 7).

<SqlLogicTest id="sql/functions/datepart/isodow" />

#### `isoyear(date)`

ISO Year number (Starts on Monday of week containing Jan 4th).

<SqlLogicTest id="sql/functions/datepart/isoyear" />

#### `julian(date)`

`DOUBLE` Julian Day number.

<SqlLogicTest id="sql/functions/datepart/julian" />

#### `microsecond(date)`

Sub-minute microseconds.

<SqlLogicTest id="sql/functions/datepart/microsecond" />

#### `millennium(date)`

Millennium.

<SqlLogicTest id="sql/functions/datepart/millennium" />

#### `millisecond(date)`

Sub-minute milliseconds.

<SqlLogicTest id="sql/functions/datepart/millisecond" />

#### `minute(date)`

Minutes.

<SqlLogicTest id="sql/functions/datepart/minute" />

#### `month(date)`

Month.

<SqlLogicTest id="sql/functions/datepart/month" />

#### `quarter(date)`

Quarter.

<SqlLogicTest id="sql/functions/datepart/quarter" />

#### `second(date)`

Seconds.

<SqlLogicTest id="sql/functions/datepart/second" />

#### `timezone_hour(date)`

Time zone offset hour portion.

<SqlLogicTest id="sql/functions/datepart/timezone_hour" />

#### `timezone_minute(date)`

Time zone offset minutes portion.

<SqlLogicTest id="sql/functions/datepart/timezone_minute" />

#### `timezone(date)`

Time zone offset in seconds.

<SqlLogicTest id="sql/functions/datepart/timezone" />

#### `week(date)`

ISO Week.

<SqlLogicTest id="sql/functions/datepart/week" />

#### `weekday(date)`

Numeric weekday synonym (Sunday = 0, Saturday = 6).

<SqlLogicTest id="sql/functions/datepart/weekday" />

#### `weekofyear(date)`

ISO Week (synonym).

<SqlLogicTest id="sql/functions/datepart/weekofyear" />

#### `year(date)`

Year.

<SqlLogicTest id="sql/functions/datepart/year" />

#### `yearweek(date)`

`BIGINT` of combined ISO Year number and 2-digit version of ISO Week number.

<SqlLogicTest id="sql/functions/datepart/yearweek" />
