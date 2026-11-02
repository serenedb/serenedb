---
title: Date Format Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `strftime` and `strptime` functions can be used to convert between [`DATE`](../../sql/data_types/date.md) / [`TIMESTAMP`](../../sql/data_types/timestamp.md) values and strings. This is often required when parsing CSV files, displaying output to the user or transferring information between programs. Because there are many possible date representations, these functions accept a [format string](#format-specifiers) that describes how the date or timestamp should be structured.

## `strftime` Examples

The [`strftime(timestamp, format)`](../../sql/functions/timestamp.md#strftimetimestamp-format) converts timestamps or dates to strings according to the specified pattern.

<SqlLogicTest id="sql/functions/dateformat/example_001" />

<SqlLogicTest id="sql/functions/dateformat/example_002" />

## `strptime` Examples

The [`strptime(text, format)` function](../../sql/functions/timestamp.md#strptimetext-format) converts strings to timestamps according to the specified pattern.

<SqlLogicTest id="sql/functions/dateformat/example_003" />

<SqlLogicTest id="sql/functions/dateformat/example_004" />

The `strptime` function throws an error on failure:

<SqlLogicTest id="sql/functions/dateformat/example_005" />

To return `NULL` on failure, use the [`try_strptime` function](../../sql/functions/timestamp.md#try_strptimetext-format):

<SqlLogicTest id="sql/functions/dateformat/example_008" />

## CSV Parsing

The date formats can also be specified during CSV parsing, either in the [`COPY` statement](../../sql/statements/copy/index.md) or in the `read_csv` function. This can be done by either specifying a `DATEFORMAT` or a `TIMESTAMPFORMAT` (or both). `DATEFORMAT` will be used for converting dates, and `TIMESTAMPFORMAT` will be used for converting timestamps. Below are some examples for how to use this.

In a `COPY` statement:

<SqlLogicTest id="sql/functions/dateformat/example_006" />

In a `read_csv` function:

<SqlLogicTest id="sql/functions/dateformat/example_007" />

## Format Specifiers

Below is a full list of all available format specifiers.

| Specifier | Description                                                                                                                                                                                               | Example                                      |
| :-------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------- |
| `%a`      | Abbreviated weekday name.                                                                                                                                                                                 | Sun, Mon, ...                                |
| `%A`      | Full weekday name.                                                                                                                                                                                        | Sunday, Monday, ...                          |
| `%b`      | Abbreviated month name.                                                                                                                                                                                   | Jan, Feb, ..., Dec                           |
| `%B`      | Full month name.                                                                                                                                                                                          | January, February, ...                       |
| `%c`      | ISO date and time representation                                                                                                                                                                          | 1992-03-02 10:30:20                          |
| `%d`      | Day of the month as a zero-padded decimal.                                                                                                                                                                | 01, 02, ..., 31                              |
| `%-d`     | Day of the month as a decimal number.                                                                                                                                                                     | 1, 2, ..., 30                                |
| `%f`      | Microsecond as a decimal number, zero-padded on the left.                                                                                                                                                 | 000000 - 999999                              |
| `%g`      | Millisecond as a decimal number, zero-padded on the left.                                                                                                                                                 | 000 - 999                                    |
| `%G`      | ISO 8601 year with century representing the year that contains the greater part of the ISO week (see `%V`).                                                                                               | 0001, 0002, ..., 2013, 2014, ..., 9998, 9999 |
| `%H`      | Hour (24-hour clock) as a zero-padded decimal number.                                                                                                                                                     | 00, 01, ..., 23                              |
| `%-H`     | Hour (24-hour clock) as a decimal number.                                                                                                                                                                 | 0, 1, ..., 23                                |
| `%I`      | Hour (12-hour clock) as a zero-padded decimal number.                                                                                                                                                     | 01, 02, ..., 12                              |
| `%-I`     | Hour (12-hour clock) as a decimal number.                                                                                                                                                                 | 1, 2, ... 12                                 |
| `%j`      | Day of the year as a zero-padded decimal number.                                                                                                                                                          | 001, 002, ..., 366                           |
| `%-j`     | Day of the year as a decimal number.                                                                                                                                                                      | 1, 2, ..., 366                               |
| `%m`      | Month as a zero-padded decimal number.                                                                                                                                                                    | 01, 02, ..., 12                              |
| `%-m`     | Month as a decimal number.                                                                                                                                                                                | 1, 2, ..., 12                                |
| `%M`      | Minute as a zero-padded decimal number.                                                                                                                                                                   | 00, 01, ..., 59                              |
| `%-M`     | Minute as a decimal number.                                                                                                                                                                               | 0, 1, ..., 59                                |
| `%n`      | Nanosecond as a decimal number, zero-padded on the left.                                                                                                                                                  | 000000000 - 999999999                        |
| `%p`      | Locale's AM or PM.                                                                                                                                                                                        | AM, PM                                       |
| `%S`      | Second as a zero-padded decimal number.                                                                                                                                                                   | 00, 01, ..., 59                              |
| `%-S`     | Second as a decimal number.                                                                                                                                                                               | 0, 1, ..., 59                                |
| `%u`      | ISO 8601 weekday as a decimal number where 1 is Monday.                                                                                                                                                   | 1, 2, ..., 7                                 |
| `%U`      | Week number of the year. Week 01 starts on the first Sunday of the year, so there can be week 00. Note that this is not compliant with the week date standard in ISO 8601.                                | 00, 01, ..., 53                              |
| `%V`      | ISO 8601 week as a decimal number with Monday as the first day of the week. Week 01 is the week containing Jan 4. Note that `%V` is incompatible with year directive `%Y`. Use the ISO year `%G` instead. | 01, ..., 53                                  |
| `%w`      | Weekday as a decimal number.                                                                                                                                                                              | 0, 1, ..., 6                                 |
| `%W`      | Week number of the year. Week 01 starts on the first Monday of the year, so there can be week 00. Note that this is not compliant with the week date standard in ISO 8601.                                | 00, 01, ..., 53                              |
| `%x`      | ISO date representation                                                                                                                                                                                   | 1992-03-02                                   |
| `%X`      | ISO time representation                                                                                                                                                                                   | 10:30:20                                     |
| `%y`      | Year without century as a zero-padded decimal number. Numbers 00 to 68 are turned into 2000 to 2068. Numbers 69 to 99 are turned into 1969 to 1999.                                                       | 00, 01, ..., 99                              |
| `%-y`     | Year without century as a decimal number. Numbers 0 to 68 are turned into 2000 to 2068. Numbers 69 to 99 are turned into 1969 to 1999.                                                                    | 0, 1, ..., 99                                |
| `%Y`      | Year with century as a decimal number.                                                                                                                                                                    | 2013, 2019 etc.                              |
| `%z`      | [Time offset from UTC](https://en.wikipedia.org/wiki/ISO_8601#Time_offsets_from_UTC) in the form ±HH:MM, ±HHMM, or ±HH.                                                                                   | -0700                                        |
| `%Z`      | Time zone name.                                                                                                                                                                                           | Europe/Amsterdam                             |
| `%%`      | A literal `%` character.                                                                                                                                                                                  | %                                            |

## PostgreSQL Formatting Functions

`to_char`, `to_date` and `to_timestamp` take a PostgreSQL template instead of a `%` format specifier, and follow PostgreSQL's rules for padding, fill mode and parsing. The [template patterns](#template-patterns) below describe what each part of the template does; anything in the template that is not a pattern is copied to the output as is. To format numbers, use [`to_char(x, format)`](../../sql/functions/numeric.md#to_charx-format) and [`to_number(text, format)`](../../sql/functions/numeric.md#to_numbertext-format).

| Name                                                          | Description                                                                         |
| :------------------------------------------------------------ | :---------------------------------------------------------------------------------- |
| [`to_char(timestamp, format)`](#to_chartimestamp-format)      | Formats a timestamp, or a date, as text according to the template `format`.         |
| [`to_char(timestamptz, format)`](#to_chartimestamptz-format)  | Formats a timestamp with time zone in the session time zone.                        |
| [`to_char(interval, format)`](#to_charinterval-format)        | Formats an interval as text according to the template `format`.                     |
| [`to_date(text, format)`](#to_datetext-format)                | Parses `text` into a date according to the template `format`.                       |
| [`to_timestamp(text, format)`](#to_timestamptext-format)      | Parses `text` into a timestamp with time zone according to the template `format`.  |

#### `to_char(timestamp, format)`

Formats a timestamp as text according to the template `format`. A date is formatted as a timestamp at midnight. An empty template returns `NULL`.

<SqlLogicTest id="sql/functions/dateformat/to_char_timestamp" />

#### `to_char(timestamptz, format)`

Formats a timestamp with time zone in the session time zone. `TZ` and `tz` print `UTC` or `GMT` when the session time zone is one of those, and the offset of the zone otherwise (for example `+0530`), where PostgreSQL prints the abbreviation of the zone, such as `CET` or `EST`. `OF`, `TZH` and `TZM` print the offset as PostgreSQL does.

<SqlLogicTest id="sql/functions/dateformat/to_char_timestamptz" />

#### `to_char(interval, format)`

Formats an interval. `HH24` counts all the hours of the interval, and `YYYY`, `MM` and `DD` print its years, months and days. Patterns that need a calendar date, such as `Day`, `Month`, `D` or `TZ`, are rejected. A `TIME` value is formatted as the interval since midnight, as PostgreSQL does.

<SqlLogicTest id="sql/functions/dateformat/to_char_interval" />

#### `to_date(text, format)`

Parses `text` into a date. Spaces in the template match any amount of white space, and separators match any separator, unless the template uses `FX`. Fields that are out of range raise an error.

<SqlLogicTest id="sql/functions/dateformat/to_date" />

<SqlLogicTest id="sql/functions/dateformat/to_date_out_of_range" />

#### `to_timestamp(text, format)`

Parses `text` into a timestamp with time zone. The text is read in the session time zone unless the template has a `TZ`, `TZH`, `TZM` or `OF` field. `TZ` recognizes the abbreviations `UTC`, `GMT`, `UT`, `UCT`, `Z` and `ZULU` and reads a numeric offset like `OF`; other abbreviations raise an error. To convert seconds since the epoch, use [`to_timestamp(double)`](../../sql/functions/timestamptz.md#to_timestampdouble).

<SqlLogicTest id="sql/functions/dateformat/to_timestamp" />

### Template Patterns

Most patterns are also recognized in lowercase, such as `yyyy`, `dd` or `q`, so letters meant as text belong in double quotes.

| Pattern                      | Description                                                                         |
| :--------------------------- | :---------------------------------------------------------------------------------- |
| `HH`, `HH12`                 | Hour of day (01-12).                                                                |
| `HH24`                       | Hour of day (00-23).                                                                |
| `MI`                         | Minute (00-59).                                                                     |
| `SS`                         | Second (00-59).                                                                     |
| `MS`                         | Millisecond (000-999).                                                              |
| `US`                         | Microsecond (000000-999999).                                                        |
| `FF1` ... `FF6`              | Tenth of a second up to microsecond, with 1 to 6 digits.                            |
| `SSSS`, `SSSSS`              | Seconds past midnight (0-86399).                                                    |
| `AM`, `am`, `PM`, `pm`       | Meridiem indicator without periods.                                                 |
| `A.M.`, `a.m.`, `P.M.`, `p.m.` | Meridiem indicator with periods.                                                  |
| `Y,YYY`                      | Year (4 or more digits) with a comma.                                               |
| `YYYY`                       | Year (4 or more digits).                                                            |
| `YYY`, `YY`, `Y`             | Last 3, 2 or 1 digits of the year.                                                  |
| `IYYY`, `IYY`, `IY`, `I`     | ISO 8601 week-numbering year, or its last 3, 2 or 1 digits.                         |
| `BC`, `bc`, `AD`, `ad`       | Era indicator without periods.                                                      |
| `B.C.`, `b.c.`, `A.D.`, `a.d.` | Era indicator with periods.                                                       |
| `MONTH`, `Month`, `month`    | Full month name in upper, capitalized or lower case, padded to 9 characters.        |
| `MON`, `Mon`, `mon`          | Abbreviated month name in upper, capitalized or lower case.                         |
| `MM`                         | Month number (01-12).                                                               |
| `DAY`, `Day`, `day`          | Full day name in upper, capitalized or lower case, padded to 9 characters.          |
| `DY`, `Dy`, `dy`             | Abbreviated day name in upper, capitalized or lower case.                           |
| `DDD`                        | Day of the year (001-366).                                                          |
| `IDDD`                       | Day of the ISO 8601 week-numbering year (001-371).                                  |
| `DD`                         | Day of the month (01-31).                                                           |
| `D`                          | Day of the week, Sunday (1) to Saturday (7).                                        |
| `ID`                         | ISO 8601 day of the week, Monday (1) to Sunday (7).                                 |
| `W`                          | Week of the month (1-5); the first week starts on the first day of the month.       |
| `WW`                         | Week of the year (1-53); the first week starts on the first day of the year.        |
| `IW`                         | Week of the ISO 8601 week-numbering year (01-53).                                   |
| `CC`                         | Century; the 21st century starts on 2001-01-01.                                     |
| `J`                          | Julian Day, the number of days since November 24, 4714 BC at local midnight.        |
| `Q`                          | Quarter; ignored by `to_date` and `to_timestamp`.                                   |
| `RM`, `rm`                   | Month in upper or lower case Roman numerals, padded to 4 characters.                |
| `TZ`, `tz`                   | Time zone abbreviation in upper or lower case.                                      |
| `TZH`                        | Time zone hours.                                                                    |
| `TZM`                        | Time zone minutes.                                                                  |
| `OF`                         | Time zone offset from UTC, as `+HH` or `+HH:MM`.                                    |

### Template Pattern Modifiers

| Modifier    | Description                                                                         | Example     |
| :---------- | :---------------------------------------------------------------------------------- | :---------- |
| `FM` prefix | Fill mode: no padding blanks and no leading zeros.                                  | `FMMonth`   |
| `TH` suffix | Upper case ordinal number suffix.                                                   | `DDTH`      |
| `th` suffix | Lower case ordinal number suffix.                                                   | `DDth`      |
| `FX` prefix | Fixed format: `to_date` and `to_timestamp` match the template character by character. | `FX Month DD` |
| `TM` prefix | Translation mode: day and month names in the server locale (English), without padding. | `TMMonth` |

Text in double quotes is copied as is, and a backslash before a double quote prints the double quote.
