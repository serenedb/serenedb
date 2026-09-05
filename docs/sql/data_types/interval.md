---
title: Interval
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`INTERVAL`s represent periods of time that can be added to or subtracted from `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, or `TIME` values.

| Name       | Description    |
| :--------- | :------------- |
| `INTERVAL` | Period of time |

An `INTERVAL` can be constructed by providing amounts together with units.
Units that aren't _months_, _days_, or _microseconds_ are converted to equivalent amounts in the next smaller of these three basis units.

<SqlLogicTest id="sql/data_types/interval/example_001" />

<DocCallout type="attention">
Decimal values are truncated to integers when used with unit keywords (unless the unit is `SECONDS` or `MILLISECONDS`).

<SqlLogicTest id="sql/data_types/interval/example_002" />

For more precision, include the unit in the string or use a more granular unit; e.g., `INTERVAL '1.5 years'` or `INTERVAL 18 MONTHS`.
</DocCallout>

Three independent basis units are necessary because a month does not correspond to a fixed amount of days (February has fewer days than March) and a day doesn't correspond to a fixed amount of microseconds (days can be 25 hours or 23 hours long because of daylight saving time).
The division into components makes the `INTERVAL` class suitable for adding or subtracting specific time units to a date. For example, we can generate a table with the first day of every month using the following SQL query:

<SqlLogicTest id="sql/data_types/interval/example_003" />

When `INTERVAL`s are deconstructed via the `datepart` function, the _months_ component is additionally split into years and months, and the _microseconds_ component is split into hours, minutes and microseconds. The _days_ component is not split into additional units. To demonstrate this, the following query generates an `INTERVAL` called `period` by summing random amounts of the three basis units. It then extracts the aforementioned six parts from `period`, adds them back together, and confirms that the result is always equal to the original `period`.

<SqlLogicTest id="sql/data_types/interval/example_004" />

<DocCallout type="attention">
The _microseconds_ component is split only into hours, minutes and microseconds, rather than hours, minutes, _seconds_ and microseconds.
</DocCallout>

The following table describes how these parts are extracted by `datepart` in formulas, as a function of the three basis units.

| Part          | Formula                                    |
| ------------- | ------------------------------------------ |
| `year`        | `#months // 12`                            |
| `month`       | `#months % 12`                             |
| `day`         | `#days`                                    |
| `hour`        | `#microseconds // (60 * 60 * 1_000_000)`   |
| `minute`      | `(#microseconds // (60 * 1_000_000)) % 60` |
| `microsecond` | `#microseconds % (60 * 1_000_000)`         |

Additionally, `datepart` may be used to extract centuries, decades, quarters, seconds and milliseconds from `INTERVAL`s. However, these parts are not required when reassembling the original `INTERVAL`. In fact, if the previous query additionally extracted any of these additional parts, then the sum of the extracted parts would generally be larger than the original `period`.

| Part          | Formula                                          |
| ------------- | ------------------------------------------------ |
| `century`     | `datepart('year', interval) // 100`              |
| `decade`      | `datepart('year', interval) // 10`               |
| `quarter`     | `datepart('month', interval) // 3 + 1`           |
| `second`      | `datepart('microsecond', interval) // 1_000_000` |
| `millisecond` | `datepart('microsecond', interval) // 1_000`     |

<DocCallout type="tip">
All units use 0-based indexing, except for quarters, which use 1-based indexing.
</DocCallout>

For example:

<SqlLogicTest id="sql/data_types/interval/example_005" />

## Arithmetic with Timestamps, Dates and Intervals

`INTERVAL`s can be added to and subtracted from `TIMESTAMP`s, `TIMESTAMPTZ`s, `DATE`s, and `TIME`s using the `+` and `-` operators.

<SqlLogicTest id="sql/data_types/interval/example_006" />

<DocCallout type="tip">
Adding an `INTERVAL` to a `DATE` returns a `TIMESTAMP` even when the `INTERVAL` has no microseconds component. The result is the same as if the `DATE` was cast to a `TIMESTAMP` (which sets the time component to `00:00:00`) before adding the `INTERVAL`.
</DocCallout>

Conversely, subtracting two `TIMESTAMP`s or two `TIMESTAMPTZ`s from one another creates an `INTERVAL` describing the difference between the timestamps with only the _days and microseconds_ components. For example:

<SqlLogicTest id="sql/data_types/interval/example_007" />

Subtracting two `DATE`s from one another does not create an `INTERVAL` but rather returns the number of days between the given dates as integer value.

<DocCallout type="attention">
Extracting a part of the `INTERVAL` difference between two `TIMESTAMP`s is not equivalent to computing the number of partition boundaries between the two `TIMESTAMP`s for the corresponding unit, as computed by the `datediff` function:

<SqlLogicTest id="sql/data_types/interval/example_008" />
</DocCallout>

## Equality and Comparison

For equality and ordering comparisons only, the total number of microseconds in an `INTERVAL` is computed by converting the days basis unit to `24 * 60 * 60 * 1e6` microseconds and the months basis unit to 30 days, or `30 * 24 * 60 * 60 * 1e6` microseconds.

As a result, `INTERVAL`s can compare equal even when they are functionally different, and the ordering of `INTERVAL`s is not always preserved when they are added to dates or timestamps.

For example:

-   `INTERVAL 30 DAYS = INTERVAL 1 MONTH`
-   but `DATE '2020-01-01' + INTERVAL 30 DAYS != DATE '2020-01-01' + INTERVAL 1 MONTH`.

and

-   `INTERVAL '30 days 12 hours' > INTERVAL 1 MONTH`
-   but `DATE '2020-01-01' + INTERVAL '30 days 12 hours' < DATE '2020-01-01' + INTERVAL 1 MONTH`.

## Functions

See the [Date Part Functions page](../../sql/functions/datepart.md) for a list of available date parts for use with an `INTERVAL`.

See the [Interval Operators page](../../sql/functions/interval.md) for functions that operate on intervals.
