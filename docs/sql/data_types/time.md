---
title: Time
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `TIME` and `TIMETZ` types specify the hour, minute, second, microsecond of a day.

| Name      | Aliases                  | Description                        |
| :-------- | :----------------------- | :--------------------------------- |
| `TIME`    | `TIME WITHOUT TIME ZONE` | Time of day                        |
| `TIMETZ`  | `TIME WITH TIME ZONE`    | Time of day, with time zone offset |
| `TIME_NS` |                          | Time of day, nanosecond precision  |

Instances can be created using the type names as a keyword, where the data must be formatted according to the ISO 8601 format (`hh:mm:ss[.zzzzzz[zzz]][+-TT[:tt]]`).

<SqlLogicTest id="sql/data_types/time/example_001" />

<SqlLogicTest id="sql/data_types/time/example_002" />

<SqlLogicTest id="sql/data_types/time/example_003" />

<SqlLogicTest id="sql/data_types/time/example_004" />

<SqlLogicTest id="sql/data_types/time/example_005" />

`TIME_NS` values can also be read from Parquet when the type is [`TIME` with unit `NANOS`](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time).

<DocCallout type="attention">
The `TIME` type should only be used in rare cases, where the date part of the timestamp can be disregarded.
Most applications should use the [`TIMESTAMP` types](../../sql/data_types/timestamp.md) to represent their timestamps.
</DocCallout>
