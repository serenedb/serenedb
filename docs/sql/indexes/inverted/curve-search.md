---
title: Multidimensional Search
sidebar_position: 10
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`curve(ROW(...))` maps a point of two to eight values to a position on a space-filling curve. An [inverted index](./index.md) on that expression answers range queries over several columns at once: it keeps the rows in curve order and reads only the parts of the curve that cross the box.

## The curve function

`curve(ROW(c1, ..., cN))` is an ordinary scalar function: it can be selected, compared, stored or used in `ORDER BY`, which sorts the rows along the curve. Its first argument is the point, a `ROW(...)` or a STRUCT of two to eight coordinates, which can have different types. It returns the code of the point as a BLOB, and NULL when the point or any coordinate is NULL. The options follow the point as named arguments with constant values; an unknown or repeated name is an error:

| Option | Default | Meaning |
| --- | --- | --- |
| `kind` | `'morton'` | `'morton'` or `'hilbert'`, see [Choosing a curve](#choosing-a-curve). |
| `max_level` | the most bits a coordinate needs, up to `256 / dimensions` | 1 through 64 bits of each coordinate in the code, 256 bits in all, see [Choosing a depth](#choosing-a-depth). |
| `lower`, `upper` | NULL | `ROW(...)`s or STRUCTs with one value per coordinate that normalize the axes, see [Axis normalization](#axis-normalization). |

Coordinates can be signed integers through BIGINT, unsigned integers through UBIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMPTZ and INET, and can be expressions over several columns. Integers keep all their bits; they are not converted to floating point. Decimals, 128-bit integers and other timestamp resolutions need an explicit cast. Floating point values order as in SQL: NaN equals NaN and sorts above positive infinity, and negative zero equals positive zero.

## Indexing and searching

Index the call as an expression, query the index by name and match the same call with `@@ ts_between(lower, upper, lower_inclusive, upper_inclusive)`:

<SqlLogicTest id="sql/indexes/inverted/curve-search/points" />

The left side must be the indexed call itself: the same coordinates and the same options. The indexed point must be written as `ROW(...)` or a struct literal of the coordinates; a call on a STRUCT column can be indexed but not searched. `lower` and `upper` are `ROW(...)`s with one bound per coordinate. A NULL component, or a NULL `ROW`, leaves that side of that coordinate open, and an inverted interval matches nothing. The inclusivity flags apply to every coordinate; for mixed endpoints, write ordinary ranges. The condition holds when every coordinate is within its bounds, so a row with a NULL coordinate never matches, as its `curve(ROW(...))` value is NULL.

The bounds must be constants or parameters of a prepared statement. The condition must be a top-level `AND` term of the `WHERE` clause: inside `OR` or `NOT` it is an error. Modifiers such as `::boost` are not supported on it.

At the default depth the code holds integers, dates, timestamps, FLOAT and DOUBLE exactly, so the index answers the box by itself and reads no coordinate. The columns need no `INCLUDE`. INET coordinates, normalized axes and coordinates with more bits than `max_level` also need a comparison of the real values, as do bounds that do not fit the coordinate's type. That exact check runs on the rows the index selects, and `EXPLAIN` shows it as the column filter of the index scan.

Bounds use the family of their coordinate. Integer bounds are accepted for every integer coordinate and for FLOAT and DOUBLE, decimal and floating point bounds for FLOAT and DOUBLE; dates, timestamps, timestamps with time zone and INET need bounds of their own type. Cast other bounds explicitly.

Ordinary ranges also use the curve when every coordinate of an indexed call has both a lower and an upper constant bound. Supported predicates are `BETWEEN`, `>=`, `<=`, `>`, `<` and `=`; equality supplies both endpoints, and several ranges on one coordinate are intersected. Coordinates match the components of the indexed point, also through order-preserving widening casts within the signed or unsigned integer family and from FLOAT to DOUBLE. Predicates the index answers by itself go away; the others stay as the exact check.

## Axis normalization

An axis spans the whole range of its type: 8 bits for TINYINT and UTINYINT, 16 for SMALLINT and USMALLINT, 32 for INTEGER, UINTEGER and DATE, and 64 for the other types. A code with fewer bits keeps only the leading ones, and when the values of a coordinate use a small part of the range, such as small integers next to large ones, or doubles in a narrow range far from zero, those bits cannot tell them apart. `lower` and `upper` declare the range of the data, per coordinate:

<SqlLogicTest id="sql/indexes/inverted/curve-search/normalized" />

Distinct integers, dates and timestamps inside the declared range stay distinct; FLOAT and DOUBLE values are mapped linearly onto the range, and an INET value takes the position of its network. IPv6 ranges are described in [INET coordinates](#inet-coordinates). Values outside the range take the position of the nearest end and are still found, since the exact check compares the real values. A NULL component leaves that axis as it is. Every pair of bounds needs `lower < upper`; FLOAT and DOUBLE bounds and their difference must be finite, while date and timestamp bounds can be infinite. The bounds are part of the call, so the query repeats them.

With DOUBLE values in `[1000, 1001]` and `max_level := 24`, a box matching 220 of 10,000 rows selected 3,750 rows without normalization and 220 with it. A normalized axis always keeps the exact check, so at the default depth normalization only makes queries slower.

## INET coordinates

INET values order by family, then network, then mask, then address, where the network is the address with the bits past the mask cleared. A value takes the position of its network: IPv4 networks in the lower half of the axis and IPv6 networks in the upper half, by their leading 63 bits. The mask, the host bits and the lowest IPv6 bits do not change the position; the exact check still compares them. Normalization bounds must have increasing positions, so bounds in one network, such as `10.0.0.0/24` and `10.0.0.255/24`, cannot define a range.

When `lower` and `upper` of an INET coordinate are both IPv6 addresses, the axis measures the full 128-bit distance of a network from `lower`. A range of up to 2^64 addresses, such as one /64 subnet, keeps every host distinct; a wider range keeps the leading 64 bits of the distance. Networks outside the range, IPv4 networks included, take the position of the nearest end. Hosts of one subnet need such a range, as their leading 63 bits are the same.

## Choosing a depth

`max_level` sets the number of bits of each coordinate in the code. By default it is the most bits any coordinate needs, so the code holds every value exactly: 8, 16 or 32 for TINYINT, SMALLINT, INTEGER, DATE and their unsigned forms, 64 for BIGINT, UBIGINT, timestamps, FLOAT, DOUBLE and INET, and for a normalized integer, date, timestamp or IPv4 axis the bits of its declared range, 7 for `[0, 100]` and 37 for one day of timestamps. A normalized IPv6 range needs the bits of its size, up to 64, and a normalized FLOAT or DOUBLE axis 64. A code holds at most 256 bits, so the depth stops at 51 for five dimensions, 42 for six, 36 for seven and 32 for eight, and 64-bit coordinates of such a call keep the exact check. `max_level := NULL` asks for the default, and an explicit value equal to the default names the same call.

A smaller depth makes the index smaller, but adds the exact check, which reads the coordinates of every selected row. On ten million points of three DOUBLE coordinates, `max_level := 32` took 235 MB instead of 352 MB, and boxes matching more than a few rows took 2 to 3 times longer on one thread.

BIGINT is different: identifiers or counts far below `2^32` share their upper 32 bits, so at `max_level := 32` every row falls into the same cell and the index selects every row. Keep the default unless the index size matters more than the query time, and compare `rows_scanned` on the application's own boxes.

## Choosing a curve

`'morton'` is the better choice: the index reads the position of a row from its code, which takes much longer for `'hilbert'`. On ten million uniform points boxes took 1.6 to 3 times longer with `'hilbert'`, and a box bounded on one of three axes 5 times longer. Both select the same rows.

A curve index suits boxes bounded on every coordinate, and takes about 60% of the size and half the build time of a per-column index on the same columns. On three uniform DOUBLE coordinates at fifty million rows, boxes matching up to a few thousand rows took 30 to 45% less time than per-column ranges with all threads and 3 to 5 times less on one thread. At ten million rows the gain is up to a third with all threads and up to 1.8 times on one, and at one million rows the times are about equal. Wider boxes take about as long as per-column ranges with all threads, as the threads of a scan share the reading of the index, and stay faster on one thread. Correlated coordinates take about the same time through both. A box bounded on only one of three axes takes up to 1.5 times as long as a per-column range. Benchmark the application's own distributions before choosing a curve index or a depth.

`rows_scanned` in JSON profiling counts the rows a streaming index scan selects before the exact check. Compare it with the result count to see how much the exact check removes. A query answered from metadata alone, such as a plain count, may not stream at all.
