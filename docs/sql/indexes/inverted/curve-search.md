---
title: Multidimensional Search
sidebar_position: 10
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`curve(ROW(...))` maps a point of two to eight values to a position on a space-filling curve. An [inverted index](./index.md) on that expression answers range queries over several columns at once: it reads the rows near the box, and an exact check of the coordinates removes the rest.

## The curve function

`curve(ROW(c1, ..., cN))` is an ordinary scalar function: it can be selected, compared, stored or used in `ORDER BY`, which sorts the rows along the curve. Its first argument is the point, a `ROW(...)` or a STRUCT of two to eight coordinates, which can have different types. It returns the code of the point's deepest cell as a BLOB, and NULL when the point or any coordinate is NULL. The options follow the point as named arguments with constant values; an unknown or repeated name is an error:

| Option | Default | Meaning |
| --- | --- | --- |
| `kind` | `'morton'` | `'morton'` or `'hilbert'`. |
| `max_level` | the most bits a coordinate needs | 0 through 64 coordinate bits used for cells, see [Choosing a depth](#choosing-a-depth). Smaller values reduce depth and precision. Level 0 is the whole domain. |
| `level_step` | `1 + 4 / dimensions` | 1 through `1 + 12 / dimensions`. An index writes a point at every `level_step`-th level and at `max_level`. |
| `max_cells` | `min(64, 4 * 2^dimensions)` | 1 through 4096 covering cells per query of an index on this call, see [Coverings](#coverings). |
| `lower`, `upper` | NULL | `ROW(...)`s or STRUCTs with one value per coordinate that normalize the axes, see [Axis normalization](#axis-normalization). |

Coordinates can be signed integers through BIGINT, unsigned integers through UBIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMPTZ and INET, and can be expressions over several columns. Integers keep all their bits; they are not converted to floating point. Decimals, 128-bit integers and other timestamp resolutions need an explicit cast. Floating point values order as in SQL: NaN equals NaN and sorts above positive infinity, and negative zero equals positive zero.

## Indexing and searching

Index the call as an expression, with the coordinate columns included for the exact check, query the index by name and match the same call with `@@ ts_between(lower, upper, lower_inclusive, upper_inclusive)`:

<SqlLogicTest id="sql/indexes/inverted/curve-search/points" />

The left side must be the indexed call itself: the same coordinates and the same options. The indexed point must be written as `ROW(...)` or a struct literal of the coordinates, which the exact check compares; a call on a STRUCT column can be indexed but not searched. `lower` and `upper` are `ROW(...)`s with one bound per coordinate. A NULL component, or a NULL `ROW`, leaves that side of that coordinate open, and an inverted interval matches nothing. The inclusivity flags apply to every coordinate; for mixed endpoints, write ordinary ranges. The condition holds when every coordinate is within its bounds, so a row with a NULL coordinate never matches, as its `curve(ROW(...))` value is NULL.

The exact comparisons of the coordinates, `x >= 0 AND x <= 10` and so on, run on the candidates the index selects; `EXPLAIN` shows them as the column filter of the index scan. The bounds must be constants or parameters of a prepared statement. The condition must be a top-level `AND` term of the `WHERE` clause: inside `OR` or `NOT` it is an error.

Bounds use the family of their coordinate. Integer bounds are accepted for every integer coordinate and for FLOAT and DOUBLE, decimal and floating point bounds for FLOAT and DOUBLE; dates, timestamps, timestamps with time zone and INET need bounds of their own type. Cast other bounds explicitly.

Ordinary ranges also use the curve when every coordinate of an indexed call has both a lower and an upper constant bound. Supported predicates are `BETWEEN`, `>=`, `<=`, `>`, `<` and `=`; equality supplies both endpoints, and several ranges on one coordinate are intersected. Coordinates match the components of the indexed point, also through order-preserving widening casts within the signed or unsigned integer family and from FLOAT to DOUBLE. The original predicates stay as the exact check.

## Axis normalization

An axis spans the whole range of its type: 8 bits for TINYINT and UTINYINT, 16 for SMALLINT and USMALLINT, 32 for INTEGER, UINTEGER and DATE, and 64 for the other types. When the values of a coordinate use a small part of that range, such as small integers next to large ones, or doubles in a narrow range far from zero, most cells cannot tell them apart. `lower` and `upper` declare the range of the data, per coordinate:

<SqlLogicTest id="sql/indexes/inverted/curve-search/normalized" />

Distinct integers, dates and timestamps inside the declared range stay distinct; FLOAT and DOUBLE values are mapped linearly onto the range, and an INET value takes the position of its network. IPv6 ranges are described in [INET coordinates](#inet-coordinates). Values outside the range take the position of the nearest end and are still found, since the exact check compares the real values. A NULL component leaves that axis as it is. Every pair of bounds needs `lower < upper`; FLOAT and DOUBLE bounds and their difference must be finite, while date and timestamp bounds can be infinite. The bounds are part of the call, so the query repeats them.

On 10,000 points with `x` in `[0, 100)` and `y` in `[0, 10^9)`, a box matching 220 rows read 2,003 candidates without normalization and 234 with it. With DOUBLE values in `[1000, 1001]` and `max_level := 24`, the same box read 3,750 candidates without normalization and 239 with it. Normalization also writes more distinct terms, so the index grows, and it speeds up nothing when the leading bits already separate the values.

## INET coordinates

INET values order by family, then network, then mask, then address, where the network is the address with the bits past the mask cleared. A value takes the position of its network: IPv4 networks in the lower half of the axis and IPv6 networks in the upper half, by their leading 63 bits. The mask, the host bits and the lowest IPv6 bits do not change the position; the exact check still compares them. Normalization bounds must have increasing positions, so bounds in one network, such as `10.0.0.0/24` and `10.0.0.255/24`, cannot define a range.

When `lower` and `upper` of an INET coordinate are both IPv6 addresses, the axis measures the full 128-bit distance of a network from `lower`. A range of up to 2^64 addresses, such as one /64 subnet, keeps every host distinct; a wider range keeps the leading 64 bits of the distance. Networks outside the range, IPv4 networks included, take the position of the nearest end. On 20,000 hosts of one /64 subnet, a box matching 239 rows read 4,016 candidates without the range and 1,251 with `lower := ROW('2001:db8:1::'::INET, NULL)` and `upper := ROW('2001:db8:1::ffff:ffff:ffff:ffff'::INET, NULL)`.

## Coverings

A query reads the rows of the cells that cover its box. `max_cells` bounds those cells: where one more split would exceed it, the cell stays whole, so a smaller budget reads more candidates but cannot lose a match. The budget is not a limit on returned rows, and a coarse cover can read every row. An open side leaves that axis unconstrained, so a box that is open on most axes prunes little.

The default budget is 16 cells for two dimensions, 32 for three and 64 from four up, enough for a boundary cell to split on every axis at once up to six dimensions. A larger budget reads fewer candidates but adds query terms, and each term costs more than the candidates it saves in few dimensions. Six dimensions need 64: with 32, a box of 7 rows read 607 candidates.

An index writes a term for each indexed level of a row: every `level_step`-th level and `max_level`, at most `max_level / level_step + 2` terms. The step is the main control over index size and build time. A covering cell between two indexed levels becomes up to `2^(dimensions * (level_step - 1))` query terms: the candidates are as selective as with a step of 1, and the query takes longer. The default keeps that at no more than 16 terms per cell, with a step of 3 for two dimensions, 2 for three and four, and 1 from five dimensions up.

## Choosing a depth

`max_level` sets the number of coordinate bits of the deepest cell, and every indexed level writes one term per row. By default it is the most bits any coordinate needs, so the deepest cell still tells distinct values apart: 8, 16 or 32 for TINYINT, SMALLINT, INTEGER, DATE and their unsigned forms, 64 for BIGINT, UBIGINT, timestamps, FLOAT, DOUBLE and INET, and for a normalized integer, date, timestamp or IPv4 axis the bits of its declared range, 7 for `[0, 100]` and 37 for one day of timestamps. A normalized IPv6 range needs the bits of its size, up to 64, and a normalized FLOAT or DOUBLE axis 64. `max_level := NULL` asks for the default, and an explicit value equal to the default names the same call.

Doubles usually differ within their upper bits, so a shallower index often selects the same candidates, with a smaller index that builds faster. On one million uniform DOUBLE points in three dimensions, `max_level := 32` selected exactly the same candidates as 64 for boxes from ±0.5 to ±100 of a range 1000 wide. The useful depth of timestamps depends on the unit and the range of the data.

BIGINT is different: identifiers or counts far below `2^32` share their upper 32 bits, so at `max_level := 32` every row falls into the same cell and the index stops pruning. A box on one million BIGINT pairs read 1,000,000 candidates at depth 32 against 127 at 64.

Lower `max_level` for floating point and time coordinates whose values differ within that many bits, or declare `lower` and `upper` so that the leading bits cover the data; keep the default for unnormalized integer coordinates, and compare `rows_scanned` on the application's own boxes.

Building an index buffers up to `segment_memory_max` per build worker (see [write memory](maintenance.md#write-memory)), and a curve index writes more terms per row than per-column fields. A lower `segment_memory_max` keeps the build's memory down at the cost of a longer compaction to one segment.

## Choosing a curve

`'morton'` and `'hilbert'` select the same candidates, and `'hilbert'` takes longer to encode.

A curve index pays off for boxes that are selective in several dimensions at once, where each column's range alone matches many rows. Correlated data narrows the gap: per-column ranges win small correlated boxes, and wide correlated boxes lose to a scan, as does a box bounded on only one of three axes. The index costs several times the build time and the size of a per-column index. Benchmark the application's own distributions before choosing a budget, a step or a depth.

`rows_scanned` in JSON profiling counts the candidates of a streaming index scan before the exact check. Compare it with the result count to measure how much the index prunes. A query answered from metadata alone, such as a plain count, may not stream at all.
