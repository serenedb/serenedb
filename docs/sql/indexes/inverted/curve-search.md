---
title: Multidimensional Search
sidebar_position: 10
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`curve(ROW(...))` maps a point of two to eight values to a position on a
space-filling curve. An inverted index on that expression answers
multidimensional range queries with bounded coverings and an exact check of the
candidates.

## The curve function

`curve(ROW(c1, ..., cN))` is an ordinary scalar function: it can be selected,
compared, stored or used in `ORDER BY`, which sorts the rows along the curve.
Its first argument is the point, a `ROW(...)` or a STRUCT of two to eight
coordinates, which can have different types. It returns the code of the
point's deepest cell as a BLOB, and NULL when the point or any coordinate is
NULL. The options follow the point as named arguments with constant values; an
unknown or repeated name is an error:

| Option | Default | Meaning |
| --- | --- | --- |
| `kind` | `'morton'` | `'morton'` or `'hilbert'`. |
| `max_level` | the most bits a coordinate needs | 0 through 64 coordinate bits used for cells, see [Choosing a depth](#choosing-a-depth). Smaller values reduce depth and precision. Level 0 is the whole domain. |
| `level_step` | `1 + 4 / dimensions` | 1 through `1 + 12 / dimensions`. An index writes a point at every `level_step`-th level and at `max_level`. |
| `max_cells` | `min(64, 4 * 2^dimensions)` | 1 through 4096 covering cells per query of an index on this call, see [Coverings](#coverings). |
| `lower`, `upper` | NULL | `ROW(...)`s with one value per coordinate that normalize the axes, see [below](#axis-normalization). |

Coordinates can be signed integers through BIGINT, unsigned integers through
UBIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, TIMESTAMPTZ and INET, and can be
expressions over several columns. Integers keep all their bits; they are not
converted to floating point. Decimals, 128-bit integers and other timestamp
resolutions need an explicit cast. Floating point values order as in SQL: NaN
equals NaN and sorts above positive infinity, and negative zero equals positive
zero.

## Indexing and searching

Index the call as an expression, with the coordinate columns included for the
exact check, query the index by name and match the same call with
`@@ ts_between(lower, upper, lower_inclusive, upper_inclusive)`:

<SqlLogicTest id="sql/indexes/inverted/curve-search/points" />

The left side must be the indexed call itself: the same coordinates and the
same options. The indexed point must be written as `ROW(...)` or a struct
literal of the coordinates, which the exact check compares; a call on a STRUCT
column can be indexed but not searched. `lower` and `upper` are `ROW(...)`s
with one bound per
coordinate. A NULL component, or a NULL `ROW`, leaves that side of that
coordinate open, and an inverted interval matches nothing. The inclusivity
flags apply to every coordinate; for mixed endpoints, write ordinary ranges.
The condition holds when every coordinate is within its bounds, so a row with
a NULL coordinate never matches, as its `curve(ROW(...))` value is NULL.

The index selects candidates with a covering of the box, and the condition is
replaced by the exact comparisons of the coordinates, `x >= 0 AND x <= 10` and
so on, which run on the candidates only; `EXPLAIN` shows them as the column
filter of the index scan. The bounds must be constants or parameters of a
prepared statement. The condition must be a top-level `AND` term of the
`WHERE` clause: inside `OR` or `NOT` it is rejected, because `@@` has no
evaluation outside the index.

Bounds use the family of their coordinate. Integer bounds are accepted for
every integer coordinate and for FLOAT and DOUBLE, decimal and floating point
bounds for FLOAT and DOUBLE; dates, timestamps, timestamps with time zone and
INET need bounds of their own type. Cast other bounds explicitly.

Ordinary ranges also use the curve when every coordinate of an indexed call
has both a lower and an upper constant bound. Supported predicates are
`BETWEEN`, `>=`, `<=`, `>`, `<` and `=`; equality supplies both endpoints, and
several ranges on one coordinate are intersected. Coordinates match the
components of the indexed point, also through order-preserving widening casts
within the signed or unsigned integer family and from FLOAT to DOUBLE. The
covering selects candidates; the original predicates stay as the exact check.

A strict endpoint is left out of the covering box when the coordinate maps
values one to one: integers, floating point values, dates and timestamps
without normalization. Normalized axes and INET can map several values to one
position, so a strict endpoint stays in the box and only the exact check
removes it.

## Axis normalization

Each axis holds the complete ordered representation of its type in its leading
bits: 8 for TINYINT and UTINYINT, 16 for SMALLINT and USMALLINT, 32 for
INTEGER, UINTEGER and DATE, and 64 for the other types. The cells split every
axis at the same bits. When the values of an axis use a
small part of that range -- small integers next to large ones, or doubles in a
narrow range far from zero, whose leading bits are the sign and the exponent
-- most levels split nothing on that axis. `lower` and `upper` declare the
range of the data, per coordinate:

<SqlLogicTest id="sql/indexes/inverted/curve-search/normalized" />

Integers, dates, timestamps and IPv4 addresses are shifted so that the declared
range starts at zero and fills the leading bits, which keeps distinct integers,
dates and timestamps inside the range distinct; an INET value takes the
position of its network.
FLOAT and DOUBLE values are mapped linearly onto the 64-bit range. IPv6 ranges
are described [below](#inet-coordinates). Values outside the range take the
position of the nearest end and are still found, since the exact check compares
the real values. A NULL component leaves that axis as it is. Every pair of
bounds needs `lower < upper`; FLOAT and DOUBLE bounds and their difference must
be finite, while date and timestamp bounds can be infinite.

On 10,000 points with `x` in `[0, 100)` and `y` in `[0, 10^9)`, a box matching
220 rows produced 2,003 candidates without normalization and 234 with it. With
DOUBLE values in `[1000, 1001]` and `max_level := 24`, the same box produced
3,750 candidates without normalization and 239 with it. On one million uniform
DOUBLE points in `[0, 1000)` at `max_level := 32`, whose leading bits already
separate the values, normalization did not speed up queries, and the index took
39 MB against 35 MB. The bounds are part of the call, so the query repeats
them.

## INET coordinates

INET values order as in PostgreSQL: by family, then network, then mask, then
address, where the network is the address with the bits past the mask cleared.
The curve places a value at its network: IPv4 networks in the lower half of the
axis and IPv6 networks in the upper half, by their leading 63 bits. The mask,
the host bits and the lowest IPv6 bits do not change the position; the exact
check still compares them. Normalization bounds must have increasing positions,
so bounds in one network, such as `10.0.0.0/24` and `10.0.0.255/24`, cannot
define a range.

When `lower` and `upper` of an INET coordinate are both IPv6 addresses, the axis
measures the full 128-bit distance of a network from `lower`. A range of up to
2^64 addresses, such as one /64 subnet, keeps every host distinct; a wider range
keeps the leading 64 bits of the distance. Networks outside the range, IPv4
networks included, take the position of the nearest end. On 20,000 hosts of
one /64 subnet, a box matching 239 rows produced 4,016 candidates without the
range and 1,251 with `lower := ROW('2001:db8:1::'::INET, NULL)` and
`upper := ROW('2001:db8:1::ffff:ffff:ffff:ffff'::INET, NULL)`.

## Coverings

Covering considers both interior and boundary cells. When refining a cell would
exceed `max_cells`, the parent remains in the cover. Coverage is widened rather
than truncated, so reducing the budget cannot lose matches. The cell budget is
not a limit on returned documents: a coarse cover can require checking every
row. An open side leaves that axis unconstrained, and every cell constrains all
axes at once, so a box that is open on most axes prunes little.

A point writes a term for each indexed level: every `level_step`-th level and
`max_level`, `max_level / level_step + 2` terms at most. When a segment is
written, a row's terms stop at the first indexed level where the row is alone
in its cell within that segment, and its leaf term keeps the row's full
position together with that level. Doubles, timestamps and integers of a wide
range reach one row per cell long before their last level, so most rows keep
only a few terms. A query cell also reads, for its own indexed level and each
one above it, the leaves whose position lies inside the cell, so it finds
exactly the rows that every level would find: an index fed in small batches,
whose segments are merged later, returns the same candidates as one built at
once. An index created by a release without this trimming keeps every level
until it is recreated. On ten million points the index of three DOUBLE
coordinates took 568 MB instead of 3,949 MB, two INTEGER coordinates and a DATE
361 MB instead of 769 MB, and INET with a TIMESTAMP 536 MB instead of
1,557 MB. On fifty million points the DOUBLE index at `max_level := 40` took
2,422 MB instead of 8,167 MB and compacted in 4 minutes instead of 23. One
million points inserted in batches of 1,000 took 48 MB instead of 400 MB. The
leaf lookups add at most 0.3 ms to a query on one million points. Every level
is still buffered before a segment is written, so the step and the depth keep
controlling build time and build memory. The query cover is computed at full
precision; a covering cell between two indexed levels is
replaced by its descendants on the next indexed level that intersect the box, at
most `2^(dimensions * (level_step - 1))` terms per cell. The candidates are therefore
never coarser than with `level_step = 1`, only the query carries more terms, and
every query term is a term dictionary lookup. The default keeps that expansion
at no more than 16 terms per cell: 3 for two dimensions, 2 for three and four,
and 1 from five dimensions up. On one million three-dimensional DOUBLE points a
step of 2 built the index in 1.50 s instead of 2.82 s at a step of 1 with
queries at most 12% slower, while a step of 3 built in 1.10 s but turns a
64-cell cover into up to 1,233 query terms.

Every query term is a dictionary lookup and one more posting list to merge,
while candidates read from a few long posting lists are cheap to recheck, so a
smaller cover often wins. The default budget is 16 cells for two dimensions, 32
for three and 64 from four up, enough for a boundary cell to split on every
axis at once up to six dimensions. On one million INTEGER pairs, 16 cells
instead of 64 cut the query terms of thirteen box sizes from 4,642 to 939 and
their total time by 9 to 11%, with 8% more candidates; on ten million pairs in
the same range the time fell by 7%. Three and four dimensions ran as fast with
16, 32 or 64 cells, while six dimensions need 64: with 32, a box of 7 rows read
607 candidates.

## Choosing a depth

`max_level` sets the number of coordinate bits of the deepest cell, and every
indexed level writes one term per row. By default it is the most bits any
coordinate needs, so the deepest cell still tells distinct values apart: 8, 16
or 32 for TINYINT, SMALLINT, INTEGER, DATE and their unsigned forms, 64 for
BIGINT, UBIGINT, timestamps, FLOAT, DOUBLE and INET, and for a normalized
integer, date, timestamp or IPv4 axis the bits of its declared range, 7 for
`[0, 100]` and 37 for one day of timestamps. A normalized IPv6 range needs the
bits of its size, up to 64, and a normalized FLOAT or DOUBLE axis 64.
`max_level := NULL` asks for the default, and an explicit value equal to the
default names the same call. On one million points of two INTEGER coordinates
and one DATE, the default depth of 32 built the index in 0.64 s instead of
2.23 s at depth 64. Normalized to their ranges, the same coordinates took a
depth of 20 and built in 0.38 s. Both selected the same candidates as at
depth 64.

Doubles usually differ within their upper bits, because their ordered encoding
starts with the sign and the exponent, so a shallower index often selects the
same candidates. Timestamps use signed integer keys, so their useful depth
depends on the unit and the range of the data.
On one million uniform DOUBLE points in three dimensions, `max_level := 32`
selected exactly the same candidates as 64 for boxes from ±0.5 to ±100 of a
range 1000 wide, built in 0.58 s instead of 1.60 s and took 35 MB instead of
46 MB, as a leaf keeps a row's position down to `max_level`. BIGINT is different: identifiers or counts far below `2^32`
share their upper 32 bits, so at `max_level := 32` every row falls into the
same cell and the index stops pruning. A box on one million BIGINT pairs read
1,000,000 candidates at depth 32 against 127 at 64. Lower `max_level` to build
faster for floating point and time coordinates whose values differ within that
many bits, or declare `lower` and `upper` so that the leading bits cover the
data; keep the default for unnormalized integer coordinates, and compare
`rows_scanned` on the application's own boxes.

Which levels a query's cells fall on depends on the box size, so the number of
query terms moves with it: a covering cell just below an indexed level becomes
up to `2^(dimensions * (level_step - 1))` terms. Across thirteen box sizes from
±700 to ±85,000 on one million INTEGER pairs at `max_cells := 64`, single boxes
ran up to 20% faster or slower at depth 32 than at depth 64, and all thirteen
took 22.9 ms in total at either depth.

Building an index buffers up to `segment_memory_max` per build worker (see
[write memory](maintenance.md#write-memory)), and a curve index writes more
terms per row than per-column fields. On ten million three-dimensional points
the default build peaked at about 10.5 GB of RAM; `WITH (segment_memory_max =
16777216)` kept it below 2 GB at the cost of a longer compaction to one segment,
62 s instead of 9 s.

## Choosing a curve

Morton interleaves the ordered coordinate bits. Hilbert transforms the axes
before interleaving them. Both use the same dyadic cell hierarchy and covering
policy here, so changing the curve does not change which candidate documents
are selected. Hilbert has extra encoding cost; better ordering locality does
not automatically imply fewer postings for this term representation.

A curve index pays off for boxes that are selective in several dimensions at
once, where each column's range alone matches many rows: on one million
independent three-dimensional points such boxes run 10 to 24 times faster than
intersecting per-column ranges. A full scan of one million rows takes about
2 ms, so at that size the index beats a scan by at most about two times; on ten
million rows selective boxes run up to three and a half times faster than a
scan. Correlated data narrows the gap: per-column ranges win small correlated
boxes, and wide correlated boxes lose to a scan. A box bounded on one of three
axes took 2.8 ms against a 1.9 ms scan of one million points. On one million
points of three DOUBLE coordinates the index at the default depth built in
1.50 s against 0.34 s for a per-column index and took 46 MB against 84 MB; on
two INTEGER coordinates and a DATE it built in 0.64 s against 0.19 s and took
33 MB against 24 MB. Benchmark the application's own
distributions before choosing a budget, a step or a depth.

For a streaming index scan, `rows_scanned` in JSON profiling counts hits emitted
by the posting filter after zonemap skipping and before
the column recheck. Compare this with the final result count to measure native
candidate pruning. It is not a count of compressed bytes or individual posting
iterator advances, and a metadata-only count can avoid streaming entirely.

The [lindel registry entry](https://duckdb.org/community_extensions/extensions/lindel)
and [implementation](https://github.com/Query-farm/lindel/tree/v1.5/duckdb_lindel_rust)
provide the prior-art reference for scalar Morton/Hilbert encoding. SereneDB
does not load lindel: term generation, coverings,
budgets and candidate selection belong to the native index.
