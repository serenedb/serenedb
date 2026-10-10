---
title: Multidimensional and Cartesian Search
sidebar_position: 10
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Use `curve(...)` to index a numeric tuple together, or `cartesian(...)` to index
straight-edge geometry. Both use native inverted-index terms, with bounded
coverings and an exact scalar check of the candidates.

## Numeric tuples

A curve index is an expression index over `ROW(...)`, with two to eight
coordinates. The expression can combine several columns with different types.
Query the index by name and use the same tuple expression in
`sdb_box_contains(point, lower, upper)`:

<SqlLogicTest id="sql/indexes/inverted/curve-search/points" />

The endpoints are inclusive. A NULL component of a bound leaves that side of
that dimension unconstrained. An inverted interval matches nothing. A wholly
NULL point or bound makes the predicate NULL. A NULL coordinate contributes
UNKNOWN when constrained and is ignored when unconstrained. Combining dimensions
follows SQL AND: FALSE takes precedence over UNKNOWN. Thus partial boxes and
negated boxes retain the per-column comparison semantics. Missing coordinates
use a placeholder key for candidate generation and are checked against their
stored validity during recheck. A wholly NULL tuple produces no terms; its
`IS NULL` behavior is unchanged.

Coordinates support signed integers through BIGINT, unsigned integers through
UBIGINT, FLOAT/DOUBLE, DATE, TIMESTAMP and TIMESTAMPTZ. Integers retain all 64 bits;
they are not converted to floating point. Signed widths can be mixed within a
dimension, as can unsigned widths and FLOAT/DOUBLE. Bounds must use the same
family as their coordinate, except that integer bounds are accepted for FLOAT
and DOUBLE coordinates; cast other bounds explicitly. Dates, timestamps and
timestamps with time zones are distinct families. Decimal, 128-bit integers and
other timestamp resolutions require an explicit conversion before indexing.
Floating point values compare as in SQL: NaN equals NaN and sorts above
positive infinity, and negative zero equals positive zero.

The scalar predicate also works on an ordinary table. Against an index, a
positive predicate with constant bounds uses the term index, followed by the
same scalar predicate. Negated predicates, OR expressions and nonconstant bounds
retain scalar evaluation; an approximation is never negated and treated as exact.
A box with an open side, a NULL component in either bound, is evaluated by the
scalar predicate too: every curve cell constrains all dimensions at once, so the
index cannot prune by the dimensions that are left unconstrained.

## Cartesian geometry

Declare the explicit local coordinate system `GEOMETRY('SDB:CARTESIAN')` and use
the `cartesian(...)` opclass. Coordinates are interpreted directly in the units
of the application. The declaration does not transform coordinates or recognize
arbitrary EPSG systems as Cartesian.

<SqlLogicTest id="sql/indexes/inverted/curve-search/shapes" />

Points, lines, polygons and their multi variants use an XY covering. Z and M are
preserved in storage and ignored by the covering and the existing planar
predicates. Three-dimensional topology and distances are outside this index's
scope. Use a three-coordinate numeric tuple for a three-dimensional point box.

The index accelerates positive `ST_Intersects`, `ST_Contains`, `ST_Within`,
`ST_Covers`, `ST_CoveredBy`, `ST_Touches`, `ST_Crosses` and `ST_Overlaps` calls
with an indexed geometry on either side and a constant geometry on the other.
Every supported predicate has a candidate-plus-recheck guarantee: the original
predicate remains in the DuckDB plan and runs on the stored source geometry.
The current spatial extension uses Boost.Geometry for that check, with its
existing boundary, validity and geometry-collection behavior.

Empty shapes produce no terms. An empty query, `ST_Equals`, `ST_Disjoint`,
distance predicates and negation use the scalar path. Geometry collections can
be covered for `ST_Intersects`; the scalar extension's restrictions still apply
to the other predicates. Invalid or degenerate shapes receive a root covering
instead of trusting topology-based pruning. Shapes with nonfinite XY
coordinates or coordinate magnitudes above `1e100` also receive a root
covering, so every query rechecks them.

The covering follows the exact geometry, while Boost.Geometry compares
coordinates and orientations with a tolerance relative to the coordinates
that never drops below about `2.2e-16` in absolute terms. Below a magnitude
of 1 the predicates can therefore report contact between shapes that are
slightly apart, and the index can miss such rows that a scan returns: a point
`2e-9` away from a segment `1.4e-8` long touches it for Boost, and so does
`POINT(1e-16 0)` for `POINT(0 0)`. At larger magnitudes the tolerance stays
below the few ulps by which every covering cell is widened.

The geographic `encode_geojson` and `encode_geopoint` dictionaries continue to
use S2 and their existing CRS84 contract. They reject `SDB:CARTESIAN`; the
Cartesian opclass rejects CRS84. The two paths do not reinterpret each other's
index terms.

## Options and bounds

Both opclasses require parentheses. Their options are stored with the index:

| Option | Default | Meaning |
| --- | --- | --- |
| `curve` | `'morton'` | `'morton'` or `'hilbert'`. |
| `max_level` | `64` | 0 through 64 coordinate bits used for cells. Smaller values reduce depth and precision. Level 0 is the whole domain. |
| `max_cells` | `64` | 1 through 4096 covering cells, independently bounded for each indexed shape and each query. |
| `level_step` | `1 + 4 / dimensions` | `curve` only: 1 through `1 + 12 / dimensions`. A point is indexed at every `level_step`-th level and at `max_level`. |

There are no declared spatial bounds. Each axis uses the complete ordered
64-bit representation of its type. For doubles this is an order-preserving
IEEE-754 encoding, so levels measure representable bits rather than a fixed
physical distance. This avoids clipping coordinates or rebuilding an index when
the domain grows. Different columns can select different depth and cell budgets.

Covering considers both interior and boundary cells. Cartesian refinement
prioritizes the largest cells by coordinate-space area within the shape's bounds
(length for an axis-aligned line), so tiny floating point intervals near zero
do not consume the budget before useful larger cells. When refining a cell would
exceed the budget, the parent remains in the cover. Coverage is widened rather
than truncated, so reducing the budget cannot lose matches. Each indexed leaf
emits a leaf term and ancestor terms; query leaves match both indexed ancestors
and indexed descendants. The number of emitted terms is bounded by
`max_cells * (max_level + 1)` before deduplication (query terms can add one term
per cell). The cell budget is not a limit on returned documents: a coarse cover
can require checking every row.

Numeric points are always indexed at the configured maximum depth, so a numeric
box needs only one query term per covering cell. It does not enumerate the
ancestor leaf terms needed to match shapes indexed at varying depths.

A point writes a term for each indexed level: every `level_step`-th level and
`max_level`, `max_level / level_step + 2` terms at most. Most of the levels hold
no information for real data -- integer columns share their top levels in every
row, and doubles reach one row per cell long before level 64 -- so the step is
the main control over index size and build time. The query cover is computed at
full precision; a covering cell between two indexed levels is replaced by its
descendants on the next indexed level that intersect the box, at most
`2^(dimensions * (level_step - 1))` terms per cell. The candidates are therefore
never coarser than with `level_step = 1`, only the query carries more terms, and
every query term is a term dictionary lookup. The default keeps that expansion
at no more than 16 terms per cell: 3 for two dimensions, 2 for three and four,
and 1 from five dimensions up. On one million three-dimensional points a step
of 2 builds the index in 56 to 59% of the time of a step of 1 with queries at
most 12% slower, while a step of 3 builds faster still but turns a 64-cell
cover into up to 1,233 query terms. The `cartesian` opclass indexes shapes at
the depth of their covering cells and always uses every level.

## Choosing a depth

`max_level` sets the number of coordinate bits of the deepest cell, and every
indexed level writes one term per row. Doubles, dates and timestamps usually
differ within their upper bits, because the ordered encoding starts with the
sign and the exponent, so a shallower index often selects the same candidates.
On one million uniform DOUBLE points in three dimensions, `max_level=32`
selected exactly the same candidates as 64 for boxes from ±0.5 to ±100 of a
range 1000 wide, with an index of 122 MB instead of 427 MB built in 0.65 s
instead of 1.78 s. Integers are different: identifiers or counts far below
`2^32` share their upper 32 bits, so at `max_level=32` every row falls into the
same cell and the index stops pruning. A box on one million BIGINT pairs read
1,000,000 candidates at depth 32 against 127 at 64. Lower `max_level` for
floating point and time tuples whose values differ within that many bits, keep
64 for integer coordinates, and compare `rows_scanned` on the application's own
boxes.

Building an index buffers up to `segment_memory_max` per build worker (see
[write memory](maintenance.md#write-memory)), and a curve index writes more
terms per row than per-column fields. On ten million three-dimensional points
the default build peaked near 11 GB of RAM; `WITH (segment_memory_max =
16777216)` kept it below 2 GB at the cost of a longer compaction to one segment,
617 s instead of 92 s.

## Choosing a curve

Morton interleaves the ordered coordinate bits. Hilbert transforms the axes
before interleaving them. Both use the same dyadic cell hierarchy and covering
policy here, so changing the curve does not change which candidate documents
are selected. Hilbert has extra encoding cost; better ordering locality does
not automatically imply fewer postings for this term representation.

A curve index pays off for boxes that are selective in several dimensions at
once, where each column's range alone matches many rows: on one million
independent three-dimensional points such boxes run 12 to 22 times faster than
intersecting per-column ranges. A full scan of one million rows takes about
2 ms, so at that size the index beats a scan by at most about two times; on ten
million rows selective boxes run up to seven times faster than a scan.
Correlated data narrows the gap: small boxes on correlated data are a tie, wide
correlated boxes lose to a scan, and a box with an open side is evaluated by
the scalar predicate. At the default depth the index costs five to six times
the build time and four times the size of a per-column index. For Cartesian
shapes the covering wins most for long or diagonal queries, whose bounding boxes
overlap most shapes. Benchmark the application's own distributions before
choosing a budget, a step or a depth.

For a streaming index scan, `rows_scanned` in JSON profiling counts hits emitted
by the posting filter after zonemap skipping and before
the column recheck. Compare this with the final result count to measure native
candidate pruning. It is not a count of compressed bytes or individual posting
iterator advances, and a metadata-only count can avoid streaming entirely.

The [lindel registry entry](https://duckdb.org/community_extensions/extensions/lindel)
and [implementation](https://github.com/Query-farm/lindel/tree/v1.5/duckdb_lindel_rust)
provide the prior-art reference for scalar Morton/Hilbert encoding. SereneDB
does not load lindel or DuckDB's R-tree index: term generation, coverings,
budgets and candidate selection belong to the native index.
