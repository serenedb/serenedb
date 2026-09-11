---
title: Geometry Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

This section describes the built-in functions for examining and manipulating [`GEOMETRY`](../../sql/data_types/geometry.md) values.

## Geometry Operators

The table below lists the operators that can be used with `GEOMETRY` values.

| Operator | Description                                                                                   | Example                                                        | Result |
| :------- | :-------------------------------------------------------------------------------------------- | :------------------------------------------------------------- | :----- |
| `&&`     | Returns true if the geometries bounding boxes intersect. Equivalent to `ST_IntersectsExtent`. | `'POINT(5 5)'::GEOMETRY && 'LINESTRING(0 0, 10 20)'::GEOMETRY` | `true` |

## Built-in Geometry Functions

| Name                                                     | Description                                                              |
| :------------------------------------------------------- | :----------------------------------------------------------------------- |
| [`ST_GeomFromWKB`](#st_geomfromwkb-function)             | Creates a geometry from Well-Known Binary (WKB) representation           |
| [`ST_AsWKB`](#st_aswkb-function)                         | Returns the Well-Known Binary (WKB) representation of the geometry       |
| [`ST_AsWKT`](#st_aswkt-function)                         | Returns the Well-Known Text (WKT) representation of the geometry         |
| [`ST_Intersects_Extent`](#st_intersects_extent-function) | Returns true if the geometries bounding boxes intersect                  |
| [`ST_CRS`](#st_crs-function)                             | Returns the Coordinate Reference System (CRS) identifier of the geometry |
| [`ST_SetCRS`](#st_setcrs-function)                       | Sets the Coordinate Reference System (CRS) identifier of the geometry    |

#### `ST_GeomFromWKB` function

Creates a geometry from Well-Known Binary (WKB) representation.

<SqlLogicTest id="sql/functions/geometry/st_geomfromwkb" />

#### `ST_AsWKB` function

Returns the Well-Known Binary (WKB) representation of the geometry. Alias: `ST_AsBinary`.

<SqlLogicTest id="sql/functions/geometry/st_aswkb" />

#### `ST_AsWKT` function

Returns the Well-Known Text (WKT) representation of the geometry. Alias: `ST_AsText`.

<SqlLogicTest id="sql/functions/geometry/st_aswkt" />

#### `ST_Intersects_Extent` function

Returns true if the geometries bounding boxes intersect. Alias: `&&`.

<SqlLogicTest id="sql/functions/geometry/st_intersects_extent" />

#### `ST_CRS` function

Returns the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_crs" />

#### `ST_SetCRS` function

Sets the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_setcrs" />

## PostGIS Functions That Are Not Available

Geometry operations run on [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/). The following PostGIS functions have no equivalent there and are therefore not provided. Calling one reports that the function does not exist.

| Not available | Closest thing that is |
| :------------ | :-------------------- |
| `ST_MakeValid` | `ST_IsValid` reports the problem, but nothing repairs it |
| `ST_Node`, `ST_Polygonize`, `ST_BuildArea` | — |
| `ST_LineMerge` | — |
| `ST_SimplifyPreserveTopology` | `ST_Simplify`, which may break topology |
| `ST_ConcaveHull` | `ST_ConvexHull` |
| `ST_ReducePrecision` | — |
| `ST_MaximumInscribedCircle` | `ST_PointOnSurface` for a point known to be inside |
| `ST_VoronoiDiagram` | — |
| `ST_MinimumRotatedRectangle` | `ST_Envelope`, which is axis-aligned |

Three further differences apply to functions that do exist:

-   **`ST_Union` and `ST_SymDifference` require both arguments to have the same dimension.** Combining a point with a polygon would produce a `GEOMETRYCOLLECTION`, which cannot be built. `ST_Intersection` and `ST_Difference` accept mixed dimensions.
-   **`Z` and `M` are dropped from returned geometries.** Measurement and predicates are computed in two dimensions, as they are in PostGIS, but PostGIS carries the extra dimensions through to the result and SereneDB does not.
-   **`GEOMETRYCOLLECTION` arguments are accepted only by `ST_Intersects` and `ST_Disjoint`.** Every other predicate rejects them, because a collection's answer is not the combination of its parts' answers.
