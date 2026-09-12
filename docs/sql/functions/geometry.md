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

| Name                                                     | Description                                                              | Aliases        |
| :------------------------------------------------------- | :----------------------------------------------------------------------- | :------------- |
| [`ST_GeomFromWKB(wkb)`](#st_geomfromwkb-function)        | Creates a geometry from Well-Known Binary (WKB) representation           |                |
| [`ST_AsWKB(geom)`](#st_aswkb-function)                   | Returns the Well-Known Binary (WKB) representation of the geometry       | `ST_AsBinary`  |
| [`ST_AsWKT(geom)`](#st_aswkt-function)                   | Returns the Well-Known Text (WKT) representation of the geometry         | `ST_AsText`    |
| [`ST_Intersects_Extent(geom1, geom2)`](#st_intersects_extent-function) | Returns true if the geometries bounding boxes intersect     | `&&`           |
| [`ST_CRS(geom)`](#st_crs-function)                       | Returns the Coordinate Reference System (CRS) identifier of the geometry |                |
| [`ST_SetCRS(geom, crs)`](#st_setcrs-function)            | Sets the Coordinate Reference System (CRS) identifier of the geometry    |                |

#### `ST_GeomFromWKB(wkb)` {#st_geomfromwkb-function}

Creates a geometry from Well-Known Binary (WKB) representation.

<SqlLogicTest id="sql/functions/geometry/st_geomfromwkb" />

#### `ST_AsWKB(geom)` {#st_aswkb-function}

Returns the Well-Known Binary (WKB) representation of the geometry. Alias: `ST_AsBinary`.

<SqlLogicTest id="sql/functions/geometry/st_aswkb" />

#### `ST_AsWKT(geom)` {#st_aswkt-function}

Returns the Well-Known Text (WKT) representation of the geometry. Alias: `ST_AsText`.

<SqlLogicTest id="sql/functions/geometry/st_aswkt" />

#### `ST_Intersects_Extent(geom1, geom2)` {#st_intersects_extent-function}

Returns true if the geometries bounding boxes intersect. Alias: `&&`.

<SqlLogicTest id="sql/functions/geometry/st_intersects_extent" />

#### `ST_CRS(geom)` {#st_crs-function}

Returns the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_crs" />

#### `ST_SetCRS(geom, crs)` {#st_setcrs-function}

Sets the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_setcrs" />
