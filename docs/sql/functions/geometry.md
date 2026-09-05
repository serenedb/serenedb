---
title: Geometry Functions
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
