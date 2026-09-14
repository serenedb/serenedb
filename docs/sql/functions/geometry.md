---
title: Geometry Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

This section describes the built-in functions for examining and manipulating [`GEOMETRY`](../../sql/data_types/geometry.md) values.

Geometry operations run on [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/). Function names and argument order follow PostGIS, but the set of functions is smaller and a few behaviours differ; see [Differences from PostGIS](#differences-from-postgis) at the end of this page.

## Geometry Operators

The table below lists the operators that can be used with `GEOMETRY` values.

| Operator | Description                                                                                   | Example                                                        | Result |
| :------- | :-------------------------------------------------------------------------------------------- | :------------------------------------------------------------- | :----- |
| `&&`     | Returns true if the geometries bounding boxes intersect. Equivalent to `ST_IntersectsExtent`. | `'POINT(5 5)'::GEOMETRY && 'LINESTRING(0 0, 10 20)'::GEOMETRY` | `true` |

## Constructing Geometries

| Name                                           | Description                                                    |
| :--------------------------------------------- | :------------------------------------------------------------- |
| [`ST_Point`](#st_point-function)               | Creates a point from an X and a Y coordinate                   |
| [`ST_MakeLine`](#st_makeline-function)         | Creates a line through the given points                        |
| [`ST_MakePolygon`](#st_makepolygon-function)   | Creates a polygon from a closed ring, optionally with holes    |
| [`ST_MakeEnvelope`](#st_makeenvelope-function) | Creates a rectangular polygon from minimum and maximum bounds  |
| [`ST_Collect`](#st_collect-function)           | Collects a list of geometries into a multi-geometry            |
| [`ST_Points`](#st_points-function)             | Collects every vertex of a geometry into a `MULTIPOINT`        |
| [`ST_Multi`](#st_multi-function)               | Wraps a single geometry in the matching multi-geometry         |

#### `ST_Point` function

Creates a point from an X and a Y coordinate.

<SqlLogicTest id="sql/functions/geometry/st_point" />

#### `ST_MakeLine` function

Creates a line through the given points. Accepts either two point arguments or a list of points.

<SqlLogicTest id="sql/functions/geometry/st_makeline" />

#### `ST_MakePolygon` function

Creates a polygon from a closed ring. A second argument supplies a list of interior rings (holes).

<SqlLogicTest id="sql/functions/geometry/st_makepolygon" />

#### `ST_MakeEnvelope` function

Creates a rectangular polygon from `min_x`, `min_y`, `max_x` and `max_y`.

<SqlLogicTest id="sql/functions/geometry/st_makeenvelope" />

#### `ST_Collect` function

Collects a list of geometries into a multi-geometry. Geometries of one type become the matching multi-geometry; mixed types cannot be collected, because that would require a `GEOMETRYCOLLECTION`.

<SqlLogicTest id="sql/functions/geometry/st_collect" />

#### `ST_Points` function

Collects every vertex of a geometry into a `MULTIPOINT`.

<SqlLogicTest id="sql/functions/geometry/st_points" />

#### `ST_Multi` function

Wraps a single geometry in the matching multi-geometry. A geometry that is already a multi-geometry is returned unchanged.

<SqlLogicTest id="sql/functions/geometry/st_multi" />

## Reading and Writing Formats

| Name                                                   | Description                                                    |
| :------------------------------------------------------ | :------------------------------------------------------------- |
| [`ST_GeomFromText`](#st_geomfromtext-function)         | Creates a geometry from Well-Known Text (WKT)                  |
| [`ST_GeomFromWKB`](#st_geomfromwkb-function)           | Creates a geometry from Well-Known Binary (WKB)                |
| [`ST_GeomFromGeoJSON`](#st_geomfromgeojson-function)   | Creates a geometry from a GeoJSON object                       |
| [`ST_AsWKT`](#st_aswkt-function)                       | Returns the Well-Known Text (WKT) representation               |
| [`ST_AsWKB`](#st_aswkb-function)                       | Returns the Well-Known Binary (WKB) representation             |
| [`ST_AsHEXWKB`](#st_ashexwkb-function)                 | Returns the WKB representation as a hexadecimal string         |
| [`ST_AsGeoJSON`](#st_asgeojson-function)               | Returns the GeoJSON representation                             |
| [`ST_AsSVG`](#st_assvg-function)                       | Returns the SVG path data for the geometry                     |

#### `ST_GeomFromText` function

Creates a geometry from Well-Known Text (WKT). A second argument of `true` returns `NULL` for invalid input instead of raising an error.

<SqlLogicTest id="sql/functions/geometry/st_geomfromtext" />

#### `ST_GeomFromWKB` function

Creates a geometry from Well-Known Binary (WKB) representation.

<SqlLogicTest id="sql/functions/geometry/st_geomfromwkb" />

#### `ST_GeomFromGeoJSON` function

Creates a geometry from a GeoJSON object.

<SqlLogicTest id="sql/functions/geometry/st_geomfromgeojson" />

#### `ST_AsWKT` function

Returns the Well-Known Text (WKT) representation of the geometry. Alias: `ST_AsText`.

<SqlLogicTest id="sql/functions/geometry/st_aswkt" />

#### `ST_AsWKB` function

Returns the Well-Known Binary (WKB) representation of the geometry. Alias: `ST_AsBinary`.

<SqlLogicTest id="sql/functions/geometry/st_aswkb" />

#### `ST_AsHEXWKB` function

Returns the WKB representation as a hexadecimal string.

<SqlLogicTest id="sql/functions/geometry/st_ashexwkb" />

#### `ST_AsGeoJSON` function

Returns the GeoJSON representation of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_asgeojson" />

#### `ST_AsSVG` function

Returns the SVG path data for the geometry. The second argument selects relative moves, the third sets the coordinate precision.

<SqlLogicTest id="sql/functions/geometry/st_assvg" />

## Accessing Properties

| Name                                                     | Description                                                       |
| :--------------------------------------------------------- | :----------------------------------------------------------------- |
| [`ST_GeometryType`](#st_geometrytype-function)           | Returns the geometry type                                         |
| [`ST_Dimension`](#st_dimension-function)                 | Returns the topological dimension (0, 1 or 2)                     |
| [`ST_X`](#st_x-function)                                 | Returns the X (and `ST_Y` the Y) coordinate of a point            |
| [`ST_Z`](#st_z-function)                                 | Returns the Z (and `ST_M` the M) coordinate of a point            |
| [`ST_XMin`](#st_xmin-function)                           | Returns a bound of the geometry's extent                          |
| [`ST_NPoints`](#st_npoints-function)                     | Returns the number of vertices. Alias: `ST_NumPoints`             |
| [`ST_NumGeometries`](#st_numgeometries-function)         | Returns the number of geometries in a collection                  |
| [`ST_NumInteriorRings`](#st_numinteriorrings-function)   | Returns the number of interior rings of a polygon                 |
| [`ST_ExteriorRing`](#st_exteriorring-function)           | Returns the exterior ring of a polygon                            |
| [`ST_InteriorRingN`](#st_interiorringn-function)         | Returns the nth interior ring of a polygon                        |
| [`ST_PointN`](#st_pointn-function)                       | Returns the nth vertex of a line                                  |
| [`ST_StartPoint`](#st_startpoint-function)               | Returns the first (and `ST_EndPoint` the last) vertex of a line   |
| [`ST_Dump`](#st_dump-function)                            | Expands a collection into its parts with their paths              |
| [`ST_CollectionExtract`](#st_collectionextract-function) | Extracts the elements of one dimension from a collection          |
| [`ST_HasZ`](#st_hasz-function)                           | Reports the vertex dimensions. Also `ST_HasM`, `ST_ZMFlag`        |
| [`ST_Extent`](#st_extent-function)                        | Returns the bounding box of the geometry                          |

#### `ST_GeometryType` function

Returns the geometry type, such as `POINT` or `LINESTRING`.

<SqlLogicTest id="sql/functions/geometry/st_geometrytype" />

#### `ST_Dimension` function

Returns the topological dimension: `0` for points, `1` for lines, `2` for areas.

<SqlLogicTest id="sql/functions/geometry/st_dimension" />

#### `ST_X` function

Returns the X coordinate of a point; `ST_Y` returns the Y coordinate.

<SqlLogicTest id="sql/functions/geometry/st_x" />

#### `ST_Z` function

Returns the Z coordinate of a point; `ST_M` returns the M coordinate. Both return `NULL` when the geometry does not carry that dimension.

<SqlLogicTest id="sql/functions/geometry/st_z" />

#### `ST_XMin` function

Returns a bound of the geometry's extent. The full set is `ST_XMin`, `ST_XMax`, `ST_YMin`, `ST_YMax`, `ST_ZMin` and `ST_ZMax`.

<SqlLogicTest id="sql/functions/geometry/st_extremes" />

#### `ST_NPoints` function

Returns the number of vertices in the geometry. Alias: `ST_NumPoints`.

<SqlLogicTest id="sql/functions/geometry/st_npoints" />

#### `ST_NumGeometries` function

Returns the number of geometries in a collection. A single geometry counts as one.

<SqlLogicTest id="sql/functions/geometry/st_numgeometries" />

#### `ST_NumInteriorRings` function

Returns the number of interior rings (holes) of a polygon.

<SqlLogicTest id="sql/functions/geometry/st_numinteriorrings" />

#### `ST_ExteriorRing` function

Returns the exterior ring (the shell) of a polygon as a line.

<SqlLogicTest id="sql/functions/geometry/st_exteriorring" />

#### `ST_InteriorRingN` function

Returns the nth interior ring of a polygon as a line. Rings are numbered from 1.

<SqlLogicTest id="sql/functions/geometry/st_interiorringn" />

#### `ST_PointN` function

Returns the nth vertex of a line. Vertices are numbered from 1.

<SqlLogicTest id="sql/functions/geometry/st_pointn" />

#### `ST_StartPoint` function

Returns the first vertex of a line; `ST_EndPoint` returns the last.

<SqlLogicTest id="sql/functions/geometry/st_startpoint" />

#### `ST_Dump` function

Expands a collection into a list of its parts, each with the path that locates it. There is no `ST_GeometryN` in SereneDB, so this is how an individual part is reached.

<SqlLogicTest id="sql/functions/geometry/st_dump" />

#### `ST_CollectionExtract` function

Extracts the elements of one dimension from a collection, as a multi-geometry: `1` for points, `2` for lines, `3` for polygons. Without the second argument, the highest dimension present is used.

<SqlLogicTest id="sql/functions/geometry/st_collectionextract" />

#### `ST_HasZ` function

Reports whether the geometry carries Z coordinates; `ST_HasM` does the same for M, and `ST_ZMFlag` returns a code for the combination (`0` for XY, `1` for XYM, `2` for XYZ, `3` for XYZM).

<SqlLogicTest id="sql/functions/geometry/st_haszm" />

#### `ST_Extent` function

Returns the bounding box of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_extent" />

## Measuring Geometries

These functions work in the geometry's own coordinate units. For measurements on the earth, see [Spheroidal Measurements](#spheroidal-measurements).

| Name                                     | Description                                                 |
| :---------------------------------------- | :----------------------------------------------------------- |
| [`ST_Area`](#st_area-function)           | Returns the area of the geometry                            |
| [`ST_Length`](#st_length-function)       | Returns the length of the geometry                          |
| [`ST_Perimeter`](#st_perimeter-function) | Returns the perimeter of the geometry                       |
| [`ST_Distance`](#st_distance-function)   | Returns the shortest distance between two geometries        |
| [`ST_Azimuth`](#st_azimuth-function)     | Returns the bearing from one point to another, in radians   |

#### `ST_Area` function

Returns the area of the geometry. Points and lines have zero area.

<SqlLogicTest id="sql/functions/geometry/st_area" />

#### `ST_Length` function

Returns the length of the geometry. Points and polygons have zero length; for a polygon's outline use `ST_Perimeter`.

<SqlLogicTest id="sql/functions/geometry/st_length" />

#### `ST_Perimeter` function

Returns the perimeter of the geometry, the total length of its rings.

<SqlLogicTest id="sql/functions/geometry/st_perimeter" />

#### `ST_Distance` function

Returns the shortest distance between two geometries. Geometries that intersect are zero apart.

<SqlLogicTest id="sql/functions/geometry/st_distance" />

#### `ST_Azimuth` function

Returns the bearing from one point to another, in radians clockwise from north.

<SqlLogicTest id="sql/functions/geometry/st_azimuth" />

## Testing Relationships

| Name                                                     | Description                                                        |
| :--------------------------------------------------------- | :------------------------------------------------------------------ |
| [`ST_Intersects`](#st_intersects-function)               | Returns true if the geometries share any point                     |
| [`ST_Disjoint`](#st_disjoint-function)                    | Returns true if the geometries share no point                      |
| [`ST_Contains`](#st_contains-function)                   | Returns true if the first geometry contains the second             |
| [`ST_Covers`](#st_covers-function)                       | Like `ST_Contains`, but a boundary point counts as covered         |
| [`ST_ContainsProperly`](#st_containsproperly-function)   | Returns true if the second geometry is in the first's interior     |
| [`ST_Crosses`](#st_crosses-function)                     | Returns true if the geometries cross                               |
| [`ST_Overlaps`](#st_overlaps-function)                   | Returns true if the geometries overlap at their own dimension      |
| [`ST_Touches`](#st_touches-function)                     | Returns true if the geometries meet only at their boundaries       |
| [`ST_Equals`](#st_equals-function)                       | Returns true if the geometries cover the same space                |
| [`ST_DWithin`](#st_dwithin-function)                     | Returns true if the geometries are within a given distance         |
| [`ST_Intersects_Extent`](#st_intersects_extent-function) | Returns true if the geometries bounding boxes intersect            |
| [`ST_IsValid`](#st_isvalid-function)                     | Reports whether the geometry is valid. Also `ST_IsEmpty`           |
| [`ST_IsClosed`](#st_isclosed-function)                   | Reports line properties. Also `ST_IsRing`, `ST_IsSimple`           |

#### `ST_Intersects` function

Returns true if the geometries share any point.

<SqlLogicTest id="sql/functions/geometry/st_intersects" />

#### `ST_Disjoint` function

Returns true if the geometries share no point. The exact inverse of `ST_Intersects`.

<SqlLogicTest id="sql/functions/geometry/st_disjoint" />

#### `ST_Contains` function

Returns true if the first geometry contains the second. `ST_Within` is the same test with the arguments swapped.

<SqlLogicTest id="sql/functions/geometry/st_contains" />

#### `ST_Covers` function

Like `ST_Contains`, except that a point lying on the boundary counts as covered. `ST_CoveredBy` is the same test with the arguments swapped. The example shows the one case where the two disagree: the polygon's own corner.

<SqlLogicTest id="sql/functions/geometry/st_covers" />

#### `ST_ContainsProperly` function

Returns true if the second geometry lies in the interior of the first, touching neither its boundary nor its exterior. `ST_WithinProperly` is the same test with the arguments swapped.

<SqlLogicTest id="sql/functions/geometry/st_containsproperly" />

#### `ST_Crosses` function

Returns true if the geometries cross, meaning they share some but not all interior points and the shared part has a lower dimension than at least one of them.

<SqlLogicTest id="sql/functions/geometry/st_crosses" />

#### `ST_Overlaps` function

Returns true if the geometries have the same dimension and share some, but not all, of their interiors.

<SqlLogicTest id="sql/functions/geometry/st_overlaps" />

#### `ST_Touches` function

Returns true if the geometries meet only at their boundaries, with no shared interior.

<SqlLogicTest id="sql/functions/geometry/st_touches" />

#### `ST_Equals` function

Returns true if the geometries cover the same space, regardless of vertex order or representation.

<SqlLogicTest id="sql/functions/geometry/st_equals" />

#### `ST_DWithin` function

Returns true if the geometries are within the given distance of one another.

<SqlLogicTest id="sql/functions/geometry/st_dwithin" />

#### `ST_Intersects_Extent` function

Returns true if the geometries bounding boxes intersect. Alias: `&&`.

<SqlLogicTest id="sql/functions/geometry/st_intersects_extent" />

#### `ST_IsValid` function

Reports whether the geometry is valid; `ST_IsEmpty` reports whether it holds no points.

<SqlLogicTest id="sql/functions/geometry/st_isvalid" />

#### `ST_IsClosed` function

Reports whether a line starts and ends at the same point. `ST_IsRing` additionally requires the line to be simple, and `ST_IsSimple` reports whether a geometry has no self-intersections.

<SqlLogicTest id="sql/functions/geometry/st_isclosed" />

## Deriving New Geometries

| Name                                                 | Description                                                        |
| :----------------------------------------------------- | :------------------------------------------------------------------ |
| [`ST_Intersection`](#st_intersection-function)       | Returns the part shared by both geometries                         |
| [`ST_Union`](#st_union-function)                     | Returns the combination of both geometries                         |
| [`ST_Difference`](#st_difference-function)           | Returns the part of the first geometry not in the second           |
| [`ST_SymDifference`](#st_symdifference-function)     | Returns the parts belonging to exactly one of the geometries       |
| [`ST_Buffer`](#st_buffer-function)                   | Returns the area within a given distance of the geometry           |
| [`ST_ConvexHull`](#st_convexhull-function)           | Returns the smallest convex geometry enclosing the input           |
| [`ST_Simplify`](#st_simplify-function)               | Removes vertices that fall within a tolerance                      |
| [`ST_Centroid`](#st_centroid-function)               | Returns the centre of mass of the geometry                         |
| [`ST_Envelope`](#st_envelope-function)               | Returns the bounding box as a polygon                              |
| [`ST_Boundary`](#st_boundary-function)               | Returns the boundary of the geometry                               |
| [`ST_PointOnSurface`](#st_pointonsurface-function)   | Returns a point guaranteed to lie on the geometry                  |
| [`ST_ClosestPoint`](#st_closestpoint-function)       | Returns the point of the first geometry closest to the second      |
| [`ST_ShortestLine`](#st_shortestline-function)       | Returns the shortest line between two geometries                   |

#### `ST_Intersection` function

Returns the part shared by both geometries.

<SqlLogicTest id="sql/functions/geometry/st_intersection" />

#### `ST_Union` function

Returns the combination of both geometries. Both arguments must have the same dimension, because a mixed result would need a `GEOMETRYCOLLECTION`.

<SqlLogicTest id="sql/functions/geometry/st_union" />

#### `ST_Difference` function

Returns the part of the first geometry that is not in the second.

<SqlLogicTest id="sql/functions/geometry/st_difference" />

#### `ST_SymDifference` function

Returns the parts that belong to exactly one of the geometries. Like `ST_Union`, both arguments must have the same dimension.

<SqlLogicTest id="sql/functions/geometry/st_symdifference" />

#### `ST_Buffer` function

Returns the area within the given distance of the geometry. The optional third argument sets the number of triangles used per quarter circle, which controls how round the result is.

<SqlLogicTest id="sql/functions/geometry/st_buffer" />

#### `ST_ConvexHull` function

Returns the smallest convex geometry that encloses the input.

<SqlLogicTest id="sql/functions/geometry/st_convexhull" />

#### `ST_Simplify` function

Removes vertices that fall within the given tolerance, using the Douglas-Peucker algorithm. The result may be invalid or may break topology shared with neighbouring geometries; there is no `ST_SimplifyPreserveTopology`.

<SqlLogicTest id="sql/functions/geometry/st_simplify" />

#### `ST_Centroid` function

Returns the centre of mass of the geometry, which is not necessarily on the geometry itself. For a point that is, use `ST_PointOnSurface`.

<SqlLogicTest id="sql/functions/geometry/st_centroid" />

#### `ST_Envelope` function

Returns the bounding box of the geometry as a polygon. `ST_Extent` returns the same bounds as a box value instead.

<SqlLogicTest id="sql/functions/geometry/st_envelope" />

#### `ST_Boundary` function

Returns the boundary of the geometry: the rings of a polygon, the endpoints of a line.

<SqlLogicTest id="sql/functions/geometry/st_boundary" />

#### `ST_PointOnSurface` function

Returns a point guaranteed to lie on the geometry.

<SqlLogicTest id="sql/functions/geometry/st_pointonsurface" />

#### `ST_ClosestPoint` function

Returns the point of the first geometry that lies closest to the second.

<SqlLogicTest id="sql/functions/geometry/st_closestpoint" />

#### `ST_ShortestLine` function

Returns the shortest line between two geometries.

<SqlLogicTest id="sql/functions/geometry/st_shortestline" />

## Editing Geometries

| Name                                                             | Description                                                     |
| :------------------------------------------------------------------ | :---------------------------------------------------------------- |
| [`ST_Affine`](#st_affine-function)                               | Applies an affine transformation to every vertex                |
| [`ST_Expand`](#st_expand-function)                               | Returns the bounding box grown by a distance                    |
| [`ST_Reverse`](#st_reverse-function)                             | Reverses the vertex order                                       |
| [`ST_Normalize`](#st_normalize-function)                         | Rewrites the geometry into a canonical form                     |
| [`ST_FlipCoordinates`](#st_flipcoordinates-function)             | Swaps the X and Y of every vertex                               |
| [`ST_Force2D`](#st_force2d-function)                             | Drops the Z and M dimensions                                    |
| [`ST_RemoveRepeatedPoints`](#st_removerepeatedpoints-function)   | Removes consecutive duplicate vertices                          |

#### `ST_Affine` function

Applies an affine transformation to every vertex. The six-argument form takes `a, b, d, e, xoff, yoff` and maps a vertex to `(a*x + b*y + xoff, d*x + e*y + yoff)`; a thirteen-argument form does the same in three dimensions. SereneDB has no `ST_Translate`, `ST_Scale` or `ST_Rotate`, so this is how those are expressed.

<SqlLogicTest id="sql/functions/geometry/st_affine" />

#### `ST_Expand` function

Returns the bounding box of the geometry grown by the given distance in every direction.

<SqlLogicTest id="sql/functions/geometry/st_expand" />

#### `ST_Reverse` function

Reverses the vertex order of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_reverse" />

#### `ST_Normalize` function

Rewrites the geometry into a canonical form, so that geometries covering the same space become identical.

<SqlLogicTest id="sql/functions/geometry/st_normalize" />

#### `ST_FlipCoordinates` function

Swaps the X and Y of every vertex. Useful for data stored in latitude/longitude order.

<SqlLogicTest id="sql/functions/geometry/st_flipcoordinates" />

#### `ST_Force2D` function

Drops the Z and M dimensions from the geometry.

<SqlLogicTest id="sql/functions/geometry/st_force2d" />

#### `ST_RemoveRepeatedPoints` function

Removes consecutive duplicate vertices.

<SqlLogicTest id="sql/functions/geometry/st_removerepeatedpoints" />

## Linear Referencing

| Name                                                             | Description                                                      |
| :------------------------------------------------------------------ | :----------------------------------------------------------------- |
| [`ST_LineInterpolatePoint`](#st_lineinterpolatepoint-function)   | Returns the point at a fraction along a line                     |
| [`ST_LineSubstring`](#st_linesubstring-function)                 | Returns the part of a line between two fractions                 |
| [`ST_LineLocatePoint`](#st_linelocatepoint-function)             | Returns the fraction along a line closest to a point             |
| [`ST_LocateAlong`](#st_locatealong-function)                     | Returns the positions on a measured line at a measure           |
| [`ST_LocateBetween`](#st_locatebetween-function)                 | Returns the part of a measured line between two measures        |

#### `ST_LineInterpolatePoint` function

Returns the point at the given fraction along a line, where the fraction runs from `0` at the start to `1` at the end. `ST_LineInterpolatePoints` returns several such points at a repeating interval.

<SqlLogicTest id="sql/functions/geometry/st_lineinterpolatepoint" />

#### `ST_LineSubstring` function

Returns the part of a line between two fractions of its length.

<SqlLogicTest id="sql/functions/geometry/st_linesubstring" />

#### `ST_LineLocatePoint` function

Returns the fraction along a line at which it passes closest to the given point.

<SqlLogicTest id="sql/functions/geometry/st_linelocatepoint" />

#### `ST_LocateAlong` function

Returns the positions on a line carrying M values where the measure equals the given value. `ST_InterpolatePoint` is the inverse: it returns the M value of a line at the position closest to a point.

<SqlLogicTest id="sql/functions/geometry/st_locatealong" />

#### `ST_LocateBetween` function

Returns the part of a line carrying M values that falls between two measures.

<SqlLogicTest id="sql/functions/geometry/st_locatebetween" />

## Coordinate Reference Systems

SereneDB attaches the coordinate reference system to the `GEOMETRY` type itself rather than storing an SRID number alongside each value, so `ST_SRID` and `ST_SetSRID` do not exist. `ST_CRS` and `ST_SetCRS` take their place, and identifiers are strings such as `OGC:CRS84` or `EPSG:4326`.

| Name                                       | Description                                                   |
| :------------------------------------------ | :-------------------------------------------------------------- |
| [`ST_CRS`](#st_crs-function)               | Returns the CRS identifier of the geometry                    |
| [`ST_SetCRS`](#st_setcrs-function)         | Sets the CRS identifier of the geometry                       |
| [`ST_Transform`](#st_transform-function)   | Reprojects the geometry into another CRS                      |

#### `ST_CRS` function

Returns the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_crs" />

#### `ST_SetCRS` function

Sets the Coordinate Reference System (CRS) identifier of the geometry. The coordinates are left as they are; only the label changes.

<SqlLogicTest id="sql/functions/geometry/st_setcrs" />

#### `ST_Transform` function

Reprojects the geometry from one CRS into another. The source may be omitted when the geometry already carries a CRS.

Authority definitions decide the axis order, and for `EPSG:4326` that order is latitude then longitude -- so the X of the input is read as a latitude. Pass `true` as the final argument to force the conventional longitude/latitude order instead. The example transforms `POINT(1 2)`, so X is a latitude of 1 and Y a longitude of 2, and the easting in the result is the one belonging to 2 degrees.

<SqlLogicTest id="sql/functions/geometry/st_transform" />

## Spheroidal Measurements

These functions measure on the earth rather than in the coordinate plane and return metres. They take **X as the latitude and Y as the longitude**, which is the opposite of the order used by the planar functions on this page.

| Name                                                       | Description                                                    |
| :------------------------------------------------------------ | :--------------------------------------------------------------- |
| [`ST_Distance_Sphere`](#st_distance_sphere-function)       | Returns the great-circle distance between two points          |
| [`ST_Length_Spheroid`](#st_length_spheroid-function)       | Returns the length of a line on the WGS84 spheroid            |
| [`ST_Area_Spheroid`](#st_area_spheroid-function)           | Returns the area of a polygon on the WGS84 spheroid           |
| [`ST_Perimeter_Spheroid`](#st_perimeter_spheroid-function) | Returns the perimeter of a polygon on the WGS84 spheroid      |

#### `ST_Distance_Sphere` function

Returns the great-circle distance between two points in metres, treating the earth as a sphere. The example measures one degree along the equator.

<SqlLogicTest id="sql/functions/geometry/st_distance_sphere" />

#### `ST_Length_Spheroid` function

Returns the length of a line in metres on the WGS84 spheroid. The example spans one degree of latitude, which is why the result is close to 110.6 km rather than the 111.3 km of a degree of longitude at the equator.

<SqlLogicTest id="sql/functions/geometry/st_length_spheroid" />

#### `ST_Area_Spheroid` function

Returns the area of a polygon in square metres on the WGS84 spheroid.

<SqlLogicTest id="sql/functions/geometry/st_area_spheroid" />

#### `ST_Perimeter_Spheroid` function

Returns the perimeter of a polygon in metres on the WGS84 spheroid.

<SqlLogicTest id="sql/functions/geometry/st_perimeter_spheroid" />

## Spatial Ordering and Tiling

| Name                                           | Description                                                     |
| :------------------------------------------------ | :---------------------------------------------------------------- |
| [`ST_Hilbert`](#st_hilbert-function)           | Returns the Hilbert curve index of the geometry's centre        |
| [`ST_QuadKey`](#st_quadkey-function)           | Returns the Bing Maps quadkey for a point at a zoom level       |
| [`ST_TileEnvelope`](#st_tileenvelope-function) | Returns the Web Mercator extent of an XYZ tile                  |

#### `ST_Hilbert` function

Returns the Hilbert curve index of the geometry's centre, which sorts nearby geometries close together.

<SqlLogicTest id="sql/functions/geometry/st_hilbert" />

#### `ST_QuadKey` function

Returns the Bing Maps quadkey for a point at the given zoom level.

<SqlLogicTest id="sql/functions/geometry/st_quadkey" />

#### `ST_TileEnvelope` function

Returns the extent of an XYZ tile as a polygon in Web Mercator coordinates.

<SqlLogicTest id="sql/functions/geometry/st_tileenvelope" />

## Differences from PostGIS

### How geometries are displayed

A `GEOMETRY` sent to a client is rendered as upper-case hexadecimal WKB, byte for byte what PostGIS sends:

```sql
SELECT 'POINT(1 2)'::GEOMETRY;
-- 0101000000000000000000F03F0000000000000040
```

Wrap the value in `ST_AsText` for the readable spelling, as the examples on this page do.

The hex agrees with PostGIS for two-dimensional geometries. Geometries carrying `Z` or `M` do not: SereneDB writes the ISO WKB type codes (`POINT Z` is `01E9030000...`) where PostGIS sets the EWKB high-bit flags (`0101000080...`).

### Identifying the coordinate reference system

The CRS belongs to the type, not to the value, so there is no SRID integer. Use `ST_CRS` and `ST_SetCRS` with string identifiers in place of `ST_SRID` and `ST_SetSRID`.

### PostGIS functions that are not available

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
| `ST_GeometryN` | `ST_Dump`, which expands every part at once |
| `ST_SRID`, `ST_SetSRID` | `ST_CRS` and `ST_SetCRS`, which use string identifiers |
| `ST_Translate`, `ST_Scale`, `ST_Rotate` | `ST_Affine`, which expresses all three |
| `ST_Relate` | the individual predicates |
| `ST_Segmentize`, `ST_Split`, `ST_Snap`, `ST_OffsetCurve` | — |
| `ST_GeoHash` | `ST_QuadKey`, `ST_Hilbert` |

Three further differences apply to functions that do exist:

-   **`ST_Union` and `ST_SymDifference` require both arguments to have the same dimension.** Combining a point with a polygon would produce a `GEOMETRYCOLLECTION`, which cannot be built. `ST_Intersection` and `ST_Difference` accept mixed dimensions.
-   **`Z` and `M` are dropped from returned geometries.** Measurement and predicates are computed in two dimensions, as they are in PostGIS, but PostGIS carries the extra dimensions through to the result and SereneDB does not.
-   **`GEOMETRYCOLLECTION` arguments are accepted only by `ST_Intersects` and `ST_Disjoint`.** Every other predicate rejects them, because a collection's answer is not the combination of its parts' answers.
