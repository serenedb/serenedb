---
title: Geometry Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

This section describes the built-in functions for examining and manipulating [`GEOMETRY`](../../sql/data_types/geometry.md) values.

Geometry operations run on [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/). Function names and argument order follow PostGIS, but the set of functions is smaller and a few behaviours differ; see [Differences from PostGIS](#differences-from-postgis) at the end of this page.

## Geometry Operators

The table below lists the operators that can be used with `GEOMETRY` values.

| Operator | Description | Example | Result |
| :------- | :---------- | :------ | :----- |
| `&&` | Returns true if the geometries bounding boxes intersect. Equivalent to [`ST_Intersects_Extent`](#st_intersects_extent-function). | `'POINT(5 5)'::GEOMETRY && 'LINESTRING(0 0, 10 20)'::GEOMETRY` | `true` |

## Constructing Geometries

| Name | Description |
| :--- | :---------- |
| [`ST_Point(x, y)`](#st_point-function) | Creates a point from an X and a Y coordinate |
| [`ST_MakeLine(start, end)`](#st_makeline-function) | Creates a line between two points |
| [`ST_MakeLine(geoms)`](#st_makeline-list-function) | Creates a line through a list of points |
| [`ST_MakePolygon(shell[, holes])`](#st_makepolygon-function) | Creates a polygon from a closed ring, optionally with holes |
| [`ST_MakeEnvelope(min_x, min_y, max_x, max_y)`](#st_makeenvelope-function) | Creates a rectangular polygon from minimum and maximum bounds |
| [`ST_Collect(geoms)`](#st_collect-function) | Collects a list of geometries into a multi-geometry |
| [`ST_Points(geom)`](#st_points-function) | Collects every vertex of a geometry into a `MULTIPOINT` |
| [`ST_Multi(geom)`](#st_multi-function) | Wraps a single geometry in the matching multi-geometry |

#### `ST_Point(x, y)` {#st_point-function}

Creates a point from an X and a Y coordinate.

<SqlLogicTest id="sql/functions/geometry/st_point" />

#### `ST_MakeLine(start, end)` {#st_makeline-function}

Creates a line from the point `start` to the point `end`.

<SqlLogicTest id="sql/functions/geometry/st_makeline" />

#### `ST_MakeLine(geoms)` {#st_makeline-list-function}

Creates a line through a list of points, in list order.

#### `ST_MakePolygon(shell[, holes])` {#st_makepolygon-function}

Creates a polygon from the closed ring `shell`. The optional `holes` supplies a list of interior rings.

<SqlLogicTest id="sql/functions/geometry/st_makepolygon" />

#### `ST_MakeEnvelope(min_x, min_y, max_x, max_y)` {#st_makeenvelope-function}

Creates a rectangular polygon from `min_x`, `min_y`, `max_x` and `max_y`.

<SqlLogicTest id="sql/functions/geometry/st_makeenvelope" />

#### `ST_Collect(geoms)` {#st_collect-function}

Collects an array of geometries into one geometry. Geometries of one type become the matching multi-geometry, mixed types become a `GEOMETRYCOLLECTION`. `NULL` elements are skipped, and an empty array or an array of only `NULL`s yields `NULL`, as in PostGIS.

<SqlLogicTest id="sql/functions/geometry/st_collect" />

#### `ST_Points(geom)` {#st_points-function}

Collects every vertex of a geometry into a `MULTIPOINT`.

<SqlLogicTest id="sql/functions/geometry/st_points" />

#### `ST_Multi(geom)` {#st_multi-function}

Wraps a single geometry in the matching multi-geometry. A geometry that is already a multi-geometry is returned unchanged.

<SqlLogicTest id="sql/functions/geometry/st_multi" />

## Reading and Writing Formats

| Name | Description | Aliases |
| :--- | :---------- | :------ |
| [`ST_GeomFromText(wkt[, ignore_invalid])`](#st_geomfromtext-function) | Creates a geometry from Well-Known Text (WKT) | |
| [`ST_GeomFromWKB(wkb)`](#st_geomfromwkb-function) | Creates a geometry from Well-Known Binary (WKB) | |
| [`ST_GeomFromGeoJSON(geojson)`](#st_geomfromgeojson-function) | Creates a geometry from a GeoJSON object | |
| [`ST_AsWKT(geom)`](#st_aswkt-function) | Returns the Well-Known Text (WKT) representation | `ST_AsText` |
| [`ST_AsWKB(geom)`](#st_aswkb-function) | Returns the Well-Known Binary (WKB) representation | `ST_AsBinary` |
| [`ST_AsHEXWKB(geom)`](#st_ashexwkb-function) | Returns the WKB representation as a hexadecimal string | |
| [`ST_AsGeoJSON(geom)`](#st_asgeojson-function) | Returns the GeoJSON representation | |
| [`ST_AsSVG(geom, relative, precision)`](#st_assvg-function) | Returns the SVG path data for the geometry | |

#### `ST_GeomFromText(wkt[, ignore_invalid])` {#st_geomfromtext-function}

Creates a geometry from Well-Known Text (WKT).

<SqlLogicTest id="sql/functions/geometry/st_geomfromtext" />

Pass `ignore_invalid := true` to get `NULL` for text that does not parse instead of an error. The argument only takes effect when it is named: a positional `true` is ignored.

<SqlLogicTest id="sql/functions/geometry/st_geomfromtext_ignore_invalid" />

#### `ST_GeomFromWKB(wkb)` {#st_geomfromwkb-function}

Creates a geometry from Well-Known Binary (WKB) representation.

<SqlLogicTest id="sql/functions/geometry/st_geomfromwkb" />

#### `ST_GeomFromGeoJSON(geojson)` {#st_geomfromgeojson-function}

Creates a geometry from a GeoJSON object.

<SqlLogicTest id="sql/functions/geometry/st_geomfromgeojson" />

#### `ST_AsWKT(geom)` {#st_aswkt-function}

Returns the Well-Known Text (WKT) representation of the geometry. Alias: `ST_AsText`.

<SqlLogicTest id="sql/functions/geometry/st_aswkt" />

#### `ST_AsWKB(geom)` {#st_aswkb-function}

Returns the Well-Known Binary (WKB) representation of the geometry. Alias: `ST_AsBinary`.

<SqlLogicTest id="sql/functions/geometry/st_aswkb" />

#### `ST_AsHEXWKB(geom)` {#st_ashexwkb-function}

Returns the WKB representation as a hexadecimal string.

<SqlLogicTest id="sql/functions/geometry/st_ashexwkb" />

#### `ST_AsGeoJSON(geom)` {#st_asgeojson-function}

Returns the GeoJSON representation of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_asgeojson" />

#### `ST_AsSVG(geom, relative, precision)` {#st_assvg-function}

Returns the SVG path data for the geometry. `relative` selects relative moves and `precision` sets the number of decimal digits.

<SqlLogicTest id="sql/functions/geometry/st_assvg" />

## Accessing Properties

| Name | Description | Aliases |
| :--- | :---------- | :------ |
| [`ST_GeometryType(geom)`](#st_geometrytype-function) | Returns the geometry type | |
| [`ST_Dimension(geom)`](#st_dimension-function) | Returns the topological dimension (0, 1 or 2) | |
| [`ST_X(geom)`](#st_x-function) | Returns the X coordinate of a point | |
| [`ST_Y(geom)`](#st_y-function) | Returns the Y coordinate of a point | |
| [`ST_Z(geom)`](#st_z-function) | Returns the Z coordinate of a point | |
| [`ST_M(geom)`](#st_m-function) | Returns the M coordinate of a point | |
| [`ST_XMin(geom)`](#st_xmin-function) | Returns the smallest X of the geometry's extent | |
| [`ST_XMax(geom)`](#st_xmax-function) | Returns the largest X of the geometry's extent | |
| [`ST_YMin(geom)`](#st_ymin-function) | Returns the smallest Y of the geometry's extent | |
| [`ST_YMax(geom)`](#st_ymax-function) | Returns the largest Y of the geometry's extent | |
| [`ST_ZMin(geom)`](#st_zmin-function) | Returns the smallest Z of the geometry's extent | |
| [`ST_ZMax(geom)`](#st_zmax-function) | Returns the largest Z of the geometry's extent | |
| [`ST_NPoints(geom)`](#st_npoints-function) | Returns the number of vertices | `ST_NumPoints` |
| [`ST_NumGeometries(geom)`](#st_numgeometries-function) | Returns the number of geometries in a collection | |
| [`ST_NumInteriorRings(geom)`](#st_numinteriorrings-function) | Returns the number of interior rings of a polygon | |
| [`ST_ExteriorRing(geom)`](#st_exteriorring-function) | Returns the exterior ring of a polygon | |
| [`ST_InteriorRingN(geom, n)`](#st_interiorringn-function) | Returns the nth interior ring of a polygon | |
| [`ST_PointN(geom, index)`](#st_pointn-function) | Returns the nth vertex of a line | |
| [`ST_StartPoint(geom)`](#st_startpoint-function) | Returns the first vertex of a line | |
| [`ST_EndPoint(geom)`](#st_endpoint-function) | Returns the last vertex of a line | |
| [`ST_Dump(geom)`](#st_dump-function) | Expands a collection into its parts with their paths | |
| [`ST_CollectionExtract(geom[, type])`](#st_collectionextract-function) | Extracts the elements of one dimension from a collection | |
| [`ST_HasZ(geom)`](#st_hasz-function) | Reports whether the geometry carries Z coordinates | |
| [`ST_HasM(geom)`](#st_hasm-function) | Reports whether the geometry carries M coordinates | |
| [`ST_ZMFlag(geom)`](#st_zmflag-function) | Returns a code for the coordinate dimensions | |
| [`ST_Extent(geom)`](#st_extent-function) | Returns the bounding box of the geometry | |

#### `ST_GeometryType(geom)` {#st_geometrytype-function}

Returns the geometry type, such as `POINT` or `LINESTRING`.

<SqlLogicTest id="sql/functions/geometry/st_geometrytype" />

#### `ST_Dimension(geom)` {#st_dimension-function}

Returns the topological dimension: `0` for points, `1` for lines, `2` for areas.

<SqlLogicTest id="sql/functions/geometry/st_dimension" />

#### `ST_X(geom)` {#st_x-function}

Returns the X coordinate of a point. The example also shows `ST_Y`.

<SqlLogicTest id="sql/functions/geometry/st_x" />

#### `ST_Y(geom)` {#st_y-function}

Returns the Y coordinate of a point. See the `ST_X` example.

#### `ST_Z(geom)` {#st_z-function}

Returns the Z coordinate of a point, or `NULL` when the geometry does not carry Z. The example also shows `ST_M`.

<SqlLogicTest id="sql/functions/geometry/st_z" />

#### `ST_M(geom)` {#st_m-function}

Returns the M coordinate of a point, or `NULL` when the geometry does not carry M. See the `ST_Z` example.

#### `ST_XMin(geom)` {#st_xmin-function}

Returns the smallest X of the geometry's extent. The example shows `ST_XMax`, `ST_YMin` and `ST_YMax` too.

<SqlLogicTest id="sql/functions/geometry/st_extremes" />

#### `ST_XMax(geom)` {#st_xmax-function}

Returns the largest X of the geometry's extent. See the `ST_XMin` example.

#### `ST_YMin(geom)` {#st_ymin-function}

Returns the smallest Y of the geometry's extent. See the `ST_XMin` example.

#### `ST_YMax(geom)` {#st_ymax-function}

Returns the largest Y of the geometry's extent. See the `ST_XMin` example.

#### `ST_ZMin(geom)` {#st_zmin-function}

Returns the smallest Z of the geometry's extent.

#### `ST_ZMax(geom)` {#st_zmax-function}

Returns the largest Z of the geometry's extent.

#### `ST_NPoints(geom)` {#st_npoints-function}

Returns the number of vertices in the geometry. Alias: `ST_NumPoints`.

<SqlLogicTest id="sql/functions/geometry/st_npoints" />

#### `ST_NumGeometries(geom)` {#st_numgeometries-function}

Returns the number of geometries in a collection. A single geometry counts as one.

<SqlLogicTest id="sql/functions/geometry/st_numgeometries" />

#### `ST_NumInteriorRings(geom)` {#st_numinteriorrings-function}

Returns the number of interior rings (holes) of a polygon.

<SqlLogicTest id="sql/functions/geometry/st_numinteriorrings" />

#### `ST_ExteriorRing(geom)` {#st_exteriorring-function}

Returns the exterior ring (the shell) of a polygon as a line.

<SqlLogicTest id="sql/functions/geometry/st_exteriorring" />

#### `ST_InteriorRingN(geom, n)` {#st_interiorringn-function}

Returns the nth interior ring of a polygon as a line. Rings are numbered from 1.

<SqlLogicTest id="sql/functions/geometry/st_interiorringn" />

#### `ST_PointN(geom, index)` {#st_pointn-function}

Returns the nth vertex of a line. Vertices are numbered from 1.

<SqlLogicTest id="sql/functions/geometry/st_pointn" />

#### `ST_StartPoint(geom)` {#st_startpoint-function}

Returns the first vertex of a line. The example also shows `ST_EndPoint`.

<SqlLogicTest id="sql/functions/geometry/st_startpoint" />

#### `ST_EndPoint(geom)` {#st_endpoint-function}

Returns the last vertex of a line. See the `ST_StartPoint` example.

#### `ST_Dump(geom)` {#st_dump-function}

Expands a collection into a list of its parts, each with the path that locates it. There is no `ST_GeometryN` in SereneDB, so this is how an individual part is reached.

<SqlLogicTest id="sql/functions/geometry/st_dump" />

#### `ST_CollectionExtract(geom[, type])` {#st_collectionextract-function}

Extracts the elements of one dimension from a collection, as a multi-geometry: `1` for points, `2` for lines, `3` for polygons. Without `type`, the highest dimension present is used.

<SqlLogicTest id="sql/functions/geometry/st_collectionextract" />

#### `ST_HasZ(geom)` {#st_hasz-function}

Reports whether the geometry carries Z coordinates. The example shows `ST_HasM` and `ST_ZMFlag` too.

<SqlLogicTest id="sql/functions/geometry/st_haszm" />

#### `ST_HasM(geom)` {#st_hasm-function}

Reports whether the geometry carries M coordinates. See the `ST_HasZ` example.

#### `ST_ZMFlag(geom)` {#st_zmflag-function}

Returns a code for the coordinate dimensions of the geometry: `0` for XY, `1` for XYM, `2` for XYZ, `3` for XYZM. See the `ST_HasZ` example.

#### `ST_Extent(geom)` {#st_extent-function}

Returns the bounding box of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_extent" />

## Measuring Geometries

These functions work in the geometry's own coordinate units. For measurements on the earth, see [Spheroidal Measurements](#spheroidal-measurements).

| Name | Description |
| :--- | :---------- |
| [`ST_Area(geom)`](#st_area-function) | Returns the area of the geometry |
| [`ST_Length(geom)`](#st_length-function) | Returns the length of the geometry |
| [`ST_Perimeter(geom)`](#st_perimeter-function) | Returns the perimeter of the geometry |
| [`ST_Distance(geom1, geom2)`](#st_distance-function) | Returns the shortest distance between two geometries |
| [`ST_Azimuth(origin, target)`](#st_azimuth-function) | Returns the bearing from one point to another, in radians |

#### `ST_Area(geom)` {#st_area-function}

Returns the area of the geometry. Points and lines have zero area.

<SqlLogicTest id="sql/functions/geometry/st_area" />

#### `ST_Length(geom)` {#st_length-function}

Returns the length of the geometry. Points and polygons have zero length; for a polygon's outline use `ST_Perimeter`.

<SqlLogicTest id="sql/functions/geometry/st_length" />

#### `ST_Perimeter(geom)` {#st_perimeter-function}

Returns the perimeter of the geometry, the total length of its rings.

<SqlLogicTest id="sql/functions/geometry/st_perimeter" />

#### `ST_Distance(geom1, geom2)` {#st_distance-function}

Returns the shortest distance between two geometries. Geometries that intersect are zero apart.

<SqlLogicTest id="sql/functions/geometry/st_distance" />

#### `ST_Azimuth(origin, target)` {#st_azimuth-function}

Returns the bearing from the point `origin` to the point `target`, in radians clockwise from north.

<SqlLogicTest id="sql/functions/geometry/st_azimuth" />

## Testing Relationships

| Name | Description | Aliases |
| :--- | :---------- | :------ |
| [`ST_Intersects(geom1, geom2)`](#st_intersects-function) | Returns true if the geometries share any point | |
| [`ST_Disjoint(geom1, geom2)`](#st_disjoint-function) | Returns true if the geometries share no point | |
| [`ST_Contains(geom1, geom2)`](#st_contains-function) | Returns true if the first geometry contains the second | |
| [`ST_Within(geom1, geom2)`](#st_within-function) | Returns true if the first geometry lies within the second | |
| [`ST_Covers(geom1, geom2)`](#st_covers-function) | Like `ST_Contains`, but a boundary point counts as covered | |
| [`ST_CoveredBy(geom1, geom2)`](#st_coveredby-function) | Like `ST_Within`, but a boundary point counts as covered | |
| [`ST_ContainsProperly(geom1, geom2)`](#st_containsproperly-function) | Returns true if the second geometry is in the first's interior | |
| [`ST_WithinProperly(geom1, geom2)`](#st_withinproperly-function) | Returns true if the first geometry is in the second's interior | |
| [`ST_Crosses(geom1, geom2)`](#st_crosses-function) | Returns true if the geometries cross | |
| [`ST_Overlaps(geom1, geom2)`](#st_overlaps-function) | Returns true if the geometries overlap at their own dimension | |
| [`ST_Touches(geom1, geom2)`](#st_touches-function) | Returns true if the geometries meet only at their boundaries | |
| [`ST_Equals(geom1, geom2)`](#st_equals-function) | Returns true if the geometries cover the same space | |
| [`ST_DWithin(geom1, geom2, distance)`](#st_dwithin-function) | Returns true if the geometries are within a given distance | |
| [`ST_Intersects_Extent(geom1, geom2)`](#st_intersects_extent-function) | Returns true if the geometries bounding boxes intersect | `&&` |
| [`ST_IsValid(geom)`](#st_isvalid-function) | Reports whether the geometry is valid | |
| [`ST_IsEmpty(geom)`](#st_isempty-function) | Reports whether the geometry holds no points | |
| [`ST_IsClosed(geom)`](#st_isclosed-function) | Reports whether a line starts and ends at the same point | |
| [`ST_IsRing(geom)`](#st_isring-function) | Reports whether a line is closed and simple | |
| [`ST_IsSimple(geom)`](#st_issimple-function) | Reports whether a geometry has no self-intersections | |

#### `ST_Intersects(geom1, geom2)` {#st_intersects-function}

Returns true if the geometries share any point.

<SqlLogicTest id="sql/functions/geometry/st_intersects" />

#### `ST_Disjoint(geom1, geom2)` {#st_disjoint-function}

Returns true if the geometries share no point. The exact inverse of `ST_Intersects`.

<SqlLogicTest id="sql/functions/geometry/st_disjoint" />

#### `ST_Contains(geom1, geom2)` {#st_contains-function}

Returns true if the first geometry contains the second. The example also shows `ST_Within`.

<SqlLogicTest id="sql/functions/geometry/st_contains" />

#### `ST_Within(geom1, geom2)` {#st_within-function}

Returns true if the first geometry lies within the second. It is `ST_Contains` with the arguments swapped. See the `ST_Contains` example.

#### `ST_Covers(geom1, geom2)` {#st_covers-function}

Like `ST_Contains`, except that a point lying on the boundary counts as covered. The example shows the one case where the two disagree: the polygon's own corner.

<SqlLogicTest id="sql/functions/geometry/st_covers" />

#### `ST_CoveredBy(geom1, geom2)` {#st_coveredby-function}

Returns true if every point of the first geometry lies in the second, boundary included. It is `ST_Covers` with the arguments swapped.

#### `ST_ContainsProperly(geom1, geom2)` {#st_containsproperly-function}

Returns true if the second geometry lies in the interior of the first, touching neither its boundary nor its exterior.

<SqlLogicTest id="sql/functions/geometry/st_containsproperly" />

#### `ST_WithinProperly(geom1, geom2)` {#st_withinproperly-function}

Returns true if the first geometry lies in the interior of the second. It is `ST_ContainsProperly` with the arguments swapped.

#### `ST_Crosses(geom1, geom2)` {#st_crosses-function}

Returns true if the geometries cross, meaning they share some but not all interior points and the shared part has a lower dimension than at least one of them.

<SqlLogicTest id="sql/functions/geometry/st_crosses" />

#### `ST_Overlaps(geom1, geom2)` {#st_overlaps-function}

Returns true if the geometries have the same dimension and share some, but not all, of their interiors.

<SqlLogicTest id="sql/functions/geometry/st_overlaps" />

#### `ST_Touches(geom1, geom2)` {#st_touches-function}

Returns true if the geometries meet only at their boundaries, with no shared interior.

<SqlLogicTest id="sql/functions/geometry/st_touches" />

#### `ST_Equals(geom1, geom2)` {#st_equals-function}

Returns true if the geometries cover the same space, regardless of vertex order or representation.

<SqlLogicTest id="sql/functions/geometry/st_equals" />

#### `ST_DWithin(geom1, geom2, distance)` {#st_dwithin-function}

Returns true if the geometries are within `distance` of one another.

<SqlLogicTest id="sql/functions/geometry/st_dwithin" />

#### `ST_Intersects_Extent(geom1, geom2)` {#st_intersects_extent-function}

Returns true if the geometries bounding boxes intersect. Alias: `&&`.

<SqlLogicTest id="sql/functions/geometry/st_intersects_extent" />

#### `ST_IsValid(geom)` {#st_isvalid-function}

Reports whether the geometry is valid. The example also shows `ST_IsEmpty`.

<SqlLogicTest id="sql/functions/geometry/st_isvalid" />

#### `ST_IsEmpty(geom)` {#st_isempty-function}

Reports whether the geometry holds no points. See the `ST_IsValid` example.

#### `ST_IsClosed(geom)` {#st_isclosed-function}

Reports whether a line starts and ends at the same point. The example shows `ST_IsRing` and `ST_IsSimple` too.

<SqlLogicTest id="sql/functions/geometry/st_isclosed" />

#### `ST_IsRing(geom)` {#st_isring-function}

Reports whether a line is closed and simple. See the `ST_IsClosed` example.

#### `ST_IsSimple(geom)` {#st_issimple-function}

Reports whether a geometry has no self-intersections. See the `ST_IsClosed` example.

## Deriving New Geometries

| Name | Description |
| :--- | :---------- |
| [`ST_Intersection(geom1, geom2)`](#st_intersection-function) | Returns the part shared by both geometries |
| [`ST_Union(geom1, geom2)`](#st_union-function) | Returns the combination of both geometries |
| [`ST_Difference(geom1, geom2)`](#st_difference-function) | Returns the part of the first geometry not in the second |
| [`ST_SymDifference(geom1, geom2)`](#st_symdifference-function) | Returns the parts belonging to exactly one of the geometries |
| [`ST_Buffer(geom, distance[, num_triangles])`](#st_buffer-function) | Returns the area within a given distance of the geometry |
| [`ST_ConvexHull(geom)`](#st_convexhull-function) | Returns the smallest convex geometry enclosing the input |
| [`ST_Simplify(geom, tolerance)`](#st_simplify-function) | Removes vertices that fall within a tolerance |
| [`ST_Centroid(geom)`](#st_centroid-function) | Returns the centre of mass of the geometry |
| [`ST_Envelope(geom)`](#st_envelope-function) | Returns the bounding box as a polygon |
| [`ST_Boundary(geom)`](#st_boundary-function) | Returns the boundary of the geometry |
| [`ST_PointOnSurface(geom)`](#st_pointonsurface-function) | Returns a point guaranteed to lie on the geometry |
| [`ST_ClosestPoint(geom1, geom2)`](#st_closestpoint-function) | Returns the point of the first geometry closest to the second |
| [`ST_ShortestLine(geom1, geom2)`](#st_shortestline-function) | Returns the shortest line between two geometries |

#### `ST_Intersection(geom1, geom2)` {#st_intersection-function}

Returns the part shared by both geometries.

<SqlLogicTest id="sql/functions/geometry/st_intersection" />

#### `ST_Union(geom1, geom2)` {#st_union-function}

Returns the combination of both geometries. Both arguments must have the same dimension, because a mixed result would need a `GEOMETRYCOLLECTION`.

<SqlLogicTest id="sql/functions/geometry/st_union" />

#### `ST_Difference(geom1, geom2)` {#st_difference-function}

Returns the part of the first geometry that is not in the second.

<SqlLogicTest id="sql/functions/geometry/st_difference" />

#### `ST_SymDifference(geom1, geom2)` {#st_symdifference-function}

Returns the parts that belong to exactly one of the geometries. Like `ST_Union`, both arguments must have the same dimension.

<SqlLogicTest id="sql/functions/geometry/st_symdifference" />

#### `ST_Buffer(geom, distance[, num_triangles])` {#st_buffer-function}

Returns the area within `distance` of the geometry. The optional `num_triangles` sets the number of triangles used per quarter circle, which controls how round the result is.

<SqlLogicTest id="sql/functions/geometry/st_buffer" />

#### `ST_ConvexHull(geom)` {#st_convexhull-function}

Returns the smallest convex geometry that encloses the input.

<SqlLogicTest id="sql/functions/geometry/st_convexhull" />

#### `ST_Simplify(geom, tolerance)` {#st_simplify-function}

Removes vertices that fall within `tolerance`, using the Douglas-Peucker algorithm. The result may be invalid or may break topology shared with neighbouring geometries; there is no `ST_SimplifyPreserveTopology`.

<SqlLogicTest id="sql/functions/geometry/st_simplify" />

#### `ST_Centroid(geom)` {#st_centroid-function}

Returns the centre of mass of the geometry, which is not necessarily on the geometry itself. For a point that is, use `ST_PointOnSurface`.

<SqlLogicTest id="sql/functions/geometry/st_centroid" />

#### `ST_Envelope(geom)` {#st_envelope-function}

Returns the bounding box of the geometry as a polygon. `ST_Extent` returns the same bounds as a box value instead.

<SqlLogicTest id="sql/functions/geometry/st_envelope" />

#### `ST_Boundary(geom)` {#st_boundary-function}

Returns the boundary of the geometry: the rings of a polygon, the endpoints of a line.

<SqlLogicTest id="sql/functions/geometry/st_boundary" />

#### `ST_PointOnSurface(geom)` {#st_pointonsurface-function}

Returns a point guaranteed to lie on the geometry.

<SqlLogicTest id="sql/functions/geometry/st_pointonsurface" />

#### `ST_ClosestPoint(geom1, geom2)` {#st_closestpoint-function}

Returns the point of the first geometry that lies closest to the second.

<SqlLogicTest id="sql/functions/geometry/st_closestpoint" />

#### `ST_ShortestLine(geom1, geom2)` {#st_shortestline-function}

Returns the shortest line between two geometries.

<SqlLogicTest id="sql/functions/geometry/st_shortestline" />

## Editing Geometries

| Name | Description | Aliases |
| :--- | :---------- | :------ |
| [`ST_Affine(geom, a, b, d, e, xoff, yoff)`](#st_affine-function) | Applies a two dimensional affine transformation to every vertex | |
| [`ST_Affine(geom, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff)`](#st_affine-3d-function) | Applies a three dimensional affine transformation to every vertex | |
| [`ST_Translate(geom, dx, dy[, dz])`](#st_translate-function) | Moves the geometry by an offset | |
| [`ST_Scale(geom, xs, ys[, zs])`](#st_scale-function) | Scales the geometry by a factor per axis | |
| [`ST_TransScale(geom, dx, dy, xs, ys)`](#st_transscale-function) | Moves and then scales the geometry | |
| [`ST_RotateZ(geom, radians)`](#st_rotatez-function) | Rotates the geometry around the Z axis | `ST_Rotate` |
| [`ST_RotateX(geom, radians)`](#st_rotatex-function) | Rotates the geometry around the X axis | |
| [`ST_RotateY(geom, radians)`](#st_rotatey-function) | Rotates the geometry around the Y axis | |
| [`ST_Expand(geom, distance)`](#st_expand-function) | Returns the bounding box grown by a distance | |
| [`ST_Reverse(geom)`](#st_reverse-function) | Reverses the vertex order | |
| [`ST_Normalize(geom)`](#st_normalize-function) | Rewrites the geometry into a canonical form | |
| [`ST_FlipCoordinates(geom)`](#st_flipcoordinates-function) | Swaps the X and Y of every vertex | |
| [`ST_Force2D(geom)`](#st_force2d-function) | Drops the Z and M dimensions | |
| [`ST_RemoveRepeatedPoints(geom)`](#st_removerepeatedpoints-function) | Removes consecutive duplicate vertices | |

#### `ST_Affine(geom, a, b, d, e, xoff, yoff)` {#st_affine-function}

Applies an affine transformation to every vertex, mapping it to `(a*x + b*y + xoff, d*x + e*y + yoff)`.

<SqlLogicTest id="sql/functions/geometry/st_affine" />

#### `ST_Affine(geom, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff)` {#st_affine-3d-function}

Applies an affine transformation in three dimensions, mapping every vertex to `(a*x + b*y + c*z + xoff, d*x + e*y + f*z + yoff, g*x + h*y + i*z + zoff)`.

#### `ST_Translate(geom, dx, dy[, dz])` {#st_translate-function}

Moves every vertex by `dx` along X, `dy` along Y and, when given, `dz` along Z. Without `dz` the Z coordinates are left as they are.

<SqlLogicTest id="sql/functions/geometry/st_translate" />

#### `ST_Scale(geom, xs, ys[, zs])` {#st_scale-function}

Multiplies the coordinates of every vertex by `xs`, `ys` and, when given, `zs`.

<SqlLogicTest id="sql/functions/geometry/st_scale" />

#### `ST_TransScale(geom, dx, dy, xs, ys)` {#st_transscale-function}

Moves every vertex by `dx` and `dy`, then scales it by `xs` and `ys`.

<SqlLogicTest id="sql/functions/geometry/st_transscale" />

#### `ST_RotateZ(geom, radians)` {#st_rotatez-function}

Rotates the geometry counterclockwise around the Z axis, which is the plain rotation of a two dimensional geometry about the origin. Alias: `ST_Rotate`.

<SqlLogicTest id="sql/functions/geometry/st_rotate" />

#### `ST_RotateX(geom, radians)` {#st_rotatex-function}

Rotates the geometry around the X axis. The example shows all three axes.

<SqlLogicTest id="sql/functions/geometry/st_rotate_axes" />

#### `ST_RotateY(geom, radians)` {#st_rotatey-function}

Rotates the geometry around the Y axis. See the `ST_RotateX` example.

#### `ST_Expand(geom, distance)` {#st_expand-function}

Returns the bounding box of the geometry grown by `distance` in every direction.

<SqlLogicTest id="sql/functions/geometry/st_expand" />

#### `ST_Reverse(geom)` {#st_reverse-function}

Reverses the vertex order of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_reverse" />

#### `ST_Normalize(geom)` {#st_normalize-function}

Rewrites the geometry into a canonical form, so that geometries covering the same space become identical.

<SqlLogicTest id="sql/functions/geometry/st_normalize" />

#### `ST_FlipCoordinates(geom)` {#st_flipcoordinates-function}

Swaps the X and Y of every vertex. Useful for data stored in latitude/longitude order.

<SqlLogicTest id="sql/functions/geometry/st_flipcoordinates" />

#### `ST_Force2D(geom)` {#st_force2d-function}

Drops the Z and M dimensions from the geometry.

<SqlLogicTest id="sql/functions/geometry/st_force2d" />

#### `ST_RemoveRepeatedPoints(geom)` {#st_removerepeatedpoints-function}

Removes consecutive duplicate vertices.

<SqlLogicTest id="sql/functions/geometry/st_removerepeatedpoints" />

## Linear Referencing

| Name | Description |
| :--- | :---------- |
| [`ST_LineInterpolatePoint(line, fraction)`](#st_lineinterpolatepoint-function) | Returns the point at a fraction along a line |
| [`ST_LineInterpolatePoints(line, fraction, repeat)`](#st_lineinterpolatepoints-function) | Returns the points at a repeating fraction along a line |
| [`ST_LineSubstring(line, start_fraction, end_fraction)`](#st_linesubstring-function) | Returns the part of a line between two fractions |
| [`ST_LineLocatePoint(line, point)`](#st_linelocatepoint-function) | Returns the fraction along a line closest to a point |
| [`ST_LocateAlong(line, measure[, offset])`](#st_locatealong-function) | Returns the positions on a measured line at a measure |
| [`ST_LocateBetween(line, start_measure, end_measure[, offset])`](#st_locatebetween-function) | Returns the part of a measured line between two measures |
| [`ST_InterpolatePoint(line, point)`](#st_interpolatepoint-function) | Returns the measure of a line at the position closest to a point |

#### `ST_LineInterpolatePoint(line, fraction)` {#st_lineinterpolatepoint-function}

Returns the point at `fraction` along a line, where the fraction runs from `0` at the start to `1` at the end.

<SqlLogicTest id="sql/functions/geometry/st_lineinterpolatepoint" />

#### `ST_LineInterpolatePoints(line, fraction, repeat)` {#st_lineinterpolatepoints-function}

Returns the point at `fraction` along a line and, when `repeat` is true, another point at every further multiple of `fraction`, as a `MULTIPOINT`.

#### `ST_LineSubstring(line, start_fraction, end_fraction)` {#st_linesubstring-function}

Returns the part of a line between two fractions of its length.

<SqlLogicTest id="sql/functions/geometry/st_linesubstring" />

#### `ST_LineLocatePoint(line, point)` {#st_linelocatepoint-function}

Returns the fraction along a line at which it passes closest to the given point.

<SqlLogicTest id="sql/functions/geometry/st_linelocatepoint" />

#### `ST_LocateAlong(line, measure[, offset])` {#st_locatealong-function}

Returns the positions on a line carrying M values where the measure equals `measure`. A positive `offset` moves the result that far to the right of the line's direction, a negative one to the left.

<SqlLogicTest id="sql/functions/geometry/st_locatealong" />

#### `ST_LocateBetween(line, start_measure, end_measure[, offset])` {#st_locatebetween-function}

Returns the part of a line carrying M values that falls between two measures. `offset` works as in `ST_LocateAlong`.

<SqlLogicTest id="sql/functions/geometry/st_locatebetween" />

#### `ST_InterpolatePoint(line, point)` {#st_interpolatepoint-function}

Returns the M value of a line at the position closest to `point`. It is the inverse of `ST_LocateAlong`.

## Coordinate Reference Systems

SereneDB attaches the coordinate reference system to the `GEOMETRY` type itself rather than storing an SRID number alongside each value, so `ST_SRID` and `ST_SetSRID` do not exist. `ST_CRS` and `ST_SetCRS` take their place, and identifiers are strings such as `OGC:CRS84`.

A CRS is a label that SereneDB tracks and checks. It does not reproject: there is no `ST_Transform`, because reprojection needs a database of coordinate-system definitions that SereneDB does not ship. Convert coordinates before loading them, and use `ST_SetCRS` to record which system the result is in. Measuring on the WGS84 ellipsoid needs no such database and is supported, see [Spheroidal Measurements](#spheroidal-measurements).

| Name | Description |
| :--- | :---------- |
| [`ST_CRS(geom)`](#st_crs-function) | Returns the CRS identifier of the geometry |
| [`ST_SetCRS(geom, crs)`](#st_setcrs-function) | Sets the CRS identifier of the geometry |

#### `ST_CRS(geom)` {#st_crs-function}

Returns the Coordinate Reference System (CRS) identifier of the geometry.

<SqlLogicTest id="sql/functions/geometry/st_crs" />

#### `ST_SetCRS(geom, crs)` {#st_setcrs-function}

Sets the Coordinate Reference System (CRS) identifier of the geometry. The coordinates are left as they are; only the label changes.

<SqlLogicTest id="sql/functions/geometry/st_setcrs" />

## Spheroidal Measurements

These functions measure on the earth rather than in the coordinate plane and return metres. They take **X as the latitude and Y as the longitude**, which is the opposite of the order used by the planar functions on this page.

`ST_Distance_Sphere` treats the earth as a sphere, which is fast and approximate. The `*_Spheroid` functions use the WGS84 ellipsoid, which is slower and accurate.

| Name | Description |
| :--- | :---------- |
| [`ST_Distance_Sphere(geom1, geom2)`](#st_distance_sphere-function) | Returns the great-circle distance between two points |
| [`ST_Length_Spheroid(geom)`](#st_length_spheroid-function) | Returns the length of a line on the WGS84 spheroid |
| [`ST_Area_Spheroid(geom)`](#st_area_spheroid-function) | Returns the area of a polygon on the WGS84 spheroid |
| [`ST_Perimeter_Spheroid(geom)`](#st_perimeter_spheroid-function) | Returns the perimeter of a polygon on the WGS84 spheroid |

#### `ST_Distance_Sphere(geom1, geom2)` {#st_distance_sphere-function}

Returns the great-circle distance between two points in metres, treating the earth as a sphere. The example measures one degree along the equator.

<SqlLogicTest id="sql/functions/geometry/st_distance_sphere" />

#### `ST_Length_Spheroid(geom)` {#st_length_spheroid-function}

Returns the length of a line in metres on the WGS84 spheroid. The example spans one degree of latitude, which is why the result is close to 110.6 km rather than the 111.3 km of a degree of longitude at the equator.

<SqlLogicTest id="sql/functions/geometry/st_length_spheroid" />

#### `ST_Area_Spheroid(geom)` {#st_area_spheroid-function}

Returns the area of a polygon in square metres on the WGS84 spheroid.

<SqlLogicTest id="sql/functions/geometry/st_area_spheroid" />

#### `ST_Perimeter_Spheroid(geom)` {#st_perimeter_spheroid-function}

Returns the perimeter of a polygon in metres on the WGS84 spheroid.

<SqlLogicTest id="sql/functions/geometry/st_perimeter_spheroid" />

## Spatial Ordering and Tiling

| Name | Description |
| :--- | :---------- |
| [`ST_Hilbert(geom[, bounds])`](#st_hilbert-function) | Returns the Hilbert curve index of the geometry's centre |
| [`ST_Hilbert(x, y, bounds)`](#st_hilbert-xy-function) | Returns the Hilbert curve index of a coordinate pair |
| [`ST_QuadKey(point, level)`](#st_quadkey-function) | Returns the Bing Maps quadkey for a point at a zoom level |
| [`ST_QuadKey(longitude, latitude, level)`](#st_quadkey-lonlat-function) | Returns the Bing Maps quadkey for a longitude and latitude |
| [`ST_TileEnvelope(tile_zoom, tile_x, tile_y)`](#st_tileenvelope-function) | Returns the Web Mercator extent of an XYZ tile |

#### `ST_Hilbert(geom[, bounds])` {#st_hilbert-function}

Returns the Hilbert curve index of the geometry's centre, which sorts nearby geometries close together. The optional `bounds` is the box, such as one from `ST_Extent`, that the curve covers.

<SqlLogicTest id="sql/functions/geometry/st_hilbert" />

#### `ST_Hilbert(x, y, bounds)` {#st_hilbert-xy-function}

Returns the Hilbert curve index of the coordinate pair `x`, `y` within the box `bounds`.

#### `ST_QuadKey(point, level)` {#st_quadkey-function}

Returns the Bing Maps quadkey for a point at zoom level `level`.

<SqlLogicTest id="sql/functions/geometry/st_quadkey" />

#### `ST_QuadKey(longitude, latitude, level)` {#st_quadkey-lonlat-function}

Returns the Bing Maps quadkey for a longitude and latitude at zoom level `level`.

#### `ST_TileEnvelope(tile_zoom, tile_x, tile_y)` {#st_tileenvelope-function}

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
| `ST_Node`, `ST_Polygonize`, `ST_BuildArea` | -- |
| `ST_LineMerge` | -- |
| `ST_SimplifyPreserveTopology` | `ST_Simplify`, which may break topology |
| `ST_ConcaveHull` | `ST_ConvexHull` |
| `ST_ReducePrecision` | -- |
| `ST_MaximumInscribedCircle` | `ST_PointOnSurface` for a point known to be inside |
| `ST_VoronoiDiagram` | -- |
| `ST_MinimumRotatedRectangle` | `ST_Envelope`, which is axis-aligned |
| `ST_GeometryN` | `ST_Dump`, which expands every part at once |
| `ST_SRID`, `ST_SetSRID` | `ST_CRS` and `ST_SetCRS`, which use string identifiers |
| `ST_Relate` | the individual predicates |
| `ST_Segmentize`, `ST_Split`, `ST_Snap`, `ST_OffsetCurve` | -- |
| `ST_GeoHash` | `ST_QuadKey`, `ST_Hilbert` |

Four further differences apply to functions that do exist:

-   **`ST_Union` and `ST_SymDifference` require both arguments to have the same dimension.** Combining a point with a polygon would produce a `GEOMETRYCOLLECTION`, which cannot be built. `ST_Intersection` and `ST_Difference` accept mixed dimensions.
-   **The Boost-backed operations drop `Z` and `M` from their results.** Measurement and predicates are computed in two dimensions, as they are in PostGIS, but PostGIS carries the extra dimensions through to the result and these do not: `ST_Envelope`, `ST_Boundary`, `ST_ConvexHull`, `ST_Simplify`, `ST_Intersection`, `ST_PointOnSurface`, `ST_Normalize` and `ST_RemoveRepeatedPoints`. The functions that move vertices around rather than computing new ones keep every dimension: `ST_Reverse`, `ST_Multi`, `ST_Points`, `ST_StartPoint`, `ST_EndPoint` and `ST_PointN`, and so does `ST_Centroid`.
-   **Only some functions accept a `GEOMETRYCOLLECTION`.** Those that can answer member by member do: `ST_Reverse`, `ST_Centroid`, `ST_Envelope`, `ST_ConvexHull`, `ST_Area`, `ST_Length`, `ST_NumGeometries`, `ST_IsValid`, `ST_IsEmpty`, and the `ST_Intersects` / `ST_Disjoint` predicates. `ST_Boundary` returns `NULL` for one. Everything else rejects it, because a collection's answer is not the combination of its parts' answers -- the other predicates, `ST_Buffer` and `ST_Simplify` among them.
-   **An empty geometry is valid, and empty input yields `NULL` where a geometry is expected.** `ST_IsValid('LINESTRING EMPTY')` is true, and `ST_ClosestPoint` and `ST_ShortestLine` return `NULL` when either argument is empty rather than raising.
