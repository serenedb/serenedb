---
title: Geometry
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name       | Description       |
| :--------- | :---------------- |
| `GEOMETRY` | Geospatial entity |

The `GEOMETRY` data type is used to store and manipulate geometric objects, such as points, lines, and polygons.

## Types of Geometries

Conceptually, the `GEOMETRY` type follows the core data model defined in the [Simple Features](https://en.wikipedia.org/wiki/Simple_Features) standard, which is widely used in geospatial databases and GIS software. A `GEOMETRY` value can therefore represent 7 types of shapes:

| Geometry Type          | Description                                                                                                                                                                                                       |
| :--------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Point**              | A single location in space, defined by its coordinates (e.g., longitude and latitude).                                                                                                                            |
| **LineString**         | A sequence of points connected by straight lines, representing a path or route.                                                                                                                                   |
| **Polygon**            | A set of closed rings defined by a sequence of points, representing an area such as a country border or a building footprint. The first ring is the "shell", and "interior" rings represent holes in the polygon. |
| **MultiPoint**         | A collection of points.                                                                                                                                                                                           |
| **MultiLineString**    | A collection of LineStrings.                                                                                                                                                                                      |
| **MultiPolygon**       | A collection of Polygons.                                                                                                                                                                                         |
| **GeometryCollection** | A collection of different geometry types, allowing for complex geometries that combine points, lines, and polygons or even other nested geometry collections.                                                     |

The textual representation of geometries uses ["Well-Known Text" (WKT)](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format. Geometries can be cast to and from WKT strings, so you can use string literals to create geometries directly in SQL statements.

In the following example, we create a `GEOMETRY` column with the 7 different types of supported geometries:

<SqlLogicTest id="sql/data_types/geometry/example_001" />

## Multi-Dimensional Geometries

The `GEOMETRY` type is primarily used to model shapes in two dimensions (e.g. `X`/`Y` or `longitude`/`latitude`), but it also supports shapes with additional vertex dimensions such as `Z` for elevation or `M` for "measure", or both.

The vertex dimensions of a `GEOMETRY` value must be consistent across all vertices. For example, if one vertex has `X`, `Y`, and `Z` coordinates, then all other vertices in that geometry must also have `X`, `Y`, and `Z` coordinates. This means that you cannot have a mix of 2D and 3D vertices within the same geometry. This also applies for collections of geometries, such as `MULTIPOINT` or `GEOMETRYCOLLECTION`, where all geometries within the collection must have the same vertex dimensions.

Functions that operate on `GEOMETRY` values typically ignore any additional dimensions beyond the `X` and `Y` unless explicitly specified, but they can still be stored and can be retrieved if needed.

In the following example, we create a `GEOMETRY` table with 2D, 3D(Z), 3D(M) and 4D(ZM) points:

<SqlLogicTest id="sql/data_types/geometry/example_002" />

## Empty Geometries

Geometries can also be "empty" (e.g., `POINT EMPTY`, `LINESTRING EMPTY`, `MULTIPOLYGON EMPTY`, etc.) which means they don't contain any vertices. Empty geometries are still valid geometries and can be used in spatial operations, but they are mostly useful for representing the result of topological operations that don't have a valid geometrical representation (e.g., the intersection of two non-overlapping geometries is an empty geometry).

## Geometry Storage

### Shredding and Compression

The `GEOMETRY` type supports a storage optimization called "shredding", which improves compression for geometry columns where all values share the same geometry type and vertex dimensions.

When a row group qualifies, SereneDB splits the geometry segment within the row group into primitive `STRUCT`, `LIST`, and `DOUBLE` segments that can be compressed independently using lightweight algorithms - far more efficiently than storing variable-size binary blobs.

The shredded layout depends on the geometry type:

-   `POINT` - STRUCT(X DOUBLE, Y DOUBLE) (and/or Z, M)
-   `LINESTRING` - STRUCT(X DOUBLE, Y DOUBLE)[]
-   `POLYGON` - STRUCT(X DOUBLE, Y DOUBLE)[][]
-   `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON` - same as above, with one additional level of list nesting

Row groups are not shredded if they contain `GEOMETRYCOLLECTION`s, any `EMPTY` geometries, or multiple geometry sub-types.

Additionally, row groups are not shredded if they fall below the minimum size threshold (default: ~25% of the maximum row group size, i.e., 30,000 rows).

This threshold is configurable via the `geometry_minimum_shredding_size` setting. Set it to `0` to always shred, or `-1` to disable shredding entirely.

<SqlLogicTest id="sql/data_types/geometry/example_003" />

The primary benefit of shredding is significantly improved compression, but in the future we plan to add ways to expose the shredded representation directly to the execution engine without having to "reassemble" the geometry back into binary again.

The following example illustrates the effects of shredding on the storage footprint of a `GEOMETRY` column.

<SqlLogicTest id="sql/data_types/geometry/example_004" />

### Geometry Statistics

`GEOMETRY` columns contain geometry-specific statistics that track the bounding box of the geometries in each row group, as well as the set of geometry types and vertex dimensions that are present within the row group.

You can inspect the statistics of a column using the `stats()` function:

<SqlLogicTest id="sql/data_types/geometry/example_005" />

These statistics can be used by the query optimizer to skip row groups that don't match the geometry type or vertex dimensions required by a query, or to speed up spatial predicates by first checking if the bounding box of the geometries in the row group overlaps with the bounding box of the query geometry.

Currently, only the `&&` operator, which is used to check if the bounding box of a geometry intersects the bounding box of another geometry, can take advantage of geometry statistics when used in a `WHERE` clause. There is ongoing work to add support for more statistics-based optimizations to spatial functions, such as `ST_Intersects`, `ST_Distance`, etc.

Persisting geometry statistics is only possible in storage versions v1.5 and above, and so if you are using an older storage version, the geometry statistics will turn into "unknown" statistics when checkpointing. In other words, the bounding box will be set to an infinitely large bounding box and all geometry types and vertex dimensions will be marked as maybe present, which means that the execution engine will not be able to do any optimizations based on the geometry statistics.

## Coordinate Reference Systems

As far as the execution engine is concerned, geometries are considered to exist in a Cartesian coordinate system. In practice, however, most geospatial data is associated with a specific **Coordinate Reference System** (CRS) that defines how the coordinates relate to real-world locations on the Earth's surface.

A helpful analogy is to think of CRSs as the equivalent of "time zones", but for geospatial data. Just like how time zones define how local time relates to a standard reference time (e.g., UTC), CRSs define how the coordinates of a geometry relate to a standard reference system (e.g., WGS 84). CRSs are usually either geographic (e.g., WGS 84, which uses latitude and longitude) or projected (e.g., UTM, which uses linear units like meters).

When working with geospatial data, it's important to be aware of the CRS associated with different datasets. Performing spatial operations on geometries in different CRSs without proper transformation will most likely lead to incorrect results.

### How are Coordinate Reference Systems Stored in SereneDB?

To avoid these kinds of mistakes, SereneDB makes it possible to explicitly associate a CRS with a `GEOMETRY` column.

This is done by passing a CRS "identifier" as a parameter of the `GEOMETRY` type. For example, a column of type `GEOMETRY('OGC:CRS84')` stores geometries that are associated with the "OGC CRS84" coordinate reference system.

CRS identifiers in SereneDB are always strings. `OGC:CRS84` is the identifier for a common geographic coordinate system spanning the whole globe where the `X` coordinate represents longitude and the `Y` coordinate represents latitude. SereneDB only knows this because the identifier 'OGC:CRS84' is registered as a _known_ CRS in the system catalog.

Only a handful of common CRSs are registered as known in this build of SereneDB. Registering additional CRSs, such as the over 7000 CRSs from the [EPSG Geodetic Parameter Dataset](https://epsg.org/home.html), is not available in this build.

You can list all available CRSs known to SereneDB using the [`duckdb_coordinate_systems()`](../../sql/functions/duckdb_table_functions.md#duckdb_coordinate_systems) function:

<SqlLogicTest id="sql/data_types/geometry/example_006" />

### Handling Unknown Coordinate Reference Systems

As mentioned above, only coordinate systems that are registered in the system catalog (and therefore "known" to SereneDB) can be used when creating `GEOMETRY` columns.
If you try to create a `GEOMETRY` column with an unknown CRS identifier, either manually or by importing an external geospatial dataset, the statement will fail with an error.

<SqlLogicTest id="sql/data_types/geometry/example_007" />

This restriction exists because SereneDB needs the complete CRS definition, not just an identifier, to perform coordinate transformations and to export to formats that embed CRS metadata, such as GeoParquet. Without a system catalog entry, there is no way to resolve an identifier to its full definition.

You can set the `ignore_unknown_crs` configuration option to `true` to simply skip any unknown CRSs and create `GEOMETRY` columns without CRS instead.

<SqlLogicTest id="sql/data_types/geometry/example_008" />

Alternatively, if you are trying to define a `GEOMETRY` column yourself, you can provide a complete CRS definition in WKT or PROJJSON format instead of a shorthand identifier as the CRS parameter. However, as complete CRS definitions are usually very large, this gets unwieldy very quickly and is not recommended for interactive use.

It is currently not possible to define a custom CRS from within SQL, or to persist custom CRS definitions in a database such that SereneDB can use them to resolve CRS identifiers for geometry columns, but this is something we are considering for the future.

### Working with Geometries in Different Coordinate Reference Systems

One benefit of tracking CRSs as part of the type system is that it prevents a lot of common mistakes that can occur when working with geometries from different coordinate systems. Most spatial functions that operate on multiple `GEOMETRY` values verify that all input expressions have the same CRS before performing the operation. Similarly, `GEOMETRY` columns can only be implicitly cast to and from other `GEOMETRY` columns if the source or the target don't have a CRS specified.

Converting a geometry from one CRS to another with `ST_Transform(geom, crs)` is not available in this build of SereneDB.

You can also use the `ST_SetCRS(geom, crs)` function to assign a CRS to a geometry that doesn't have one, or to reassign a CRS without transforming coordinates (e.g., when the data is already in the correct coordinate system but lacks the correct CRS).

<SqlLogicTest id="sql/data_types/geometry/example_010" />

Or if you want to remove the CRS from a geometry, you can either just cast to `GEOMETRY`, or set the CRS to `''`:

<SqlLogicTest id="sql/data_types/geometry/example_011" />

You can of course also use `ST_CRS(geom)` to retrieve the CRS of a geometry:

<SqlLogicTest id="sql/data_types/geometry/example_012" />

## Functions

-   See [geometry functions](../../sql/functions/geometry.md) for the list of built-in geometry functions.
