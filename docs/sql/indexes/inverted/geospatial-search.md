---
title: Geospatial Search
sidebar_position: 9
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The [inverted index](./index.md) indexes geographic shapes for fast spatial predicates — finding which points fall inside a region, which regions contain a location and so on. Geometries can be stored either as [GeoJSON](https://geojson.org/) in a `JSON` column or as values of the native `GEOMETRY` type, and both are indexed with a geospatial text search dictionary.

## Creating a geospatial index

Use the `encode_geojson` dictionary template on a `JSON` column holding GeoJSON. The column type must be `JSON` or `GEOMETRY` — a plain `VARCHAR` is rejected for geo analyzers:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_001" />

The `encode_geojson` template indexes `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString` and `MultiPolygon` geometries. A row holding a `GeometryCollection`, or a value that is not valid GeoJSON, is stored without an error but gets no geo terms, so the spatial predicates below never find it. GeoJSON coordinates are `[longitude, latitude]`. An `encode_geopoint` template is also available for extracting a point from latitude/longitude fields of a JSON object, and the `encode_geojson` template accepts `coding := 's2point'` to store a compact [S2](http://s2geometry.io/) encoding of each geometry instead of the source value.

### Indexing a `GEOMETRY` column

The native [`GEOMETRY`](../../data_types/geometry.md) type is indexed the same way, with two requirements. The column must be declared with the CRS84 coordinate reference system, as `GEOMETRY('OGC:CRS84')`: without one, or with another, `CREATE INDEX` fails. And it takes only the `encode_geojson` template, with the default `source` coding or with `coding := 's2point'`; `encode_geopoint` reads JSON fields and needs a `JSON` column. Values are written from WKT, which uses the same `longitude latitude` axis order as GeoJSON:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_005" />

Query shapes may be supplied as GeoJSON text (as above) **or** as `GEOMETRY` literals. For example, `ST_Distance_Centroid` accepts a `GEOMETRY` centroid:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_006" />

### Indexing latitude/longitude with `encode_geopoint`

When your data already stores coordinates as separate latitude and longitude fields of a JSON object, the `encode_geopoint` template builds a point from them directly — no GeoJSON assembly needed. Name the two fields with the `latitude` and `longitude` options:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_007" />

## Spatial predicates

### `ST_Intersects`

`ST_Intersects(field, shape)` matches rows whose indexed geometry shares any space with the query `shape` — the bounding-box query at the top of this page finds every point and polygon intersecting that region. Intersection with a `Point` matches the polygons that cover it:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_002" />

`ST_Intersects` is commutative — the field and shape arguments may be swapped.

### `ST_Contains`

`ST_Contains` is directional, so argument order matters:

- `ST_Contains(query_shape, field)` — rows whose indexed geometry is **fully contained within** `query_shape`:

  <SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_003" />

- `ST_Contains(field, query_shape)` — rows whose indexed geometry **fully contains** `query_shape` (e.g. which polygon contains a point):

  <SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_004" />

### Distance predicates

`ST_Distance_Between(field, centroid, min, max [, incl_min [, incl_max]])` matches rows within a geodesic distance range of a centroid. `ST_Distance_Centroid(field, centroid)` compared with a constant in `WHERE`, as in `ST_Distance_Centroid(geo, c) < 500.0`, keeps the rows within that many metres of the centroid. It cannot order or return rows: in `ORDER BY` or a select list it fails with `Inverted index function called outside inverted index context. Use in WHERE clause on a table with an inverted index.` See the [function reference](../../functions/search/geo.md).

<DocCallout type="attention">

The indexed column must be `JSON` or `GEOMETRY`. GeoJSON uses `[longitude, latitude]` coordinate order.

</DocCallout>

### How rows are checked

Geo predicates are exact: a row matches only when its stored geometry satisfies the predicate. The [function reference](../../functions/search/geo.md) lists the pairs of shapes a predicate rejects or always answers false for. When the predicate is a required condition of the query, it is checked with the query's other filters: `EXPLAIN` lists it under `Table Filter` with `Verify: table filter`, and the `Index Filter` shows the index lookup with `Verify: false`. Under `OR`, under `NOT`, next to `ts_offsets()`, in `ts_dict_agg` and with a vector score, it is checked as the index finds the rows, shown as `Verify: inline`.

## See also

- [Inverted Index](./index.md) · [Full-Text Search](./full-text-search.md)
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [CREATE TEXT SEARCH DICTIONARY](../../statements/create_text_search_dictionary/index.md)
- [`GEOMETRY` data type](../../data_types/geometry.md)
