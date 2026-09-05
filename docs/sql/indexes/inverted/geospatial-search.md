---
title: Geospatial Search
sidebar_position: 9
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The [inverted index](./index.md) indexes geographic shapes for fast spatial predicates — finding which points fall inside a region, which regions contain a location and so on. Geometries can be stored either as [GeoJSON](https://geojson.org/) in a `JSON` column or as values of the native `GEOMETRY` type, and both are indexed with a geospatial text search dictionary.

## Creating a geospatial index

Use the `geojson` dictionary template on a `JSON` column holding GeoJSON. The column type must be `JSON` or `GEOMETRY` — a plain `VARCHAR` is rejected for geo analyzers:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_001" />

The `geojson` template indexes `Point`, `Polygon` and other GeoJSON geometries. GeoJSON coordinates are `[longitude, latitude]`. A `geopoint` template is also available for extracting a point from latitude/longitude fields of a JSON object, and the `geojson` template accepts `coding = 's2point'` to index points via S2 cell coverings.

### Indexing a `GEOMETRY` column

The native [`GEOMETRY`](../../data_types/geometry.md) type is indexed the same way — point it at the `geojson` template (typically with `coding = 's2point'`). `GEOMETRY` values carry WKB internally and are written from WKT with a coordinate reference system; WKT uses the same `longitude latitude` axis order as GeoJSON:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_005" />

Query shapes may be supplied as GeoJSON text (as above) **or** as `GEOMETRY` literals. For example, `ST_Distance_Centroid` accepts a `GEOMETRY` centroid:

<SqlLogicTest id="sql/indexes/inverted/geospatial-search/example_006" />

### Indexing latitude/longitude with `geopoint`

When your data already stores coordinates as separate latitude and longitude fields of a JSON object, the `geopoint` template builds a point from them directly — no GeoJSON assembly needed. Name the two fields with the `latitude` and `longitude` options:

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

`ST_Distance_Between(field, centroid, min, max [, incl_min [, incl_max]])` matches rows within a geodesic distance range of a centroid, and `ST_Distance_Centroid(field, centroid)` returns the geodesic distance for ordering within an index scan. See the [function reference](../../functions/search/geo.md).

<DocCallout type="attention">

The indexed column must be `JSON` or `GEOMETRY`. GeoJSON uses `[longitude, latitude]` coordinate order.

</DocCallout>

## See also

- [Inverted Index](./index.md) · [Full-Text Search](./full-text-search.md)
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [CREATE TEXT SEARCH DICTIONARY](../../statements/create_text_search_dictionary/index.md)
- [`GEOMETRY` data type](../../data_types/geometry.md)
