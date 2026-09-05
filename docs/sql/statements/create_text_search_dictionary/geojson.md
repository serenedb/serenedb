---
title: "geojson"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# geojson

The `geojson` template is a geospatial analyzer: instead of breaking text into word tokens, it reads a geometry and emits the set of [S2](http://s2geometry.io/) cell-ID terms that cover it. Those terms are what the [inverted index](../../indexes/inverted/index.md) stores and matches, so a `JSON` or `GEOMETRY` column indexed through `geojson` can be queried with spatial predicates such as containment, intersection and distance.

## How it works

Geometries are supplied as [GeoJSON](https://geojson.org/) — `Point`, `LineString`, `Polygon` and the other GeoJSON types. GeoJSON coordinates are `[longitude, latitude]`. The analyzer approximates each shape with a covering of S2 cells at a range of levels and emits the cell IDs as terms; a query shape is covered the same way, and rows match when their coverings overlap.

`coding` controls how a representative geometry is stored alongside the index terms so predicates can be evaluated precisely:

- `source` (the default) keeps the original geometry and re-parses it at query time — exact, no precision loss, larger footprint.
- `s2point`, `s2latlngf64` and `s2latlngu32` store an S2 encoding of the geometry instead. They are progressively more compact (and `s2latlngu32` quantizes to ~centimetre precision), trading exactness for a smaller index. WKB ingest of a `GEOMETRY` column supports only `s2point`.

`type` controls what each shape is reduced to before terms are computed: `shape` (the default) indexes the full geometry, `centroid` indexes only its centroid point and `point` accepts point inputs only.

### When to use `geojson` vs `geopoint`

Use `geojson` when rows hold arbitrary geometries — polygons, lines, multi-geometries — or points already expressed as GeoJSON. Reach for [`geopoint`](./geopoint.md) instead when every row is a single point whose latitude and longitude live in two separate fields of a JSON object; it builds the point directly without GeoJSON assembly. Both templates emit the same kind of S2 terms, so a point indexed either way is queried identically.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `type` | string | `shape` | What to index: `shape`, `centroid` or `point` |
| `coding` | string | `source` | Geometry encoding: `source`, `s2point`, `s2latlngf64`, `s2latlngu32` |
| `minlevel` | integer | `4` | Minimum S2 cell level (0–30) |
| `maxlevel` | integer | `23` | Maximum S2 cell level (0–30); ~1 m precision at level 23 |
| `maxcells` | integer | `20` | Maximum number of S2 cells in a covering |
| `levelmod` | integer | `1` | S2 level step (1, 2 or 3) |
| `optimizeforspace` | boolean | `false` | Optimize the S2 covering for space rather than speed |

## Usage

Create the dictionary, then attach it to a `JSON` or `GEOMETRY` column in a `USING inverted` index. A plain `VARCHAR` column is rejected for geo analyzers.

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_001" />

With `coding = 's2point'` the same geometries are stored as compact S2 points — the usual choice for a `GEOMETRY` column:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_002" />

`ts_lexize` shows the cell-ID terms a geometry expands into — here a single point in central Berlin produces a covering of S2 cells from coarse to fine:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_003" />

For the full indexing-and-query walkthrough — `ST_Intersects`, `ST_Contains` and distance predicates over both `JSON` and `GEOMETRY` columns — see [Geospatial Search](../../indexes/inverted/geospatial-search.md).

## See also

- [Geospatial Search](../../indexes/inverted/geospatial-search.md) — index and query geometries with `ST_*`
- [geopoint](./geopoint.md) — index points from latitude/longitude fields
- [`GEOMETRY` data type](../../data_types/geometry.md)
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
