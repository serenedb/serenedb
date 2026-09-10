---
title: "geojson"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# geojson

The `geojson` template is a geospatial analyzer: instead of breaking text into word tokens, it reads a geometry and emits the [S2](http://s2geometry.io/) cell-ID terms that cover it. Those terms are what the [inverted index](../../indexes/inverted/index.md) stores and matches, so a `JSON` or `GEOMETRY` column indexed through `geojson` can be queried with spatial predicates such as containment, intersection and distance.

## How it works

Geometries are supplied as [GeoJSON](https://geojson.org/) in a `JSON` column, or as WKB in a `GEOMETRY` column. From JSON the analyzer reads a geometry object whose `type` is `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString` or `MultiPolygon`, matched case-insensitively; `GeometryCollection` is rejected. A bare coordinate array such as `[13.405, 52.52]` is also accepted and read as a point, in GeoJSON's `[longitude, latitude]` order. The analyzer approximates each shape with a covering of S2 cells at a range of levels and emits the cell IDs as terms; a query shape is covered the same way, and rows match when their coverings overlap.

`CODING` controls how a representative geometry is stored alongside the index terms so predicates can be evaluated precisely. It leaves the emitted terms alone, except under `s2latlngu32`: that coding snaps polygon vertices onto its 32-bit grid before the covering is computed, so a polygon's covering can differ.

- `source` (the default) writes no derived encoding: the original column value is stored in the index and re-parsed at query time, so predicates see the exact input geometry.
- `s2point`, `s2latlngf64` and `s2latlngu32` write a compact S2 encoding into a synthetic index column instead — of the whole geometry, or of just its centroid under `TYPE = centroid`. They are progressively smaller: an S2 unit vector as three doubles, a latitude/longitude pair as two doubles, then that pair with each coordinate quantized to 32 bits. Only `s2point` is lossless; both `LatLng` codings go through latitude and longitude, and `s2latlngu32` is the coarsest of the three. A `GEOMETRY` column accepts `source` and `s2point`; the two `LatLng` codings are refused for it.

`TYPE` controls what each geometry is reduced to before terms are computed: `shape` (the default) indexes the whole geometry, `centroid` indexes only its centroid point whatever the input geometry, and `point` accepts point inputs only — any other geometry produces no terms. Under `shape`, an input that parses to a single point takes the point path below, and a shape that does not contain its own centroid has the centroid's levels appended after its covering terms.

Terms are `BLOB`s in two forms. An ancestor term is exactly 8 bytes, the big-endian S2 cell ID. A covering term is 9 bytes: a `$` marker byte followed by the same 8 bytes. Only `TYPE = shape` over a non-point geometry produces covering terms, and the geometry itself is never emitted as a term.

A point — `TYPE = point`, `TYPE = centroid`, or a `Point` geometry under `TYPE = shape` — expands to one ancestor term per S2 level, walking from `MINLEVEL` to `MAXLEVEL` in steps of `LEVELMOD`, coarsest first. That is `floor((MAXLEVEL - MINLEVEL) / LEVELMOD) + 1` terms — 20 with the defaults. A covering emits, in S2 cell order for each of its cells: a covering term while the cell sits below the effective finest level (`MAXLEVEL` minus `(MAXLEVEL - MINLEVEL) mod LEVELMOD`); an ancestor term for the cell itself when it is at that effective level, and unconditionally unless `OPTIMIZEFORSPACE` is set; then ancestor terms for its ancestors, stepping down by `LEVELMOD` to `MINLEVEL` and stopping as soon as the previous covering cell already covered them. Terms are not deduplicated, so a cell can repeat where an appended centroid chain meets ancestors already emitted.

Geo dictionaries support no [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` are all rejected at `CREATE TEXT SEARCH DICTIONARY` — and no text options such as `CASE` or `LOCALE` apply.

A geometry that does not parse simply produces no terms: invalid JSON, a missing or non-array `coordinates`, an unrecognized `type` or `GeometryCollection`, an invalid `LineString` or `Polygon`, a non-point geometry under `TYPE = point`, and a degenerate geometry whose centroid is not a unit vector, such as a zero-area polygon. At index time such a row is indexed without geo terms and no error is raised.

### When to use `geojson` vs `geopoint`

Use `geojson` when rows hold arbitrary geometries — polygons, lines, multi-geometries — points already expressed as GeoJSON, or a `GEOMETRY` column. Reach for [`geopoint`](./geopoint.md) instead when every row is a single point whose latitude and longitude live in two separate fields of a JSON object; it builds the point directly without GeoJSON assembly. Given the same level options both templates emit the same terms for a point, so a point indexed either way is queried identically.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `TYPE` | string | `'shape'` | What each geometry is reduced to: `shape`, `centroid` or `point` |
| `CODING` | string | `'source'` | How a representative geometry is stored: `source`, `s2point`, `s2latlngf64`, `s2latlngu32` |
| `MINLEVEL` | integer | `4` | Coarsest S2 cell level indexed (0–30); must be ≤ `MAXLEVEL` |
| `MAXLEVEL` | integer | `23` | Finest S2 cell level indexed (0–30); ~1 m precision at level 23 |
| `MAXCELLS` | integer | `20` | Size target for the S2 covering (0–2147483647); only affects `TYPE = shape` over a non-point geometry |
| `LEVELMOD` | integer | `1` | Level step between the emitted cells, counted up from `MINLEVEL` (1, 2 or 3) |
| `OPTIMIZEFORSPACE` | boolean | `false` | Optimize the S2 covering for space rather than speed; only affects `TYPE = shape` over a non-point geometry |

`TYPE` and `CODING` values are matched case-insensitively; a value outside the list fails with `invalid value in "type" parameter` or `invalid value in "coding" parameter`. `MAXCELLS` is a target rather than a hard cap: it bounds how much work the coverer does, and a covering often uses fewer cells. `MINLEVEL` takes priority over it, so a geometry that is large relative to `MINLEVEL` can produce more. Levels outside their range are reported by option name — `geo_json: 'min_level' out of bounds: [0..30].`, `geo_json: 'level_mod' out of bounds: [1..3].`, `geo_json: 'min_level' should be less than or equal to 'max_level'.`

## Usage

Create the dictionary, then attach it to a `JSON` or `GEOMETRY` column in a `USING inverted` index. A plain `VARCHAR` column is rejected for geo analyzers, and a `GEOMETRY` column must declare a CRS84 coordinate reference system (`EPSG:4326`, `OGC:CRS84` or `4326`).

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_001" />

With `CODING = 's2point'` the same geometries are stored as compact S2 points — the compact choice for a `GEOMETRY` column, which accepts only `source` and `s2point`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_002" />

`ts_lexize` shows the cell-ID terms a geometry expands into. It always takes GeoJSON text, even for a dictionary attached to a `GEOMETRY` column. The terms are `BLOB`s, so the example projects them through `hex()`, and the dictionary name has to be a constant — `ts_lexize` refuses a non-constant name for a dictionary that produces `BLOB` terms. Here a single point in central Berlin gives one ancestor cell per level, coarsest first:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geojson/example_003" />

For the full indexing-and-query walkthrough — `ST_Intersects`, `ST_Contains` and distance predicates over both `JSON` and `GEOMETRY` columns — see [Geospatial Search](../../indexes/inverted/geospatial-search.md).

## See also

- [Geospatial Search](../../indexes/inverted/geospatial-search.md) — index and query geometries with `ST_*`
- [geopoint](./geopoint.md) — index points from latitude/longitude fields
- [`GEOMETRY` data type](../../data_types/geometry.md)
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
