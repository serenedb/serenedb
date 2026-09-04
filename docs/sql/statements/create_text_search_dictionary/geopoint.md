---
title: "geopoint"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# geopoint

The `geopoint` template is a geospatial analyzer for the common case where each row is a single point whose coordinates are already stored as latitude and longitude. Rather than requiring [GeoJSON](https://geojson.org/), it reads the two coordinates straight from a JSON value and emits the [S2](http://s2geometry.io/) cell-ID terms that the [inverted index](../../indexes/inverted/index.md) stores and matches — so the column can be queried by region and distance just like a [`geojson`](./geojson.md) column.

## How it works

The analyzer pulls a latitude and a longitude out of each indexed JSON value, builds the point and emits an S2 cell covering of it as index terms. There are two input shapes:

- **Named fields** — set `latitude` and `longitude` to the field names (or slash-separated paths such as `loc/lat`). The analyzer reads `{"lat": …, "lng": …}`-style objects.
- **Coordinate array** — leave both options unset (the default) and the analyzer treats the indexed value as a `[latitude, longitude]` array. Note this is `[lat, lng]` order, the reverse of GeoJSON's `[longitude, latitude]`.

`latitude` and `longitude` must be set together or left unset together — setting only one is an error.

### When to use `geopoint` vs `geojson`

Use `geopoint` when each row is one point held as separate lat/lng fields: it skips GeoJSON assembly and indexes the coordinates directly. Use [`geojson`](./geojson.md) when rows hold arbitrary geometries (polygons, lines) or points already expressed as GeoJSON. Both emit the same S2 terms, so a given point is queried identically whichever template indexed it.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `latitude` | string | _(none — array input)_ | Field name or slash path holding the latitude |
| `longitude` | string | _(none — array input)_ | Field name or slash path holding the longitude |
| `minlevel` | integer | `4` | Minimum S2 cell level (0–30) |
| `maxlevel` | integer | `23` | Maximum S2 cell level (0–30); ~1 m precision at level 23 |
| `maxcells` | integer | `20` | Maximum number of S2 cells in a covering |
| `levelmod` | integer | `1` | S2 level step (1, 2 or 3) |
| `optimizeforspace` | boolean | `false` | Optimize the S2 covering for space rather than speed |

## Usage

Create the dictionary naming the coordinate fields, then attach it to a `JSON` column in a `USING inverted` index:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geopoint/example_001" />

`ts_lexize` shows the cell-ID terms a point expands into. Because `geopoint` and `geojson` describe the same physical location, the same point produces an identical covering whichever template indexes it — here central Berlin as `{"lat": …, "lng": …}` versus the GeoJSON `[lon, lat]` of [the geojson example](./geojson.md#usage):

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geopoint/example_002" />

For the full indexing-and-query walkthrough — `ST_Intersects`, `ST_Contains` and distance predicates — see [Geospatial Search](../../indexes/inverted/geospatial-search.md).

## See also

- [Geospatial Search](../../indexes/inverted/geospatial-search.md) — index and query points with `ST_*`
- [geojson](./geojson.md) — index arbitrary GeoJSON geometries
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
