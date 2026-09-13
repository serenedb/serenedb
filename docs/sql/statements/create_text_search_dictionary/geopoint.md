---
title: "encode_geopoint"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# encode_geopoint

The `encode_geopoint` template is a geospatial analyzer for the common case where each row is a single point whose coordinates are already stored as latitude and longitude. Rather than requiring [GeoJSON](https://geojson.org/), it reads the two coordinates straight from a JSON value and emits the [S2](http://s2geometry.io/) cell-ID terms that the [inverted index](../../indexes/inverted/index.md) stores and matches — so the column can be queried by region and distance just like a [`encode_geojson`](./geojson.md) column.

## How it works

The analyzer pulls a latitude and a longitude out of each indexed JSON value, builds the point and emits one S2 cell-ID term per level as index terms. There are two input shapes:

- **Named fields** — set `LATITUDE` and `LONGITUDE` to the field names, to slash-separated paths such as `loc/lat`, or to lists of path segments such as `['loc', 'lat']`. The value must then be a JSON object; each path segment is looked up by name in any order, and the final field must be a JSON number, so `{"lat": 52.52, "lng": 13.405}` is read but `{"lat": "52.52", "lng": "13.405"}` is not.
- **Coordinate array** — leave both options unset (the default) and the analyzer treats the indexed value as a `[latitude, longitude]` array of exactly two numbers. Note this is `[lat, lng]` order, the reverse of GeoJSON's `[longitude, latitude]`.

`LATITUDE` and `LONGITUDE` must be set together or left unset together — setting only one is an error. Paths are split on `/` with empty segments dropped, so `LATITUDE = '/'` counts as unset. Latitude is clamped into `[-90, 90]` and longitude wrapped into `[-180, 180]` before the cell is computed.

The indexed column must be `JSON`. A `GEOMETRY` column is rejected for `encode_geopoint`, because the latitude and longitude paths are JSON-only — index a `GEOMETRY` column through [`encode_geojson`](./geojson.md) instead.

A point needs no covering, so `encode_geopoint` emits only ancestor terms: one per S2 level, walking from `MINLEVEL` to `MAXLEVEL` in steps of `LEVELMOD`, coarsest first. That is `floor((MAXLEVEL - MINLEVEL) / LEVELMOD) + 1` terms — 20 with the defaults. Each term is a `BLOB` of exactly 8 bytes, the big-endian S2 cell ID of the point's ancestor cell at that level, and the level is part of the cell ID, so all the terms of one value are distinct. Because no covering is built, `MAXCELLS` and `OPTIMIZEFORSPACE` are accepted and range-checked but have no effect. `encode_geopoint` also writes no compact geometry copy of its own, so it has no `CODING` option: exact predicate evaluation reads the original column value.

Geo dictionaries support no [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` are all rejected at `CREATE TEXT SEARCH DICTIONARY` — and no text options such as `CASE` or `LOCALE` apply.

A value that does not parse simply produces no terms: invalid JSON, a value that is not the expected object or array, a missing or non-numeric coordinate field, or an array whose length is not exactly 2. At index time such a row is indexed without geo terms and no error is raised.

### When to use `encode_geopoint` vs `encode_geojson`

Use `encode_geopoint` when each row is one point held as separate lat/lng fields: it skips GeoJSON assembly and indexes the coordinates directly. Use [`encode_geojson`](./geojson.md) when rows hold arbitrary geometries (polygons, lines), points already expressed as GeoJSON, or a `GEOMETRY` column. Given the same level options both emit the same terms for a point, so a given point is queried identically whichever template indexed it.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LATITUDE` | string | `''` _(array input)_ | Field name, slash path or list of path segments holding the latitude; must be set together with `LONGITUDE` |
| `LONGITUDE` | string | `''` _(array input)_ | Field name, slash path or list of path segments holding the longitude; must be set together with `LATITUDE` |
| `MINLEVEL` | integer | `4` | Coarsest S2 cell level indexed (0–30); must be ≤ `MAXLEVEL` |
| `MAXLEVEL` | integer | `23` | Finest S2 cell level indexed (0–30); ~1 m precision at level 23 |
| `MAXCELLS` | integer | `20` | Maximum number of S2 cells in a covering (0–2147483647); no effect on `encode_geopoint` |
| `LEVELMOD` | integer | `1` | Level step between the emitted cells, counted up from `MINLEVEL` (1, 2 or 3) |
| `OPTIMIZEFORSPACE` | boolean | `false` | Optimize the S2 covering for space rather than speed; no effect on `encode_geopoint` |

## Usage

Create the dictionary naming the coordinate fields; a `USING inverted` index then attaches it to a `JSON` column:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geopoint/example_001" />

`ts_lexize` shows the cell-ID terms a point expands into. The terms are `BLOB`s, so the example projects them through `hex()`, and the dictionary name has to be a constant — `ts_lexize` refuses a non-constant name for a dictionary that produces `BLOB` terms. Because `encode_geopoint` and `encode_geojson` describe the same physical location, the same point produces identical terms whichever template indexes it — here central Berlin as `{"lat": …, "lng": …}` versus the GeoJSON `[lon, lat]` of [the geojson example](./geojson.md#usage):

<SqlLogicTest id="sql/statements/create_text_search_dictionary/geopoint/example_002" />

For the full indexing-and-query walkthrough — `ST_Intersects`, `ST_Contains` and distance predicates — see [Geospatial Search](../../indexes/inverted/geospatial-search.md).

## See also

- [Geospatial Search](../../indexes/inverted/geospatial-search.md) — index and query points with `ST_*`
- [geojson](./geojson.md) — index arbitrary GeoJSON geometries
- [Geospatial Search Functions](../../functions/search/geo.md) — `ST_*` reference
- [`encode_geopoint()`](../../functions/search/tokenizers.md#encode_geopoint) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
