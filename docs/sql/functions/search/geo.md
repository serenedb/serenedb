---
title: Geo Functions
sidebar_label: Geo Functions
sidebar_position: 3
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Geospatial Functions {#geospatial-functions}

These predicates run inside a [geospatial search](../../indexes/inverted/geospatial-search.md) over an [inverted index](../../indexes/inverted/index.md). They answer the common spatial questions: which shapes overlap a region, which region encloses a point, what falls within a distance band of a location.

The **indexed column must be `JSON` (holding [GeoJSON](https://geojson.org/)) or [`GEOMETRY`](../../data_types/geometry.md)** — a plain `VARCHAR` is rejected for geo analyzers. The query shape passed as the other argument may be supplied either way: as GeoJSON text or as a `GEOMETRY` literal. In GeoJSON and WKT alike, coordinates are written **`[longitude, latitude]`** (x then y), not lat/lon.

| Function | Description |
| :--- | :--- |
| [`ST_Intersects(field, shape)`](#st_intersects) | True where the indexed geometry shares any space with `shape`. Commutative. |
| [`ST_Contains(a, b)`](#st_contains) | True where `a` fully contains `b`. Direction-dependent — argument order matters. |
| [`ST_Distance_Between(field, centroid, min, max [, incl_min [, incl_max]])`](#st_distance_between) | True where the geodesic distance to `centroid` falls in `[min, max]`. |
| [`ST_Distance_Centroid(field, centroid)`](#st_distance_centroid) | Geodesic distance (metres) from the indexed geometry to `centroid`. |
| [`field <-> centroid`](#distance-operator) | Operator synonym for `ST_Distance_Centroid`. |

:::note Index-scan context
These functions are predicates evaluated by the index, not scalar functions you can call in a bare `SELECT` list. Use them in the `WHERE` clause of a query against an indexed table (or its `_idx` alias). `ST_Distance_Centroid` in particular returns the distance only inside an index scan; selecting it as a plain projection raises *"Inverted index function called outside inverted index context."*
:::

#### `ST_Intersects(field, shape)` {#st_intersects}

True for rows whose indexed geometry shares **any** space with `shape` — touching an edge or a single point counts. This is the workhorse predicate: a bounding box, a neighbourhood polygon or a single point all flow through the same call.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `field` | indexed `JSON` / `GEOMETRY` | The indexed geometry column. |
| `shape` | GeoJSON text or `GEOMETRY` | The query shape to test for overlap. |

<SqlLogicTest id="sql/functions/full_text_search/st_intersects" />

The query `shape` may equally be supplied as **GeoJSON text** instead of a `GEOMETRY` literal — every geo function accepts either form (GeoJSON coordinates are `[longitude, latitude]`):

<SqlLogicTest id="sql/functions/full_text_search/st_intersects_geojson" />

**How it works.** `ST_Intersects` is **commutative**: `ST_Intersects(field, shape)` and `ST_Intersects(shape, field)` return the same rows, so argument order is free. Intersection includes shared boundaries — a point lying exactly on a polygon's edge intersects it. It is the complement of disjointness, so "find everything *not* near this region" is `NOT ST_Intersects(...)`.

**Behavior matrix.** Against a fixed query polygon — the square `[0,0]–[10,10]` — over a small set of shapes:

| Row | Shape | Intersects `[0,0]–[10,10]`? | Why |
| :--- | :--- | :---: | :--- |
| `red_square` | polygon `[0,0]–[10,10]` | ✅ | identical to the query — fully overlaps |
| `inner_point` | point `(5,5)` | ✅ | sits inside the square |
| `edge_point` | point `(10,5)` | ✅ | lies on the right edge — boundary counts |
| `overlap_box` | polygon `[5,5]–[15,15]` | ✅ | overlaps the upper-right quadrant |
| `far_point` | point `(50,50)` | ❌ | well outside the square |

<SqlLogicTest id="sql/functions/full_text_search/st_intersects_matrix" />

#### `ST_Contains(a, b)` {#st_contains}

True where `a` fully contains `b` — every part of `b` lies inside `a` (boundary included). Unlike `ST_Intersects`, `ST_Contains` is **directional**: the order of arguments decides the question being asked.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `a` | GeoJSON text / `GEOMETRY` / indexed column | The containing ("outer") shape. |
| `b` | GeoJSON text / `GEOMETRY` / indexed column | The contained ("inner") shape. |

<SqlLogicTest id="sql/functions/full_text_search/st_contains" />

**How it works — argument order matters.** Put the indexed `field` in whichever slot frames your question:

- **`ST_Contains(query_shape, field)`** — *which indexed geometries fall inside the query region?* The query shape is the outer boundary; matches are the rows contained within it. Over the same `[0,0]–[10,10]` square:

  <SqlLogicTest id="sql/functions/full_text_search/st_contains_query_field" />

  | Row | Shape | Inside `[0,0]–[10,10]`? | Why |
  | :--- | :--- | :---: | :--- |
  | `red_square` | polygon `[0,0]–[10,10]` | ✅ | equal shapes — contained (boundary included) |
  | `inner_point` | point `(5,5)` | ✅ | strictly inside |
  | `edge_point` | point `(10,5)` | ✅ | on the boundary — still contained |
  | `overlap_box` | polygon `[5,5]–[15,15]` | ❌ | spills outside the square |
  | `far_point` | point `(50,50)` | ❌ | outside |

- **`ST_Contains(field, query_shape)`** — *which indexed geometries enclose this location?* The field is the outer shape; a point query finds the polygon(s) covering it. Over the same data with the query point `(5,5)`:

  <SqlLogicTest id="sql/functions/full_text_search/st_contains_field_query" />

  | Row | Shape | Contains point `(5,5)`? | Why |
  | :--- | :--- | :---: | :--- |
  | `red_square` | polygon `[0,0]–[10,10]` | ✅ | the point is inside it |
  | `inner_point` | point `(5,5)` | ✅ | a geometry contains itself |
  | `overlap_box` | polygon `[5,5]–[15,15]` | ❌ | `(5,5)` is its corner, not its interior — not contained |
  | `edge_point`, `far_point` | points | ❌ | a point can only contain itself |

Swapping the arguments flips the result set, so always frame the containing shape first.

#### `ST_Distance_Between(field, centroid, min, max [, incl_min [, incl_max]])` {#st_distance_between}

True for rows whose **geodesic** distance to `centroid` falls inside the `[min, max]` band — a distance ring around a point.

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `field` | indexed `JSON` / `GEOMETRY` | — | The indexed geometry column. |
| `centroid` | GeoJSON / `GEOMETRY` point | — | The point distances are measured from. |
| `min` | `DOUBLE` (metres) | — | Lower bound of the distance band. |
| `max` | `DOUBLE` (metres) | — | Upper bound of the distance band. |
| `incl_min` | `BOOLEAN` | `true` | Whether `min` itself matches. |
| `incl_max` | `BOOLEAN` | `true` | Whether `max` itself matches. |

<SqlLogicTest id="sql/functions/full_text_search/st_distance_between" />

**How it works.** Distance is **geodesic** — measured over the curve of the Earth in **metres**, not in coordinate degrees — so a 3 km band means 3 km on the ground regardless of latitude. Setting `min = 0` gives a plain radius query; a non-zero `min` excludes a hole in the middle, which is how you express "between 300 m and 3 km away". Around the Kremlin (`POINT(37.617499 55.752023)`) over four points, the band `[300, 3000]` keeps only the mid-ring location:

<SqlLogicTest id="sql/functions/full_text_search/st_distance_between_ring" />

| Row | Approx. distance | In band `[300, 3000]`? | Why |
| :--- | :--- | :---: | :--- |
| `kremlin` | ~0 m | ❌ | below `min` (it is the centre) |
| `red_square` | ~280 m | ❌ | below `min` (300 m) |
| `gorky_park` | ~2.5 km | ✅ | inside the band |
| `spb_palace` | ~630 km | ❌ | far above `max` |

#### `ST_Distance_Centroid(field, centroid)` {#st_distance_centroid}

Geodesic distance, in metres, from the indexed geometry's centroid to `centroid`. Used inside an index scan as the left side of a comparison — a radius query reads as `ST_Distance_Centroid(...) < radius`.

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `field` | indexed `JSON` / `GEOMETRY` | The indexed geometry column. |
| `centroid` | GeoJSON / `GEOMETRY` point | The point distances are measured from. |

<SqlLogicTest id="sql/functions/full_text_search/st_distance_centroid" />

**How it works.** A bare `< radius` comparison is a circle around `centroid`. Around the Kremlin, a 3 km radius keeps the three nearby points and drops the one ~630 km away:

<SqlLogicTest id="sql/functions/full_text_search/st_distance_centroid_ring" />

`ST_Distance_Centroid(field, c) < r` is the open-disc form of `ST_Distance_Between(field, c, 0, r, true, false)`.

#### `field <-> centroid` {#distance-operator}

The `<->` operator is a synonym for `ST_Distance_Centroid`: `field <-> centroid` is exactly equivalent to `ST_Distance_Centroid(field, centroid)`, used the same way in a `WHERE` comparison within an index scan. It reads naturally as a distance and keeps radius queries terse:

<SqlLogicTest id="sql/functions/full_text_search/st_distance_centroid_op" />

## Coming from Elasticsearch

[Elasticsearch / OpenSearch geo queries](https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-queries.html) map onto SereneDB's `ST_*` predicates as follows (the left column links to the Elasticsearch reference):

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| [`geo_shape`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html) `INTERSECTS` (default relation) | `ST_Intersects` |
| [`geo_shape`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html) `WITHIN` | `ST_Contains(query_shape, field)` |
| [`geo_shape`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html) `CONTAINS` | `ST_Contains(field, query_shape)` |
| [`geo_distance`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html) | `ST_Distance_Centroid(...) < r`, `ST_Distance_Between` |
| [`geo_distance`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-distance-query.html) ring (two clauses) | `ST_Distance_Between` (single call, inclusive flags) |
| [`geo_bounding_box`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-bounding-box-query.html) | `ST_Intersects` with a rectangular `Polygon` |
| [`geo_polygon`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-geo-polygon-query.html) | `ST_Intersects` / `ST_Contains` with a `Polygon` |

**Notable differences.** SereneDB folds bounding-box and polygon queries into the same `ST_Intersects` / `ST_Contains` predicates rather than offering them as distinct query types, and expresses a distance ring in a single `ST_Distance_Between` call.

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| `geo_shape` `DISJOINT` relation | no `ST_Disjoint`; use `NOT ST_Intersects(...)` |

## See also

- [Full-Text Search Functions](./full-text.md) — operators, constructors, parsers
- [Inverted Index](../../indexes/inverted/index.md) · [Geospatial Search](../../indexes/inverted/geospatial-search.md)
- [`GEOMETRY` data type](../../data_types/geometry.md) · [CREATE TEXT SEARCH DICTIONARY](../../statements/create_text_search_dictionary/index.md)
