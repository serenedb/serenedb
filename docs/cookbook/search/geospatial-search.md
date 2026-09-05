---
title: Geospatial Search
sidebar_position: 28
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Geospatial Search

"Near me" is a search too. The [inverted index](../../sql/indexes/inverted/index.md) keeps geometry next to text, so a distance filter runs on the same index that answers your keyword queries and you can mix the two in one `WHERE`. Here you find shops within a radius, inside a distance band and near a location that also matches a name, then bucket every point into a heatmap grid.

The shops are a coffee chain scattered across the Bay Area. `geo` is a `GEOMETRY` column indexed with a `geojson` dictionary using `s2point` coding, coordinates run `longitude latitude` and every distance is geodesic metres.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/geospatial-search/setup" />

</details>

## Within a radius

[`ST_Distance_Centroid`](../../sql/functions/search/geo.md) returns the metres from the indexed point to your location. Compare it to a radius and you have "walkable from the office".

<SqlLogicTest id="cookbook/search/geospatial-search/example_001" />

## Inside a distance band

[`ST_Distance_Between`](../../sql/functions/search/geo.md) takes a min and a max, so it carves out a ring. Here is the "too far to walk but worth a drive" band from one to ten kilometres.

<SqlLogicTest id="cookbook/search/geospatial-search/example_002" />

## Near and named

Because geometry and text share the index, a location filter and a name match combine into one query with no join. This finds the shop near the office whose name starts with "Sight".

<SqlLogicTest id="cookbook/search/geospatial-search/example_003" />

## Bucket points into a heatmap grid

A heatmap wants points rolled up into cells: snap every location onto a grid and count what lands in each square. There is no `geohash_grid` aggregation here and no `ST_X` / `ST_Y` accessor, so the grid is hand-rolled. Pull each point's coordinates out of its [`ST_AsText`](../../sql/functions/search/geo.md) string, truncate them to a fixed step and group on the result. That is a full scan rather than an index lookup, but the counts are exact and you pick the cell size.

### Count points per cell

Truncating each coordinate to a tenth of a degree snaps every point to the south-west corner of a roughly ten kilometre cell. Group on the two truncated values and `count(*)` is the cell density, hottest cell first.

<SqlLogicTest id="cookbook/search/geospatial-search/example_004" />

### Drop the sparse cells

A heatmap does not care about a cell holding one stray shop. `HAVING count(*) >= 2` keeps only the squares worth painting (`min_doc_count`, if you come from Elastic).

<SqlLogicTest id="cookbook/search/geospatial-search/example_005" />

### Zoom into a cell

Click the hottest square and you want the shops behind the number. Filter on the same truncated coordinates to list every point in that cell with its exact location.

<SqlLogicTest id="cookbook/search/geospatial-search/example_006" />

## See also

- [Geospatial Search guide](../../sql/indexes/inverted/geospatial-search.md): indexing polygons, GeoJSON columns and the lat/lon `geopoint` template
- [Geo functions reference](../../sql/functions/search/geo.md): `ST_Intersects`, `ST_Contains`, `ST_Distance_Between`, `ST_Distance_Centroid` and the `<->` operator
- [Faceted Search](./faceted-search.md): combine a location filter with facet counts over the survivors
