---
title: Map Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

| Name                                                                      | Description                                                                                                                                                                                                                  |
| :------------------------------------------------------------------------ | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`cardinality(map)`](#cardinalitymap)                                     | Return the size of the map (or the number of entries in the map).                                                                                                                                                            |
| [`element_at(map, key)`](#element_atmap-key)                              | Return the value for a given `key` as a list, or an empty list if the key is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys; else, an error is thrown. |
| [`map_concat(maps...)`](#map_concatmaps)                                  | Returns a map created from merging the input `maps`. On key collision the value is taken from the last map with that key.                                                                                                    |
| [`map_contains(map, key)`](#map_containsmap-key)                          | Checks if a map contains a given key.                                                                                                                                                                                        |
| [`map_contains_entry(map, key, value)`](#map_contains_entrymap-key-value) | Check if a map contains a given key-value pair.                                                                                                                                                                              |
| [`map_contains_value(map, value)`](#map_contains_valuemap-value)          | Checks if a map contains a given value.                                                                                                                                                                                      |
| [`map_entries(map)`](#map_entriesmap)                                     | Return a list of struct(k, v) for each key-value pair in the map.                                                                                                                                                            |
| [`map_extract(map, key)`](#map_extractmap-key)                            | Return the value for a given `key` as a list, or an empty list if the key is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys; else, an error is thrown. |
| [`map_extract_value(map, key)`](#map_extract_valuemap-key)                | Returns the value for a given `key` or `NULL` if the `key` is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys; else, an error is thrown.                |
| [`map_from_entries(STRUCT(k, v)[])`](#map_from_entriesstructk-v)          | Returns a map created from the entries of the array.                                                                                                                                                                         |
| [`map_keys(map)`](#map_keysmap)                                           | Return a list of all keys in the map.                                                                                                                                                                                        |
| [`map_values(map)`](#map_valuesmap)                                       | Return a list of all values in the map.                                                                                                                                                                                      |
| [`map()`](#map)                                                           | Returns an empty map.                                                                                                                                                                                                        |
| [`map[entry]`](#mapentry)                                                 | Returns the value for a given `key` or `NULL` if the `key` is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys; else, an error is thrown.                |

#### `cardinality(map)`

Return the size of the map (or the number of entries in the map).

<SqlLogicTest id="sql/functions/map/cardinality" />

#### `element_at(map, key)`

Return the value for a given `key` as a list, or an empty list if the key is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys else an error is thrown. Alias: `map_extract(map, key)`.

<SqlLogicTest id="sql/functions/map/element_at" />

#### `map_concat(maps...)`

Returns a map created from merging the input `maps`. On key collision the value is taken from the last map with that key.

<SqlLogicTest id="sql/functions/map/map_concat" />

#### `map_contains(map, key)`

Checks if a map contains a given key.

<SqlLogicTest id="sql/functions/map/map_contains" />

#### `map_contains_entry(map, key, value)`

Check if a map contains a given key-value pair.

<SqlLogicTest id="sql/functions/map/map_contains_entry" />

#### `map_contains_value(map, value)`

Checks if a map contains a given value.

<SqlLogicTest id="sql/functions/map/map_contains_value" />

#### `map_entries(map)`

Return a list of struct(k, v) for each key-value pair in the map.

<SqlLogicTest id="sql/functions/map/map_entries" />

#### `map_extract(map, key)`

Return the value for a given `key` as a list, or an empty list if the key is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys else an error is thrown. Alias: `element_at(map, key)`.

<SqlLogicTest id="sql/functions/map/map_extract" />

#### `map_extract_value(map, key)`

Returns the value for a given `key` or `NULL` if the `key` is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys else an error is thrown. Alias: `map[key]`.

<SqlLogicTest id="sql/functions/map/map_extract_value" />

#### `map_from_entries(STRUCT(k, v)[])`

Returns a map created from the entries of the array.

<SqlLogicTest id="sql/functions/map/map_from_entries" />

#### `map_keys(map)`

Return a list of all keys in the map.

<SqlLogicTest id="sql/functions/map/map_keys" />

#### `map_values(map)`

Return a list of all values in the map.

<SqlLogicTest id="sql/functions/map/map_values" />

#### `map()`

Returns an empty map.

<SqlLogicTest id="sql/functions/map/map" />

#### `map[entry]`

Returns the value for a given `key` or `NULL` if the `key` is not contained in the map. The type of the key provided in the second parameter must match the type of the map's keys else an error is thrown. Alias: `map_extract_value(map, key)`.

<SqlLogicTest id="sql/functions/map/map_entry" />
