---
title: Indexes
sidebar_position: 8
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import DocCallout from "@site/src/components/DocCallout";

An index in SereneDB is an [inverted index](./inverted/index.md), on a table or on a view.

`PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` constraints are enforced by the table itself and need no index. `CREATE UNIQUE INDEX` adds a unique key the same way.

## Index Types

| Index | Created | Purpose |
|---|---|---|
| [Inverted Index](./inverted/index.md) | With `CREATE INDEX` or `CREATE UNIQUE INDEX`, with or without `USING inverted` | Equality and range lookups, full-text search, [vector / ANN search](./inverted/vector-search.md) and [geospatial search](./inverted/geospatial-search.md) |

The catalog lists the keys of `PRIMARY KEY` and `UNIQUE` constraints as indexes with the `secondary` access method.

## Persistence

Indexes are persisted on disk.

## `CREATE INDEX` and `DROP INDEX`

Indexes are created with `CREATE INDEX` and removed with `DROP INDEX`. `CREATE INDEX` creates an inverted index, with or without `USING inverted`. See [CREATE INDEX](../statements/create_index/index.md) for the full statement reference and [the inverted index pages](./inverted/index.md) for the column dictionaries, `INCLUDE` columns and index options.

### `CREATE INDEX`

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

### `DROP INDEX`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

<DocCallout type="tip">

Indexes have a strong effect on performance, slowing down loading and updates, but speeding up certain queries. Please consult the [Performance Guide](../../cookbook/performance/indexing.md) for details.

</DocCallout>
