---
title: Indexes
sidebar_position: 8
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import DocCallout from "@site/src/components/DocCallout";

SereneDB supports two kinds of **secondary index**, each suited to a different access pattern: the Adaptive Radix Tree (ART) — the default index created by `CREATE INDEX` — enforces constraints and speeds up point lookups, while the inverted index powers full-text, vector and geospatial search.

## Index Types

| Index | Created | Purpose |
|---|---|---|
| [Adaptive Radix Tree (ART)](./art.md) | Automatically for `PRIMARY KEY` / `UNIQUE`, or manually | Enforce constraints and speed up highly selective point queries |
| [Inverted Index](./inverted/index.md) | Manually, with `USING inverted` | Full-text search, [vector / ANN search](./inverted/vector-search.md) and [geospatial search](./inverted/geospatial-search.md) |

## Persistence

Both index types are persisted on disk.

## `CREATE INDEX` and `DROP INDEX`

Indexes are created with `CREATE INDEX` and removed with `DROP INDEX`. The index type is selected with the `USING` clause (`USING inverted` for an inverted index); when omitted, an ART index is created. See [CREATE INDEX](../statements/create_index/index.md) for the full statement reference and [the inverted index pages](./inverted/index.md) for the `USING inverted` syntax.

### `CREATE INDEX`

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

### `DROP INDEX`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

<DocCallout type="tip">

Indexes have a strong effect on performance, slowing down loading and updates, but speeding up certain queries. Please consult the Performance Guide for details.

</DocCallout>
