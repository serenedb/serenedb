---
title: "path_hierarchy"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# path_hierarchy

The `path_hierarchy` template tokenizes a hierarchical path into every prefix along the way, so a value indexed at `/usr/local/bin` is also found by a search for `/usr` or `/usr/local`.

This is ideal for file paths, category trees and URL paths where you want a query on any ancestor to match the descendants stored beneath it. By default it splits on `/`; set `DELIMITER` to use another separator. With `REVERSE = true` it builds the hierarchy from the right instead — the natural choice for domain names, where `docs.serenedb.com` should also match `serenedb.com` and `com`. Unlike a plain [`delimiter`](./delimiter.md) split, which would emit the individual components, `path_hierarchy` emits the cumulative prefixes.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITER` | string | `'/'` | Path separator character or string |
| `REPLACEMENT` | string | same as `DELIMITER` | String that replaces the delimiter in the emitted tokens |
| `REVERSE` | boolean | `false` | Build the hierarchy from the right (for domain-like values) |
| `SKIP` | integer | `0` | Number of leading components to drop before building prefixes |
| `BUFFERSIZE` | integer | `1024` | Term buffer size hint (characters per pass) |

## Tokenization

Each token is a cumulative prefix of the path. The input is cut on `DELIMITER` and one token is emitted for the first component, then for the first two, and so on up to the whole value. `REVERSE` builds the prefixes from the trailing end instead; `SKIP` discards a number of leading components before prefixes are formed; `REPLACEMENT` rewrites the delimiter character in the output.

The table shows the tokens emitted for a few option combinations:

| Options | Input | Tokens |
|---|---|---|
| `DELIMITER = '/'` | `/usr/local/bin` | `/usr`, `/usr/local`, `/usr/local/bin` |
| `DELIMITER = '/'`, `SKIP = 1` | `/usr/local/bin` | `/local`, `/local/bin` |
| `DELIMITER = '.'`, `REVERSE = true` | `docs.serenedb.com` | `docs.serenedb.com`, `serenedb.com`, `com` |

### Index a filesystem path into its ancestors

A search for any ancestor prefix matches every path stored beneath it:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_001" />

### Reverse mode for a domain name

With `REVERSE = true` and `DELIMITER = '.'` the prefixes grow from the right, so a subdomain matches its parent domains:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_002" />

### Skip leading components

`SKIP = 1` drops the first component before building the prefixes — useful for stripping a common root such as a mount point or a leading category:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_003" />

## See also

- [delimiter](./delimiter.md) — split on a single delimiter without building prefixes
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
