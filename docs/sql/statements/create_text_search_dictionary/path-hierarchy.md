---
title: "expand_path"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# expand_path

The `expand_path` template tokenizes a hierarchical path into every prefix along the way, so a value indexed at `/usr/local/bin` is also found by a search for `/usr` or `/usr/local`.

This is ideal for file paths, category trees and URL paths where you want a query on any ancestor to match the descendants stored beneath it. By default it splits on `/`; set `DELIMITER` to use another separator. With `REVERSE = true` it builds the hierarchy from the right instead — the natural choice for domain names, where `docs.serenedb.com` should also match `serenedb.com` and `com`. Unlike a plain [`split_csv`](./csv.md) split, which would emit the individual components, `expand_path` emits the cumulative prefixes.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITER` | string | `'/'` | Path separator, matched as a raw byte sequence; it may be longer than one byte, such as `'::'`. An explicitly empty value falls back to `/` |
| `REPLACEMENT` | string | same as `DELIMITER` | String written in place of every delimiter occurrence inside the emitted tokens. An explicitly empty value keeps the delimiter |
| `REVERSE` | boolean | `false` | Build the hierarchy from the trailing end (for domain-like values) |
| `SKIP` | integer | `0` | Number of leading components to drop before prefixes are formed; with `REVERSE = true`, the number of trailing components to drop |

## Tokenization

Each token is a cumulative prefix of the path. The input is cut on `DELIMITER` and one token is emitted for the first component, then for the first two, and so on up to the whole value, so the original value survives as the last token. With `REVERSE = true` the tokens are cumulative suffixes instead, longest first, so the original value comes out first. A trailing delimiter is kept in the token that ends on it.

`SKIP` discards leading components before the prefixes are formed, and the surviving tokens start at the delimiter in front of the first kept component. A leading delimiter is consumed as part of the first skip step, so `/a/b/c` and `a/b/c` drop the same number of named components. In reverse mode `SKIP` discards trailing components and every token keeps the trailing delimiter. With a non-zero `SKIP` the whole value is no longer emitted. If `SKIP` consumes every delimiter, no tokens are emitted.

`REPLACEMENT` rewrites every delimiter occurrence inside the token text, including a leading one.

Matching is byte-based. There is no case folding, accent folding or Unicode normalization, and a multi-byte `DELIMITER` must match exactly. Empty input emits no tokens. Positions are consecutive in emission order, and offsets point into the original value even when `REPLACEMENT` changes the token length, so `OFFSET` works with this template.

The table shows the tokens emitted for a few option combinations:

| Options | Input | Tokens |
|---|---|---|
| `DELIMITER = '/'` | `/usr/local/bin` | `/usr`, `/usr/local`, `/usr/local/bin` |
| `DELIMITER = '/'` | `/a/b/` | `/a`, `/a/b`, `/a/b/` |
| `DELIMITER = '::'` | `a::b::c` | `a`, `a::b`, `a::b::c` |
| `DELIMITER = '/'`, `REPLACEMENT = '-'` | `/a/b/c` | `-a`, `-a-b`, `-a-b-c` |
| `DELIMITER = '/'`, `SKIP = 1` | `/usr/local/bin` | `/local`, `/local/bin` |
| `DELIMITER = '.'`, `REVERSE = true` | `docs.serenedb.com` | `docs.serenedb.com`, `serenedb.com`, `com` |
| `DELIMITER = '.'`, `REVERSE = true`, `SKIP = 1` | `com.example.www.api` | `com.example.www.`, `example.www.`, `www.` |

### Index a filesystem path into its ancestors

A search for any ancestor prefix matches every path stored beneath it:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_001" />

### Reverse mode for a domain name

With `REVERSE = true` and `DELIMITER = '.'` each token is a suffix of the value, so a subdomain matches its parent domains:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_002" />

### Skip leading components

`SKIP = 1` drops the first component before building the prefixes — useful for stripping a common root such as a mount point or a leading category:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/path-hierarchy/example_003" />

## See also

- [csv](./csv.md) — split on a single delimiter without building prefixes
- [`expand_path()`](../../functions/search/tokenizers.md#expand_path) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
