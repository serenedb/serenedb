---
title: Saved Searches
sidebar_position: 27
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Saved Searches

Normal search runs one query against many documents. Alerting flips that around: you keep a library of saved queries and ask which of them a brand new document matches. That is the pattern behind saved searches, watchlists and "notify me when" rules.

An `alerts` table holds the saved queries and an incoming document is indexed on its tokenized `body`. The move is to read the document's own terms out of the index with [`ts_dict_agg`](../../sql/functions/search/term-dictionary.md), then match those terms against the saved queries.

Keep the scope in mind. Each alert here is a single term and you check one incoming document at a time, so this is not the full Elasticsearch percolator with stored phrase, boolean and range queries. It covers the common "did this document mention X" case cleanly and it stays ordinary SQL the whole way.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/saved-searches/setup" />

</details>

## Read the document's terms

Every alert is a single keyword. To decide which alerts fire we first need the set of terms the incoming document actually contains. `ts_dict_agg` reads that set out of the inverted index without ever touching the raw document, already lowercased and tokenized the same way a search query would be.

<SqlLogicTest id="cookbook/search/saved-searches/example_001" />

## Which alerts fire

Now match the saved queries against those terms. An alert fires when its keyword appears in the document, so a simple `IN` against the document's term set gives you the firing alerts. Here three of the five saved queries match.

<SqlLogicTest id="cookbook/search/saved-searches/example_002" />

## Include the matched term

Alert payloads usually want to say what triggered them. Join the alerts to the document terms instead of using `IN` and you get the matched keyword back in the same row, ready to drop into a notification.

<SqlLogicTest id="cookbook/search/saved-searches/example_003" />

## Which alerts stayed quiet

The inverse is just as useful for dashboards: the saved queries that did not match this document. Swap `IN` for `NOT IN` against the same term set.

<SqlLogicTest id="cookbook/search/saved-searches/example_004" />

## A different document, different alerts

The saved queries never change. Point the same match at a new incoming document and a different set of alerts fires. Here a laptop listing trips the `laptops` and `wireless` watches while the audio and battery alerts stay silent.

<SqlLogicTest id="cookbook/search/saved-searches/example_005" />

## See also

- [Term Dictionary](../../sql/functions/search/term-dictionary.md): how `ts_dict_agg` enumerates the terms stored in an index
- [Full-text functions](../../sql/functions/search/full-text.md): the `@@` match operator for the forward direction
- [Faceted Search](./faceted-search.md): more ways to aggregate over indexed terms
