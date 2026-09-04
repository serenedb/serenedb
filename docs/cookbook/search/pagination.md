---
title: Pagination
sidebar_position: 25
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Pagination

Search results rarely fit on one screen. This recipe pages through matches two ways. A `LIMIT`/`OFFSET` page ladder keeps whatever [ranking](./ranking.md) you picked and walks forward a page at a time. Keyset pagination (the "load more" pattern) seeks by a stable key like an `id` or a timestamp and stays fast no matter how deep the page. Both run over an [inverted index](../../sql/indexes/inverted/index.md).

The sample data is seven article titles that all match the word "search", enough to walk across a few pages.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/pagination/setup" />

</details>

## Page with LIMIT and OFFSET

The obvious way to page. Order the results, take the first `LIMIT` rows for page one, then add `OFFSET` to skip past them for the next. The sort here is relevance with `id` as a tie-break so the sequence stays stable across requests.

<SqlLogicTest id="cookbook/search/pagination/example_001" />

Page two keeps the same order and skips the first three rows.

<SqlLogicTest id="cookbook/search/pagination/example_002" />

## Why OFFSET gets slow

`OFFSET` does not seek. To reach page 100 the engine still produces every row ahead of it and then throws those rows away, so a 10,000 row offset walks and discards 10,000 rows to return 10. You will not feel it on seven titles, but the cost climbs with the offset: fine for the first few pages, rough for a "jump to page 500" link. Keyset pagination is the fix.

## Keyset pagination for load-more

Instead of counting rows to skip, keyset remembers the last row it showed and asks for everything after it. The cursor is that row's sort key. Order by a stable total-order column (an `id` or a timestamp), show the first page, then carry the last value forward.

<SqlLogicTest id="cookbook/search/pagination/example_003" />

The last row on page one is `id` 5, so that value becomes the cursor. Page two asks for everything with a smaller `id` and takes the next three.

<SqlLogicTest id="cookbook/search/pagination/example_004" />

Keyset seeks straight to the cursor: the index jumps to `id` 5 and reads three rows, so page 500 costs the same as page one. The one requirement is a stable total-order key. An `id` or a timestamp qualifies because every row carries its own orderable value. A relevance score does not: `BM25` is a float, ties are common and the same query can score a row differently as the index changes, so it makes a weak cursor. When you need results in relevance order, cap them at a top-K window (the first `LIMIT` rows, no deep paging) instead of trying to keyset on the score.

## See also

- [Ranking](./ranking.md): choosing and tuning the sort your pages walk through
- [Faceted Search](./faceted-search.md): narrow the result set before you page it
