---
title: Elasticsearch API
sidebar_position: 14
split: headings
---

# Elasticsearch API

A SereneDB server can answer a subset of the Elasticsearch REST API over HTTP. Code and tools written for Elasticsearch, such as the official client libraries, ingestion jobs that post to `_bulk` and `curl` scripts, can create indices, write documents and search them without changes, as long as they stay inside the subset this page describes.

Every index is an ordinary table, so the same documents are queryable with SQL, and SQL remains the full interface to them. Use this API to keep existing Elasticsearch code working and SQL for everything it does not cover. To port the queries themselves, see [Migrating from Elasticsearch](../sql/indexes/inverted/migrating-from-elasticsearch.md).

## Enabling the API

Add an HTTP listener with `?api=es`:

```bash
serened ./data --listen 'postgres://127.0.0.1:7890,http://127.0.0.1:9200?api=es'
```

`--listen` takes every endpoint in one comma-separated value, so keep the PostgreSQL endpoint in the same list.

Indices live in the listener's database, which is `postgres` unless `db=` names another one, as in `?api=es&db=shop`. That database must exist, or the server does not start and logs `database 'shop' does not exist`.

To serve another HTTP API on the same port, repeat the `api` key: `?api=es&api=mcp` also serves the [MCP endpoint](../sql/functions/docs.md#documentation-for-ai-agents-over-mcp). Do not join the values with a comma. The comma separates endpoints in `--listen`, so `?api=es,mcp` stops the server at startup with `invalid network endpoint 'mcp'`.

Browser-based tools need CORS. `--http_cors_origins` takes a comma-separated list of allowed origins or `*`. The server echoes an allowed `Origin` and answers preflight `OPTIONS` requests.

Check that the listener answers:

```bash
curl -s -u postgres: http://127.0.0.1:9200/
```

```json
{"name":"serenedb","cluster_name":"serenedb","version":{"number":"8.11.0","build_flavor":"default"},"tagline":"You Know, for Search"}
```

The server reports version 8.11.0. Responses from its endpoints carry the `X-Elastic-Product: Elasticsearch` header that the official clients check before they accept a server.

## Authentication

Every request needs an `Authorization` header, from the local machine too. A request without one or with wrong credentials gets `401` with a `WWW-Authenticate: Basic` challenge.

| Scheme | Credentials | Enabled by |
|---|---|---|
| `Basic` | a catalog role and its [password](../security/client_authentication.md#passwords), the same credentials `psql` uses | always on |
| `Bearer` | a static token | `--auth_bearer_token=<token>` |
| `ApiKey` | base64 of a static `<id>:<key>` pair | `--auth_api_key=<id>:<key>` |

A role without a password, such as `postgres` on a fresh server, is accepted only from a loopback address. That is why the examples on this page use `-u postgres:`. From any other address such a role gets `401`.

Bearer and ApiKey answer `401` until their flag is set. They have no role of their own: requests that use them act as the `postgres` superuser.

```bash
serened ./data --listen 'postgres://127.0.0.1:7890,http://127.0.0.1:9200?api=es' --auth_bearer_token=s3cret
curl -s -H 'Authorization: Bearer s3cret' http://127.0.0.1:9200/_cat/indices
```

## Indices and mappings

`PUT /<index>` creates an index from the `properties` of its `mappings`:

```bash
curl -s -u postgres: -X PUT http://127.0.0.1:9200/articles \
  -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "title":  {"type": "text"},
      "tag":    {"type": "keyword"},
      "views":  {"type": "long"},
      "posted": {"type": "date"}
    }
  }
}'
```

```json
{"acknowledged":true,"shards_acknowledged":true,"index":"articles"}
```

### Field types

| Mapping type | SQL column type | Notes |
|---|---|---|
| `text` | `text` | analyzed for full-text search |
| `keyword` | `text` | matched as an exact value |
| `long` | `bigint` | |
| `integer` | `integer` | |
| `double` | `double precision` | |
| `float` | `real` | |
| `boolean` | `boolean` | |
| `date` | `timestamp without time zone` | stored in UTC |

- Any other type fails with `400 mapper_parsing_exception` and `No handler for type [...]`. That includes `object` and `nested`: fields are flat, and a field with `properties` of its own is rejected too.
- A field name cannot start with `_` or contain `.`.
- Only the `type` of each property is read. The request's `settings` and all other mapping parameters, such as `analyzer`, `fields`, `index` or `dynamic`, are accepted and ignored.
- A `PUT` without a body creates an index with no fields. Its documents are stored and returned whole, but only `match_all` and queries on `_id` find them.
- A mapping cannot change after the index exists.
- An index name uses lowercase letters, digits, `-`, `_`, `+` and `.`, is at most 255 characters long and cannot start with `-`, `_` or `+`.

### Text analysis

Every `text` field is analyzed the same way: split into words, lowercased and folded to unaccented letters, with no stemming and no stop words. So `cafe` finds `Café`, while `run` does not find `running`. The analyzer is the text search dictionary `es.standard`, which you can try from SQL:

```sql
SELECT ts_lexize('es.standard', 'The Café RUNNING runs');
-- {the,cafe,running,runs}
```

### Index endpoints

| Request | Answer |
|---|---|
| `GET /<index>` | the mapping, empty `aliases` and fixed `settings` |
| `GET /<index>/_mapping` | the mapping, with properties in alphabetical order |
| `HEAD /<index>` | `200` if the index exists, `404` if not |
| `DELETE /<index>` | drops the index with all its documents |
| `GET /_cat/indices` | one line per index (JSON with `?format=json`) |

## Writing documents

| Request | Writes |
|---|---|
| `PUT /<index>/_doc/<id>` | one document under that ID; `POST` works too |
| `POST /<index>/_doc` | one document under a generated 20-character ID |
| `POST /<index>/_bulk` | many documents from NDJSON; `PUT` works too |
| `POST /_bulk` | the same, into the index named by `_index` in the action lines |

```bash
curl -s -u postgres: -X PUT http://127.0.0.1:9200/articles/_doc/1 \
  -H 'Content-Type: application/json' \
  -d '{"title": "Postgres wire protocol explained", "tag": "db", "views": 120, "posted": "2026-09-01T10:00:00Z"}'
```

```json
{"_index":"articles","_id":"1","_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
```

- Documents are create-only. Writing an ID that already exists fails with `409 version_conflict_engine_exception`, whatever the endpoint or bulk action. There is no way to replace, update or delete a document through this API.
- The index must exist. A write to a missing index fails with `404 index_not_found_exception`, and no write creates an index.
- A document is one JSON object. Fields outside the mapping are kept in `_source` and returned with the document, but queries, sorts and aggregations cannot use them.
- An ID is at most 512 bytes and cannot be empty.

A mapped field takes a single JSON value. An array fails with `400 mapper_parsing_exception`, and so does a value the field does not accept:

| Mapping type | Accepts |
|---|---|
| `text`, `keyword` | strings |
| `long`, `integer` | numbers and numeric strings such as `"42"`; a fraction is dropped, so `12.9` becomes `12` |
| `double`, `float` | numbers and numeric strings |
| `boolean` | `true`, `false`, `"true"` and `"false"` |
| `date` | ISO 8601 strings (an offset is converted to UTC) and epoch milliseconds as a JSON number |

`null` leaves the column NULL, and so does an empty string for any type other than `text` and `keyword`.

### Bulk requests

```bash
curl -s -u postgres: -X POST 'http://127.0.0.1:9200/articles/_bulk?refresh=true' \
  -H 'Content-Type: application/x-ndjson' --data-binary @- <<'EOF'
{"index": {"_id": "2"}}
{"title": "Search with BM25 ranking", "tag": "search", "views": 300, "posted": "2026-09-02T11:00:00Z"}
{"create": {"_id": "3"}}
{"title": "Hybrid search in one SQL query", "tag": "search", "views": 80, "posted": "2026-09-10T09:30:00Z", "draft": true}
EOF
```

```json
{"took":22,"errors":false,"items":[{"index":{"_index":"articles","_id":"2","_version":1,"result":"created",...,"status":201}},{"create":{"_index":"articles","_id":"3","_version":1,"result":"created",...,"status":201}}]}
```

`draft` is not in the mapping, so it lives only in `_source`.

- An action line is `index` or `create`, followed by its document line. The two behave the same, since every write creates. A `delete` or `update` action fails the request with `400`.
- A bulk request writes to one index: the one in the URL or, for a bare `/_bulk`, the one named by `_index` in the first action line. Every `_index` in the body must name that index.
- `_id` is optional. Other action metadata, such as `routing`, is accepted and ignored.
- A bulk request is all or nothing. A bad line or an existing ID fails the whole request with an error response, and none of its documents is written. A successful response therefore always has `"errors": false`.

### Refresh and request bodies

- `?refresh=true`, `?refresh=wait_for` or a bare `?refresh` on a write refreshes the index before the response returns, so the new documents are searchable at once. `POST /<index>/_refresh` refreshes one index and `POST /_refresh` all of them. See [Visibility](#visibility).
- A request body is at most 64 MiB. A larger one gets `413`.
- The API does not decode `Content-Encoding`, so a gzip-compressed body fails. Leave request compression off in the client.

## Reading documents

| Request | Answer |
|---|---|
| `GET /<index>/_doc/<id>` | the document under `_source`, with `"found": true`; a missing ID answers `404` with `"found": false` |
| `HEAD /<index>/_doc/<id>` | `200` if the document exists, `404` if not |
| `GET /<index>/_source/<id>` | the document alone; a missing ID answers `404` |
| `POST /<index>/_mget` | the documents for `{"ids": [...]}` or `{"docs": [{"_id": ...}]}`, in request order, each with its own `found`; `GET` works too |

`_source` is the document exactly as it was sent. `_version` is always `1` and `_seq_no` always `0`, since a document never changes after it is written.

## Indices in SQL

An index is a table in the `es` schema of the listener's database. The table has `_id` as its primary key, one column per mapped field in alphabetical order and `_source`, which holds the document as sent. The `text` fields are indexed together in an [inverted index](../sql/indexes/inverted/index.md) named `<index>$text`.

```text
postgres=# \d es.articles
                          Table "es.articles"
 Column  |            Type             | Collation | Nullable | Default
---------+-----------------------------+-----------+----------+---------
 _id     | text                        |           | not null |
 posted  | timestamp without time zone |           |          |
 tag     | text                        |           |          |
 title   | text                        |           |          |
 views   | bigint                      |           |          |
 _source | text                        |           |          |
Indexes:
    "articles_pkey" PRIMARY KEY,
    "articles$text"
```

Plain predicates go against the table. Full-text predicates go against the `<index>$text` index relation, as with [any inverted index](../sql/indexes/inverted/index.md#querying-an-inverted-index):

```sql
SELECT _id, title, views FROM es.articles WHERE tag = 'search' ORDER BY views DESC;

SELECT _id, title, BM25(t.tableoid) AS score
FROM es."articles$text" AS t
WHERE title @@ 'search'
ORDER BY score DESC;
```

```text
 _id |             title              |   score
-----+--------------------------------+------------
 2   | Search with BM25 ranking       |  0.2268983
 3   | Hybrid search in one SQL query | 0.19128051
```

Quote index names that contain `-`, `.` or `+`, as in `es."app-logs.v1"`. A row inserted with SQL needs the document JSON in `_source`, since that column is what the API returns as the document.

## Searching

`GET` or `POST` on `/<index>/_search` searches one index, and `/<index>/_count` counts its matches. A request names exactly one index: a comma-separated list, `_all` or a path without an index answers `404`, and a name with `*` answers an empty result.

```bash
curl -s -u postgres: http://127.0.0.1:9200/articles/_search \
  -H 'Content-Type: application/json' -d '{"query": {"match": {"title": "search"}}}'
```

```json
{
  "took": 51,
  ...
  "hits": {
    "total": {"value": 2, "relation": "eq"},
    "max_score": 0.22689829766750336,
    "hits": [
      {"_index": "articles", "_id": "2", "_score": 0.22689829766750336,
       "_source": {"title": "Search with BM25 ranking", "tag": "search", "views": 300, "posted": "2026-09-02T11:00:00Z"}},
      {"_index": "articles", "_id": "3", "_score": 0.1912805140018463,
       "_source": {"title": "Hybrid search in one SQL query", "tag": "search", "views": 80, "posted": "2026-09-10T09:30:00Z", "draft": true}}
    ]
  }
}
```

### Request body

| Key | Accepts |
|---|---|
| `query` | the clauses below; without it every document matches |
| `size`, `from` | integers, `10` and `0` by default; `from + size` is at most 10000 |
| `sort` | see [Sorting and paging](#sorting-and-paging) |
| `_source` | `true` or `false` |
| `aggs`, `aggregations` | see [Aggregations](#aggregations) |
| `track_total_hits` | accepted and ignored, since totals are always exact |

Any other key fails with `400 illegal_argument_exception`, for example `highlight`, `search_after`, `fields` or `collapse`.

In the URL, `size` and `from` apply unless the body sets them, and `rest_total_hits_as_int=true` reports `hits.total` as a plain number. Every other URL parameter is ignored, `q` and `sort` included: `/articles/_search?q=title:wire` returns every document.

`_count` accepts only `query` in its body.

### Query DSL

| Clause | Form | Matches |
|---|---|---|
| `match_all` | `{}` | every document |
| `match` | `{"<field>": "text"}` or `{"<field>": {"query": "text", "operator": "and"}}` | on a `text` field, documents with any term of the analyzed text, or with all of them for `"operator": "and"`; on other fields, the exact value |
| `match_phrase` | `{"<field>": "text"}` or `{"<field>": {"query": "text"}}` | on a `text` field, the terms adjacent and in order; on other fields, the exact value |
| `term` | `{"<field>": value}` or `{"<field>": {"value": value}}` | the exact value; on a `text` field the value is analyzed first, so `"Quick"` finds `quick` |
| `range` | `{"<field>": {"gte": a, "lt": b}}` with any of `gt`, `gte`, `lt` and `lte` | values inside the bounds |
| `bool` | `must`, `filter`, `must_not`, `should` and `minimum_should_match` | see [Bool queries](#bool-queries) |

A `match`, `match_phrase` or `term` on a `text` field is a full-text clause. The rest of this page uses that name.

- A clause names one field. A field that is not in the mapping matches nothing.
- `match` and `match_phrase` take a string. Use `term` or `range` for numbers.
- `range` bounds on a `date` field are ISO 8601 strings in UTC, like `"2026-09-02"` or `"2026-09-02T10:30:00Z"`. Epoch milliseconds and date math such as `now-30d` fail with `400`.
- Other clause types fail with `400` and `query type [...] is not supported yet`, for example `terms`, `prefix`, `wildcard`, `fuzzy`, `exists`, `ids`, `multi_match`, `query_string` and `nested`. Parameters beyond the ones in the table fail the same way, for example `boost`, `fuzziness`, `slop` and `format`.

### Bool queries

- `must` and `filter` both require all their clauses, and a full-text clause scores in either of them.
- `must_not` excludes the documents that match any of its clauses.
- Without `must` or `filter`, at least one `should` clause has to match.
- With `must` or `filter`, `should` clauses are ignored: they neither filter nor add to the score. Set `minimum_should_match` to `1` to require one of them.
- `minimum_should_match` accepts only the integers `0` and `1`.
- A `should` group cannot mix full-text clauses with other clauses.
- Each group takes one clause or an array of clauses.

```bash
curl -s -u postgres: http://127.0.0.1:9200/articles/_search \
  -H 'Content-Type: application/json' -d '{
  "query": {
    "bool": {
      "must":   [{"match": {"title": "search"}}],
      "filter": [{"range": {"views": {"gte": 100}}}]
    }
  },
  "_source": false
}'
```

```json
{...,"hits":{"total":{"value":1,"relation":"eq"},"max_score":0.22689829766750336,"hits":[{"_index":"articles","_id":"2","_score":0.22689829766750336}]}}
```

### Scores

- A query with a full-text clause and no `sort` is ranked by [BM25](../sql/indexes/inverted/ranking.md): hits come in score order, `_score` is the BM25 score and `max_score` the highest one.
- Without a full-text clause, every hit scores `1.0`.
- With a `sort`, `_score` and `max_score` are `null`.

### Sorting and paging

```bash
curl -s -u postgres: http://127.0.0.1:9200/articles/_search \
  -H 'Content-Type: application/json' -d '{"sort": [{"views": "desc"}], "size": 2, "_source": false}'
```

```json
{...,"hits":{"total":{"value":3,"relation":"eq"},"max_score":null,"hits":[{"_index":"articles","_id":"2","_score":null,"sort":[300]},{"_index":"articles","_id":"1","_score":null,"sort":[120]}]}}
```

- `sort` takes a field name (ascending), `{"<field>": "desc"}`, `{"<field>": {"order": "desc"}}` or an array of these. `_score` and `_doc` entries are accepted and skipped.
- Documents without a value sort last in both directions.
- Each hit carries its `sort` values, with dates as epoch milliseconds.
- Sorting by a field that is not in the mapping fails with `400 query_shard_exception`. Other sort parameters, such as `missing`, fail with `400`.
- `from` and `size` reach at most 10000 hits deep. Use a [scroll](#scroll) to read further.

### Aggregations

```bash
curl -s -u postgres: http://127.0.0.1:9200/articles/_search \
  -H 'Content-Type: application/json' -d '{
  "size": 0,
  "aggs": {
    "tags":   {"terms": {"field": "tag"}},
    "weekly": {"date_histogram": {"field": "posted", "calendar_interval": "week"}},
    "views":  {"sum": {"field": "views"}}
  }
}'
```

```json
"aggregations": {
  "tags": {
    "doc_count_error_upper_bound": 0,
    "sum_other_doc_count": 0,
    "buckets": [{"key": "search", "doc_count": 2}, {"key": "db", "doc_count": 1}]
  },
  "weekly": {
    "buckets": [
      {"key_as_string": "2026-08-31T00:00:00.000Z", "key": 1788134400000, "doc_count": 2},
      {"key_as_string": "2026-09-07T00:00:00.000Z", "key": 1788739200000, "doc_count": 1}
    ]
  },
  "views": {"value": 500.0}
}
```

| Type | Parameters | Result |
|---|---|---|
| `terms` | `field`, `size` (10 by default, at most 10000) | `buckets` with the largest `doc_count` first; `sum_other_doc_count` counts the rest |
| `date_histogram` | `field`, `calendar_interval` | `buckets` with `key` in epoch milliseconds and `key_as_string` |
| `min`, `max` | `field` | `value`, with dates as epoch milliseconds |
| `avg`, `sum` | `field` | `value` |
| `value_count` | `field` | `value`, the number of documents with a value |
| `cardinality` | `field` | `value`, an approximate number of distinct values |

- `calendar_interval` is one of `minute`, `hour`, `day`, `week`, `month`, `quarter` or `year`, or the short forms `1m`, `1h`, `1d`, `1w`, `1M`, `1q` or `1y`. Weeks start on Monday, and buckets without documents are left out.
- Aggregations cover the documents the query matches. `"size": 0` returns the aggregations without hits.
- Sub-aggregations fail with `400`, and so do other aggregation types and parameters, for example `histogram`, `percentiles`, `order`, `missing` and `fixed_interval`.
- An aggregation on a field that is not in the mapping fails with `400 query_shard_exception`.

### Scroll

```bash
curl -s -u postgres: 'http://127.0.0.1:9200/articles/_search?scroll=1m&size=2' \
  -H 'Content-Type: application/json' -d '{"query": {"term": {"tag": "search"}}}'
```

The response carries a `_scroll_id`. Post it to `/_search/scroll`, as `{"scroll_id": "..."}` in the body or `?scroll_id=` in the URL, to get the next page, and repeat until a page comes back without hits. `DELETE /_search/scroll` answers success.

- A scroll search cannot have `sort` (other than `_doc`), `aggs` or `from`.
- Pages come in `_id` order, with `_score` set to `null`.
- The scroll ID carries the query and the last `_id` returned, not a snapshot. Documents written during the scroll can appear on later pages, `hits.total` is the count from the first page, the keep-alive is not enforced and an ID can be used again.

The Python client's `helpers.scan` pages through a scroll this way.

## Visibility

- `GET /<index>/_doc/<id>`, `_source`, `_mget` and searches without a full-text clause read the table, so they see a document as soon as its write returns.
- Searches with a full-text clause, with their counts and aggregations, read the `<index>$text` index. It picks up new documents when it refreshes, every second by default (its [`refresh_interval`](../sql/indexes/inverted/maintenance.md#visibility-and-the-refresh-model)).
- `?refresh=true` on the write or `POST /<index>/_refresh` makes new documents visible to those searches at once.

```bash
curl -s -u postgres: -X PUT http://127.0.0.1:9200/articles/_doc/4 \
  -H 'Content-Type: application/json' -d '{"title": "Vector search basics", "tag": "search", "views": 5}'

curl -s -u postgres: http://127.0.0.1:9200/articles/_count \
  -H 'Content-Type: application/json' -d '{"query": {"match": {"title": "vector"}}}'
# {"count":0,...}  the text index has not refreshed yet

curl -s -u postgres: http://127.0.0.1:9200/articles/_count \
  -H 'Content-Type: application/json' -d '{"query": {"term": {"tag": "search"}}}'
# {"count":3,...}  document 4 is already in the table
```

After the next refresh, the first count returns `1`.

## Using a client library

Point an Elasticsearch client at the listener and give it role credentials. This example runs with the official Python client, versions 8.19 and 9.5:

```python
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch("http://127.0.0.1:9200", basic_auth=("postgres", ""))

helpers.bulk(es, [
    {"_index": "articles", "_id": "5",
     "_source": {"title": "Faceted search with SQL", "tag": "search", "views": 42}},
], refresh=True)

resp = es.search(index="articles", query={"match": {"title": "search"}}, size=3)
for hit in resp["hits"]["hits"]:
    print(hit["_id"], hit["_score"], hit["_source"]["title"])
```

```text
4 0.14807166159152985 Vector search basics
2 0.1333625465631485 Search with BM25 ranking
5 0.1333625465631485 Faceted search with SQL
```

The client's defaults fit the API: `helpers.bulk` posts to `/_bulk` with `_index` on every action line, and request compression is off. The client raises its usual exceptions for the [errors](#errors) below, such as `ConflictError` for an existing ID and `NotFoundError` for a missing index.

## Cluster endpoints

Tools that check a cluster before they start get fixed answers:

| Request | Answer |
|---|---|
| `GET /_cluster/health`, `GET /_cluster/health/<index>` | status `green` with one node |
| `GET /_nodes/stats` | one node with empty statistics |
| `GET /_stats`, `GET /<index>/_stats` | no merges in progress |
| `POST /_forcemerge`, `POST /<index>/_forcemerge` | success, without doing anything |
| `GET /_cluster/settings`, `PUT /_cluster/settings` | empty settings; a `PUT` is acknowledged and stores nothing |

None of them reflects the real state of the server.

## What is not supported

- Replacing, updating or deleting documents. An existing ID answers `409`, and `_update`, `_update_by_query`, `_delete_by_query` and `DELETE /<index>/_doc/<id>` answer `404`.
- Creating an index on first write, dynamic mapping and mapping changes.
- `object` and `nested` fields, arrays, other field types, analyzers and multi-fields.
- Requests across several indices, index patterns, aliases, index templates, `_msearch` and `_analyze`.
- The `q` URL parameter and query clauses or parameters beyond the ones listed above.
- `highlight`, `search_after`, `fields`, `collapse` and other search body keys beyond the ones listed above.
- Sub-aggregations and other aggregation types.
- Compressed request bodies.

## Errors

Errors use the Elasticsearch envelope:

```json
{"error":{"type":"index_not_found_exception","reason":"no such index [nosuch]"},"status":404}
```

| Status | `type` | Cause |
|---|---|---|
| 400 | `parsing_exception` | the body is not a JSON object or a `_count` body has keys other than `query` |
| 400 | `illegal_argument_exception` | an unsupported clause, parameter, aggregation or bulk action; an invalid `size`, `from` or document ID |
| 400 | `mapper_parsing_exception` | an invalid mapping; a document or value that does not parse |
| 400 | `invalid_index_name_exception` | an index name outside the naming rules |
| 400 | `resource_already_exists_exception` | the index already exists |
| 400 | `query_shard_exception` | a sort or aggregation on a field that is not in the mapping |
| 403 | `security_exception` | the role does not exist or cannot log in |
| 404 | `index_not_found_exception` | the index does not exist |
| 404 | `resource_not_found_exception` | `_source/<id>` for a missing document |
| 404 | `search_context_missing_exception` | a scroll ID that does not decode |
| 409 | `version_conflict_engine_exception` | a document with that ID already exists |
| 500 | `exception` | any other failure |

A few answers have no envelope: `401` with `{"error":"unauthorized"}` for missing or wrong credentials, `404` with `{"error":"not_found"}` for a path the API does not serve, `413` for a body over 64 MiB and, as in Elasticsearch, the `404` with `"found": false` for a missing document on `GET /<index>/_doc/<id>`.
