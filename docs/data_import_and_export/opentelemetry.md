---
title: OpenTelemetry
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB stores OpenTelemetry logs, traces and metrics in a fixed set of **search tables**, so a log body is full-text searchable and rankable with BM25 without a second index relation to maintain.

The schema ships in the repository as [`resources/otel/otel_schema.sql`](https://github.com/serenedb/serenedb/blob/main/resources/otel/otel_schema.sql). Apply it with `psql`, or let the `otel` listener create it at startup.

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -f resources/otel/otel_schema.sql
```

## Receiving OTLP over HTTP

Add an HTTP listener with the `otel` API and SereneDB serves the three OTLP export endpoints directly — no collector component to install:

```bash
serened ./data --listen 'postgres://0.0.0.0:7890,http://0.0.0.0:4318?api=otel'
```

By default the tables live in the `public` schema of the default database. `db=` puts the listener — and with it the OTel tables — in another database, and `schema=` in another schema; both are created if they do not exist:

```bash
serened ./data --listen 'postgres://0.0.0.0:7890,http://0.0.0.0:4318?api=otel&db=telemetry&schema=otel'
```

| Endpoint | Accepts | Writes to |
|---|---|---|
| `POST /v1/logs` | `ExportLogsServiceRequest` | `otel_logs` |
| `POST /v1/traces` | `ExportTraceServiceRequest` | `otel_traces` |
| `POST /v1/metrics` | `ExportMetricsServiceRequest` | the five `otel_metrics_*` tables |

Point any OTel SDK or the collector's stock `otlphttp` exporter at it:

```yaml
exporters:
  otlphttp:
    endpoint: http://serenedb:4318
    auth:
      authenticator: basicauth

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp]
```

A successful export answers `200` with an empty `Export<Signal>ServiceResponse` (`{}`).

The tables are created **at startup**, in the listener's database, when a listener serves `?api=otel` without `create_schema=false` and the schema is not there yet — so a fresh database needs no setup step, and the first export lands in a schema that already exists.

Startup leaves a table that exists as it is, and checks that every table has every shipped column in its shipped type: a missing or retyped column stops the server at startup with a message naming the database, the schema and the column. Fix the table, or leave it alone and start with the built-in schema somewhere new: `db=` or `schema=` on the listener creates it, e.g. `--listen='http://0.0.0.0:4318?api=otel&schema=otel'`.

For a schema of your own, start the listener with `create_schema=false`: see [Your own schema](#your-own-schema).

A table dropped or changed after startup fails each export to it with `503` until it is fixed: `the OpenTelemetry schema is missing: …` for a dropped table, and `invalid OpenTelemetry schema: column … is …, expected …` for a changed one. Other failures follow the OTLP retry rules: rejected data answers `400`, which clients do not retry, and a transient server condition (a transaction conflict, out of memory, shutdown) answers `503`, which they do.

Each export is written in one transaction, so a failed export stores nothing: a metrics export whose points span several `otel_metrics_*` tables is written to all of them or to none, and a client retry cannot duplicate the tables that succeeded the first time.

### Encodings

Both OTLP encodings are accepted, and the response always uses the request's:

| `Content-Type` | Notes |
|---|---|
| `application/x-protobuf` | the OTLP binary wire format, and the collector's default |
| `application/json` | ProtoJSON |

In binary payloads, unknown fields are skipped, so a newer sender keeps working.

JSON follows the OTLP ProtoJSON rules: both lowerCamelCase and the original snake_case field names, 64-bit integers as decimal strings or numbers, enums as integers or as their proto value names, trace and span ids as hex, and unknown fields are skipped.

The same payload produces the same rows in either encoding.

Request bodies may be compressed with any coding the HTTP listener supports — `gzip` (the collector's default), `zstd`, `br`, `deflate`, `lz4`, `zxc` or `snappy`; see [HTTP Compression](../configuration/http_compression.md). An unsupported `Content-Encoding` answers `415`, and a corrupt compressed body `400`, both before the payload is decoded.

A request body is at most 100 MiB, and a larger one answers `413`. The limit applies to the body after decompression too. A listener that also serves other APIs (`?api=otel&api=es`) applies the largest of their limits to every request.

### Batching

Send few large requests rather than many small ones. Each export is one transaction with a fixed cost, and a large export is parsed and inserted on several threads, so throughput grows with the request size up to the 100 MiB limit. SDKs export small batches, so put an OpenTelemetry Collector in front of SereneDB as a batching proxy: the SDKs send to the collector, and its `batch` processor merges their exports into large requests to SereneDB:

```yaml
processors:
  batch:
    send_batch_size: 50000
    send_batch_max_size: 100000
    timeout: 5s

exporters:
  otlphttp:
    endpoint: http://serenedb:4318
    auth:
      authenticator: basicauth

service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlphttp]
```

The batch sizes count items (log records, spans or metric data points), not bytes: keep `send_batch_max_size` low enough that a request stays under 100 MiB after decompression.

The listener serves several exports at once, each on a connection of its own. When every export is a large batch, throughput and latency are best with about as many exports in flight as the server has CPU cores. The collector's `otlphttp` exporter sends `sending_queue.num_consumers` exports at once, 10 by default; on a 16-core server:

```yaml
exporters:
  otlphttp:
    endpoint: http://serenedb:4318
    sending_queue:
      num_consumers: 16
```

Errors of the export itself use `google.rpc.Status`, encoded the same way as the request: `400` for an undecodable payload.

Authentication is the HTTP layer's usual Basic auth against the catalog roles.

### Mapping

OTLP data is stored by these rules:

- `time_unix_nano` of 0 falls back to `observed_time_unix_nano`.
- All-zero or empty trace and span ids become `NULL`.
- `resource.attributes["service.name"]` is promoted to `service_name` and kept in `resource_attributes` as well.
- A non-string log body is stored as compact JSON text.
- Attribute maps are stored as JSON objects with sorted keys.
- Span `kind` and `status.code` are stored in their string form (`Server`, `Error`, ...), and `duration_ns` is computed from the span's end and start.
- Span events and links stay in the span row as JSON, with `event_names` and `link_trace_ids` alongside them for index-side lookups.
- A data point's value is stored as `DOUBLE PRECISION` whether OTLP sent it as an integer or a double, so an integer gauge beyond 2^53 loses precision.
- An exponential histogram's `scale` and bucket offsets keep their sign, so negative values are stored as sent.

## Tables

| Table | One row per |
|---|---|
| `otel_logs` | log record |
| `otel_traces` | span |
| `otel_metrics_gauge` | gauge data point |
| `otel_metrics_sum` | sum (counter) data point |
| `otel_metrics_histogram` | explicit-bucket histogram data point |
| `otel_metrics_exponential_histogram` | exponential histogram data point |
| `otel_metrics_summary` | summary data point |

The five metric tables exist because the OTLP metric shapes do not share a column set — a gauge carries one `DOUBLE PRECISION`, a histogram carries bucket arrays, a summary carries quantile pairs.

Each table is created `WITH (storage = 'search')` and gets one inverted index covering what dashboards filter, group and facet on. Columns outside the index are still stored and retrievable; they are read with a columnstore scan.

## Your own schema

The shipped tables and their indexes are defined in [`resources/otel/otel_schema.sql`](https://github.com/serenedb/serenedb/blob/main/resources/otel/otel_schema.sql). To run your own, for example with another codec for a column or an index of your own, start the listener with `create_schema=false` and create the seven tables from your own copy of that file:

```bash
serened ./data --listen 'postgres://0.0.0.0:7890,http://0.0.0.0:4318?api=otel&create_schema=false'
psql -h 127.0.0.1 -p 7890 -U postgres -d postgres -f my_otel_schema.sql
```

With `create_schema=false`, startup creates nothing — no database, schema, table or index — so the listener's database must exist, or the server does not start. A missing or mismatched table is logged as a warning at startup instead of stopping the server. Create each table where it does not exist yet: a search table's columns [cannot change](#schema-evolution) once it exists, so the example below uses plain `CREATE`, which fails on a table that is already there instead of skipping it.

Each table must keep every shipped column in its shipped type, in any order; column options that keep the type, such as `USING COMPRESSION`, and the index are yours to choose. An extra column is left `NULL` by ingest, even with a `DEFAULT`.

Until the tables an export writes to exist and match, the export answers `503`, which OTLP clients retry with backoff: what they send while you set the tables up arrives once the tables are in place, as long as the client is still retrying. A JSON export to a missing table gets:

```json
{"code":14,"message":"the OpenTelemetry schema is missing: Table with name otel_logs does not exist! …","details":[]}
```

For example, this `otel_logs` stores `body` compressed with zstd and indexes it with `otel_words`, which splits identifiers such as `downstream_remote_disconnect` into their parts, and its index adds the `error.type` log attribute, kept whole by `otel_keyword`, a `keyword` dictionary; the other six tables are as shipped:

<SqlLogicTest id="data_import_and_export/opentelemetry/own_schema" hideResult />

The shipped `otel_body_dict` keeps words joined by `_` whole; `otel_words` splits at every character that is not a letter or a digit, so `downstream_remote_disconnect` becomes three words:

<SqlLogicTest id="data_import_and_export/opentelemetry/body_tokens" />

Each part is then searchable on its own:

<SqlLogicTest id="data_import_and_export/opentelemetry/body_part" />

A plain string matches a row holding any of its words, so `body @@ 'downstream_remote_disconnect'` also finds `remote peer closed the stream`. Search the whole identifier as a phrase, its parts in order:

<SqlLogicTest id="data_import_and_export/opentelemetry/body_phrase" />

`otel_keyword` keeps each `error.type` value whole, so `downstream_remote_disconnect` matches only as a whole, and `remote` finds only the record whose `error.type` is exactly `remote`; a column or expression indexed without a dictionary, such as `service_name`, matches the same way:

<SqlLogicTest id="data_import_and_export/opentelemetry/attribute_filter" />

Facet the attribute with `GROUP BY` on the indexed expression, which the index answers from its term dictionary; `IS NOT NULL` drops the group of records without the attribute:

<SqlLogicTest id="data_import_and_export/opentelemetry/attribute_counts" />

`pragma_storage_info` shows the codec of each column:

<SqlLogicTest id="data_import_and_export/opentelemetry/body_compression" />

## Timestamps

Every table carries `"timestamp" TIMESTAMP_NS NOT NULL`, indexed, plus the signal's own `observed_timestamp`, `end_timestamp` or `start_timestamp`. Nanosecond precision is preserved on the way in through `INSERT` and through `COPY ... FROM STDIN` in PG **text** format:

```sql
SELECT epoch_ns("timestamp") FROM otel_logs ORDER BY 1 LIMIT 1;
```

The PG **binary** (extended) protocol truncates `TIMESTAMP_NS` to microseconds in both directions. Clients that read through pgx or Grafana therefore see microseconds; `epoch_ns("timestamp")` returns the exact integer when the extra digits matter.

## Attributes

Attributes are stored in two tiers:

- **Promoted columns** — `service_name`, `severity_text`, `severity_number`, `event_name`, `span_name`, `span_kind`, `status_code`, `metric_name`. These are what the index covers, so they are what you filter and facet on.
- **JSON blobs** — `resource_attributes`, `scope_attributes`, and one of `log_attributes` / `span_attributes` / `attributes`. Lossless, retrievable, and filterable with `->>` (a columnstore scan).

A deployment that knows its hot attribute paths can promote them to index speed with an expression index in the index of [its own schema](#your-own-schema), such as `(log_attributes ->> 'error.type')`. That buys a term filter and facets with `GROUP BY` on the same expression. The `ts_dict_*` functions take an indexed column only: on an expression each fails with `<function>() argument must be an indexed column`, for example `ts_dict_agg() argument must be an indexed column`.

## The body dictionary

`body` is analyzed with `otel_body_dict`, which splits on Unicode word boundaries and lowercases, with no stemming:

```sql
CREATE TEXT SEARCH DICTIONARY otel_body_dict AS
    split_text(case := 'lower', break := 'alpha')
    WITH (frequency, position, norm);
```

Log bodies are multilingual and full of identifiers, paths, IPs and durations, where stemming is wrong. Splitting on word boundaries keeps dotted identifiers and addresses whole:

```sql
SELECT ts_lexize('otel_body_dict', 'ERROR io.grpc.Client 10.0.0.7:5432 took 12ms');
-- {error,io.grpc.client,10.0.0.7,5432,took,12ms}
```

## Querying

Search and range predicates go against the table; it is already an index scan.

```sql
SELECT "timestamp", service_name, severity_text, body,
       BM25(otel_logs.tableoid) AS score
FROM otel_logs
WHERE body @@ websearch_to_tsquery('"connection reset" -healthcheck')
  AND "timestamp" @@ ts_between(TIMESTAMP_NS '2026-09-08 00:00:00',
                                TIMESTAMP_NS '2026-09-09 00:00:00', true, false)
ORDER BY score DESC LIMIT 20;
```

Facets are served straight from the term dictionary:

```sql
SELECT unnest(ts_dict_agg(severity_text)) AS severity,
       unnest(ts_dict_count(severity_text)) AS n
FROM otel_logs;

SELECT unnest(ts_dict_agg(service_name)) AS service,
       unnest(ts_dict_count(service_name)) AS n
FROM otel_logs WHERE body @@ 'timeout';
```

A trace and its correlated logs:

```sql
SELECT * FROM otel_traces
WHERE trace_id @@ '0af7651916cd43dd8448eb211c80319c' ORDER BY "timestamp";

SELECT "timestamp", severity_text, body FROM otel_logs
WHERE trace_id @@ '0af7651916cd43dd8448eb211c80319c' ORDER BY "timestamp";
```

Span events and links are kept in the span row as JSON, with two searchable side arrays so the common lookups stay index-side:

```sql
SELECT trace_id, span_id, span_name FROM otel_traces WHERE event_names @@ 'exception';
SELECT trace_id, span_id FROM otel_traces WHERE link_trace_ids @@ '1bc2d3e4f5a6b7c8d9e0f1a2b3c4d5e6';
```

Latency and error rate:

```sql
SELECT service_name, quantile_cont(duration_ns, [0.5, 0.95, 0.99]) AS p
FROM otel_traces
WHERE "timestamp" @@ ts_ge(TIMESTAMP_NS '2026-09-08 00:00:00')
  AND span_kind @@ 'Server'
GROUP BY service_name;

SELECT date_trunc('minute', "timestamp") AS minute,
       count(*) FILTER (WHERE status_code = 'Error')::DOUBLE / count(*) AS error_rate
FROM otel_traces
WHERE "timestamp" @@ ts_ge(TIMESTAMP_NS '2026-09-08 00:00:00')
GROUP BY 1 ORDER BY 1;
```

A gauge rollup:

```sql
SELECT metric_name, date_trunc('minute', "timestamp") AS minute, avg(value)
FROM otel_metrics_gauge
WHERE metric_name @@ 'process.memory.usage'
  AND "timestamp" @@ ts_ge(TIMESTAMP_NS '2026-09-08 00:00:00')
GROUP BY 1, 2 ORDER BY 1, 2;
```

### Counters and histograms

SDKs export sums and histograms as running totals by default (`aggregation_temporality = 'Cumulative'`), so `sum(value)` adds up totals instead of counting what happened in a time bucket. [`otel_rate`, `otel_increase` and `otel_irate`](../sql/functions/aggregates/index.md#counter-and-histogram-aggregates) read them as counters. Compute them per series, one metric with one set of resource and data point attributes, then add the series up:

```sql
SELECT minute, service_name, sum(per_second) AS requests_per_second
FROM (
  SELECT date_trunc('minute', "timestamp") AS minute, service_name,
         resource_attributes, attributes,
         otel_rate(value, "timestamp") AS per_second
  FROM otel_metrics_sum
  WHERE metric_name = 'http.server.request.count'
  GROUP BY ALL
)
GROUP BY ALL ORDER BY ALL;
```

Latency percentiles come from the histogram buckets the same way: `otel_increase(bucket_counts, "timestamp")` per series, then `otel_histogram_quantile` over the series:

```sql
SELECT minute,
       otel_histogram_quantile(0.5, buckets, explicit_bounds) AS p50,
       otel_histogram_quantile(0.99, buckets, explicit_bounds) AS p99
FROM (
  SELECT date_trunc('minute', "timestamp") AS minute,
         resource_attributes, attributes, explicit_bounds,
         otel_increase(bucket_counts, "timestamp") AS buckets
  FROM otel_metrics_histogram
  WHERE metric_name = 'http.server.duration'
  GROUP BY ALL
)
GROUP BY minute ORDER BY minute;
```

Points exported with delta temporality (`aggregation_temporality = 'Delta'`) need neither: `sum(value)` and `otel_histogram_quantile(q, bucket_counts, explicit_bounds)` over the rows of a time bucket give what happened in it.

The same per-series buckets give the share of requests under a latency target with `otel_histogram_fraction`, and the spread with `otel_histogram_stddev`:

```sql
SELECT minute,
       otel_histogram_fraction('-inf'::DOUBLE, 0.3, buckets, explicit_bounds) AS under_300ms,
       otel_histogram_stddev(buckets, explicit_bounds) AS stddev
FROM (
  SELECT date_trunc('minute', "timestamp") AS minute,
         resource_attributes, attributes, explicit_bounds,
         otel_increase(bucket_counts, "timestamp") AS buckets
  FROM otel_metrics_histogram
  WHERE metric_name = 'http.server.duration'
  GROUP BY ALL
)
GROUP BY minute ORDER BY minute;
```

Exponential histograms (`otel_metrics_exponential_histogram`) are read the same way once [`otel_exponential_histogram`](../sql/functions/aggregates/index.md#counter-and-histogram-aggregates) packs their bucket columns into one value; rows at different scales are combined:

```sql
SELECT minute,
       otel_histogram_quantile(0.99, h) AS p99,
       otel_histogram_fraction('-inf'::DOUBLE, 0.3, h) AS under_300ms
FROM (
  SELECT date_trunc('minute', "timestamp") AS minute, resource_attributes, attributes,
         otel_increase(otel_exponential_histogram(scale, zero_count, positive_offset, positive_bucket_counts,
                                                  negative_offset, negative_bucket_counts), "timestamp") AS h
  FROM otel_metrics_exponential_histogram
  WHERE metric_name = 'http.server.duration'
  GROUP BY ALL
)
GROUP BY minute ORDER BY minute;
```

Gauges (`otel_metrics_gauge`) are averaged or compared directly; `otel_deriv` gives their per-second trend and `otel_predict_linear` extrapolates it, for example the free disk space an hour after the last sample:

```sql
SELECT service_name, otel_predict_linear(value, "timestamp", 3600) AS free_in_an_hour
FROM otel_metrics_gauge
WHERE metric_name = 'system.filesystem.free'
  AND "timestamp" @@ ts_ge(TIMESTAMP_NS '2026-09-08 00:00:00')
GROUP BY service_name;
```

On a column indexed without a dictionary, such as `service_name` or `severity_text`, a plain `=` is served by the index exactly like `@@`. On an analyzed column such as `body`, `=` compares the whole value as a columnstore `Column Filter`: correct, but it reads the column instead of the index. Use `@@` to search those.

## Visibility

Search tables are commit-time visible: a row becomes visible to *any* query after the next refresh, governed by `refresh_interval` (1 s by default). Force it when you need determinism, for example in tests:

```sql
VACUUM (REFRESH_TABLE) otel_logs;
```

## Retention

The tables keep every row until it is deleted. A [job](../sql/statements/create_job/index.md) that deletes old rows on a schedule keeps each table to a [retention period](../sql/statements/delete/index.md#retention) of its own:

<SqlLogicTest id="data_import_and_export/opentelemetry/retention" hideResult />

Telemetry arrives in roughly time order, so the rows a job deletes are mostly consecutive: the log space of the delete grows with the number of such stretches, not with the number of rows.

## Schema evolution

A search table's schema is fixed. `ALTER TABLE ADD COLUMN`, `DROP COLUMN` and `ALTER COLUMN TYPE` are rejected; an inverted index can still be added later, and it indexes the rows already in the table. To change the schema, create a new table with `CREATE TABLE ... AS SELECT`, switch writers to it, and rename.

Re-running the shipped DDL is safe — every statement is `IF NOT EXISTS`, including on a table that already holds rows. Startup creates only the tables that are missing, so a restart of a complete schema does no work at all.
