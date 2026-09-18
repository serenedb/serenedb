---
title: OpenTelemetry
split: headings
---

SereneDB stores OpenTelemetry logs, traces and metrics in a fixed set of
**search tables**, so a log body is full-text searchable and rankable with BM25
without a second index relation to maintain.

The schema ships in the repository as `resources/otel/otel_schema.sql`. Apply it
with `psql`, or let the OTLP endpoint create it on first write.

```bash
psql -h 127.0.0.1 -p 5432 -U postgres -d postgres -f resources/otel/otel_schema.sql
```

## Receiving OTLP over HTTP

Add an HTTP listener with the `otlp` API and SereneDB serves the three OTLP
export endpoints directly — no collector component to install:

```bash
serened ./data --listen 'postgres://0.0.0.0:5432,http://0.0.0.0:4318?api=otlp'
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
    compression: none
    auth:
      authenticator: basicauth

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp]
```

A successful export answers `200` with an empty `Export<Signal>ServiceResponse`
(`{}`).

The tables are created **at startup**, when a listener serves `?api=otlp` and
the schema is not there yet — so a fresh database needs no setup step, and the
first export lands in a schema that already exists. A server started without
the `otlp` API creates nothing, and an export against a missing schema answers
`500` naming the absent relation rather than creating it behind your back.

To run a schema of your own — extra promoted columns, expression indexes over
hot attribute paths — apply it before the first start; startup leaves an
existing schema alone.

### Encodings

Both OTLP encodings are accepted, and the response always uses the request's:

| `Content-Type` | Notes |
|---|---|
| `application/x-protobuf` | the OTLP binary wire format, and the collector's default |
| `application/json` | ProtoJSON |

The JSON reader takes both lowerCamelCase and the original snake_case field
names, 64-bit integers as decimal strings or numbers, enums as integers or
names, and trace/span ids as hex (the OTLP deviation from ProtoJSON, which
would otherwise ask for base64). The protobuf reader skips unknown fields, so
a newer sender keeps working, and accepts both packed and unpacked encodings
of repeated numeric fields.

Both decoders feed the same mapper, and the conformance fixtures ship as an
`.otlp.json` and an `.otlp.pb` of the same payload: a test asserts that each
row shape arrives twice, once per decoder.

**No request compression yet.** A `Content-Encoding` header answers `400`, so
set `compression: none` on the exporter.

Errors use `google.rpc.Status`, encoded the same way as the request: `400` for
an undecodable payload or an unsupported `Content-Encoding`.

Authentication is the HTTP layer's usual Basic auth against the catalog roles.

### Mapping

Every route into SereneDB produces the same rows for the same OTLP input; the
rules live in `resources/otel/conformance/`, as an OTLP request paired with the
exact rows it must produce. The ones worth knowing:

- `time_unix_nano` of 0 falls back to `observed_time_unix_nano`.
- All-zero or empty trace and span ids become `NULL`.
- `resource.attributes["service.name"]` is promoted to `service_name` and kept
  in `resource_attributes` as well.
- A non-string log body is stored as compact JSON text.
- Attribute maps are serialized with sorted keys, so two routes cannot disagree
  on byte order.
- Span `kind` and `status.code` are stored in their string form (`Server`,
  `Error`, ...), and `duration_ns` is computed from the span's end and start.
- Span events and links stay in the span row as JSON, with `event_names` and
  `link_trace_ids` alongside them for index-side lookups.
- A data point's value is stored as `DOUBLE PRECISION` whether OTLP sent it as
  an integer or a double, so an integer gauge beyond 2^53 loses precision.

The exponential histogram's `scale` and bucket `offset` are `sint32` on the
wire — zigzag-encoded, so a negative scale is only read correctly by a decoder
that knows it. Both of ours do, and a fixture pins it.

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

The five metric tables exist because the OTLP metric shapes do not share a
column set — a gauge carries one `DOUBLE PRECISION`, a histogram carries bucket
arrays, a summary carries quantile pairs.

Each table is created `WITH (storage = 'search')` and gets one inverted index
covering what dashboards filter, group and facet on. Columns outside the index
are still stored and retrievable; they are read with a columnstore scan.

## Timestamps

Every table carries `"timestamp" TIMESTAMP_NS NOT NULL`, indexed, plus the
signal's own `observed_timestamp`, `end_timestamp` or `start_timestamp`.
Nanosecond precision is preserved on the way in through `INSERT` and through
`COPY ... FROM STDIN` in PG **text** format:

```sql
SELECT epoch_ns("timestamp") FROM otel_logs ORDER BY 1 LIMIT 1;
```

The PG **binary** (extended) protocol truncates `TIMESTAMP_NS` to microseconds
in both directions. Clients that read through pgx or Grafana therefore see
microseconds; `epoch_ns("timestamp")` returns the exact integer when the extra
digits matter.

## Attributes

Attributes are stored in two tiers:

- **Promoted columns** — `service_name`, `severity_text`, `severity_number`,
  `event_name`, `span_name`, `span_kind`, `status_code`, `metric_name`. These
  are what the index covers, so they are what you filter and facet on.
- **JSON blobs** — `resource_attributes`, `scope_attributes`, and one of
  `log_attributes` / `span_attributes` / `attributes`. Lossless, retrievable,
  and filterable with `->>` (a columnstore scan).

A deployment that knows its hot attribute paths can promote them to index speed
with an expression index, added in the same `CREATE INDEX` statement:

```sql
CREATE INDEX otel_logs_idx ON otel_logs USING inverted (
    "timestamp", body otel_body_dict, severity_text, severity_number,
    service_name, event_name, trace_id,
    (log_attributes ->> 'http.status_code')
);
```

That buys a term filter — `(log_attributes ->> 'http.status_code') @@ '502'` —
but not facets: `ts_dict_*` accepts only real indexed columns.

## The body dictionary

`body` is analyzed with `otel_body_dict`, which splits on Unicode word
boundaries and lowercases, with no stemming:

```sql
CREATE TEXT SEARCH DICTIONARY otel_body_dict AS
    split_text(case := 'lower', break := 'alpha')
    WITH (frequency, position, norm);
```

Log bodies are multilingual and full of identifiers, paths, IPs and durations,
where stemming is wrong. Splitting on word boundaries keeps dotted
identifiers and addresses whole:

```sql
SELECT ts_lexize('otel_body_dict', 'ERROR io.grpc.Client 10.0.0.7:5432 took 12ms');
-- {error, io.grpc.client, 10.0.0.7, 5432, took, 12ms}
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

Span events and links are kept in the span row as JSON, with two searchable
side arrays so the common lookups stay index-side:

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

A metric rollup:

```sql
SELECT metric_name, date_trunc('minute', "timestamp") AS minute, sum(value)
FROM otel_metrics_sum
WHERE metric_name @@ 'http.server.request_count'
  AND "timestamp" @@ ts_ge(TIMESTAMP_NS '2026-09-08 00:00:00')
GROUP BY 1, 2 ORDER BY 1, 2;
```

Use `@@` rather than `=` when you want a term lookup. A plain `=` on a
non-primary-key column is a columnstore `Column Filter` — correct, but it reads
the column instead of the term dictionary.

## Visibility

Search tables are commit-time visible: a row becomes visible to *any* query
after the next refresh, governed by `refresh_interval` (1 s by default). Force
it when you need determinism, for example in tests:

```sql
VACUUM (REFRESH_TABLE) otel_logs;
```

## Schema evolution

A search table's schema is fixed. `ALTER TABLE ADD COLUMN`, `DROP COLUMN` and
`ALTER COLUMN TYPE` are rejected, and an inverted index can only be created
while the table is still empty. To change the schema, create a new table with
`CREATE TABLE ... AS SELECT`, switch writers to it, and rename.

Re-running the DDL is safe — every statement is `IF NOT EXISTS`, including on
a table that already holds rows. Startup checks for the schema before running
any of it, so a restart does no work at all.
