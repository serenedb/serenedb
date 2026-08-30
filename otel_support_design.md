# OpenTelemetry support for SereneDB: `serenedbexporter`

Design doc, 2026-08-13. All SQL in this document was executed against a local serened built from
`bdd28ac4f` (feat: Add reindex support); the marked EXPLAIN claims were audited against actual
plans. Statements that could not be verified are explicitly flagged.

## 1. Why

We want SereneDB to be a first-class storage backend for OpenTelemetry telemetry, the way
ClickHouse is via the `clickhouseexporter` in otel-collector-contrib. That exporter is the
ingestion layer of ClickStack (ClickHouse's official observability stack post-HyperDX) and the
pattern behind SigNoz, Highlight.io, qryn and BetterStack: a vendor exporter inside the collector
ecosystem is the established distribution channel for DB-backed observability, and it puts the
vendor's name in every user's collector config. The whole receiver/processor ecosystem (every log
source, batching, retry, persistent queues) comes for free.

SereneDB's log pitch is stronger than ClickHouse's: real full-text search (BM25 ranking, phrase
queries, analyzers) plus dictionary-served facets over schemaless attributes, on a PG-compatible
SQL surface. ClickHouse approximates all of that with skip indexes and materialized columns.

**Scope**: logs, traces, metrics (all five OTLP metric shapes). **Out of scope for v1**:
retention/TTL (workaround documented in §7), profiles.

## 2. Approach in one paragraph

A new Go collector exporter, `serenedbexporter`, mirroring `clickhouseexporter` conventions. It
connects over pgwire (pgx), optionally creates the schema, and bulk-loads batches with
`COPY ... FROM STDIN` in PG TEXT format — SereneDB's documented bulk path. Schemas lean on the
inverted search index: BM25 body search, dictionary-served facets over exporter-computed
`'key=value'` tag arrays, and index-served time rollups. Full-fidelity attribute maps ride along
as `JSON` columns `INCLUDE`d in the index. A timeboxed spike (§6) tests the stock
`elasticsearchexporter` against SereneDB's ES-compatible `_bulk` endpoint as a zero-install
fallback story.

## 3. Cross-cutting design decisions

### 3.1 Timestamps: BIGINT nanoseconds + generated µs TIMESTAMP

Two SereneDB facts force the design:

- `TIMESTAMP_NS` is truncated to µs over the binary/extended wire protocol (ns survives only in
  text and on disk).
- `TIMESTAMP_NS` cannot be inverted-indexed at all; `TIMESTAMP` (µs) and `BIGINT` can.

So `TIMESTAMP_NS` is not used anywhere. Every table carries:

```sql
time_unix_nano BIGINT NOT NULL,
"timestamp"    TIMESTAMP GENERATED ALWAYS AS (make_timestamp(time_unix_nano // 1000)) STORED
```

`time_unix_nano` is the source of truth — OTel's `timeUnixNano` verbatim, exact-ns range scans via
the index (verified). The generated `"timestamp"` is µs, human/Grafana-friendly, indexed for range
filters and `date_trunc` rollups. Verified: the generated expression works, COPY with an explicit
column list omitting the generated column works, and direct writes to it are rejected
(`Cannot insert a non-DEFAULT value into generated column`). Convention: naive TIMESTAMP, values
are UTC (matches the ClickHouse exporter's DateTime64 behavior; TIMESTAMPTZ is open question §8.5).

### 3.2 Attributes: three tiers

OTel attribute maps are open-schema with string/int/double/bool/bytes/array/map values. SereneDB
constraints: `MAP`, `STRUCT` and whole-`VARIANT` columns cannot be indexed; `VARCHAR[]` indexes
element-wise; the `'key=value'` `VARCHAR[]` + keyword-dictionary facet pattern is first-class and
tested (`tests/sqllogic/sdb/pg/index/ts_dict_list.test`).

| Tier | Storage | Role |
|---|---|---|
| Promoted typed columns | `service_name`, `severity_text`, `severity_number`, `span_name`, `status_code`, `metric_name`, ... | What every query touches. Keyword-indexed; `GROUP BY` on them is dictionary-served (verified via EXPLAIN: `IRESEARCH_SCAN / TsDict`) |
| Facet tag arrays | `resource_tags VARCHAR[]`, `log_tags` / `span_tags` / `attr_tags VARCHAR[]` of `'key=value'` strings, computed by the exporter in Go | Schemaless filtering (`tags @@ 'k=v'`, `ts_any`/`ts_all`), facet sidebars (`ts_dict_agg`/`ts_dict_count`), key discovery (`LIKE 'http.%'`) |
| Full-fidelity blobs | `resource_attributes JSON`, `scope_attributes JSON`, `log_attributes JSON`, ... | Lossless retrieval, `INCLUDE`d in the index; ad-hoc per-path indexing later via `(attrs->>'k') dict` |

**Why `JSON`, not `VARIANT`, for tier 3.** The docs recommend `VARIANT` for query-side
performance, but no ingest path produces a *structured* VARIANT today: binary COPY is rejected for
VARIANT, text input stores the JSON as a single string scalar, and `COPY (FORMAT json)` into a
VARIANT column fails with `Cannot read a value of type VARIANT from a json file` (verified).
Structured VARIANT requires an explicit `::json::variant` cast in SQL. `JSON` columns COPY in as
plain text, round-trip losslessly, `INCLUDE` fine, and `->>` extractions on them are indexable.
Switching tier 3 to VARIANT is a follow-up gated on the core gap in §7.3.

**Why the exporter computes tags in Go rather than a generated column.** A generated `VARCHAR[]`
over a JSON column would need `json_keys` plus lambdas whose acceptance in generated columns is
untested, and the exporter already holds typed `pcommon.Map` values, so one Go function applies an
exact, documented rule.

**The `k=v` stringification rule** (one Go function, documented in the exporter README):

- Tag = `key + "=" + repr(value)`. `repr`: strings verbatim; ints `strconv.FormatInt`; doubles
  `strconv.FormatFloat(v, 'g', -1, 64)`; bools `true`/`false`; bytes base64.
- Array / map / nested values are not emitted as tags (they stay queryable in the JSON column) —
  unbounded terms pollute the dictionary and `k=[...]` facets are meaningless.
- Values longer than `tags_value_limit` (default 256 bytes) are dropped from tags, not truncated —
  a truncated tag is a wrong facet. The value is still in the JSON column.
- Keys containing `=` are allowed; prefix restriction still works on the full key, but collisions
  (key `a` value `b=c` vs key `a=b` value `c`) are theoretically ambiguous — documented, not
  prevented, matching the pattern's contract.
- Placement: resource attributes → `resource_tags` (low per-row cardinality, high duplication —
  `k8s.namespace`-style facets); record attributes → `log_tags`/`span_tags`/`attr_tags`; scope
  attributes folded into the record tag column with a `scope.` key prefix (rare enough that a
  third indexed column isn't worth the write amplification; full fidelity stays in
  `scope_attributes`).

### 3.3 IDs

`trace_id`/`span_id` as lowercase-hex `VARCHAR` (32/16 chars): `UUID` is not indexable, and trace
IDs aren't UUIDs anyway. All-zero/empty IDs map to SQL `NULL`, so `trace_id IS NOT NULL` stays a
posting-list read. `trace_id` is indexed verbatim (no dictionary = exact, case-sensitive,
single-token match — verified); on logs, `span_id` is `INCLUDE`-only; on traces it's indexed.

### 3.4 Ingest: COPY FROM STDIN, PG TEXT format

The exporter writes with `pgconn.CopyFrom(ctx, reader, "COPY tbl (cols...) FROM STDIN")`,
streaming PG TEXT that it encodes itself (tab-separated, `\N` nulls, backslash escapes, `{...}`
arrays — the `pg_dump` format, ~200 lines of Go). Explicit column list omits the generated
`"timestamp"` (verified working).

Rejected alternatives: pgx's high-level `CopyFrom` uses PG BINARY, which JSON columns can't decode
and which carries the µs-truncation semantics; multi-row INSERT is explicitly discouraged by the
SereneDB bulk-load docs in favor of COPY. Kept in the back pocket:
`COPY ... FROM STDIN (FORMAT json)` (NDJSON) — simpler escaping and a possible VARIANT upgrade
path once §7.3 lands.

One COPY per table per collector batch = one atomic unit for retry purposes.

### 3.5 Search visibility

Inverted indexes refresh in the background (`refresh_interval`, default 1000 ms). The exporter
does nothing about this — search results lag ingest by up to ~1 s (base-table scans see rows
immediately). Documented as the visibility SLA; a `VACUUM (REFRESH_TABLE)` per flush would thrash
and is offered only as an off-by-default demo/test option (§8.10).

## 4. Schemas

All DDL below executed successfully against `bdd28ac4f`. `IF NOT EXISTS` is supported on
dictionaries, tables and indexes (verified), so `create_schema` is idempotent without error-code
tolerance.

### 4.1 Shared dictionaries

```sql
CREATE TEXT SEARCH DICTIONARY IF NOT EXISTS otel_body_dict (
    template  = 'text',
    locale    = 'en_US.UTF-8',
    case      = 'lower',
    stemming  = true,
    frequency = true,     -- scoring
    position  = true,     -- ts_phrase
    norm      = true      -- BM25 length norm + WAND
);

CREATE TEXT SEARCH DICTIONARY IF NOT EXISTS otel_kw_dict (
    template  = 'keyword',
    frequency = true      -- enables ts_dict_freq alongside ts_dict_count
);
```

### 4.2 `otel_logs`

```sql
CREATE TABLE IF NOT EXISTS otel_logs (
    time_unix_nano          BIGINT NOT NULL,
    observed_time_unix_nano BIGINT,
    "timestamp"             TIMESTAMP GENERATED ALWAYS AS
                              (make_timestamp(time_unix_nano // 1000)) STORED,
    trace_id            VARCHAR,        -- 32-char lowercase hex, NULL if unset
    span_id             VARCHAR,        -- 16-char lowercase hex, NULL if unset
    trace_flags         INTEGER,
    severity_text       VARCHAR,
    severity_number     SMALLINT,
    service_name        VARCHAR,        -- promoted resource["service.name"]
    event_name          VARCHAR,
    body                VARCHAR,        -- AnyValue; maps/arrays stringified to JSON text
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    log_attributes      JSON,
    resource_tags       VARCHAR[],      -- exporter-computed 'k=v'
    log_tags            VARCHAR[]       -- record attrs + scope attrs as 'scope.k=v'
);

CREATE INDEX IF NOT EXISTS otel_logs_idx ON otel_logs USING inverted (
    "timestamp",                        -- µs range filters, date_trunc rollups
    time_unix_nano,                     -- exact-ns ranges / ordering
    body            otel_body_dict,
    severity_text   otel_kw_dict,
    severity_number,                    -- numeric range (>= ERROR etc.)
    service_name    otel_kw_dict,
    event_name      otel_kw_dict,
    trace_id,                           -- verbatim exact-match lookup
    resource_tags   otel_kw_dict,
    log_tags        otel_kw_dict
)
INCLUDE (
    observed_time_unix_nano,
    span_id,
    trace_flags,
    scope_name,
    scope_version,
    resource_schema_url included (compression = 'dict_fsst'),
    scope_schema_url    included (compression = 'dict_fsst'),
    resource_attributes included (compression = 'zstd'),
    scope_attributes    included (compression = 'zstd'),
    log_attributes      included (compression = 'zstd')
)
WITH (optimize_top_k = 'bm25(1.2, 0.75)');
```

Rationale:

- Everything users filter or facet on is indexed; everything they only render is `INCLUDE`d so hit
  rendering doesn't touch the base table. Schema URLs are near-constant strings → `dict_fsst`;
  JSON blobs → `zstd`. (Note: the compression option value is `dict_fsst`, not `fsst` — the
  accepted set is `auto, uncompressed, rle, bitpacking, zstd, alp, alprd, roaring, dict_fsst`.)
- `optimize_top_k = 'bm25(1.2, 0.75)'` enables WAND pruning for the search-box query shape;
  verified in EXPLAIN as `Score: bm25(k1=1.2, b=0.75) ... Top: 20, optimized`.
- No PRIMARY KEY: OTel logs have no natural key; tables without a PK get generated row identity.
- Optional (documented, not default): a hot errors-only partial index —

```sql
CREATE INDEX otel_logs_errors_idx ON otel_logs
  USING inverted (body otel_body_dict, service_name otel_kw_dict)
  WHERE severity_number >= 17;    -- ERROR and above
```

### 4.3 `otel_traces`

Span events and links are lists of structs, which cannot be indexed (`STRUCT` and nested lists are
rejected). Child tables were rejected too: every trace-detail render becomes a join, ingest
becomes three COPYs without cross-table atomicity, and retention DELETEs triple. Chosen shape:
`JSON` columns for fidelity plus exporter-computed searchable side arrays — `event_names`
answers "spans with an `exception` event", `link_trace_ids` answers reverse link lookups
(both verified).

```sql
CREATE TABLE IF NOT EXISTS otel_traces (
    time_unix_nano      BIGINT NOT NULL,          -- span start
    "timestamp"         TIMESTAMP GENERATED ALWAYS AS
                          (make_timestamp(time_unix_nano // 1000)) STORED,
    trace_id            VARCHAR NOT NULL,
    span_id             VARCHAR NOT NULL,
    parent_span_id      VARCHAR,
    trace_state         VARCHAR,
    span_name           VARCHAR,
    span_kind           VARCHAR,                  -- 'Server','Client',... (CH-style strings)
    service_name        VARCHAR,
    duration_ns         BIGINT,                   -- end - start
    status_code         VARCHAR,                  -- 'Unset','Ok','Error'
    status_message      VARCHAR,
    resource_schema_url VARCHAR,
    scope_schema_url    VARCHAR,
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    resource_attributes JSON,
    scope_attributes    JSON,
    span_attributes     JSON,
    events              JSON,   -- [{"time_unix_nano":..,"name":..,"attributes":{..}},..]
    links               JSON,   -- [{"trace_id":..,"span_id":..,"trace_state":..,"attributes":{..}},..]
    resource_tags       VARCHAR[],
    span_tags           VARCHAR[],
    event_names         VARCHAR[],
    link_trace_ids      VARCHAR[]
);

CREATE INDEX IF NOT EXISTS otel_traces_idx ON otel_traces USING inverted (
    "timestamp",
    time_unix_nano,
    trace_id,
    span_id,
    span_name       otel_kw_dict,
    span_kind       otel_kw_dict,
    service_name    otel_kw_dict,
    status_code     otel_kw_dict,
    duration_ns,                        -- slow-span ranges, index-served min/max
    resource_tags   otel_kw_dict,
    span_tags       otel_kw_dict,
    event_names     otel_kw_dict,
    link_trace_ids
)
INCLUDE (
    parent_span_id,
    trace_state,
    status_message      included (compression = 'zstd'),
    scope_name,
    scope_version,
    resource_schema_url included (compression = 'dict_fsst'),
    scope_schema_url    included (compression = 'dict_fsst'),
    resource_attributes included (compression = 'zstd'),
    scope_attributes    included (compression = 'zstd'),
    span_attributes     included (compression = 'zstd'),
    events              included (compression = 'zstd'),
    links               included (compression = 'zstd')
);
```

### 4.4 Metrics: five plain tables

Metrics are analytics-only — no full text, extreme row rates, dashboards filter on
`(metric_name, service, a few attrs, time)`. **Default: no inverted index.** Index write
amplification at metrics volume buys little; scans handle the aggregation shape. The exporter
still writes `attr_tags`, so an optional facet index for metric/label discovery UIs can be enabled
(config `metrics_indexing: none | facets`, default `none`):

```sql
-- optional, per metrics table T:
CREATE INDEX T_idx ON T USING inverted (
    "timestamp", metric_name otel_kw_dict, service_name otel_kw_dict, attr_tags otel_kw_dict
) INCLUDE (value);
```

Common block shared by all five tables (ClickHouse exporter naming, snake_cased; unsigned OTel
fields map to signed `BIGINT` — unsigned ints aren't indexable and PG has no unsigned wire types):

```sql
    resource_attributes JSON,
    resource_schema_url VARCHAR,
    resource_tags       VARCHAR[],
    scope_name          VARCHAR,
    scope_version       VARCHAR,
    scope_attributes    JSON,
    scope_schema_url    VARCHAR,
    service_name        VARCHAR,
    metric_name         VARCHAR NOT NULL,
    metric_description  VARCHAR,
    metric_unit         VARCHAR,
    attributes          JSON,           -- datapoint attributes
    attr_tags           VARCHAR[],
    start_time_unix_nano BIGINT,
    time_unix_nano      BIGINT NOT NULL,
    "timestamp"         TIMESTAMP GENERATED ALWAYS AS
                          (make_timestamp(time_unix_nano // 1000)) STORED,
    flags               INTEGER
```

Per-shape columns:

```sql
CREATE TABLE otel_metrics_gauge (
    <common>,
    value      DOUBLE PRECISION,
    exemplars  JSON   -- [{"time_unix_nano":..,"value":..,"span_id":..,"trace_id":..,"filtered_attributes":{..}}]
);

CREATE TABLE otel_metrics_sum (
    <common>,
    value                   DOUBLE PRECISION,
    aggregation_temporality VARCHAR,      -- 'Delta' | 'Cumulative' | 'Unspecified'
    is_monotonic            BOOLEAN,
    exemplars               JSON
);

CREATE TABLE otel_metrics_histogram (
    <common>,
    count           BIGINT,
    sum             DOUBLE PRECISION,
    bucket_counts   BIGINT[],
    explicit_bounds DOUBLE PRECISION[],
    min             DOUBLE PRECISION,
    max             DOUBLE PRECISION,
    aggregation_temporality VARCHAR,
    exemplars       JSON
);

CREATE TABLE otel_metrics_exponential_histogram (
    <common>,
    count            BIGINT,
    sum              DOUBLE PRECISION,
    scale            INTEGER,
    zero_count       BIGINT,
    positive_offset  INTEGER,
    positive_bucket_counts BIGINT[],
    negative_offset  INTEGER,
    negative_bucket_counts BIGINT[],
    min              DOUBLE PRECISION,
    max              DOUBLE PRECISION,
    aggregation_temporality VARCHAR,
    exemplars        JSON
);

CREATE TABLE otel_metrics_summary (
    <common>,
    count     BIGINT,
    sum       DOUBLE PRECISION,
    quantiles DOUBLE PRECISION[],   -- positionally aligned pair
    values    DOUBLE PRECISION[]
);
```

## 5. How you query it

All queries below ran against sample data on `bdd28ac4f` and returned correct results; the facet
queries were EXPLAIN-audited to confirm they are dictionary-served (`IRESEARCH_SCAN` with
`TsDict:`), and the ranked search shows the full predicate lowered into the index with WAND top-k.

```sql
-- Needle in the haystack: ranked body search in a time window
SELECT "timestamp", service_name, severity_text, body,
       BM25(otel_logs_idx.tableoid) AS score
FROM otel_logs_idx
WHERE body @@ websearch_to_tsquery('"connection reset" -healthcheck')
  AND "timestamp" @@ ts_between(TIMESTAMP '2026-08-12 00:00:00',
                                TIMESTAMP '2026-08-13 00:00:00', true, false)
ORDER BY score DESC LIMIT 20;

-- Facet sidebar over unknown attribute keys, restricted to the current search
SELECT t AS term, c AS docs
FROM (SELECT unnest(ts_dict_agg(log_tags)) AS t,
             unnest(ts_dict_count(log_tags)) AS c
      FROM otel_logs_idx
      WHERE body @@ 'timeout') sub
ORDER BY docs DESC LIMIT 50;

-- Values of one attribute key only (prefix-restricted)
SELECT t, c
FROM (SELECT unnest(ts_dict_agg(log_tags)) t, unnest(ts_dict_count(log_tags)) c
      FROM otel_logs_idx) sub
WHERE t LIKE 'http.status_code=%' ORDER BY c DESC;

-- Trace lookup by id, and its correlated logs
SELECT * FROM otel_traces_idx
WHERE trace_id @@ '0af7651916cd43dd8448eb211c80319c' ORDER BY time_unix_nano;
SELECT "timestamp", severity_text, body FROM otel_logs_idx
WHERE trace_id @@ '0af7651916cd43dd8448eb211c80319c' ORDER BY time_unix_nano;

-- Service latency percentiles
SELECT service_name, quantile_cont(duration_ns, [0.5, 0.95, 0.99]) AS p
FROM otel_traces_idx
WHERE "timestamp" @@ ts_ge(TIMESTAMP '2026-08-12 00:00:00')
  AND span_kind @@ 'Server'
GROUP BY service_name;

-- Error rate over time
SELECT date_trunc('minute', "timestamp") AS minute,
       count(*) FILTER (WHERE status_code = 'Error')::DOUBLE / count(*) AS error_rate
FROM otel_traces_idx
WHERE "timestamp" @@ ts_ge(TIMESTAMP '2026-08-12 00:00:00')
GROUP BY 1 ORDER BY 1;

-- Spans with an exception event; reverse link lookup
SELECT trace_id, span_id, span_name FROM otel_traces_idx WHERE event_names @@ 'exception';
SELECT trace_id, span_id FROM otel_traces_idx WHERE link_trace_ids @@ '1bc2d3e4...';

-- Attribute retrieval from the INCLUDEd JSON
SELECT log_attributes ->> 'http.method' AS method, body
FROM otel_logs_idx WHERE severity_number @@ ts_ge(17);

-- Severity facet via the implicit dictionary rewrite (plain SQL, no ts_* needed)
SELECT severity_text, count(*) FROM otel_logs_idx GROUP BY severity_text;
```

## 6. Exporter architecture

### 6.1 Repo layout

Standalone repo `serenedb/opentelemetry-collector-serenedb` (contrib upstreaming is slow and
couples releases to the collector train; an OCB manifest + published image gives a one-line
install now; the module can be proposed to contrib later unchanged):

```
exporter/serenedbexporter/
    config.go                     # Config + Validate()
    factory.go                    # NewFactory, per-signal exporters
    exporter_logs.go
    exporter_traces.go
    exporter_metrics.go           # dispatches to the 5 shape writers
    internal/
        ddl/                      # embedded CREATE DICTIONARY/TABLE/INDEX templates
        pgtext/                   # PG TEXT COPY encoder (rows -> stream)
        tags/                     # k=v stringification (§3.2)
        sqlstate/                 # retryable vs permanent classification
distributions/serenedb-otel-collector/
    manifest.yaml                 # OCB manifest: otlp receiver, batch, this exporter
    Dockerfile
tests/integration/                # docker-compose: serened + collector + telemetrygen
```

### 6.2 Config (clickhouseexporter option names where semantics match)

```go
type Config struct {
    Endpoint         string   // DSN: postgres://user:pass@host:5432/otel?sslmode=...
    Database         string   // optional override of the DSN database
    Schema           string   // default "public"
    LogsTableName    string   // default "otel_logs"    (index: <table>_idx)
    TracesTableName  string   // default "otel_traces"
    MetricsTableName string   // default "otel_metrics" (suffixes _gauge/_sum/...)
    CreateSchema     bool     // default true
    MetricsIndexing  string   // "none" (default) | "facets"
    TagsValueLimit   int      // default 256 (§3.2)
    TimeoutSettings, QueueBatchConfig, BackOffConfig   // standard collector plumbing
}
```

`ttl` (the ClickHouse option) is deliberately absent — the README points at the retention runbook
(§7.1). Batch guidance copied from ClickHouse: `sending_queue::batch` with min ~5 000 rows.

### 6.3 Data flow

1. `Consume{Logs,Traces,Metrics}` flattens pdata into per-table row slices (metrics: one slice per
   shape; empty tables skipped).
2. `internal/tags` computes tag arrays; `encoding/json` marshals attribute maps (AnyValue → JSON
   with the same repr rules as §3.2).
3. `internal/pgtext` streams rows into
   `pgconn.CopyFrom(ctx, r, "COPY schema.tbl (cols...) FROM STDIN")` — one COPY per table per
   batch; COPY is atomic on its own, so no explicit transaction is needed.
4. Connections: `pgxpool` sized to `sending_queue::num_consumers`.

### 6.4 Schema creation and drift

- `create_schema: true`: on start, run the embedded DDL (dictionaries → tables → indexes), all
  with `IF NOT EXISTS` (verified supported for all three object kinds).
- Column introspection at startup, like clickhouseexporter: `SELECT * FROM <table> LIMIT 0` per
  table, build the column set from the RowDescription, and drive the COPY column list from the
  intersection of expected vs actual — warn on missing expected columns, ignore user-added extras.
  Users can safely customize (extra promoted columns, different dictionaries).
- A `serenedb_otel_schema_version(version INTEGER, applied_at TIMESTAMP)` table records the DDL
  version. The exporter never ALTERs; on version mismatch it logs an error pointing at
  release-notes migration SQL (same stance as clickhouseexporter).

### 6.5 Errors and retries

COPY fails as a whole batch; classify by SQLSTATE: `08***` (connection), `53***` (resources),
`57P0x` (shutdown), `40001`/`40P01` → retryable (transient consumer error → sending_queue backoff,
persistent-queue safe); `22***` (data), `42***` (schema), `23***` (constraint) → permanent (drop
and log). A retry after a mid-stream connection loss can duplicate rows (no PK, no dedup) — same
at-least-once semantics as clickhouseexporter; documented.

## 7. Gaps, workarounds, core follow-ups

1. **Retention/TTL — no native support.** Runbook: a scheduled job runs
   `DELETE FROM otel_logs WHERE time_unix_nano < <cutoff_ns>` (verified shape) followed by
   `VACUUM` / `compact_index` so segment merges reclaim deleted postings, plus a periodic
   `CHECKPOINT`. Deletes mask rows in index results immediately. Core follow-up: native TTL.
2. **No table partitioning / sort keys.** Retention DELETE is O(data) and time locality relies on
   insertion order. Mitigations: partial indexes for hot windows (static predicates — must be
   rotated); table-per-window rotation behind a view is limited because view indexes are static
   snapshots. Core follow-up: range partitioning or cheap time-range truncation.
3. **No structured-VARIANT ingest path** (verified: binary rejected, text stores a string scalar,
   `COPY (FORMAT json)` errors). Blocks the preferred VARIANT attribute storage. Core follow-up:
   make text/NDJSON input parse JSON into shredded VARIANT.
4. **TIMESTAMP_NS**: not indexable, µs-truncated on the binary wire. Worked around (§3.1). Core
   follow-ups: index support, ns wire precision.
5. **Not indexable and shaping this design**: UUID, unsigned ints, DECIMAL, MAP, STRUCT — hence
   hex VARCHAR IDs and signed BIGINT counts. No action needed; documented.
6. **ES `_bulk` gaps** (see §8/spike): no index auto-create on bulk, no data streams / index
   templates / ILM. File issues if the spike confirms they block the elasticsearchexporter.
7. **Monitoring**: point users at the per-index stats (`num_docs`, consolidation metrics) and
   `sdb_log`; the exporter emits standard collector telemetry.

## Appendix A. Spike: stock `elasticsearchexporter` against `?api=es`

**Timebox: 3 days.** Goal: can the unmodified contrib elasticsearchexporter land logs in SereneDB
today, as a zero-install fallback? Produce a pass/fail matrix and file server issues.

Already in place server-side: `X-Elastic-Product: Elasticsearch` header and a `GET /` handshake
reporting v8.11.0 (the go-elasticsearch product check should pass); `_bulk` with `index`/`create`
actions and per-item error envelopes; `PUT /{index}` with mappings; `_search`, `_refresh`,
`_count`, `_cat/*`, `_cluster/health`. Body cap 64 MiB (default docappender flush ~5 MB — fine).

Known gaps going in:

1. No index auto-creation on bulk (`es_bulk` raises UNDEFINED_TABLE) — the exporter assumes ES
   auto-creates; the spike must pre-create indices.
2. No data streams / index templates / ILM endpoints — the exporter's default `otel` mapping mode
   (writes to `logs-*-*` data streams) will likely fail; test `raw`, `bodymap`, `ecs` modes with a
   pinned `logs_index`.
3. Mapping type allow-list is `keyword, text, long, integer, double, float, boolean, date` — no
   `object`/`flattened`/`nested`/`date_nanos`; unmapped fields survive only in `_source`.

Matrix (local serened with an `?api=es` listener, `telemetrygen logs` through the collector):
startup handshake; pinned index + `raw` mapping with a minimal pre-created mapping; `bodymap` and
`ecs` modes; failure modes (missing index, >64 MiB batch, concurrent flushes). Expected verdict to
validate: works only in pinned-index flat-mapping configurations — acceptable as a demo/fallback,
not the recommended path.

## Appendix B. Open questions for review

1. Promoted-column set: fixed (`service_name`, `event_name`, ...) or configurable at
   DDL-generation time (config → generated DDL + COPY list)?
2. Split `resource_tags`/`log_tags` (proposed; halves dictionary sizes, matches OTel semantics) vs
   one combined tag column (single facet call; UIs can UNION the split)?
3. Should metrics get the facet index by default? Needs an ingest-cost benchmark at realistic
   metric rates.
4. `span_kind`/`status_code`/`aggregation_temporality` as strings (proposed, ClickHouse-compatible,
   self-describing) vs SMALLINT enums (smaller)?
5. Naive-UTC `TIMESTAMP` (proposed) vs `TIMESTAMPTZ` for the generated column — any known
   Grafana/pg-client pitfalls?
6. Structured (map/array) log bodies: single stringified `body VARCHAR` (proposed,
   ClickHouse-compatible) vs an extra `body_json JSON` populated only for non-string bodies?
7. Is `WITH (storage = 'search')` (seen in tests, undocumented) recommended for these append-heavy
   indexed tables?
8. `otel_logs_idx` etc. are user-visible query surface (`FROM otel_logs_idx`) — bless the naming in
   docs?
9. Should the exporter offer an off-by-default `refresh_on_flush` (issues
   `VACUUM (REFRESH_TABLE)`) for demo/test determinism?

## Appendix C. Verification log

Executed on 2026-08-13 against serened built from `bdd28ac4f`, fresh datadir:

- All DDL in §4 (2 dictionaries, 8 tables, 3 indexes incl. the partial errors index): pass. One
  correction found: the INCLUDE compression option value `fsst` does not exist; use `dict_fsst`
  (schema URLs) / `zstd` (JSON blobs).
- Generated column `make_timestamp(time_unix_nano // 1000)`: pass; ns retained in BIGINT
  (`...789` sub-µs digits), µs in the TIMESTAMP; direct writes rejected.
- `COPY ... FROM STDIN` (PG TEXT via psql `\copy`) with an explicit column list omitting the
  generated column: pass, rows searchable after refresh.
- All §5 queries: pass with correct results. EXPLAIN audits: facet queries are dictionary-served
  (`IRESEARCH_SCAN` / `TsDict:`); ranked search lowers phrase + range + exclusion into the index
  with `Score: bm25(k1=1.2, b=0.75)` and `Top: 20, optimized`.
- `IF NOT EXISTS`: works for dictionaries, tables and indexes.
- `quantile_cont(col, [0.5, 0.95, 0.99])` list form: pass.
- `COPY (FORMAT json)` into VARIANT: fails (`Cannot read a value of type VARIANT from a json
  file`) — confirms gap §7.3.
