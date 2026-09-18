# OpenTelemetry schema and conformance fixtures

`otel_schema.sql` is the **single source of truth** for how OpenTelemetry
telemetry is stored in SereneDB. Everything that writes OTLP data embeds this
file rather than carrying its own copy of the DDL:

| Consumer | How it uses the file |
|---|---|
| `?api=otlp` handlers | embedded at build time, run on first write when `otlp_auto_create_schema` is on |
| `?api=es` handlers (otel data streams) | same embedded copy |
| `serenedbexporter` (Go) | shipped with the exporter, run by `create_schema: true` |
| `tests/sqllogic/sdb/pg/otel/schema.test` | via the generated `schema.inc` |

`schema.inc` is generated — never edit it by hand:

```bash
scripts/otel_schema.py generate   # rewrite the include from otel_schema.sql
scripts/otel_schema.py check      # fail if the include has drifted
```

## References

- OTLP data model and wire formats — https://github.com/open-telemetry/opentelemetry-proto
- OTLP/HTTP spec (endpoints, responses, encodings) — https://opentelemetry.io/docs/specs/otlp/
- Attribute keys promoted to columns are semantic conventions — https://opentelemetry.io/docs/specs/semconv/
- Column naming follows the ClickHouse exporter's schema, the de-facto standard
  for SQL-backed OTel storage — https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter

## Why the schema is shaped this way

- **Search tables.** `WITH (storage = 'search')` makes the table its own
  iresearch store, so `body @@ ...` and BM25 ranking run against the table
  directly. There is no separate row store to keep in sync.
- **Indexes are created while the table is empty.** A search table cannot gain
  an index after the first row, and cannot gain a column at all. Deployments
  that want extra promoted columns or expression indexes over hot attribute
  paths create the tables themselves before first write; the DDL here is the
  default.
- **`TIMESTAMP_NS` everywhere.** Nanosecond precision is preserved through
  `INSERT` and through `COPY ... FROM STDIN` in PG **text** format. The binary
  (extended) protocol truncates to microseconds, so every write path uses text
  or constructs values in-process. `epoch_ns("timestamp")` returns the exact
  integer for clients that need it.
- **Attributes are a JSON blob plus promoted columns.** The engine cannot index
  a JSON column *as a map* yet, so what dashboards filter and facet on is
  promoted to real columns (`service_name`, `severity_text`, `span_kind`, ...)
  and full fidelity is kept in `resource_attributes` / `scope_attributes` /
  `log_attributes` / `span_attributes`.
- **Five metric tables.** The OTLP metric shapes do not share a column set;
  one table each keeps every column meaningful.

## Conformance fixtures

`conformance/` holds the canonical-mapping fixtures: an OTLP request in
ProtoJSON plus the exact rows it must produce. Every ingestion route is tested
against the same files, so a route that maps a field differently fails CI
rather than silently diverging. See `conformance/README.md`.
