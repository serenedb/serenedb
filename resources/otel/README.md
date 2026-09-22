# OpenTelemetry schema

`otel_schema.sql` is the single source of truth for the OTel tables. Nothing
keeps a second copy of the DDL; the two forms other tools need are generated:

| Artifact | Made by | Used by |
|---|---|---|
| `otel/schema_sql.h` | `scripts/otel/schema.py embed`, at build time | the server, to create the schema at startup |
| `tests/sqllogic/sdb/pg/otel/schema.inc` | `scripts/otel/schema.py generate`, by hand | the sqllogic contract test |

`scripts/otel/schema.py check` fails on drift and runs in CI.

## Tables

`otel_logs`, `otel_traces`, and one per metric shape: `otel_metrics_gauge`,
`_sum`, `_histogram`, `_exponential_histogram`, `_summary`. All are search
tables; each carries `"timestamp" TIMESTAMP_NS` plus an inverted index over
what dashboards filter on.

Attributes are promoted columns (`service_name`, `severity_text`, `span_kind`,
...) alongside full-fidelity JSON (`resource_attributes`, `scope_attributes`,
`log_attributes` / `span_attributes` / `attributes`).

## Ingestion

OTLP/HTTP on a listener with `?api=otel`: `POST /v1/logs`, `/v1/traces`,
`/v1/metrics`, in either `application/x-protobuf` or `application/json`.

Both decoders fill the model in `server/otel/model.h`: `protobuf.cpp` reads
the wire format with `third_party/protozero`, `protojson.cpp` reads ProtoJSON
through the reflective serializer (`iresearch/utils/serializer.hpp` over
simdjson), with overloads only for the ProtoJSON deviations.

## Fixtures

`conformance/` holds OTLP payloads paired across both encodings, used to check
the decoders agree. See `conformance/README.md`.

## References

- OTLP data model and wire formats — https://github.com/open-telemetry/opentelemetry-proto
- OTLP/HTTP spec — https://opentelemetry.io/docs/specs/otlp/
- Promoted attribute keys are semantic conventions — https://opentelemetry.io/docs/specs/semconv/
- Column naming follows https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter
