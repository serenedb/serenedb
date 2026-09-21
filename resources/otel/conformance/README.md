# Conformance fixtures

OTLP payloads used to check that the two decoders — protobuf and JSON —
produce identical rows.

```
<signal>/<case>.otlp.json       an Export<Signal>ServiceRequest
<signal>/<case>.otlp.pb         the same request, binary
<signal>/<case>.expected.jsonl  the rows it should produce
```

`.otlp.pb` is generated from `.otlp.json` by `scripts/otel_fixtures.py`, using
the official `opentelemetry-proto` python bindings, so the server's decoder is
checked against an independent encoder. `check` fails on drift and runs in the
python driver suite.

`upstream.otlp.json` are vendored from
https://github.com/open-telemetry/opentelemetry-proto/tree/main/examples; the
rest are written here for cases those do not reach.

Consumed by `tests/drivers/python/test_otlp_api.py` and
`tests/sqllogic/sdb/pg/otel/schema.test`. `.expected.jsonl` is not asserted by
anything yet.

## Mapping rules the fixtures pin

| Subject | Rule |
|---|---|
| JSON attribute columns | keys sorted, compact, no whitespace |
| Timestamps in JSON columns | RFC3339, 9 fractional digits, `Z` |
| Integer attribute values | JSON numbers, not strings |
| Byte attribute values | base64 |
| Trace and span ids | lowercase hex; all-zero or empty becomes NULL |
