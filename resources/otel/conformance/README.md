# Canonical-mapping conformance fixtures

Three implementations map OTLP to rows — the `?api=otlp` handlers, the `?api=es`
otel-document translator, and the Go `serenedbexporter`. They must produce
**identical rows for identical input**, or a user switching routes sees their
saved queries change behaviour. These fixtures are the shared ground truth.

## Layout

```
<signal>/<case>.otlp.json       one Export<Signal>ServiceRequest, ProtoJSON
<signal>/<case>.expected.jsonl  the rows it must produce, one JSON object per line
```

`<case>.otlp.json` is exactly what an SDK or collector would POST to
`/v1/logs`, `/v1/traces` or `/v1/metrics`.

Each line of `<case>.expected.jsonl` is one row. `__table` names the target
table; every other key is a column name. **A column that is absent from the
object must be NULL in the row** — that keeps the files readable. Row order is
significant and must match the mapper's emission order.

## Normalization rules

These exist so the comparison can be a byte comparison. Every implementation
must follow them, not just the test harness.

| Subject | Rule |
|---|---|
| JSON attribute columns | object keys sorted, compact separators, no whitespace |
| Timestamps | RFC3339 with exactly 9 fractional digits and a `Z` suffix |
| Integer attribute values | JSON numbers, not strings |
| Byte attribute values | base64, standard alphabet with padding |
| Doubles | shortest representation that round-trips |
| Trace and span ids | lowercase hex; all-zero or empty becomes `null` |

## Consumers

| Route | Harness |
|---|---|
| `?api=otlp` | posts each `.otlp.json`, reads the rows back, diffs |
| `?api=es` | replays `_bulk` bodies recorded from the real `elasticsearchexporter` for the same input, diffs |
| `serenedbexporter` | maps the fixture and diffs the encoded `COPY` text rows |
| mapper unit test | `tests/server/otel_mapper_test.cpp`, in process, no server |

A new fixture must pass in all of them.

## Adding a case

1. Write the `.otlp.json`, or capture one with `telemetrygen`.
2. Run the mapper unit test with `--update` to write the `.expected.jsonl`.
3. Read the generated file in the diff. **That review is the spec review** —
   the expected file is the specification, not a recording of whatever the
   first implementation happened to do.
