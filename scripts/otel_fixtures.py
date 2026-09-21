#!/usr/bin/env python3
"""Encode the conformance fixtures' ProtoJSON into OTLP protobuf.

The `.otlp.json` files are the source of truth; this writes the matching
`.otlp.pb` next to each one using the OFFICIAL opentelemetry-proto bindings,
so the protozero decoder in server/otel/protobuf.cpp is checked against an
independent implementation rather than against itself.

    pip install opentelemetry-proto protobuf
    scripts/otel_fixtures.py generate
    scripts/otel_fixtures.py check     # fail if a .pb is missing or stale
"""

import base64
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONFORMANCE = ROOT / "resources" / "otel" / "conformance"

SIGNALS = {
    "logs": (
        "opentelemetry.proto.collector.logs.v1.logs_service_pb2",
        "ExportLogsServiceRequest",
    ),
    "traces": (
        "opentelemetry.proto.collector.trace.v1.trace_service_pb2",
        "ExportTraceServiceRequest",
    ),
    "metrics": (
        "opentelemetry.proto.collector.metrics.v1.metrics_service_pb2",
        "ExportMetricsServiceRequest",
    ),
}


# OTLP/JSON deviates from standard ProtoJSON: trace and span ids are written
# as hex, not base64. json_format.Parse only knows the ProtoJSON rule, so the
# ids are rewritten before it sees them.
# https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
ID_KEYS = (
    "traceId",
    "spanId",
    "parentSpanId",
    "trace_id",
    "span_id",
    "parent_span_id",
)


def hex_ids_to_base64(node):
    if isinstance(node, dict):
        return {
            key: (
                base64.b64encode(bytes.fromhex(value)).decode()
                if key in ID_KEYS and isinstance(value, str) and value
                else hex_ids_to_base64(value)
            )
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [hex_ids_to_base64(item) for item in node]
    return node


def encode(signal, text):
    import importlib

    from google.protobuf import json_format

    module_name, message_name = SIGNALS[signal]
    module = importlib.import_module(module_name)
    message = getattr(module, message_name)()
    json_format.Parse(
        json.dumps(hex_ids_to_base64(json.loads(text))),
        message,
        # ProtoJSON allows a receiver to skip fields it does not know; our
        # reader does, so the encoder must too or the pair would diverge.
        ignore_unknown_fields=True,
    )
    return message.SerializeToString(deterministic=True)


def each_fixture():
    for signal in SIGNALS:
        for source in sorted((CONFORMANCE / signal).glob("*.otlp.json")):
            yield signal, source, source.with_suffix("").with_suffix(".otlp.pb")


def main(argv):
    if len(argv) != 2 or argv[1] not in ("generate", "check"):
        print(__doc__, file=sys.stderr)
        return 2
    failures = []
    for signal, source, target in each_fixture():
        wire = encode(signal, source.read_text())
        if argv[1] == "generate":
            target.write_bytes(wire)
            print(f"wrote {target.relative_to(ROOT)} ({len(wire)} bytes)")
        elif not target.exists() or target.read_bytes() != wire:
            failures.append(target.relative_to(ROOT))
    for failure in failures:
        print(f"{failure} is missing or stale", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
