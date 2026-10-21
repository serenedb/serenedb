#!/usr/bin/env python3
"""Turns a slice of the public OpenTelemetry demo corpus into OTLP requests.

The corpus is parquet in the ClickHouse exporter schema. A slice of about
--size bytes of protobuf is fetched (only the row groups it needs), sorted by
time and cut into requests of --batch records, each written as protobuf and
as JSON next to the offsets of its timestamps, so run.py can move the data to
the present without re-encoding it.

    scripts/perf/otel_bench/prepare.py --signal logs --size 1g

Needs pyarrow, protobuf and opentelemetry-proto.
"""

import argparse
import array
import concurrent.futures
import json
import math
import os
import pathlib
import re
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs
import pyarrow.parquet as pq
from google.protobuf import json_format

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "otel"))
import fixtures

BUCKET = "public-pme"
REGION = "eu-west-3"
DATASET = "otel_demo"
UNITS = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}

SIGNALS = ("logs", "traces", "metrics")
METRIC_KINDS = {
    "otel_metrics_gauge": "gauge",
    "otel_metrics_sum": "sum",
    "otel_metrics_histogram": "histogram",
}
TABLES = {
    "logs": ["otel_logs"],
    "traces": ["otel_traces"],
    "metrics": list(METRIC_KINDS),
}
TIME_COLUMN = {"otel_logs": "Timestamp", "otel_traces": "Timestamp"}

SPAN_KINDS = {"unspecified": 0, "internal": 1, "server": 2, "client": 3,
              "producer": 4, "consumer": 5}
STATUS_CODES = {"unset": 0, "ok": 1, "error": 2}

TS = object()
EXEMPLAR = {2: TS}
NUMBER_POINT = {2: TS, 3: TS, 5: EXEMPLAR}
METRIC = {
    5: {1: NUMBER_POINT},
    7: {1: NUMBER_POINT},
    9: {1: {2: TS, 3: TS, 8: EXEMPLAR}},
    10: {1: {2: TS, 3: TS, 11: EXEMPLAR}},
    11: {1: {2: TS, 3: TS}},
}
LAYOUT = {
    "logs": {1: {2: {2: {1: TS, 11: TS}}}},
    "traces": {1: {2: {2: {7: TS, 8: TS, 11: {1: TS}}}}},
    "metrics": {1: {2: {2: METRIC}}},
}
JSON_TIME = re.compile(
    rb'"(?:time|startTime|endTime|observedTime)UnixNano":"(\d{19})"')


def parse_size(text):
    text = text.strip().lower().rstrip("b")
    return int(text[:-1]) * UNITS[text[-1]] if text[-1] in UNITS else int(text)


def time_column(table):
    return TIME_COLUMN.get(table, "TimeUnix")


def s3():
    return pyarrow.fs.S3FileSystem(anonymous=True, region=REGION)


def int_timestamps(schema):
    fields = []
    for field in schema:
        kind = field.type
        if pa.types.is_timestamp(kind):
            kind = pa.int64()
        elif pa.types.is_list(kind) and pa.types.is_timestamp(kind.value_type):
            kind = pa.list_(pa.field(kind.value_field.name, pa.int64(),
                                     kind.value_field.nullable))
        fields.append(pa.field(field.name, kind, field.nullable))
    return pa.schema(fields)


class Source:
    def __init__(self, data_dir, table):
        self.dir = data_dir / "parquet" / DATASET / table
        self.remote = f"{BUCKET}/{DATASET}/{table}.parquet"
        self.file = None
        meta = self.dir / "row_groups.json"
        if meta.exists():
            self.row_groups = json.loads(meta.read_text())
        else:
            metadata = self.parquet().metadata
            self.row_groups = [metadata.row_group(i).num_rows
                               for i in range(metadata.num_row_groups)]
            self.dir.mkdir(parents=True, exist_ok=True)
            meta.write_text(json.dumps(self.row_groups))
        self.rows = sum(self.row_groups)

    def parquet(self):
        if self.file is None:
            self.file = pq.ParquetFile(self.remote, filesystem=s3())
        return self.file

    def group(self, index):
        path = self.dir / f"rg_{index:05d}.parquet"
        if not path.exists():
            print(f"  fetching {self.remote} row group {index}", file=sys.stderr)
            table = self.parquet().read_row_group(index)
            table = table.cast(int_timestamps(table.schema))
            partial = path.with_suffix(".partial")
            pq.write_table(table, partial)
            partial.rename(path)
        return pq.read_table(path)

    def head(self, rows):
        if rows > self.rows:
            sys.exit(f"{self.remote} has only {self.rows} rows, {rows} wanted")
        tables, have = [], 0
        for index in range(len(self.row_groups)):
            if have >= rows:
                break
            table = self.group(index).slice(0, rows - have)
            tables.append(table)
            have += table.num_rows
        return pa.concat_tables(tables)


def attributes(pairs):
    return [{"key": key, "value": {"stringValue": value}} for key, value in pairs]


def double(value):
    if math.isfinite(value):
        return value
    return "NaN" if math.isnan(value) else ("Infinity" if value > 0 else "-Infinity")


def scope_object(name, version, attrs, dropped):
    scope = {}
    if name:
        scope["name"] = name
    if version:
        scope["version"] = version
    if attrs:
        scope["attributes"] = attributes(attrs)
    if dropped:
        scope["droppedAttributesCount"] = dropped
    return scope


class Groups:
    def __init__(self):
        self.resources = {}

    def scope(self, row):
        resource = row["ResourceAttributes"]
        service = row.get("ServiceName")
        if service and not any(key == "service.name" for key, _ in resource):
            resource = resource + [("service.name", service)]
        resource_key = (tuple(resource), row.get("ResourceSchemaUrl", ""))
        scope_key = (row.get("ScopeName", ""), row.get("ScopeVersion", ""),
                     tuple(row.get("ScopeAttributes") or ()),
                     row.get("ScopeDroppedAttrCount", 0),
                     row.get("ScopeSchemaUrl", ""))
        return self.resources.setdefault(resource_key, {}).setdefault(scope_key, {})

    def request(self, resources_key, scopes_key, records_key, finish):
        resources = []
        for (resource, resource_schema), scopes in self.resources.items():
            entries = []
            for (name, version, attrs, dropped, schema), records in scopes.items():
                entry = {"scope": scope_object(name, version, attrs, dropped),
                         records_key: finish(records)}
                if schema:
                    entry["schemaUrl"] = schema
                entries.append(entry)
            item = {"resource": {"attributes": attributes(resource)},
                    scopes_key: entries}
            if resource_schema:
                item["schemaUrl"] = resource_schema
            resources.append(item)
        return {resources_key: resources}


def log_record(row):
    record = {"timeUnixNano": str(row["Timestamp"])}
    if row["SeverityNumber"]:
        record["severityNumber"] = row["SeverityNumber"]
    if row["SeverityText"]:
        record["severityText"] = row["SeverityText"]
    record["body"] = {"stringValue": row["Body"]}
    if row["LogAttributes"]:
        record["attributes"] = attributes(row["LogAttributes"])
    if row["TraceFlags"]:
        record["flags"] = row["TraceFlags"]
    if row["TraceId"]:
        record["traceId"] = row["TraceId"]
    if row["SpanId"]:
        record["spanId"] = row["SpanId"]
    return record


def span(row):
    start = row["Timestamp"]
    out = {"traceId": row["TraceId"], "spanId": row["SpanId"]}
    if row["ParentSpanId"]:
        out["parentSpanId"] = row["ParentSpanId"]
    if row["TraceState"]:
        out["traceState"] = row["TraceState"]
    out["name"] = row["SpanName"]
    kind = SPAN_KINDS[row["SpanKind"].lower().removeprefix("span_kind_")]
    if kind:
        out["kind"] = kind
    out["startTimeUnixNano"] = str(start)
    out["endTimeUnixNano"] = str(start + row["Duration"])
    if row["SpanAttributes"]:
        out["attributes"] = attributes(row["SpanAttributes"])
    events = []
    for time, name, attrs in zip(row["Events.Timestamp"], row["Events.Name"],
                                 row["Events.Attributes"]):
        event = {"timeUnixNano": str(time), "name": name}
        if attrs:
            event["attributes"] = attributes(attrs)
        events.append(event)
    if events:
        out["events"] = events
    links = []
    for trace_id, span_id, state, attrs in zip(
            row["Links.TraceId"], row["Links.SpanId"], row["Links.TraceState"],
            row["Links.Attributes"]):
        link = {"traceId": trace_id, "spanId": span_id}
        if state:
            link["traceState"] = state
        if attrs:
            link["attributes"] = attributes(attrs)
        links.append(link)
    if links:
        out["links"] = links
    status = {}
    code = STATUS_CODES[row["StatusCode"].lower().removeprefix("status_code_")]
    if code:
        status["code"] = code
    if row["StatusMessage"]:
        status["message"] = row["StatusMessage"]
    out["status"] = status
    return out


def exemplars(row):
    out = []
    for attrs, time, value, span_id, trace_id in zip(
            row["Exemplars.FilteredAttributes"], row["Exemplars.TimeUnix"],
            row["Exemplars.Value"], row["Exemplars.SpanId"],
            row["Exemplars.TraceId"]):
        exemplar = {"timeUnixNano": str(time), "asDouble": double(value)}
        if attrs:
            exemplar["filteredAttributes"] = attributes(attrs)
        if span_id:
            exemplar["spanId"] = span_id
        if trace_id:
            exemplar["traceId"] = trace_id
        out.append(exemplar)
    return out


def data_point(kind, row):
    point = {}
    if row["Attributes"]:
        point["attributes"] = attributes(row["Attributes"])
    if row["StartTimeUnix"]:
        point["startTimeUnixNano"] = str(row["StartTimeUnix"])
    point["timeUnixNano"] = str(row["TimeUnix"])
    if kind == "histogram":
        point["count"] = str(row["Count"])
        point["sum"] = double(row["Sum"])
        point["bucketCounts"] = [str(count) for count in row["BucketCounts"]]
        point["explicitBounds"] = [double(bound) for bound in row["ExplicitBounds"]]
        if row["Min"] or row["Max"]:
            point["min"] = double(row["Min"])
            point["max"] = double(row["Max"])
    else:
        point["asDouble"] = double(row["Value"])
    found = exemplars(row)
    if found:
        point["exemplars"] = found
    if row["Flags"]:
        point["flags"] = row["Flags"]
    return point


def metric_object(key, points):
    name, description, unit, kind, temporality, monotonic = key
    metric = {"name": name}
    if description:
        metric["description"] = description
    if unit:
        metric["unit"] = unit
    body = {"dataPoints": points}
    if kind != "gauge" and temporality:
        body["aggregationTemporality"] = temporality
    if kind == "sum" and monotonic:
        body["isMonotonic"] = True
    metric[kind] = body
    return metric


def logs_request(rows):
    groups = Groups()
    for row in rows["otel_logs"]:
        groups.scope(row).setdefault(None, []).append(log_record(row))
    return groups.request("resourceLogs", "scopeLogs", "logRecords",
                          lambda records: records[None])


def traces_request(rows):
    groups = Groups()
    for row in rows["otel_traces"]:
        groups.scope(row).setdefault(None, []).append(span(row))
    return groups.request("resourceSpans", "scopeSpans", "spans",
                          lambda records: records[None])


def metrics_request(rows):
    groups = Groups()
    for table, kind in METRIC_KINDS.items():
        for row in rows.get(table, ()):
            key = (row["MetricName"], row["MetricDescription"], row["MetricUnit"],
                   kind, row.get("AggregationTemporality", 0),
                   row.get("IsMonotonic", False))
            groups.scope(row).setdefault(key, []).append(data_point(kind, row))
    return groups.request(
        "resourceMetrics", "scopeMetrics", "metrics",
        lambda metrics: [metric_object(key, points)
                         for key, points in metrics.items()])


REQUESTS = {"logs": logs_request, "traces": traces_request,
            "metrics": metrics_request}


def encode(signal, request):
    import importlib

    module_name, message_name = fixtures.SIGNALS[signal]
    message = getattr(importlib.import_module(module_name), message_name)()
    json_format.ParseDict(fixtures.hex_ids_to_base64(request), message)
    return message.SerializeToString()


def varint(buf, pos):
    value = shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, pos
        shift += 7


def timestamp_offsets(buf, layout, start=0, end=None, out=None):
    out = array.array("I") if out is None else out
    pos, end = start, len(buf) if end is None else end
    while pos < end:
        key, pos = varint(buf, pos)
        field, wire = key >> 3, key & 7
        if wire == 0:
            _, pos = varint(buf, pos)
        elif wire == 1:
            if layout.get(field) is TS:
                out.append(pos)
            pos += 8
        elif wire == 2:
            size, pos = varint(buf, pos)
            child = layout.get(field)
            if isinstance(child, dict):
                timestamp_offsets(buf, child, pos, pos + size, out)
            pos += size
        elif wire == 5:
            pos += 4
        else:
            raise ValueError(f"unexpected wire type {wire} at {pos}")
    return out


def shift_pb(body, offsets, delta):
    buf = bytearray(body)
    for offset in offsets:
        value = int.from_bytes(buf[offset:offset + 8], "little") + delta
        buf[offset:offset + 8] = value.to_bytes(8, "little")
    return bytes(buf)


def shift_json(body, offsets, delta):
    buf = bytearray(body)
    for offset in offsets:
        value = b"%d" % (int(buf[offset:offset + 19]) + delta)
        if len(value) != 19:
            raise ValueError(f"timestamp {value!r} does not fit 19 digits")
        buf[offset:offset + 19] = value
    return bytes(buf)


_files = {}


def read_rows(path, start, count):
    if count == 0:
        return []
    key = (path, path.stat().st_mtime_ns)
    entry = _files.get(key)
    if entry is None:
        file = pq.ParquetFile(path)
        bounds = [0]
        for i in range(file.metadata.num_row_groups):
            bounds.append(bounds[-1] + file.metadata.row_group(i).num_rows)
        entry = _files[key] = (file, bounds)
    file, bounds = entry
    groups = [i for i in range(len(bounds) - 1)
              if bounds[i] < start + count and bounds[i + 1] > start]
    table = file.read_row_groups(groups)
    return table.slice(start - bounds[groups[0]], count).to_pylist()


def build(signal, rows):
    request = REQUESTS[signal](rows)
    pb = encode(signal, request)
    text = json.dumps(request, separators=(",", ":")).encode()
    return pb, text


def batch_rows(slice_dir, ranges):
    return {table: read_rows(slice_dir / f"{table}.parquet", start, count)
            for table, (start, count) in ranges.items()}


def record_time(table, row):
    if table == "otel_traces":
        return row["Timestamp"] + row["Duration"]
    return row[time_column(table)]


def measure(task):
    signal, slice_dir, ranges = task
    rows = batch_rows(slice_dir, ranges)
    pb, _ = build(signal, rows)
    return len(pb), sum(len(table_rows) for table_rows in rows.values())


def convert(task):
    signal, slice_dir, batch_dir, batches = task
    results = []
    for index, ranges in batches:
        rows = batch_rows(slice_dir, ranges)
        pb, text = build(signal, rows)
        pb_offsets = timestamp_offsets(pb, LAYOUT[signal])
        json_offsets = array.array("I", (m.start(1) for m in JSON_TIME.finditer(text)))
        if len(pb_offsets) != len(json_offsets):
            raise ValueError(f"batch {index}: {len(pb_offsets)} protobuf timestamps,"
                             f" {len(json_offsets)} JSON timestamps")
        stem = batch_dir / f"{index:06d}"
        stem.with_suffix(".pb").write_bytes(pb)
        stem.with_suffix(".pb.ts").write_bytes(pb_offsets.tobytes())
        stem.with_suffix(".json").write_bytes(text)
        stem.with_suffix(".json.ts").write_bytes(json_offsets.tobytes())
        times = [record_time(table, row)
                 for table, table_rows in rows.items() for row in table_rows]
        samples = sum(len(row["BucketCounts"]) + 2 if table == "otel_metrics_histogram"
                      else 1 for table, table_rows in rows.items() for row in table_rows)
        results.append({
            "index": index, "pb": len(pb), "json": len(text),
            "rows": {table: len(table_rows) for table, table_rows in rows.items()},
            "samples": samples, "max_time": max(times), "min_time": min(times)})
    return results


def bytes_per_record(signal, source, table, sample=4096):
    rows = source.group(0).slice(0, sample).to_pylist()
    pb, _ = build(signal, {table: rows})
    return len(pb) / len(rows)


def plan_rows(signal, sources, size):
    tables = TABLES[signal]
    weights = {t: sources[t].rows for t in tables}
    total_weight = sum(weights.values())
    cost = sum(weights[t] / total_weight * bytes_per_record(signal, sources[t], t)
               for t in tables)
    records = size / cost
    return {t: max(1, round(records * weights[t] / total_weight)) for t in tables}


def plan_batches(signal, slices, batch):
    if signal != "metrics":
        table, rows = next(iter(slices.items()))
        return [(i, {table: (start, min(batch, rows.num_rows - start))})
                for i, start in enumerate(range(0, rows.num_rows, batch))]
    merged = pa.concat_tables([
        pa.table({"time": rows.column(time_column(table)),
                  "kind": pa.array([k] * rows.num_rows, pa.int8())})
        for k, (table, rows) in enumerate(slices.items())])
    kinds = merged.take(pc.sort_indices(merged, [("time", "ascending")])
                        ).column("kind").to_pylist()
    names = list(slices)
    offsets = [0] * len(names)
    batches = []
    for i, start in enumerate(range(0, len(kinds), batch)):
        counts = [0] * len(names)
        for kind in kinds[start:start + batch]:
            counts[kind] += 1
        batches.append((i, {names[k]: (offsets[k], counts[k])
                            for k in range(len(names)) if counts[k]}))
        offsets = [offsets[k] + counts[k] for k in range(len(names))]
    return batches


def output_dir(data_dir, signal, size, batch):
    return data_dir / "otlp" / f"{signal}-{size}-b{batch}"


def prepare(data_dir, signal, size_text, batch, jobs):
    out = output_dir(data_dir, signal, size_text, batch)
    manifest_path = out / "manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    size = parse_size(size_text)
    sources = {t: Source(data_dir, t) for t in TABLES[signal]}
    wanted = plan_rows(signal, sources, size)
    slice_dir = out / "slice"
    batch_dir = out / "batches"
    slice_dir.mkdir(parents=True, exist_ok=True)
    batch_dir.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ProcessPoolExecutor(jobs) as pool:
        for _ in range(4):
            slices = {}
            for table, rows in wanted.items():
                print(f"  slicing {rows} rows of {table}", file=sys.stderr)
                sliced = sources[table].head(rows).sort_by(time_column(table))
                pq.write_table(sliced, slice_dir / f"{table}.parquet",
                               row_group_size=batch)
                slices[table] = sliced
            batches = plan_batches(signal, slices, batch)
            del slices
            sample = batches[::max(1, math.ceil(len(batches) / jobs))]
            measured = list(pool.map(measure, [(signal, slice_dir, ranges)
                                               for _, ranges in sample]))
            estimate = (sum(b for b, _ in measured) / sum(r for _, r in measured)
                        * sum(wanted.values()))
            print(f"  about {estimate / 2 ** 20:.0f} MB of protobuf", file=sys.stderr)
            if abs(estimate / size - 1) <= 0.03:
                break
            wanted = {t: max(1, round(n * size / estimate)) for t, n in wanted.items()}
        chunk = max(1, 32768 // batch)
        tasks = [(signal, slice_dir, batch_dir, batches[i:i + chunk])
                 for i in range(0, len(batches), chunk)]
        results = []
        for done in pool.map(convert, tasks):
            results.extend(done)
            print(f"\r  encoded {len(results)}/{len(batches)} requests",
                  end="", file=sys.stderr)
    print(file=sys.stderr)
    rows = {}
    for result in results:
        for table, count in result["rows"].items():
            rows[table] = rows.get(table, 0) + count
    manifest = {
        "signal": signal, "dataset": DATASET, "size": size_text, "batch": batch,
        "batches": len(results), "rows": rows, "records": sum(rows.values()),
        "samples": sum(r["samples"] for r in results),
        "bytes": {"pb": sum(r["pb"] for r in results),
                  "json": sum(r["json"] for r in results)},
        "min_time": min(r["min_time"] for r in results),
        "max_time": max(r["max_time"] for r in results),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest


def batch_paths(data_dir, manifest, encoding):
    batch_dir = output_dir(data_dir, manifest["signal"], manifest["size"],
                           manifest["batch"]) / "batches"
    return [batch_dir / f"{i:06d}.{encoding}" for i in range(manifest["batches"])]


def default_data_dir():
    value = os.environ.get("OTEL_BENCH_DATA_DIR")
    return pathlib.Path(value) if value else None


def add_data_arguments(parser):
    parser.add_argument("--signal", choices=SIGNALS, default="logs")
    parser.add_argument("--size", default="1g",
                        help="protobuf bytes to prepare, e.g. 64m, 1g")
    parser.add_argument("--batch", type=int, default=8192,
                        help="records (log records, spans, data points) per request")
    parser.add_argument("--data-dir", type=pathlib.Path, default=default_data_dir(),
                        help="where the corpus and the requests go "
                             "(default: $OTEL_BENCH_DATA_DIR)")
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count(), 16))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_data_arguments(parser)
    args = parser.parse_args()
    if args.data_dir is None:
        sys.exit("--data-dir or $OTEL_BENCH_DATA_DIR is required")
    manifest = prepare(args.data_dir, args.signal, args.size, args.batch, args.jobs)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
