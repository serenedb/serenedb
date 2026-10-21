#!/usr/bin/env python3
"""OTLP/HTTP ingestion: SereneDB vs ClickHouse, Elasticsearch, Victoria* and Quickwit.

Every engine receives the same requests from prepare.py on its own OTLP/HTTP
endpoint, one engine at a time, each started fresh: docker engines run with
--network host, SereneDB from --bin. Before sending, all timestamps are moved
so the newest record lands at the present. All requests go out over
--concurrency keep-alive connections, then the engine's own count is polled
until every record is visible.

    scripts/perf/otel_bench/run.py --signal logs --size 1g
    scripts/perf/otel_bench/run.py --signal metrics --engines serenedb,victoria \\
        --formats pb+gzip,pb,json --concurrency 1,8

Engines: serenedb; clickhouse behind otelcol-contrib's clickhouse exporter
(no queue, no batching, so a request is acknowledged once inserted; CPU and
memory include the collector); elasticsearch (native /_otlp, protobuf only);
victoria (VictoriaLogs, VictoriaTraces or VictoriaMetrics per signal, protobuf
only); quickwit (logs and traces, protobuf only, per-shard ingest throughput at
its 20 MiB/s maximum, burst, queues and request size limits raised, one shard
per CPU, 1 s commits). The collector and quickwit accept requests up to 256 MiB.

MB/s counts the uncompressed protobuf payload for every format. "ack" is the
time until the last request was answered, "visible" the time until the engine
counted every record (or, where its count cannot be predicted, until the count
stopped changing).
"""

import argparse
import array
import concurrent.futures
import gzip
import http.client
import importlib
import json
import os
import pathlib
import statistics
import sys
import threading
import time

import engines as engines_module
import prepare

ROOT = pathlib.Path(__file__).resolve().parents[3]

CONTENT_TYPES = {"pb": "application/x-protobuf", "json": "application/json"}
RESPONSES = {
    "logs": ("opentelemetry.proto.collector.logs.v1.logs_service_pb2",
             "ExportLogsServiceResponse", "rejected_log_records"),
    "traces": ("opentelemetry.proto.collector.trace.v1.trace_service_pb2",
               "ExportTraceServiceResponse", "rejected_spans"),
    "metrics": ("opentelemetry.proto.collector.metrics.v1.metrics_service_pb2",
                "ExportMetricsServiceResponse", "rejected_data_points"),
}
MAX_ATTEMPTS = 30


def parse_format(text):
    encoding, _, compression = text.partition("+")
    if encoding not in CONTENT_TYPES or compression not in ("", "gzip"):
        sys.exit(f"unknown format {text!r}; use pb, pb+gzip, json or json+gzip")
    return encoding, compression == "gzip"


def load_body(task):
    path, encoding, compress, delta = task
    offsets = array.array("I")
    offsets.frombytes(path.with_suffix(path.suffix + ".ts").read_bytes())
    shift = prepare.shift_pb if encoding == "pb" else prepare.shift_json
    body = shift(path.read_bytes(), offsets, delta)
    return gzip.compress(body, compresslevel=6, mtime=0) if compress else body


def load_bodies(data_dir, manifest, encoding, compress, jobs):
    delta = time.time_ns() - manifest["max_time"]
    tasks = [(path, encoding, compress, delta)
             for path in prepare.batch_paths(data_dir, manifest, encoding)]
    with concurrent.futures.ProcessPoolExecutor(jobs) as pool:
        return list(pool.map(load_body, tasks, chunksize=8))


def camel(name):
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def rejected(signal, content_type, data):
    if not data:
        return 0, ""
    module, name, field = RESPONSES[signal]
    try:
        if "json" in content_type:
            partial = json.loads(data).get("partialSuccess") or {}
            return int(partial.get(camel(field), 0)), partial.get("errorMessage", "")
        message = getattr(importlib.import_module(module), name)()
        message.ParseFromString(data)
        return (getattr(message.partial_success, field),
                message.partial_success.error_message)
    except Exception:
        return 0, ""


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


class Sender:
    def __init__(self, engine, signal, encoding, compress, bodies, concurrency):
        self.host, self.port, self.path = engine.endpoint(signal)
        self.headers = {"Content-Type": CONTENT_TYPES[encoding], **engine.headers()}
        if compress:
            self.headers["Content-Encoding"] = "gzip"
        self.signal = signal
        self.retryable = engine.retryable
        self.bodies = bodies
        self.concurrency = concurrency
        self.latencies = [0.0] * len(bodies)
        self.retries = 0
        self.rejected = 0
        self.messages = []
        self.errors = []
        self.lock = threading.Lock()
        self.next = iter(range(len(bodies)))

    def post(self, connection, body):
        if connection is None:
            connection = http.client.HTTPConnection(self.host, self.port, timeout=600)
        connection.request("POST", self.path, body=body, headers=self.headers)
        response = connection.getresponse()
        return connection, response.status, response.getheader("Content-Type", ""), response.read()

    def send(self, connection, index):
        started = time.perf_counter()
        status = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                connection, status, content_type, data = self.post(connection, self.bodies[index])
            except (OSError, http.client.HTTPException) as error:
                if connection is not None:
                    connection.close()
                connection, status, data = None, str(error), b""
            else:
                if status == 200:
                    count, message = rejected(self.signal, content_type, data)
                    with self.lock:
                        self.rejected += count
                        if message and len(self.messages) < 3:
                            self.messages.append(message)
                    self.latencies[index] = time.perf_counter() - started
                    return connection
                if not self.retryable(status, data):
                    raise RuntimeError(f"request {index}: HTTP {status}: {data[:500]!r}")
            with self.lock:
                self.retries += 1
            time.sleep(min(5.0, 0.1 * 2 ** attempt))
        raise RuntimeError(f"request {index}: {status} after {MAX_ATTEMPTS} attempts")

    def worker(self):
        connection = None
        try:
            while not self.errors:
                with self.lock:
                    index = next(self.next, None)
                if index is None:
                    return
                connection = self.send(connection, index)
        except Exception as error:
            with self.lock:
                self.errors.append(str(error))
        finally:
            if connection is not None:
                connection.close()

    def run(self):
        threads = [threading.Thread(target=self.worker, daemon=True)
                   for _ in range(self.concurrency)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if self.errors:
            raise RuntimeError(self.errors[0])


def wait_visible(engine, signal, expected, started, timeout):
    last, changed = None, time.perf_counter()
    while True:
        try:
            count = engine.count(signal)
        except Exception:
            count = last or 0
        now = time.perf_counter()
        if count != last:
            last, changed = count, now
        if expected is not None and count >= expected:
            return now - started, count
        if count and now - changed >= engine.settle:
            return changed - started, count
        if now - started > timeout:
            return None, count
        time.sleep(0.2)


def run_one(engine, signal, encoding, compress, bodies, concurrency, manifest, args):
    engine.check_ports()
    try:
        engine.start(signal)
        baseline = engine.count(signal)
        expected = engine.expected(signal, manifest)
        sender = Sender(engine, signal, encoding, compress, bodies, concurrency)
        cpu_before = engine.cpu_seconds()
        client_before = time.process_time()
        started = time.perf_counter()
        sender.run()
        ack = time.perf_counter() - started
        client_cpu = time.process_time() - client_before
        if expected is not None:
            expected -= sender.rejected
        visible, stored = wait_visible(
            engine, signal, None if expected is None else expected + baseline,
            started, args.visible_timeout)
        stored -= baseline
        cpu = engine.cpu_seconds() - cpu_before
        rss = engine.peak_rss()
        engine.flush(signal)
        disk = engine.disk_bytes()
    except Exception:
        print(engine.logs()[-3000:], file=sys.stderr)
        raise
    finally:
        engine.stop()
    latencies = [x * 1000 for x in sender.latencies]
    payload = manifest["bytes"]["pb"]
    return {
        "engine": engine.name, "signal": signal,
        "format": encoding + ("+gzip" if compress else ""),
        "concurrency": concurrency, "requests": len(bodies),
        "payload_bytes": payload, "wire_bytes": sum(len(b) for b in bodies),
        "records": manifest["records"], "expected": expected, "stored": stored,
        "rejected": sender.rejected, "rejections": sender.messages,
        "retries": sender.retries, "ack_s": ack, "visible_s": visible,
        "mb_s": payload / 2 ** 20 / ack, "records_s": manifest["records"] / ack,
        "server_cpu_s": cpu, "client_cpu_s": client_cpu, "peak_rss_bytes": rss,
        "disk_bytes": disk, "p50_ms": statistics.median(latencies),
        "p99_ms": percentile(latencies, 0.99), "max_ms": max(latencies),
    }


def show(results, manifest):
    print(f"\n{manifest['signal']}: {manifest['records']} records in "
          f"{manifest['batches']} requests of {manifest['batch']}, "
          f"{manifest['bytes']['pb'] / 2 ** 20:.0f} MB protobuf "
          f"({manifest['bytes']['json'] / 2 ** 20:.0f} MB JSON); "
          "MB/s counts the protobuf payload")
    print(f"{'engine':<14} {'format':<10} {'conc':>4} {'wire MB':>8} {'MB/s':>7} "
          f"{'records/s':>10} {'ack s':>7} {'visible s':>9} {'srv CPU s':>9} "
          f"{'cli CPU s':>9} {'RSS MB':>7} {'disk MB':>8} {'p50 ms':>8} "
          f"{'p99 ms':>8} {'max ms':>8} {'retry':>5}  stored/expected")
    for r in results:
        if "error" in r:
            print(f"{r['engine']:<14} {r['format']:<10} {r['concurrency']:>4}  "
                  f"{r['error']}")
            continue
        visible = f"{r['visible_s']:9.2f}" if r["visible_s"] is not None else f"{'timeout':>9}"
        expected = r["expected"] if r["expected"] is not None else "?"
        mark = "" if r["expected"] is None or r["stored"] == r["expected"] else " !"
        rejected = f" ({r['rejected']} rejected)" if r["rejected"] else ""
        print(f"{r['engine']:<14} {r['format']:<10} {r['concurrency']:>4} "
              f"{r['wire_bytes'] / 2 ** 20:8.0f} {r['mb_s']:7.1f} "
              f"{r['records_s']:10.0f} {r['ack_s']:7.2f} {visible} "
              f"{r['server_cpu_s']:9.1f} {r['client_cpu_s']:9.1f} "
              f"{r['peak_rss_bytes'] / 2 ** 20:7.0f} {r['disk_bytes'] / 2 ** 20:8.0f} "
              f"{r['p50_ms']:8.1f} {r['p99_ms']:8.1f} {r['max_ms']:8.1f} "
              f"{r['retries']:5d}  {r['stored']}/{expected}{mark}{rejected}")
        for message in r["rejections"]:
            print(f"{'':<14} rejected: {message[:300]}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    prepare.add_data_arguments(parser)
    parser.add_argument("--engines", default=",".join(engines_module.ENGINES))
    parser.add_argument("--formats", default="pb+gzip",
                        help="pb, pb+gzip, json, json+gzip (comma separated)")
    parser.add_argument("--concurrency", default="8",
                        help="parallel keep-alive connections, e.g. 1,8")
    parser.add_argument("--bin", default=str(ROOT / "build_bench_no_lto/bin/serened"))
    parser.add_argument("--visible-timeout", type=float, default=900)
    parser.add_argument("--json", type=pathlib.Path, help="also write results here")
    args = parser.parse_args()
    if args.data_dir is None:
        sys.exit("--data-dir or $OTEL_BENCH_DATA_DIR is required")

    names = args.engines.split(",")
    formats = [parse_format(f) for f in args.formats.split(",")]
    concurrencies = [int(c) for c in args.concurrency.split(",")]
    prefix = f"otel-bench-{os.getpid()}"
    try:
        engines = [engines_module.make(name, args.bin, prefix) for name in names]
    except engines_module.EngineError as error:
        sys.exit(str(error))

    manifest = prepare.prepare(args.data_dir, args.signal, args.size, args.batch,
                               args.jobs)
    results = []
    for encoding, compress in formats:
        label = encoding + ("+gzip" if compress else "")
        runnable = [e for e in engines if e.supports(args.signal, encoding)]
        for engine in engines:
            if engine not in runnable:
                print(f"  skip {engine.name}: no {args.signal} as {label}", file=sys.stderr)
        if not runnable:
            continue
        bodies = load_bodies(args.data_dir, manifest, encoding, compress, args.jobs)
        for concurrency in concurrencies:
            for engine in runnable:
                print(f"  {engine.name} {args.signal} {label} c={concurrency}",
                      file=sys.stderr)
                try:
                    result = run_one(engine, args.signal, encoding, compress,
                                     bodies, concurrency, manifest, args)
                    visible = result["visible_s"]
                    print(f"    {result['mb_s']:.1f} MB/s, ack {result['ack_s']:.1f}s, "
                          f"visible {'timeout' if visible is None else f'{visible:.1f}s'}",
                          file=sys.stderr)
                except (engines_module.EngineError, RuntimeError, OSError) as error:
                    print(f"  {engine.name} failed: {error}", file=sys.stderr)
                    result = {"engine": engine.name, "format": label,
                              "concurrency": concurrency, "error": str(error)}
                results.append(result)
                if args.json:
                    args.json.write_text(json.dumps(
                        {"manifest": manifest, "results": results}, indent=2))
        del bodies
    show(results, manifest)


if __name__ == "__main__":
    main()
