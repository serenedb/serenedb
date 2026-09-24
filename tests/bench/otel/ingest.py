#!/usr/bin/env python3
r"""OTLP/JSON ingest: HTTP endpoint vs pg wire bulk load.

HTTP sends the ProtoJSON payload to POST /v1/{logs,traces,metrics}, which
parses it and inserts the rows. psql sends those same rows, already mapped to
the otel_* columns, with `\copy <table> FROM <file>` in COPY text format -- the
fastest exact pg path: a VALUES list is dominated by SQL parsing, and binary
COPY truncates TIMESTAMP_NS to microseconds. A metrics payload feeds all five
otel_metrics_* tables, so its psql side is five \copy commands in one session.
The COPY files are produced once per payload, before timing, by the mapping the
endpoint uses, so both paths store identical rows.

The gzip client sends the same payload gzip-compressed (compressed before
timing), with Content-Encoding: gzip. The payloads repeat a few records, so
they compress far better than real telemetry; the server's inflate cost is
still proportional to the decompressed size.

The timed requests go through the stock clients, and each tool reports its
own time: curl's `%{time_total}` and psql's `\timing` (summed over the \copy
commands of one request). The harness starts its own serened on free ports
with a fresh datadir, and also records the server CPU time.

Single requests, per signal and size (tables truncated between runs):

    tests/bench/otel/ingest.py --sizes 64k,1m,32m

Sustained ingestion (never truncated, fresh server per path): --total JSON in
--chunk sized requests, cycling logs -> traces -> metrics:

    tests/bench/otel/ingest.py --total 1g --chunk 32m

--network lan|wan puts toxiproxy (docker) between the clients and serened, on
both the HTTP and the pg port, with latency and a bandwidth cap in each
direction; --latency-ms / --bandwidth-mbit override the preset. Setup and
verification queries bypass it.

    tests/bench/otel/ingest.py --total 1g --chunk 32m --network wan
"""

import argparse
import gzip
import io
import json
import math
import os
import pathlib
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import psycopg2

ROOT = pathlib.Path(__file__).resolve().parents[3]
CONFORMANCE = ROOT / "resources" / "otel" / "conformance"
TICK_MS = 1000 / os.sysconf("SC_CLK_TCK")
UNITS = {"k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}

METRIC_TABLES = ("gauge", "sum", "histogram", "exponential_histogram", "summary")

TOXIPROXY_IMAGE = os.environ.get("TOXIPROXY_IMAGE", "ghcr.io/shopify/toxiproxy")
# name -> (one-way latency ms, jitter ms, bandwidth Mbit/s per direction)
NETWORKS = {
    "lan": (1, 0, 1000),   # same datacenter, 1 GbE
    "wan": (20, 5, 100),   # cross-region, 100 Mbit/s
}


def read_fixture(path):
    return json.loads((CONFORMANCE / path).read_text())


def with_extra(seed, extra, resources, scopes, key):
    """Appends every record of `extra` to the seed's first scope."""
    seed[resources][0][scopes][0][key] += [
        record for resource in extra[resources]
        for scope in resource[scopes] for record in scope[key]]
    return seed


def logs_seed():
    return read_fixture("logs/basic.json")


def traces_seed():
    # Every span kind and status, plus events and links.
    return with_extra(read_fixture("traces/kinds_and_statuses.json"),
                      read_fixture("traces/span_with_events_and_links.json"),
                      "resourceSpans", "scopeSpans", "spans")


def metrics_seed():
    # gauge, sum, histogram and summary, plus an exponential histogram, so one
    # payload feeds all five metric tables.
    return with_extra(read_fixture("metrics/mixed_batch.json"),
                      read_fixture("metrics/exponential_histogram.json"),
                      "resourceMetrics", "scopeMetrics", "metrics")


# signal -> (seed, JSON path to the repeated records, [(table, parse function)])
SIGNALS = {
    "logs": (logs_seed, ("resourceLogs", "scopeLogs", "logRecords"),
             [("otel_logs", "otel_parse_logs")]),
    "traces": (traces_seed, ("resourceSpans", "scopeSpans", "spans"),
               [("otel_traces", "otel_parse_traces")]),
    "metrics": (metrics_seed, ("resourceMetrics", "scopeMetrics", "metrics"),
                [(f"otel_metrics_{t}", f"otel_parse_metrics_{t}")
                 for t in METRIC_TABLES]),
}


def parse_size(text):
    text = text.strip().lower().rstrip("b")
    return int(text[:-1]) * UNITS[text[-1]] if text[-1] in UNITS else int(text)


def human(size):
    for unit, factor in (("GB", 1024 ** 3), ("MB", 1024 * 1024), ("KB", 1024)):
        if size >= factor:
            return f"{size / factor:.0f} {unit}"
    return f"{size} B"


def make_payload(signal, target):
    """Repeats the seed's records until the compact JSON reaches target."""
    seed_fn, (resources, scopes, key), _ = SIGNALS[signal]
    seed = seed_fn()
    records = seed[resources][0][scopes][0][key]
    template = list(records)
    while True:
        body = json.dumps(seed, separators=(",", ":"))
        if len(body) >= target:
            return body.encode()
        grow = max(1, len(records) * (target - len(body)) // len(body))
        records.extend(template[i % len(template)] for i in range(grow))


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cpu_ms(pid):
    fields = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) * TICK_MS


def peak_rss_mb(pid):
    for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) / 1024
    return 0.0


class Network:
    """toxiproxy in docker; proxies clients to each server with latency and a
    bandwidth cap in both directions."""

    def __init__(self, latency_ms, jitter_ms, bandwidth_mbit):
        self.latency_ms = latency_ms
        self.jitter_ms = jitter_ms
        # toxiproxy's bandwidth toxic takes KB/s
        self.rate_kbps = int(bandwidth_mbit * 1000 / 8)
        self.api_port = free_port()
        self.name = f"otel-bench-toxiproxy-{self.api_port}"
        subprocess.run(["docker", "run", "-d", "--rm", "--name", self.name,
                        "--network", "host", TOXIPROXY_IMAGE, "-host",
                        "127.0.0.1", "-port", str(self.api_port)],
                       check=True, capture_output=True)
        deadline = time.monotonic() + 60
        while True:
            try:
                self._call("GET", "/version")
                break
            except OSError:
                if time.monotonic() > deadline:
                    self.stop()
                    sys.exit("toxiproxy did not come up")
                time.sleep(0.3)

    def describe(self):
        return (f"{self.latency_ms} ms (+/- {self.jitter_ms}) one-way latency, "
                f"{self.rate_kbps * 8 / 1000:.0f} Mbit/s each way")

    def _call(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.api_port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read()

    def proxy(self, name, upstream_port):
        """A proxied port in front of `upstream_port`."""
        port = free_port()
        self._call("POST", "/proxies", {
            "name": name, "listen": f"127.0.0.1:{port}",
            "upstream": f"127.0.0.1:{upstream_port}", "enabled": True})
        for stream in ("upstream", "downstream"):
            self._call("POST", f"/proxies/{name}/toxics", {
                "name": f"latency_{stream}", "type": "latency", "stream": stream,
                "attributes": {"latency": self.latency_ms,
                               "jitter": self.jitter_ms}})
            self._call("POST", f"/proxies/{name}/toxics", {
                "name": f"bandwidth_{stream}", "type": "bandwidth",
                "stream": stream, "attributes": {"rate": self.rate_kbps}})
        return port

    def remove(self, name):
        try:
            self._call("DELETE", f"/proxies/{name}")
        except OSError:
            pass

    def stop(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


class Server:
    def __init__(self, binary, network=None):
        self.datadir = tempfile.mkdtemp(prefix="otel-bench-")
        self.pg_port, self.http_port = free_port(), free_port()
        self.network = network
        listen = (f"postgres://127.0.0.1:{self.pg_port},"
                  f"http://127.0.0.1:{self.http_port}?api=otel")
        self.log = open(os.path.join(self.datadir, "serened.log"), "w")
        self.proc = subprocess.Popen([binary, self.datadir, "--listen", listen],
                                     stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                self.connect().close()
                break
            except psycopg2.OperationalError:
                time.sleep(0.2)
        else:
            self.stop()
            sys.exit(f"serened did not start; see {self.datadir}/serened.log")
        # The ports the timed clients use: through the proxy when there is one.
        self.client_http_port, self.client_pg_port = self.http_port, self.pg_port
        if network is not None:
            tag = f"{self.pg_port}"
            self.client_http_port = network.proxy(f"http_{tag}", self.http_port)
            self.client_pg_port = network.proxy(f"pg_{tag}", self.pg_port)

    def connect(self):
        conn = psycopg2.connect(host="127.0.0.1", port=self.pg_port,
                                user="postgres", dbname="postgres")
        conn.autocommit = True
        return conn

    def stop(self):
        if self.network is not None:
            self.network.remove(f"http_{self.pg_port}")
            self.network.remove(f"pg_{self.pg_port}")
        self.proc.kill()
        self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)

    def truncate(self, tables):
        with self.connect() as conn:
            for table in tables:
                conn.cursor().execute(f"TRUNCATE {table}")

    def count(self, table):
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(f"VACUUM (REFRESH_TABLE) {table}")
            cur.execute(f"SELECT count(*) FROM {table}")
            return cur.fetchone()[0]


def copy_rows(server, payload, function):
    """The payload's rows in COPY text format, mapped by the endpoint's code."""
    with server.connect() as conn:
        literal = "'" + payload.decode().replace("'", "''") + "'"
        out = io.BytesIO()
        conn.cursor().copy_expert(
            f"COPY (SELECT * FROM {function}({literal}, 'json')) TO STDOUT", out)
    data = out.getvalue()
    return data, data.count(b"\n")


class Batch:
    """One request's worth of one signal, prepared for both clients."""

    def __init__(self, server, signal, payload):
        self.signal = signal
        self.payload = payload
        self.copies = []  # (table, COPY text, rows)
        for table, function in SIGNALS[signal][2]:
            data, rows = copy_rows(server, payload, function)
            self.copies.append((table, data, rows))

    def tables(self):
        return [table for table, _, _ in self.copies]

    def rows(self):
        return {table: rows for table, _, rows in self.copies}

    def copy_bytes(self):
        return sum(len(data) for _, data, _ in self.copies)


class CurlPath:
    def __init__(self, server, workdir, batch, index):
        self.url = f"http://127.0.0.1:{server.client_http_port}/v1/{batch.signal}"
        self.file = os.path.join(workdir, f"payload{index}.json")
        pathlib.Path(self.file).write_bytes(batch.payload)

    def send(self):
        out = subprocess.run(
            ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
             "-u", "postgres:", "-H", "Content-Type: application/json",
             "--data-binary", f"@{self.file}", self.url],
            capture_output=True, text=True, check=True).stdout.split()
        if out[0] != "200":
            sys.exit(f"curl: HTTP {out[0]} on {self.url}")
        return float(out[1]) * 1000


class CurlGzipPath(CurlPath):
    """curl with the body gzip-compressed (untimed), as the collector sends it."""

    def __init__(self, server, workdir, batch, index):
        super().__init__(server, workdir, batch, index)
        compressed = self.file + ".gz"
        pathlib.Path(compressed).write_bytes(gzip.compress(batch.payload))
        self.file = compressed

    def send(self):
        out = subprocess.run(
            ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
             "-u", "postgres:", "-H", "Content-Type: application/json",
             "-H", "Content-Encoding: gzip",
             "--data-binary", f"@{self.file}", self.url],
            capture_output=True, text=True, check=True).stdout.split()
        if out[0] != "200":
            sys.exit(f"curl: HTTP {out[0]} on {self.url}")
        return float(out[1]) * 1000


class PsqlCopyPath:
    def __init__(self, server, workdir, batch, index):
        self.port = str(server.client_pg_port)
        self.commands = []
        for n, (table, data, _) in enumerate(batch.copies):
            file = os.path.join(workdir, f"rows{index}_{n}.copy")
            pathlib.Path(file).write_bytes(data)
            self.commands += ["-c", f"\\copy {table} FROM '{file}'"]

    def send(self):
        out = subprocess.run(
            ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
             "-p", self.port, "-U", "postgres", "-d", "postgres",
             "-c", "\\timing on", *self.commands],
            capture_output=True, text=True, check=True).stdout
        times = re.findall(r"Time: ([0-9.]+) ms", out)
        if len(times) != len(self.commands) // 2:
            sys.exit(f"psql printed {len(times)} timings: {out[:300]!r}")
        return sum(float(t) for t in times)


PATHS = {"curl": CurlPath, "gzip": CurlGzipPath, "psql": PsqlCopyPath}


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


def single(args, paths, signals):
    rows = []
    for size in [parse_size(s) for s in args.sizes.split(",")]:
        for signal in signals:
            payload = make_payload(signal, size)
            runs = args.runs or (100 if size <= 256 * 1024 else
                                 20 if size <= 4 * 1024 * 1024 else 5)
            for name in paths:
                server = Server(args.bin, args.net)
                try:
                    batch = Batch(server, signal, payload)
                    path = PATHS[name](server, server.datadir, batch, 0)
                    wall, cpu = [], []
                    for i in range(args.warmup + runs):
                        server.truncate(batch.tables())
                        cpu_before = cpu_ms(server.proc.pid)
                        elapsed = path.send()
                        if i >= args.warmup:
                            wall.append(elapsed)
                            cpu.append(cpu_ms(server.proc.pid) - cpu_before)
                finally:
                    server.stop()
                p50 = statistics.median(wall)
                sent = (batch.copy_bytes() if name == "psql" else
                        os.path.getsize(path.file))
                rows.append((human(len(payload)), signal, name, sent,
                             sum(batch.rows().values()), p50,
                             percentile(wall, 0.99),
                             len(payload) / (1024 * 1024) / (p50 / 1000),
                             statistics.median(cpu), runs))
                print(f"  done {rows[-1][0]:>6} {signal} {name}", file=sys.stderr)

    print(f"\n{'size':>6}  {'signal':<8} {'path':<5} {'sent':>8} {'rows':>7} "
          f"{'p50 ms':>9} {'p99 ms':>9} {'MB/s':>7} {'srv CPU ms':>11} {'runs':>5}")
    for size, signal, name, sent, count, p50, p99, mbps, cpu, runs in rows:
        print(f"{size:>6}  {signal:<8} {name:<5} {human(sent):>8} {count:7d} "
              f"{p50:9.2f} {p99:9.2f} {mbps:7.1f} {cpu:11.1f} {runs:5d}")


def sustained(args, paths, signals):
    total = parse_size(args.total)
    chunk = parse_size(args.chunk)
    payloads = {signal: make_payload(signal, chunk) for signal in signals}
    chunk_bytes = statistics.mean(len(p) for p in payloads.values())
    order = [signals[i % len(signals)]
             for i in range(math.ceil(total / chunk_bytes))]
    results = []
    for concurrency in [int(c) for c in args.concurrency.split(",")]:
        for name in paths:
            server = Server(args.bin, args.net)
            try:
                batches = {s: Batch(server, s, payloads[s]) for s in signals}
                senders = {s: PATHS[name](server, server.datadir, batches[s], i)
                           for i, s in enumerate(signals)}
                cpu_before, start = cpu_ms(server.proc.pid), time.perf_counter()
                with ThreadPoolExecutor(concurrency) as pool:
                    times = list(pool.map(lambda s: senders[s].send(), order))
                wall = time.perf_counter() - start
                cpu = cpu_ms(server.proc.pid) - cpu_before
                rss = peak_rss_mb(server.proc.pid)
                expected = {}
                for signal in order:
                    for table, count in batches[signal].rows().items():
                        expected[table] = expected.get(table, 0) + count
                stored = {table: server.count(table) for table in expected}
            finally:
                server.stop()
            if stored != expected:
                sys.exit(f"{name}: stored {stored}, expected {expected}")
            sent = sum(len(payloads[s]) for s in order)
            results.append((concurrency, name, len(order),
                            sent / (1024 * 1024) / wall,
                            sum(stored.values()) / wall, wall, cpu / 1000, rss,
                            statistics.median(times), percentile(times, 0.99),
                            max(times)))
            print(f"  done c={concurrency} {name}", file=sys.stderr)

    print(f"\n{human(total)} of OTLP/JSON per run, {human(chunk)} requests "
          f"cycling {'/'.join(signals)} (MB/s counts the JSON payload for both "
          "paths)")
    print(f"network: {args.net.describe() if args.net else 'loopback'}")
    print(f"{'conc':>4}  {'path':<5} {'chunks':>6} {'MB/s':>7} {'rows/s':>9} "
          f"{'wall s':>7} {'srv CPU s':>9} {'peak RSS MB':>11} "
          f"{'p50 ms':>8} {'p99 ms':>8} {'max ms':>8}")
    for (conc, name, chunks, mbps, rps, wall, cpu, rss, p50, p99,
         worst) in results:
        print(f"{conc:>4}  {name:<5} {chunks:>6} {mbps:7.1f} {rps:9.0f} "
              f"{wall:7.2f} {cpu:9.2f} {rss:11.0f} "
              f"{p50:8.1f} {p99:8.1f} {worst:8.1f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bin", default=str(ROOT / "build_bench_no_lto/bin/serened"))
    parser.add_argument("--paths", default="curl,gzip,psql",
                        help="which clients to run: curl, gzip (curl with a gzip body), psql")
    parser.add_argument("--signals", default="logs,traces,metrics",
                        help="which signals to send, e.g. logs")
    parser.add_argument("--sizes", default="64k,1m,32m",
                        help="single-request mode: JSON payload sizes")
    parser.add_argument("--runs", type=int, default=0,
                        help="single-request mode: runs per case (default: by size)")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--total", help="sustained mode: JSON to ingest, e.g. 1g")
    parser.add_argument("--chunk", default="32m",
                        help="sustained mode: request size, e.g. 32m")
    parser.add_argument("--concurrency", default="1",
                        help="sustained mode: parallel senders, e.g. 1,8")
    parser.add_argument("--network", choices=sorted(NETWORKS),
                        help="emulate a network with toxiproxy (docker)")
    parser.add_argument("--latency-ms", type=int,
                        help="one-way latency, overrides the --network preset")
    parser.add_argument("--bandwidth-mbit", type=float,
                        help="bandwidth each way, overrides the --network preset")
    args = parser.parse_args()
    args.net = None
    if args.network or args.latency_ms is not None or args.bandwidth_mbit:
        latency, jitter, bandwidth = NETWORKS.get(args.network or "lan")
        if args.latency_ms is not None:
            latency, jitter = args.latency_ms, 0
        args.net = Network(latency, jitter, args.bandwidth_mbit or bandwidth)
    paths = args.paths.split(",")
    signals = args.signals.split(",")
    for signal in signals:
        if signal not in SIGNALS:
            sys.exit(f"unknown signal {signal!r}; known: {', '.join(SIGNALS)}")
    try:
        if args.total:
            sustained(args, paths, signals)
        else:
            single(args, paths, signals)
    finally:
        if args.net is not None:
            args.net.stop()


if __name__ == "__main__":
    main()
