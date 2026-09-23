#!/usr/bin/env python3
r"""OTLP/JSON ingest: HTTP endpoint vs pg wire bulk load.

HTTP sends the ProtoJSON payload to POST /v1/logs, which parses it and inserts
the rows. psql sends those same rows, already mapped to otel_logs columns, with
`\copy otel_logs FROM <file>` in COPY text format -- the fastest exact pg path:
a VALUES list is dominated by SQL parsing, and binary COPY truncates
TIMESTAMP_NS to microseconds. The COPY file is produced once per size, before
timing, by the mapping the endpoint uses, so both paths store identical rows.

The timed requests go through the stock clients, and each tool reports its
own time: curl's `%{time_total}` and psql's `\timing`. The harness starts its
own serened on free ports with a fresh datadir, and also records the server
CPU time per request (from /proc/<pid>/stat).

    tests/bench/otel/ingest.py --bin build_bench_no_lto/bin/serened
    tests/bench/otel/ingest.py --sizes 64k,1m --runs 50

With --total it measures sustained ingestion instead: that much JSON, sent in
--chunk sized requests by --concurrency parallel senders, into a table that
is never truncated, on a fresh server per path.

    tests/bench/otel/ingest.py --total 1g --chunk 1m,32m --concurrency 1,8
"""

import argparse
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
from concurrent.futures import ThreadPoolExecutor

import psycopg2

ROOT = pathlib.Path(__file__).resolve().parents[3]
SEED = ROOT / "resources" / "otel" / "conformance" / "logs" / "basic.json"
TICK_MS = 1000 / os.sysconf("SC_CLK_TCK")
UNITS = {"k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}


def parse_size(text):
    text = text.strip().lower().rstrip("b")
    return int(text[:-1]) * UNITS[text[-1]] if text[-1] in UNITS else int(text)


def human(size):
    for unit, factor in (("MB", 1024 * 1024), ("KB", 1024)):
        if size >= factor:
            return f"{size / factor:.0f} {unit}"
    return f"{size} B"


def make_payload(target):
    """Repeats the seed's log records until the compact JSON reaches target."""
    seed = json.loads(SEED.read_text())
    records = seed["resourceLogs"][0]["scopeLogs"][0]["logRecords"]
    template = list(records)
    body = ""
    while True:
        body = json.dumps(seed, separators=(",", ":"))
        if len(body) >= target:
            break
        grow = max(1, len(records) * (target - len(body)) // len(body))
        records.extend(template[i % len(template)] for i in range(grow))
    return body.encode()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cpu_ms(pid):
    fields = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) * TICK_MS


class Server:
    def __init__(self, binary):
        self.datadir = tempfile.mkdtemp(prefix="otel-bench-")
        self.pg_port, self.http_port = free_port(), free_port()
        listen = (f"postgres://127.0.0.1:{self.pg_port},"
                  f"http://127.0.0.1:{self.http_port}?api=otel")
        self.log = open(os.path.join(self.datadir, "serened.log"), "w")
        self.proc = subprocess.Popen([binary, self.datadir, "--listen", listen],
                                     stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                psycopg2.connect(host="127.0.0.1", port=self.pg_port,
                                 user="postgres", dbname="postgres").close()
                return
            except psycopg2.OperationalError:
                time.sleep(0.2)
        self.stop()
        sys.exit(f"serened did not start; see {self.datadir}/serened.log")

    def stop(self):
        self.proc.kill()
        self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)


class CurlPath:
    def __init__(self, server, workdir):
        self.url = f"http://127.0.0.1:{server.http_port}/v1/logs"
        self.file = os.path.join(workdir, "payload.json")

    def prepare(self, payload):
        pathlib.Path(self.file).write_bytes(payload)

    def send(self):
        out = subprocess.run(
            ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
             "-u", "postgres:", "-H", "Content-Type: application/json",
             "--data-binary", f"@{self.file}", self.url],
            capture_output=True, text=True, check=True).stdout.split()
        if out[0] != "200":
            sys.exit(f"curl: HTTP {out[0]}")
        return float(out[1]) * 1000


def copy_rows(server, payload):
    """The payload's rows in COPY text format, mapped by the endpoint's code."""
    with psycopg2.connect(host="127.0.0.1", port=server.pg_port,
                          user="postgres", dbname="postgres") as conn:
        literal = "'" + payload.decode().replace("'", "''") + "'"
        out = io.BytesIO()
        conn.cursor().copy_expert(
            f"COPY (SELECT * FROM otel_parse_logs({literal}, 'json')) TO STDOUT",
            out)
    data = out.getvalue()
    return data, data.count(b"\n")


class PsqlCopyPath:
    def __init__(self, server, workdir):
        self.port = str(server.pg_port)
        self.file = os.path.join(workdir, "rows.copy")

    def prepare(self, rows):
        pathlib.Path(self.file).write_bytes(rows)

    def send(self):
        out = subprocess.run(
            ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
             "-p", self.port, "-U", "postgres", "-d", "postgres",
             "-c", "\\timing on", "-c", f"\\copy otel_logs FROM '{self.file}'"],
            capture_output=True, text=True, check=True).stdout
        times = re.findall(r"Time: ([0-9.]+) ms", out)
        if not times:
            sys.exit(f"psql printed no timing: {out[:300]!r}")
        return float(times[-1])


def truncate(server):
    with psycopg2.connect(host="127.0.0.1", port=server.pg_port,
                          user="postgres", dbname="postgres") as conn:
        conn.autocommit = True
        conn.cursor().execute("TRUNCATE otel_logs")


def measure(server, path, body, runs, warmup):
    path.prepare(body)
    wall, cpu = [], []
    for i in range(warmup + runs):
        truncate(server)
        cpu_before = cpu_ms(server.proc.pid)
        elapsed = path.send()
        if i >= warmup:
            wall.append(elapsed)
            cpu.append(cpu_ms(server.proc.pid) - cpu_before)
    return wall, cpu


def peak_rss_mb(pid):
    for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) / 1024
    return 0.0


def stored_rows(server):
    with psycopg2.connect(host="127.0.0.1", port=server.pg_port,
                          user="postgres", dbname="postgres") as conn:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("VACUUM (REFRESH_TABLE) otel_logs")
        cur.execute("SELECT count(*) FROM otel_logs")
        return cur.fetchone()[0]


def sustained(args):
    total = parse_size(args.total)
    results = []
    for chunk in [parse_size(c) for c in args.chunk.split(",")]:
        payload = make_payload(chunk)
        copy, rows_per_chunk = None, 0
        chunks = math.ceil(total / len(payload))
        for concurrency in [int(c) for c in args.concurrency.split(",")]:
            for name, cls in (("curl", CurlPath), ("psql", PsqlCopyPath)):
                if name not in args.paths.split(","):
                    continue
                server = Server(args.bin)
                try:
                    if copy is None:
                        copy, rows_per_chunk = copy_rows(server, payload)
                    path = cls(server, server.datadir)
                    path.prepare(payload if name == "curl" else copy)
                    cpu_before, start = cpu_ms(server.proc.pid), time.perf_counter()
                    with ThreadPoolExecutor(concurrency) as pool:
                        times = list(pool.map(lambda _: path.send(), range(chunks)))
                    wall = time.perf_counter() - start
                    cpu = cpu_ms(server.proc.pid) - cpu_before
                    rss = peak_rss_mb(server.proc.pid)
                    stored = stored_rows(server)
                finally:
                    server.stop()
                expected = chunks * rows_per_chunk
                if stored != expected:
                    sys.exit(f"{name}: stored {stored} rows, expected {expected}")
                sent = chunks * len(payload)
                results.append((human(len(payload)), concurrency, name, chunks,
                                 sent / (1024 * 1024) / wall, stored / wall, wall,
                                 cpu / 1000, rss, statistics.median(times),
                                 percentile(times, 0.99), max(times)))
                print(f"  done chunk={human(len(payload))} c={concurrency} {name}",
                      file=sys.stderr)

    print(f"\n{human(total)} of OTLP/JSON per run (MB/s counts the JSON payload "
          "for both paths)")
    print(f"{'chunk':>6} {'conc':>4}  {'path':<5} {'chunks':>6} {'MB/s':>7} "
          f"{'rows/s':>9} {'wall s':>7} {'srv CPU s':>9} {'peak RSS MB':>11} "
          f"{'p50 ms':>8} {'p99 ms':>8} {'max ms':>8}")
    for (chunk, conc, name, chunks, mbps, rps, wall, cpu, rss,
         p50, p99, worst) in results:
        print(f"{chunk:>6} {conc:>4}  {name:<5} {chunks:>6} {mbps:7.1f} "
              f"{rps:9.0f} {wall:7.2f} {cpu:9.2f} {rss:11.0f} "
              f"{p50:8.1f} {p99:8.1f} {worst:8.1f}")


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bin", default=str(ROOT / "build_bench_no_lto/bin/serened"))
    parser.add_argument("--sizes", default="64k,1m,32m",
                        help="JSON payload sizes, e.g. 64k,1m,32m")
    parser.add_argument("--runs", type=int, default=0,
                        help="measured runs per case (default: by size)")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--paths", default="curl,psql",
                        help="which clients to run, e.g. curl")
    parser.add_argument("--total", help="sustained mode: JSON to ingest, e.g. 1g")
    parser.add_argument("--chunk", default="32m",
                        help="sustained mode: request sizes, e.g. 1m,32m")
    parser.add_argument("--concurrency", default="1",
                        help="sustained mode: parallel senders, e.g. 1,8")
    args = parser.parse_args()
    if args.total:
        sustained(args)
        return

    sizes = [parse_size(s) for s in args.sizes.split(",")]
    server = Server(args.bin)
    rows = []
    try:
        for size in sizes:
            payload = make_payload(size)
            copy, row_count = copy_rows(server, payload)
            runs = args.runs or (100 if size <= 256 * 1024 else
                                 20 if size <= 4 * 1024 * 1024 else 5)
            for name, cls, body in (("curl", CurlPath, payload),
                                    ("psql", PsqlCopyPath, copy)):
                if name not in args.paths.split(","):
                    continue
                path = cls(server, server.datadir)
                wall, cpu = measure(server, path, body, runs, args.warmup)
                p50 = statistics.median(wall)
                rows.append((human(len(payload)), name, len(body), row_count, p50,
                             percentile(wall, 0.99),
                             len(payload) / (1024 * 1024) / (p50 / 1000),
                             statistics.median(cpu), runs))
                print(f"  done {rows[-1][0]:>6} {name}", file=sys.stderr)
    finally:
        server.stop()

    print(f"\n{'size':>6}  {'path':<5} {'sent':>8} {'rows':>7} {'p50 ms':>9} "
          f"{'p99 ms':>9} {'MB/s':>7} {'srv CPU ms':>11} {'runs':>5}")
    for size, path, sent, count, p50, p99, mbps, cpu, runs in rows:
        print(f"{size:>6}  {path:<5} {human(sent):>8} {count:7d} {p50:9.2f} "
              f"{p99:9.2f} {mbps:7.1f} {cpu:11.1f} {runs:5d}")

    print("\ncurl vs psql, p50 as each tool reports it "
          "(MB/s counts the JSON payload size for both):")
    by_key = {(r[0], r[1]): r for r in rows}
    for size in dict.fromkeys(r[0] for r in rows):
        if (size, "curl") not in by_key or (size, "psql") not in by_key:
            continue
        ratio = by_key[(size, "curl")][4] / by_key[(size, "psql")][4]
        print(f"  {size:>6}  curl/psql = {ratio:.2f}x")


if __name__ == "__main__":
    main()
