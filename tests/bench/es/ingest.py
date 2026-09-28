#!/usr/bin/env python3
r"""Elasticsearch _bulk ingest: HTTP endpoint vs pg wire bulk load.

HTTP sends NDJSON to POST /<index>/_bulk. psql sends the same rows, already
mapped to the index table's columns, with `\copy es.<index> FROM <file>` in
COPY text format. The COPY files are produced once per request, before
timing, by the endpoint's own mapping (es_bulk), so both paths store identical
rows. Every request carries distinct documents (the table is keyed by _id).

psql uses text COPY, the fastest text-protocol load; `prep` (a prepared
single-row INSERT, pipelined) and `prepbatch` (a prepared 1000-row INSERT)
are there to compare, and are ~7x slower.

The documents are web-access-log shaped (timestamp, client ip, request line,
status, size), like rally's http_logs track. The gzip client sends the same
NDJSON gzip-compressed (compressed before timing).

Each tool reports its own time: curl's `%{time_total}` and psql's `\timing`.
The harness starts its own serened on free ports with a fresh datadir per
path, and also records the server CPU time.

    tests/bench/es/ingest.py --total 512m --chunk 8m
"""

import argparse
import datetime
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
from concurrent.futures import ThreadPoolExecutor

import psycopg2

ROOT = pathlib.Path(__file__).resolve().parents[3]
TICK_MS = 1000 / os.sysconf("SC_CLK_TCK")
UNITS = {"k": 1024, "m": 1024 * 1024, "g": 1024 * 1024 * 1024}
INDEX = "bench"
MAPPING = {
    "mappings": {
        "properties": {
            "@timestamp": {"type": "date"},
            "clientip": {"type": "keyword"},
            "request": {"type": "text"},
            "status": {"type": "integer"},
            "size": {"type": "long"},
        }
    }
}
STATUSES = (200, 200, 200, 200, 304, 404, 500)
VERBS = ("GET", "GET", "GET", "POST", "HEAD")
BASE = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def parse_size(text):
    text = text.strip().lower()
    return int(float(text[:-1]) * UNITS[text[-1]]) if text[-1] in UNITS else int(text)


def human(size):
    for unit, factor in (("GB", UNITS["g"]), ("MB", UNITS["m"]), ("KB", UNITS["k"])):
        if size >= factor:
            return f"{size / factor:.0f} {unit}"
    return f"{size} B"


def make_chunk(first, target):
    """NDJSON of documents numbered from `first` until `target` bytes."""
    out = io.StringIO()
    n = first
    while out.tell() < target:
        doc = {
            "@timestamp": (BASE + datetime.timedelta(seconds=n)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"),
            "clientip": f"10.{(n >> 16) & 255}.{(n >> 8) & 255}.{n & 255}",
            "request": f"{VERBS[n % len(VERBS)]} /images/{(n * 7919) % 9973}.gif HTTP/1.1",
            "status": STATUSES[n % len(STATUSES)],
            "size": (n * 104729) % 65536,
        }
        out.write('{"index":{}}\n')
        out.write(json.dumps(doc, separators=(",", ":")))
        out.write("\n")
        n += 1
    return out.getvalue().encode(), n - first


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


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


class Server:
    def __init__(self, binary):
        self.datadir = tempfile.mkdtemp(prefix="es-bench-")
        self.pg_port, self.http_port = free_port(), free_port()
        listen = (f"postgres://127.0.0.1:{self.pg_port},"
                  f"http://127.0.0.1:{self.http_port}?api=es")
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
        self.curl("PUT", f"/{INDEX}", json.dumps(MAPPING).encode(),
                  "application/json")

    def connect(self):
        conn = psycopg2.connect(host="127.0.0.1", port=self.pg_port,
                                user="postgres", dbname="postgres")
        conn.autocommit = True
        return conn

    def curl(self, method, path, body, content_type):
        out = subprocess.run(
            ["curl", "-sS", "-X", method, "-u", "postgres:", "-H",
             f"Content-Type: {content_type}", "--data-binary", "@-",
             f"http://127.0.0.1:{self.http_port}{path}"],
            input=body, capture_output=True, check=True).stdout
        if b"error" in out[:200]:
            sys.exit(f"{method} {path}: {out[:300]!r}")

    def stop(self):
        self.proc.kill()
        self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)

    def copy_rows(self, payload):
        """The payload's rows in COPY text format, mapped by es_bulk."""
        literal = "'" + payload.decode().replace("'", "''") + "'"
        out = io.BytesIO()
        with self.connect() as conn:
            conn.cursor().copy_expert(
                f"COPY (SELECT * FROM es_bulk('{INDEX}', {literal})) TO STDOUT",
                out)
        return out.getvalue()

    def count(self):
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(f"VACUUM (REFRESH_TABLE) es.{INDEX}")
            cur.execute(f"SELECT count(*) FROM es.{INDEX}")
            return cur.fetchone()[0]


class CurlPath:
    headers = ["-H", "Content-Type: application/x-ndjson"]

    def __init__(self, server, file, payload):
        self.url = f"http://127.0.0.1:{server.http_port}/{INDEX}/_bulk"
        self.file = file
        pathlib.Path(file).write_bytes(self.encode(payload))

    def encode(self, payload):
        return payload

    def send(self):
        out = subprocess.run(
            ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
             "-u", "postgres:", *self.headers, "--data-binary", f"@{self.file}",
             self.url],
            capture_output=True, text=True, check=True).stdout.split()
        if out[0] != "200":
            sys.exit(f"curl: HTTP {out[0]} on {self.url}")
        return float(out[1]) * 1000


class CurlGzipPath(CurlPath):
    headers = CurlPath.headers + ["-H", "Content-Encoding: gzip"]

    def encode(self, payload):
        return gzip.compress(payload)


class PsqlCopyPath:
    def __init__(self, server, file, payload):
        self.port = str(server.pg_port)
        self.file = file
        pathlib.Path(file).write_bytes(server.copy_rows(payload))

    def send(self):
        out = subprocess.run(
            ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
             "-p", self.port, "-U", "postgres", "-d", "postgres",
             "-c", "\\timing on",
             "-c", f"\\copy es.{INDEX} FROM '{self.file}'"],
            capture_output=True, text=True, check=True).stdout
        times = re.findall(r"Time: ([0-9.]+) ms", out)
        if len(times) != 1:
            sys.exit(f"psql printed {len(times)} timings: {out[:300]!r}")
        return float(times[0])


def unescape_copy(field):
    if field == "\\N":
        return None
    if "\\" not in field:
        return field
    out, i = [], 0
    while i < len(field):
        if field[i] == "\\" and i + 1 < len(field):
            out.append({"t": "\t", "n": "\n", "r": "\r"}.get(field[i + 1],
                                                            field[i + 1]))
            i += 2
        else:
            out.append(field[i])
            i += 1
    return "".join(out)


class PreparedInsertPath:
    """One prepared single-row INSERT, bound per row with text parameters and
    pipelined (psycopg 3 executemany)."""

    batch = 1

    def __init__(self, server, file, payload):
        self.port = server.pg_port
        text = server.copy_rows(payload).decode()
        self.rows = [tuple(unescape_copy(f) for f in line.split("\t"))
                     for line in text.splitlines()]
        columns = len(self.rows[0])
        values = ", ".join(
            "(" + ", ".join(f"%s" for _ in range(columns)) + ")"
            for _ in range(self.batch))
        self.sql = f"INSERT INTO es.{INDEX} VALUES {values}"
        self.tail_sql = None
        if self.batch > 1 and len(self.rows) % self.batch:
            tail = len(self.rows) % self.batch
            self.tail_sql = f"INSERT INTO es.{INDEX} VALUES " + ", ".join(
                "(" + ", ".join("%s" for _ in range(columns)) + ")"
                for _ in range(tail))

    def send(self):
        import psycopg

        with psycopg.connect(host="127.0.0.1", port=self.port,
                             user="postgres", dbname="postgres") as conn:
            start = time.perf_counter()
            with conn.cursor() as cur:
                if self.batch == 1:
                    cur.executemany(self.sql, self.rows)
                else:
                    full = len(self.rows) - len(self.rows) % self.batch
                    params = [
                        tuple(v for row in self.rows[i:i + self.batch]
                              for v in row)
                        for i in range(0, full, self.batch)
                    ]
                    cur.executemany(self.sql, params)
                    if self.tail_sql:
                        cur.execute(self.tail_sql, tuple(
                            v for row in self.rows[full:] for v in row),
                                    prepare=True)
            conn.commit()
            return (time.perf_counter() - start) * 1000


class PreparedBatchInsertPath(PreparedInsertPath):
    """A prepared INSERT of `batch` rows per VALUES list."""

    batch = 1000


PATHS = {"curl": CurlPath, "gzip": CurlGzipPath, "psql": PsqlCopyPath,
         "prep": PreparedInsertPath, "prepbatch": PreparedBatchInsertPath}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bin", default=str(ROOT / "build_bench_no_lto/bin/serened"))
    parser.add_argument("--paths", default="curl,gzip,psql",
                        help="which clients to run: curl, gzip, psql (text "
                             "COPY), prep (prepared INSERT per row), prepbatch "
                             "(prepared 1000-row INSERT)")
    parser.add_argument("--total", default="512m", help="NDJSON to ingest, e.g. 1g")
    parser.add_argument("--chunk", default="8m", help="_bulk request size")
    parser.add_argument("--concurrency", default="1",
                        help="parallel senders, e.g. 1,8")
    args = parser.parse_args()

    total, chunk = parse_size(args.total), parse_size(args.chunk)
    chunks, first = [], 0
    for _ in range(math.ceil(total / chunk)):
        payload, docs = make_chunk(first, chunk)
        chunks.append((payload, docs))
        first += docs
    sent = sum(len(p) for p, _ in chunks)
    expected = sum(d for _, d in chunks)

    results = []
    for concurrency in [int(c) for c in args.concurrency.split(",")]:
        for name in args.paths.split(","):
            server = Server(args.bin)
            try:
                senders = [PATHS[name](server, os.path.join(server.datadir, f"req{i}"),
                                       payload)
                           for i, (payload, _) in enumerate(chunks)]
                cpu_before, start = cpu_ms(server.proc.pid), time.perf_counter()
                with ThreadPoolExecutor(concurrency) as pool:
                    times = list(pool.map(lambda s: s.send(), senders))
                wall = time.perf_counter() - start
                cpu = cpu_ms(server.proc.pid) - cpu_before
                rss = peak_rss_mb(server.proc.pid)
                stored = server.count()
            finally:
                server.stop()
            if stored != expected:
                sys.exit(f"{name}: stored {stored} documents, expected {expected}")
            results.append((concurrency, name, len(chunks),
                            sent / UNITS["m"] / wall, stored / wall, wall,
                            cpu / 1000, rss, statistics.median(times),
                            percentile(times, 0.99), max(times)))
            print(f"  done c={concurrency} {name}", file=sys.stderr)

    print(f"\n{human(sent)} of NDJSON ({expected} documents) per run, "
          f"{human(chunk)} _bulk requests (MB/s counts the NDJSON for every path)")
    print(f"{'conc':>4}  {'path':<9} {'chunks':>6} {'MB/s':>7} {'docs/s':>9} "
          f"{'wall s':>7} {'srv CPU s':>9} {'peak RSS MB':>11} "
          f"{'p50 ms':>8} {'p99 ms':>8} {'max ms':>8}")
    for (conc, name, count, mbps, dps, wall, cpu, rss, p50, p99, worst) in results:
        print(f"{conc:>4}  {name:<9} {count:>6} {mbps:7.1f} {dps:9.0f} "
              f"{wall:7.2f} {cpu:9.2f} {rss:11.0f} "
              f"{p50:8.1f} {p99:8.1f} {worst:8.1f}")


if __name__ == "__main__":
    main()
