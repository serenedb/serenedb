#!/usr/bin/env python3
import argparse
import multiprocessing as mp
import os
import random
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time

import duckdb
import psycopg

DUCK_TOKEN = "bench"
SERENE_TOKEN = "postgres:"
COLUMNS = "id BIGINT, i INTEGER, d DOUBLE, s VARCHAR, ts TIMESTAMP, n DECIMAL(12, 2)"
WIDE_ROWS = ("SELECT g, g % 1000, g * 0.5, 'customer-' || g, "
             "TIMESTAMP '2024-01-01' + g * INTERVAL '1 second', "
             "(g % 100000) / 100.0 FROM generate_series(1, {rows}) AS t(g)")
NARROW_ROWS = 10_000_000
DOWNLOADS = (
    ("10M BIGINT", "SELECT v FROM narrow", "max(v)", NARROW_ROWS),
    ("wide table", "SELECT * FROM wide",
     "max(id), max(i), max(d), max(s), max(ts), max(n)", None),
)
SMALL = {
    "SELECT 1": ("SELECT 1", "SELECT 1"),
    "point lookup": ("SELECT * FROM wide WHERE id = {k}",
                     "SELECT * FROM wide WHERE id = %s"),
    "1-row insert": ("INSERT INTO ins VALUES ({k}, 'v')",
                     "INSERT INTO ins VALUES (%s, 'v')"),
}
ATTACHED = {
    "SELECT 1": "FROM r.query('SELECT 1')",
    "point lookup": "SELECT * FROM r.{schema}.wide WHERE id = {k}",
}


def cpu_set(spec: str) -> set[int]:
    cpus = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        cpus.update(range(int(lo), int(hi or lo) + 1))
    return cpus


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_tcp(port: int, proc: subprocess.Popen, what: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            sys.exit(f"{what} exited with {proc.returncode}")
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            return
        except OSError:
            time.sleep(0.05)
    sys.exit(f"{what} did not listen on {port} within 60 s")


def setup_sql(rows: int, uploads: int) -> list[str]:
    return [
        "SET force_compression = 'uncompressed'",
        f"CREATE TABLE wide ({COLUMNS.replace('id BIGINT', 'id BIGINT PRIMARY KEY')})",
        f"INSERT INTO wide {WIDE_ROWS.format(rows=rows)}",
        "CREATE TABLE narrow (v BIGINT)",
        "INSERT INTO narrow SELECT (g * 2654435761) % 1000000007 "
        f"FROM generate_series(1, {NARROW_ROWS}) AS t(g)",
        "CREATE TABLE ins (id BIGINT, v VARCHAR)",
        *(f"CREATE TABLE up_{k} ({COLUMNS})" for k in range(uploads)),
    ]


def duckdb_server(port: str, path: str, threads: str, rows: str, uploads: str,
                  ext: str) -> None:
    con = duckdb.connect(path, config={"extension_directory": ext,
                                       "threads": int(threads)})
    con.execute("LOAD quack")
    for sql in setup_sql(int(rows), int(uploads)):
        con.execute(sql)
    con.execute(f"CALL quack_serve('quack:127.0.0.1:{port}', token := '{DUCK_TOKEN}')")
    print("ready", flush=True)
    sys.stdin.read()


class Target:
    def __init__(self, name: str, kind: str, port: int, token: str = "",
                 schema: str = "public"):
        self.name, self.kind, self.port = name, kind, port
        self.token, self.schema = token, schema

    @property
    def url(self) -> str:
        return f"quack:127.0.0.1:{self.port}"

    @property
    def dsn(self) -> str:
        return f"host=127.0.0.1 port={self.port} user=postgres dbname=postgres"


def quoted(sql: str) -> str:
    return sql.replace("'", "''")


def client(ext: str, threads: int) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(config={"extension_directory": ext, "threads": threads})


def measure_download(con, target: Target, sql: str, aggregate: str,
                     reps: int) -> tuple[list[float], tuple]:
    if target.kind == "pg":
        source = f"postgres_query('pg', '{quoted(sql)}')"
    else:
        source = f"quack_query('{target.url}', '{quoted(sql)}', token := '{target.token}')"
    query = f"SELECT {aggregate} FROM {source}"
    times, result = [], None
    for rep in range(reps + 1):
        start = time.perf_counter()
        result = con.execute(query).fetchone()
        if rep:
            times.append(time.perf_counter() - start)
    return times, result


def measure_upload(args, target: Target, rows: int, first: int) -> list[float]:
    if target.kind == "pg":
        attach = f"ATTACH '{target.dsn}' AS r (TYPE postgres);"
    else:
        attach = f"ATTACH '{target.url}' AS r (TOKEN '{target.token}');"
    script = "\n".join([
        f"SET threads = {len(cpu_set(args.client_cpus))};",
        attach,
        f"CREATE TABLE src AS {WIDE_ROWS.format(rows=rows)};",
        ".timer on",
        *(f"INSERT INTO r.{target.schema}.up_{k} SELECT * FROM src;"
          for k in range(first, first + args.reps + 1)),
    ]) + "\n"
    run = subprocess.run(
        ["taskset", "-c", args.client_cpus, args.serened, "shell", ":memory:", "-bail"],
        input=script, capture_output=True, text=True, timeout=300,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"})
    times = [float(t) for t in re.findall(r"Run Time \(s\): real ([0-9.]+)", run.stdout)]
    if run.returncode or len(times) != args.reps + 1:
        sys.exit(f"upload to {target.name} failed:\n{run.stdout}\n{run.stderr}")
    return times[1:]


def worker(spec: dict, barrier, results) -> None:
    kind, op_name, rows = spec["kind"], spec["op"], spec["rows"]
    quack_sql, pg_sql = SMALL[op_name]
    if kind == "pg":
        conn = psycopg.connect(spec["dsn"], autocommit=True)
        cur = conn.cursor()
        params = "%s" in pg_sql

        def op(k: int) -> None:
            cur.execute(pg_sql, (k,) if params else None, prepare=True)
            if cur.description:
                cur.fetchall()
    else:
        con = client(spec["ext"], 1)
        con.execute("LOAD quack")
        if kind == "attach":
            con.execute(f"ATTACH '{spec['url']}' AS r (TOKEN '{spec['token']}')")
            template = ATTACHED[op_name].format(schema=spec["schema"], k="{k}")
        else:
            template = (f"FROM quack_query('{spec['url']}', '{quoted(quack_sql)}', "
                        f"token := '{spec['token']}')")

        def op(k: int) -> None:
            con.execute(template.format(k=k)).fetchall()

    rng = random.Random(spec["id"])
    serial = spec["id"] * 1_000_000_000

    def key() -> int:
        nonlocal serial
        serial += 1
        return serial if op_name == "1-row insert" else rng.randint(1, rows)

    barrier.wait()
    warm_end = time.perf_counter() + spec["warmup"]
    while time.perf_counter() < warm_end:
        op(key())
    latencies = []
    start = time.perf_counter_ns()
    end = start + int(spec["window"] * 1e9)
    now = start
    while now < end:
        k = key()
        before = time.perf_counter_ns()
        op(k)
        now = time.perf_counter_ns()
        latencies.append(now - before)
    results.put((latencies, (now - start) / 1e9))


def measure_small(args, target: Target, op_name: str, clients: int,
                  kind: str | None = None) -> dict:
    ctx = mp.get_context("spawn")
    barrier, results = ctx.Barrier(clients), ctx.Queue()
    base = {"kind": kind or target.kind, "op": op_name, "rows": args.rows,
            "dsn": target.dsn, "url": target.url, "token": target.token,
            "schema": target.schema, "ext": args.ext, "warmup": args.warmup,
            "window": args.window}
    procs = [ctx.Process(target=worker, args=({**base, "id": i}, barrier, results))
             for i in range(clients)]
    for p in procs:
        p.start()
    done = [results.get(timeout=120) for _ in procs]
    for p in procs:
        p.join(30)
    latencies = sorted(x for lat, _ in done for x in lat)
    return {
        "qps": sum(len(lat) / secs for lat, secs in done),
        "p50_ms": latencies[len(latencies) // 2] / 1e6,
        "p99_ms": latencies[int(len(latencies) * 0.99)] / 1e6,
    }


def table(title: str, header: list[str], rows: list[list]) -> str:
    lines = [f"### {title}", "", "| " + " | ".join(header) + " |",
             "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines) + "\n"


def spread(values: list[float], digits: int = 3) -> str:
    return (f"{statistics.median(values):.{digits}f} "
            f"({min(values):.{digits}f}..{max(values):.{digits}f})")


def variants(targets: list[Target], op_name: str,
             clients: int) -> list[tuple[str, Target, str]]:
    cells = []
    for t in targets:
        cells.append((t.name, t, t.kind))
        if t.kind == "quack" and op_name in ATTACHED and clients == 1:
            cells.append((f"{t.name}, ATTACH", t, "attach"))
    return cells


def main() -> None:
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    parser = argparse.ArgumentParser(
        description="Quack and pg wire on serened against DuckDB's own quack server, "
                    "from a stock DuckDB client (download, small requests) and "
                    "serened shell (upload), over localhost. Both servers store data "
                    "uncompressed, since serened's codecs differ from DuckDB's and would be "
                    "measured instead of the protocol. Needs the duckdb==2.0.0 and "
                    "psycopg packages.")
    parser.add_argument("--serened", default=os.path.join(root, "build_perf/bin/serened"))
    parser.add_argument("--work", help="kept directory for data and logs (default: a removed temp dir)")
    parser.add_argument("--server-cpus", default="8-11,24-27")
    parser.add_argument("--client-cpus", default="12-15,28-31")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--window", type=float, default=2.0)
    parser.add_argument("--warmup", type=float, default=0.3)
    parser.add_argument("--clients", type=int, nargs="+", default=[1, 8])
    parser.add_argument("--runs", type=int, default=1,
                        help="times every measurement repeats; tables show the median and the range")
    args = parser.parse_args()
    if duckdb.__version__ != "2.0.0":
        sys.exit(f"needs the duckdb 2.0.0 package, found {duckdb.__version__}")

    work = args.work or tempfile.mkdtemp(prefix="bench-quack.")
    os.makedirs(work, exist_ok=True)
    args.ext = os.path.join(work, "extensions")
    os.sched_setaffinity(0, cpu_set(args.client_cpus))
    threads = len(cpu_set(args.server_cpus))
    uploads = (args.reps + 1) * args.runs
    started = time.monotonic()
    procs = []
    try:
        con = client(args.ext, len(cpu_set(args.client_cpus)))
        con.execute("INSTALL quack; LOAD quack; INSTALL postgres; LOAD postgres")

        duck_port = free_port()
        duck = subprocess.Popen(
            ["taskset", "-c", args.server_cpus, sys.executable, os.path.abspath(__file__),
             "_duckdb_server", str(duck_port), os.path.join(work, "duck.db"),
             str(threads), str(args.rows), str(uploads), args.ext],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        procs.append(duck)
        if duck.stdout.readline().strip() != "ready":
            sys.exit("the DuckDB server did not start")

        pg_port, quack_port = free_port(), free_port()
        serened = subprocess.Popen(
            ["taskset", "-c", args.server_cpus, args.serened,
             tempfile.mkdtemp(dir=work, prefix="serened."),
             f"--listen=postgres://127.0.0.1:{pg_port},http://127.0.0.1:{quack_port}?api=quack",
             f"--cpu_threads={threads}"],
            stdout=open(os.path.join(work, "serened.log"), "w"), stderr=subprocess.STDOUT)
        procs.append(serened)
        wait_tcp(pg_port, serened, "serened")
        wait_tcp(quack_port, serened, "serened")
        with psycopg.connect(Target("", "pg", pg_port).dsn, autocommit=True) as setup:
            for sql in setup_sql(args.rows, uploads):
                setup.execute(sql)

        targets = [
            Target("A DuckDB quack", "quack", duck_port, DUCK_TOKEN, "main"),
            Target("B serened quack", "quack", quack_port, SERENE_TOKEN),
            Target("D serened pg wire", "pg", pg_port),
        ]
        con.execute(f"ATTACH '{targets[2].dsn}' AS pg (TYPE postgres, READ_ONLY)")
        storage = ("SELECT list(DISTINCT compression ORDER BY compression) FROM pragma_storage_info('wide') "
                   "WHERE segment_type <> 'VALIDITY'")
        for t in targets[:2]:
            stored = con.execute(f"FROM quack_query('{t.url}', '{quoted(storage)}', "
                                 f"token := '{t.token}')").fetchone()[0]
            if stored != ["Uncompressed"]:
                sys.exit(f"{t.name} stores the wide table as {stored}, not uncompressed")
        print(f"setup {time.monotonic() - started:.0f} s, load average {os.getloadavg()[0]:.1f}",
              flush=True)
        report = [f"duckdb {duckdb.__version__}, {args.rows} rows, server CPUs "
                  f"{args.server_cpus}, client CPUs {args.client_cpus}, "
                  f"{threads} server threads, localhost, storage uncompressed, {args.runs} run(s)\n"]

        downloads, uploaded, small = {}, {}, {}
        for run in range(args.runs):
            for name, sql, aggregate, count in DOWNLOADS:
                results = {}
                for t in targets:
                    times, results[t.name] = measure_download(con, t, sql, aggregate, args.reps)
                    downloads.setdefault((name, t.name, count or args.rows), []).extend(times)
                if len({repr(r) for r in results.values()}) != 1:
                    sys.exit(f"{name}: the setups disagree: {results}")
            for t in targets:
                uploaded.setdefault(t.name, []).extend(
                    measure_upload(args, t, args.rows, run * (args.reps + 1)))
            for op_name in SMALL:
                for clients in args.clients if op_name != "1-row insert" else args.clients[:1]:
                    for name, t, kind in variants(targets, op_name, clients):
                        small.setdefault((op_name, clients, name), []).append(
                            measure_small(args, t, op_name, clients, kind))
            print(f"run {run + 1}/{args.runs} done at {time.monotonic() - started:.0f} s, "
                  f"load average {os.getloadavg()[0]:.1f}", flush=True)

        report.append(table(
            "S1 download (stock DuckDB client)",
            ["data", "setup", "seconds, median (min..max)", "M rows/s"],
            [[name, setup, spread(times), f"{n / statistics.median(times) / 1e6:.1f}"]
             for (name, setup, n), times in downloads.items()]))
        report.append(table(
            f"S2 upload, {args.rows} wide rows (serened shell client)",
            ["setup", "seconds, median (min..max)", "M rows/s"],
            [[setup, spread(times), f"{args.rows / statistics.median(times) / 1e6:.2f}"]
             for setup, times in uploaded.items()]))
        report.append(table(
            f"S3/S4 small requests, {args.window:g} s each",
            ["request", "clients", "setup", "QPS, median (min..max)", "p50 ms", "p99 ms"],
            [[op_name, clients, setup, spread([r["qps"] for r in cells], 0),
              f"{statistics.median(r['p50_ms'] for r in cells):.3f}",
              f"{statistics.median(r['p99_ms'] for r in cells):.3f}"]
             for (op_name, clients, setup), cells in small.items()]))
        print("\n".join(report), flush=True)

        report.append(f"total {time.monotonic() - started:.0f} s, load average "
                      f"{os.getloadavg()[0]:.1f}\n")
        print(report[-1])
        with open(os.path.join(work, "report.md"), "w") as f:
            f.write("\n".join(report))
    finally:
        for p in procs:
            if p.stdin:
                p.stdin.close()
            p.terminate()
        for p in procs:
            p.wait(30)
        if not args.work:
            shutil.rmtree(work)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_duckdb_server":
        duckdb_server(*sys.argv[2:])
    else:
        main()
