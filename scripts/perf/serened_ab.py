import argparse
import collections
import json
import math
import multiprocessing
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import time
import urllib.request

import psycopg

COMMANDS = ("count", "top_10", "top_100")
TABLE = "wiki"
INDEX = "wiki_idx"
SQL = {
    "count": f"SELECT count(*) FROM {INDEX} WHERE text @@ to_tsquery(%s)",
    "top_10": f"SELECT id FROM {INDEX} WHERE text @@ to_tsquery(%s) ORDER BY BM25({INDEX}.tableoid) DESC LIMIT 10",
    "top_100": f"SELECT id FROM {INDEX} WHERE text @@ to_tsquery(%s) ORDER BY BM25({INDEX}.tableoid) DESC LIMIT 100",
}


def node_cpus(node):
    with open(f"/sys/devices/system/node/node{node}/cpulist") as f:
        spec = f.read().strip()
    cpus = []
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        cpus.extend(range(int(lo), int(hi or lo) + 1))
    return cpus


def cpu_busy(cpus):
    busy = 0
    with open("/proc/stat") as f:
        for line in f:
            name, *fields = line.split()
            if name.startswith("cpu") and name[3:].isdigit() and int(name[3:]) in cpus:
                values = list(map(int, fields))
                busy += sum(values) - values[3] - values[4]
    return busy


def tree_ticks(pid):
    total = 0
    for task in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{task}/stat") as f:
                fields = f.read().rsplit(")", 1)[1].split()
            total += int(fields[11]) + int(fields[12])
        except OSError:
            pass
    return total


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, binary, datadir, node, log_path, flags=()):
        self.port = free_port()
        self.log = open(log_path, "a")
        cmd = ["numactl", f"--cpunodebind={node}", f"--membind={node}", binary, datadir,
               f"--listen=postgres://127.0.0.1:{self.port}", *flags]
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 900
        while True:
            if self.proc.poll() is not None:
                raise RuntimeError(f"serened exited with {self.proc.returncode}, see {log_path}")
            try:
                with psycopg.connect(self.dsn(), connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    self.stop()
                    raise
                time.sleep(0.5)

    def dsn(self):
        return f"host=127.0.0.1 port={self.port} user=postgres dbname=postgres"

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=600)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


def load_queries(path, limit):
    out = []
    with open(path) as f:
        for line in f:
            out.append(json.loads(line)["query"])
            if limit and len(out) == limit:
                break
    return out


def location_flags(args):
    return [f"--index_location={args.index_location}"] if args.index_location else []


def cmd_prepare(args):
    if os.path.exists(args.data) and os.listdir(args.data):
        sys.exit(f"{args.data} is not empty")
    os.makedirs(args.data, exist_ok=True)
    server = Server(args.serened, args.data, args.node, args.data + ".log", location_flags(args))
    try:
        with psycopg.connect(server.dsn(), autocommit=True) as con:
            if args.s3_endpoint:
                con.execute(f"CREATE PERSISTENT SECRET index_store (TYPE s3, KEY_ID 'key', SECRET 'secret', "
                            f"ENDPOINT '{args.s3_endpoint}', URL_STYLE 'path', USE_SSL false, "
                            f"SCOPE '{args.index_location}')")
            con.execute(f"CREATE TABLE {TABLE} (id VARCHAR, text VARCHAR)")
            con.execute("CREATE TEXT SEARCH DICTIONARY wiki_dict AS split_text(case := 'lower') "
                        "WITH (frequency, position, norm)")
            con.execute(f"CREATE INDEX {INDEX} ON {TABLE} USING inverted (id, text wiki_dict) "
                        "WITH (compaction_interval = 0, optimize_top_k = 'bm25(1.2, 0.75)')")
            if args.segments == 1:
                con.execute(f"INSERT INTO {TABLE} SELECT id, text FROM read_json(%s, "
                            "columns = {'id': 'VARCHAR', 'text': 'VARCHAR'})", (args.corpus,))
                con.execute(f"VACUUM (REFRESH_TABLE) {TABLE}")
                con.execute(f"VACUUM (COMPACT_INDEX) {INDEX}")
            else:
                con.execute("CREATE TABLE staging AS SELECT row_number() OVER () - 1 AS rn, id, text "
                            "FROM read_json(%s, columns = {'id': 'VARCHAR', 'text': 'VARCHAR'})", (args.corpus,))
                total = con.execute("SELECT count(*) FROM staging").fetchone()[0]
                con.execute("SET threads = 1")
                for k in range(args.segments):
                    lo = total * k // args.segments
                    hi = total * (k + 1) // args.segments
                    con.execute(f"INSERT INTO {TABLE} SELECT id, text FROM staging WHERE rn >= {lo} AND rn < {hi} "
                                "ORDER BY rn")
                    con.execute(f"VACUUM (REFRESH_TABLE) {TABLE}")
                con.execute("RESET threads")
                con.execute("DROP TABLE staging")
            con.execute(f"VACUUM (REFRESH_TABLE) {TABLE}")
            rows = dict(con.execute(
                "SELECT m.metric, m.value::BIGINT FROM sdb_metrics m JOIN pg_class c ON c.oid = m.relation_id "
                f"WHERE c.relname = '{INDEX}'").fetchall())
            print(json.dumps({"segments": rows.get("num_segments"), "docs": rows.get("num_live_docs")}))
            con.execute("CHECKPOINT")
    finally:
        server.stop()


def worker(dsn, items, seed, duration, cpus, start, out):
    os.sched_setaffinity(0, cpus)
    rng = random.Random(seed)
    order = list(items)
    rng.shuffle(order)
    latencies = []
    errors = 0
    with psycopg.connect(dsn, autocommit=True) as con:
        start.wait()
        deadline = time.monotonic() + duration
        i = 0
        while time.monotonic() < deadline:
            cmd, q = order[i % len(order)]
            i += 1
            began = time.perf_counter_ns()
            try:
                con.execute(SQL[cmd], (q,)).fetchall()
            except psycopg.Error:
                errors += 1
                continue
            latencies.append(time.perf_counter_ns() - began)
    out.put((latencies, errors))


def valid_items(dsn, queries, commands):
    items = []
    rejected = 0
    with psycopg.connect(dsn, autocommit=True) as con:
        for q in queries:
            try:
                for cmd in commands:
                    con.execute(SQL[cmd], (q,)).fetchall()
            except psycopg.Error:
                rejected += 1
                continue
            items.extend((cmd, q) for cmd in commands)
    return items, rejected


def run_level(server, items, sessions, duration, client_cpus, server_cpus):
    ctx = multiprocessing.get_context("fork")
    start = ctx.Barrier(sessions + 1)
    out = ctx.Queue()
    procs = [ctx.Process(target=worker, args=(server.dsn(), items, s, duration, client_cpus, start, out))
             for s in range(sessions)]
    for p in procs:
        p.start()
    start.wait()
    busy_before = cpu_busy(server_cpus)
    own_before = tree_ticks(server.proc.pid)
    began = time.monotonic()
    results = [out.get() for _ in procs]
    elapsed = time.monotonic() - began
    for p in procs:
        p.join()
    hz = os.sysconf("SC_CLK_TCK")
    foreign_ticks = (cpu_busy(server_cpus) - busy_before) - (tree_ticks(server.proc.pid) - own_before)
    latencies = sorted(x for r in results for x in r[0])
    errors = sum(r[1] for r in results)
    return {
        "sessions": sessions,
        "queries": len(latencies),
        "errors": errors,
        "qps": len(latencies) / elapsed,
        "p50_ms": latencies[len(latencies) // 2] / 1e6 if latencies else math.nan,
        "p99_ms": latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))] / 1e6 if latencies else math.nan,
        "foreign_load": max(0.0, foreign_ticks / hz) / (len(server_cpus) * elapsed),
    }


def cmd_run(args):
    queries = load_queries(args.queries, args.limit)
    commands = args.commands.split(",")
    levels = [int(x) for x in args.sessions.split(",")]
    server_cpus = node_cpus(args.node)
    client_cpus = node_cpus(args.client_node)
    builds = [("base", args.base), ("test", args.test)]
    items = None
    with open(args.out, "a") as out:
        for pair in range(args.pairs):
            for name, binary in (builds if pair % 2 == 0 else builds[::-1]):
                server = Server(binary, args.data, args.node, args.out + f".{name}.log")
                try:
                    if items is None:
                        items, rejected = valid_items(server.dsn(), queries, commands)
                        print(f"{len(items)} statements, {rejected} queries rejected", file=sys.stderr)
                        if not items:
                            sys.exit("no query runs on the base server")
                    else:
                        valid_items(server.dsn(), queries[: args.warm], commands)
                    for sessions in levels:
                        result = run_level(server, items, sessions, args.duration, client_cpus, server_cpus)
                        result.update(pair=pair, build=name)
                        print(json.dumps(result), file=out, flush=True)
                        print(json.dumps(result), file=sys.stderr)
                finally:
                    server.stop()


def store_admin(url, method, path):
    request = urllib.request.Request(f"{url}/__admin/{path}", method=method, data=b"" if method != "GET" else None)
    with urllib.request.urlopen(request) as response:
        body = response.read()
    return json.loads(body) if body else None


def store_requests(url):
    return collections.Counter(store_admin(url, "GET", "stats")["requests"])


def timed_statement(con, cmd, query):
    began = time.perf_counter_ns()
    con.execute(SQL[cmd], (query,)).fetchall()
    return (time.perf_counter_ns() - began) / 1e6


def cmd_remote(args):
    queries = load_queries(args.queries, args.limit)
    commands = args.commands.split(",")
    store = args.s3_admin
    if store:
        store_admin(store, "POST", "reset")
        store_admin(store, "PUT", f"latency?seconds={args.latency}")
    began = time.monotonic()
    server = Server(args.serened, args.data, args.node, args.out + ".log", location_flags(args))
    with open(args.out, "a") as out:
        try:
            result = {"phase": "open", "ms": (time.monotonic() - began) * 1000}
            if store:
                result["requests"] = dict(store_requests(store))
            print(json.dumps(result), file=out, flush=True)
            with psycopg.connect(server.dsn(), autocommit=True) as con:
                items = [(cmd, q) for q in queries for cmd in commands]
                for i, (cmd, q) in enumerate(items):
                    con.execute("SET GLOBAL enable_external_file_cache = false")
                    con.execute("SET GLOBAL enable_external_file_cache = true")
                    before = store_requests(store) if store else None
                    try:
                        ms = timed_statement(con, cmd, q)
                    except psycopg.Error as e:
                        print(json.dumps({"phase": "cold", "cmd": cmd, "item": i, "error": str(e)}), file=out)
                        continue
                    result = {"phase": "cold", "cmd": cmd, "item": i, "ms": ms}
                    if store:
                        result["requests"] = dict(store_requests(store) - before)
                    print(json.dumps(result), file=out, flush=True)
                for cmd, q in items:
                    con.execute(SQL[cmd], (q,)).fetchall()
                before = store_requests(store) if store else None
                for _ in range(args.repeats):
                    for i, (cmd, q) in enumerate(items):
                        try:
                            ms = timed_statement(con, cmd, q)
                        except psycopg.Error as e:
                            print(json.dumps({"phase": "warm", "cmd": cmd, "item": i, "error": str(e)}), file=out)
                            continue
                        print(json.dumps({"phase": "warm", "cmd": cmd, "item": i, "ms": ms}), file=out, flush=True)
                if store:
                    print(json.dumps({"phase": "warm", "statements": args.repeats * len(items),
                                      "requests": dict(store_requests(store) - before)}), file=out, flush=True)
        finally:
            server.stop()
    if store:
        store_admin(store, "PUT", "latency?seconds=0")


def cmd_remote_summary(args):
    rows = collections.defaultdict(list)
    for path in args.results:
        with open(path) as f:
            for line in f:
                r = json.loads(line)
                if "error" in r:
                    continue
                rows[(path, r["phase"], r.get("cmd", ""))].append(r)
    for (path, phase, cmd), runs in sorted(rows.items()):
        requests = collections.Counter()
        for r in runs:
            requests.update(r.get("requests", {}))
        ms = [r["ms"] for r in runs if "ms" in r]
        statements = len(ms) or sum(r.get("statements", 0) for r in runs)
        row = {"file": os.path.basename(path), "phase": phase, "cmd": cmd, "n": len(ms),
               "requests_per_run": {k: round(v / statements, 2) for k, v in sorted(requests.items())}}
        if ms:
            row.update(p50_ms=round(statistics.median(ms), 2), p95_ms=round(pct(ms, 0.95), 2))
        print(json.dumps(row))


def item_medians(path, phase):
    runs = collections.defaultdict(list)
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            if r["phase"] == phase and "ms" in r:
                runs[(r["cmd"], r["item"])].append(r["ms"])
    return {k: statistics.median(v) for k, v in runs.items()}


def cmd_remote_compare(args):
    base = item_medians(args.base, args.phase)
    test = item_medians(args.test, args.phase)
    for cmd in sorted({k[0] for k in base}):
        keys = [k for k in base if k[0] == cmd and k in test]
        ratios = [test[k] / base[k] for k in keys]
        print(json.dumps({
            "phase": args.phase, "cmd": cmd, "items": len(keys),
            "base_p50_ms": round(statistics.median(base[k] for k in keys), 3),
            "test_p50_ms": round(statistics.median(test[k] for k in keys), 3),
            "geomean_ratio": round(math.exp(statistics.fmean(math.log(r) for r in ratios)), 4),
            "p95_ratio": round(pct(ratios, 0.95), 4),
        }))


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))]


def cmd_compare(args):
    runs = {}
    with open(args.results) as f:
        for line in f:
            r = json.loads(line)
            if r["foreign_load"] > args.max_foreign_load or r["errors"]:
                continue
            runs.setdefault((r["sessions"], r["build"]), []).append(r)
    for sessions in sorted({k[0] for k in runs}):
        base = runs.get((sessions, "base"), [])
        test = runs.get((sessions, "test"), [])
        if not base or not test:
            continue
        row = {"sessions": sessions, "pairs": min(len(base), len(test))}
        for metric in ("qps", "p99_ms"):
            b = statistics.median(r[metric] for r in base)
            t = statistics.median(r[metric] for r in test)
            spread = [r[metric] for r in base]
            row[metric] = {"base": round(b, 3), "test": round(t, 3), "ratio": round(t / b, 4),
                           "base_band": round((max(spread) - min(spread)) / b, 4)}
        print(json.dumps(row))


def main():
    parser = argparse.ArgumentParser(description="Concurrent search sessions over pgwire, serened against serened")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prepare", help="load the corpus into a fresh data directory")
    p.add_argument("--serened", required=True)
    p.add_argument("--corpus", required=True)
    p.add_argument("--data", required=True)
    p.add_argument("--segments", type=int, required=True)
    p.add_argument("--node", type=int, default=0)
    p.add_argument("--index-location", default="", help="--index_location of the server, e.g. s3://bucket/wiki")
    p.add_argument("--s3-endpoint", default="", help="host:port of the S3 endpoint for an s3:// location")
    p.set_defaults(func=cmd_prepare)
    m = sub.add_parser("remote", help="open, cold and warm statement latency of one build, with store requests")
    m.add_argument("--serened", required=True)
    m.add_argument("--data", required=True)
    m.add_argument("--queries", required=True)
    m.add_argument("--out", required=True)
    m.add_argument("--index-location", default="")
    m.add_argument("--s3-admin", default="", help="http://host:port of the fake S3 endpoint, for its counters")
    m.add_argument("--latency", type=float, default=0.03, help="seconds the store adds to every request")
    m.add_argument("--commands", default="top_10")
    m.add_argument("--limit", type=int, default=100)
    m.add_argument("--repeats", type=int, default=5, help="timed passes over the warm statements")
    m.add_argument("--node", type=int, default=0)
    m.set_defaults(func=cmd_remote)
    s = sub.add_parser("remote-summary", help="medians and requests per statement of remote runs")
    s.add_argument("results", nargs="+")
    s.set_defaults(func=cmd_remote_summary)
    rc = sub.add_parser("remote-compare", help="per-statement latency ratios of two remote runs")
    rc.add_argument("base")
    rc.add_argument("test")
    rc.add_argument("--phase", default="warm")
    rc.set_defaults(func=cmd_remote_compare)
    r = sub.add_parser("run", help="alternate two serened builds over one data directory")
    r.add_argument("--base", required=True)
    r.add_argument("--test", required=True)
    r.add_argument("--data", required=True)
    r.add_argument("--queries", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--commands", default=",".join(COMMANDS))
    r.add_argument("--sessions", default="1,8,16,32")
    r.add_argument("--duration", type=float, default=30)
    r.add_argument("--pairs", type=int, default=5)
    r.add_argument("--limit", type=int, default=0)
    r.add_argument("--warm", type=int, default=100)
    r.add_argument("--node", type=int, default=0)
    r.add_argument("--client-node", type=int, default=1)
    r.set_defaults(func=cmd_run)
    c = sub.add_parser("compare", help="medians and ratios per session count")
    c.add_argument("results")
    c.add_argument("--max-foreign-load", type=float, default=0.05)
    c.set_defaults(func=cmd_compare)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
