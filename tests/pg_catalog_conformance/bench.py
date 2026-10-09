#!/usr/bin/env python3
import argparse
import json
import os
import re
import statistics
import subprocess
import time

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
TEST = os.path.normpath(os.path.join(HERE, "..", "sqllogic", "any", "pg", "system", "catalog_conformance.test"))
HZ = os.sysconf("SC_CLK_TCK")
SCHEMA = "catbench"
MARKER = f"{SCHEMA}.catbench_done"


class QueryError(Exception):
    pass


class Result:
    def __init__(self, rows):
        self.rows = rows


def parse_endpoint(text):
    m = re.fullmatch(r"(?:(?P<user>[^@]+)@)?(?P<host>[^:]+):(?P<port>\d+)(?:/(?P<db>.+))?", text)
    if not m:
        raise ValueError(f"bad endpoint '{text}', expected [user@]host:port[/admin_db]")
    return {"user": m["user"] or "postgres", "host": m["host"], "port": int(m["port"]), "db": m["db"] or "postgres"}


def connect(endpoint, dbname):
    conn = psycopg.connect(host=endpoint["host"], port=endpoint["port"], user=endpoint["user"], dbname=dbname,
                           autocommit=True, connect_timeout=30)
    conn.prepare_threshold = None
    return conn


def connect_retry(endpoint, dbname):
    last = None
    for _ in range(20):
        try:
            return connect(endpoint, dbname)
        except psycopg.OperationalError as e:
            last = e
            time.sleep(0.25)
    raise last


class Session:
    def __init__(self, conn):
        self.conn = conn

    def exec(self, sql):
        res = self.conn.pgconn.exec_(sql.encode())
        status = res.status
        if status == psycopg.pq.ExecStatus.TUPLES_OK:
            rows = []
            for r in range(res.ntuples):
                row = []
                for c in range(res.nfields):
                    v = res.get_value(r, c)
                    row.append(None if v is None else v.decode(errors="replace"))
                rows.append(tuple(row))
            return Result(rows)
        if status in (psycopg.pq.ExecStatus.COMMAND_OK, psycopg.pq.ExecStatus.EMPTY_QUERY):
            return Result([])
        msg = (res.error_message or b"").decode(errors="replace").strip()
        raise QueryError(" ".join(msg.split()) or f"status {status.name}")

    def try_exec(self, sql):
        try:
            return self.exec(sql), None
        except QueryError as e:
            return None, str(e)
        except psycopg.OperationalError as e:
            return None, f"connection error: {' '.join(str(e).split())}"

    def scalar(self, sql):
        res = self.exec(sql)
        return res.rows[0][0] if res.rows else None

    def close(self):
        self.conn.close()


def parse_args():
    ap = argparse.ArgumentParser(description="Catalog query latency bench at N tables, with a CPU-quietness check.")
    ap.add_argument("--target", action="append", required=True, help="name=[user@]host:port, repeatable (e.g. sdb=127.0.0.1:7861 pg=127.0.0.1:55433)")
    ap.add_argument("--tables", default="1000,10000", help="comma-separated catalog sizes")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--budget-ms", type=float, default=400.0, help="wall time per run; a run is never fewer than --min-iters executions")
    ap.add_argument("--min-iters", type=int, default=3)
    ap.add_argument("--max-foreign-cores", type=float, default=2.0, help="flag runs where other processes used more cores than this")
    ap.add_argument("--server-comm", action="append", default=[], help="name=comm: processes named comm count as that target's server (e.g. pg=postgres for a docker reference)")
    ap.add_argument("--filter", default=None, help="regex over query names (line number and first catalog) and SQL")
    ap.add_argument("--explain-floor", type=int, default=1000, help="scan rows always accepted by the EXPLAIN ANALYZE check")
    ap.add_argument("--explain-factor", type=float, default=4.0, help="scan rows accepted per result row")
    ap.add_argument("--regenerate", action="store_true", help="drop and rebuild the bench databases")
    ap.add_argument("--test", default=TEST, help="sqllogic file whose statements are the setup and whose queries are timed")
    ap.add_argument("--json", default=None)
    return ap.parse_args()


def parse_test(path):
    records = []
    lines = open(path).read().split("\n")
    i, skip, only = 0, set(), set()
    while i < len(lines):
        line = lines[i]
        words = line.split()
        if words[:1] in (["skipif"], ["onlyif"]) and len(words) > 1:
            (skip if words[0] == "skipif" else only).add(words[1])
            i += 1
            continue
        if words[:1] not in (["statement"], ["query"]):
            if not line.strip():
                skip, only = set(), set()
            i += 1
            continue
        start = i + 1
        i += 1
        sql = []
        while i < len(lines) and lines[i].strip() and lines[i] != "----":
            sql.append(lines[i])
            i += 1
        if i < len(lines) and lines[i] == "----":
            i += 1
            while i < len(lines) and lines[i].strip():
                i += 1
        records.append({"kind": words[0], "error": words[1:2] == ["error"], "sql": "\n".join(sql), "line": start,
                        "skip": skip, "only": only})
        skip, only = set(), set()
    return records


def applies(record, labels):
    return not (record["skip"] & labels) and (not record["only"] or record["only"] & labels)


def query_name(record):
    m = (re.search(r"\bFROM\s+((?:pg_catalog|information_schema)\.\w+|pg_\w+)", record["sql"], re.I)
         or re.search(r"\b(pg_catalog\.\w+|information_schema\.\w+|pg_[a-z_]+)", record["sql"]))
    label = m.group(1).split(".")[-1] if m else " ".join(record["sql"].split())[:24]
    return f"L{record['line']} {label}"


def load_test(path, flt):
    records = parse_test(path)
    queries = [r for r in records if r["kind"] == "query"]
    first = records.index(queries[0]) if queries else len(records)
    last = records.index(queries[-1]) if queries else len(records)
    setup = [r for r in records[:first] if r["kind"] == "statement"]
    middle = [r for r in records[first:last] if r["kind"] == "statement"]
    teardown = [r for r in records[last:] if r["kind"] == "statement"]
    out = []
    for q in queries:
        name = query_name(q)
        if flt and not re.search(flt, f"{name}\n{q['sql']}"):
            continue
        out.append({"name": name, "sql": q["sql"], "skip": q["skip"], "only": q["only"]})
    return setup + middle, teardown, out


def schema_statements(n):
    out = [f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"]
    for i in range(n):
        out.append(f"CREATE TABLE {SCHEMA}.t{i} (id bigint PRIMARY KEY, name varchar(50) NOT NULL, descr text, amount numeric(10,2), "
                   f"created timestamp DEFAULT now(), flag boolean, cnt integer DEFAULT 0, code text)")
        out.append(f"CREATE INDEX t{i}_name_idx ON {SCHEMA}.t{i} (name)")
        if i % 10 == 0:
            out.append(f"COMMENT ON TABLE {SCHEMA}.t{i} IS 'table {i}'")
            out.append(f"COMMENT ON COLUMN {SCHEMA}.t{i}.name IS 'name of {i}'")
        if i % 5 == 0:
            out.append(f"CREATE VIEW {SCHEMA}.v{i} AS SELECT id, name, amount FROM {SCHEMA}.t{i} WHERE flag")
        if i % 20 == 0:
            out.append(f"CREATE SEQUENCE {SCHEMA}.seq{i}")
    out.append(f"CREATE TABLE {MARKER} (n int)")
    return out


def ensure_database(endpoint, n, regenerate, log):
    name = f"catbench_{n}"
    admin = Session(connect(endpoint, endpoint["db"]))
    exists = admin.scalar(f"SELECT count(*) FROM pg_catalog.pg_database WHERE datname = '{name}'") not in (None, "0")
    if exists and not regenerate:
        s = Session(connect_retry(endpoint, name))
        _, err = s.try_exec(f"SELECT 1 FROM {MARKER}")
        if not err:
            admin.close()
            return s
        s.close()
    if exists:
        admin.exec(f"DROP DATABASE {name}")
    admin.exec(f"CREATE DATABASE {name}")
    admin.close()
    s = Session(connect_retry(endpoint, name))
    stmts = schema_statements(n)
    t0 = time.time()
    for i in range(0, len(stmts), 200):
        batch = stmts[i:i + 200]
        _, err = s.try_exec(";\n".join(batch))
        if err:
            for stmt in batch:
                s.exec(stmt)
        if i and i % 4000 == 0:
            log(f"    {i}/{len(stmts)} statements, {time.time() - t0:.0f}s")
    log(f"    generated {n} tables in {time.time() - t0:.0f}s")
    return s


def engine_labels(session):
    version = session.scalar("SELECT version()") or ""
    return {"serenedb" if "SereneDB" in version else "postgres", "pg-wire-simple"}


def run_statements(session, records, labels, strict):
    for r in records:
        if not applies(r, labels):
            continue
        _, err = session.try_exec(r["sql"])
        if strict and bool(err) != r["error"]:
            raise QueryError(f"line {r['line']}: " + (err or "expected an error"))


def cpu_busy():
    with open("/proc/stat") as f:
        vals = list(map(int, f.readline().split()[1:]))
    return sum(vals) - vals[3] - vals[4]


def proc_jiffies(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return int(fields[11]) + int(fields[12]) + int(fields[13]) + int(fields[14])
    except (FileNotFoundError, ProcessLookupError, IndexError):
        return 0


def listen_pid(port):
    out = subprocess.run(["lsof", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def server_pids(target, comms):
    pids = set()
    if target["pid"]:
        pids.add(target["pid"])
    if comms:
        for p in os.listdir("/proc"):
            if not p.isdigit():
                continue
            try:
                with open(f"/proc/{p}/comm") as f:
                    if f.read().strip() in comms:
                        pids.add(int(p))
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                pass
    return pids


def server_jiffies(pids):
    return sum(proc_jiffies(p) for p in pids)


def client_jiffies():
    t = os.times()
    return (t.user + t.system) * HZ


def run_once(session, sql, iters, pids):
    busy0, srv0, cli0 = cpu_busy(), server_jiffies(pids), client_jiffies()
    t0 = time.perf_counter()
    for _ in range(iters):
        session.exec(sql)
    wall = time.perf_counter() - t0
    busy1, srv1, cli1 = cpu_busy(), server_jiffies(pids), client_jiffies()
    foreign = max(0.0, (busy1 - busy0) - (srv1 - srv0) - (cli1 - cli0)) / HZ / max(wall, 1e-6)
    return wall / iters * 1000.0, foreign


def flatten(v):
    return " AND ".join(flatten(x) for x in v) if isinstance(v, list) else " ".join(str(v).split())


def explain_scans(session, sql):
    res, err = session.try_exec(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}")
    if err or not res.rows:
        return None, err
    try:
        plan = json.loads("\n".join(r[-1] for r in res.rows))
    except ValueError:
        return None, "unparsable EXPLAIN output"
    scans = []

    def walk(node):
        info = node.get("extra_info") or {}
        if isinstance(info, dict) and info.get("Function") == "SYSTEM_TABLE_SCAN":
            proj = info.get("Projections", "")
            scans.append({"projections": ", ".join(proj) if isinstance(proj, list) else " ".join(str(proj).split())[:60], "rows": int(node.get("intermediate_rows") or 0),
                          "filters": " AND ".join(flatten(v) for k, v in info.items() if "Filter" in k)[:100]})
        for c in node.get("children") or []:
            walk(c)

    if isinstance(plan, dict):
        for op in plan.get("operator") or plan.get("children") or []:
            walk(op)
    return scans, None


def bench_size(args, targets, n, setup, teardown, queries, log):
    sessions, labels = {}, {}
    for t in targets:
        log(f"  [{t['name']}] preparing catbench_{n}")
        s = ensure_database(t["endpoint"], n, args.regenerate, log)
        sessions[t["name"]] = s
        labels[t["name"]] = engine_labels(s)
        run_statements(s, teardown, labels[t["name"]], False)
        run_statements(s, setup, labels[t["name"]], True)
        t["pids"] = server_pids(t, [c.split("=", 1)[1] for c in args.server_comm if c.split("=", 1)[0] == t["name"]])
    iters, errors = {}, {}
    for q in queries:
        for t in targets:
            key = (q["name"], t["name"])
            if not applies(q, labels[t["name"]]):
                errors[key] = "skipped for this engine"
                continue
            res, err = sessions[t["name"]].try_exec(q["sql"])
            if err:
                errors[key] = err
                continue
            try:
                lat, _ = run_once(sessions[t["name"]], q["sql"], 1, t["pids"])
            except QueryError as e:
                errors[key] = str(e)
                continue
            iters[key] = max(args.min_iters, min(2000, int(args.budget_ms / max(lat, 0.01))))
    runs = {}
    for r in range(args.rounds):
        order = targets if r % 2 == 0 else list(reversed(targets))
        log(f"  round {r + 1}/{args.rounds}")
        for q in queries:
            for t in order:
                key = (q["name"], t["name"])
                if key in errors:
                    continue
                try:
                    runs.setdefault(key, []).append(run_once(sessions[t["name"]], q["sql"], iters[key], t["pids"]))
                except QueryError as e:
                    errors[key] = f"round {r + 1}: {e}"
                    runs.pop(key, None)
    explain = []
    for t in targets:
        for q in queries:
            if (q["name"], t["name"]) in errors:
                continue
            scans, err = explain_scans(sessions[t["name"]], q["sql"])
            if not scans:
                continue
            res, err = sessions[t["name"]].try_exec(q["sql"])
            if err:
                continue
            result_rows = len(res.rows)
            limit = max(args.explain_floor, args.explain_factor * result_rows)
            for s in scans:
                explain.append({"target": t["name"], "query": q["name"], "result_rows": result_rows, "scan_rows": s["rows"],
                                "projections": s["projections"], "filters": s["filters"], "ok": s["rows"] <= limit})
    for t in targets:
        s = sessions[t["name"]]
        run_statements(s, teardown, labels[t["name"]], False)
        s.close()
    return runs, errors, explain


def fmt_cell(xs):
    lats = [x[0] for x in xs]
    return f"{statistics.median(lats):.3f} ({min(lats):.3f}-{max(lats):.3f})"


def report(args, targets, results):
    lines = []
    out = {"targets": {t["name"]: f"{t['endpoint']['host']}:{t['endpoint']['port']}" for t in targets}, "sizes": {}}
    for n, (runs, errors, explain) in results.items():
        lines.append(f"\n== {n} tables: latency per query in ms, median (min-max) over {args.rounds} rounds")
        hdr = "".join(f"{t['name']:>30}" for t in targets)
        lines.append(f"{'query':32}{hdr}  {'foreign cores (max)':>20}")
        flagged = 0
        all_foreign = []
        size = {"queries": {}, "errors": {f"{q}/{t}": e for (q, t), e in errors.items()}, "explain": explain}
        names = list(dict.fromkeys([k[0] for k in runs] + [k[0] for k in errors]))
        for qname in names:
            cells, worst = "", 0.0
            qout = {}
            for t in targets:
                key = (qname, t["name"])
                if key in errors:
                    cells += f"{'SKIP' if errors[key] == 'skipped for this engine' else 'ERROR':>30}"
                    continue
                xs = runs[key]
                foreign = [x[1] for x in xs]
                all_foreign += foreign
                worst = max([worst] + foreign)
                noisy = sum(1 for f in foreign if f > args.max_foreign_cores)
                flagged += noisy
                cells += f"{fmt_cell(xs) + ('*' if noisy else ''):>30}"
                lats = [x[0] for x in xs]
                qout[t["name"]] = {"median_ms": statistics.median(lats), "min_ms": min(lats), "max_ms": max(lats),
                                   "runs": [{"ms": x[0], "foreign_cores": x[1]} for x in xs], "noisy_runs": noisy}
            size["queries"][qname] = qout
            lines.append(f"{qname[:31]:32}{cells}  {worst:20.2f}")
        quiet = flagged == 0
        lines.append(f"quiet check: foreign CPU per run max {max(all_foreign or [0]):.2f} cores, median {statistics.median(all_foreign or [0]):.2f}; "
                     f"{flagged} runs above {args.max_foreign_cores} cores" + ("" if quiet else " (marked *; box was NOT quiet, rerun)"))
        for (q, t), e in errors.items():
            if e != "skipped for this engine":
                lines.append(f"error {t} {q}: {e[:160]}")
        if explain:
            bad = [e for e in explain if not e["ok"]]
            lines.append(f"EXPLAIN ANALYZE: {len(explain)} system-table scans checked, {len(bad)} emit more than "
                         f"max({args.explain_floor}, {args.explain_factor:g} x result rows)")
            for e in explain:
                lines.append(f"  {'OK  ' if e['ok'] else 'FAIL'} {e['target']} {e['query']}: scan rows {e['scan_rows']} for {e['result_rows']} result rows"
                             f" [{e['projections']}]" + (f" filter {e['filters']}" if e["filters"] else ""))
        size["quiet"] = quiet
        out["sizes"][str(n)] = size
    print("\n".join(lines))
    if args.json:
        with open(args.json, "w") as f:
            json.dump(out, f, indent=1)


def main():
    args = parse_args()
    targets = []
    for spec in args.target:
        name, ep = spec.split("=", 1)
        endpoint = parse_endpoint(ep)
        targets.append({"name": name, "endpoint": endpoint, "pid": listen_pid(endpoint["port"])})
    setup, teardown, queries = load_test(args.test, args.filter)
    results = {}

    def log(msg):
        print(msg, flush=True)

    for n in [int(x) for x in args.tables.split(",")]:
        log(f"bench at {n} tables")
        results[n] = bench_size(args, targets, n, setup, teardown, queries, log)
    report(args, targets, results)


if __name__ == "__main__":
    main()
