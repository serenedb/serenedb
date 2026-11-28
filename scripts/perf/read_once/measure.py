#!/usr/bin/env python3
import argparse
import array
import ctypes
import ctypes.util
import json
import mmap
import os
import platform
import re
import shutil
import signal
import statistics
import struct
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg

HEADER = struct.Struct("<II")
U32 = struct.Struct("<I")
SNAPSHOT_HEADER = struct.Struct("<QQQQQ")
SEGMENT_SUFFIXES = (".col", ".idx", ".doc", ".pos", ".pay")
FOOTER_SUFFIXES = (".col", ".idx")
SIZE_BUCKETS = [
    (1 << 10, "lt_1k"),
    (16 << 10, "lt_16k"),
    (256 << 10, "lt_256k"),
    (1 << 20, "lt_1m"),
    (4 << 20, "lt_4m"),
    (None, "ge_4m"),
]
LOG_METRICS = (
    "catalog_wal_appended_bytes",
    "catalog_wal_size_on_disk",
    "catalog_wal_since_snapshot_bytes",
    "catalog_history_bytes",
)
HITS_INDEXED = ("URL", "Title", "SearchPhrase", "Referer", "UserID", "CounterID")
LOGGED_AT = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)\+00")
LOGGED_START = "starting SereneDB"
LOGGED_READY = "ready for business"
PAGE = os.sysconf("SC_PAGE_SIZE")

LIBC = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
LIBC.mmap.restype = ctypes.c_void_p
LIBC.mmap.argtypes = [
    ctypes.c_void_p,
    ctypes.c_size_t,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_long,
]
LIBC.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
LIBC.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
MAP_FAILED = ctypes.c_void_p(-1).value

LIVE = []


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def data_files(root):
    return sorted(p for p in Path(root).rglob("*") if p.is_file())


def tree_bytes(root):
    return sum(p.stat().st_size for p in data_files(root))


def resident_bytes(path):
    size = path.stat().st_size
    if size == 0:
        return 0
    fd = os.open(path, os.O_RDONLY)
    try:
        addr = LIBC.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
        if addr in (None, MAP_FAILED):
            raise OSError(ctypes.get_errno(), f"mmap {path}")
        try:
            pages = ctypes.create_string_buffer((size + PAGE - 1) // PAGE)
            if LIBC.mincore(addr, size, pages) != 0:
                raise OSError(ctypes.get_errno(), f"mincore {path}")
            return (len(pages.raw) - pages.raw.count(0)) * PAGE
        finally:
            LIBC.munmap(addr, size)
    finally:
        os.close(fd)


def drop_cache(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def evict(root):
    for path in data_files(root):
        drop_cache(path)
    return {
        "bytes": tree_bytes(root),
        "resident_bytes": sum(resident_bytes(p) for p in data_files(root)),
    }


def warm(path):
    with open(path, "rb", buffering=0) as f:
        while f.read(16 << 20):
            pass
    return {"bytes": path.stat().st_size, "resident_bytes": resident_bytes(path)}


def free_port():
    for port in range(7400, 7600):
        listening = subprocess.run(
            ["ss", "-ltnH", f"sport = :{port}"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if not listening:
            return port
    raise RuntimeError("no free port in 7400-7599")


def proc_read_bytes(pid):
    with open(f"/proc/{pid}/io") as io:
        for line in io:
            if line.startswith("read_bytes:"):
                return int(line.split()[1])
    raise RuntimeError(f"/proc/{pid}/io has no read_bytes")


def proc_cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as stat:
        fields = stat.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def proc_peak_rss_bytes(pid):
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError(f"/proc/{pid}/status has no VmHWM")


def logged_ready_s(log_path, timeout):
    begin = time.monotonic()
    while True:
        start = ready = None
        for line in log_path.read_text(errors="replace").splitlines():
            if start is None and LOGGED_START in line:
                start = datetime.fromisoformat(LOGGED_AT.search(line).group(1))
            elif ready is None and LOGGED_READY in line:
                ready = datetime.fromisoformat(LOGGED_AT.search(line).group(1))
        if start and ready:
            return (ready - start).total_seconds()
        if time.monotonic() - begin > timeout:
            raise RuntimeError(f"{log_path} has no '{LOGGED_START}' and '{LOGGED_READY}' lines")
        time.sleep(0.001)


def latency(samples):
    ms = sorted(s * 1000 for s in samples)
    if not ms:
        return {}
    cuts = statistics.quantiles(ms, n=100, method="inclusive") if len(ms) > 1 else ms * 99
    return {
        "count": len(ms),
        "mean_ms": statistics.fmean(ms),
        "p50_ms": cuts[49],
        "p95_ms": cuts[94],
        "p99_ms": cuts[98],
        "max_ms": ms[-1],
        "total_s": sum(ms) / 1000,
    }


class Server:
    def __init__(self, binary, datadir, logs, timeout):
        self.binary = binary
        self.datadir = datadir
        self.logs = logs
        self.timeout = timeout
        self.proc = None
        self.conn = None
        self.starts = 0

    def start(self, tag):
        self.starts += 1
        port = free_port()
        log_path = self.logs / f"{self.datadir.name}-{self.starts:02d}-{tag}.log"
        begin = time.monotonic()
        with open(log_path, "w") as out:
            self.proc = subprocess.Popen(
                [
                    str(self.binary),
                    str(self.datadir),
                    f"--listen=postgres://127.0.0.1:{port}",
                ],
                stdout=out,
                stderr=subprocess.STDOUT,
            )
        LIVE.append(self)
        conninfo = f"host=127.0.0.1 port={port} user=postgres dbname=postgres"
        while True:
            if self.proc.poll() is not None:
                raise RuntimeError(f"serened exited with {self.proc.returncode}, see {log_path}")
            try:
                self.conn = psycopg.connect(conninfo, autocommit=True, connect_timeout=int(self.timeout))
                break
            except psycopg.OperationalError:
                if time.monotonic() - begin > self.timeout:
                    raise RuntimeError(f"serened did not accept connections in {self.timeout}s, see {log_path}")
                time.sleep(0.001)
        return {
            "ready_s": time.monotonic() - begin,
            "server_ready_s": logged_ready_s(log_path, self.timeout),
            "cpu_s": proc_cpu_seconds(self.proc.pid),
            "read_bytes": self.read_bytes(),
            "peak_rss_bytes": proc_peak_rss_bytes(self.proc.pid),
        }

    def read_bytes(self):
        return proc_read_bytes(self.proc.pid)

    def sql(self, query):
        with self.conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall() if cur.description else []

    def timed(self, query):
        before = self.read_bytes()
        cpu = proc_cpu_seconds(self.proc.pid)
        begin = time.monotonic()
        rows = self.sql(query)
        return {
            "s": time.monotonic() - begin,
            "cpu_s": proc_cpu_seconds(self.proc.pid) - cpu,
            "read_bytes": self.read_bytes() - before,
        }, rows

    def log_metrics(self):
        names = ", ".join(f"'{m}'" for m in LOG_METRICS)
        return dict(self.sql(f"SELECT metric, value FROM sdb_metrics WHERE metric IN ({names})"))

    def relation_metrics(self, relation):
        return dict(self.sql(f"SELECT metric, value FROM sdb_metrics WHERE relation_id = '{relation}'::regclass"))

    def stop(self, crash=False):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        if self.proc is None:
            return {}
        begin = time.monotonic()
        self.proc.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        try:
            self.proc.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            raise RuntimeError(f"serened did not stop in {self.timeout}s")
        finally:
            LIVE.remove(self)
        result = {"stop_s": time.monotonic() - begin, "exit_code": self.proc.returncode}
        self.proc = None
        return result

    def kill(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        subprocess.run(["pkill", "-KILL", "-f", str(self.datadir)], check=False)


class Blobs:
    def __init__(self, root):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def add(self, region, name, data):
        with open(self.root / f"{region}.bin", "ab") as out:
            offset = out.tell()
            out.write(data)
        with open(self.root / f"{region}.jsonl", "a") as index:
            index.write(json.dumps({"name": name, "offset": offset, "size": len(data)}) + "\n")


class PayloadStats:
    def __init__(self):
        self.records = 0
        self.framed_bytes = 0
        self.catalog_bytes = 0
        self.search_bytes = 0
        self.max_payload_bytes = 0
        self.by_size = {label: {"records": 0, "bytes": 0} for _, label in SIZE_BUCKETS}

    def add(self, payload):
        catalog = U32.unpack_from(payload)[0]
        search = len(payload) - U32.size - catalog
        if search < 0:
            raise RuntimeError(f"record of {len(payload)} bytes claims a catalog part of {catalog}")
        self.records += 1
        self.framed_bytes += HEADER.size + len(payload)
        self.catalog_bytes += catalog
        self.search_bytes += search
        self.max_payload_bytes = max(self.max_payload_bytes, len(payload))
        for limit, label in SIZE_BUCKETS:
            if limit is None or len(payload) < limit:
                self.by_size[label]["records"] += 1
                self.by_size[label]["bytes"] += len(payload)
                break
        return catalog, search

    def result(self):
        return {
            "records": self.records,
            "framed_bytes": self.framed_bytes,
            "catalog_bytes": self.catalog_bytes,
            "search_bytes": self.search_bytes,
            "max_payload_bytes": self.max_payload_bytes,
            "by_size": self.by_size,
        }


def parse_records(buf, start=0):
    payloads = []
    offset = start
    while offset + HEADER.size <= len(buf):
        _, size = HEADER.unpack_from(buf, offset)
        if size == 0 or size > len(buf) - offset - HEADER.size:
            break
        begin = offset + HEADER.size
        payloads.append(memoryview(buf)[begin : begin + size])
        offset = begin + size
    return payloads, offset


def dump_payload(blobs, region, name, payload):
    if blobs is None:
        return
    catalog = U32.unpack_from(payload)[0]
    if catalog:
        blobs.add(f"{region}_catalog", name, bytes(payload[U32.size : U32.size + catalog]))
    if len(payload) > U32.size + catalog:
        blobs.add(f"{region}_search", name, bytes(payload[U32.size + catalog :]))


def parse_wal(datadir, blobs, tag):
    result = {}
    covered = number = begin = 0
    wal = datadir / "wal"
    snapshot_path = datadir / "snapshot"
    if snapshot_path.exists():
        buf = snapshot_path.read_bytes()
        payloads, end = parse_records(buf)
        if not payloads or len(payloads[0]) != SNAPSHOT_HEADER.size:
            raise RuntimeError(f"{snapshot_path} does not start with a snapshot header")
        _, covered, number, begin, _ = SNAPSHOT_HEADER.unpack_from(payloads[0])
        history = PayloadStats()
        tables = PayloadStats()
        for i, payload in enumerate(payloads[1:]):
            catalog = U32.unpack_from(payload)[0]
            (history if catalog else tables).add(payload)
            dump_payload(blobs, f"{tag}_snapshot", f"record_{i:06d}", payload)
        result["snapshot"] = {
            "file_bytes": len(buf),
            "parsed_bytes": end,
            "covered_lsn": covered,
            "history": history.result(),
            "tables": tables.result(),
        }
    stats = PayloadStats()
    chunks = 0
    chunk_file_bytes = 0
    while True:
        path = wal / f"{number:016x}"
        if not path.exists():
            break
        buf = path.read_bytes()
        payloads, end = parse_records(buf, covered - begin if chunks == 0 else 0)
        if chunks and not payloads:
            break
        chunks += 1
        chunk_file_bytes += len(buf)
        for i, payload in enumerate(payloads):
            stats.add(payload)
            dump_payload(blobs, f"{tag}_log", f"{number:016x}_{i:06d}", payload)
        number += 1
    result["log"] = stats.result() | {
        "chunks": chunks,
        "chunk_file_bytes": chunk_file_bytes,
    }
    return result


def segment_files(store, relation, blobs, tag):
    per = {}
    for path in data_files(store):
        if path.suffix not in SEGMENT_SUFFIXES:
            continue
        size = path.stat().st_size
        with open(path, "rb") as f:
            f.seek(size - HEADER.size)
            _, footer = HEADER.unpack(f.read(HEADER.size))
            if footer > size - HEADER.size:
                raise RuntimeError(f"{path} claims a footer of {footer} bytes")
            if blobs is not None and path.suffix in FOOTER_SUFFIXES:
                f.seek(size - HEADER.size - footer)
                blobs.add(f"{tag}_footer{path.suffix.replace('.', '_')}", f"{relation}/{path.name}", f.read(footer))
        entry = per.setdefault(relation, {}).setdefault(
            path.suffix.lstrip("."),
            {"files": 0, "bytes": 0, "footer_bytes": 0, "max_footer_bytes": 0},
        )
        entry["files"] += 1
        entry["bytes"] += size
        entry["footer_bytes"] += footer
        entry["max_footer_bytes"] = max(entry["max_footer_bytes"], footer)
    return per


def hnsw_graph(store, blobs, tag, m, docs):
    paths = [p for p in data_files(store) if p.suffix == ".idx"]
    if len(paths) != 1:
        return {"skipped": f"{len(paths)} .idx files, expected one segment"}
    n = docs
    with open(paths[0], "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
        found = []
        at = data.find(struct.pack("<Q", n))
        while at != -1:
            base_at = at + 8 + n
            if (
                at >= 12
                and U32.unpack_from(data, at - 12)[0] == m
                and base_at + 8 <= len(data)
                and struct.unpack_from("<Q", data, base_at)[0] == n * 2 * m
            ):
                found.append(at)
            at = data.find(struct.pack("<Q", n), at + 1)
        if len(found) != 1:
            return {"skipped": f"{len(found)} graph headers in {paths[0].name}"}
        at = found[0]
        levels = bytes(data[at + 8 : at + 8 + n])
        base_at = at + 8 + n + 8
        base = data[base_at : base_at + n * 2 * m * 4]
        upper_at = base_at + len(base)
        upper_count = struct.unpack_from("<Q", data, upper_at)[0]
        upper = data[upper_at + 8 : upper_at + 8 + upper_count * 4]
        expected_upper = sum((level - 1) * m * levels.count(level) for level in set(levels) if level > 1)
        if upper_count != expected_upper:
            raise RuntimeError(f"{paths[0]}: upper links {upper_count}, levels imply {expected_upper}")
        empty_base = array.array("I", base).count(0xFFFFFFFF)
        empty_upper = array.array("I", upper).count(0xFFFFFFFF)
        if blobs is not None:
            blobs.add(f"{tag}_graph_levels", paths[0].name, levels)
            blobs.add(f"{tag}_graph_base", paths[0].name, bytes(base))
            blobs.add(f"{tag}_graph_upper", paths[0].name, bytes(upper))
        return {
            "file": paths[0].name,
            "offset": at - 12,
            "nodes": n,
            "m": m,
            "max_level": U32.unpack_from(data, at - 4)[0],
            "levels_bytes": len(levels),
            "base_bytes": len(base),
            "upper_bytes": len(upper),
            "base_empty_slots": empty_base,
            "upper_empty_slots": empty_upper,
        }


def fresh_dir(path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def hits_source(args):
    return f"read_parquet('{args.hits}', binary_as_string = true)"


def phase_log(args, out, blobs):
    datadir = fresh_dir(out / "log_data")
    server = Server(args.serened, datadir, out / "logs", args.timeout)
    res = {"loadavg": os.getloadavg()}
    try:
        res["empty_start"] = server.start("empty")
        server.sql("SET max_wal_size = '64GB'")
        source = hits_source(args)
        inline_rows = args.inline_commits * args.inline_rows
        total = inline_rows + args.single_commits
        server.sql(f"CREATE TABLE hits_log WITH (compaction_interval = 0) AS SELECT * FROM {source} LIMIT 0")
        server.sql(f"CREATE TEMP TABLE stage AS SELECT row_number() OVER () AS rn, * FROM {source} LIMIT {total}")
        staged = server.sql("SELECT count(*) FROM stage")[0][0]
        if staged != total:
            raise RuntimeError(f"{args.hits} has {staged} rows, the log phase needs {total}")

        log(f"log: {args.inline_commits} commits of {args.inline_rows} rows")
        before = server.log_metrics()["catalog_wal_appended_bytes"]
        samples = []
        for i in range(args.inline_commits):
            lo = i * args.inline_rows
            timing, _ = server.timed(
                f"INSERT INTO hits_log SELECT * EXCLUDE (rn) FROM stage WHERE rn > {lo} AND rn <= {lo + args.inline_rows}"
            )
            samples.append(timing["s"])
        after = server.log_metrics()["catalog_wal_appended_bytes"]
        res["inline_commits"] = {
            "rows_per_commit": args.inline_rows,
            "latency": latency(samples),
            "log_bytes": after - before,
            "log_bytes_per_commit": (after - before) / max(args.inline_commits, 1),
        }

        log(f"log: {args.single_commits} single-row commits")
        before = after
        samples = []
        for j in range(args.single_commits):
            timing, _ = server.timed(
                f"INSERT INTO hits_log SELECT * EXCLUDE (rn) FROM stage WHERE rn = {inline_rows + j + 1}"
            )
            samples.append(timing["s"])
        after = server.log_metrics()["catalog_wal_appended_bytes"]
        res["single_commits"] = {
            "latency": latency(samples),
            "log_bytes": after - before,
            "log_bytes_per_commit": (after - before) / max(args.single_commits, 1),
        }
        res["metrics_after_writes"] = server.log_metrics()
        res["table_after_writes"] = server.relation_metrics("hits_log")

        res["crash"] = server.stop(crash=True)
        res["wal_after_crash"] = parse_wal(datadir, blobs, "writes")
        res["evicted_before_recovery"] = evict(datadir)
        log("log: recovery")
        res["recovery"] = server.start("recovery")
        server.sql("CHECKPOINT")
        time.sleep(args.settle)
        timing, rows = server.timed("SELECT count(*) FROM hits_log")
        if rows[0][0] != total:
            raise RuntimeError(f"recovered {rows[0][0]} rows of {total}")
        res["recovery"]["first_query"] = timing
        res["table_after_recovery"] = server.relation_metrics("hits_log")
        res["metrics_after_checkpoint"] = server.log_metrics()
        res["crash_after_checkpoint"] = server.stop(crash=True)
        res["wal_after_checkpoint"] = parse_wal(datadir, blobs, "checkpoint")
        res["evicted_before_snapshot_start"] = evict(datadir)
        log("log: start from the snapshot")
        res["snapshot_start"] = server.start("snapshot")
        time.sleep(args.settle)
        timing, rows = server.timed("SELECT count(*) FROM hits_log")
        if rows[0][0] != total:
            raise RuntimeError(f"started with {rows[0][0]} rows of {total}")
        res["snapshot_start"]["first_query"] = timing
        log("log: checkpoint")
        server.sql("INSERT INTO hits_log SELECT * FROM hits_log LIMIT 1")
        res["checkpoint"], _ = server.timed("CHECKPOINT")
        res["clean_stop"] = server.stop()
    finally:
        server.kill()
    return datadir, res


def phase_update(args, out, blobs):
    datadir = fresh_dir(out / "update_data")
    server = Server(args.serened, datadir, out / "logs", args.timeout)
    res = {"loadavg": os.getloadavg()}
    try:
        server.start("empty")
        server.sql("SET max_wal_size = '64GB'")
        source = hits_source(args)
        server.sql(
            f"CREATE TABLE hits_upd WITH (compaction_interval = 0) AS SELECT * FROM {source} LIMIT {args.update_rows}"
        )
        server.sql("VACUUM (FLUSH_TABLE) hits_upd")
        server.sql("CHECKPOINT")
        commits = args.update_commits

        log(f"update: {commits} commits updating 1% of the rows each")
        before = server.log_metrics()["catalog_wal_appended_bytes"]
        samples = []
        for i in range(commits):
            timing, _ = server.timed(
                "UPDATE hits_upd SET \"Title\" = \"Title\" || ' (edited)', "
                '"ResolutionWidth" = "ResolutionWidth" + 1, "IsRefresh" = 1 - "IsRefresh" '
                f'WHERE hash("WatchID") % 100 = {i}'
            )
            samples.append(timing["s"])
        after = server.log_metrics()["catalog_wal_appended_bytes"]
        res["update_commits"] = {
            "latency": latency(samples),
            "log_bytes": after - before,
            "log_bytes_per_commit": (after - before) / max(commits, 1),
        }

        log(f"update: {commits} commits deleting 0.1% of the rows each")
        before = after
        samples = []
        for i in range(commits):
            timing, _ = server.timed(f'DELETE FROM hits_upd WHERE hash("WatchID") % 1000 = {500 + i}')
            samples.append(timing["s"])
        after = server.log_metrics()["catalog_wal_appended_bytes"]
        res["delete_commits"] = {
            "latency": latency(samples),
            "log_bytes": after - before,
            "log_bytes_per_commit": (after - before) / max(commits, 1),
        }
        expected = server.sql("SELECT count(*) FROM hits_upd")[0][0]

        res["crash"] = server.stop(crash=True)
        res["wal_after_crash"] = parse_wal(datadir, blobs, "updates")
        res["evicted_before_recovery"] = evict(datadir)
        log("update: recovery")
        res["recovery"] = server.start("recovery")
        server.sql("CHECKPOINT")
        time.sleep(args.settle)
        timing, rows = server.timed("SELECT count(*) FROM hits_upd")
        if rows[0][0] != expected:
            raise RuntimeError(f"recovered {rows[0][0]} rows of {expected}")
        res["recovery"]["first_query"] = timing
        res["clean_stop"] = server.stop()
    finally:
        server.kill()
    return datadir, res


def phase_bulk(args, out, blobs):
    datadir = fresh_dir(out / "bulk_data")
    store = datadir / "segments"
    server = Server(args.serened, datadir, out / "logs", args.timeout)
    res = {"loadavg": os.getloadavg()}
    try:
        server.start("empty")
        source = hits_source(args)
        limit = f" LIMIT {args.bulk_rows}" if args.bulk_rows else ""
        server.sql(f"CREATE TABLE hits WITH (compaction_interval = 0) AS SELECT * FROM {source} LIMIT 0")
        columns = ", ".join(f'"{c}"' for c in HITS_INDEXED)
        server.sql(f"CREATE INDEX hits_idx ON hits USING inverted ({columns})")
        res["hits_warmed"] = warm(args.hits)
        log("bulk: load")
        res["load"], _ = server.timed(f"INSERT INTO hits SELECT * FROM {source}{limit}")
        drop_cache(args.hits)
        log("bulk: flush and compact")
        res["flush"], _ = server.timed("VACUUM (FLUSH_TABLE) hits")
        if args.bulk_compact:
            res["compact"], _ = server.timed("VACUUM (COMPACT_TABLE) hits")
        res["table"] = server.relation_metrics("hits")
        rows = server.sql("SELECT count(*) FROM hits")[0][0]
        url = server.sql("SELECT \"URL\" FROM hits WHERE \"URL\" <> '' LIMIT 1")[0][0]
        res["rows"] = rows
        res["clean_stop"] = server.stop()
        res["data_dir_bytes"] = tree_bytes(datadir)
        res["files"] = segment_files(store, "hits", blobs, "bulk")
        res["evicted_before_open"] = evict(datadir)
        log("bulk: cold open")
        res["open"] = server.start("open")
        time.sleep(args.settle)
        res["open"]["count"], counted = server.timed("SELECT count(*) FROM hits")
        if counted[0][0] != rows:
            raise RuntimeError(f"opened {counted[0][0]} rows of {rows}")
        quoted = url.replace("'", "''")
        res["open"]["index_lookup"], _ = server.timed(f"SELECT count(*) FROM hits WHERE \"URL\" = '{quoted}'")
        res["clean_stop_after_open"] = server.stop()
    finally:
        server.kill()
    return datadir, res


def ann_query(args):
    vector = ", ".join(f"{((i * 37) % 201 - 100) / 100.0:.2f}" for i in range(args.dim))
    return f"SELECT id FROM vec ORDER BY emb <-> [{vector}]::FLOAT[{args.dim}] LIMIT 10"


def phase_hnsw(args, out, blobs):
    datadir = fresh_dir(out / "hnsw_data")
    store = datadir / "segments"
    server = Server(args.serened, datadir, out / "logs", args.timeout)
    res = {"loadavg": os.getloadavg()}
    try:
        server.start("empty")
        server.sql(f"CREATE TABLE vec (id BIGINT, emb FLOAT[{args.dim}]) WITH (compaction_interval = 0)")
        server.sql(
            "CREATE INDEX vec_idx ON vec USING inverted "
            f"(id, emb hnsw (metric = 'l2', m = {args.hnsw_m}, quant = 'tq', nb_bits = 2))"
        )
        log(f"hnsw: build {args.hnsw_rows} x {args.dim}")
        res["build"], _ = server.timed(
            "INSERT INTO vec SELECT i, "
            f"[((hash(i * {args.dim} + j) % 201)::INTEGER - 100)::FLOAT / 100.0 FOR j IN range({args.dim})]"
            f"::FLOAT[{args.dim}] FROM range({args.hnsw_rows}) t(i)"
        )
        res["flush"], _ = server.timed("VACUUM (FLUSH_TABLE) vec")
        res["compact"], _ = server.timed("VACUUM (COMPACT_TABLE) vec")
        res["table"] = server.relation_metrics("vec")
        res["explain"] = "\n".join(r[-1] for r in server.sql(f"EXPLAIN {ann_query(args)}"))
        res["clean_stop"] = server.stop()
        res["data_dir_bytes"] = tree_bytes(datadir)
        res["files"] = segment_files(store, "vec", blobs, "hnsw")
        res["graph"] = hnsw_graph(store, blobs, "hnsw", args.hnsw_m, res["table"]["num_docs"])
        res["evicted_before_open"] = evict(datadir)
        log("hnsw: cold first query")
        res["open"] = server.start("open")
        time.sleep(args.settle)
        res["open"]["first_ann"], _ = server.timed(ann_query(args))
        res["open"]["warm_ann"], _ = server.timed(ann_query(args))
        res["clean_stop_after_open"] = server.stop()
    finally:
        server.kill()
    return datadir, res


PHASES = {"log": phase_log, "update": phase_update, "bulk": phase_bulk, "hnsw": phase_hnsw}

SUMMARY = [
    ("log: bytes per inline commit", "log.inline_commits.log_bytes_per_commit"),
    ("log: inline commit p50 ms", "log.inline_commits.latency.p50_ms"),
    ("log: inline commit p99 ms", "log.inline_commits.latency.p99_ms"),
    ("log: bytes per single-row commit", "log.single_commits.log_bytes_per_commit"),
    ("log: single-row commit p50 ms", "log.single_commits.latency.p50_ms"),
    ("log: single-row commit p99 ms", "log.single_commits.latency.p99_ms"),
    ("log: records after writes", "log.wal_after_crash.log.records"),
    ("log: framed bytes after writes", "log.wal_after_crash.log.framed_bytes"),
    ("log: search part bytes", "log.wal_after_crash.log.search_bytes"),
    ("log: catalog part bytes", "log.wal_after_crash.log.catalog_bytes"),
    ("log: chunk file bytes", "log.wal_after_crash.log.chunk_file_bytes"),
    ("recovery: ready s", "log.recovery.ready_s"),
    ("recovery: server ready s", "log.recovery.server_ready_s"),
    ("recovery: cpu s", "log.recovery.cpu_s"),
    ("recovery: read bytes", "log.recovery.read_bytes"),
    ("recovery: peak RSS bytes", "log.recovery.peak_rss_bytes"),
    ("recovery: first query s", "log.recovery.first_query.s"),
    ("checkpoint: s", "log.checkpoint.s"),
    ("checkpoint: cpu s", "log.checkpoint.cpu_s"),
    ("snapshot: file bytes", "log.wal_after_checkpoint.snapshot.file_bytes"),
    ("snapshot: table record bytes", "log.wal_after_checkpoint.snapshot.tables.framed_bytes"),
    ("snapshot: history bytes", "log.wal_after_checkpoint.snapshot.history.framed_bytes"),
    ("snapshot start: ready s", "log.snapshot_start.ready_s"),
    ("snapshot start: server ready s", "log.snapshot_start.server_ready_s"),
    ("snapshot start: cpu s", "log.snapshot_start.cpu_s"),
    ("snapshot start: read bytes", "log.snapshot_start.read_bytes"),
    ("snapshot start: peak RSS bytes", "log.snapshot_start.peak_rss_bytes"),
    ("snapshot start: first query s", "log.snapshot_start.first_query.s"),
    ("update: bytes per update commit", "update.update_commits.log_bytes_per_commit"),
    ("update: update commit p50 ms", "update.update_commits.latency.p50_ms"),
    ("update: update commit p99 ms", "update.update_commits.latency.p99_ms"),
    ("update: bytes per delete commit", "update.delete_commits.log_bytes_per_commit"),
    ("update: delete commit p50 ms", "update.delete_commits.latency.p50_ms"),
    ("update: recovery ready s", "update.recovery.ready_s"),
    ("update: recovery server ready s", "update.recovery.server_ready_s"),
    ("update: recovery cpu s", "update.recovery.cpu_s"),
    ("update: recovery read bytes", "update.recovery.read_bytes"),
    ("bulk: parquet resident bytes", "bulk.hits_warmed.resident_bytes"),
    ("bulk: load s", "bulk.load.s"),
    ("bulk: load cpu s", "bulk.load.cpu_s"),
    ("bulk: load read bytes", "bulk.load.read_bytes"),
    ("bulk: flush s", "bulk.flush.s"),
    ("bulk: compact s", "bulk.compact.s"),
    ("bulk: compact cpu s", "bulk.compact.cpu_s"),
    ("bulk: data dir bytes", "bulk.data_dir_bytes"),
    ("bulk: segments", "bulk.files.hits.col.files"),
    ("bulk: .col bytes", "bulk.files.hits.col.bytes"),
    ("bulk: .col footer bytes", "bulk.files.hits.col.footer_bytes"),
    ("bulk: .idx bytes", "bulk.files.hits.idx.bytes"),
    ("bulk: .idx footer bytes", "bulk.files.hits.idx.footer_bytes"),
    ("bulk: cold ready s", "bulk.open.ready_s"),
    ("bulk: cold server ready s", "bulk.open.server_ready_s"),
    ("bulk: cold ready cpu s", "bulk.open.cpu_s"),
    ("bulk: cold ready read bytes", "bulk.open.read_bytes"),
    ("bulk: cold ready peak RSS bytes", "bulk.open.peak_rss_bytes"),
    ("bulk: cold count s", "bulk.open.count.s"),
    ("bulk: cold index lookup s", "bulk.open.index_lookup.s"),
    ("hnsw: build s", "hnsw.build.s"),
    ("hnsw: build cpu s", "hnsw.build.cpu_s"),
    ("hnsw: compact s", "hnsw.compact.s"),
    ("hnsw: compact cpu s", "hnsw.compact.cpu_s"),
    ("hnsw: .idx bytes", "hnsw.files.vec.idx.bytes"),
    ("hnsw: .idx footer bytes", "hnsw.files.vec.idx.footer_bytes"),
    ("hnsw: graph levels bytes", "hnsw.graph.levels_bytes"),
    ("hnsw: graph level-0 links bytes", "hnsw.graph.base_bytes"),
    ("hnsw: graph upper links bytes", "hnsw.graph.upper_bytes"),
    ("hnsw: graph empty level-0 slots", "hnsw.graph.base_empty_slots"),
    ("hnsw: cold ready s", "hnsw.open.ready_s"),
    ("hnsw: cold server ready s", "hnsw.open.server_ready_s"),
    ("hnsw: cold ready cpu s", "hnsw.open.cpu_s"),
    ("hnsw: cold ready read bytes", "hnsw.open.read_bytes"),
    ("hnsw: cold ready peak RSS bytes", "hnsw.open.peak_rss_bytes"),
    ("hnsw: cold first ANN s", "hnsw.open.first_ann.s"),
    ("hnsw: cold first ANN read bytes", "hnsw.open.first_ann.read_bytes"),
    ("hnsw: warm ANN s", "hnsw.open.warm_ann.s"),
]


def lookup(results, path):
    node = results
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def fmt(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.3f}"
    return f"{value:,}"


def print_summary(results):
    width = max(len(label) for label, _ in SUMMARY)
    for label, path in SUMMARY:
        value = lookup(results, path)
        if value is not None:
            print(f"{label:<{width}}  {fmt(value):>16}")


def compare(a_path, b_path):
    a = json.loads(Path(a_path).read_text())
    b = json.loads(Path(b_path).read_text())
    width = max(len(label) for label, _ in SUMMARY)
    print(f"{'':<{width}}  {a['label']:>16}  {b['label']:>16}  {'delta':>8}")
    for label, path in SUMMARY:
        x = lookup(a, path)
        y = lookup(b, path)
        if x is None and y is None:
            continue
        delta = f"{(y - x) / x * 100:+.1f}%" if x and y is not None else "-"
        print(f"{label:<{width}}  {fmt(x):>16}  {fmt(y):>16}  {delta:>8}")


def run(args):
    out = args.work / args.label
    if out.exists():
        if not args.force:
            raise SystemExit(f"{out} exists; pass --force to replace it")
        shutil.rmtree(out)
    (out / "logs").mkdir(parents=True)
    blobs = Blobs(out / "blobs") if args.dump_blobs else None
    binary = args.serened.resolve()
    results = {
        "label": args.label,
        "serened": str(binary),
        "serened_bytes": binary.stat().st_size,
        "serened_mtime": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(binary.stat().st_mtime)),
        "hits": str(args.hits),
        "kernel": platform.release(),
        "cpus": os.cpu_count(),
        "params": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items() if k != "func"},
    }
    try:
        for name in args.phases.split(","):
            log(f"phase {name}")
            datadir, results[name] = PHASES[name](args, out, blobs)
            if not args.keep_data:
                shutil.rmtree(datadir)
            (out / "results.json").write_text(json.dumps(results, indent=1))
    finally:
        (out / "results.json").write_text(json.dumps(results, indent=1))
    print_summary(results)
    log(f"results: {out / 'results.json'}")


def main():
    parser = argparse.ArgumentParser(
        description="Sizes, times and disk reads of what serened writes once and reads once: "
        "the log, its snapshot, segment footers and HNSW graphs."
    )
    sub = parser.add_subparsers(required=True)
    r = sub.add_parser("run", help="measure one serened binary")
    r.add_argument("--serened", type=Path, required=True)
    r.add_argument("--label", required=True)
    r.add_argument("--work", type=Path, required=True, help="results and data dirs, on the disk being measured")
    r.add_argument("--hits", type=Path, required=True, help="ClickBench hits parquet")
    r.add_argument("--phases", default="log,bulk,hnsw")
    r.add_argument("--inline-commits", type=int, default=100)
    r.add_argument("--inline-rows", type=int, default=2000, help="rows per commit, below the inline budget")
    r.add_argument("--single-commits", type=int, default=500)
    r.add_argument("--update-rows", type=int, default=1000000)
    r.add_argument("--update-commits", type=int, default=20)
    r.add_argument("--bulk-rows", type=int, default=0, help="0 loads the whole parquet")
    r.add_argument(
        "--bulk-compact",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="merge the loaded segments into one before the cold open",
    )
    r.add_argument("--hnsw-rows", type=int, default=100000)
    r.add_argument("--hnsw-m", type=int, default=16)
    r.add_argument("--dim", type=int, default=1536)
    r.add_argument("--timeout", type=float, default=600, help="seconds a start or stop may take")
    r.add_argument(
        "--settle",
        type=float,
        default=1.0,
        help="seconds to wait after a restart and its snapshot before the first query, past DuckDB's idle worker flush",
    )
    r.add_argument("--dump-blobs", action="store_true", help="write the region bytes for the codec sweep")
    r.add_argument("--keep-data", action="store_true")
    r.add_argument("--force", action="store_true")
    r.set_defaults(func=run)
    c = sub.add_parser("compare", help="compare two results.json")
    c.add_argument("a", type=Path)
    c.add_argument("b", type=Path)
    c.set_defaults(func=lambda a: compare(a.a, a.b))
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        args.func(args)
    finally:
        for server in list(LIVE):
            server.kill()


if __name__ == "__main__":
    main()
