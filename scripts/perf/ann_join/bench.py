#!/usr/bin/env python3
r"""Nearest-neighbour joins: one batched join against a kNN query per row.

For every row of `users`, the k nearest rows of `items` through its vector
index, three ways on the same server and data:

- join:  SELECT ... FROM users u JOIN items_idx i NEAREST k BY DISTANCE ...
- loop:  one prepared `ORDER BY emb <-> $1 LIMIT k` per user, pipelined
- exact: the same join with EXACT, the ground truth for recall

The data is either dbpedia embeddings (a parquet with an `emb` list column;
items are its first --rows rows, users the next --queries) or a synthetic
Gaussian mixture of --dim dimensions. The harness starts its own serened on a
free port with a fresh datadir, builds one index, runs each mode --repeat
times interleaved, and prints one JSON line per run with the wall time, the
machine's load average when the run started, and the recall against exact.
With --cache cold, every timed run starts on a restarted serened whose datadir
was dropped from the page cache; every line gives the bytes serened read.

    scripts/perf/ann_join/bench.py --bin build_perf/bin/serened \
        --dataset ~/vecbench/dbpedia.parquet --rows 90000 --queries 1000
    scripts/perf/ann_join/bench.py --bin build_perf/bin/serened \
        --dim 128 --rows 1000000 --queries 10000 --index hnsw
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import numpy as np
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_server(binary, datadir, port, log):
    proc = subprocess.Popen(
        [binary, datadir, f"--listen=postgres://127.0.0.1:{port}"],
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(600):
        if proc.poll() is not None:
            sys.exit(f"serened exited with {proc.returncode}, see {log.name}")
        try:
            psycopg.connect(
                host="127.0.0.1", port=port, user="postgres", dbname="postgres"
            ).close()
            return proc
        except psycopg.OperationalError:
            time.sleep(0.5)
    proc.kill()
    sys.exit("serened did not come up")


def synthetic(rows, dim, clusters, seed):
    rng = np.random.default_rng(seed)
    centers = rng.standard_normal((clusters, dim)).astype(np.float32)
    which = rng.integers(0, clusters, rows)
    noise = rng.standard_normal((rows, dim)).astype(np.float32)
    return centers[which] + 0.5 * noise


def write_parquet(path, ids, vectors):
    dim = vectors.shape[1]
    emb = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.reshape(-1), type=pa.float32()), dim
    )
    pq.write_table(pa.table({"id": pa.array(ids, type=pa.int64()), "emb": emb}), path)


def prepare_data(args, workdir):
    items_path = os.path.join(workdir, "items.parquet")
    users_path = os.path.join(workdir, "users.parquet")
    if args.dataset:
        table = pq.read_table(args.dataset, columns=["emb"])
        total = args.rows + args.queries
        if table.num_rows < total:
            sys.exit(f"{args.dataset} has {table.num_rows} rows, {total} needed")
        emb = table.column("emb").combine_chunks().slice(0, total)
        dim = len(emb[0])
        flat = np.asarray(emb.flatten().to_numpy(zero_copy_only=False), dtype=np.float32)
        vectors = flat.reshape(total, dim)
    else:
        dim = args.dim
        vectors = synthetic(args.rows + args.queries, dim, args.clusters, args.seed)
    write_parquet(items_path, np.arange(args.rows), vectors[: args.rows])
    write_parquet(users_path, np.arange(args.queries), vectors[args.rows :])
    return dim, items_path, users_path, vectors[args.rows :]


def evict(datadir):
    os.sync()
    for root, _, files in os.walk(datadir):
        for name in files:
            try:
                fd = os.open(os.path.join(root, name), os.O_RDONLY)
            except OSError:
                continue
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)


def read_bytes(pid):
    with open(f"/proc/{pid}/io") as f:
        for line in f:
            if line.startswith("read_bytes:"):
                return int(line.split()[1])
    return 0


def load_avg():
    return os.getloadavg()[0]


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return time.perf_counter() - start, result


def neighbours(rows):
    out = {}
    for user, item in rows:
        out.setdefault(user, set()).add(item)
    return out


def recall(found, truth, k):
    hits = sum(len(found.get(u, set()) & items) for u, items in truth.items())
    return hits / (k * len(truth))


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--bin", required=True)
    ap.add_argument("--dataset", help="parquet with an emb list column")
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--clusters", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--queries", type=int, default=1000)
    ap.add_argument("--k", type=int, default=30)
    ap.add_argument("--index", choices=["ivf", "hnsw"], default="ivf")
    ap.add_argument("--quant", default="sq8")
    ap.add_argument(
        "--option", action="append", default=[],
        help="another index option, name = value",
    )
    ap.add_argument("--metric", default="l2", choices=["l2", "ip", "cosine"])
    ap.add_argument("--set", action="append", default=[], help="SET name = value")
    ap.add_argument("--modes", default="join,loop")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--keep", action="store_true", help="keep the datadir")
    ap.add_argument("--profile", help="record the join runs with perf into this file")
    ap.add_argument(
        "--cache", default="warm",
        help="comma list of warm and cold: a cold run starts on a restarted "
        "serened whose datadir was dropped from the page cache",
    )
    ap.add_argument(
        "--small", type=int, default=0,
        help="also run join_small and loop_small over the first N users",
    )
    args = ap.parse_args()
    caches = args.cache.split(",")

    op = {"l2": "<->", "ip": "<#>", "cosine": "<=>"}[args.metric]
    workdir = tempfile.mkdtemp(prefix="ann_join_")
    port = free_port()
    log = open(os.path.join(workdir, "serened.log"), "w")
    datadir = os.path.join(workdir, "data")
    os.mkdir(datadir)
    dim, items_path, users_path, user_vectors = prepare_data(args, workdir)
    server = {"proc": start_server(args.bin, datadir, port, log)}

    def connect():
        conn = psycopg.connect(
            host="127.0.0.1", port=port, user="postgres", dbname="postgres",
            autocommit=True,
        )
        for setting in args.set:
            conn.execute(f"SET {setting}")
        server["conn"] = conn
        return conn

    def restart_cold():
        server["conn"].close()
        server["proc"].terminate()
        server["proc"].wait()
        evict(datadir)
        server["proc"] = start_server(args.bin, datadir, port, log)
        connect()

    try:
        conn = connect()
        conn.execute(f"CREATE TABLE items (id BIGINT PRIMARY KEY, emb FLOAT[{dim}])")
        conn.execute(f"INSERT INTO items SELECT id, emb FROM read_parquet('{items_path}')")
        conn.execute(f"CREATE TABLE users (id BIGINT, emb FLOAT[{dim}])")
        conn.execute(f"INSERT INTO users SELECT id, emb FROM read_parquet('{users_path}')")
        conn.execute(f"CREATE TABLE users_small (id BIGINT, emb FLOAT[{dim}])")
        conn.execute(
            f"INSERT INTO users_small SELECT id, emb FROM "
            f"read_parquet('{users_path}') WHERE id < {args.small}"
        )
        options = ", ".join(
            [f"metric = '{args.metric}'", f"quant = '{args.quant}'", *args.option]
        )
        build, _ = timed(lambda: (
            conn.execute(
                f"CREATE INDEX items_idx ON items USING inverted"
                f"(id, emb {args.index} ({options}))"
            ),
            conn.execute("VACUUM (REFRESH_TABLE) items"),
        ))
        print(json.dumps({"event": "built", "seconds": round(build, 3)}), flush=True)

        def join_sql(kind, users="users"):
            return (
                f"SELECT u.id, i.id FROM {users} u JOIN items_idx i {kind}NEAREST "
                f"{args.k} BY DISTANCE u.emb {op} i.emb"
            )

        _, exact_rows = timed(lambda: conn.execute(join_sql("EXACT ")).fetchall())
        truth = neighbours(exact_rows)
        small_truth = {u: truth[u] for u in range(min(args.small, args.queries))}
        knn = (
            f"SELECT id FROM items_idx ORDER BY emb {op} %s::FLOAT[{dim}] "
            f"LIMIT {args.k}"
        )
        literals = ["[" + ",".join(f"{x:.7g}" for x in v) + "]" for v in user_vectors]

        def run_join(users="users"):
            return server["conn"].execute(join_sql("", users)).fetchall()

        def run_loop(count=args.queries):
            rows = []
            conn = server["conn"]
            with conn.pipeline():
                cursors = [
                    conn.execute(knn, (lit,), prepare=True)
                    for lit in literals[:count]
                ]
            for user, cur in enumerate(cursors):
                rows.extend((user, item) for (item,) in cur.fetchall())
            return rows

        def run_exact():
            return server["conn"].execute(join_sql("EXACT ")).fetchall()

        runners = {
            "join": (run_join, truth),
            "loop": (run_loop, truth),
            "exact": (run_exact, truth),
            "join_small": (lambda: run_join("users_small"), small_truth),
            "loop_small": (lambda: run_loop(args.small), small_truth),
        }
        modes = args.modes.split(",")
        if args.small:
            modes += ["join_small", "loop_small"]
        profiler = None
        if args.profile:
            profiler = subprocess.Popen(
                ["perf", "record", "-F", "999", "-g", "-p",
                 str(server["proc"].pid), "-o", args.profile],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(1)
        for rep in range(args.repeat):
            for cache in caches:
                if cache == "warm":
                    for mode in modes:
                        runners[mode][0]()
                for mode in modes:
                    if cache == "cold":
                        restart_cold()
                    run, expected = runners[mode]
                    pid = server["proc"].pid
                    load = load_avg()
                    cpu = cpu_seconds(pid)
                    read = read_bytes(pid)
                    seconds, rows = timed(run)
                    cpu = cpu_seconds(pid) - cpu
                    read = read_bytes(pid) - read
                    print(json.dumps({
                        "mode": mode,
                        "rep": rep,
                        "cache": cache,
                        "seconds": round(seconds, 4),
                        "server_cpu_seconds": round(cpu, 2),
                        "read_mb": round(read / 2**20, 2),
                        "per_query_ms": round(1000 * seconds / len(expected), 4),
                        "recall": round(recall(neighbours(rows), expected, args.k), 4),
                        "load": round(load, 2),
                        "index": args.index,
                        "quant": args.quant,
                        "options": args.option,
                        "rows": args.rows,
                        "queries": len(expected),
                        "dim": dim,
                        "k": args.k,
                    }), flush=True)
        if profiler:
            profiler.send_signal(2)
            profiler.wait()
    finally:
        server["proc"].terminate()
        server["proc"].wait()
        log.close()
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
