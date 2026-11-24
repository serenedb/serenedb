import argparse
import contextlib
import importlib.util
import json
import os
import platform
import re
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import numpy as np
import psycopg
import pyarrow.parquet as pq
from vectorbench import metrics


K = 10
NPROBES = (8, 32, 128)
THREADS = 4
POSTING_SIZE = 1024
SAMPLE_FACTOR = 0.2
CREATE_INDEX = """CREATE INDEX items_ivf ON items
    USING inverted(id, emb ivf (metric = 'ip', quant = 'none'))"""
PROFILE = re.compile(r"IVF build profile rows=\d+ train_us=(\d+) assign_us=(\d+) gpu_batches=(\d+)")


@contextlib.contextmanager
def server(binary, data, log):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, SDB_IVF_BUILD_PROFILE="1")
    with log.open("wb") as output:
        process = subprocess.Popen(
            [str(binary), "server", str(data), f"--listen=postgres://127.0.0.1:{port}",
             "--log_level=debug"],
            stdout=output, stderr=subprocess.STDOUT, env=env,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                try:
                    conn = psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                                           dbname="postgres", autocommit=True, connect_timeout=1)
                    break
                except psycopg.OperationalError:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError(f"server failed to start, see {log}") from None
                    time.sleep(0.1)
            with conn:
                yield process.pid, port, conn
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def timed_build(conn, pid):
    def read_rss():
        value = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        return int(value.strip())

    rss = [read_rss()]
    stop = threading.Event()

    def sample_rss():
        while not stop.wait(0.1):
            try:
                rss.append(read_rss())
            except subprocess.CalledProcessError:
                return

    sampler = threading.Thread(target=sample_rss)
    sampler.start()
    start = time.perf_counter()
    try:
        conn.execute(CREATE_INDEX)
        conn.execute("VACUUM (FLUSH_INDEX) items_ivf")
        elapsed = time.perf_counter() - start
    finally:
        stop.set()
        sampler.join()
    return elapsed, max(rss) * 1024


def measure_recall(client_class, port, dims, queries, gt_ids, gt_dists, probes):
    client = client_class({"port": port, "dims": dims, "metric": "ip"})
    client.connect()
    result = {}
    with client.conn:
        client.conn.execute("SET statement_timeout = '1min'")
        for nprobe in probes:
            handle = client.prepare_group(
                f"SET sdb_ivf_search_nprobe = {nprobe}; "
                "SELECT id FROM items_ivf ORDER BY emb <#> $q LIMIT $k"
            )
            plan = client.explain(handle, queries[0], K, {})
            if "IRESEARCH_SCAN" not in plan or "Top:" not in plan:
                raise RuntimeError(f"expected an IVF top-k plan, got {plan}")
            returned = [client.search(handle, query, K, {}) for query in queries]
            if any(len(ids) != K or len(set(ids)) != K for ids in returned):
                raise RuntimeError("IVF query returned the wrong number of distinct IDs")
            result[nprobe] = metrics.recall(metrics.pad_ids(returned, K), gt_ids, gt_dists, K)["recall"]
    return result


def trial(args, label, binary, repeat, manifest, client_class, queries, gt_ids, gt_dists):
    stem = f"{label}-{repeat}"
    log = args.out / f"{stem}.log"
    shards = str(args.dataset / "base" / "part_*.parquet")
    dims = manifest["dims"]
    with tempfile.TemporaryDirectory(prefix=f"ivf-{stem}-") as data:
        with server(binary, Path(data), log) as (pid, port, conn):
            conn.execute("SET statement_timeout = '10min'")
            conn.execute(f"SET threads = {THREADS}")
            conn.execute(f"SET sdb_ivf_posting_size = {POSTING_SIZE}")
            conn.execute(f"SET sdb_ivf_sample_factor = {SAMPLE_FACTOR}")
            conn.execute(f"""CREATE TABLE items (id BIGINT PRIMARY KEY, emb FLOAT[{dims}])
                WITH (compaction_interval = 0)""")
            conn.execute(f"INSERT INTO items SELECT id, emb::FLOAT[{dims}] FROM read_parquet(%s)", (shards,))
            if conn.execute("SELECT count(*) FROM items").fetchone()[0] != manifest["rows"]:
                raise RuntimeError("loaded row count does not match the VectorBench manifest")
            build_seconds, peak_rss_bytes = timed_build(conn, pid)
            recall = measure_recall(client_class, port, dims, queries, gt_ids, gt_dists, NPROBES)
        with server(args.cpu_bin, Path(data), args.out / f"{stem}-reopen.log") as (_, port, _):
            reopened = measure_recall(client_class, port, dims, queries, gt_ids, gt_dists, NPROBES)
            if reopened != recall:
                raise RuntimeError(f"recall changed after reopening {stem} with the CPU binary")
    phase_rows = PROFILE.findall(log.read_text(errors="replace"))
    if not phase_rows:
        raise RuntimeError(f"missing IVF build profile in {log}")
    train_us, assign_us, gpu_batches = (sum(map(int, values)) for values in zip(*phase_rows))
    if bool(gpu_batches) != (label != "cpu"):
        raise RuntimeError(f"unexpected GPU batch count in {log}: {gpu_batches}")
    return {"device": label, "repeat": repeat, "build_seconds": build_seconds,
            "train_segment_seconds": train_us / 1e6, "assign_segment_seconds": assign_us / 1e6,
            "peak_sampled_rss_bytes": peak_rss_bytes, "gpu_batches": gpu_batches, "recall_at_10": recall}


def main():
    parser = argparse.ArgumentParser(description="Compare CPU and CUDA IVF builds on VectorBench data")
    for name in ("cpu-bin", "cuda-bin", "vectorbench", "dataset", "out"):
        parser.add_argument(f"--{name}", required=True, type=lambda value: Path(value).resolve())
    parser.add_argument("--cuda-train-only-bin", type=lambda value: Path(value).resolve())
    parser.add_argument("--query-limit", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    binaries = {
        "cpu": args.cpu_bin,
        "cuda": args.cuda_bin,
    }
    if args.cuda_train_only_bin is not None:
        binaries["cuda-train-only"] = args.cuda_train_only_bin
    if args.repeats < 1 or args.query_limit < 1:
        parser.error("--repeats and --query-limit must be positive")
    args.out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((args.dataset / "manifest.json").read_text())
    if manifest["metric"] != "ip" or manifest["rows"] < K:
        parser.error("expected prepared VectorBench inner-product data")
    table = pq.read_table(args.dataset / "queries.parquet", columns=["qid", "emb"]).slice(0, args.query_limit)
    if table.num_rows == 0:
        parser.error("no queries in the VectorBench dataset")
    qids = table["qid"].to_numpy()
    queries = table["emb"].combine_chunks().flatten().to_numpy().reshape(-1, manifest["dims"])
    with np.load(args.dataset / "gt" / "none.npz") as ground_truth:
        gt_ids, gt_dists = ground_truth["ids"][qids], ground_truth["dists"][qids]
    spec = importlib.util.spec_from_file_location(
        "vectorbench_serenedb_client", args.vectorbench / "serenedb-hnsw-quant" / "client.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    results = []
    variants = list(binaries.items())
    for repeat in range(1, args.repeats + 1):
        offset = (repeat - 1) % len(variants) if args.cuda_train_only_bin else 0
        for label, binary in variants[offset:] + variants[:offset]:
            with (args.out / f"{label}-{repeat}-load.log").open("w") as load_log:
                subprocess.run(["top", "-b", "-n", "2", "-d", "1", "-o", "%CPU"],
                               stdout=load_log, stderr=subprocess.STDOUT, check=True)
            point = trial(args, label, binary, repeat, manifest, module.Client, queries, gt_ids, gt_dists)
            results.append(point)
            print(json.dumps(point), flush=True)
    medians = {label: statistics.median(p["build_seconds"] for p in results if p["device"] == label)
               for label in binaries}
    report = {
        "dataset": args.dataset.name,
        "rows": manifest["rows"],
        "dims": manifest["dims"],
        "queries": len(qids),
        "platform": platform.platform(),
        "binaries": {label: str(binary) for label, binary in binaries.items()},
        "settings": {"threads": THREADS, "posting_size": POSTING_SIZE, "sample_factor": SAMPLE_FACTOR},
        "median_build_seconds": medians,
        "build_speedup": medians["cpu"] / medians["cuda"],
        "trials": results,
    }
    (args.out / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
