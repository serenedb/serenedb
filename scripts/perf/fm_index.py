#!/usr/bin/env python3

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import statistics
import subprocess
import tempfile
import time


CLICKBENCH_URL = "https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet"
COLUMNS = ["URL", "Title", "SearchPhrase"]
QUERY_SOURCE = ("https://github.com/ClickHouse/ClickBench/blob/"
                "a101cf89a316d3fffd77f54df065c67d07123681/clickhouse/queries.sql#L21-L24")
PREDICATES = [
    ("q21_url_count", "count(*)", "URL", "%google%", ""),
    ("q22_url_count", "count(*)", "URL", "%google%", " AND SearchPhrase <> ''"),
    ("q23_title_count", "count(*)", "Title", "%Google%",
     " AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''"),
    ("q24_url_locate", "id", "URL", "%google%", " ORDER BY id"),
    ("q23_title_locate", "id", "Title", "%Google%", " ORDER BY id"),
    ("control_dense_count", "count(*)", "URL", "%http%", ""),
    ("control_absent_count", "count(*)", "URL", "%zzzzfmindexnotpresentzzzz%", ""),
    ("control_prefix_count", "count(*)", "URL", "http%", ""),
]


class HttpRangeFile(io.RawIOBase):
    def __init__(self, url):
        super().__init__()
        self.url = url
        self.position = 0
        self.bytes_read = 0
        _, self.length = self.fetch(0, 1)

    def fetch(self, begin, end):
        with tempfile.TemporaryDirectory(prefix="boffa-fm-range-") as directory:
            body = Path(directory) / "body"
            headers = Path(directory) / "headers"
            subprocess.run(["curl", "-fsSL", "--retry", "2", "--connect-timeout", "20",
                            "--max-time", "120", "--max-filesize", str(end - begin),
                            "--range", f"{begin}-{end - 1}", "--dump-header", str(headers),
                            "--output", str(body), self.url], check=True)
            lines = headers.read_text().strip().split("\n\n")[-1].splitlines()
            fields = dict((key.lower(), value.strip()) for key, value in
                          (line.split(":", 1) for line in lines[1:] if ":" in line))
            content_range = fields.get("content-range", "")
            expected = f"bytes {begin}-{end - 1}/"
            if lines[0].split()[1] != "206" or not content_range.startswith(expected):
                raise RuntimeError("dataset server returned an unexpected byte range")
            data = body.read_bytes()
        if len(data) != end - begin:
            raise RuntimeError("short dataset read")
        self.bytes_read += len(data)
        return data, int(content_range.split("/")[1])

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence == 0:
            position = offset
        elif whence == 1:
            position = self.position + offset
        elif whence == 2:
            position = self.length + offset
        else:
            raise ValueError("invalid seek origin")
        if position < 0:
            raise ValueError("negative seek")
        self.position = position
        return position

    def read(self, size=-1):
        end = self.length if size < 0 else min(self.length, self.position + size)
        if self.position >= end:
            return b""
        begin = self.position
        data, length = self.fetch(begin, end)
        if length != self.length:
            raise RuntimeError("dataset size changed during download")
        self.position = end
        return data


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def prepare(args):
    import pyarrow.parquet as pq
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise RuntimeError(f"refusing to replace existing sample: {args.output}")
    source = HttpRangeFile(args.source)
    try:
        parquet = pq.ParquetFile(source)
        if not 0 < args.rows <= parquet.metadata.num_rows:
            raise ValueError("sample row count is outside the dataset")
        if not 1 <= args.groups <= min(parquet.num_row_groups, args.rows):
            raise ValueError("invalid number of sample row groups")
        group_ids = ([0] if args.groups == 1 else
                     [i * (parquet.num_row_groups - 1) // (args.groups - 1)
                      for i in range(args.groups)])
        selected = []
        remaining = args.rows
        writer = None
        try:
            groups = range(parquet.num_row_groups) if args.groups == 1 else group_ids
            for position, group in enumerate(groups):
                table = parquet.read_row_group(group, columns=COLUMNS)
                quota = (remaining if args.groups == 1 else
                         args.rows // args.groups + (position < args.rows % args.groups))
                take = min(quota, table.num_rows)
                if args.groups > 1 and take != quota:
                    raise ValueError("selected row group is smaller than its sample quota")
                table = table.slice(0, take)
                if writer is None:
                    writer = pq.ParquetWriter(args.output, table.schema, compression="zstd")
                writer.write_table(table)
                selected.append({"row_group": group, "rows": table.num_rows})
                remaining -= table.num_rows
                if remaining == 0:
                    break
        finally:
            if writer is not None:
                writer.close()
        metadata = {
            "source": args.source,
            "source_rows": parquet.metadata.num_rows,
            "sample_rows": args.rows,
            "selection": ("first rows in file order" if args.groups == 1 else
                          "prefixes of evenly spaced row groups"),
            "selected_row_groups": selected,
            "columns": COLUMNS,
            "downloaded_bytes": source.bytes_read,
            "parquet_bytes": args.output.stat().st_size,
            "sha256": sha256(args.output),
        }
        args.output.with_suffix(".metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        print(json.dumps(metadata, indent=2), flush=True)
    finally:
        source.close()


def result_digest(rows):
    payload = json.dumps(rows, ensure_ascii=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def resident_bytes(pid):
    values = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM"):
            values[key] = int(value.split()[0]) * 1024
    return values


def process_counters(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return {"cpu_ms": (int(fields[11]) + int(fields[12])) * 1000 / os.sysconf("SC_CLK_TCK"),
            "minor_faults": int(fields[7]), "major_faults": int(fields[9])}


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    listeners = subprocess.run(["ss", "-ltnH", f"sport = :{port}"], check=True,
                               text=True, capture_output=True).stdout
    if listeners.strip():
        raise RuntimeError("selected port is occupied")
    return port


def connect(process, port):
    import psycopg
    deadline = time.monotonic() + 60
    while process.poll() is None and time.monotonic() < deadline:
        try:
            return psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                                   dbname="postgres", autocommit=True, connect_timeout=1,
                                   prepare_threshold=None)
        except psycopg.OperationalError:
            time.sleep(0.1)
    raise RuntimeError("serened did not become ready; inspect its log")


def check(_args):
    import psycopg
    import pyarrow
    port = free_port()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", port))
        listener.listen(1)
        with socket.create_connection(("127.0.0.1", port), timeout=5):
            connection, _ = listener.accept()
            connection.close()
    print(f"Local SQL transport ready; psycopg {psycopg.__version__}, "
          f"PyArrow {pyarrow.__version__}")


def statements(indexed):
    queries = []
    for name, projection, column, pattern, rest in PREDICATES:
        predicate = (f"{column} @@ ts_like('{pattern}')" if indexed
                     else f"{column} LIKE '{pattern}'")
        query = (f"SELECT {projection} FROM hits_idx WHERE {predicate}{rest}" if indexed
                 else f"SELECT {projection} FROM hits WHERE {predicate}{rest}")
        queries.append((name, query, projection))
    return queries


def run_mode(args, binary, mode):
    from psycopg import sql
    indexed = mode != "scan"
    port = free_port()
    datadir = Path(tempfile.mkdtemp(prefix=f"boffa-fm-{mode}-"))
    log_path = args.output.parent / f"{mode}.server.log"
    measurement = {"binary": str(binary), "binary_sha256": sha256(binary),
                   "mode": mode, "queries": []}
    process = None
    try:
        with log_path.open("w") as log:
            process = subprocess.Popen([str(binary), str(datadir),
                                        f"--listen=postgres://127.0.0.1:{port}"],
                                       stdout=log, stderr=subprocess.STDOUT)
            with connect(process, port) as connection:
                connection.execute(f"SET threads = {args.threads}")
                connection.execute("SET statement_timeout = '120s'")
                connection.execute(sql.SQL("CREATE VIEW hits AS SELECT file_row_number AS id, "
                                           "URL, Title, SearchPhrase FROM read_parquet({}, file_row_number = true)")
                                   .format(sql.Literal(str(args.data))))
                started = time.perf_counter()
                if indexed:
                    dictionary = ("generate_wildcard_ngrams(keyword(), 3)" if mode == "ngram"
                                  else "keyword()")
                    connection.execute(f"CREATE TEXT SEARCH DICTIONARY fm_bench_dict AS {dictionary}")
                    connection.execute("CREATE INDEX hits_idx ON hits USING inverted "
                                       "(id, URL fm_bench_dict, Title fm_bench_dict, SearchPhrase) "
                                       "WITH (refresh_interval = 0, compaction_interval = 0)")
                measurement["index_build_seconds"] = time.perf_counter() - started
                measurement["before_queries"] = resident_bytes(process.pid)
                for name, query, projection in statements(indexed):
                    timings = []
                    counters = []
                    expected_digest = None
                    for attempt in range(args.tries):
                        before = process_counters(process.pid)
                        started = time.perf_counter()
                        rows = connection.execute(query).fetchall()
                        timings.append((time.perf_counter() - started) * 1000)
                        after = process_counters(process.pid)
                        counters.append({key: after[key] - value for key, value in before.items()})
                        digest = result_digest(rows)
                        if expected_digest is not None and expected_digest != digest:
                            raise RuntimeError(f"unstable results: {mode} {name}")
                        expected_digest = digest
                    plan = connection.execute("EXPLAIN " + query).fetchall()
                    result = {"name": name, "sql": query, "plan": plan,
                              "first_ms": timings[0], "warm_ms": timings[1:],
                              "process_counters": counters,
                              "warm_median_ms": statistics.median(timings[1:]),
                              "returned_rows": len(rows), "sha256": expected_digest}
                    if mode == "fm":
                        result["fm_index_serialized_bytes"] = int(connection.execute(
                            "SELECT sum(value) FROM sdb_metrics "
                            "WHERE metric = 'fm_index_serialized_bytes'").fetchone()[0])
                    if projection == "count(*)":
                        result["count"] = rows[0][0]
                    measurement["queries"].append(result)
                    print(f"{mode} {name}: {result['warm_median_ms']:.3f} ms, "
                          f"{len(rows)} result rows", flush=True)
                metrics = connection.execute("SELECT metric, value FROM sdb_metrics "
                                             "WHERE metric IN ('index_size', 'fm_index_serialized_bytes')").fetchall()
                measurement["metrics"] = metrics
                measurement["fm_index_serialized_bytes"] = sum(int(value) for metric, value in metrics
                                                              if metric == "fm_index_serialized_bytes")
                measurement["index_disk_bytes"] = sum(int(value) for metric, value in metrics
                                                      if metric == "index_size")
                measurement["after_queries"] = resident_bytes(process.pid)
                connection.execute("CHECKPOINT")
                measurement["database_directory_bytes"] = sum(path.stat().st_size
                                                               for path in datadir.rglob("*")
                                                               if path.is_file())
                if mode == "fm" and measurement["fm_index_serialized_bytes"] == 0:
                    raise RuntimeError("no FM-index was constructed; the intended path was not exercised")
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        shutil.rmtree(datadir)
    return measurement


def run(args):
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for binary in (args.baseline, args.candidate):
        if not binary.is_file():
            raise FileNotFoundError(binary)
    if not args.data.is_file():
        raise FileNotFoundError(args.data)
    if args.tries < 3:
        raise ValueError("at least three attempts are required")
    if args.threads < 1:
        raise ValueError("at least one query thread is required")
    report = {"data": str(args.data), "data_bytes": args.data.stat().st_size,
              "data_sha256": sha256(args.data), "uname": list(os.uname()),
              "query_source": QUERY_SOURCE, "attempts_per_query": args.tries,
              "threads": args.threads, "modes": []}
    report["machine"] = {
        "cpu_model": next(line.split(":", 1)[1].strip()
                          for line in Path("/proc/cpuinfo").read_text().splitlines()
                          if line.startswith("model name")),
        "logical_cpus": os.cpu_count(),
        "memory_total_bytes": int(Path("/proc/meminfo").read_text()
                                  .splitlines()[0].split()[1]) * 1024,
    }
    metadata = args.data.with_suffix(".metadata.json")
    if metadata.is_file():
        report["data_provenance"] = json.loads(metadata.read_text())
    modes = [(args.baseline, "scan"), (args.baseline, "keyword"),
             (args.candidate, "fm"), (args.baseline, "ngram")]
    expected = {}
    for binary, mode in modes:
        result = run_mode(args, binary, mode)
        for query in result["queries"]:
            name = query["name"]
            if name in expected and expected[name] != query["sha256"]:
                raise RuntimeError(f"result mismatch: {mode} {name}")
            expected[name] = query["sha256"]
        report["modes"].append(result)
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    preflight = commands.add_parser("check")
    preflight.set_defaults(action=check)
    sample = commands.add_parser("prepare")
    sample.add_argument("--source", default=CLICKBENCH_URL)
    sample.add_argument("--rows", type=int, default=100000)
    sample.add_argument("--groups", type=int, default=1)
    sample.add_argument("--output", type=Path, required=True)
    sample.set_defaults(action=prepare)
    evaluate = commands.add_parser("run")
    evaluate.add_argument("--baseline", type=lambda value: Path(value).resolve(), required=True)
    evaluate.add_argument("--candidate", type=lambda value: Path(value).resolve(), required=True)
    evaluate.add_argument("--data", type=lambda value: Path(value).resolve(), required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    evaluate.add_argument("--tries", type=int, default=3)
    evaluate.add_argument("--threads", type=int, default=2)
    evaluate.set_defaults(action=run)
    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
