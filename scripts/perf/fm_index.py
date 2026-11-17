#!/usr/bin/env python3

import argparse
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import tempfile
import time


SOURCE = "https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet"
PREDICATES = [
    ("q21", "count(*)", "URL", "%google%", "", "Q21: count, URL contains `google`"),
    ("q22", "count(*)", "URL", "%google%", " AND SearchPhrase <> ''",
     "Q22: Q21 with nonempty SearchPhrase"),
    ("q23", "count(*)", "Title", "%Google%", " AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''",
     "Q23: count, Title contains `Google`, URL lacks `.google.`, nonempty SearchPhrase"),
    ("url_ids", "id", "URL", "%google%", " ORDER BY id", "IDs, URL contains `google`"),
    ("title_ids", "id", "Title", "%Google%", " ORDER BY id", "IDs, Title contains `Google`"),
    ("wiki", "count(*)", "URL", "%wiki%", "", "Count, URL contains `wiki`"),
    ("blog", "count(*)", "URL", "%blog%", "", "Count, URL contains `blog`"),
    ("games", "count(*)", "URL", "%games%", "", "Count, URL contains `games`"),
    ("absent", "count(*)", "URL", "%zzzzfmindexnotpresentzzzz%", "",
     "Count, URL contains an absent string"),
    ("yandex", "count(*)", "URL", "%yandex%", "", "Control: count, URL contains `yandex`"),
    ("com", "count(*)", "URL", "%com%", "", "Control: count, URL contains `com`"),
    ("http", "count(*)", "URL", "%http%", "", "Control: count, URL contains `http`"),
    ("prefix", "count(*)", "URL", "http%", "", "Control: count, URL starts with `http`"),
    ("suffix", "count(*)", "URL", "%.html", "", "Control: count, URL ends with `.html`"),
]
LABELS = {p[0]: p[5] for p in PREDICATES} | {"exact": "Control: count, URL equals one value"}
METHODS = {"scan": "No index (Parquet scan)", "ngram": "3-gram index"}


class HttpRangeFile(io.RawIOBase):
    def __init__(self, url):
        self.url, self.position = url, 0
        _, self.length = self.fetch(0, 1)

    def fetch(self, begin, end):
        with tempfile.TemporaryDirectory(prefix="fm-range-") as directory:
            body, headers = Path(directory) / "body", Path(directory) / "headers"
            subprocess.run(["curl", "-fsSL", "--retry", "2", "--connect-timeout", "20",
                            "--max-time", "120", "--max-filesize", str(end - begin),
                            "--range", f"{begin}-{end - 1}", "--dump-header", str(headers),
                            "--output", str(body), self.url], check=True)
            lines = headers.read_text().strip().split("\n\n")[-1].splitlines()
            fields = dict((key.lower(), value.strip()) for key, value in
                          (line.split(":", 1) for line in lines[1:] if ":" in line))
            content_range = fields.get("content-range", "")
            if lines[0].split()[1] != "206" or not content_range.startswith(f"bytes {begin}-{end - 1}/"):
                raise RuntimeError("dataset server returned an unexpected byte range")
            data = body.read_bytes()
        if len(data) != end - begin:
            raise RuntimeError("short dataset read")
        return data, int(content_range.split("/")[1])

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=0):
        if whence not in (0, 1, 2):
            raise ValueError("invalid seek origin")
        position = offset + (0, self.position, self.length)[whence]
        if position < 0:
            raise ValueError("negative seek")
        self.position = position
        return position

    def read(self, size=-1):
        end = self.length if size < 0 else min(self.length, self.position + size)
        if self.position >= end:
            return b""
        data, length = self.fetch(self.position, end)
        if length != self.length:
            raise RuntimeError("dataset size changed during download")
        self.position = end
        return data


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare(args):
    import pyarrow.parquet as pq
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with HttpRangeFile(args.source) as source:
        parquet = pq.ParquetFile(source)
        if not 0 < args.rows <= parquet.metadata.num_rows:
            raise ValueError("sample row count is outside the dataset")
        if not 1 <= args.groups <= min(parquet.num_row_groups, args.rows):
            raise ValueError("invalid number of sample row groups")
        groups = ([0] if args.groups == 1 else
                  [i * (parquet.num_row_groups - 1) // (args.groups - 1) for i in range(args.groups)])
        selected = []
        with pq.ParquetWriter(args.output, parquet.schema_arrow.empty_table().select(
                ["URL", "Title", "SearchPhrase"]).schema, compression="zstd") as writer:
            for position, group in enumerate(groups):
                table = parquet.read_row_group(group, columns=["URL", "Title", "SearchPhrase"])
                quota = args.rows // args.groups + (position < args.rows % args.groups)
                if table.num_rows < quota:
                    raise ValueError("selected row group is smaller than its sample quota")
                writer.write_table(table.slice(0, quota))
                selected.append({"row_group": group, "rows": quota})
        args.output.with_suffix(".metadata.json").write_text(json.dumps({
            "source": args.source, "source_rows": parquet.metadata.num_rows,
            "selected_row_groups": selected, "sha256": sha256(args.output)}, indent=2) + "\n")


def memory(pid):
    return {key: int(value.split()[0]) * 1024 for key, value in
            (line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines())
            if key in ("VmRSS", "VmHWM")}


@contextmanager
def server(args, mode):
    import psycopg
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    if subprocess.check_output(["ss", "-ltnH", f"sport = :{port}"], text=True).strip():
        raise RuntimeError("selected port is occupied")
    with tempfile.TemporaryDirectory(prefix="fm-bench-") as datadir, \
            args.output.with_suffix(f".{mode}.log").open("w") as log:
        process = subprocess.Popen([str(args.binary), datadir,
                                    f"--listen=postgres://127.0.0.1:{port}"],
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 60
            while process.poll() is None and time.monotonic() < deadline:
                try:
                    connection = psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                                                 dbname="postgres", autocommit=True,
                                                 connect_timeout=1, prepare_threshold=None)
                    break
                except psycopg.OperationalError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("serened did not become ready; inspect its log")
            with connection:
                if args.server_cpu not in os.sched_getaffinity(process.pid):
                    raise ValueError("requested CPU is outside the server's allowed affinity")
                for task in Path(f"/proc/{process.pid}/task").iterdir():
                    try:
                        os.sched_setaffinity(int(task.name), {args.server_cpu})
                    except ProcessLookupError:
                        continue
                connection.execute(f"SET threads = {args.threads}")
                connection.execute("SET statement_timeout = '180s'")
                connection.execute("SET sdb_enable_fm_index = false")
                yield connection, process.pid
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)


def queries(connection, indexed):
    from psycopg import sql
    table = "hits_idx" if indexed else "hits"
    result = {name: f"SELECT {projection} FROM {table} WHERE {column} "
                    f"{'@@ ts_like(' if indexed else 'LIKE '}'{pattern}'{')' if indexed else ''}{rest}"
              for name, projection, column, pattern, rest, _ in PREDICATES}
    exact = connection.execute("SELECT min(URL) FROM hits WHERE URL <> ''").fetchone()[0]
    if exact is None:
        raise ValueError("the sample needs a nonempty URL for the exact-match control")
    result["exact"] = sql.SQL("SELECT count(*) FROM {} WHERE URL = {}").format(
        sql.Identifier(table), sql.Literal(exact)).as_string(connection)
    return result


def measure(connection, query, expected=None):
    start = time.perf_counter()
    rows = connection.execute(query).fetchall()
    elapsed = (time.perf_counter() - start) * 1000
    digest = hashlib.sha256(json.dumps(rows, ensure_ascii=True, default=str,
                                      separators=(",", ":")).encode()).hexdigest()
    if expected is not None and digest != expected:
        raise RuntimeError(f"result does not match scan oracle: {query}")
    return {"ms": elapsed, "sha256": digest}


def median(runs):
    return statistics.median(r["ms"] for r in runs)


def run(args):
    from psycopg import sql
    if args.tries < 2 or args.threads < 1:
        raise ValueError("at least two pairs and one query thread are required")
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"backend": args.backend, "binary": str(args.binary), "binary_sha256": sha256(args.binary),
              "data_sha256": sha256(args.data), "threads": args.threads,
              "server_cpu_after_startup": args.server_cpu, "uname": list(os.uname()),
              "load_average_before": list(os.getloadavg()), "tries": args.tries, "modes": {}}
    expected = {}
    for mode in (("keyword", "scan", "ngram") if args.references else ("keyword",)):
        with server(args, mode) as (connection, pid):
            connection.execute(sql.SQL("CREATE VIEW hits AS SELECT file_row_number AS id, "
                "URL, Title, SearchPhrase FROM read_parquet({}, file_row_number = true)").format(
                    sql.Literal(str(args.data))))
            indexed = mode != "scan"
            start = time.perf_counter()
            if indexed:
                dictionary = "generate_wildcard_ngrams(keyword(), 3)" if mode == "ngram" else "keyword()"
                connection.execute(f"CREATE TEXT SEARCH DICTIONARY fm_bench_dict AS {dictionary}")
                connection.execute("CREATE INDEX hits_idx ON hits USING inverted "
                    "(id, URL fm_bench_dict, Title fm_bench_dict, SearchPhrase) WITH (compaction_interval = 0)")
            result = {"index_build_seconds": time.perf_counter() - start, "queries": {},
                      "load_average_before": list(os.getloadavg()),
                      "memory_before": memory(pid)}
            report["modes"][mode] = result
            workload = queries(connection, indexed)
            if mode == "keyword":
                report["oracle"] = {name: {"sql": query, **measure(connection, query)}
                                    for name, query in queries(connection, False).items()}
                expected = {name: r["sha256"] for name, r in report["oracle"].items()}
            else:
                workload = {name: query for name, query in workload.items()
                            if name in list(expected)[:3]}
            for name, query in workload.items():
                runs = [measure(connection, query, expected[name]) for _ in range(args.tries + 1)]
                result["queries"][name] = {"sql": query, "before": runs, "median_ms": median(runs[1:])}
            if mode == "keyword":
                result["memory_before"] = memory(pid)
                fm_bytes = lambda: int(connection.execute(
                    "SELECT sum(value) FROM sdb_metrics WHERE metric = 'fm_index_bytes'").fetchone()[0])
                if fm_bytes() != 0:
                    raise RuntimeError("disabled FM path constructed an index")
                connection.execute("SET sdb_enable_fm_index = true")
                result["first_use"] = {name: measure(connection, query, expected[name])
                                       for name, query in workload.items()}
                if fm_bytes() == 0:
                    raise RuntimeError("no FM index was constructed")
                result["fm_index_bytes"] = fm_bytes()
                result["idle_seconds"] = 60
                time.sleep(60)
                result["memory_after_idle"] = memory(pid)
                for name, query in workload.items():
                    runs = []
                    for cycle in range(args.tries):
                        for enabled in ((False, True) if cycle % 2 == 0 else (True, False)):
                            connection.execute(f"SET sdb_enable_fm_index = {'true' if enabled else 'false'}")
                            runs.append({"enabled": enabled, **measure(connection, query, expected[name])})
                    result["queries"][name]["pairs"] = runs
            result["plans"] = {name: connection.execute("EXPLAIN " + query).fetchall()
                               for name, query in workload.items()}
            result["index_metrics"] = dict(connection.execute(
                "SELECT metric, value FROM sdb_metrics WHERE relation_id = 'hits_idx'::regclass").fetchall()) if indexed else {}
            result["memory_after"] = memory(pid)
            result["load_average_after"] = list(os.getloadavg())
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    report["load_average_after"] = list(os.getloadavg())
    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print_tables([report])


def spread(runs):
    quartiles = statistics.quantiles([r["ms"] for r in runs], n=4, method="inclusive")
    return quartiles[2] - quartiles[0]


def check_baselines(reports):
    unstable = []
    for report in reports:
        for name, *_ in PREDICATES[:3]:
            query = report["modes"]["keyword"]["queries"][name]
            before = median(query["before"][1:])
            after = median([r for r in query["pairs"] if not r["enabled"]])
            if abs(after - before) > 0.15 * before:
                unstable.append(f"{report['backend']} {name.upper()}: {before:.2f} -> {after:.2f} ms "
                                f"({100 * (after / before - 1):+.1f}%)")
    if unstable:
        raise SystemExit("Unstable baseline (>15% change); retain the reports and investigate before "
                         "using these timings:\n" + "\n".join(unstable))


def print_tables(reports):
    if len({r["backend"] for r in reports}) != len(reports) or len({r["data_sha256"] for r in reports}) != 1:
        raise ValueError("reports need distinct backends and the same dataset")
    names = [q[0] for q in PREDICATES[:3]]
    arms = lambda query: [[r for r in query["pairs"] if r["enabled"] == on] for on in (False, True)]
    cell = lambda runs: f"{median(runs):.2f} [{spread(runs):.2f}]"
    mb = lambda value: f"{int(value)/1e6:.1f}"
    times, costs, references = [], [], []
    for report in reports:
        for mode, result in report["modes"].items():
            fm = mode == "keyword"
            selected = [result["queries"][name] for name in names]
            cost = [mb(result["index_metrics"].get("index_size", 0)),
                    f"{result['index_build_seconds']:.2f}" if mode != "scan" else "-"]
            memory = [mb(result["memory_before"]["VmRSS"]), mb(result["memory_after"]["VmHWM"])]
            if not fm:
                references.append(([METHODS[mode], *(cell(q["before"][1:]) for q in selected), "-"],
                                   [METHODS[mode], *cost, "-", "-", *memory]))
                continue
            if not times:
                times.append([f"Keyword index (baseline, `{report['backend']}` run)",
                              *(cell(arms(q)[0]) for q in selected), "-"])
            method = f"Keyword index + FM-index `{report['backend']}`"
            times.append([method, *(cell(arms(q)[1]) for q in selected),
                          " / ".join(f"{median(arms(q)[0])/median(arms(q)[1]):.1f}x" for q in selected)])
            first = " / ".join(f"{result['first_use'][name]['ms']/1000:.1f}" for name in (names[0], names[2]))
            costs.append([method, *cost, mb(result["fm_index_bytes"]), first, *memory])
    times += [r[0] for r in references]
    costs += [r[1] for r in references]
    print("| Search method | Q21, ms [IQR] | Q22, ms [IQR] | Q23, ms [IQR] | Speedup over the same run's baseline, Q21 / Q22 / Q23 |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for row in times:
        print("| " + " | ".join(row) + " |")
    print("\n| Search method | Index on disk, MB | Index build, s | FM-index memory, MB | First URL / Title query, s | RSS before queries, MB | Peak RSS, MB |")
    print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
    for row in costs:
        print("| " + " | ".join(row) + " |")
    report = reports[0]
    print(f"\n| Query (`{report['backend']}`) | Baseline before preparation, ms | Baseline, ms [IQR] | With FM-index, ms [IQR] | Speedup | Baseline change after preparation |")
    print("| --- | ---: | ---: | ---: | ---: | ---: |")
    for name, query in report["modes"]["keyword"]["queries"].items():
        before, (native, fm) = query["median_ms"], arms(query)
        print(f"| {LABELS[name]} | {before:.2f} | {cell(native)} | {cell(fm)} | "
              f"{median(native)/median(fm):.2f}x | {100*(median(native)/before-1):+.1f}% |")
    for report in reports:
        print(f"\n`{report['backend']}`: binary SHA-256 `{report['binary_sha256']}`; one-minute load average "
              f"{report['load_average_before'][0]:.2f} before, {report['load_average_after'][0]:.2f} after.")
    check_baselines(reports)


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(required=True)
    sample = commands.add_parser("prepare")
    sample.add_argument("--source", default=SOURCE)
    sample.add_argument("--rows", type=int, default=1000000)
    sample.add_argument("--groups", type=int, default=5)
    sample.add_argument("--output", type=Path, required=True)
    sample.set_defaults(action=prepare)
    evaluate = commands.add_parser("run")
    evaluate.add_argument("--binary", type=lambda v: Path(v).resolve(), required=True)
    evaluate.add_argument("--backend", choices=("rrr8", "rrr32", "plain8", "plain32"), required=True)
    evaluate.add_argument("--references", action="store_true")
    evaluate.add_argument("--data", type=lambda v: Path(v).resolve(), required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    evaluate.add_argument("--tries", type=int, default=6)
    evaluate.add_argument("--threads", type=int, default=1)
    evaluate.add_argument("--server-cpu", type=int, default=0)
    evaluate.set_defaults(action=run)
    report = commands.add_parser("report")
    report.add_argument("input", type=Path, nargs="+")
    report.set_defaults(action=lambda a: print_tables([json.loads(p.read_text()) for p in a.input]))
    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
