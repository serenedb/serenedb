import argparse
import os
import statistics
import time

import psycopg

COLUMNS = "id BIGINT, a INT, b DOUBLE, s TEXT, t TIMESTAMP, f BOOLEAN"


def cpu_seconds(pid):
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def connect(port, timeout=0):
    deadline = time.monotonic() + timeout
    while True:
        try:
            return psycopg.connect(host="127.0.0.1", port=port, user="postgres",
                                   dbname="postgres", autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def make_data(port, rows, text_path, bin_path):
    if os.path.exists(text_path) and os.path.exists(bin_path):
        return
    with connect(port) as conn:
        conn.execute("DROP TABLE IF EXISTS gen")
        conn.execute(
            f"CREATE TABLE gen AS SELECT i::BIGINT AS id, (i * 7919 % 1000003)::INT AS a, "
            f"(i * 0.37)::DOUBLE AS b, 'name_' || (i * 104729 % 9999991)::VARCHAR AS s, "
            f"TIMESTAMP '2026-01-01' + to_seconds(i) AS t, i % 3 = 0 AS f "
            f"FROM range({rows}) r(i)")
        for path, opts in ((text_path, ""), (bin_path, "(FORMAT binary)")):
            with open(path + ".tmp", "wb") as out, conn.cursor() as cur:
                with cur.copy(f"COPY gen TO STDOUT {opts}") as copy:
                    for block in copy:
                        out.write(block)
            os.replace(path + ".tmp", path)
        conn.execute("DROP TABLE gen")


def run_copy(conn, table, path, binary, block):
    opts = "(FORMAT binary)" if binary else ""
    start = time.perf_counter()
    with conn.cursor() as cur, open(path, "rb") as src:
        with cur.copy(f"COPY {table} FROM STDIN {opts}") as copy:
            while data := src.read(block):
                copy.write(data)
    return time.perf_counter() - start


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="append", required=True,
                        help="LABEL:PORT:PID, repeat to compare servers")
    parser.add_argument("--data", required=True)
    parser.add_argument("--rows", type=int, default=4_000_000)
    parser.add_argument("--targets", default="duck,search")
    parser.add_argument("--formats", default="text,binary")
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--block", type=int, default=1 << 16)
    parser.add_argument("--extra", default="")
    args = parser.parse_args()

    servers = []
    for spec in args.server:
        label, port, pid = spec.rsplit(":", 2)
        servers.append((label, int(port), int(pid)))
    for _, port, _ in servers:
        connect(port, timeout=120).close()
    os.makedirs(args.data, exist_ok=True)
    text_path = os.path.join(args.data, f"rows_{args.rows}.tsv")
    bin_path = os.path.join(args.data, f"rows_{args.rows}.bin")
    make_data(servers[0][1], args.rows, text_path, bin_path)
    sizes = {"text": os.path.getsize(text_path), "binary": os.path.getsize(bin_path)}

    conns = {}
    for label, port, _ in servers:
        conn = connect(port)
        conn.execute(f"SET threads TO {args.threads}")
        for statement in filter(None, args.extra.split(";")):
            conn.execute(statement)
        conns[label] = conn

    print("| target | format | server | best s | median s | MB/s | Mrows/s | server cores | runs s/cores |")
    print("|---|---|---|---|---|---|---|---|---|")
    for target in args.targets.split(","):
        storage = " WITH (storage = 'search')" if target == "search" else ""
        for fmt in args.formats.split(","):
            runs = {label: ([], []) for label, _, _ in servers}
            for _ in range(args.repeat):
                for label, _, pid in servers:
                    conn = conns[label]
                    conn.execute("DROP TABLE IF EXISTS b")
                    conn.execute(f"CREATE TABLE b ({COLUMNS}){storage}")
                    cpu = cpu_seconds(pid)
                    took = run_copy(conn, "b", text_path if fmt == "text" else bin_path,
                                    fmt == "binary", args.block)
                    runs[label][0].append(took)
                    runs[label][1].append((cpu_seconds(pid) - cpu) / took)
                    conn.execute("DROP TABLE b")
            for label, _, _ in servers:
                times, cores = runs[label]
                best = min(times)
                each = " ".join(f"{t:.2f}/{c:.1f}" for t, c in zip(times, cores))
                print(f"| {target} | {fmt} | {label} | {best:.2f} | {statistics.median(times):.2f} | "
                      f"{sizes[fmt] / 1e6 / best:.0f} | {args.rows / best / 1e6:.2f} | "
                      f"{statistics.median(cores):.1f} | {each} |", flush=True)
    for conn in conns.values():
        conn.close()


if __name__ == "__main__":
    main()
