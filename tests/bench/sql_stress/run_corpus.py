#!/usr/bin/env python3
import argparse
import hashlib
import pathlib
import re
import subprocess
import sys
import time

DIALECTS = ("duckdb", "postgres", "cedardb", "sqlite", "mysql", "clickhouse", "datafusion", "serenedb")


def find_queries(corpus, dialects, only):
    for project in sorted(p for p in corpus.iterdir() if p.is_dir() and not p.name.startswith("_")):
        if only and not any(o in project.name for o in only):
            continue
        for query in sorted(project.glob("*.sql")):
            parts = query.name.rsplit(".", 2)
            if len(parts) != 3 or parts[1] not in dialects:
                continue
            yield project, query, parts[0], parts[1]


def common_prefix(a, b):
    count = 0
    for x, y in zip(a, b):
        if x != y:
            return 0 if x.isdigit() and y.isdigit() else count
        count += 1
    return count


def setup_files(project, name, ports=None):
    if ports is not None:
        ported = ports / project.name / f"{name}.setup.sql"
        if ported.exists():
            return [ported]
    own = project / f"{name}.setup.sql"
    if own.exists():
        return [own]
    parts = name.split("__")
    setups = sorted(project.glob("*.setup.sql"))
    scored = [(common_prefix(parts[:-1], s.name[:-len(".setup.sql")].split("__")[:-1]), s) for s in setups]
    best = max((score for score, _ in scored), default=0)
    if best == 0 and parts[:-1]:
        return []
    chosen = [s for score, s in scored if score == best]
    for variant in ("sample", "down"):
        rest = [s for s in chosen if variant not in s.name]
        if rest:
            chosen = rest
    return chosen


LITERAL = re.compile(r"'([^'\n]{1,200})'")


def absolutize(text, project, ports=None):
    bases = [project / "data", project]
    if ports is not None:
        bases.append(ports / project.name / "data")

    def replace(match):
        literal = match.group(1)
        relative = literal[2:] if literal.startswith("./") else literal
        if relative.startswith("/") or not relative:
            return match.group(0)
        for base in bases:
            candidate = base / relative
            try:
                if candidate.is_file() or (any(c in relative for c in "*?[") and any(c in relative for c in "/.")
                                           and any(base.glob(relative))):
                    return f"'{candidate}'"
            except (OSError, ValueError, IndexError):
                pass
        return match.group(0)

    return LITERAL.sub(replace, text)


def server_alive(port):
    return subprocess.run(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", "-X", "-qtAc", "select 1"],
                          capture_output=True, timeout=10).returncode == 0


def resolve_query(args, project, query):
    port = args.ports / project.name / query.name
    return port if port.exists() else query


FLOAT_DIVISION_DIALECTS = ("duckdb", "mysql", "clickhouse")


def prelude(dialect, timeout):
    script = [
        "DROP SCHEMA IF EXISTS sql_stress CASCADE;",
        "CREATE SCHEMA sql_stress;",
        "SET search_path TO sql_stress, public;",
        f"SET statement_timeout = '{timeout}s';",
    ]
    if dialect in FLOAT_DIVISION_DIALECTS:
        script.append("SET integer_division = false;")
    return script


def run_one(args, project, query, name):
    script = prelude(query.name.rsplit(".", 2)[1], args.timeout)
    for setup in setup_files(project, name, args.ports):
        script.append(absolutize(setup.read_text(errors="replace"), project, args.ports))
    script.append(absolutize(resolve_query(args, project, query).read_text(errors="replace"), project, args.ports))
    combined = args.work / f"{project.name}__{name}.run.sql"
    combined.write_text("\n".join(script))
    command = ["psql", "-h", "127.0.0.1", "-p", str(args.port), "-U", "postgres", "-d", "postgres", "-X", "-q", "-A",
               "-t", "-v", "ON_ERROR_STOP=1", "-f", str(combined)]
    start = time.time()
    try:
        result = subprocess.run(command, capture_output=True, timeout=args.timeout + 5, cwd=project)
    except subprocess.TimeoutExpired:
        return "TIMEOUT", time.time() - start, b"", ""
    wall = time.time() - start
    error = result.stderr.decode(errors="replace").strip().splitlines()
    error_lines = [line for line in error if "ERROR" in line or "FATAL" in line]
    if result.returncode != 0:
        status = "ERROR"
        if not server_alive(args.port):
            status = "CRASH"
    else:
        status = "OK"
    return status, wall, result.stdout, (error_lines[0] if error_lines else "")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", type=pathlib.Path, default=pathlib.Path.home() / "projects/sql-stress/_corpus")
    parser.add_argument("--ports", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parent / "ports")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--dialects", default=",".join(DIALECTS))
    parser.add_argument("--only", default="")
    parser.add_argument("--restart", required=True)
    parser.add_argument("--work", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    dialects = set(args.dialects.split(","))
    only = [o for o in args.only.split(",") if o]
    with args.out.open("w") as out:
        out.write("project\tquery\tdialect\tstatus\twall_s\tout_bytes\tout_md5\terror\n")
        for project, query, name, dialect in find_queries(args.corpus, dialects, only):
            status, wall, stdout, error = run_one(args, project, query, name)
            (args.work / f"{project.name}__{name}.out").write_bytes(stdout)
            line = [project.name, query.name, dialect, status, f"{wall:.3f}", str(len(stdout)),
                    hashlib.md5(stdout).hexdigest(), error.replace("\t", " ")[:300]]
            out.write("\t".join(line) + "\n")
            out.flush()
            print("\t".join(line[:5] + line[7:]), flush=True)
            if status in ("TIMEOUT", "CRASH") or not server_alive(args.port):
                subprocess.run(args.restart, shell=True, check=True)


if __name__ == "__main__":
    sys.exit(main())
